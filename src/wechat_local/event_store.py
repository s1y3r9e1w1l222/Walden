"""Private event storage for durable live-message ingestion.

The source identity deliberately includes an operator-scoped account id, a
source generation, the shard name, the message table, and ``local_id``.  A
stable event may have multiple immutable revisions so later reconciliation can
record recalls or content changes without changing the event identity.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import uuid
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Iterable, Mapping

from .common import ensure_private_parent
from .live_sessions import SessionHint, SessionHintRegression


SCHEMA_VERSION = 7
APPLICATION_ID = 0x574C5231  # ASCII "WLR1"
INITIAL_POSITIONS = {"beginning", "latest"}
SOURCE_INITIALIZATION_POLICIES = INITIAL_POSITIONS | {"operator-hold"}
QUALITY_STATUSES = {"normalized", "degraded"}
STANDARD_EVENT_CONSUMER = "standard-event-normalizer"


class EventStoreError(RuntimeError):
    """Raised when the private event store cannot preserve its invariants."""


class MaterializationConflict(EventStoreError):
    """Raised when pinned versions produce a different materialization hash."""


class MissingSourceRevision(EventStoreError):
    """Raised when a claimed outbox signal has no complete raw source relation."""


@dataclass(frozen=True)
class SourceKey:
    """Identity of one physical message table within one database generation."""

    account_id: str
    source_generation: str
    shard_name: str
    table_name: str

    def __post_init__(self) -> None:
        for field_name in (
            "account_id",
            "source_generation",
            "shard_name",
            "table_name",
        ):
            value = getattr(self, field_name)
            if not value or value.strip() != value:
                raise ValueError(f"{field_name} must be a non-empty trimmed string")
            if any(ord(character) < 32 for character in value):
                raise ValueError(f"{field_name} must not contain control characters")


@dataclass(frozen=True)
class RawMessage:
    """Raw fields needed to persist and later reprocess one physical message."""

    local_id: int
    server_id: int | None
    local_type: int
    create_time: int
    real_sender_id: int | None
    raw_content: str | bytes | None
    compression_type: int | None
    # Bounded, query-window-only source XML for display facts such as mentions.
    # These fields are intentionally not persisted into EventStore revisions.
    raw_source: str | bytes | None = None
    source_compression_type: int | None = None

    def __post_init__(self) -> None:
        if self.local_id < 0:
            raise ValueError("local_id must not be negative")


@dataclass(frozen=True)
class StoreBatchResult:
    inserted_events: int
    inserted_revisions: int
    parse_errors: int
    cursor: int


@dataclass(frozen=True)
class WindowIngestRun:
    """Bounded, content-free facts describing a single source window read."""

    requested_start: datetime
    requested_end: datetime
    source_coverage: Mapping[str, int | None]

    def __post_init__(self) -> None:
        for value, name in (
            (self.requested_start, "requested_start"),
            (self.requested_end, "requested_end"),
        ):
            if not isinstance(value, datetime):
                raise TypeError(f"{name} must be a datetime")
            if value.tzinfo is None or value.utcoffset() != timedelta(0):
                raise ValueError(f"{name} must be UTC")
        if self.requested_end < self.requested_start:
            raise ValueError("requested_end must not be before requested_start")
        if not isinstance(self.source_coverage, Mapping):
            raise TypeError("source_coverage must be a mapping")
        expected = {"source_min_time", "source_max_time", "examined_count"}
        if set(self.source_coverage) != expected:
            raise ValueError("source_coverage has unsupported or missing fields")
        coverage = dict(self.source_coverage)
        for name in ("source_min_time", "source_max_time"):
            value = coverage[name]
            if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
                raise TypeError(f"{name} must be an integer or None")
        examined_count = coverage["examined_count"]
        if isinstance(examined_count, bool) or not isinstance(examined_count, int):
            raise TypeError("examined_count must be an integer")
        if examined_count < 0:
            raise ValueError("examined_count must not be negative")
        minimum = coverage["source_min_time"]
        maximum = coverage["source_max_time"]
        if minimum is not None and maximum is not None and minimum > maximum:
            raise ValueError("source_min_time must not exceed source_max_time")
        object.__setattr__(self, "source_coverage", MappingProxyType(coverage))

    @property
    def requested_start_utc(self) -> str:
        return self.requested_start.astimezone(timezone.utc).isoformat(timespec="microseconds")

    @property
    def requested_end_utc(self) -> str:
        return self.requested_end.astimezone(timezone.utc).isoformat(timespec="microseconds")


@dataclass(frozen=True)
class WindowIngestResult:
    """Outcome for one cursor-free, idempotent window persistence attempt."""

    run_id: str
    inserted_events: int
    inserted_revisions: int
    parse_errors: int


@dataclass(frozen=True)
class SessionScanTask:
    account_id: str
    source_generation: str
    session_database_name: str
    username: str
    table_name: str
    target_local_id: int
    target_timestamp: int
    lease_owner: str


@dataclass(frozen=True)
class PhysicalScanTask:
    source: SourceKey
    request_version: int
    claim_version: int
    target_local_id: int
    lease_owner: str
    lease_epoch: int
    lease_expires_at: str


@dataclass(frozen=True)
class ScanLeaseFence:
    source: SourceKey
    lease_owner: str
    lease_epoch: int
    claim_version: int


@dataclass(frozen=True)
class SessionObservationResult:
    observed_sessions: int
    changed_sessions: int
    queued_sessions: int


@dataclass(frozen=True)
class OutboxConsumerState:
    """Consumer-owned processing state anchored to one durable outbox signal."""

    id: int
    outbox_id: int
    consumer_name: str
    contract_version: str
    normalizer_version: str
    status: str
    attempt_count: int
    available_at: str
    lease_owner: str | None
    lease_expires_at: str | None
    last_error_type: str
    completed_at: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class StandardEventMaterializationInput:
    """Caller-provided derived projection of one immutable raw revision."""

    source_revision_id: int
    event_id: str
    subject_id: str
    contract_version: str
    normalizer_name: str
    normalizer_version: str
    canonical_payload_json: str
    payload_sha256: str
    quality_status: str


@dataclass(frozen=True)
class StandardEventMaterialization:
    """Stored, append-only Standard Event materialization."""

    id: int
    source_revision_id: int
    event_id: str
    subject_id: str
    contract_version: str
    normalizer_name: str
    normalizer_version: str
    canonical_payload_json: str
    payload_sha256: str
    quality_status: str
    created_at: str


@dataclass(frozen=True)
class MaterializationWriteResult:
    materialization: StandardEventMaterialization
    inserted: bool


@dataclass(frozen=True)
class ClaimedProcessEvent:
    """Exact immutable raw event/revision selected by one claimed outbox state."""

    consumer_state_id: int
    outbox_id: int
    task_kind: str
    source_revision_id: int
    event_uid: str
    account_id: str
    source_generation: str
    shard_name: str
    table_name: str
    local_id: int
    content_fingerprint: str
    revision_number: int
    server_id: str | None
    local_type: int
    create_time: int | None
    real_sender_id: int | None
    raw_content: str | bytes | None = field(repr=False)
    raw_content_kind: str
    compression_type: int | None
    observed_at: str


@dataclass(frozen=True)
class OutboxConsumerStateCounts:
    """Aggregate-only lifecycle counts for one consumer version profile."""

    pending: int
    processing: int
    complete: int
    failed: int


Parser = Callable[[RawMessage], Mapping[str, Any]]


CORE_SCHEMA = """
CREATE TABLE IF NOT EXISTS cursors (
    account_id TEXT NOT NULL,
    source_generation TEXT NOT NULL,
    shard_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    last_local_id INTEGER NOT NULL CHECK(last_local_id >= 0),
    initial_position TEXT NOT NULL CHECK(initial_position IN ('beginning', 'latest')),
    initialized_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (account_id, source_generation, shard_name, table_name)
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    event_uid TEXT NOT NULL UNIQUE,
    account_id TEXT NOT NULL,
    source_generation TEXT NOT NULL,
    shard_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    local_id INTEGER NOT NULL CHECK(local_id >= 0),
    current_revision_id INTEGER REFERENCES event_revisions(id) ON DELETE SET NULL,
    first_seen_at TEXT NOT NULL,
    UNIQUE (account_id, source_generation, shard_name, table_name, local_id)
);

CREATE TABLE IF NOT EXISTS event_revisions (
    id INTEGER PRIMARY KEY,
    event_id INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    revision_number INTEGER NOT NULL CHECK(revision_number > 0),
    content_fingerprint TEXT NOT NULL,
    server_id TEXT,
    local_type INTEGER NOT NULL,
    create_time INTEGER NOT NULL,
    real_sender_id INTEGER,
    raw_content BLOB,
    raw_content_kind TEXT NOT NULL CHECK(raw_content_kind IN ('null', 'text', 'blob')),
    compression_type INTEGER,
    parse_status TEXT NOT NULL CHECK(parse_status IN ('pending', 'parsed', 'error')),
    parse_error TEXT NOT NULL,
    parsed_payload_json TEXT,
    observed_at TEXT NOT NULL,
    UNIQUE (event_id, revision_number),
    UNIQUE (event_id, content_fingerprint)
);

CREATE TABLE IF NOT EXISTS processing_outbox (
    id INTEGER PRIMARY KEY,
    revision_id INTEGER NOT NULL UNIQUE
        REFERENCES event_revisions(id) ON DELETE CASCADE,
    task_kind TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'processing', 'complete', 'failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
    available_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS event_revisions_event_idx
    ON event_revisions(event_id, revision_number);
CREATE INDEX IF NOT EXISTS processing_outbox_status_idx
    ON processing_outbox(status, available_at, id);
"""


SESSION_SCHEMA = """
CREATE TABLE IF NOT EXISTS session_observations (
    account_id TEXT NOT NULL,
    source_generation TEXT NOT NULL,
    session_database_name TEXT NOT NULL,
    username TEXT NOT NULL,
    table_name TEXT NOT NULL,
    seen_last_local_id INTEGER NOT NULL CHECK(seen_last_local_id >= 0),
    seen_last_timestamp INTEGER NOT NULL,
    completed_last_local_id INTEGER NOT NULL CHECK(completed_last_local_id >= 0),
    completed_last_timestamp INTEGER NOT NULL,
    initialized_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (
        account_id, source_generation, session_database_name, username
    )
);

CREATE TABLE IF NOT EXISTS session_scan_queue (
    account_id TEXT NOT NULL,
    source_generation TEXT NOT NULL,
    session_database_name TEXT NOT NULL,
    username TEXT NOT NULL,
    table_name TEXT NOT NULL,
    target_local_id INTEGER NOT NULL CHECK(target_local_id >= 0),
    target_timestamp INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'processing', 'blocked')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
    available_at TEXT NOT NULL,
    lease_owner TEXT,
    lease_expires_at TEXT,
    last_error TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (
        account_id, source_generation, session_database_name, username
    ),
    FOREIGN KEY (
        account_id, source_generation, session_database_name, username
    ) REFERENCES session_observations (
        account_id, source_generation, session_database_name, username
    ) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS session_scan_queue_ready_idx
    ON session_scan_queue(status, available_at, updated_at);
"""


# These additive tables never replace raw events/revisions as source truth.
# ``processing_outbox`` remains an owner-neutral durable work signal; each
# downstream consumer owns only its row below.
NORMALIZATION_SCHEMA = """
CREATE TABLE IF NOT EXISTS outbox_consumer_state (
    id INTEGER PRIMARY KEY,
    outbox_id INTEGER NOT NULL
        REFERENCES processing_outbox(id) ON DELETE RESTRICT,
    consumer_name TEXT NOT NULL
        CHECK(length(consumer_name) BETWEEN 1 AND 100
              AND consumer_name = trim(consumer_name)),
    contract_version TEXT NOT NULL
        CHECK(length(contract_version) BETWEEN 1 AND 100
              AND contract_version = trim(contract_version)),
    normalizer_version TEXT NOT NULL
        CHECK(length(normalizer_version) BETWEEN 1 AND 100
              AND normalizer_version = trim(normalizer_version)),
    status TEXT NOT NULL
        CHECK(status IN ('pending', 'processing', 'complete', 'failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
    available_at TEXT NOT NULL CHECK(length(available_at) > 0),
    lease_owner TEXT
        CHECK(lease_owner IS NULL OR (
            length(lease_owner) BETWEEN 1 AND 100
            AND lease_owner = trim(lease_owner)
        )),
    lease_expires_at TEXT,
    last_error_type TEXT NOT NULL DEFAULT ''
        CHECK(
            last_error_type = '' OR (
                length(last_error_type) BETWEEN 1 AND 100
                AND substr(last_error_type, 1, 1) GLOB '[A-Za-z]'
                AND last_error_type NOT GLOB '*[^A-Za-z0-9_]*'
            )
        ),
    completed_at TEXT,
    created_at TEXT NOT NULL CHECK(length(created_at) > 0),
    updated_at TEXT NOT NULL CHECK(length(updated_at) > 0),
    UNIQUE (outbox_id, consumer_name, contract_version, normalizer_version),
    CHECK (
        (status = 'processing'
         AND lease_owner IS NOT NULL
         AND lease_expires_at IS NOT NULL
         AND completed_at IS NULL)
        OR
        (status IN ('pending', 'failed')
         AND lease_owner IS NULL
         AND lease_expires_at IS NULL
         AND completed_at IS NULL)
        OR
        (status = 'complete'
         AND lease_owner IS NULL
         AND lease_expires_at IS NULL
         AND completed_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS outbox_consumer_state_ready_idx
    ON outbox_consumer_state(
        consumer_name, contract_version, normalizer_version,
        status, available_at, id
    );
CREATE INDEX IF NOT EXISTS outbox_consumer_state_expired_lease_idx
    ON outbox_consumer_state(
        consumer_name, contract_version, normalizer_version,
        status, lease_expires_at, id
    );

CREATE TABLE IF NOT EXISTS standard_event_materializations (
    id INTEGER PRIMARY KEY,
    source_revision_id INTEGER NOT NULL
        REFERENCES event_revisions(id) ON DELETE RESTRICT,
    event_id TEXT NOT NULL
        CHECK(length(event_id) BETWEEN 1 AND 200 AND event_id = trim(event_id)),
    subject_id TEXT NOT NULL
        CHECK(length(subject_id) BETWEEN 1 AND 200 AND subject_id = trim(subject_id)),
    contract_version TEXT NOT NULL
        CHECK(length(contract_version) BETWEEN 1 AND 100
              AND contract_version = trim(contract_version)),
    normalizer_name TEXT NOT NULL
        CHECK(length(normalizer_name) BETWEEN 1 AND 100
              AND normalizer_name = trim(normalizer_name)),
    normalizer_version TEXT NOT NULL
        CHECK(length(normalizer_version) BETWEEN 1 AND 100
              AND normalizer_version = trim(normalizer_version)),
    canonical_payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL
        CHECK(length(payload_sha256) = 64
              AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    quality_status TEXT NOT NULL
        CHECK(quality_status IN ('normalized', 'degraded')),
    created_at TEXT NOT NULL CHECK(length(created_at) > 0),
    UNIQUE (
        source_revision_id, contract_version,
        normalizer_name, normalizer_version
    )
);

CREATE INDEX IF NOT EXISTS standard_event_materializations_event_idx
    ON standard_event_materializations(event_id, id);
CREATE INDEX IF NOT EXISTS standard_event_materializations_subject_idx
    ON standard_event_materializations(subject_id, id);
"""


# Phase 4 operational metadata is additive. Frozen cursor/event/revision
# tables remain the only ingestion truth; this table records whether a
# physical source is currently discoverable and whether a new source has an
# explicit initialization decision.
SOURCE_CATALOG_SCHEMA = """
CREATE TABLE IF NOT EXISTS source_catalog (
    account_id TEXT NOT NULL,
    source_generation TEXT NOT NULL,
    shard_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    lifecycle TEXT NOT NULL CHECK(lifecycle IN (
        'pending_initialization', 'available', 'unavailable',
        'replacement_suspected'
    )),
    initialization_policy TEXT CHECK(
        initialization_policy IS NULL OR initialization_policy IN (
            'beginning', 'latest', 'operator-hold'
        )
    ),
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    missing_since TEXT,
    suspicion_reason TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL,
    PRIMARY KEY (account_id, source_generation, shard_name, table_name),
    CHECK (
        lifecycle != 'pending_initialization'
        OR initialization_policy IS NULL
        OR initialization_policy = 'operator-hold'
    ),
    CHECK (
        lifecycle != 'replacement_suspected' OR length(suspicion_reason) > 0
    )
);

CREATE INDEX IF NOT EXISTS source_catalog_lifecycle_idx
    ON source_catalog(account_id, source_generation, lifecycle, shard_name, table_name);

-- A v1/v2/v3 durable cursor is sufficient continuity evidence for I1. It is
-- deliberately preserved and remains runnable; migration never reseeds it.
INSERT OR IGNORE INTO source_catalog (
    account_id, source_generation, shard_name, table_name, lifecycle,
    initialization_policy, first_seen_at, last_seen_at, missing_since,
    suspicion_reason, updated_at
)
SELECT account_id, source_generation, shard_name, table_name, 'available',
       initial_position, initialized_at, updated_at, NULL, '', updated_at
FROM cursors;
"""


SCAN_COORDINATOR_SCHEMA = """
CREATE TABLE IF NOT EXISTS physical_scan_tasks (
    account_id TEXT NOT NULL,
    source_generation TEXT NOT NULL,
    shard_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN (
        'idle', 'pending', 'processing', 'blocked'
    )),
    request_version INTEGER NOT NULL CHECK(request_version > 0),
    completed_version INTEGER NOT NULL DEFAULT 0
        CHECK(completed_version >= 0 AND completed_version <= request_version),
    claim_version INTEGER NOT NULL DEFAULT 0 CHECK(claim_version >= 0),
    target_local_id INTEGER NOT NULL DEFAULT 0 CHECK(target_local_id >= 0),
    completed_target_local_id INTEGER NOT NULL DEFAULT 0
        CHECK(completed_target_local_id >= 0
              AND completed_target_local_id <= target_local_id),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
    available_at TEXT NOT NULL,
    lease_owner TEXT,
    lease_epoch INTEGER NOT NULL DEFAULT 0 CHECK(lease_epoch >= 0),
    lease_expires_at TEXT,
    last_error TEXT NOT NULL DEFAULT '',
    last_observed_max INTEGER CHECK(last_observed_max IS NULL OR last_observed_max >= 0),
    last_cursor INTEGER CHECK(last_cursor IS NULL OR last_cursor >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (account_id, source_generation, shard_name, table_name),
    FOREIGN KEY (account_id, source_generation, shard_name, table_name)
        REFERENCES source_catalog (
            account_id, source_generation, shard_name, table_name
        ) ON DELETE RESTRICT,
    CHECK (
        (status='processing' AND lease_owner IS NOT NULL
         AND lease_expires_at IS NOT NULL AND claim_version > completed_version)
        OR
        (status!='processing' AND lease_owner IS NULL AND lease_expires_at IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS physical_scan_tasks_ready_idx
    ON physical_scan_tasks(status, available_at, updated_at);
"""


RECONCILIATION_SCHEMA = """
CREATE TABLE IF NOT EXISTS source_reconciliation_state (
    account_id TEXT NOT NULL,
    source_generation TEXT NOT NULL,
    shard_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    next_due_at TEXT NOT NULL,
    last_successful_reconciliation TEXT,
    successful_reconciliation_count INTEGER NOT NULL DEFAULT 0
        CHECK(successful_reconciliation_count >= 0),
    last_observed_max INTEGER
        CHECK(last_observed_max IS NULL OR last_observed_max >= 0),
    last_cursor INTEGER CHECK(last_cursor IS NULL OR last_cursor >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (account_id, source_generation, shard_name, table_name),
    FOREIGN KEY (account_id, source_generation, shard_name, table_name)
        REFERENCES source_catalog (
            account_id, source_generation, shard_name, table_name
        ) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS source_reconciliation_due_idx
    ON source_reconciliation_state(next_due_at, account_id, source_generation,
                                   shard_name, table_name);
"""


# Window ingestion deliberately stores no source identifiers, content, or
# arbitrary operator text. The opaque run id is sufficient to retrieve the
# bounded request/coverage/outcome facts without exposing source keys.
WINDOW_INGEST_SCHEMA = """
CREATE TABLE IF NOT EXISTS window_ingest_runs (
    run_id TEXT PRIMARY KEY CHECK(
        length(run_id) = 32 AND run_id NOT GLOB '*[^0-9a-f]*'
    ),
    requested_start TEXT NOT NULL CHECK(length(requested_start) > 0),
    requested_end TEXT NOT NULL CHECK(length(requested_end) > 0),
    source_min_time INTEGER,
    source_max_time INTEGER,
    examined_count INTEGER NOT NULL CHECK(examined_count >= 0),
    created_at TEXT NOT NULL CHECK(length(created_at) > 0),
    CHECK(source_min_time IS NULL OR source_max_time IS NULL
          OR source_min_time <= source_max_time)
);

CREATE TABLE IF NOT EXISTS window_ingest_outcomes (
    run_id TEXT PRIMARY KEY REFERENCES window_ingest_runs(run_id)
        ON DELETE CASCADE,
    received_messages INTEGER NOT NULL CHECK(received_messages >= 0),
    inserted_events INTEGER NOT NULL CHECK(inserted_events >= 0),
    inserted_revisions INTEGER NOT NULL CHECK(inserted_revisions >= 0),
    parse_errors INTEGER NOT NULL CHECK(parse_errors >= 0),
    completed_at TEXT NOT NULL CHECK(length(completed_at) > 0)
);
"""

# Current private data-coverage state, separate from anonymous ingestion audit.
EMPTY_COVERAGE_SCHEMA = """
CREATE TABLE IF NOT EXISTS current_empty_window_coverage (
    account_id TEXT NOT NULL, source_generation TEXT NOT NULL,
    shard_name TEXT NOT NULL, table_name TEXT NOT NULL,
    window_start INTEGER NOT NULL, window_end INTEGER NOT NULL,
    source_stamps TEXT NOT NULL, path_binding TEXT NOT NULL,
    predicate_version INTEGER NOT NULL, no_future INTEGER NOT NULL CHECK(no_future IN (0,1)),
    PRIMARY KEY (account_id,source_generation,shard_name,table_name)
);
CREATE TABLE IF NOT EXISTS current_window_baseline (
    account_id TEXT NOT NULL, source_generation TEXT NOT NULL,
    predicate_version INTEGER NOT NULL, completed INTEGER NOT NULL DEFAULT 1 CHECK(completed IN (0,1)),
    PRIMARY KEY (account_id,source_generation)
);
"""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _typed_bytes(value: Any) -> bytes:
    if value is None:
        payload = b""
        kind = b"N"
    elif isinstance(value, bytes):
        payload = value
        kind = b"B"
    elif isinstance(value, str):
        payload = value.encode("utf-8", errors="surrogatepass")
        kind = b"S"
    else:
        payload = str(value).encode("ascii", errors="strict")
        kind = b"I"
    return kind + len(payload).to_bytes(8, "big") + payload


def _content_fingerprint(message: RawMessage) -> str:
    digest = hashlib.sha256()
    for value in (
        message.server_id,
        message.local_type,
        message.create_time,
        message.real_sender_id,
        message.raw_content,
        message.compression_type,
    ):
        digest.update(_typed_bytes(value))
    return digest.hexdigest()


def _event_uid(source: SourceKey, local_id: int) -> str:
    digest = hashlib.sha256()
    for value in (
        source.account_id,
        source.source_generation,
        source.shard_name,
        source.table_name,
        local_id,
    ):
        digest.update(_typed_bytes(value))
    return digest.hexdigest()


def _raw_content_parts(value: str | bytes | None) -> tuple[str, str | bytes | None]:
    if value is None:
        return "null", None
    if isinstance(value, bytes):
        return "blob", value
    return "text", value


def _validate_trimmed_text(value: str, field_name: str, max_length: int) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be text")
    if not value or value.strip() != value or len(value) > max_length:
        raise ValueError(
            f"{field_name} must be a non-empty trimmed string of at most "
            f"{max_length} characters"
        )
    if any(ord(character) < 32 for character in value):
        raise ValueError(f"{field_name} must not contain control characters")
    return value


def _validate_utc_timestamp(value: str, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be text")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a canonical UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must be a canonical UTC timestamp")
    canonical = parsed.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if value != canonical:
        raise ValueError(f"{field_name} must be a canonical UTC timestamp")
    return value


def _consumer_error_type(error: BaseException | str) -> str:
    value = type(error).__name__ if isinstance(error, BaseException) else error
    if not isinstance(value, str) or re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,99}", value) is None:
        raise ValueError("Only a short controlled consumer error type may be persisted")
    return value


def _reject_non_json_constant(_value: str) -> None:
    raise ValueError("Non-standard JSON constants are not allowed")


def _validate_materialization_input(
    materialization: StandardEventMaterializationInput,
) -> None:
    if not isinstance(materialization, StandardEventMaterializationInput):
        raise TypeError("materialization must be StandardEventMaterializationInput")
    if materialization.source_revision_id <= 0:
        raise ValueError("source_revision_id must be positive")
    _validate_trimmed_text(materialization.event_id, "event_id", 200)
    _validate_trimmed_text(materialization.subject_id, "subject_id", 200)
    _validate_trimmed_text(materialization.contract_version, "contract_version", 100)
    _validate_trimmed_text(materialization.normalizer_name, "normalizer_name", 100)
    _validate_trimmed_text(
        materialization.normalizer_version, "normalizer_version", 100
    )
    if not isinstance(materialization.canonical_payload_json, str):
        raise TypeError("canonical_payload_json must be text")
    try:
        json.loads(
            materialization.canonical_payload_json,
            parse_constant=_reject_non_json_constant,
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("canonical_payload_json must contain valid JSON") from exc
    if re.fullmatch(r"[0-9a-f]{64}", materialization.payload_sha256) is None:
        raise ValueError("payload_sha256 must be a lowercase SHA-256 hex digest")
    actual_hash = hashlib.sha256(
        materialization.canonical_payload_json.encode("utf-8")
    ).hexdigest()
    if actual_hash != materialization.payload_sha256:
        raise ValueError("payload_sha256 does not match canonical payload bytes")
    if materialization.quality_status not in QUALITY_STATUSES:
        raise ValueError("quality_status must be 'normalized' or 'degraded'")


def _consumer_state_from_row(row: sqlite3.Row) -> OutboxConsumerState:
    return OutboxConsumerState(
        id=int(row["id"]),
        outbox_id=int(row["outbox_id"]),
        consumer_name=str(row["consumer_name"]),
        contract_version=str(row["contract_version"]),
        normalizer_version=str(row["normalizer_version"]),
        status=str(row["status"]),
        attempt_count=int(row["attempt_count"]),
        available_at=str(row["available_at"]),
        lease_owner=str(row["lease_owner"]) if row["lease_owner"] is not None else None,
        lease_expires_at=(
            str(row["lease_expires_at"])
            if row["lease_expires_at"] is not None
            else None
        ),
        last_error_type=str(row["last_error_type"]),
        completed_at=(
            str(row["completed_at"]) if row["completed_at"] is not None else None
        ),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def _materialization_from_row(row: sqlite3.Row) -> StandardEventMaterialization:
    return StandardEventMaterialization(
        id=int(row["id"]),
        source_revision_id=int(row["source_revision_id"]),
        event_id=str(row["event_id"]),
        subject_id=str(row["subject_id"]),
        contract_version=str(row["contract_version"]),
        normalizer_name=str(row["normalizer_name"]),
        normalizer_version=str(row["normalizer_version"]),
        canonical_payload_json=str(row["canonical_payload_json"]),
        payload_sha256=str(row["payload_sha256"]),
        quality_status=str(row["quality_status"]),
        created_at=str(row["created_at"]),
    )


class EventStore:
    """Writable private SQLite state kept strictly separate from WeChat DBs."""

    def __init__(self, path: Path):
        if path.is_symlink():
            raise EventStoreError("Event store path must not be a symbolic link")
        self.path = path.resolve()
        ensure_private_parent(self.path)
        self._conn = sqlite3.connect(self.path, timeout=5.0)
        self._conn.row_factory = sqlite3.Row
        try:
            self._conn.execute("PRAGMA foreign_keys = ON")
            self._conn.execute("PRAGMA busy_timeout = 5000")
            current_version = int(self._conn.execute("PRAGMA user_version").fetchone()[0])
            application_id = int(
                self._conn.execute("PRAGMA application_id").fetchone()[0]
            )
            if current_version not in (0, 1, 2, 3, 4, 5, 6, SCHEMA_VERSION):
                raise EventStoreError(
                    f"Unsupported event store schema version: {current_version}"
                )
            if current_version == 0:
                if application_id != 0:
                    raise EventStoreError(
                        "Refusing to initialize a database with another application id"
                    )
                existing_tables = self._conn.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name NOT LIKE 'sqlite_%'
                    """
                ).fetchall()
                if existing_tables:
                    raise EventStoreError(
                        "Refusing to initialize a non-empty database as an event store"
                    )
            elif application_id != APPLICATION_ID:
                raise EventStoreError("Database is not a WeChat Live Reader event store")

            self._conn.execute("PRAGMA synchronous = FULL")
            self._conn.execute("PRAGMA journal_mode = WAL")
            if current_version == 0:
                self._conn.executescript(
                    "BEGIN IMMEDIATE;\n"
                    + CORE_SCHEMA
                    + SESSION_SCHEMA
                    + NORMALIZATION_SCHEMA
                    + SOURCE_CATALOG_SCHEMA
                    + SCAN_COORDINATOR_SCHEMA
                    + RECONCILIATION_SCHEMA
                    + WINDOW_INGEST_SCHEMA
                    + f"\nPRAGMA application_id = {APPLICATION_ID};"
                    + f"\nPRAGMA user_version = {SCHEMA_VERSION};\nCOMMIT;"
                )
            elif current_version == 1:
                self._conn.executescript(
                    "BEGIN IMMEDIATE;\n"
                    + SESSION_SCHEMA
                    + NORMALIZATION_SCHEMA
                    + SOURCE_CATALOG_SCHEMA
                    + SCAN_COORDINATOR_SCHEMA
                    + RECONCILIATION_SCHEMA
                    + WINDOW_INGEST_SCHEMA
                    + f"\nPRAGMA user_version = {SCHEMA_VERSION};\nCOMMIT;"
                )
            elif current_version == 2:
                self._conn.executescript(
                    "BEGIN IMMEDIATE;\n"
                    + NORMALIZATION_SCHEMA
                    + SOURCE_CATALOG_SCHEMA
                    + SCAN_COORDINATOR_SCHEMA
                    + RECONCILIATION_SCHEMA
                    + WINDOW_INGEST_SCHEMA
                    + f"\nPRAGMA user_version = {SCHEMA_VERSION};\nCOMMIT;"
                )
            elif current_version == 3:
                self._conn.executescript(
                    "BEGIN IMMEDIATE;\n"
                    + SOURCE_CATALOG_SCHEMA
                    + SCAN_COORDINATOR_SCHEMA
                    + RECONCILIATION_SCHEMA
                    + WINDOW_INGEST_SCHEMA
                    + f"\nPRAGMA user_version = {SCHEMA_VERSION};\nCOMMIT;"
                )
            elif current_version == 4:
                self._conn.executescript(
                    "BEGIN IMMEDIATE;\n"
                    + SCAN_COORDINATOR_SCHEMA
                    + RECONCILIATION_SCHEMA
                    + WINDOW_INGEST_SCHEMA
                    + f"\nPRAGMA user_version = {SCHEMA_VERSION};\nCOMMIT;"
                )
            elif current_version == 5:
                self._conn.executescript(
                    "BEGIN IMMEDIATE;\n"
                    + RECONCILIATION_SCHEMA
                    + WINDOW_INGEST_SCHEMA
                    + f"\nPRAGMA user_version = {SCHEMA_VERSION};\nCOMMIT;"
                )
            elif current_version == 6:
                self._conn.executescript(
                    "BEGIN IMMEDIATE;\n"
                    + WINDOW_INGEST_SCHEMA
                    + f"\nPRAGMA user_version = {SCHEMA_VERSION};\nCOMMIT;"
                )
            else:
                self._conn.executescript(
                    CORE_SCHEMA + SESSION_SCHEMA + NORMALIZATION_SCHEMA
                    + SOURCE_CATALOG_SCHEMA
                    + SCAN_COORDINATOR_SCHEMA
                    + RECONCILIATION_SCHEMA
                    + WINDOW_INGEST_SCHEMA
                )
                self._conn.commit()
            self._conn.executescript(EMPTY_COVERAGE_SCHEMA)
            columns = {row[1] for row in self._conn.execute('PRAGMA table_info(current_window_baseline)')}
            if 'completed' not in columns:
                # A 0fd marker was written only after completion.
                self._conn.execute('ALTER TABLE current_window_baseline ADD COLUMN completed INTEGER NOT NULL DEFAULT 1 CHECK(completed IN (0,1))')
                self._conn.commit()
            os.chmod(self.path, 0o600)
        except Exception:
            self._conn.close()
            raise

    def __enter__(self) -> "EventStore":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def close(self) -> None:
        self._conn.close()

    @staticmethod
    def _source_values(source: SourceKey) -> tuple[str, str, str, str]:
        return (
            source.account_id,
            source.source_generation,
            source.shard_name,
            source.table_name,
        )

    def get_cursor(self, source: SourceKey) -> int | None:
        row = self._conn.execute(
            """
            SELECT last_local_id FROM cursors
            WHERE account_id=? AND source_generation=?
              AND shard_name=? AND table_name=?
            """,
            self._source_values(source),
        ).fetchone()
        return int(row[0]) if row else None

    def initialize_cursor(
        self, source: SourceKey, last_local_id: int, initial_position: str
    ) -> int:
        if initial_position not in INITIAL_POSITIONS:
            raise ValueError("initial_position must be 'beginning' or 'latest'")
        if last_local_id < 0:
            raise ValueError("last_local_id must not be negative")
        now = _utc_now()
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            catalog_row = self._conn.execute(
                """
                SELECT lifecycle FROM source_catalog
                WHERE account_id=? AND source_generation=?
                  AND shard_name=? AND table_name=?
                """,
                self._source_values(source),
            ).fetchone()
            if catalog_row and catalog_row[0] == "replacement_suspected":
                raise EventStoreError(
                    "Replacement-suspected source cannot initialize a cursor"
                )
            self._conn.execute(
                """
                INSERT OR IGNORE INTO cursors (
                    account_id, source_generation, shard_name, table_name,
                    last_local_id, initial_position, initialized_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (*self._source_values(source), last_local_id, initial_position, now, now),
            )
            self._conn.execute(
                """
                INSERT INTO source_catalog (
                    account_id, source_generation, shard_name, table_name,
                    lifecycle, initialization_policy, first_seen_at,
                    last_seen_at, missing_since, suspicion_reason, updated_at
                ) VALUES (?, ?, ?, ?, 'available', ?, ?, ?, NULL, '', ?)
                ON CONFLICT(account_id, source_generation, shard_name, table_name)
                DO UPDATE SET
                    lifecycle=CASE
                        WHEN source_catalog.lifecycle='replacement_suspected'
                        THEN source_catalog.lifecycle ELSE 'available' END,
                    initialization_policy=COALESCE(
                        CASE WHEN source_catalog.initialization_policy='operator-hold'
                             THEN NULL ELSE source_catalog.initialization_policy END,
                        excluded.initialization_policy
                    ),
                    missing_since=NULL,
                    updated_at=excluded.updated_at
                """,
                (*self._source_values(source), initial_position, now, now, now),
            )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        cursor = self.get_cursor(source)
        if cursor is None:
            raise EventStoreError("Cursor initialization did not persist")
        return cursor

    def record_catalog_refresh(
        self,
        account_id: str,
        source_generation: str,
        discovered: Iterable[tuple[str, str]],
    ) -> None:
        """Atomically record one complete physical-source enumeration.

        Newly discovered sources deliberately receive no beginning/latest
        choice. Existing durable cursors remain available. Sources absent from
        this complete snapshot become unavailable without touching cursors.
        """

        sources = [
            SourceKey(account_id, source_generation, shard_name, table_name)
            for shard_name, table_name in discovered
        ]
        identities = {self._source_values(source) for source in sources}
        if len(identities) != len(sources):
            raise EventStoreError("Catalog refresh contains duplicate physical sources")
        now = _utc_now()
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            for source in sources:
                values = self._source_values(source)
                cursor = self._conn.execute(
                    """
                    SELECT initial_position FROM cursors
                    WHERE account_id=? AND source_generation=?
                      AND shard_name=? AND table_name=?
                    """,
                    values,
                ).fetchone()
                lifecycle = "available" if cursor else "pending_initialization"
                policy = str(cursor[0]) if cursor else None
                self._conn.execute(
                    """
                    INSERT INTO source_catalog (
                        account_id, source_generation, shard_name, table_name,
                        lifecycle, initialization_policy, first_seen_at,
                        last_seen_at, missing_since, suspicion_reason, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, '', ?)
                    ON CONFLICT(account_id, source_generation, shard_name, table_name)
                    DO UPDATE SET
                        lifecycle=CASE
                            WHEN source_catalog.lifecycle='replacement_suspected'
                            THEN source_catalog.lifecycle
                            WHEN ?='available' THEN 'available'
                            WHEN source_catalog.initialization_policy='operator-hold'
                            THEN 'pending_initialization'
                            ELSE 'pending_initialization' END,
                        initialization_policy=COALESCE(
                            source_catalog.initialization_policy,
                            excluded.initialization_policy
                        ),
                        last_seen_at=excluded.last_seen_at,
                        missing_since=NULL,
                        updated_at=excluded.updated_at
                    """,
                    (*values, lifecycle, policy, now, now, now, lifecycle),
                )

            rows = self._conn.execute(
                """
                SELECT shard_name, table_name FROM source_catalog
                WHERE account_id=? AND source_generation=?
                  AND lifecycle!='replacement_suspected'
                """,
                (account_id, source_generation),
            ).fetchall()
            for row in rows:
                values = (account_id, source_generation, str(row[0]), str(row[1]))
                if values not in identities:
                    self._conn.execute(
                        """
                        UPDATE source_catalog
                        SET lifecycle='unavailable',
                            missing_since=COALESCE(missing_since, ?), updated_at=?
                        WHERE account_id=? AND source_generation=?
                          AND shard_name=? AND table_name=?
                        """,
                        (now, now, *values),
                    )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def source_catalog_entry(self, source: SourceKey) -> sqlite3.Row | None:
        return self._conn.execute(
            """
            SELECT * FROM source_catalog
            WHERE account_id=? AND source_generation=?
              AND shard_name=? AND table_name=?
            """,
            self._source_values(source),
        ).fetchone()

    def source_catalog_rows_for_table(
        self, account_id: str, source_generation: str, table_name: str
    ) -> list[sqlite3.Row]:
        return self._conn.execute(
            """
            SELECT * FROM source_catalog
            WHERE account_id=? AND source_generation=? AND table_name=?
            ORDER BY shard_name
            """,
            (account_id, source_generation, table_name),
        ).fetchall()

    def source_catalog_sources(self, lifecycle: str | None = None) -> list[SourceKey]:
        if lifecycle is None:
            rows = self._conn.execute(
                """
                SELECT account_id, source_generation, shard_name, table_name
                FROM source_catalog
                ORDER BY account_id, source_generation, shard_name, table_name
                """
            ).fetchall()
        else:
            rows = self._conn.execute(
                """
                SELECT account_id, source_generation, shard_name, table_name
                FROM source_catalog WHERE lifecycle=?
                ORDER BY account_id, source_generation, shard_name, table_name
                """,
                (lifecycle,),
            ).fetchall()
        return [SourceKey(*map(str, row)) for row in rows]

    def eligible_reconciliation_sources(self) -> list[SourceKey]:
        rows = self._conn.execute(
            """
            SELECT catalog.account_id, catalog.source_generation,
                   catalog.shard_name, catalog.table_name
            FROM source_catalog AS catalog
            JOIN cursors AS cursor
              USING (account_id, source_generation, shard_name, table_name)
            WHERE catalog.lifecycle='available'
            ORDER BY catalog.account_id, catalog.source_generation,
                     catalog.shard_name, catalog.table_name
            """
        ).fetchall()
        return [SourceKey(*map(str, row)) for row in rows]

    def ensure_reconciliation_schedules(
        self, entries: Iterable[tuple[SourceKey, str]], *, now: str
    ) -> int:
        values = [(*self._source_values(source), due_at, now, now) for source, due_at in entries]
        if not values:
            return 0
        before = self._conn.total_changes
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.executemany(
                """
                INSERT OR IGNORE INTO source_reconciliation_state (
                    account_id, source_generation, shard_name, table_name,
                    next_due_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )
            self._conn.commit()
            return self._conn.total_changes - before
        except Exception:
            self._conn.rollback()
            raise

    def due_reconciliation_sources(self, *, now: str, limit: int) -> list[SourceKey]:
        if limit < 1:
            return []
        rows = self._conn.execute(
            """
            SELECT state.account_id, state.source_generation,
                   state.shard_name, state.table_name
            FROM source_reconciliation_state AS state
            JOIN source_catalog AS catalog
              USING (account_id, source_generation, shard_name, table_name)
            JOIN cursors AS cursor
              USING (account_id, source_generation, shard_name, table_name)
            LEFT JOIN physical_scan_tasks AS task
              USING (account_id, source_generation, shard_name, table_name)
            WHERE state.next_due_at <= ? AND catalog.lifecycle='available'
              AND (task.status IS NULL OR task.status NOT IN ('pending', 'processing'))
            ORDER BY state.next_due_at, state.account_id, state.source_generation,
                     state.shard_name, state.table_name
            LIMIT ?
            """,
            (now, limit),
        ).fetchall()
        return [SourceKey(*map(str, row)) for row in rows]

    def active_physical_scan_count(self) -> int:
        row = self._conn.execute(
            "SELECT count(*) FROM physical_scan_tasks "
            "WHERE status IN ('pending', 'processing')"
        ).fetchone()
        return int(row[0])

    def reconciliation_state(self, source: SourceKey) -> sqlite3.Row | None:
        return self._conn.execute(
            """
            SELECT * FROM source_reconciliation_state
            WHERE account_id=? AND source_generation=?
              AND shard_name=? AND table_name=?
            """,
            self._source_values(source),
        ).fetchone()

    def reconciliation_health_snapshot(self, *, now: str) -> sqlite3.Row:
        return self._conn.execute(
            """
            SELECT
              (SELECT count(*) FROM source_catalog AS c
               JOIN cursors AS u USING (
                   account_id, source_generation, shard_name, table_name)
               WHERE c.lifecycle='available') AS eligible_count,
              (SELECT count(*) FROM source_catalog
               WHERE lifecycle!='available') AS blocked_unavailable_count,
              (SELECT count(*) FROM source_reconciliation_state AS s
               JOIN source_catalog AS c USING (
                   account_id, source_generation, shard_name, table_name)
               JOIN cursors AS u USING (
                   account_id, source_generation, shard_name, table_name)
               WHERE c.lifecycle='available' AND s.next_due_at <= ?) AS overdue_count,
              (SELECT min(s.next_due_at) FROM source_reconciliation_state AS s
               JOIN source_catalog AS c USING (
                   account_id, source_generation, shard_name, table_name)
               JOIN cursors AS u USING (
                   account_id, source_generation, shard_name, table_name)
               WHERE c.lifecycle='available' AND s.next_due_at <= ?) AS oldest_due_at,
              (SELECT count(*) FROM source_reconciliation_state AS s
               JOIN physical_scan_tasks AS t USING (
                   account_id, source_generation, shard_name, table_name)
               WHERE s.next_due_at <= ?
                 AND t.status IN ('pending', 'processing')) AS pending_count,
              (SELECT coalesce(sum(successful_reconciliation_count), 0)
               FROM source_reconciliation_state) AS successful_count
            """,
            (now, now, now),
        ).fetchone()

    def source_reliability_health_snapshot(self) -> sqlite3.Row:
        """Return privacy-safe aggregate lifecycle and physical-task counts."""

        return self._conn.execute(
            """
            SELECT
              (SELECT count(*) FROM source_catalog
               WHERE missing_since IS NULL) AS discovered_count,
              (SELECT count(*) FROM source_catalog AS c
               JOIN cursors AS u USING (
                   account_id, source_generation, shard_name, table_name)
               WHERE c.lifecycle='available') AS eligible_count,
              (SELECT count(*) FROM source_catalog
               WHERE lifecycle='pending_initialization') AS pending_initialization_count,
              (SELECT count(*) FROM source_catalog
               WHERE lifecycle='unavailable') AS unavailable_count,
              (SELECT count(*) FROM source_catalog
               WHERE lifecycle='replacement_suspected') AS replacement_suspected_count,
              (SELECT count(*) FROM physical_scan_tasks
               WHERE status='pending') AS pending_physical_count,
              (SELECT count(*) FROM physical_scan_tasks
               WHERE status='processing') AS active_lease_count
            """
        ).fetchone()

    def source_is_runnable(self, source: SourceKey) -> bool:
        row = self.source_catalog_entry(source)
        return bool(
            row
            and row["lifecycle"] == "available"
            and self.get_cursor(source) is not None
        )

    def initialize_catalog_source(
        self,
        source: SourceKey,
        policy: str,
        *,
        observed_max_local_id: int | None = None,
    ) -> int | None:
        """Apply an explicit first-run decision to a discovered source."""

        if policy not in SOURCE_INITIALIZATION_POLICIES:
            raise ValueError("policy must be 'beginning', 'latest', or 'operator-hold'")
        row = self.source_catalog_entry(source)
        if row is None:
            raise EventStoreError("Source must be discovered before initialization")
        if row["lifecycle"] == "replacement_suspected":
            raise EventStoreError("Replacement-suspected source cannot be initialized")
        existing = self.get_cursor(source)
        if existing is not None:
            return existing
        if policy == "operator-hold":
            now = _utc_now()
            self._conn.execute(
                """
                UPDATE source_catalog
                SET lifecycle='pending_initialization',
                    initialization_policy='operator-hold', updated_at=?
                WHERE account_id=? AND source_generation=?
                  AND shard_name=? AND table_name=?
                """,
                (now, *self._source_values(source)),
            )
            self._conn.commit()
            return None
        if policy == "latest" and observed_max_local_id is None:
            raise ValueError("latest initialization requires observed_max_local_id")
        if observed_max_local_id is not None and observed_max_local_id < 0:
            raise ValueError("observed_max_local_id must not be negative")
        seed = int(observed_max_local_id or 0) if policy == "latest" else 0
        return self.initialize_cursor(source, seed, policy)

    def mark_replacement_suspected(
        self,
        source: SourceKey,
        reason: str,
        *,
        scan_fence: ScanLeaseFence | None = None,
    ) -> None:
        if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,99}", reason) is None:
            raise ValueError("reason must be a short controlled category")
        now = _utc_now()
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            if scan_fence is not None:
                if scan_fence.source != source:
                    raise EventStoreError("Physical scan fence SourceKey mismatch")
                self._validate_scan_fence(scan_fence, now)
            cursor = self._conn.execute(
                """
                UPDATE source_catalog
                SET lifecycle='replacement_suspected', suspicion_reason=?, updated_at=?
                WHERE account_id=? AND source_generation=?
                  AND shard_name=? AND table_name=?
                """,
                (reason, now, *self._source_values(source)),
            )
            if cursor.rowcount != 1:
                raise EventStoreError("Cannot suspect an undiscovered source")
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def request_physical_scan(
        self,
        source: SourceKey,
        *,
        target_local_id: int = 0,
        force: bool = False,
    ) -> int:
        """Coalesce one durable scan need into the SourceKey-owned row."""

        if target_local_id < 0:
            raise ValueError("target_local_id must not be negative")
        now = _utc_now()
        values = self._source_values(source)
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            catalog = self._conn.execute(
                """
                SELECT lifecycle, initialization_policy FROM source_catalog
                WHERE account_id=? AND source_generation=?
                  AND shard_name=? AND table_name=?
                """,
                values,
            ).fetchone()
            if catalog is None:
                raise EventStoreError("Physical scan source is not catalogued")
            cursor_exists = self._conn.execute(
                """
                SELECT 1 FROM cursors
                WHERE account_id=? AND source_generation=?
                  AND shard_name=? AND table_name=?
                """,
                values,
            ).fetchone() is not None
            runnable = str(catalog["lifecycle"]) == "available" and cursor_exists
            blocked_reason = ""
            if not runnable:
                lifecycle = str(catalog["lifecycle"])
                if lifecycle == "replacement_suspected":
                    blocked_reason = "ReplacementSuspected"
                elif lifecycle == "pending_initialization":
                    blocked_reason = (
                        "OperatorHold"
                        if catalog["initialization_policy"] == "operator-hold"
                        else "PendingInitialization"
                    )
                else:
                    blocked_reason = "SourceUnavailable"
            row = self._conn.execute(
                """
                SELECT * FROM physical_scan_tasks
                WHERE account_id=? AND source_generation=?
                  AND shard_name=? AND table_name=?
                """,
                values,
            ).fetchone()
            if row is None:
                self._conn.execute(
                    """
                    INSERT INTO physical_scan_tasks (
                        account_id, source_generation, shard_name, table_name,
                        status, request_version, completed_version, claim_version,
                        target_local_id, completed_target_local_id, attempt_count,
                        available_at, lease_owner, lease_epoch, lease_expires_at,
                        last_error, last_observed_max, last_cursor,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 1, 0, 0, ?, 0, 0, ?, NULL, 0,
                              NULL, ?, NULL, NULL, ?, ?)
                    """,
                    (
                        *values,
                        "pending" if runnable else "blocked",
                        target_local_id,
                        now,
                        blocked_reason,
                        now,
                        now,
                    ),
                )
                request_version = 1
            else:
                completed_covers = (
                    int(row["completed_version"]) == int(row["request_version"])
                    and target_local_id <= int(row["completed_target_local_id"])
                    and row["last_observed_max"] is not None
                    and row["last_cursor"] is not None
                    and int(row["last_observed_max"]) >= target_local_id
                    and int(row["last_cursor"]) >= target_local_id
                )
                outstanding_covers = (
                    str(row["status"]) in ("pending", "processing")
                    and target_local_id <= int(row["target_local_id"])
                )
                needs_request = force or not (completed_covers or outstanding_covers)
                request_version = int(row["request_version"]) + int(needs_request)
                target = max(int(row["target_local_id"]), target_local_id)
                status = str(row["status"])
                if status != "processing":
                    if needs_request or status == "blocked":
                        status = "pending" if runnable else "blocked"
                self._conn.execute(
                    """
                    UPDATE physical_scan_tasks
                    SET status=?, request_version=?, target_local_id=?,
                        available_at=CASE WHEN ?='pending' THEN ? ELSE available_at END,
                        last_error=CASE WHEN ?='blocked' THEN ? ELSE '' END,
                        updated_at=?
                    WHERE account_id=? AND source_generation=?
                      AND shard_name=? AND table_name=?
                    """,
                    (
                        status,
                        request_version,
                        target,
                        status,
                        now,
                        status,
                        blocked_reason,
                        now,
                        *values,
                    ),
                )
            self._conn.commit()
            return request_version
        except Exception:
            self._conn.rollback()
            raise

    def claim_physical_scan(
        self,
        lease_owner: str,
        *,
        lease_seconds: float = 120.0,
    ) -> PhysicalScanTask | None:
        if not lease_owner or lease_owner.strip() != lease_owner:
            raise ValueError("lease_owner must be a non-empty trimmed string")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat(timespec="microseconds")
        expires = (now_dt + timedelta(seconds=lease_seconds)).isoformat(
            timespec="microseconds"
        )
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.execute(
                """
                UPDATE physical_scan_tasks
                SET status='pending', lease_owner=NULL, lease_expires_at=NULL,
                    available_at=?, last_error='LeaseExpired', updated_at=?
                WHERE status='processing' AND lease_expires_at <= ?
                """,
                (now, now, now),
            )
            row = self._conn.execute(
                """
                SELECT * FROM physical_scan_tasks
                WHERE status='pending' AND available_at <= ?
                ORDER BY available_at, updated_at, account_id,
                         source_generation, shard_name, table_name
                LIMIT 1
                """,
                (now,),
            ).fetchone()
            if row is None:
                self._conn.commit()
                return None
            values = (
                str(row["account_id"]),
                str(row["source_generation"]),
                str(row["shard_name"]),
                str(row["table_name"]),
            )
            epoch = int(row["lease_epoch"]) + 1
            claim_version = int(row["request_version"])
            updated = self._conn.execute(
                """
                UPDATE physical_scan_tasks
                SET status='processing', claim_version=?,
                    attempt_count=attempt_count+1, lease_owner=?, lease_epoch=?,
                    lease_expires_at=?, updated_at=?
                WHERE account_id=? AND source_generation=?
                  AND shard_name=? AND table_name=? AND status='pending'
                """,
                (claim_version, lease_owner, epoch, expires, now, *values),
            )
            if updated.rowcount != 1:
                raise EventStoreError("Physical scan claim was not exclusive")
            self._conn.commit()
            return PhysicalScanTask(
                source=SourceKey(*values),
                request_version=int(row["request_version"]),
                claim_version=claim_version,
                target_local_id=int(row["target_local_id"]),
                lease_owner=lease_owner,
                lease_epoch=epoch,
                lease_expires_at=expires,
            )
        except Exception:
            self._conn.rollback()
            raise

    def renew_physical_scan(
        self,
        task: PhysicalScanTask,
        *,
        lease_seconds: float = 120.0,
    ) -> PhysicalScanTask:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat(timespec="microseconds")
        expires = (now_dt + timedelta(seconds=lease_seconds)).isoformat(
            timespec="microseconds"
        )
        updated = self._conn.execute(
            """
            UPDATE physical_scan_tasks
            SET lease_expires_at=?, updated_at=?
            WHERE account_id=? AND source_generation=?
              AND shard_name=? AND table_name=? AND status='processing'
              AND lease_owner=? AND lease_epoch=? AND claim_version=?
              AND lease_expires_at > ?
            """,
            (
                expires,
                now,
                *self._source_values(task.source),
                task.lease_owner,
                task.lease_epoch,
                task.claim_version,
                now,
            ),
        )
        if updated.rowcount != 1:
            self._conn.rollback()
            raise EventStoreError("Cannot renew a stale physical scan lease")
        self._conn.commit()
        return PhysicalScanTask(
            source=task.source,
            request_version=task.request_version,
            claim_version=task.claim_version,
            target_local_id=task.target_local_id,
            lease_owner=task.lease_owner,
            lease_epoch=task.lease_epoch,
            lease_expires_at=expires,
        )

    def retry_physical_scan(
        self,
        task: PhysicalScanTask,
        error_type: str,
        *,
        delay_seconds: float = 0.0,
    ) -> None:
        if delay_seconds < 0:
            raise ValueError("delay_seconds must not be negative")
        error_type = self._validate_error_type(error_type)
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat(timespec="microseconds")
        available = (now_dt + timedelta(seconds=delay_seconds)).isoformat(
            timespec="microseconds"
        )
        updated = self._conn.execute(
            """
            UPDATE physical_scan_tasks
            SET status='pending', available_at=?, lease_owner=NULL,
                lease_expires_at=NULL, last_error=?, updated_at=?
            WHERE account_id=? AND source_generation=?
              AND shard_name=? AND table_name=? AND status='processing'
              AND lease_owner=? AND lease_epoch=? AND claim_version=?
              AND lease_expires_at > ?
            """,
            (
                available,
                error_type,
                now,
                *self._source_values(task.source),
                task.lease_owner,
                task.lease_epoch,
                task.claim_version,
                now,
            ),
        )
        if updated.rowcount != 1:
            self._conn.rollback()
            raise EventStoreError("Cannot retry a stale physical scan lease")
        self._conn.commit()

    def block_physical_scan(
        self, task: PhysicalScanTask, error_type: str
    ) -> None:
        error_type = self._validate_error_type(error_type)
        now = _utc_now()
        updated = self._conn.execute(
            """
            UPDATE physical_scan_tasks
            SET status='blocked', lease_owner=NULL, lease_expires_at=NULL,
                last_error=?, updated_at=?
            WHERE account_id=? AND source_generation=?
              AND shard_name=? AND table_name=? AND status='processing'
              AND lease_owner=? AND lease_epoch=? AND claim_version=?
              AND lease_expires_at > ?
            """,
            (
                error_type,
                now,
                *self._source_values(task.source),
                task.lease_owner,
                task.lease_epoch,
                task.claim_version,
                now,
            ),
        )
        if updated.rowcount != 1:
            self._conn.rollback()
            raise EventStoreError("Cannot block a stale physical scan lease")
        self._conn.commit()

    def complete_physical_scan(
        self,
        task: PhysicalScanTask,
        *,
        observed_max_local_id: int,
        cursor: int,
        reconciliation_coverage_seconds: float | None = None,
        reconciliation_observed_at: datetime | None = None,
    ) -> bool:
        if observed_max_local_id < 0 or cursor < 0:
            raise ValueError("Scan observations must not be negative")
        if (
            reconciliation_coverage_seconds is not None
            and reconciliation_coverage_seconds <= 0
        ):
            raise ValueError("reconciliation coverage period must be positive")
        now = _utc_now()
        coverage_now = reconciliation_observed_at or datetime.fromisoformat(now)
        if coverage_now.tzinfo is None:
            raise ValueError("reconciliation observation time must be timezone-aware")
        coverage_now = coverage_now.astimezone(timezone.utc)
        values = self._source_values(task.source)
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute(
                """
                SELECT task.request_version FROM physical_scan_tasks AS task
                JOIN source_catalog AS catalog
                  USING (account_id, source_generation, shard_name, table_name)
                JOIN cursors AS cursor
                  USING (account_id, source_generation, shard_name, table_name)
                WHERE task.account_id=? AND task.source_generation=?
                  AND task.shard_name=? AND task.table_name=?
                  AND task.status='processing'
                  AND task.lease_owner=? AND task.lease_epoch=?
                  AND task.claim_version=? AND task.lease_expires_at > ?
                  AND catalog.lifecycle='available'
                """,
                (
                    *values,
                    task.lease_owner,
                    task.lease_epoch,
                    task.claim_version,
                    now,
                ),
            ).fetchone()
            if row is None:
                raise EventStoreError("Cannot complete a stale physical scan lease")
            requeued = int(row["request_version"]) > task.claim_version
            completed_target = min(
                task.target_local_id, observed_max_local_id, cursor
            )
            self._conn.execute(
                """
                UPDATE physical_scan_tasks
                SET status=?, completed_version=?,
                    completed_target_local_id=CASE
                        WHEN completed_target_local_id < ? THEN ?
                        ELSE completed_target_local_id END,
                    available_at=?, lease_owner=NULL, lease_expires_at=NULL,
                    last_error='', last_observed_max=?, last_cursor=?, updated_at=?
                WHERE account_id=? AND source_generation=?
                  AND shard_name=? AND table_name=?
                """,
                (
                    "pending" if requeued else "idle",
                    task.claim_version,
                    completed_target,
                    completed_target,
                    now,
                    observed_max_local_id,
                    cursor,
                    now,
                    *values,
                ),
            )
            if reconciliation_coverage_seconds is not None:
                due = (
                    coverage_now
                    + timedelta(seconds=reconciliation_coverage_seconds)
                ).isoformat(timespec="microseconds")
                observed_at = coverage_now.isoformat(timespec="microseconds")
                self._conn.execute(
                    """
                    INSERT INTO source_reconciliation_state (
                        account_id, source_generation, shard_name, table_name,
                        next_due_at, last_successful_reconciliation,
                        successful_reconciliation_count, last_observed_max,
                        last_cursor, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
                    ON CONFLICT (
                        account_id, source_generation, shard_name, table_name
                    ) DO UPDATE SET
                        next_due_at=excluded.next_due_at,
                        last_successful_reconciliation=
                            excluded.last_successful_reconciliation,
                        successful_reconciliation_count=
                            successful_reconciliation_count + 1,
                        last_observed_max=excluded.last_observed_max,
                        last_cursor=excluded.last_cursor,
                        updated_at=excluded.updated_at
                    """,
                    (
                        *values,
                        due,
                        observed_at,
                        observed_max_local_id,
                        cursor,
                        observed_at,
                        now,
                    ),
                )
            self._conn.commit()
            return not requeued
        except Exception:
            self._conn.rollback()
            raise

    def physical_scan_status(self, source: SourceKey) -> sqlite3.Row | None:
        return self._conn.execute(
            """
            SELECT * FROM physical_scan_tasks
            WHERE account_id=? AND source_generation=?
              AND shard_name=? AND table_name=?
            """,
            self._source_values(source),
        ).fetchone()

    def physical_scan_request_satisfied(
        self, source: SourceKey, request_version: int
    ) -> bool:
        row = self.physical_scan_status(source)
        return bool(row and int(row["completed_version"]) >= request_version)

    def physical_scan_count(self, status: str | None = None) -> int:
        if status is None:
            row = self._conn.execute(
                "SELECT count(*) FROM physical_scan_tasks"
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT count(*) FROM physical_scan_tasks WHERE status=?",
                (status,),
            ).fetchone()
        return int(row[0])

    def _validate_scan_fence(self, fence: ScanLeaseFence, now: str) -> None:
        row = self._conn.execute(
            """
            SELECT 1 FROM physical_scan_tasks AS task
            JOIN source_catalog AS catalog
              USING (account_id, source_generation, shard_name, table_name)
            JOIN cursors AS cursor
              USING (account_id, source_generation, shard_name, table_name)
            WHERE task.account_id=? AND task.source_generation=?
              AND task.shard_name=? AND task.table_name=?
              AND task.status='processing'
              AND task.lease_owner=? AND task.lease_epoch=?
              AND task.claim_version=? AND task.lease_expires_at > ?
              AND catalog.lifecycle='available'
            """,
            (
                *self._source_values(fence.source),
                fence.lease_owner,
                fence.lease_epoch,
                fence.claim_version,
                now,
            ),
        ).fetchone()
        if row is None:
            raise EventStoreError(
                "Physical scan lease fence rejected stale worker or non-runnable source"
            )

    def _advance_cursor(self, source: SourceKey, last_local_id: int, now: str) -> None:
        cursor = self._conn.execute(
            """
            UPDATE cursors
            SET last_local_id = CASE
                    WHEN last_local_id < ? THEN ? ELSE last_local_id END,
                updated_at = ?
            WHERE account_id=? AND source_generation=?
              AND shard_name=? AND table_name=?
            """,
            (
                last_local_id,
                last_local_id,
                now,
                *self._source_values(source),
            ),
        )
        if cursor.rowcount != 1:
            raise EventStoreError("Cannot advance a cursor that is not initialized")

    @staticmethod
    def _session_identity_values(
        account_id: str,
        source_generation: str,
        session_database_name: str,
        username: str,
    ) -> tuple[str, str, str, str]:
        values = (account_id, source_generation, session_database_name, username)
        for value in values:
            if not value or value.strip() != value:
                raise ValueError("Session scan identity values must be non-empty and trimmed")
            if any(ord(character) < 32 for character in value):
                raise ValueError("Session scan identity must not contain control characters")
        return values

    def observe_session_hints(
        self,
        account_id: str,
        source_generation: str,
        session_database_name: str,
        hints: Iterable[SessionHint],
    ) -> SessionObservationResult:
        """Persist one SessionTable snapshot and coalesce durable scan work."""

        snapshot = list(hints)
        if len({hint.username for hint in snapshot}) != len(snapshot):
            raise EventStoreError("A SessionTable snapshot must not repeat usernames")
        observed = len(snapshot)
        changed = 0
        queued = 0
        now = _utc_now()
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            for hint in snapshot:
                identity = self._session_identity_values(
                    account_id,
                    source_generation,
                    session_database_name,
                    hint.username,
                )
                row = self._conn.execute(
                    """
                    SELECT table_name, seen_last_local_id, seen_last_timestamp
                    FROM session_observations
                    WHERE account_id=? AND source_generation=?
                      AND session_database_name=? AND username=?
                    """,
                    identity,
                ).fetchone()
                is_changed = row is None
                if row is None:
                    self._conn.execute(
                        """
                        INSERT INTO session_observations (
                            account_id, source_generation, session_database_name,
                            username, table_name, seen_last_local_id,
                            seen_last_timestamp, completed_last_local_id,
                            completed_last_timestamp, initialized_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)
                        """,
                        (
                            *identity,
                            hint.table_name,
                            hint.last_msg_local_id,
                            hint.last_timestamp,
                            now,
                            now,
                        ),
                    )
                else:
                    previous_id = int(row["seen_last_local_id"])
                    previous_timestamp = int(row["seen_last_timestamp"])
                    if hint.last_msg_local_id < previous_id:
                        raise SessionHintRegression(
                            "SessionTable last message local id moved backwards"
                        )
                    is_changed = (
                        hint.last_msg_local_id != previous_id
                        or hint.last_timestamp != previous_timestamp
                    )
                    if is_changed:
                        self._conn.execute(
                            """
                            UPDATE session_observations
                            SET table_name=?, seen_last_local_id=?,
                                seen_last_timestamp=?, updated_at=?
                            WHERE account_id=? AND source_generation=?
                              AND session_database_name=? AND username=?
                            """,
                            (
                                hint.table_name,
                                hint.last_msg_local_id,
                                hint.last_timestamp,
                                now,
                                *identity,
                            ),
                        )

                if not is_changed:
                    continue
                changed += 1
                queue_row = self._conn.execute(
                    """
                    SELECT target_local_id, target_timestamp, status
                    FROM session_scan_queue
                    WHERE account_id=? AND source_generation=?
                      AND session_database_name=? AND username=?
                    """,
                    identity,
                ).fetchone()
                if queue_row is None:
                    self._conn.execute(
                        """
                        INSERT INTO session_scan_queue (
                            account_id, source_generation, session_database_name,
                            username, table_name, target_local_id, target_timestamp,
                            status, attempt_count, available_at, lease_owner,
                            lease_expires_at, last_error, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, NULL,
                                  NULL, '', ?, ?)
                        """,
                        (
                            *identity,
                            hint.table_name,
                            hint.last_msg_local_id,
                            hint.last_timestamp,
                            now,
                            now,
                            now,
                        ),
                    )
                    queued += 1
                elif str(queue_row["status"]) != "blocked":
                    old_target = int(queue_row["target_local_id"])
                    target_id = max(old_target, hint.last_msg_local_id)
                    target_timestamp = (
                        hint.last_timestamp
                        if hint.last_msg_local_id >= old_target
                        else int(queue_row["target_timestamp"])
                    )
                    self._conn.execute(
                        """
                        UPDATE session_scan_queue
                        SET table_name=?, target_local_id=?, target_timestamp=?,
                            available_at=CASE WHEN status='pending' THEN ?
                                              ELSE available_at END,
                            updated_at=?
                        WHERE account_id=? AND source_generation=?
                          AND session_database_name=? AND username=?
                        """,
                        (
                            hint.table_name,
                            target_id,
                            target_timestamp,
                            now,
                            now,
                            *identity,
                        ),
                    )
                    queued += 1
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return SessionObservationResult(observed, changed, queued)

    def claim_session_scan(
        self,
        lease_owner: str,
        *,
        lease_seconds: float = 30.0,
    ) -> SessionScanTask | None:
        if not lease_owner or lease_owner.strip() != lease_owner:
            raise ValueError("lease_owner must be a non-empty trimmed string")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat(timespec="microseconds")
        expires = (now_dt + timedelta(seconds=lease_seconds)).isoformat(
            timespec="microseconds"
        )
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.execute(
                """
                UPDATE session_scan_queue
                SET status='pending', lease_owner=NULL, lease_expires_at=NULL,
                    available_at=?, last_error='LeaseExpired', updated_at=?
                WHERE status='processing' AND lease_expires_at <= ?
                """,
                (now, now, now),
            )
            row = self._conn.execute(
                """
                SELECT * FROM session_scan_queue
                WHERE status='pending' AND available_at <= ?
                ORDER BY updated_at DESC, available_at
                LIMIT 1
                """,
                (now,),
            ).fetchone()
            if row is None:
                self._conn.commit()
                return None
            identity = (
                row["account_id"],
                row["source_generation"],
                row["session_database_name"],
                row["username"],
            )
            updated = self._conn.execute(
                """
                UPDATE session_scan_queue
                SET status='processing', attempt_count=attempt_count+1,
                    lease_owner=?, lease_expires_at=?, updated_at=?
                WHERE account_id=? AND source_generation=?
                  AND session_database_name=? AND username=?
                  AND status='pending'
                """,
                (lease_owner, expires, now, *identity),
            )
            if updated.rowcount != 1:
                raise EventStoreError("Session scan claim was not exclusive")
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return SessionScanTask(
            account_id=str(row["account_id"]),
            source_generation=str(row["source_generation"]),
            session_database_name=str(row["session_database_name"]),
            username=str(row["username"]),
            table_name=str(row["table_name"]),
            target_local_id=int(row["target_local_id"]),
            target_timestamp=int(row["target_timestamp"]),
            lease_owner=lease_owner,
        )

    @staticmethod
    def _validate_error_type(error_type: str) -> str:
        if (
            not error_type
            or len(error_type) > 100
            or not error_type.replace("_", "").isalnum()
        ):
            raise ValueError("Only a short exception type may be persisted")
        return error_type

    def retry_session_scan(
        self,
        task: SessionScanTask,
        error_type: str,
        *,
        delay_seconds: float = 0.0,
    ) -> None:
        if delay_seconds < 0:
            raise ValueError("delay_seconds must not be negative")
        error_type = self._validate_error_type(error_type)
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat(timespec="microseconds")
        available = (now_dt + timedelta(seconds=delay_seconds)).isoformat(
            timespec="microseconds"
        )
        identity = self._session_identity_values(
            task.account_id,
            task.source_generation,
            task.session_database_name,
            task.username,
        )
        updated = self._conn.execute(
            """
            UPDATE session_scan_queue
            SET status='pending', available_at=?, lease_owner=NULL,
                lease_expires_at=NULL, last_error=?, updated_at=?
            WHERE account_id=? AND source_generation=?
              AND session_database_name=? AND username=?
              AND status='processing' AND lease_owner=?
            """,
            (available, error_type, now, *identity, task.lease_owner),
        )
        if updated.rowcount != 1:
            self._conn.rollback()
            raise EventStoreError("Cannot retry an unowned session scan")
        self._conn.commit()

    def renew_session_scan(
        self,
        task: SessionScanTask,
        *,
        lease_seconds: float = 120.0,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat(timespec="microseconds")
        expires = (now_dt + timedelta(seconds=lease_seconds)).isoformat(
            timespec="microseconds"
        )
        identity = self._session_identity_values(
            task.account_id,
            task.source_generation,
            task.session_database_name,
            task.username,
        )
        updated = self._conn.execute(
            """
            UPDATE session_scan_queue
            SET lease_expires_at=?, updated_at=?
            WHERE account_id=? AND source_generation=?
              AND session_database_name=? AND username=?
              AND status='processing' AND lease_owner=?
            """,
            (expires, now, *identity, task.lease_owner),
        )
        if updated.rowcount != 1:
            self._conn.rollback()
            raise EventStoreError("Cannot renew an unowned session scan")
        self._conn.commit()

    def block_session_scan(self, task: SessionScanTask, error_type: str) -> None:
        error_type = self._validate_error_type(error_type)
        now = _utc_now()
        identity = self._session_identity_values(
            task.account_id,
            task.source_generation,
            task.session_database_name,
            task.username,
        )
        updated = self._conn.execute(
            """
            UPDATE session_scan_queue
            SET status='blocked', lease_owner=NULL, lease_expires_at=NULL,
                last_error=?, updated_at=?
            WHERE account_id=? AND source_generation=?
              AND session_database_name=? AND username=?
              AND status='processing' AND lease_owner=?
            """,
            (error_type, now, *identity, task.lease_owner),
        )
        if updated.rowcount != 1:
            self._conn.rollback()
            raise EventStoreError("Cannot block an unowned session scan")
        self._conn.commit()

    def complete_session_scan(self, task: SessionScanTask) -> bool:
        """Complete a claimed target, preserving a newer coalesced target."""

        now = _utc_now()
        identity = self._session_identity_values(
            task.account_id,
            task.source_generation,
            task.session_database_name,
            task.username,
        )
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute(
                """
                SELECT target_local_id, target_timestamp
                FROM session_scan_queue
                WHERE account_id=? AND source_generation=?
                  AND session_database_name=? AND username=?
                  AND status='processing' AND lease_owner=?
                """,
                (*identity, task.lease_owner),
            ).fetchone()
            if row is None:
                raise EventStoreError("Cannot complete an unowned session scan")
            self._conn.execute(
                """
                UPDATE session_observations
                SET completed_last_local_id=CASE
                        WHEN completed_last_local_id < ? THEN ?
                        ELSE completed_last_local_id END,
                    completed_last_timestamp=CASE
                        WHEN completed_last_local_id <= ? THEN ?
                        ELSE completed_last_timestamp END,
                    updated_at=?
                WHERE account_id=? AND source_generation=?
                  AND session_database_name=? AND username=?
                """,
                (
                    task.target_local_id,
                    task.target_local_id,
                    task.target_local_id,
                    task.target_timestamp,
                    now,
                    *identity,
                ),
            )
            target_advanced = (
                int(row["target_local_id"]) != task.target_local_id
                or int(row["target_timestamp"]) != task.target_timestamp
            )
            if target_advanced:
                self._conn.execute(
                    """
                    UPDATE session_scan_queue
                    SET status='pending', available_at=?, lease_owner=NULL,
                        lease_expires_at=NULL, last_error='', updated_at=?
                    WHERE account_id=? AND source_generation=?
                      AND session_database_name=? AND username=?
                    """,
                    (now, now, *identity),
                )
            else:
                self._conn.execute(
                    """
                    DELETE FROM session_scan_queue
                    WHERE account_id=? AND source_generation=?
                      AND session_database_name=? AND username=?
                    """,
                    identity,
                )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return not target_advanced

    def session_queue_count(self, status: str | None = None) -> int:
        if status is None:
            row = self._conn.execute(
                "SELECT count(*) FROM session_scan_queue"
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT count(*) FROM session_scan_queue WHERE status=?", (status,)
            ).fetchone()
        return int(row[0])

    def session_observation(self, username: str) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT * FROM session_observations WHERE username=?", (username,)
        ).fetchone()

    def session_scan_status(self, username: str) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT * FROM session_scan_queue WHERE username=?", (username,)
        ).fetchone()

    def commit_messages(
        self,
        source: SourceKey,
        messages: list[RawMessage],
        parser: Parser | None = None,
        *,
        scan_fence: ScanLeaseFence | None = None,
    ) -> StoreBatchResult:
        current_cursor = self.get_cursor(source)
        if current_cursor is None:
            raise EventStoreError("Cursor must be initialized before storing messages")
        if not messages:
            return StoreBatchResult(0, 0, 0, current_cursor)
        ordered = sorted(messages, key=lambda message: message.local_id)
        if len({message.local_id for message in ordered}) != len(ordered):
            raise EventStoreError("A source batch must not repeat local_id values")

        now = _utc_now()
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            if scan_fence is not None:
                if scan_fence.source != source:
                    raise EventStoreError("Physical scan fence SourceKey mismatch")
                self._validate_scan_fence(scan_fence, now)
            inserted_events, inserted_revisions, parse_errors = self._persist_messages(
                source, ordered, parser, now
            )

            new_cursor = max(current_cursor, max(message.local_id for message in ordered))
            self._advance_cursor(source, new_cursor, now)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return StoreBatchResult(
            inserted_events=inserted_events,
            inserted_revisions=inserted_revisions,
            parse_errors=parse_errors,
            cursor=new_cursor,
        )

    def _persist_messages(
        self,
        source: SourceKey,
        ordered: list[RawMessage],
        parser: Parser | None,
        now: str,
    ) -> tuple[int, int, int]:
        """Persist events/revisions/outbox rows inside the caller's transaction."""

        inserted_events = 0
        inserted_revisions = 0
        parse_errors = 0
        for message in ordered:
            event_cursor = self._conn.execute(
                """
                INSERT OR IGNORE INTO events (
                    event_uid, account_id, source_generation, shard_name,
                    table_name, local_id, first_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _event_uid(source, message.local_id),
                    *self._source_values(source),
                    message.local_id,
                    now,
                ),
            )
            inserted_events += max(event_cursor.rowcount, 0)
            event_row = self._conn.execute(
                """
                SELECT id FROM events
                WHERE account_id=? AND source_generation=?
                  AND shard_name=? AND table_name=? AND local_id=?
                """,
                (*self._source_values(source), message.local_id),
            ).fetchone()
            if not event_row:
                raise EventStoreError("Event identity insert could not be read back")
            event_id = int(event_row[0])
            fingerprint = _content_fingerprint(message)
            existing_revision = self._conn.execute(
                """
                SELECT id FROM event_revisions
                WHERE event_id=? AND content_fingerprint=?
                """,
                (event_id, fingerprint),
            ).fetchone()
            if existing_revision:
                continue

            parse_status = "pending"
            parse_error = ""
            parsed_payload_json = None
            if parser is not None:
                try:
                    parsed = parser(message)
                    if not isinstance(parsed, Mapping):
                        raise TypeError("Parser result must be a mapping")
                    parsed_payload_json = json.dumps(
                        dict(parsed), ensure_ascii=False, sort_keys=True
                    )
                    parse_status = "parsed"
                except Exception as exc:
                    parse_status = "error"
                    parse_error = type(exc).__name__
                    parse_errors += 1

            revision_number = int(
                self._conn.execute(
                    """
                    SELECT COALESCE(max(revision_number), 0) + 1
                    FROM event_revisions WHERE event_id=?
                    """,
                    (event_id,),
                ).fetchone()[0]
            )
            raw_content_kind, raw_content = _raw_content_parts(message.raw_content)
            revision_cursor = self._conn.execute(
                """
                INSERT INTO event_revisions (
                    event_id, revision_number, content_fingerprint, server_id,
                    local_type, create_time, real_sender_id, raw_content,
                    raw_content_kind, compression_type, parse_status,
                    parse_error, parsed_payload_json, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    revision_number,
                    fingerprint,
                    str(message.server_id) if message.server_id is not None else None,
                    message.local_type,
                    message.create_time,
                    message.real_sender_id,
                    raw_content,
                    raw_content_kind,
                    message.compression_type,
                    parse_status,
                    parse_error,
                    parsed_payload_json,
                    now,
                ),
            )
            revision_id = int(revision_cursor.lastrowid)
            inserted_revisions += 1
            self._conn.execute(
                "UPDATE events SET current_revision_id=? WHERE id=?",
                (revision_id, event_id),
            )
            self._conn.execute(
                """
                INSERT INTO processing_outbox (
                    revision_id, task_kind, status, attempt_count,
                    available_at, created_at, updated_at
                ) VALUES (?, 'process_event', 'pending', 0, ?, ?, ?)
                """,
                (revision_id, now, now, now),
            )
        return inserted_events, inserted_revisions, parse_errors

    def commit_window_messages(
        self,
        source: SourceKey,
        messages: list[RawMessage],
        run: WindowIngestRun,
    ) -> WindowIngestResult:
        """Persist one catalogued time-window without cursor or lifecycle writes."""

        if not isinstance(run, WindowIngestRun):
            raise TypeError("run must be a WindowIngestRun")
        ordered = sorted(messages, key=lambda message: message.local_id)
        if len({message.local_id for message in ordered}) != len(ordered):
            raise EventStoreError("A source batch must not repeat local_id values")

        now = _utc_now()
        run_id = uuid.uuid4().hex
        coverage = run.source_coverage
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            catalog_row = self._conn.execute(
                """
                SELECT lifecycle FROM source_catalog
                WHERE account_id=? AND source_generation=?
                  AND shard_name=? AND table_name=?
                """,
                self._source_values(source),
            ).fetchone()
            if catalog_row is None:
                raise EventStoreError("Window ingestion requires a catalogued source")
            if str(catalog_row["lifecycle"]) == "replacement_suspected":
                raise EventStoreError(
                    "Window ingestion refuses a replacement-suspected source"
                )
            self._conn.execute(
                """
                INSERT INTO window_ingest_runs (
                    run_id, requested_start, requested_end, source_min_time,
                    source_max_time, examined_count, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    run.requested_start_utc,
                    run.requested_end_utc,
                    coverage["source_min_time"],
                    coverage["source_max_time"],
                    coverage["examined_count"],
                    now,
                ),
            )
            inserted_events, inserted_revisions, parse_errors = self._persist_messages(
                source, ordered, None, now
            )
            self._conn.execute(
                """
                INSERT INTO window_ingest_outcomes (
                    run_id, received_messages, inserted_events,
                    inserted_revisions, parse_errors, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    len(ordered),
                    inserted_events,
                    inserted_revisions,
                    parse_errors,
                    now,
                ),
            )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return WindowIngestResult(
            run_id=run_id,
            inserted_events=inserted_events,
            inserted_revisions=inserted_revisions,
            parse_errors=parse_errors,
        )

    def _association_conflict(self, candidates: list[tuple]) -> bool:
        """Check only observed candidate IDs against other tables in their shard.

        Historical revisions indicate an observed conflict, not current source
        state. No source bodies are read and no existing facts are changed.
        """
        by_source: dict[SourceKey, list[tuple]] = {}
        grouped: dict[tuple, list[tuple]] = {}
        for source, local_id, server, kind, timestamp, sender, fingerprint in candidates:
            expected = (None if server is None else str(server), kind, timestamp, sender, fingerprint)
            by_source.setdefault(source, []).append((local_id, expected))
            if server is not None and str(server):
                key = (source.account_id, source.source_generation, source.shard_name)
                grouped.setdefault(key, []).append((source.table_name, local_id, expected))
        for source, items in by_source.items():
            for offset in range(0, len(items), 400):
                chunk = items[offset:offset+400]
                expected = dict(chunk)
                if len(expected) != len(chunk):
                    raise EventStoreError('association_duplicate_candidate')
                placeholders = ','.join('?' for _ in chunk)
                rows = self._conn.execute(
                    'SELECT e.local_id,r.server_id,r.local_type,r.create_time,r.real_sender_id,r.content_fingerprint '
                    'FROM events e JOIN event_revisions r ON r.id=e.current_revision_id '
                    'WHERE e.account_id=? AND e.source_generation=? AND e.shard_name=? AND e.table_name=? '
                    f'AND e.local_id IN ({placeholders})', (*self._source_values(source), *expected),
                ).fetchall()
                if len(rows) != len(chunk) or any(tuple(row)[1:] != expected.get(row[0]) for row in rows):
                    raise EventStoreError('association_current_revision_unavailable')
        for key, items in grouped.items():
            for offset in range(0, len(items), 400):
                chunk = items[offset:offset+400]
                identifiers = sorted({item[1] for item in chunk})
                placeholders = ','.join('?' for _ in identifiers)
                rows = self._conn.execute(
                    'SELECT e.table_name,e.local_id,r.server_id,r.local_type,r.create_time,r.real_sender_id,r.content_fingerprint '
                    'FROM events e JOIN event_revisions r ON r.event_id=e.id '
                    'WHERE e.account_id=? AND e.source_generation=? AND e.shard_name=? '
                    'AND e.table_name IN (SELECT DISTINCT table_name FROM events WHERE account_id=? AND source_generation=? AND shard_name=?) '
                    f'AND e.local_id IN ({placeholders})', (*key, *key, *identifiers),
                ).fetchall()
                expected_rows: dict[tuple, set[str]] = {}
                for own, local_id, expected in chunk:
                    expected_rows.setdefault((local_id, *expected), set()).add(own)
                for row in rows:
                    origins = expected_rows.get(tuple(row)[1:])
                    if origins and (len(origins) > 1 or row[0] not in origins):
                        return True
        return False

    def _empty_window_proofs(self, account_id: str, source_generation: str) -> dict:
        rows = self._conn.execute(
            'SELECT shard_name,table_name,window_start,window_end,source_stamps,path_binding,no_future '
            'FROM current_empty_window_coverage WHERE account_id=? AND source_generation=? AND predicate_version=1',
            (account_id, source_generation),
        ).fetchall()
        proofs = {}
        for row in rows:
            try:
                stamps = tuple(tuple(part) if part is not None else None for part in json.loads(row['source_stamps']))
                if len(stamps) != 2 or stamps[0] is None or any(part is not None and (len(part) != 5 or any(type(x) is not int for x in part)) for part in stamps):
                    continue
                source = SourceKey(account_id, source_generation, str(row['shard_name']), str(row['table_name']))
                start, end, future = int(row['window_start']), int(row['window_end']), int(row['no_future'])
                binding = row['path_binding']
                if start > end or future not in (0, 1) or not isinstance(binding, str) or re.fullmatch(r'[0-9a-f]{64}', binding) is None:
                    continue
                proofs[source] = (start, end, stamps, bool(future), binding)
            except (ValueError, TypeError, json.JSONDecodeError):
                continue
        return proofs

    def _window_baseline_initialized(self, account_id: str, source_generation: str) -> bool:
        return self._conn.execute(
            'SELECT 1 FROM current_window_baseline WHERE account_id=? AND source_generation=? AND predicate_version=1 AND completed=1',
            (account_id, source_generation),
        ).fetchone() is not None

    def _begin_window_baseline(self, account_id: str, source_generation: str) -> bool:
        """Persist the one allowed initial metadata attempt before any MAX."""
        with self._conn:
            result = self._conn.execute(
                'INSERT OR IGNORE INTO current_window_baseline (account_id,source_generation,predicate_version,completed) VALUES (?,?,1,0)',
                (account_id, source_generation),
            )
            return result.rowcount == 1

    def _save_empty_window_proofs(self, entries: list[tuple], invalid: tuple = (), initialized: tuple | None = None) -> None:
        if not entries and not invalid and initialized is None:
            return
        values = [(*self._source_values(source), start, end, json.dumps(stamps, separators=(',', ':')), binding, 1, int(no_future)) for source, start, end, stamps, no_future, binding in entries]
        with self._conn:
            self._conn.executemany(
                'DELETE FROM current_empty_window_coverage WHERE account_id=? AND source_generation=? AND shard_name=? AND table_name=?',
                [self._source_values(source) for source in invalid],
            )
            self._conn.executemany(
                'INSERT OR REPLACE INTO current_empty_window_coverage '
                '(account_id,source_generation,shard_name,table_name,window_start,window_end,source_stamps,path_binding,predicate_version,no_future) '
                'VALUES (?,?,?,?,?,?,?,?,?,?)', values,
            )
            if initialized is not None:
                result = self._conn.execute(
                    'UPDATE current_window_baseline SET completed=1 WHERE account_id=? AND source_generation=? AND predicate_version=1 AND completed=0', initialized,
                )
                if result.rowcount != 1:
                    raise EventStoreError('Initial baseline attempt is unavailable')

    def window_ingest_run(self, run_id: str) -> sqlite3.Row | None:
        """Read the redacted facts for an opaque, completed window ingest run."""

        if not isinstance(run_id, str) or re.fullmatch(r"[0-9a-f]{32}", run_id) is None:
            raise ValueError("run_id must be an opaque 32-character lowercase hex id")
        return self._conn.execute(
            """
            SELECT runs.run_id, runs.requested_start, runs.requested_end,
                   runs.source_min_time, runs.source_max_time,
                   runs.examined_count, outcomes.received_messages,
                   outcomes.inserted_events, outcomes.inserted_revisions,
                   outcomes.parse_errors, outcomes.completed_at
            FROM window_ingest_runs AS runs
            JOIN window_ingest_outcomes AS outcomes USING (run_id)
            WHERE runs.run_id=?
            """,
            (run_id,),
        ).fetchone()

    @staticmethod
    def _consumer_profile_values(
        consumer_name: str,
        contract_version: str,
        normalizer_version: str,
    ) -> tuple[str, str, str]:
        return (
            _validate_trimmed_text(consumer_name, "consumer_name", 100),
            _validate_trimmed_text(contract_version, "contract_version", 100),
            _validate_trimmed_text(
                normalizer_version, "normalizer_version", 100
            ),
        )

    @classmethod
    def _consumer_key_values(
        cls,
        outbox_id: int,
        consumer_name: str,
        contract_version: str,
        normalizer_version: str,
    ) -> tuple[int, str, str, str]:
        if outbox_id <= 0:
            raise ValueError("outbox_id must be positive")
        return (
            outbox_id,
            *cls._consumer_profile_values(
                consumer_name, contract_version, normalizer_version
            ),
        )

    def bootstrap_outbox_consumer_states(
        self,
        consumer_name: str,
        contract_version: str,
        normalizer_version: str,
        *,
        available_at: str | None = None,
    ) -> int:
        """Register the current durable process-event snapshot idempotently."""

        profile = self._consumer_profile_values(
            consumer_name, contract_version, normalizer_version
        )
        now = _utc_now()
        available = (
            now
            if available_at is None
            else _validate_utc_timestamp(available_at, "available_at")
        )
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.execute(
                """
                INSERT OR IGNORE INTO outbox_consumer_state (
                    outbox_id, consumer_name, contract_version,
                    normalizer_version, status, attempt_count, available_at,
                    lease_owner, lease_expires_at, last_error_type,
                    completed_at, created_at, updated_at
                )
                SELECT outbox.id, ?, ?, ?, 'pending', 0, ?, NULL, NULL, '',
                       NULL, ?, ?
                FROM processing_outbox AS outbox
                WHERE outbox.task_kind='process_event'
                """,
                (*profile, available, now, now),
            )
            inserted = int(self._conn.execute("SELECT changes()").fetchone()[0])
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return inserted

    def ensure_outbox_consumer_state(
        self,
        outbox_id: int,
        consumer_name: str,
        contract_version: str,
        normalizer_version: str,
        *,
        available_at: str | None = None,
    ) -> OutboxConsumerState:
        """Idempotently create consumer-owned state without mutating its outbox."""

        key = self._consumer_key_values(
            outbox_id, consumer_name, contract_version, normalizer_version
        )
        now = _utc_now()
        available = (
            now
            if available_at is None
            else _validate_utc_timestamp(available_at, "available_at")
        )
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.execute(
                """
                INSERT OR IGNORE INTO outbox_consumer_state (
                    outbox_id, consumer_name, contract_version,
                    normalizer_version, status, attempt_count, available_at,
                    lease_owner, lease_expires_at, last_error_type,
                    completed_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'pending', 0, ?, NULL, NULL, '',
                          NULL, ?, ?)
                """,
                (*key, available, now, now),
            )
            row = self._conn.execute(
                """
                SELECT * FROM outbox_consumer_state
                WHERE outbox_id=? AND consumer_name=? AND contract_version=?
                  AND normalizer_version=?
                """,
                key,
            ).fetchone()
            if row is None:
                raise EventStoreError("Consumer state could not be persisted")
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return _consumer_state_from_row(row)

    def get_outbox_consumer_state(
        self,
        outbox_id: int,
        consumer_name: str,
        contract_version: str,
        normalizer_version: str,
    ) -> OutboxConsumerState | None:
        key = self._consumer_key_values(
            outbox_id, consumer_name, contract_version, normalizer_version
        )
        row = self._conn.execute(
            """
            SELECT * FROM outbox_consumer_state
            WHERE outbox_id=? AND consumer_name=? AND contract_version=?
              AND normalizer_version=?
            """,
            key,
        ).fetchone()
        return _consumer_state_from_row(row) if row is not None else None

    def claim_next_outbox_consumer_state(
        self,
        consumer_name: str,
        contract_version: str,
        normalizer_version: str,
        lease_owner: str,
        lease_expires_at: str,
    ) -> OutboxConsumerState | None:
        """Recover expired leases and claim the oldest ready process-event row."""

        profile = self._consumer_profile_values(
            consumer_name, contract_version, normalizer_version
        )
        owner = _validate_trimmed_text(lease_owner, "lease_owner", 100)
        expires = _validate_utc_timestamp(lease_expires_at, "lease_expires_at")
        now = _utc_now()
        if expires <= now:
            raise ValueError("lease_expires_at must be later than the claim time")
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.execute(
                """
                UPDATE outbox_consumer_state
                SET status='pending', lease_owner=NULL, lease_expires_at=NULL,
                    available_at=?, last_error_type='LeaseExpired', updated_at=?
                WHERE consumer_name=? AND contract_version=?
                  AND normalizer_version=? AND status='processing'
                  AND lease_expires_at <= ?
                  AND EXISTS (
                      SELECT 1 FROM processing_outbox AS outbox
                      WHERE outbox.id=outbox_consumer_state.outbox_id
                        AND outbox.task_kind='process_event'
                  )
                """,
                (now, now, *profile, now),
            )
            ready = self._conn.execute(
                """
                SELECT state.id
                FROM outbox_consumer_state AS state
                JOIN processing_outbox AS outbox ON outbox.id=state.outbox_id
                WHERE state.consumer_name=? AND state.contract_version=?
                  AND state.normalizer_version=? AND state.status='pending'
                  AND state.available_at <= ?
                  AND outbox.task_kind='process_event'
                ORDER BY outbox.id ASC
                LIMIT 1
                """,
                (*profile, now),
            ).fetchone()
            if ready is None:
                self._conn.commit()
                return None
            consumer_state_id = int(ready["id"])
            updated = self._conn.execute(
                """
                UPDATE outbox_consumer_state
                SET status='processing', attempt_count=attempt_count+1,
                    lease_owner=?, lease_expires_at=?, updated_at=?
                WHERE id=? AND status='pending' AND available_at <= ?
                """,
                (owner, expires, now, consumer_state_id, now),
            )
            if updated.rowcount != 1:
                raise EventStoreError("Consumer state claim was not exclusive")
            row = self._conn.execute(
                "SELECT * FROM outbox_consumer_state WHERE id=?",
                (consumer_state_id,),
            ).fetchone()
            if row is None:
                raise EventStoreError("Claimed consumer state could not be read back")
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return _consumer_state_from_row(row)

    def renew_outbox_consumer_lease(
        self,
        consumer_state_id: int,
        lease_owner: str,
        lease_expires_at: str,
    ) -> OutboxConsumerState:
        """Extend an exact owned processing lease without changing its attempt."""

        if consumer_state_id <= 0:
            raise ValueError("consumer_state_id must be positive")
        owner = _validate_trimmed_text(lease_owner, "lease_owner", 100)
        expires = _validate_utc_timestamp(lease_expires_at, "lease_expires_at")
        now = _utc_now()
        if expires <= now:
            raise ValueError("lease_expires_at must be later than the renewal time")
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            updated = self._conn.execute(
                """
                UPDATE outbox_consumer_state
                SET lease_expires_at=?, updated_at=?
                WHERE id=? AND status='processing' AND lease_owner=?
                """,
                (expires, now, consumer_state_id, owner),
            )
            if updated.rowcount != 1:
                raise EventStoreError("Cannot renew an unowned consumer state")
            row = self._conn.execute(
                "SELECT * FROM outbox_consumer_state WHERE id=?",
                (consumer_state_id,),
            ).fetchone()
            if row is None:
                raise EventStoreError("Renewed consumer state could not be read back")
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return _consumer_state_from_row(row)

    def load_claimed_process_event(
        self,
        consumer_state_id: int,
        lease_owner: str,
    ) -> ClaimedProcessEvent:
        """Read the exact immutable revision behind an owned process-event claim."""

        if consumer_state_id <= 0:
            raise ValueError("consumer_state_id must be positive")
        owner = _validate_trimmed_text(lease_owner, "lease_owner", 100)
        row = self._conn.execute(
            """
            SELECT state.id AS consumer_state_id,
                   state.status AS consumer_status,
                   state.lease_owner AS consumer_lease_owner,
                   outbox.id AS outbox_id,
                   outbox.task_kind AS task_kind,
                   outbox.revision_id AS source_revision_id,
                   revision.id AS joined_revision_id,
                   revision.content_fingerprint,
                   revision.revision_number,
                   revision.server_id,
                   revision.local_type,
                   revision.create_time,
                   revision.real_sender_id,
                   revision.raw_content,
                   revision.raw_content_kind,
                   revision.compression_type,
                   revision.observed_at,
                   event.id AS source_event_id,
                   event.event_uid,
                   event.account_id,
                   event.source_generation,
                   event.shard_name,
                   event.table_name,
                   event.local_id
            FROM outbox_consumer_state AS state
            LEFT JOIN processing_outbox AS outbox ON outbox.id=state.outbox_id
            LEFT JOIN event_revisions AS revision ON revision.id=outbox.revision_id
            LEFT JOIN events AS event ON event.id=revision.event_id
            WHERE state.id=?
            """,
            (consumer_state_id,),
        ).fetchone()
        if row is None:
            raise EventStoreError("Claimed consumer state does not exist")
        if (
            str(row["consumer_status"]) != "processing"
            or row["consumer_lease_owner"] is None
            or str(row["consumer_lease_owner"]) != owner
        ):
            raise EventStoreError("Cannot load an unowned consumer state")
        if (
            row["outbox_id"] is None
            or row["source_revision_id"] is None
            or row["joined_revision_id"] is None
            or row["source_event_id"] is None
        ):
            raise MissingSourceRevision(
                "Claimed process-event relationship is incomplete"
            )
        if str(row["task_kind"]) != "process_event":
            raise EventStoreError("Claimed consumer state is not a process_event")
        raw_content = row["raw_content"]
        if isinstance(raw_content, memoryview):
            raw_content = bytes(raw_content)
        return ClaimedProcessEvent(
            consumer_state_id=int(row["consumer_state_id"]),
            outbox_id=int(row["outbox_id"]),
            task_kind=str(row["task_kind"]),
            source_revision_id=int(row["source_revision_id"]),
            event_uid=str(row["event_uid"]),
            account_id=str(row["account_id"]),
            source_generation=str(row["source_generation"]),
            shard_name=str(row["shard_name"]),
            table_name=str(row["table_name"]),
            local_id=int(row["local_id"]),
            content_fingerprint=str(row["content_fingerprint"]),
            revision_number=int(row["revision_number"]),
            server_id=(
                str(row["server_id"]) if row["server_id"] is not None else None
            ),
            local_type=int(row["local_type"]),
            create_time=(
                int(row["create_time"])
                if row["create_time"] is not None
                else None
            ),
            real_sender_id=(
                int(row["real_sender_id"])
                if row["real_sender_id"] is not None
                else None
            ),
            raw_content=raw_content,
            raw_content_kind=str(row["raw_content_kind"]),
            compression_type=(
                int(row["compression_type"])
                if row["compression_type"] is not None
                else None
            ),
            observed_at=str(row["observed_at"]),
        )

    def outbox_consumer_state_counts(
        self,
        consumer_name: str,
        contract_version: str,
        normalizer_version: str,
    ) -> OutboxConsumerStateCounts:
        """Return aggregate-only lifecycle counts for one process-event profile."""

        profile = self._consumer_profile_values(
            consumer_name, contract_version, normalizer_version
        )
        rows = self._conn.execute(
            """
            SELECT state.status, count(*) AS state_count
            FROM outbox_consumer_state AS state
            JOIN processing_outbox AS outbox ON outbox.id=state.outbox_id
            WHERE state.consumer_name=? AND state.contract_version=?
              AND state.normalizer_version=?
              AND outbox.task_kind='process_event'
            GROUP BY state.status
            """,
            profile,
        ).fetchall()
        counts = {str(row["status"]): int(row["state_count"]) for row in rows}
        return OutboxConsumerStateCounts(
            pending=counts.get("pending", 0),
            processing=counts.get("processing", 0),
            complete=counts.get("complete", 0),
            failed=counts.get("failed", 0),
        )

    def begin_outbox_consumer_attempt(
        self,
        consumer_state_id: int,
        lease_owner: str,
        lease_expires_at: str,
    ) -> OutboxConsumerState:
        """Persist an exact-row lease; selection and scheduling belong to I3."""

        if consumer_state_id <= 0:
            raise ValueError("consumer_state_id must be positive")
        owner = _validate_trimmed_text(lease_owner, "lease_owner", 100)
        expires = _validate_utc_timestamp(lease_expires_at, "lease_expires_at")
        now = _utc_now()
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            updated = self._conn.execute(
                """
                UPDATE outbox_consumer_state
                SET status='processing', attempt_count=attempt_count+1,
                    lease_owner=?, lease_expires_at=?, updated_at=?
                WHERE id=? AND status='pending' AND available_at <= ?
                """,
                (owner, expires, now, consumer_state_id, now),
            )
            if updated.rowcount != 1:
                raise EventStoreError("Consumer state is not available for this attempt")
            row = self._conn.execute(
                "SELECT * FROM outbox_consumer_state WHERE id=?",
                (consumer_state_id,),
            ).fetchone()
            if row is None:
                raise EventStoreError("Claimed consumer state could not be read back")
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return _consumer_state_from_row(row)

    def retry_outbox_consumer_state(
        self,
        consumer_state_id: int,
        lease_owner: str,
        error: BaseException | str,
        *,
        available_at: str,
    ) -> OutboxConsumerState:
        """Release an owned exact row for retry without calculating backoff."""

        if consumer_state_id <= 0:
            raise ValueError("consumer_state_id must be positive")
        owner = _validate_trimmed_text(lease_owner, "lease_owner", 100)
        error_type = _consumer_error_type(error)
        available = _validate_utc_timestamp(available_at, "available_at")
        now = _utc_now()
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            updated = self._conn.execute(
                """
                UPDATE outbox_consumer_state
                SET status='pending', available_at=?, lease_owner=NULL,
                    lease_expires_at=NULL, last_error_type=?, updated_at=?
                WHERE id=? AND status='processing' AND lease_owner=?
                """,
                (available, error_type, now, consumer_state_id, owner),
            )
            if updated.rowcount != 1:
                raise EventStoreError("Cannot retry an unowned consumer state")
            row = self._conn.execute(
                "SELECT * FROM outbox_consumer_state WHERE id=?",
                (consumer_state_id,),
            ).fetchone()
            if row is None:
                raise EventStoreError("Retried consumer state could not be read back")
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return _consumer_state_from_row(row)

    def fail_outbox_consumer_state(
        self,
        consumer_state_id: int,
        lease_owner: str,
        error: BaseException | str,
    ) -> OutboxConsumerState:
        """Record an owned terminal consumer error using only its controlled type."""

        if consumer_state_id <= 0:
            raise ValueError("consumer_state_id must be positive")
        owner = _validate_trimmed_text(lease_owner, "lease_owner", 100)
        error_type = _consumer_error_type(error)
        now = _utc_now()
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            updated = self._conn.execute(
                """
                UPDATE outbox_consumer_state
                SET status='failed', lease_owner=NULL, lease_expires_at=NULL,
                    last_error_type=?, updated_at=?
                WHERE id=? AND status='processing' AND lease_owner=?
                """,
                (error_type, now, consumer_state_id, owner),
            )
            if updated.rowcount != 1:
                raise EventStoreError("Cannot fail an unowned consumer state")
            row = self._conn.execute(
                "SELECT * FROM outbox_consumer_state WHERE id=?",
                (consumer_state_id,),
            ).fetchone()
            if row is None:
                raise EventStoreError("Failed consumer state could not be read back")
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return _consumer_state_from_row(row)

    def get_standard_event_materialization(
        self,
        source_revision_id: int,
        contract_version: str,
        normalizer_name: str,
        normalizer_version: str,
    ) -> StandardEventMaterialization | None:
        if source_revision_id <= 0:
            raise ValueError("source_revision_id must be positive")
        versions = (
            _validate_trimmed_text(contract_version, "contract_version", 100),
            _validate_trimmed_text(normalizer_name, "normalizer_name", 100),
            _validate_trimmed_text(
                normalizer_version, "normalizer_version", 100
            ),
        )
        row = self._conn.execute(
            """
            SELECT * FROM standard_event_materializations
            WHERE source_revision_id=? AND contract_version=?
              AND normalizer_name=? AND normalizer_version=?
            """,
            (source_revision_id, *versions),
        ).fetchone()
        return _materialization_from_row(row) if row is not None else None

    def standard_event_materializations_for_messages(
        self,
        source: SourceKey,
        messages: list[RawMessage],
        *,
        contract_version: str,
        normalizer_name: str,
        normalizer_version: str,
        _by_local_id: dict[int, StandardEventMaterialization | None] | None = None,
    ) -> dict[str, StandardEventMaterialization]:
        """Resolve only exact, current revisions observed in one source read.

        Time-bounded Agent Store search is not an identity boundary.  This
        lookup binds each result to the full physical source key, local id,
        and content fingerprint from the current read before a projection may
        be shown.
        """

        profile = (
            _validate_trimmed_text(contract_version, "contract_version", 100),
            _validate_trimmed_text(normalizer_name, "normalizer_name", 100),
            _validate_trimmed_text(normalizer_version, "normalizer_version", 100),
        )
        expected = {message.local_id: _content_fingerprint(message) for message in messages}
        if len(expected) != len(messages):
            raise EventStoreError("A source batch must not repeat local_id values")

        results: dict[str, StandardEventMaterialization] = {}
        local_ids = sorted(expected)
        for offset in range(0, len(local_ids), 500):
            chunk = local_ids[offset : offset + 500]
            placeholders = ",".join("?" for _ in chunk)
            rows = self._conn.execute(
                f"""
                SELECT materialization.*, event.local_id,
                       revision.content_fingerprint
                FROM events AS event
                JOIN event_revisions AS revision
                  ON revision.id=event.current_revision_id
                JOIN standard_event_materializations AS materialization
                  ON materialization.source_revision_id=revision.id
                WHERE event.account_id=? AND event.source_generation=?
                  AND event.shard_name=? AND event.table_name=?
                  AND event.local_id IN ({placeholders})
                  AND materialization.contract_version=?
                  AND materialization.normalizer_name=?
                  AND materialization.normalizer_version=?
                """,
                (*self._source_values(source), *chunk, *profile),
            ).fetchall()
            for row in rows:
                local_id = int(row["local_id"])
                if str(row["content_fingerprint"]) != expected.get(local_id):
                    continue
                materialization = _materialization_from_row(row)
                if materialization.event_id in results:
                    raise EventStoreError("Duplicate materialization identity")
                results[materialization.event_id] = materialization
                if _by_local_id is not None:
                    _by_local_id[local_id] = None if local_id in _by_local_id else materialization
        return results

    def _insert_standard_event_materialization(
        self,
        materialization: StandardEventMaterializationInput,
        created_at: str,
    ) -> MaterializationWriteResult:
        cursor = self._conn.execute(
            """
            INSERT OR IGNORE INTO standard_event_materializations (
                source_revision_id, event_id, subject_id, contract_version,
                normalizer_name, normalizer_version, canonical_payload_json,
                payload_sha256, quality_status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                materialization.source_revision_id,
                materialization.event_id,
                materialization.subject_id,
                materialization.contract_version,
                materialization.normalizer_name,
                materialization.normalizer_version,
                materialization.canonical_payload_json,
                materialization.payload_sha256,
                materialization.quality_status,
                created_at,
            ),
        )
        row = self._conn.execute(
            """
            SELECT * FROM standard_event_materializations
            WHERE source_revision_id=? AND contract_version=?
              AND normalizer_name=? AND normalizer_version=?
            """,
            (
                materialization.source_revision_id,
                materialization.contract_version,
                materialization.normalizer_name,
                materialization.normalizer_version,
            ),
        ).fetchone()
        if row is None:
            raise EventStoreError("Materialization could not be persisted")
        stored = _materialization_from_row(row)
        if stored.payload_sha256 != materialization.payload_sha256:
            raise MaterializationConflict(
                "Pinned materialization versions produced a different payload hash"
            )
        return MaterializationWriteResult(
            materialization=stored,
            inserted=cursor.rowcount == 1,
        )

    def insert_standard_event_materialization(
        self, materialization: StandardEventMaterializationInput
    ) -> MaterializationWriteResult:
        """Append a derived projection, or verify its idempotent existing hash."""

        _validate_materialization_input(materialization)
        now = _utc_now()
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            result = self._insert_standard_event_materialization(
                materialization, now
            )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return result

    def _mark_outbox_consumer_state_complete(
        self,
        consumer_state_id: int,
        lease_owner: str,
        completed_at: str,
    ) -> None:
        updated = self._conn.execute(
            """
            UPDATE outbox_consumer_state
            SET status='complete', lease_owner=NULL, lease_expires_at=NULL,
                last_error_type='', completed_at=?, updated_at=?
            WHERE id=? AND status='processing' AND lease_owner=?
            """,
            (completed_at, completed_at, consumer_state_id, lease_owner),
        )
        if updated.rowcount != 1:
            raise EventStoreError("Cannot complete an unowned consumer state")

    def complete_outbox_consumer_state_with_materialization(
        self,
        consumer_state_id: int,
        lease_owner: str,
        materialization: StandardEventMaterializationInput,
    ) -> MaterializationWriteResult:
        """Atomically append a projection and complete only its consumer state."""

        if consumer_state_id <= 0:
            raise ValueError("consumer_state_id must be positive")
        owner = _validate_trimmed_text(lease_owner, "lease_owner", 100)
        _validate_materialization_input(materialization)
        now = _utc_now()
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            state = self._conn.execute(
                """
                SELECT state.consumer_name, state.status, state.lease_owner,
                       state.contract_version, state.normalizer_version,
                       outbox.revision_id, outbox.task_kind
                FROM outbox_consumer_state AS state
                JOIN processing_outbox AS outbox ON outbox.id = state.outbox_id
                WHERE state.id=?
                """,
                (consumer_state_id,),
            ).fetchone()
            if (
                state is None
                or str(state["status"]) != "processing"
                or str(state["lease_owner"]) != owner
            ):
                raise EventStoreError("Cannot complete an unowned consumer state")
            if str(state["consumer_name"]) != STANDARD_EVENT_CONSUMER:
                raise EventStoreError(
                    "Standard Events require the standard-event-normalizer consumer"
                )
            if str(state["task_kind"]) != "process_event":
                raise EventStoreError("Standard Events require a process_event outbox signal")
            if int(state["revision_id"]) != materialization.source_revision_id:
                raise EventStoreError(
                    "Materialization revision does not match the consumer outbox"
                )
            if (
                str(state["contract_version"]) != materialization.contract_version
                or str(state["normalizer_version"])
                != materialization.normalizer_version
            ):
                raise EventStoreError(
                    "Materialization versions do not match the consumer state"
                )
            result = self._insert_standard_event_materialization(
                materialization, now
            )
            self._mark_outbox_consumer_state_complete(
                consumer_state_id, owner, now
            )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return result

    def count_events(self) -> int:
        return int(self._conn.execute("SELECT count(*) FROM events").fetchone()[0])

    def count_revisions(self) -> int:
        return int(
            self._conn.execute("SELECT count(*) FROM event_revisions").fetchone()[0]
        )

    def count_pending_tasks(self) -> int:
        return int(
            self._conn.execute(
                "SELECT count(*) FROM processing_outbox WHERE status='pending'"
            ).fetchone()[0]
        )

    def revision_statuses(self) -> list[sqlite3.Row]:
        return self._conn.execute(
            """
            SELECT parse_status, parse_error
            FROM event_revisions ORDER BY id
            """
        ).fetchall()

    def integrity_check(self) -> str:
        return str(self._conn.execute("PRAGMA integrity_check").fetchone()[0])
