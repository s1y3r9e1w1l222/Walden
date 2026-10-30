"""Synchronous EventStore-only orchestration for Standard Event normalization."""

from __future__ import annotations

import argparse
import json
import math
import secrets
import sqlite3
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Literal

from .event_normalizer import (
    STANDARD_EVENT_SPEC_VERSION,
    WECHAT_NORMALIZER_NAME,
    NormalizerConfig,
    RawEventInput,
    RawRevisionInput,
    normalize_event,
)
from .event_store import (
    STANDARD_EVENT_CONSUMER,
    EventStore,
    EventStoreError,
    MaterializationConflict,
    MissingSourceRevision,
    OutboxConsumerState,
    StandardEventMaterializationInput,
)


CONSUMER_NAME = STANDARD_EVENT_CONSUMER
CONTRACT_VERSION = STANDARD_EVENT_SPEC_VERSION
NORMALIZER_NAME = WECHAT_NORMALIZER_NAME
NORMALIZER_VERSION = "1"

STORAGE_CONTENTION = "StorageContention"
LEASE_EXPIRED = "LeaseExpired"
LEASE_LOST = "LeaseLost"
MISSING_SOURCE_REVISION = "MissingSourceRevision"
INVALID_RAW_RECORD = "InvalidRawRecord"
MATERIALIZATION_CONFLICT = "MaterializationConflict"
PERSISTENCE_INVARIANT = "PersistenceInvariant"
WORKER_INVARIANT = "WorkerInvariant"

ItemOutcome = Literal[
    "idle",
    "normalized",
    "degraded",
    "retried",
    "failed",
    "lease_lost",
]


@dataclass(frozen=True)
class ProcessOneResult:
    outcome: ItemOutcome
    claimed: bool


@dataclass(frozen=True)
class NormalizationRunResult:
    registered: int
    claimed: int
    normalized: int
    degraded: int
    retried: int
    failed: int
    lease_lost: int
    pending: int


def _canonical_utc(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("Worker clock must return UTC-aware datetimes")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _is_storage_contention(error: BaseException) -> bool:
    if not isinstance(error, sqlite3.OperationalError):
        return False
    error_code = getattr(error, "sqlite_errorcode", None)
    if isinstance(error_code, int):
        primary_code = error_code & 0xFF
        return primary_code in {
            getattr(sqlite3, "SQLITE_BUSY", 5),
            getattr(sqlite3, "SQLITE_LOCKED", 6),
        }
    diagnostic = str(error).lower()
    return diagnostic in {"database is busy", "database is locked"} or any(
        diagnostic.startswith(prefix)
        for prefix in (
            "database schema is locked:",
            "database table is locked:",
        )
    )


class NormalizationWorker:
    """Connect I1 persistence to the pinned I2 pure normalizer profile."""

    def __init__(
        self,
        store: EventStore,
        lease_owner: str,
        *,
        lease_seconds: float = 120.0,
        retry_base_seconds: float = 1.0,
        retry_max_seconds: float = 60.0,
        poll_interval_seconds: float = 1.0,
        clock: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        if not isinstance(store, EventStore):
            raise TypeError("store must be an EventStore")
        if not lease_owner or lease_owner.strip() != lease_owner:
            raise ValueError("lease_owner must be a non-empty trimmed string")
        if len(lease_owner) > 100 or any(ord(char) < 32 for char in lease_owner):
            raise ValueError("lease_owner must be a safe string of at most 100 characters")
        timing_values = {
            "lease_seconds": float(lease_seconds),
            "retry_base_seconds": float(retry_base_seconds),
            "retry_max_seconds": float(retry_max_seconds),
            "poll_interval_seconds": float(poll_interval_seconds),
        }
        for name, value in timing_values.items():
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if (
            timing_values["retry_max_seconds"]
            < timing_values["retry_base_seconds"]
        ):
            raise ValueError("retry_max_seconds must be at least retry_base_seconds")
        self.store = store
        self.lease_owner = lease_owner
        self.lease_seconds = timing_values["lease_seconds"]
        self.retry_base_seconds = timing_values["retry_base_seconds"]
        self.retry_max_seconds = timing_values["retry_max_seconds"]
        self.poll_interval_seconds = timing_values["poll_interval_seconds"]
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._sleeper = sleeper
        self.config = NormalizerConfig(
            contract_version=CONTRACT_VERSION,
            normalizer_name=NORMALIZER_NAME,
            normalizer_version=NORMALIZER_VERSION,
        )

    def _now(self) -> datetime:
        value = self._clock()
        _canonical_utc(value)
        return value

    def _lease_expiry(self) -> str:
        return _canonical_utc(
            self._now() + timedelta(seconds=self.lease_seconds)
        )

    def _retry_available_at(self, attempt_count: int) -> str:
        max_float_doublings = (
            sys.float_info.max_exp
            - sys.float_info.min_exp
            + sys.float_info.mant_dig
        )
        doublings = min(max(attempt_count - 1, 0), max_float_doublings)
        try:
            exponential = math.ldexp(self.retry_base_seconds, doublings)
        except OverflowError:
            exponential = self.retry_max_seconds
        delay = min(exponential, self.retry_max_seconds)
        return _canonical_utc(self._now() + timedelta(seconds=delay))

    def bootstrap(self) -> int:
        return self.store.bootstrap_outbox_consumer_states(
            CONSUMER_NAME,
            CONTRACT_VERSION,
            NORMALIZER_VERSION,
        )

    def _current_state(self, state: OutboxConsumerState) -> OutboxConsumerState | None:
        try:
            return self.store.get_outbox_consumer_state(
                state.outbox_id,
                CONSUMER_NAME,
                CONTRACT_VERSION,
                NORMALIZER_VERSION,
            )
        except Exception:
            return None

    def _still_owns(self, state: OutboxConsumerState) -> bool:
        current = self._current_state(state)
        return bool(
            current is not None
            and current.id == state.id
            and current.status == "processing"
            and current.lease_owner == self.lease_owner
        )

    def _retry(self, state: OutboxConsumerState) -> ProcessOneResult:
        if not self._still_owns(state):
            return ProcessOneResult("lease_lost", True)
        try:
            self.store.retry_outbox_consumer_state(
                state.id,
                self.lease_owner,
                STORAGE_CONTENTION,
                available_at=self._retry_available_at(state.attempt_count),
            )
        except Exception as error:
            if not self._still_owns(state):
                return ProcessOneResult("lease_lost", True)
            if _is_storage_contention(error):
                return ProcessOneResult("retried", True)
            return self._fail(state, PERSISTENCE_INVARIANT)
        return ProcessOneResult("retried", True)

    def _fail(
        self,
        state: OutboxConsumerState,
        error_category: str,
    ) -> ProcessOneResult:
        if not self._still_owns(state):
            return ProcessOneResult("lease_lost", True)
        try:
            self.store.fail_outbox_consumer_state(
                state.id,
                self.lease_owner,
                error_category,
            )
        except Exception as error:
            if not self._still_owns(state):
                return ProcessOneResult("lease_lost", True)
            if _is_storage_contention(error):
                return ProcessOneResult("retried", True)
            raise EventStoreError(
                "Cannot persist terminal consumer state"
            ) from None
        return ProcessOneResult("failed", True)

    def process_one(self) -> ProcessOneResult:
        """Claim and process at most one already-registered ready state."""

        try:
            state = self.store.claim_next_outbox_consumer_state(
                CONSUMER_NAME,
                CONTRACT_VERSION,
                NORMALIZER_VERSION,
                self.lease_owner,
                self._lease_expiry(),
            )
        except Exception as error:
            if _is_storage_contention(error):
                return ProcessOneResult("retried", False)
            raise
        if state is None:
            return ProcessOneResult("idle", False)

        try:
            source = self.store.load_claimed_process_event(
                state.id, self.lease_owner
            )
        except MissingSourceRevision:
            return self._fail(state, MISSING_SOURCE_REVISION)
        except Exception as error:
            if _is_storage_contention(error):
                return self._retry(state)
            if not self._still_owns(state):
                return ProcessOneResult("lease_lost", True)
            return self._fail(state, PERSISTENCE_INVARIANT)

        try:
            raw_event = RawEventInput(
                event_uid=source.event_uid,
                account_id=source.account_id,
                source_generation=source.source_generation,
                shard_name=source.shard_name,
                table_name=source.table_name,
                local_id=source.local_id,
            )
            raw_revision = RawRevisionInput(
                content_fingerprint=source.content_fingerprint,
                revision_number=source.revision_number,
                server_id=source.server_id,
                local_type=source.local_type,
                create_time=source.create_time,
                real_sender_id=source.real_sender_id,
                raw_content=source.raw_content,
                raw_content_kind=source.raw_content_kind,
                compression_type=source.compression_type,
                observed_at=source.observed_at,
            )
            normalized = normalize_event(raw_event, raw_revision, self.config)
        except (TypeError, ValueError, UnicodeError):
            return self._fail(state, INVALID_RAW_RECORD)
        except Exception:
            return self._fail(state, WORKER_INVARIANT)

        try:
            self.store.renew_outbox_consumer_lease(
                state.id,
                self.lease_owner,
                self._lease_expiry(),
            )
        except Exception as error:
            if _is_storage_contention(error):
                return self._retry(state)
            if not self._still_owns(state):
                return ProcessOneResult("lease_lost", True)
            return self._fail(state, PERSISTENCE_INVARIANT)

        materialization = StandardEventMaterializationInput(
            source_revision_id=source.source_revision_id,
            event_id=normalized.event_id,
            subject_id=normalized.subject_id,
            contract_version=normalized.contract_version,
            normalizer_name=normalized.normalizer_name,
            normalizer_version=normalized.normalizer_version,
            canonical_payload_json=normalized.canonical_payload_json,
            payload_sha256=normalized.payload_sha256,
            quality_status=normalized.quality_status,
        )
        try:
            self.store.complete_outbox_consumer_state_with_materialization(
                state.id,
                self.lease_owner,
                materialization,
            )
        except MaterializationConflict:
            return self._fail(state, MATERIALIZATION_CONFLICT)
        except Exception as error:
            if _is_storage_contention(error):
                return self._retry(state)
            if not self._still_owns(state):
                return ProcessOneResult("lease_lost", True)
            if isinstance(error, EventStoreError):
                return self._fail(state, PERSISTENCE_INVARIANT)
            return self._fail(state, WORKER_INVARIANT)
        return ProcessOneResult(normalized.quality_status, True)

    def _pending_count(self) -> int:
        try:
            return self.store.outbox_consumer_state_counts(
                CONSUMER_NAME,
                CONTRACT_VERSION,
                NORMALIZER_VERSION,
            ).pending
        except Exception:
            return 0

    def run_once(self) -> NormalizationRunResult:
        """Bootstrap one finite snapshot and drain its currently ready states."""

        try:
            registered = self.bootstrap()
        except Exception as error:
            if _is_storage_contention(error):
                return NormalizationRunResult(
                    0, 0, 0, 0, 1, 0, 0, self._pending_count()
                )
            raise
        claimed = normalized = degraded = retried = failed = lease_lost = 0
        while True:
            item = self.process_one()
            if item.outcome == "idle":
                break
            if item.claimed:
                claimed += 1
            if item.outcome == "normalized":
                normalized += 1
            elif item.outcome == "degraded":
                degraded += 1
            elif item.outcome == "retried":
                retried += 1
                if not item.claimed:
                    break
            elif item.outcome == "failed":
                failed += 1
            elif item.outcome == "lease_lost":
                lease_lost += 1
        return NormalizationRunResult(
            registered=registered,
            claimed=claimed,
            normalized=normalized,
            degraded=degraded,
            retried=retried,
            failed=failed,
            lease_lost=lease_lost,
            pending=self._pending_count(),
        )

    def run_loop(
        self,
        emit: Callable[[NormalizationRunResult], None] | None = None,
    ) -> None:
        """Run finite normalization passes separated by an operational sleep."""

        while True:
            result = self.run_once()
            if emit is not None:
                emit(result)
            self._sleeper(self.poll_interval_seconds)


def _positive_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("value must be finite and positive")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Normalize durable private EventStore revisions"
    )
    parser.add_argument("--event-store", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--poll-interval", type=_positive_float)
    parser.add_argument("--lease-seconds", type=_positive_float, default=120.0)
    parser.add_argument(
        "--retry-base-seconds", type=_positive_float, default=1.0
    )
    parser.add_argument(
        "--retry-max-seconds", type=_positive_float, default=60.0
    )
    parser.add_argument("--json", action="store_true")
    return parser


def _emit_result(result: NormalizationRunResult, as_json: bool) -> None:
    payload = asdict(result)
    if as_json:
        print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        return
    print("NORMALIZATION_RUN OK")
    for name in (
        "registered",
        "claimed",
        "normalized",
        "degraded",
        "retried",
        "failed",
        "lease_lost",
        "pending",
    ):
        print(f"{name.upper()} {payload[name]}")


def _fatal_category(error: BaseException) -> str:
    if _is_storage_contention(error):
        return STORAGE_CONTENTION
    if isinstance(error, MissingSourceRevision):
        return MISSING_SOURCE_REVISION
    if isinstance(error, MaterializationConflict):
        return MATERIALIZATION_CONFLICT
    if isinstance(error, EventStoreError):
        return PERSISTENCE_INVARIANT
    return WORKER_INVARIANT


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    lease_owner = "normalizer-" + secrets.token_hex(16)
    try:
        with EventStore(args.event_store.resolve()) as store:
            worker = NormalizationWorker(
                store,
                lease_owner,
                lease_seconds=args.lease_seconds,
                retry_base_seconds=args.retry_base_seconds,
                retry_max_seconds=args.retry_max_seconds,
                poll_interval_seconds=args.poll_interval or 1.0,
            )
            if args.once:
                _emit_result(worker.run_once(), args.json)
                return 0
            worker.run_loop(lambda result: _emit_result(result, args.json))
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        print(
            f"NORMALIZATION_WORKER ERROR {_fatal_category(error)}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
