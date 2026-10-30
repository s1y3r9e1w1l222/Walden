"""Narrow read-only repository for frozen Standard Event materializations."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .event_normalizer import STANDARD_EVENT_SPEC_VERSION, WECHAT_NORMALIZER_NAME
from .event_store import APPLICATION_ID, SCHEMA_VERSION
from .normalization_worker import NORMALIZER_VERSION


class EventRepositoryError(RuntimeError):
    """Raised when EventStore cannot be safely read as the expected source."""


@dataclass(frozen=True)
class ProjectionProfile:
    contract_version: str = STANDARD_EVENT_SPEC_VERSION
    normalizer_name: str = WECHAT_NORMALIZER_NAME
    normalizer_version: str = NORMALIZER_VERSION
    redaction_profile: str = "p5-safe-v1"

    def __post_init__(self) -> None:
        for field_name in (
            "contract_version",
            "normalizer_name",
            "normalizer_version",
            "redaction_profile",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value or value.strip() != value:
                raise ValueError(f"{field_name} must be nonempty trimmed text")
            if any(ord(character) < 32 for character in value):
                raise ValueError(f"{field_name} contains control characters")
            if "|" in value:
                raise ValueError(f"{field_name} contains the profile delimiter")

    @property
    def profile_id(self) -> str:
        # Values are validated and the delimiter is part of this v1 profile format.
        return "|".join(
            (
                self.contract_version,
                self.normalizer_name,
                self.normalizer_version,
                self.redaction_profile,
            )
        )


class StandardEventRepository:
    """Read Standard Events without invoking EventStore's writable initializer."""

    def __init__(self, path: Path):
        resolved = path.resolve()
        if not resolved.is_file():
            raise EventRepositoryError("EventStore is unavailable")
        self.path = resolved
        self._conn = sqlite3.connect(
            f"file:{resolved}?mode=ro", uri=True, timeout=5.0
        )
        self._conn.row_factory = sqlite3.Row
        try:
            self._conn.execute("PRAGMA query_only = ON")
            self._conn.execute("PRAGMA busy_timeout = 5000")
            query_only = int(self._conn.execute("PRAGMA query_only").fetchone()[0])
            application_id = int(
                self._conn.execute("PRAGMA application_id").fetchone()[0]
            )
            schema_version = int(
                self._conn.execute("PRAGMA user_version").fetchone()[0]
            )
            if query_only != 1:
                raise EventRepositoryError("EventStore readonly protection unavailable")
            if application_id != APPLICATION_ID:
                raise EventRepositoryError("Unexpected EventStore application id")
            if schema_version != SCHEMA_VERSION:
                raise EventRepositoryError("Unsupported EventStore schema")
            required = self._conn.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type='table' AND name='standard_event_materializations'
                """
            ).fetchone()
            if required is None:
                raise EventRepositoryError("Standard Event materializations are unavailable")
        except Exception:
            self._conn.close()
            raise

    def __enter__(self) -> "StandardEventRepository":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def close(self) -> None:
        self._conn.close()

    def set_trace_callback(self, callback) -> None:
        """Expose sqlite trace only for controlled diagnostics and tests."""

        self._conn.set_trace_callback(callback)

    def batch_after(
        self,
        profile: ProjectionProfile,
        last_materialization_id: int,
        limit: int,
    ) -> list[sqlite3.Row]:
        if not isinstance(profile, ProjectionProfile):
            raise TypeError("profile must be ProjectionProfile")
        if last_materialization_id < 0:
            raise ValueError("last_materialization_id must not be negative")
        if limit < 1 or limit > 1_000:
            raise ValueError("limit must be between 1 and 1000")
        return self._conn.execute(
            """
            SELECT id, event_id, subject_id, contract_version,
                   normalizer_name, normalizer_version,
                   canonical_payload_json, payload_sha256, quality_status
            FROM standard_event_materializations
            WHERE id > ? AND contract_version=?
              AND normalizer_name=? AND normalizer_version=?
            ORDER BY id ASC
            LIMIT ?
            """,
            (
                last_materialization_id,
                profile.contract_version,
                profile.normalizer_name,
                profile.normalizer_version,
                limit,
            ),
        ).fetchall()
