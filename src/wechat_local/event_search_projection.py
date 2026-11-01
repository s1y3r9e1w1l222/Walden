"""Validated, transactional Standard Event v1 search projection."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .agent_store import AgentStore
from .event_normalizer import QUALITY_CODE_ORDER, WECHAT_PROVIDER
from .standard_event_repository import ProjectionProfile, StandardEventRepository


MAX_CITATION_CHARS = 500
MAX_SEARCHABLE_CHARS = 2_000
MAX_SOURCE_TEXT_CHARS = 100_000
MAX_STANDARD_EVENT_BYTES = 2_000_000
MAX_QUERY_CHARS = 200
MAX_TIME_RANGE_DAYS = 3_660
CURSOR_DOMAIN = b"wechat-local-p5-search-cursor-v1"
STANDARD_EVENT_KEYS = {
    "spec_version",
    "id",
    "type",
    "subject_id",
    "sequence",
    "normalization",
    "source",
    "conversation",
    "actor",
    "time",
    "content",
    "quality",
    "provenance",
}
ALLOWED_ATTRIBUTE_FIELDS = {
    "title",
    "description",
    "label",
    "memo",
    "amount_label",
    "referenced_text",
}
STANDARD_ATTRIBUTE_FIELDS = ALLOWED_ATTRIBUTE_FIELDS | {
    "duration_seconds",
    "referenced_actor_label",
}


class ProjectionError(RuntimeError):
    """Controlled invalid-materialization or projection invariant failure."""


@dataclass(frozen=True)
class ProjectionRunResult:
    read: int
    projected: int
    checkpoint: int


@dataclass(frozen=True)
class SearchPage:
    items: list[dict[str, Any]]
    next_cursor: str | None


@dataclass(frozen=True)
class ConversationPage:
    items: list[dict[str, Any]]
    next_cursor: str | None


@dataclass(frozen=True)
class _ProjectedEvent:
    materialization_id: int
    event_id: str
    subject_id: str
    conversation_id: str
    occurred_at: str
    occurred_at_fallback: int
    observed_at: str
    event_type: str
    content_kind: str
    citation_text: str
    searchable_text: str
    quality_status: str
    quality_codes_json: str
    payload_sha256: str


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _canonical_utc(value: str, field_name: str) -> str:
    if not isinstance(value, str):
        raise ProjectionError(f"{field_name} must be text")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ProjectionError(f"{field_name} must be a UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ProjectionError(f"{field_name} must be timezone-aware")
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _clean_source_text(value: Any, field_name: str) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ProjectionError(f"{field_name} must be text or null")
    if len(value) > MAX_SOURCE_TEXT_CHARS:
        raise ProjectionError(f"{field_name} exceeds source bound")
    return "".join(
        character
        for character in value.replace("\x00", "")
        if ord(character) >= 32 or character in "\n\t"
    )


def _bounded_identifier(value: Any, field_name: str, maximum: int = 500) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise ProjectionError(f"Invalid {field_name}")
    return value


def _bounded(value: str, maximum: int) -> str:
    return value[:maximum]


def _project_materialization(
    row: Any, profile: ProjectionProfile
) -> _ProjectedEvent:
    raw = row["canonical_payload_json"]
    if not isinstance(raw, str):
        raise ProjectionError("Canonical payload must be text")
    encoded_raw = raw.encode("utf-8")
    if len(encoded_raw) > MAX_STANDARD_EVENT_BYTES:
        raise ProjectionError("Canonical payload exceeds source bound")
    actual_hash = hashlib.sha256(encoded_raw).hexdigest()
    if actual_hash != str(row["payload_sha256"]):
        raise ProjectionError("Payload hash mismatch")
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ProjectionError("Malformed Standard Event JSON") from exc
    if not isinstance(payload, dict) or set(payload) != STANDARD_EVENT_KEYS:
        raise ProjectionError("Unexpected Standard Event shape")
    try:
        canonical = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ProjectionError("Invalid Standard Event values") from exc
    if canonical != raw:
        raise ProjectionError("Standard Event payload is not canonical JSON")
    if payload["id"] != row["event_id"] or payload["subject_id"] != row["subject_id"]:
        raise ProjectionError("Standard Event identity mismatch")
    _bounded_identifier(payload["id"], "event id")
    _bounded_identifier(payload["subject_id"], "subject id")
    if payload["spec_version"] != profile.contract_version:
        raise ProjectionError("Standard Event contract mismatch")
    normalization = payload["normalization"]
    if normalization != {
        "normalizer": profile.normalizer_name,
        "normalizer_version": profile.normalizer_version,
    }:
        raise ProjectionError("Standard Event normalizer mismatch")
    if (
        row["contract_version"] != profile.contract_version
        or row["normalizer_name"] != profile.normalizer_name
        or row["normalizer_version"] != profile.normalizer_version
    ):
        raise ProjectionError("Materialization profile mismatch")

    event_type = _bounded_identifier(payload["type"], "event type", 100)
    sequence = payload["sequence"]
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
        raise ProjectionError("Invalid sequence")
    source = payload["source"]
    if not isinstance(source, dict) or set(source) != {
        "provider",
        "adapter",
        "stream_id",
    }:
        raise ProjectionError("Invalid source object")
    if source["provider"] != WECHAT_PROVIDER or source["adapter"] != profile.normalizer_name:
        raise ProjectionError("Standard Event source profile mismatch")
    _bounded_identifier(source["stream_id"], "stream id")
    conversation = payload["conversation"]
    actor = payload["actor"]
    time_payload = payload["time"]
    content = payload["content"]
    quality = payload["quality"]
    if not isinstance(conversation, dict) or set(conversation) != {"id", "kind"}:
        raise ProjectionError("Invalid conversation")
    conversation_id = _bounded_identifier(conversation["id"], "conversation id")
    if conversation["kind"] != "unknown":
        raise ProjectionError("Invalid conversation kind")
    if not isinstance(actor, dict) or set(actor) != {"id", "role", "resolution"}:
        raise ProjectionError("Invalid actor object")
    if (
        actor["id"] is not None
        or actor["role"] not in ("unknown", "system")
        or actor["resolution"] != "unresolved"
    ):
        raise ProjectionError("Invalid actor fields")
    if not isinstance(time_payload, dict) or set(time_payload) != {
        "occurred_at_unix_seconds",
        "occurred_at_valid",
        "observed_at",
    }:
        raise ProjectionError("Invalid time object")
    observed_at = _canonical_utc(time_payload["observed_at"], "observed_at")
    occurred_valid = time_payload["occurred_at_valid"]
    occurred_seconds = time_payload["occurred_at_unix_seconds"]
    if not isinstance(occurred_valid, bool):
        raise ProjectionError("Invalid occurrence validity")
    if occurred_valid:
        if not isinstance(occurred_seconds, int) or isinstance(occurred_seconds, bool):
            raise ProjectionError("Invalid occurrence time")
        try:
            occurred_at = datetime.fromtimestamp(
                occurred_seconds, timezone.utc
            ).isoformat(timespec="microseconds")
        except (OverflowError, OSError, ValueError) as exc:
            raise ProjectionError("Occurrence time is out of range") from exc
        occurred_at_fallback = 0
    else:
        if occurred_seconds is not None and (
            not isinstance(occurred_seconds, int)
            or isinstance(occurred_seconds, bool)
        ):
            raise ProjectionError("Invalid occurrence fallback")
        occurred_at = observed_at
        occurred_at_fallback = 1

    if not isinstance(content, dict) or set(content) != {"kind", "text", "attributes"}:
        raise ProjectionError("Invalid content object")
    content_kind = content["kind"]
    if not isinstance(content_kind, str) or not content_kind:
        raise ProjectionError("Invalid content kind")
    text = _clean_source_text(content["text"], "content.text")
    attributes = content["attributes"]
    if not isinstance(attributes, dict) or not set(attributes).issubset(
        STANDARD_ATTRIBUTE_FIELDS
    ):
        raise ProjectionError("Invalid content attributes")
    for key, attribute in attributes.items():
        if key == "duration_seconds":
            if (
                not isinstance(attribute, int)
                or isinstance(attribute, bool)
                or attribute < 0
            ):
                raise ProjectionError("Invalid duration_seconds")
        elif not isinstance(attribute, str):
            raise ProjectionError(f"Invalid content.attributes.{key}")
        else:
            _clean_source_text(attribute, f"content.attributes.{key}")
    safe_attributes: list[str] = []
    safe_title = ""
    for key in sorted(ALLOWED_ATTRIBUTE_FIELDS):
        if key not in attributes:
            continue
        attribute = attributes[key]
        if not isinstance(attribute, str):
            continue
        cleaned = _clean_source_text(attribute, f"content.attributes.{key}")
        if key == "title":
            safe_title = cleaned
        if cleaned:
            safe_attributes.append(cleaned)

    if not isinstance(quality, dict) or set(quality) != {"status", "codes"}:
        raise ProjectionError("Invalid quality object")
    quality_status = quality["status"]
    quality_codes = quality["codes"]
    if quality_status not in ("normalized", "degraded"):
        raise ProjectionError("Invalid quality status")
    if quality_status != row["quality_status"]:
        raise ProjectionError("Quality status mismatch")
    if not isinstance(quality_codes, list) or any(
        not isinstance(code, str) or code not in QUALITY_CODE_ORDER
        for code in quality_codes
    ):
        raise ProjectionError("Invalid quality codes")
    if quality_codes != [code for code in QUALITY_CODE_ORDER if code in quality_codes]:
        raise ProjectionError("Quality codes are not canonical")

    provenance = payload["provenance"]
    if not isinstance(provenance, dict) or set(provenance) != {
        "raw_event_uid",
        "raw_revision_number",
        "raw_revision_fingerprint",
    }:
        raise ProjectionError("Invalid provenance object")
    _bounded_identifier(provenance["raw_event_uid"], "raw event uid")
    _bounded_identifier(
        provenance["raw_revision_fingerprint"], "raw revision fingerprint"
    )
    if provenance["raw_revision_number"] != sequence:
        raise ProjectionError("Provenance revision does not match sequence")

    if text:
        citation_text = _bounded(text, MAX_CITATION_CHARS)
    elif safe_title:
        citation_text = _bounded(f"{content_kind}: {safe_title}", MAX_CITATION_CHARS)
    elif content_kind == "system":
        citation_text = "系统消息内容不可用"
    else:
        citation_text = _bounded(content_kind, MAX_CITATION_CHARS)
    searchable_text = _bounded(
        " ".join(part for part in (content_kind, text, *safe_attributes) if part),
        MAX_SEARCHABLE_CHARS,
    )
    return _ProjectedEvent(
        materialization_id=int(row["id"]),
        event_id=str(payload["id"]),
        subject_id=str(payload["subject_id"]),
        conversation_id=conversation_id,
        occurred_at=occurred_at,
        occurred_at_fallback=occurred_at_fallback,
        observed_at=observed_at,
        event_type=event_type,
        content_kind=content_kind,
        citation_text=citation_text,
        searchable_text=searchable_text,
        quality_status=quality_status,
        quality_codes_json=json.dumps(
            quality_codes, ensure_ascii=False, separators=(",", ":")
        ),
        payload_sha256=actual_hash,
    )


class ProjectionWorker:
    """Project bounded Standard Event batches atomically into Agent Store."""

    def __init__(
        self,
        repository: StandardEventRepository,
        store: AgentStore,
        profile: ProjectionProfile | None = None,
    ):
        self.repository = repository
        self.store = store
        self.profile = profile or ProjectionProfile()

    def _ensure_profile(self) -> None:
        now = _utc_now()
        connection = self.store._conn
        with self.store._lock:
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    """
                    INSERT OR IGNORE INTO projection_profiles (
                        profile_id, contract_version, normalizer_name,
                        normalizer_version, redaction_profile,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self.profile.profile_id,
                        self.profile.contract_version,
                        self.profile.normalizer_name,
                        self.profile.normalizer_version,
                        self.profile.redaction_profile,
                        now,
                        now,
                    ),
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO projection_checkpoints (
                        profile_id, last_materialization_id,
                        projected_count, updated_at
                    ) VALUES (?, 0, 0, ?)
                    """,
                    (self.profile.profile_id, now),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def run_once(self, batch_size: int = 100) -> ProjectionRunResult:
        if batch_size < 1 or batch_size > 1_000:
            raise ValueError("batch_size must be between 1 and 1000")
        self._ensure_profile()
        connection = self.store._conn
        profile_id = self.profile.profile_id
        with self.store._lock:
            checkpoint = connection.execute(
                """
                SELECT last_materialization_id, projected_count
                FROM projection_checkpoints WHERE profile_id=?
                """,
                (profile_id,),
            ).fetchone()
        if checkpoint is None:
            raise ProjectionError("Projection checkpoint is unavailable")
        rows = self.repository.batch_after(
            self.profile, int(checkpoint["last_materialization_id"]), batch_size
        )
        if not rows:
            return ProjectionRunResult(
                read=0,
                projected=0,
                checkpoint=int(checkpoint["last_materialization_id"]),
            )
        projected = [_project_materialization(row, self.profile) for row in rows]
        inserted = self._write_projected(projected, advance=True)
        return ProjectionRunResult(
            read=len(rows), projected=inserted, checkpoint=projected[-1].materialization_id
        )

    def run_exact(self, materializations) -> None:
        """Project only selected materializations without moving the global checkpoint."""
        if not materializations:
            return
        self._ensure_profile()
        projected = [_project_materialization(vars(row), self.profile) for row in materializations]
        self._write_projected(projected, advance=False)

    def _write_projected(self, projected, *, advance: bool) -> int:
        connection = self.store._conn
        profile_id = self.profile.profile_id
        now = _utc_now()
        inserted = 0
        with self.store._lock:
            try:
                connection.execute("BEGIN IMMEDIATE")
                for event in projected:
                    cursor = connection.execute(
                        """
                        INSERT OR IGNORE INTO event_search_projection (
                            profile_id, materialization_id, event_id, subject_id,
                            conversation_id, occurred_at, occurred_at_fallback,
                            observed_at, event_type, content_kind, citation_text,
                            searchable_text, quality_status, quality_codes_json,
                            payload_sha256, indexed_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            profile_id,
                            event.materialization_id,
                            event.event_id,
                            event.subject_id,
                            event.conversation_id,
                            event.occurred_at,
                            event.occurred_at_fallback,
                            event.observed_at,
                            event.event_type,
                            event.content_kind,
                            event.citation_text,
                            event.searchable_text,
                            event.quality_status,
                            event.quality_codes_json,
                            event.payload_sha256,
                            now,
                        ),
                    )
                    if cursor.rowcount == 1:
                        inserted += 1
                        rowid = int(cursor.lastrowid)
                        connection.execute(
                            """
                            INSERT INTO event_search_fts (
                                rowid, profile_id,
                                materialization_id, searchable_text
                            ) VALUES (?, ?, ?, ?)
                            """,
                            (
                                rowid,
                                profile_id,
                                event.materialization_id,
                                event.searchable_text,
                            ),
                        )
                    else:
                        existing = connection.execute(
                            """
                            SELECT rowid, payload_sha256
                            FROM event_search_projection
                            WHERE profile_id=? AND materialization_id=?
                            """,
                            (profile_id, event.materialization_id),
                        ).fetchone()
                        if existing is None or existing["payload_sha256"] != event.payload_sha256:
                            raise ProjectionError("Projection identity conflict")
                        fts = connection.execute(
                            """
                            SELECT searchable_text FROM event_search_fts
                            WHERE rowid=? AND profile_id=? AND materialization_id=?
                            """,
                            (
                                int(existing["rowid"]),
                                profile_id,
                                event.materialization_id,
                            ),
                        ).fetchone()
                        if fts is None or fts["searchable_text"] != event.searchable_text:
                            raise ProjectionError("Projection FTS consistency conflict")
                if advance:
                    last_id = projected[-1].materialization_id
                    connection.execute(
                        """
                        UPDATE projection_checkpoints
                        SET last_materialization_id=?,
                            projected_count=projected_count+?, updated_at=?
                        WHERE profile_id=?
                        """,
                        (last_id, inserted, now, profile_id),
                    )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return inserted

    def rebuild(self) -> None:
        self._ensure_profile()
        connection = self.store._conn
        profile_id = self.profile.profile_id
        with self.store._lock:
            try:
                connection.execute("BEGIN IMMEDIATE")
                rowids = connection.execute(
                    """
                    SELECT rowid FROM event_search_projection WHERE profile_id=?
                    """,
                    (profile_id,),
                ).fetchall()
                if rowids:
                    connection.executemany(
                        "DELETE FROM event_search_fts WHERE rowid=?",
                        ((int(row[0]),) for row in rowids),
                    )
                connection.execute(
                    "DELETE FROM event_search_projection WHERE profile_id=?",
                    (profile_id,),
                )
                connection.execute(
                    """
                    UPDATE projection_checkpoints
                    SET last_materialization_id=0, projected_count=0, updated_at=?
                    WHERE profile_id=?
                    """,
                    (_utc_now(), profile_id),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise


def _canonical_filter_time(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    try:
        return _canonical_utc(value, field_name)
    except ProjectionError as exc:
        raise ValueError(str(exc)) from exc


def _fts_query(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip() or len(value) > MAX_QUERY_CHARS:
        raise ValueError("q must be nonempty text of at most 200 characters")
    if "\x00" in value:
        raise ValueError("q contains a forbidden control character")
    terms = [term for term in re.split(r"\s+", value.strip()) if term]
    if not terms:
        raise ValueError("q is empty")
    return " AND ".join('"' + term.replace('"', '""') + '"' for term in terms)


def _signature(value: dict[str, Any]) -> str:
    canonical = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _encode_cursor(payload: dict[str, Any]) -> str:
    serialized = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    checksum = hashlib.sha256(CURSOR_DOMAIN + serialized).hexdigest().encode("ascii")
    return base64.urlsafe_b64encode(serialized + b"." + checksum).decode("ascii").rstrip("=")


def _decode_cursor(value: str, expected_kind: str, expected_signature: str) -> dict[str, Any]:
    if not isinstance(value, str) or not value or len(value) > 2_048:
        raise ValueError("Invalid cursor")
    try:
        padded = value + "=" * (-len(value) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode("ascii"))
        serialized, checksum = decoded.rsplit(b".", 1)
        actual = hashlib.sha256(CURSOR_DOMAIN + serialized).hexdigest().encode("ascii")
        if checksum != actual:
            raise ValueError("Cursor checksum mismatch")
        payload = json.loads(serialized)
    except (ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("Invalid cursor") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("v") != 1
        or payload.get("kind") != expected_kind
        or payload.get("signature") != expected_signature
    ):
        raise ValueError("Cursor does not match this query")
    return payload


class EventSearchService:
    """Bounded deterministic read service over one projection profile."""

    def __init__(
        self, store: AgentStore, profile: ProjectionProfile | None = None
    ):
        self.store = store
        self.profile = profile or ProjectionProfile()

    def search(
        self,
        *,
        q: str | None = None,
        conversation_id: str | None = None,
        from_time: str | None = None,
        to_time: str | None = None,
        quality_status: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> SearchPage:
        if limit < 1 or limit > 100:
            raise ValueError("limit must be between 1 and 100")
        if conversation_id is not None and (
            not isinstance(conversation_id, str)
            or not conversation_id
            or len(conversation_id) > 200
        ):
            raise ValueError("Invalid conversation_id")
        if quality_status not in (None, "normalized", "degraded"):
            raise ValueError("Invalid quality_status")
        fts_query = _fts_query(q)
        from_utc = _canonical_filter_time(from_time, "from")
        to_utc = _canonical_filter_time(to_time, "to")
        if from_utc and to_utc:
            start = datetime.fromisoformat(from_utc)
            end = datetime.fromisoformat(to_utc)
            if start > end:
                raise ValueError("from must not be after to")
            if (end - start).days > MAX_TIME_RANGE_DAYS:
                raise ValueError("time range exceeds maximum")
        signature = _signature(
            {
                "profile": self.profile.profile_id,
                "q": q,
                "conversation_id": conversation_id,
                "from": from_utc,
                "to": to_utc,
                "quality_status": quality_status,
            }
        )
        cursor_payload = (
            _decode_cursor(cursor, "events", signature) if cursor else None
        )
        where = ["p.profile_id=?"]
        parameters: list[Any] = [self.profile.profile_id]
        join = ""
        if fts_query is not None:
            join = " JOIN event_search_fts ON event_search_fts.rowid=p.rowid"
            where.append("event_search_fts MATCH ?")
            parameters.append(fts_query)
        if conversation_id is not None:
            where.append("p.conversation_id=?")
            parameters.append(conversation_id)
        if from_utc is not None:
            where.append("p.occurred_at>=?")
            parameters.append(from_utc)
        if to_utc is not None:
            where.append("p.occurred_at<=?")
            parameters.append(to_utc)
        if quality_status is not None:
            where.append("p.quality_status=?")
            parameters.append(quality_status)
        if cursor_payload is not None:
            occurred_at = cursor_payload.get("occurred_at")
            materialization_id = cursor_payload.get("materialization_id")
            if not isinstance(occurred_at, str) or not isinstance(materialization_id, int):
                raise ValueError("Invalid cursor")
            where.append(
                "(p.occurred_at < ? OR "
                "(p.occurred_at = ? AND p.materialization_id < ?))"
            )
            parameters.extend((occurred_at, occurred_at, materialization_id))
        parameters.append(limit + 1)
        with self.store._lock:
            rows = self.store._conn.execute(
                """
                SELECT p.event_id, p.subject_id, p.conversation_id,
                       p.occurred_at, p.occurred_at_fallback,
                       p.quality_status, p.citation_text,
                       p.content_kind, p.event_type, p.materialization_id
                FROM event_search_projection p
                """
                + join
                + " WHERE "
                + " AND ".join(where)
                + " ORDER BY p.occurred_at DESC, p.materialization_id DESC LIMIT ?",
                parameters,
            ).fetchall()
        has_more = len(rows) > limit
        visible = rows[:limit]
        items = [self._event_ref(row) for row in visible]
        next_cursor = None
        if has_more and visible:
            last = visible[-1]
            next_cursor = _encode_cursor(
                {
                    "v": 1,
                    "kind": "events",
                    "signature": signature,
                    "occurred_at": last["occurred_at"],
                    "materialization_id": int(last["materialization_id"]),
                }
            )
        return SearchPage(items=items, next_cursor=next_cursor)

    def get_event(self, event_id: str) -> dict[str, Any] | None:
        if not isinstance(event_id, str) or not event_id or len(event_id) > 200:
            raise ValueError("Invalid event_id")
        with self.store._lock:
            row = self.store._conn.execute(
                """
                SELECT event_id, subject_id, conversation_id, occurred_at,
                       occurred_at_fallback, quality_status, citation_text,
                       content_kind, event_type, materialization_id
                FROM event_search_projection
                WHERE profile_id=? AND event_id=?
                ORDER BY materialization_id DESC LIMIT 1
                """,
                (self.profile.profile_id, event_id),
            ).fetchone()
        return self._event_ref(row) if row is not None else None

    def conversations(
        self,
        *,
        q: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> ConversationPage:
        if limit < 1 or limit > 100:
            raise ValueError("limit must be between 1 and 100")
        if q is not None and (
            not isinstance(q, str) or not q.strip() or len(q) > MAX_QUERY_CHARS
        ):
            raise ValueError("Invalid conversation query")
        signature = _signature(
            {"profile": self.profile.profile_id, "q": q}
        )
        cursor_payload = (
            _decode_cursor(cursor, "conversations", signature) if cursor else None
        )
        where = ["profile_id=?"]
        parameters: list[Any] = [self.profile.profile_id]
        if q is not None:
            where.append("conversation_id LIKE ? ESCAPE '\\'")
            escaped = q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            parameters.append(f"%{escaped}%")
        aggregate = (
            "SELECT conversation_id, count(*) AS event_count, "
            "min(occurred_at) AS first_at, max(occurred_at) AS last_at "
            "FROM event_search_projection WHERE "
            + " AND ".join(where)
            + " GROUP BY conversation_id"
        )
        outer_where = []
        if cursor_payload is not None:
            last_at = cursor_payload.get("last_at")
            conversation_id = cursor_payload.get("conversation_id")
            if not isinstance(last_at, str) or not isinstance(conversation_id, str):
                raise ValueError("Invalid cursor")
            outer_where.append(
                "(last_at < ? OR (last_at = ? AND conversation_id < ?))"
            )
            parameters.extend((last_at, last_at, conversation_id))
        sql = "SELECT * FROM (" + aggregate + ")"
        if outer_where:
            sql += " WHERE " + " AND ".join(outer_where)
        sql += " ORDER BY last_at DESC, conversation_id DESC LIMIT ?"
        parameters.append(limit + 1)
        with self.store._lock:
            rows = self.store._conn.execute(sql, parameters).fetchall()
        has_more = len(rows) > limit
        visible = rows[:limit]
        items = [dict(row) for row in visible]
        next_cursor = None
        if has_more and visible:
            last = visible[-1]
            next_cursor = _encode_cursor(
                {
                    "v": 1,
                    "kind": "conversations",
                    "signature": signature,
                    "last_at": last["last_at"],
                    "conversation_id": last["conversation_id"],
                }
            )
        return ConversationPage(items=items, next_cursor=next_cursor)

    @staticmethod
    def _event_ref(row: Any) -> dict[str, Any]:
        return {
            "event_id": str(row["event_id"]),
            "subject_id": str(row["subject_id"]),
            "conversation_id": str(row["conversation_id"]),
            "occurred_at": str(row["occurred_at"]),
            "occurred_at_source": (
                "observed_fallback" if int(row["occurred_at_fallback"]) else "source"
            ),
            "quality_status": str(row["quality_status"]),
            "citation_text": str(row["citation_text"]),
            "content_kind": str(row["content_kind"]),
            "event_type": str(row["event_type"]),
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the private P5-I1 Standard Event search projection"
    )
    parser.add_argument("--event-store", type=Path, required=True)
    parser.add_argument("--agent-store", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.event_store.resolve() == args.agent_store.resolve():
        raise SystemExit("EventStore and Agent Store must be separate files")
    total_read = 0
    total_projected = 0
    checkpoint = 0
    with StandardEventRepository(args.event_store) as repository:
        with AgentStore(args.agent_store) as store:
            worker = ProjectionWorker(repository, store)
            if args.rebuild:
                worker.rebuild()
            while True:
                result = worker.run_once(args.batch_size)
                total_read += result.read
                total_projected += result.projected
                checkpoint = result.checkpoint
                if args.once or result.read == 0:
                    break
    payload = {
        "status": "ok",
        "read": total_read,
        "projected": total_projected,
        "checkpoint": checkpoint,
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    else:
        print("AGENT_INDEX_BUILD OK")
        print(f"READ {total_read}")
        print(f"PROJECTED {total_projected}")
        print(f"CHECKPOINT {checkpoint}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
