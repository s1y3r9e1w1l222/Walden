"""One-shot, fail-closed Codex-first live-window orchestration.

The runtime composes the frozen read, EventStore, normalization, projection,
and EventRef services.  It deliberately has no provider, action, clipboard,
web, sender, or background-loop dependency.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import sqlite3
import stat
import sys
import threading
from contextlib import nullcontext
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Optional
from zoneinfo import ZoneInfo

_WINDOW_SHARD_WORKERS = 2

from .agent_store import AgentStore, AgentStoreError
from .attachment_resolver import AttachmentResolver
from .codex_first import (
    CodexFirstError,
    CodexFirstService,
    ScanCoverage,
    ScanResult,
    TimeWindow,
    QueryRunAnchor,
    parse_time_window,
)
from .event_search_projection import (
    EventSearchService,
    ProjectionWorker,
    ProjectionError,
    SearchPage,
    _project_materialization,
)
from .event_store import (
    EventStore,
    EventStoreError,
    RawMessage,
    SourceKey,
    WindowIngestRun,
    _event_uid,
    _content_fingerprint,
    _materialization_from_row,
)
from .live_catalog import LiveCatalog, LiveCatalogError, discover_numeric_shards
from .live_identity import LiveIdentityAdapter, _username_for_table
from .live_readonly import (
    LiveReadonlyError,
    open_encrypted_readonly,
    read_private_passphrase,
    readonly_database_uri,
    validate_live_storage_paths,
)
from .local_voice_runtime import LocalWhisperTranscriber
from .live_tailer import (WindowReadResult, _empty_window_probe, read_message_window,
                          SOURCE_TIMESTAMP_LOWER_BOUND, SOURCE_TIMESTAMP_UPPER_BOUND)
from .normalization_worker import NormalizationWorker
from .standard_event_repository import ProjectionProfile, StandardEventRepository
from .voice_transcript import VoiceMessageBinding, VoiceTranscriptEnricher
from .source_setup import SourceSetupError, apply_source_config_to_args


MAX_CAPABILITY_SHARDS = 64
MAX_CAPABILITY_TABLES_PER_SHARD = 512
LARGE_QUERY_SECONDS = 24 * 60 * 60
PREFLIGHT_SAMPLE_LIMIT = 10_001


class ContactQueryError(CodexFirstError):
    """A fixed, content-free contact-resolution failure."""


class _ContactOnlyCatalog:
    shards = ()


def _inclusive_source_seconds(window: TimeWindow) -> tuple[int, int]:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    start = window.start.astimezone(timezone.utc) - epoch
    end = window.end.astimezone(timezone.utc) - epoch
    first = start.days * 86400 + start.seconds + bool(start.microseconds)
    last = end.days * 86400 + end.seconds
    return int(first), int(last)


def _diagnostic_reason(error: Exception) -> str:
    if isinstance(error, PermissionError):
        return "permission_denied"
    if isinstance(error, FileNotFoundError):
        return "file_not_found"
    if isinstance(error, SourceSetupError):
        return "configuration_error"
    if isinstance(error, LiveReadonlyError):
        return "read_only_source_error"
    if isinstance(error, EventStoreError):
        return "event_store_error"
    if isinstance(error, AgentStoreError):
        return "agent_store_error"
    if isinstance(error, LiveCatalogError):
        return "catalog_error"
    if isinstance(error, sqlite3.Error) or error.__class__.__module__ == "sqlcipher3.dbapi2":
        return "database_error"
    if isinstance(error, ValueError):
        return "invalid_value"
    if isinstance(error, OSError):
        return "io_error"
    return "unknown"


InboundMessage = Callable[[SourceKey, RawMessage], bool]
InboundEventRef = Callable[[Mapping[str, Any]], bool]
DisplayName = Callable[[str], Optional[str]]
CapabilityCheck = Callable[[LiveCatalog], "SourceCapabilityEvidence"]
BaseMessagesCallback = Callable[[ScanResult], None]
VoiceResultCallback = Callable[[str, Mapping[str, Any]], None]
ImageResultCallback = Callable[[str, Mapping[str, Any]], None]
ImageEnricher = Callable[[SourceKey, RawMessage, str], Mapping[str, Any]]
DirectionForMessage = Callable[[SourceKey, RawMessage], str]
ConversationKind = Callable[[str], str]
IncludeSource = Callable[[SourceKey], bool]
SenderIdentity = Callable[[SourceKey, RawMessage], Optional[str]]
SenderDisplayName = Callable[[SourceKey, RawMessage], Optional[str]]
SourceFacts = Callable[[SourceKey, RawMessage], Mapping[str, Any]]
SourceFilter = Callable[[SourceKey], bool]
ConversationIdentity = Callable[[SourceKey], Optional[str]]


@dataclass(frozen=True)
class SourceCapabilityEvidence:
    """Explicit proof adapters required before rendering a who-contacted view.

    The booleans alone are insufficient: callers must provide both direction
    classifiers and a display-name resolver.  This prevents a partially wired
    integration from silently treating an unresolved projection as proven.
    """

    direction_proven: bool = False
    contact_mapping_proven: bool = False
    inbound_message: InboundMessage | None = None
    inbound_event_ref: InboundEventRef | None = None
    display_name: DisplayName | None = None
    direction_for_message: DirectionForMessage | None = None
    conversation_kind: ConversationKind | None = None
    include_source: IncludeSource | None = None
    sender_identity: SenderIdentity | None = None
    sender_display_name: SenderDisplayName | None = None
    source_facts: SourceFacts | None = None
    conversation_identity_for_source: ConversationIdentity | None = None

    @property
    def available(self) -> bool:
        return bool(
            self.direction_proven
            and self.contact_mapping_proven
            and callable(self.display_name)
            and (
                callable(self.direction_for_message)
                or (callable(self.inbound_message) and callable(self.inbound_event_ref))
            )
        )


@dataclass(frozen=True)
class QueryPreflight:
    """Content-free estimate and confirmation boundary for a large query."""

    status: str
    estimated_rows: int
    available_bytes: int
    confirmation_token: str | None = None


def inspect_source_capabilities(catalog: LiveCatalog) -> SourceCapabilityEvidence:
    """Run a bounded, schema-only check and fail closed for today's schema.

    Frozen Standard Event v1 does not expose reliable message direction or a
    contact/group display mapping.  Inspecting table metadata cannot establish
    those semantics, so the safe production answer is unavailable.  The
    bounded inspection is retained as an explicit gate for a future, separately
    validated adapter and never selects message/contact rows.
    """

    for shard in catalog.shards[:MAX_CAPABILITY_SHARDS]:
        rows = shard.connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name GLOB 'Msg_*'
            ORDER BY name LIMIT ?
            """,
            (MAX_CAPABILITY_TABLES_PER_SHARD,),
        ).fetchall()
        for (table_name,) in rows:
            # LiveCatalog already validated identifiers.  This second bounded
            # metadata observation proves only shape, never field semantics.
            shard.connection.execute(
                'PRAGMA table_info("' + str(table_name).replace('"', '""') + '")'
            ).fetchall()
    return SourceCapabilityEvidence()


class CodexFirstRuntime:
    """Compose one finite, cursor-free source-window scan."""

    def __init__(
        self,
        catalog: LiveCatalog,
        event_store: EventStore,
        agent_store: AgentStore,
        *,
        account_id: str,
        source_generation: str,
        capability_check: CapabilityCheck = inspect_source_capabilities,
        voice_enricher: VoiceTranscriptEnricher | None = None,
        image_enricher: ImageEnricher | None = None,
        attachment_resolver: AttachmentResolver | None = None,
        source_filter: SourceFilter | None = None,
        batch_size: int = 500,
        projection_batch_size: int = 100,
        voice_sources_refresh: Callable[[], None] | None = None,
        scan_start: Callable[[], None] | None = None,
        _locate_only: bool = False,
    ):
        if not isinstance(catalog, LiveCatalog):
            raise TypeError("catalog must be a LiveCatalog")
        if not isinstance(event_store, EventStore):
            raise TypeError("event_store must be an EventStore")
        if not (_locate_only and agent_store is None) and not isinstance(agent_store, AgentStore):
            raise TypeError("agent_store must be an AgentStore")
        SourceKey(
            account_id,
            source_generation,
            "message_0.db",
            "Msg_00000000000000000000000000000000",
        )
        if not callable(capability_check):
            raise TypeError("capability_check must be callable")
        if voice_enricher is not None and not isinstance(
            voice_enricher, VoiceTranscriptEnricher
        ):
            raise TypeError("voice_enricher must be a VoiceTranscriptEnricher")
        if batch_size < 1 or batch_size > 10_000:
            raise ValueError("batch_size must be between 1 and 10000")
        if projection_batch_size < 1 or projection_batch_size > 1_000:
            raise ValueError("projection_batch_size must be between 1 and 1000")
        self.catalog = catalog
        self.event_store = event_store
        self.agent_store = agent_store
        self.account_id = account_id
        self.source_generation = source_generation
        self.capability_check = capability_check
        self.voice_enricher = voice_enricher
        self.image_enricher = image_enricher
        self.attachment_resolver = attachment_resolver
        self.source_filter = source_filter
        self.batch_size = batch_size
        self.projection_batch_size = projection_batch_size
        self.voice_sources_refresh = voice_sources_refresh
        self.scan_start = scan_start
        self._contact_target: str | None = None
        self._conversation_target: str | None = None
        self._contact_related = False
        self._contact_unknown_sources: set[SourceKey] = set()
        self._contact_guard: Callable[[], None] | None = None
        self._contact_group_source = None
        self._scope_senders: dict[SourceKey, int] = {}
        self._scope_private: set[SourceKey] = set()
        self._scope_allowed: set[SourceKey] | None = None
        self._scope_epochs = {}
        self._scope_gaps: list[str] = []
        self._contact_binding = None
        self._query_failures: list[dict[str, str]] = []
        self._request_preflight: QueryPreflight | None = None
        self._request_preflight_state: tuple | None = None
        self._request_preflight_window: TimeWindow | None = None
        self._bound_shards: dict[int, tuple[int, int]] = {}
        # Only the public encrypted-catalog path opts into exclusive handoff.
        self._scan_workers = 1
        self._empty_window_enabled = False
        self._latest_ids = None
        self._latest_source_state = None
        for shard in catalog.shards:
            try:
                info = shard.path.lstat()
                self._bound_shards[id(shard.connection)] = (info.st_dev, info.st_ino)
            except OSError:
                pass

    def _source_state(self) -> tuple | None:
        """Identify current shard and WAL objects for this invocation only."""
        try:
            root = self.catalog._live_root
            paths = discover_numeric_shards(root) if root is not None else tuple(shard.path for shard in self.catalog.shards)
            state = []
            for path in paths:
                values = []
                for item in (path, Path(str(path) + '-wal')):
                    try:
                        info = item.lstat()
                    except FileNotFoundError:
                        values.append(None)
                        continue
                    values.append((info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns))
                state.append((str(path), tuple(values)))
            return tuple(state)
        except (OSError, LiveCatalogError):
            return None

    def _voice_epoch(self, shard: Any) -> tuple | None:
        try:
            version = int(shard.connection.execute('PRAGMA data_version').fetchone()[0])
            stamps = []
            for path in (shard.path, Path(str(shard.path) + '-wal')):
                try:
                    info = path.lstat()
                except FileNotFoundError:
                    stamps.append(None)
                    continue
                if not stat.S_ISREG(info.st_mode):
                    return None
                stamps.append((info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns))
            if stamps[0] is None:
                return None
            identity = stamps[0][:2]
            initial = self._bound_shards.setdefault(id(shard.connection), identity)
            if initial != identity:
                return None
            return (id(shard.connection), version, tuple(stamps))
        except Exception:
            return None

    def _ensure_latest_source_state(self) -> None:
        if self._latest_source_state is None or self._source_state() != self._latest_source_state:
            raise ContactQueryError('source_changed')

    def _parallel_window_reads(self, sources, included, start, end):
        """Read per physical shard, with no main-thread connection access until join."""
        groups = []
        for source in sources:
            if not included[source]:
                continue
            shard = self.catalog.shard_for_source(source)
            if shard is None:
                continue
            try:
                info = shard.path.lstat()
                identity = (info.st_dev, info.st_ino)
            except OSError:
                identity = None
            matches = [group for group in groups if identity in group['identities'] or id(shard.connection) in group['connections']]
            if matches:
                group = matches[0]
                for other in matches[1:]:
                    group['identities'].update(other['identities'])
                    group['connections'].update(other['connections'])
                    group['items'].extend(other['items'])
                    groups.remove(other)
            else:
                group = {'identities': set(), 'connections': set(), 'items': []}
                groups.append(group)
            group['identities'].add(identity)
            group['connections'].add(id(shard.connection))
            group['items'].append((source, shard))

        # A missing identity cannot be proved distinct from any other shard.
        # Serialize this invocation instead of assuming it is a different file.
        if any(None in group['identities'] for group in groups):
            items = [(source, shard) for group in groups for source, shard in group['items']]
            groups = [{'items': items}]

        cancelled = threading.Event()
        def read_group(group):
            result = {}
            before_by_connection = {}
            if self._empty_window_enabled:
                for _, shard in group['items']:
                    identity = id(shard.connection)
                    if identity not in before_by_connection:
                        before_by_connection[identity] = self._voice_epoch(shard)
            for source, shard in group['items']:
                if cancelled.is_set():
                    break
                scoped_transaction = False
                try:
                    if source in self._scope_senders or source in self._scope_private:
                        shard.connection.execute('BEGIN')
                        scoped_transaction = True
                        if source in self._scope_private:
                            if _username_for_table(shard.connection, source.table_name) != (self._contact_target or self._conversation_target):
                                raise LiveReadonlyError('Private conversation identity changed')
                        else:
                            row = shard.connection.execute('SELECT user_name FROM Name2Id WHERE rowid=?', (self._scope_senders[source],)).fetchone()
                            if row is None or row[0] != self._contact_target:
                                raise LiveReadonlyError('Related sender identity changed')
                    scoped_epoch = self._scope_epochs.get(id(shard.connection))
                    if scoped_epoch is not None and self._voice_epoch(shard) != scoped_epoch[1]:
                        raise LiveReadonlyError('Live message source changed')
                    before = self._voice_epoch(shard) if self.voice_enricher is not None else None
                    can_prove = self._empty_window_enabled and self._baseline_metadata_scan and before_by_connection.get(id(shard.connection)) is not None
                    empty, no_future = _empty_window_probe(shard.connection, source.table_name, start=start, end=end) if can_prove else (False, False)
                    if cancelled.is_set():
                        break
                    if self._latest_ids is not None:
                        self._ensure_latest_source_state()
                    selected_ids = self._latest_ids.get(source) if self._latest_ids is not None else None
                    read = (WindowReadResult([], None, None, 0, None) if empty or selected_ids == () else
                        read_message_window(
                            shard.connection, source.table_name,
                            start_unix_seconds=start, end_unix_seconds=end,
                            batch_size=self.batch_size, _cancelled=cancelled.is_set,
                            **({'_sender_id': self._scope_senders[source]} if source in self._scope_senders else {}),
                            **({'_local_ids': selected_ids} if selected_ids is not None else {}),
                            **({'collect_voice_packed': True} if self.voice_enricher is not None else {}),
                        ))
                    epoch = before if before is not None and before == self._voice_epoch(shard) else None
                    result[source] = (read, epoch, None, no_future, empty)
                except Exception as error:
                    result[source] = (None, None, error, False, False)
                finally:
                    if scoped_transaction:
                        shard.connection.rollback()
            if self._empty_window_enabled:
                stable = {}
                for _, shard in group['items']:
                    identity = id(shard.connection)
                    if identity not in stable:
                        before = before_by_connection.get(identity)
                        stable[identity] = before if before is not None and before == self._voice_epoch(shard) else None
                for source, shard in group['items']:
                    if source in result:
                        read, epoch, error, no_future, empty = result[source]
                        result[source] = (read, epoch, error, no_future, stable[id(shard.connection)], empty)
            return result

        if self._scan_workers == 1:
            combined = {}
            for group in groups:
                combined.update(read_group(group))
            return combined

        executor = ThreadPoolExecutor(max_workers=self._scan_workers)
        futures = []
        def stop_workers():
            cancelled.set()
            for future in futures:
                future.cancel()
            for shard in self.catalog.shards:
                try:
                    shard.connection.interrupt()
                except Exception:
                    pass
        try:
            futures = [executor.submit(read_group, group) for group in groups]
            results = {}
            for future in futures:
                results.update(future.result())
            return results
        except BaseException:
            stop_workers()
            raise
        finally:
            interrupted = False
            while True:
                try:
                    executor.shutdown(wait=True)
                    break
                except KeyboardInterrupt:
                    interrupted = True
                    stop_workers()
            if interrupted:
                raise KeyboardInterrupt

    def output_preflight(self, window: TimeWindow) -> QueryPreflight | None:
        if any(self._voice_epoch(shard) is None for shard in self.catalog.shards):
            raise LiveReadonlyError("Live message source changed")
        if (window.end - window.start).total_seconds() <= LARGE_QUERY_SECONDS:
            self.catalog.refresh_catalog()
            return None
        state = self._source_state()
        if (state is not None and state == self._request_preflight_state
                and window == self._request_preflight_window and self._request_preflight is not None):
            return self._request_preflight
        return self.preflight(window)

    @staticmethod
    def _coverage(
        window: TimeWindow,
        minimums: list[int],
        maximums: list[int],
        gaps: list[str],
        *,
        capability: str = "available",
    ) -> ScanCoverage:
        actual_start = (
            datetime.fromtimestamp(min(minimums), timezone.utc) if minimums else None
        )
        actual_end = (
            datetime.fromtimestamp(max(maximums), timezone.utc) if maximums else None
        )
        return ScanCoverage(
            requested_start=window.start,
            requested_end=window.end,
            actual_start=actual_start,
            actual_end=actual_end,
            gaps=tuple(dict.fromkeys(gaps)),
            source_capability=capability,
        )

    def _sources(self) -> list[SourceKey]:
        return [
            source
            for source in self.event_store.source_catalog_sources()
            if source.account_id == self.account_id
            and source.source_generation == self.source_generation
            and self.catalog.shard_for_source(source) is not None
            and (self.source_filter is None or self.source_filter(source))
            and (self._scope_allowed is None or source in self._scope_allowed)
        ]

    def _prepare_contact_scope(self, window: TimeWindow) -> None:
        self._scope_allowed = None
        self._scope_senders = {}
        self._scope_private = set()
        self._scope_epochs = {}
        self._scope_gaps = []
        if self._contact_target is None and self._conversation_target is None:
            return
        self.catalog.refresh_catalog()
        sources = self._sources()
        allowed = set()
        by_connection = {}
        for source in sources:
            if self._conversation_target is not None or not self._contact_group_source(source):
                shard = self.catalog.shard_for_source(source)
                if shard is None:
                    self._scope_gaps.append('target_private_source_unavailable')
                    continue
                before = self._voice_epoch(shard)
                username = _username_for_table(shard.connection, source.table_name)
                if before is None or self._voice_epoch(shard) != before:
                    raise LiveReadonlyError('Live message source changed')
                if username != (self._contact_target or self._conversation_target):
                    self._scope_gaps.append('target_private_source_unavailable')
                    continue
                existing = self._scope_epochs.get(id(shard.connection))
                if existing is not None and existing[1] != before:
                    raise ContactQueryError('source_changed')
                self._scope_epochs[id(shard.connection)] = (shard, before)
                self._scope_private.add(source)
                allowed.add(source)
                continue
            shard = self.catalog.shard_for_source(source)
            if shard is None:
                self._scope_gaps.append('related_sender_unknown')
                continue
            connection = shard.connection
            key = id(connection)
            if key not in by_connection:
                before = self._voice_epoch(shard)
                try:
                    rows = connection.execute('SELECT rowid,user_name FROM Name2Id WHERE user_name=? LIMIT 2', (self._contact_target,)).fetchall()
                    sender = int(rows[0][0]) if len(rows) == 1 and rows[0][1] == self._contact_target and type(rows[0][0]) is int else None
                except Exception:
                    sender = None
                after = self._voice_epoch(shard)
                if before is None or before != after:
                    raise LiveReadonlyError('Live message source changed')
                existing = self._scope_epochs.get(key)
                if existing is not None and existing[1] != before:
                    raise LiveReadonlyError('Live message source changed')
                by_connection[key] = sender
                self._scope_epochs[key] = (shard, before)
            sender = by_connection[key]
            if sender is None:
                self._scope_gaps.append('related_sender_unknown')
                continue
            quoted = '"' + source.table_name.replace('"', '""') + '"'
            params = _inclusive_source_seconds(window)
            try:
                unknown = connection.execute(
                    f"SELECT 1 FROM {quoted} AS msg WHERE typeof(create_time)='integer' AND create_time>={SOURCE_TIMESTAMP_LOWER_BOUND} AND create_time<{SOURCE_TIMESTAMP_UPPER_BOUND} AND create_time>=? AND create_time<=? AND (real_sender_id IS NULL OR typeof(real_sender_id)<>'integer' OR NOT EXISTS (SELECT 1 FROM Name2Id AS sender WHERE sender.rowid=msg.real_sender_id AND typeof(sender.user_name)='text' AND sender.user_name<>'')) LIMIT 1", params).fetchone()
                found = connection.execute(
                    f"SELECT 1 FROM {quoted} WHERE typeof(create_time)='integer' AND create_time>={SOURCE_TIMESTAMP_LOWER_BOUND} AND create_time<{SOURCE_TIMESTAMP_UPPER_BOUND} AND create_time>=? AND create_time<=? AND typeof(real_sender_id)='integer' AND real_sender_id=? LIMIT 1", (*params,sender)).fetchone()
            except Exception as exc:
                raise LiveReadonlyError('Related sender metadata unavailable') from exc
            if unknown:
                self._scope_gaps.append('related_sender_unknown')
            if found:
                allowed.add(source)
                self._scope_senders[source] = sender
        if any(self._voice_epoch(shard) != epoch for shard,epoch in self._scope_epochs.values()):
            raise LiveReadonlyError('Live message source changed')
        self._scope_allowed = allowed

    def _latest_candidates(self, sources, limit):
        """Select bounded metadata under each source's pinned identity snapshot."""
        candidates = []
        for source in sources:
            shard = self.catalog.shard_for_source(source)
            if shard is None or source not in self._scope_private:
                raise ContactQueryError('latest_source_unavailable')
            epoch = self._scope_epochs.get(id(shard.connection))
            if epoch is None or self._voice_epoch(shard) != epoch[1]:
                raise ContactQueryError('source_changed')
            connection = shard.connection
            connection.execute('BEGIN')
            try:
                if _username_for_table(connection, source.table_name) != self._contact_target:
                    raise ContactQueryError('source_changed')
                if self._voice_epoch(shard) != epoch[1]:
                    raise ContactQueryError('source_changed')
                quoted = '"' + source.table_name.replace('"', '""') + '"'
                invalid = connection.execute(
                    f"SELECT 1 FROM {quoted} WHERE typeof(local_id)<>'integer' OR typeof(create_time)<>'integer' OR create_time < {SOURCE_TIMESTAMP_LOWER_BOUND} OR create_time >= {SOURCE_TIMESTAMP_UPPER_BOUND} LIMIT 1").fetchone()
                if invalid:
                    raise ContactQueryError('latest_order_unavailable')
                rows = connection.execute(
                    f'SELECT local_id,create_time FROM {quoted} ORDER BY create_time DESC,local_id DESC LIMIT ?',
                    (limit,)).fetchall()
                if any(type(row[0]) is not int or type(row[1]) is not int for row in rows):
                    raise ContactQueryError('latest_order_unavailable')
                candidates.extend((timestamp, source.shard_name, source.table_name, local_id, source)
                                  for local_id, timestamp in rows)
            finally:
                connection.rollback()
            if self._voice_epoch(shard) != epoch[1]:
                raise ContactQueryError('source_changed')
        candidates.sort(key=lambda item: item[:4], reverse=True)
        selected = candidates[:limit]
        ids = {source: [] for source in sources}
        for _, _, _, local_id, source in selected:
            ids[source].append(local_id)
        if any(len(set(values)) != len(values) for values in ids.values()):
            raise ContactQueryError('latest_source_unavailable')
        return {source: tuple(values) for source, values in ids.items()}, selected

    def preflight(self, window: TimeWindow) -> QueryPreflight:
        if not isinstance(window, TimeWindow):
            raise TypeError("window must be a TimeWindow")
        self.catalog.refresh_catalog()
        sources = self._sources()
        estimated_rows = 0
        start_seconds, end_seconds = _inclusive_source_seconds(window)
        for source in sources:
            shard = self.catalog.shard_for_source(source)
            if shard is not None:
                # This is a bounded lower-bound sample for the requested
                # window, not a whole-history count or coverage proof.
                quoted = '"' + source.table_name.replace('"', '""') + '"'
                sender = self._scope_senders.get(source)
                predicate = " AND typeof(real_sender_id)='integer' AND real_sender_id=?" if sender is not None else ''
                parameters = ((start_seconds, end_seconds, sender, PREFLIGHT_SAMPLE_LIMIT) if sender is not None else
                              (start_seconds, end_seconds, PREFLIGHT_SAMPLE_LIMIT))
                estimated_rows += len(shard.connection.execute(
                    f"SELECT 1 FROM {quoted} WHERE typeof(create_time)='integer' AND create_time>={SOURCE_TIMESTAMP_LOWER_BOUND} AND create_time<{SOURCE_TIMESTAMP_UPPER_BOUND} "
                    f"AND create_time >= ? AND create_time <= ?{predicate} LIMIT ?",
                    parameters,
                ).fetchall())
        if any(self._voice_epoch(shard) != epoch for shard,epoch in self._scope_epochs.values()):
            raise LiveReadonlyError('Live message source changed')
        available_bytes = os.statvfs(self.event_store.path.parent).f_bavail * os.statvfs(self.event_store.path.parent).f_frsize
        if (window.end - window.start).total_seconds() <= LARGE_QUERY_SECONDS:
            return QueryPreflight("preflight", estimated_rows, available_bytes)
        token_material = "|".join((window.start.isoformat(), window.end.isoformat(), str(estimated_rows)))
        if self._contact_target is not None or self._conversation_target is not None:
            token_material += '|' + json.dumps([self._contact_target, self._contact_related, self._conversation_target], ensure_ascii=True, separators=(',', ':'))
            scope = sorted((source.shard_name, source.table_name, sender)
                           for source, sender in self._scope_senders.items())
            whole = sorted((source.shard_name, source.table_name) for source in self._scope_private)
            stamps = sorted((str(shard.path), epoch[2]) for shard, epoch in self._scope_epochs.values())
            token_material += '|' + json.dumps([scope, whole, stamps, self._contact_binding], ensure_ascii=True, separators=(',', ':'))
        return QueryPreflight(
            "awaiting_confirmation", estimated_rows, available_bytes,
            hashlib.sha256(token_material.encode("utf-8")).hexdigest(),
        )

    def search(
        self,
        window: TimeWindow,
        *,
        query: str,
        conversation_id: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> SearchPage:
        """Run deterministic FTS only inside an explicit, bounded window."""
        if not isinstance(window, TimeWindow):
            raise TypeError("window must be a TimeWindow")
        if not isinstance(query, str) or not query.strip():
            raise CodexFirstError("query must be nonempty text")
        service = EventSearchService(self.agent_store)
        before = self._association_store_epoch()
        if before is None:
            raise CodexFirstError('conversation_attribution_unavailable')
        page = service.search(
            q=query,
            conversation_id=conversation_id,
            from_time=window.start.isoformat(timespec="microseconds"),
            to_time=window.end.isoformat(timespec="microseconds"),
            limit=limit,
            cursor=cursor,
        )
        try:
            candidates = []
            for item in page.items:
                rows = self.event_store._conn.execute(
                    'SELECT m.*,e.account_id,e.source_generation,e.shard_name,e.table_name,e.local_id, '
                    'r.server_id,r.local_type,r.create_time,r.real_sender_id,r.content_fingerprint '
                    'FROM standard_event_materializations m JOIN event_revisions r ON r.id=m.source_revision_id '
                    'JOIN events e ON e.id=r.event_id AND e.current_revision_id=r.id '
                    'WHERE m.event_id=? AND e.account_id=? AND e.source_generation=?',
                    (item['event_id'], self.account_id, self.source_generation),
                ).fetchall()
                if len(rows) != 1:
                    raise EventStoreError('association_materialization_unavailable')
                row = rows[0]
                expected = _project_materialization(vars(_materialization_from_row(row)), service.profile)
                if any(item[name] != getattr(expected, name) for name in ('event_id','subject_id','conversation_id','occurred_at','quality_status','citation_text','content_kind','event_type')) or item['occurred_at_source'] != ('observed_fallback' if expected.occurred_at_fallback else 'source'):
                    raise EventStoreError('association_projection_unavailable')
                source = SourceKey(row['account_id'],row['source_generation'],row['shard_name'],row['table_name'])
                candidates.append((source,row['local_id'],row['server_id'],row['local_type'],row['create_time'],row['real_sender_id'],row['content_fingerprint']))
            conflict = self.event_store._association_conflict(candidates)
            if before != self._association_store_epoch():
                raise EventStoreError('association_state_changed')
        except Exception as error:
            raise CodexFirstError('conversation_attribution_unavailable') from error
        if conflict:
            raise CodexFirstError('conversation_attribution_conflict')
        return page

    def _association_store_epoch(self):
        try:
            return (int(self.event_store._conn.execute('PRAGMA data_version').fetchone()[0]),
                    self.event_store._conn.total_changes,
                    int(self.agent_store._conn.execute('PRAGMA data_version').fetchone()[0]),
                    self.agent_store._conn.total_changes)
        except Exception:
            return None

    @staticmethod
    def _unavailable(
        coverage: ScanCoverage, error_code: str
    ) -> ScanResult:
        return ScanResult((), coverage, 0, error_code, "unavailable")

    def _project(self) -> None:
        worker = NormalizationWorker(
            self.event_store,
            "codex-first-" + secrets.token_hex(8),
        )
        worker.run_once()
        with StandardEventRepository(self.event_store.path) as repository:
            projection = ProjectionWorker(repository, self.agent_store)
            while True:
                result = projection.run_once(self.projection_batch_size)
                if result.read == 0:
                    break

    def _window_event_refs(
        self,
        window: TimeWindow,
        evidence: SourceCapabilityEvidence,
        materializations: Mapping[str, Any],
        message_contexts: Mapping[str, tuple[SourceKey, RawMessage]] | None = None,
    ) -> list[dict[str, Any]]:
        service = EventSearchService(self.agent_store)
        expected_by_materialization_id: dict[int, Any] = {}
        expected_projection_by_materialization_id: dict[int, Any] = {}
        for materialization in materializations.values():
            materialization_id = int(materialization.id)
            if materialization_id in expected_by_materialization_id:
                raise EventStoreError("Duplicate materialization identity")
            expected_by_materialization_id[materialization_id] = materialization
            expected_projection_by_materialization_id[materialization_id] = (
                _project_materialization(vars(materialization), service.profile)
            )

        refs: list[dict[str, Any]] = []
        seen_event_ids: set[str] = set()
        materialization_ids = sorted(expected_by_materialization_id)
        for offset in range(0, len(materialization_ids), 100):
            chunk = materialization_ids[offset : offset + 100]
            placeholders = ",".join("?" for _ in chunk)
            with self.agent_store._lock:
                rows = self.agent_store._conn.execute(
                    f"""
                    SELECT event_id, subject_id, conversation_id, occurred_at,
                           occurred_at_fallback, quality_status, citation_text,
                           content_kind, event_type, materialization_id,
                           payload_sha256
                    FROM event_search_projection
                    WHERE profile_id=?
                      AND materialization_id IN ({placeholders})
                      AND occurred_at>=? AND occurred_at<=?
                    ORDER BY occurred_at DESC, materialization_id DESC
                    """,
                    (
                        service.profile.profile_id,
                        *chunk,
                        window.start.isoformat(timespec="microseconds"),
                        window.end.isoformat(timespec="microseconds"),
                    ),
                ).fetchall()
            for row in rows:
                materialization_id = int(row["materialization_id"])
                materialization = expected_by_materialization_id.get(
                    materialization_id
                )
                expected_projection = expected_projection_by_materialization_id.get(
                    materialization_id
                )
                event_id = str(row["event_id"])
                if (
                    materialization is None
                    or expected_projection is None
                    or event_id != expected_projection.event_id
                    or str(row["payload_sha256"])
                    != expected_projection.payload_sha256
                    or str(row["subject_id"]) != expected_projection.subject_id
                    or str(row["conversation_id"])
                    != expected_projection.conversation_id
                    or str(row["occurred_at"]) != expected_projection.occurred_at
                    or int(row["occurred_at_fallback"])
                    != expected_projection.occurred_at_fallback
                    or str(row["quality_status"])
                    != expected_projection.quality_status
                    or str(row["citation_text"])
                    != expected_projection.citation_text
                    or str(row["content_kind"])
                    != expected_projection.content_kind
                    or str(row["event_type"]) != expected_projection.event_type
                ):
                    continue
                if event_id in seen_event_ids:
                    raise EventStoreError("Duplicate Agent Store event identity")
                event_ref = service._event_ref(row)
                context = (message_contexts or {}).get(event_id)
                if callable(evidence.direction_for_message) and context is not None:
                    direction = evidence.direction_for_message(*context)
                elif callable(evidence.inbound_event_ref):
                    if not evidence.inbound_event_ref(event_ref):
                        continue
                    direction = "inbound"
                else:
                    direction = "unknown"
                canonical = self._canonical_payload(materialization)
                content = canonical.get("content")
                full_text = content.get("text") if isinstance(content, dict) else None
                displayed = {**event_ref, "direction": direction}
                if context is not None and callable(evidence.sender_identity):
                    sender = evidence.sender_identity(*context)
                    displayed["sender_identity"] = sender if sender is not None else "unknown"
                if context is not None and callable(evidence.sender_display_name):
                    sender_name = evidence.sender_display_name(*context)
                    displayed["sender_display_name"] = (
                        sender_name if sender_name is not None else "unknown"
                    )
                if context is not None and callable(evidence.source_facts):
                    displayed["source_facts"] = dict(evidence.source_facts(*context))
                if isinstance(full_text, str):
                    displayed["citation_text"] = full_text
                refs.append(displayed)
                seen_event_ids.add(event_id)
        return refs

    @staticmethod
    def _canonical_payload(materialization: Any) -> dict[str, Any]:
        encoded = materialization.canonical_payload_json.encode("utf-8")
        if hashlib.sha256(encoded).hexdigest() != materialization.payload_sha256:
            raise EventStoreError("Standard Event payload hash mismatch")
        try:
            payload = json.loads(materialization.canonical_payload_json)
        except (TypeError, ValueError) as exc:
            raise EventStoreError("Standard Event payload is invalid") from exc
        if not isinstance(payload, dict) or payload.get("id") != materialization.event_id:
            raise EventStoreError("Standard Event payload identity mismatch")
        return payload

    @staticmethod
    def _voice_failure(binding: VoiceMessageBinding) -> dict[str, Any]:
        return {
            "kind": "voice",
            "label": "语音转写",
            "status": "enrichment_unavailable",
            "binding": {**asdict(binding), "packed_info_sha256": None},
            "transcript": {
                "status": "unavailable",
                "source": None,
                "text": None,
                "confidence": None,
                "recognizer_version": None,
                "model_sha256": None,
            },
            "audio": {
                "association_status": "unavailable",
                "match_count": 0,
                "payload_status": "unknown",
                "format": None,
                "byte_count": None,
                "audio_payload_sha256": None,
                "decode_status": "not_attempted",
            },
        }

    def _enrich_visible_voices(
        self,
        result: ScanResult,
        contexts: Mapping[str, tuple[SourceKey, RawMessage, Any, Any]],
        evidence: SourceCapabilityEvidence,
        on_voice_result: VoiceResultCallback | None = None,
    ) -> ScanResult:
        if self.voice_enricher is None:
            return result
        enriched_groups = []
        for group in result.groups:
            enriched_messages = []
            for displayed in group.messages:
                context = contexts.get(displayed.event_id)
                if context is None:
                    enriched_messages.append(displayed)
                    continue
                if self._contact_guard is not None:
                    self._contact_guard()
                source, raw_message, materialization, captured_packed = context
                canonical = self._canonical_payload(materialization)
                revision_number = canonical.get("sequence")
                if not isinstance(revision_number, int) or revision_number < 1:
                    enriched_messages.append(displayed)
                    continue
                binding = VoiceMessageBinding(
                    account_id=source.account_id,
                    source_generation=source.source_generation,
                    shard_name=source.shard_name,
                    table_name=source.table_name,
                    local_id=raw_message.local_id,
                    server_id=raw_message.server_id,
                    local_type=raw_message.local_type,
                    create_time=raw_message.create_time,
                    sender_id=raw_message.real_sender_id,
                    message_id=displayed.event_id,
                    conversation_id=displayed.conversation_id,
                    occurred_at=str(displayed.event_ref.get("occurred_at", "")),
                    source_revision_id=materialization.source_revision_id,
                    revision_number=revision_number,
                    direction=displayed.direction,
                )
                shard = self.catalog.shard_for_source(source)
                conversation_identity_missing = (
                    callable(evidence.conversation_identity_for_source)
                    and evidence.conversation_identity_for_source(source) is None
                )
                if shard is None or conversation_identity_missing:
                    voice = self._voice_failure(binding)
                else:
                    try:
                        if self.voice_sources_refresh is not None:
                            self.voice_sources_refresh()
                        epoch = self._voice_epoch(shard)
                        if epoch is None:
                            voice = self._voice_failure(binding)
                        elif captured_packed is not None and epoch == captured_packed[0]:
                            voice = self.voice_enricher.enrich(shard.connection, binding, _window_packed=captured_packed[1])
                        else:
                            voice = self.voice_enricher.enrich(shard.connection, binding)
                    except Exception:
                        # Media derivation is optional. Preserve the already
                        # rendered message and expose only a controlled status.
                        voice = self._voice_failure(binding)
                enriched_messages.append(
                    replace(
                        displayed,
                        event_ref={**displayed.event_ref, "voice": voice},
                    )
                )
                if on_voice_result is not None:
                    on_voice_result(displayed.event_id, voice)
            enriched_groups.append(
                replace(group, messages=tuple(enriched_messages))
            )
        return replace(result, groups=tuple(enriched_groups))

    def _enrich_visible_images(
        self,
        result: ScanResult,
        contexts: Mapping[str, tuple[SourceKey, RawMessage]],
        on_image_result: ImageResultCallback | None = None,
    ) -> ScanResult:
        if self.image_enricher is None:
            return result
        groups = []
        for group in result.groups:
            messages = []
            for displayed in group.messages:
                context = contexts.get(displayed.event_id)
                if context is None or (int(context[1].local_type) & 0xFFFFFFFF) != 3:
                    messages.append(displayed)
                    continue
                if self._contact_guard is not None:
                    self._contact_guard()
                try:
                    image = dict(self.image_enricher(*context, group.conversation_id))
                except Exception:
                    image = {"status": "unavailable", "reason": "image_source_unavailable"}
                messages.append(
                    replace(displayed, event_ref={**displayed.event_ref, "image": image})
                )
                if on_image_result is not None:
                    on_image_result(displayed.event_id, image)
            groups.append(replace(group, messages=tuple(messages)))
        return replace(result, groups=tuple(groups))

    def _resolve_visible_attachments(self, result, contexts):
        if self.attachment_resolver is None:
            return result
        groups = []
        for group in result.groups:
            messages = []
            for displayed in group.messages:
                context = contexts.get(displayed.event_id)
                if context is None or displayed.content_kind != 'file':
                    messages.append(displayed)
                    continue
                if self._contact_guard is not None:
                    self._contact_guard()
                attachment = self.attachment_resolver.resolve(*context)
                messages.append(replace(displayed, event_ref={**displayed.event_ref, 'attachment': attachment}))
            groups.append(replace(group, messages=tuple(messages)))
        return replace(result, groups=tuple(groups))

    def scan(
        self,
        window: TimeWindow,
        *,
        display_limit: int,
        display_offset: int = 0,
        query_anchor: QueryRunAnchor | None = None,
        confirmation_token: str | None = None,
        on_base_messages: BaseMessagesCallback | None = None,
        on_voice_result: VoiceResultCallback | None = None,
        on_image_result: ImageResultCallback | None = None,
        _latest_limit: int | None = None,
    ) -> ScanResult:
        if not isinstance(window, TimeWindow):
            raise TypeError("window must be a TimeWindow")
        if display_limit < 1:
            raise CodexFirstError("display_limit must be positive")
        if display_offset < 0:
            raise CodexFirstError("display_offset must not be negative")
        for callback, name in (
            (on_base_messages, "on_base_messages"),
            (on_voice_result, "on_voice_result"),
            (on_image_result, "on_image_result"),
        ):
            if callback is not None and not callable(callback):
                raise TypeError(f"{name} must be callable")

        self._query_failures = []
        if self.attachment_resolver is not None:
            self.attachment_resolver._begin_query()
        self._latest_ids = None
        self._latest_source_state = self._source_state() if _latest_limit is not None else None
        if _latest_limit is not None:
            self._ensure_latest_source_state()
        self._request_preflight = None
        self._request_preflight_state = None
        self._request_preflight_window = None
        if self.scan_start is not None:
            self.scan_start()
        if _latest_limit is not None:
            self._ensure_latest_source_state()
        if self._contact_target is not None or self._conversation_target is not None:
            if self._contact_guard is not None:
                self._contact_guard()
            self._prepare_contact_scope(window)
            if _latest_limit is not None:
                self._ensure_latest_source_state()
            if self._contact_guard is not None:
                self._contact_guard()
        latest_ids = None
        if _latest_limit is not None:
            latest_sources = self._sources()
            latest_ids, selected = self._latest_candidates(latest_sources, _latest_limit)
            self._ensure_latest_source_state()
            self._latest_ids = latest_ids
            if selected:
                window = TimeWindow.from_utc(datetime.fromtimestamp(selected[-1][0], timezone.utc),
                                             datetime.fromtimestamp(selected[0][0], timezone.utc), 'latest')
            if self._contact_guard is not None:
                self._contact_guard()
        start_seconds, end_seconds = _inclusive_source_seconds(window)
        if start_seconds > end_seconds:
            self.catalog.refresh_catalog()
            sources = self._sources()
            gaps = list(self._scope_gaps)
            if self._contact_target is not None and self._contact_unknown_sources:
                gaps.append('related_source_identity_unknown')
            if not sources:
                gaps.append('live_source_unavailable')
                if self._contact_target is not None:
                    gaps.append('target_private_source_unavailable')
                return self._unavailable(self._coverage(window, [], [], gaps), 'source_unavailable')
            if self._contact_target is not None and not self._scope_private:
                gaps.append('target_private_source_unavailable')
            result = ScanResult((), self._coverage(window, [], [], gaps), 0)
            if on_base_messages is not None:
                on_base_messages(result)
            return result
        if _latest_limit is None and (window.end - window.start).total_seconds() > LARGE_QUERY_SECONDS:
            before = self._source_state()
            preflight = self.preflight(window)
            after = self._source_state()
            self._request_preflight = preflight
            self._request_preflight_window = window
            if before is not None and before == after:
                self._request_preflight_state = after
            if confirmation_token != preflight.confirmation_token:
                coverage = self._coverage(window, [], [], ["confirmation_required"])
                return ScanResult((), coverage, 0, "confirmation_required", "confirmation_required")

        self.catalog.refresh_catalog()
        if latest_ids is not None:
            self._ensure_latest_source_state()
        sources = self._sources()
        if latest_ids is not None and set(sources) != set(latest_ids):
            raise ContactQueryError('source_changed')
        if not sources:
            missing = [
                source
                for source in self.event_store.source_catalog_sources("unavailable")
                if source.account_id == self.account_id
                and source.source_generation == self.source_generation
            ]
            gaps = ["live_source_unavailable", *self._scope_gaps]
            if self._contact_target is not None:
                gaps.append('target_private_source_unavailable')
            if missing:
                gaps.append("known_source_missing")
            coverage = self._coverage(
                window, [], [], gaps
            )
            return self._unavailable(coverage, "source_unavailable")

        evidence = self.capability_check(self.catalog)
        evidence_fields = (
            "direction_proven",
            "contact_mapping_proven",
            "inbound_message",
            "inbound_event_ref",
            "display_name",
            "direction_for_message",
            "conversation_kind",
            "include_source",
            "sender_identity",
            "sender_display_name",
            "conversation_identity_for_source",
        )
        if any(not hasattr(evidence, field) for field in evidence_fields):
            raise TypeError("capability_check must return SourceCapabilityEvidence")
        if (
            type(evidence.direction_proven) is not bool
            or type(evidence.contact_mapping_proven) is not bool
            or any(
                value is not None and not callable(value)
                for value in (
                    evidence.inbound_message,
                    evidence.inbound_event_ref,
                    evidence.display_name,
                    evidence.direction_for_message,
                    evidence.conversation_kind,
                    evidence.include_source,
                    evidence.sender_identity,
                    evidence.sender_display_name,
                    evidence.conversation_identity_for_source,
                )
            )
            or (
                evidence.contact_mapping_proven
                and not callable(evidence.display_name)
            )
            or (
                evidence.direction_proven
                and not (
                    callable(evidence.direction_for_message)
                    or (
                        callable(evidence.inbound_message)
                        and callable(evidence.inbound_event_ref)
                    )
                )
            )
        ):
            raise TypeError("capability_check returned invalid source evidence")

        start_seconds, end_seconds = _inclusive_source_seconds(window)
        minimums: list[int] = []
        maximums: list[int] = []
        gaps: list[str] = []
        gaps.extend(self._scope_gaps)
        if self._contact_target is not None:
            if self._contact_unknown_sources:
                gaps.append('related_source_identity_unknown')
            if not any(callable(evidence.conversation_identity_for_source)
                       and evidence.conversation_identity_for_source(source) == self._contact_target for source in sources):
                gaps.append('target_private_source_unavailable')
        for missing in self.event_store.source_catalog_sources("unavailable"):
            if (
                missing.account_id == self.account_id
                and missing.source_generation == self.source_generation
            ):
                gaps.append("known_source_missing")
        successful_reads = 0
        scanned_messages: list[tuple[SourceKey, list[RawMessage]]] = []
        window_voice_packed: dict[tuple[SourceKey, int], tuple[Any, tuple[Any, ...]]] = {}
        source_included: dict[SourceKey, bool] = {}
        for source in sources:
            source_included[source] = (
                evidence.include_source(source)
                if callable(evidence.include_source)
                else True
            )
        association_initial_epochs = {}
        for source in sources:
            if source_included[source]:
                shard = self.catalog.shard_for_source(source)
                if shard is not None and id(shard.connection) not in association_initial_epochs:
                    association_initial_epochs[id(shard.connection)] = (shard, self._voice_epoch(shard))
        reused = {}
        pending_proofs = []
        invalid_proofs = set()
        worker_included = dict(source_included)
        self._baseline_metadata_scan = False
        if self._empty_window_enabled:
            self._baseline_metadata_scan = self.event_store._begin_window_baseline(self.account_id, self.source_generation)
            proofs = self.event_store._empty_window_proofs(self.account_id, self.source_generation)
            epochs = {}
            for source, (_, _, stamps, _, binding) in proofs.items():
                shard = self.catalog.shard_for_source(source)
                if shard is None:
                    invalid_proofs.add(source)
                    continue
                identity = id(shard.connection)
                if identity not in epochs:
                    epochs[identity] = self._voice_epoch(shard)
                epoch = epochs[identity]
                path_binding = hashlib.sha256(os.fsencode(str(shard.path))).hexdigest()
                if epoch is None or epoch[2] != stamps or path_binding != binding:
                    invalid_proofs.add(source)
            for source in sources:
                if not source_included[source] or source not in proofs or source in invalid_proofs:
                    continue
                shard = self.catalog.shard_for_source(source)
                if shard is None:
                    continue
                identity = id(shard.connection)
                if identity not in epochs:
                    epochs[identity] = self._voice_epoch(shard)
                epoch = epochs[identity]
                old_start, old_end, stamps, no_future, _ = proofs[source]
                if epoch is not None and epoch[2] == stamps and start_seconds >= old_start and (end_seconds <= old_end or no_future):
                    reused[source] = (shard, epoch)
                    worker_included[source] = False
        def reused_is_stable():
            checked = {}
            for shard, expected in reused.values():
                identity = id(shard.connection)
                if identity not in checked:
                    checked[identity] = self._voice_epoch(shard)
                if checked[identity] != expected:
                    return False
            return True
        if latest_ids is not None:
            self._ensure_latest_source_state()
        parallel_reads = (
            self._parallel_window_reads(sources, worker_included, start_seconds, end_seconds)
            if self._scan_workers > 1 or self._empty_window_enabled or latest_ids is not None else None
        )
        if latest_ids is not None:
            self._ensure_latest_source_state()
        if any(self._voice_epoch(shard) != epoch for shard,epoch in self._scope_epochs.values()):
            return self._unavailable(self._coverage(window, minimums, maximums, [*gaps, 'source_changed']), 'source_changed')
        if self._contact_guard is not None:
            self._contact_guard()
        max_empty_epochs = {}
        if self._empty_window_enabled and parallel_reads is not None:
            for source, value in parallel_reads.items():
                if len(value) > 5 and value[5]:
                    shard = self.catalog.shard_for_source(source)
                    if shard is None or value[4] is None:
                        return self._unavailable(self._coverage(window, minimums, maximums, [*gaps, 'source_changed']), 'source_changed')
                    max_empty_epochs[id(shard.connection)] = (shard, value[4])
        def max_empty_is_stable():
            return all(self._voice_epoch(shard) == epoch for shard, epoch in max_empty_epochs.values())
        if not max_empty_is_stable():
            return self._unavailable(self._coverage(window, minimums, maximums, [*gaps, 'source_changed']), 'source_changed')
        for source in sources:
            if not source_included[source]:
                gaps.append("source_excluded_by_classification_policy")
                continue
            shard = self.catalog.shard_for_source(source)
            if shard is None:
                gaps.append("live_source_unavailable")
                continue
            failure_stage = "source_read"
            try:
                if source in reused:
                    successful_reads += 1
                    gaps.append('timestamp_integrity_unknown_outside_requested_window')
                    continue
                if parallel_reads is not None:
                    value = parallel_reads[source]
                    read, voice_before, error = value[:3]
                    if error is not None:
                        raise error
                else:
                    voice_before = self._voice_epoch(shard) if self.voice_enricher is not None else None
                    read = read_message_window(
                        shard.connection,
                        source.table_name,
                        start_unix_seconds=start_seconds,
                        end_unix_seconds=end_seconds,
                        batch_size=self.batch_size,
                        **({'collect_voice_packed': True} if self.voice_enricher is not None else {}),
                        **({'_sender_id': self._scope_senders[source]} if source in self._scope_senders else {}),
                    )
                if voice_before is not None and voice_before == self._voice_epoch(shard):
                    for local_id, packed in read.voice_packed.items():
                        window_voice_packed[(source, local_id)] = (voice_before, packed)
                successful_reads += 1
                if read.source_min_time is not None:
                    minimums.append(read.source_min_time)
                if read.source_max_time is not None:
                    maximums.append(read.source_max_time)
                if read.invalid_timestamp_count is None:
                    gaps.append("timestamp_integrity_unknown_outside_requested_window")
                messages = list(read.messages)
                if latest_ids is not None:
                    expected = {(local_id, timestamp) for timestamp, _, _, local_id, key in selected if key == source}
                    if {(message.local_id, message.create_time) for message in messages} != expected or len(messages) != len(expected):
                        raise LiveReadonlyError('Latest source changed')
                    if not expected:
                        continue
                if (
                    evidence.direction_for_message is None
                    and evidence.direction_proven
                    and callable(evidence.inbound_message)
                ):
                    messages = [
                        message
                        for message in messages
                        if evidence.inbound_message(source, message)
                    ]
                failure_stage = "result_processing"
                self.event_store.commit_window_messages(
                    source,
                    messages,
                    WindowIngestRun(
                        window.start,
                        window.end,
                        {
                            "source_min_time": read.source_min_time,
                            "source_max_time": read.source_max_time,
                            "examined_count": read.examined_count,
                        },
                    ),
                )
                if self._empty_window_enabled and parallel_reads is not None and not read.messages:
                    no_future, stable = value[3:5]
                    if stable is not None:
                        binding = hashlib.sha256(os.fsencode(str(shard.path))).hexdigest()
                        pending_proofs.append((shard, stable, (source, start_seconds, end_seconds, stable[2], no_future, binding)))
                    else:
                        gaps.append('baseline_state_unavailable')
                scanned_messages.append((source, messages))
            except Exception as error:
                if latest_ids is not None:
                    reason = 'source_read_unavailable' if failure_stage == 'source_read' else 'result_processing_unavailable'
                    self._query_failures.append({'stage': failure_stage, 'reason': _diagnostic_reason(error)})
                    return self._unavailable(self._coverage(window, minimums, maximums, [*gaps, reason]), reason)
                # Preserve the existing gap and only fixed, content-free facts.
                failure = {"stage": failure_stage, "reason": _diagnostic_reason(error)}
                if failure not in self._query_failures:
                    self._query_failures.append(failure)
                gaps.append("source_read_unavailable")

        if not reused_is_stable() or not max_empty_is_stable():
            return self._unavailable(self._coverage(window, minimums, maximums, [*gaps, 'source_changed']), 'source_changed')
        mark_initial = (self._baseline_metadata_scan and successful_reads > 0 and not
                        {'known_source_missing', 'live_source_unavailable', 'source_read_unavailable',
                         'baseline_state_unavailable', 'source_changed'}.intersection(gaps))
        if pending_proofs or invalid_proofs or mark_initial:
            try:
                epochs = {}
                valid = []
                for shard, stable, proof in pending_proofs:
                    identity = id(shard.connection)
                    if identity not in epochs:
                        epochs[identity] = self._voice_epoch(shard)
                    if epochs[identity] == stable:
                        valid.append(proof)
                    else:
                        gaps.append('baseline_state_unavailable')
                if len(valid) != len(pending_proofs):
                    mark_initial = False
                self.event_store._save_empty_window_proofs(valid, tuple(invalid_proofs),
                    (self.account_id, self.source_generation) if mark_initial else None)
            except Exception:
                gaps.append('baseline_state_unavailable')  # Keep actual messages visible.
        if not evidence.available:
            gaps.append("direction_or_contact_mapping_unavailable")
        coverage = self._coverage(window, minimums, maximums, gaps)
        if successful_reads == 0:
            return self._unavailable(coverage, "source_unavailable")
        if any(self._voice_epoch(shard) is None for shard in self.catalog.shards):
            return self._unavailable(self._coverage(window, minimums, maximums, [*gaps, "source_changed"]), "source_changed")

        worker = NormalizationWorker(self.event_store, 'codex-first-' + secrets.token_hex(8))
        selected_materializations = []
        profile = ProjectionProfile()
        for source, messages in scanned_messages:
            for outbox_id in self.event_store._exact_window_outboxes(source, messages):
                outcome = worker.process_exact(outbox_id)
                if outcome.outcome not in ('normalized', 'degraded'):
                    raise EventStoreError('Selected normalization unavailable')
            exact = self.event_store.standard_event_materializations_for_messages(
                source, messages, contract_version=profile.contract_version,
                normalizer_name=profile.normalizer_name, normalizer_version=profile.normalizer_version,
                _exact=True,
            )
            if len(exact) != len(messages):
                raise EventStoreError('Selected materialization unavailable')
            selected_materializations.extend(exact.values())
        # run_exact never reads from the repository or advances its global checkpoint.
        try:
            ProjectionWorker(None, self.agent_store).run_exact(selected_materializations)
        except ProjectionError:
            if self._contact_target is not None or self._conversation_target is not None:
                raise
            # The ordinary path historically reported a projection gap for stale rows.
            gaps.append('event_projection_unavailable')
        try:
            contributing = {id(self.catalog.shard_for_source(source).connection)
                            for source,messages in scanned_messages if messages
                            and self.catalog.shard_for_source(source) is not None}
            association_source_epochs = [value for key,value in association_initial_epochs.items() if key in contributing]
            if len(association_source_epochs) != len(contributing):
                raise EventStoreError('association_source_unavailable')
            association_epoch = self._association_store_epoch()
            if association_epoch is None or any(epoch is None or self._voice_epoch(shard) != epoch for shard, epoch in association_source_epochs):
                raise EventStoreError('association_state_unavailable')
            candidates = [(source,message.local_id,message.server_id,message.local_type,message.create_time,
                           message.real_sender_id,_content_fingerprint(message))
                          for source,messages in scanned_messages for message in messages]
            conflict = self.event_store._association_conflict(candidates)
            if association_epoch != self._association_store_epoch() or any(self._voice_epoch(shard) != epoch for shard,epoch in association_source_epochs):
                raise EventStoreError('association_state_changed')
        except Exception:
            return self._unavailable(self._coverage(window, minimums, maximums, [*gaps,'conversation_attribution_unavailable']), 'conversation_attribution_unavailable')
        if conflict:
            return self._unavailable(self._coverage(window, minimums, maximums, [*gaps,'conversation_attribution_conflict']), 'conversation_attribution_conflict')
        profile = ProjectionProfile()
        def materialization_epoch():
            try:
                return (int(self.event_store._conn.execute('PRAGMA data_version').fetchone()[0]),
                        self.event_store._conn.total_changes)
            except Exception:
                return None
        has_voice = self.voice_enricher is not None and any(
            (int(message.local_type) & 0xFFFFFFFF) == 34
            for _source, messages in scanned_messages for message in messages
        )
        voice_epoch = materialization_epoch() if has_voice else None
        materializations: dict[str, Any] = {}
        message_contexts: dict[str, tuple[SourceKey, RawMessage]] = {}
        voice_contexts: dict[str, tuple[SourceKey, RawMessage, Any, Any]] = {}
        expected_materializations = 0
        for source, messages in scanned_messages:
            expected_materializations += len(messages)
            by_local_id: dict[int, Any] = {}
            resolved = self.event_store.standard_event_materializations_for_messages(
                source,
                messages,
                contract_version=profile.contract_version,
                normalizer_name=profile.normalizer_name,
                normalizer_version=profile.normalizer_version,
                _exact=True,
                **({'_by_local_id': by_local_id} if has_voice else {}),
            )
            overlap = materializations.keys() & resolved.keys()
            if overlap:
                raise EventStoreError("Duplicate materialization identity across sources")
            materializations.update(resolved)
            raw_by_uid = {
                _event_uid(source, message.local_id): (source, message)
                for message in messages
            }
            for event_id, materialization in resolved.items():
                provenance = self._canonical_payload(materialization).get("provenance")
                raw_uid = provenance.get("raw_event_uid") if isinstance(provenance, dict) else None
                context = raw_by_uid.get(raw_uid)
                if context is not None:
                    message_contexts[event_id] = context
            if self.voice_enricher is not None:
                for message in messages:
                    if (int(message.local_type) & 0xFFFFFFFF) != 34:
                        continue
                    exact = by_local_id.get(message.local_id)
                    if exact is not None:
                        voice_contexts[exact.event_id] = (source, message, exact, window_voice_packed.get((source, message.local_id)))
        if len(materializations) != expected_materializations:
            gaps.append("event_projection_unavailable")
        event_refs = self._window_event_refs(window, evidence, materializations, message_contexts)
        if len(event_refs) != len(materializations):
            gaps.append("event_projection_unavailable")
        if self._contact_target is not None:
            selected_refs = []
            for ref in event_refs:
                context = message_contexts.get(ref.get('event_id'))
                username = evidence.conversation_identity_for_source(context[0]) if context is not None and callable(evidence.conversation_identity_for_source) else None
                if username == self._contact_target:
                    selected_refs.append(ref)
                elif self._contact_related and isinstance(username, str) and username.endswith('@chatroom'):
                    sender = ref.get('sender_identity')
                    if sender == self._contact_target:
                        ref['conversation_username'] = username
                        selected_refs.append(ref)
                    elif sender in (None, 'unknown'):
                        gaps.append('related_sender_unknown')
            event_refs = selected_refs
        coverage = self._coverage(window, minimums, maximums, gaps)
        service = CodexFirstService(
            evidence.display_name if evidence.contact_mapping_proven else None,
            direction_capability=evidence.direction_proven,
        )
        rendered = service.render(
            event_refs,
            coverage=coverage,
            display_limit=display_limit,
            display_offset=display_offset,
            query_anchor=query_anchor,
        )
        if callable(evidence.conversation_kind):
            rendered = replace(
                rendered,
                groups=tuple(
                    replace(group, conversation_kind=evidence.conversation_kind(group.conversation_id))
                    for group in rendered.groups
                ),
            )
        if evidence.direction_proven and any(
            message.direction == "unknown"
            for group in rendered.groups
            for message in group.messages
        ):
            gaps.append("direction_identity_unavailable")
            coverage = self._coverage(window, minimums, maximums, gaps)
            rendered = replace(rendered, coverage=coverage)
        if not reused_is_stable() or not max_empty_is_stable():
            return self._unavailable(self._coverage(window, minimums, maximums, [*gaps, 'source_changed']), 'source_changed')
        if association_epoch != self._association_store_epoch() or any(self._voice_epoch(shard) != epoch for shard,epoch in association_source_epochs):
            return self._unavailable(self._coverage(window, minimums, maximums, [*gaps,'conversation_attribution_unavailable']), 'conversation_attribution_unavailable')
        if self.attachment_resolver is not None:
            rendered = self._resolve_visible_attachments(rendered, message_contexts)
            if not reused_is_stable() or not max_empty_is_stable():
                return self._unavailable(self._coverage(window, minimums, maximums, [*gaps, 'source_changed']), 'source_changed')
            if association_epoch != self._association_store_epoch() or any(self._voice_epoch(shard) != epoch for shard,epoch in association_source_epochs):
                return self._unavailable(self._coverage(window, minimums, maximums, [*gaps,'conversation_attribution_unavailable']), 'conversation_attribution_unavailable')
            if self._contact_guard is not None:
                self._contact_guard()
        if on_base_messages is not None:
            if latest_ids is not None:
                self._ensure_latest_source_state()
            if latest_ids is not None and any(self._voice_epoch(shard) != epoch for shard, epoch in self._scope_epochs.values()):
                return self._unavailable(self._coverage(window, minimums, maximums, [*gaps, 'source_changed']), 'source_changed')
            on_base_messages(rendered)
        rendered = self._enrich_visible_images(
            rendered, message_contexts, on_image_result=on_image_result
        )
        if self.voice_sources_refresh is not None:
            self.voice_sources_refresh()
        if has_voice:
            current_epoch = materialization_epoch()
            if voice_epoch is None or current_epoch is None or current_epoch != voice_epoch:
                voice_contexts.clear()
                gaps.append("voice_association_unavailable")
                self._query_failures.append({"stage": "voice_association", "reason": (
                    "state_unavailable" if voice_epoch is None or current_epoch is None else "state_changed"
                )})
                rendered = replace(rendered, coverage=self._coverage(window, minimums, maximums, gaps))
        if latest_ids is not None and any(self._voice_epoch(shard) != epoch for shard, epoch in self._scope_epochs.values()):
            return self._unavailable(self._coverage(window, minimums, maximums, [*gaps, 'source_changed']), 'source_changed')
        if latest_ids is not None:
            self._ensure_latest_source_state()
        return self._enrich_visible_voices(
            rendered, voice_contexts, evidence, on_voice_result=on_voice_result
        )


def _iso(value: datetime | None) -> str | None:
    return value.isoformat(timespec="microseconds") if value is not None else None


def _result_payload(result: ScanResult) -> dict[str, Any]:
    return {
        "status": result.status,
        "error_code": result.error_code,
        "requested": {
            "start": _iso(result.coverage.requested_start),
            "end": _iso(result.coverage.requested_end),
        },
        "actual": {
            "start": _iso(result.coverage.actual_start),
            "end": _iso(result.coverage.actual_end),
        },
        "coverage_status": result.coverage.status,
        "gaps": list(result.coverage.gaps),
        "total_count": result.total_messages,
        "query_anchor": list(result.query_anchor.event_ids) if result.query_anchor else None,
        "groups": [
            {
                "conversation_id": group.conversation_id,
                "display_name": group.display_name,
                "conversation_kind": group.conversation_kind,
                "displayed_count": len(group.messages),
                "total_count": group.total_messages,
                "continuation": group.has_more,
                "continuation_offset": group.continuation_offset,
                "event_refs": [dict(message.event_ref) for message in group.messages],
            }
            for group in result.groups
        ],
    }


def _confirmation_payload(preflight: QueryPreflight, window: TimeWindow, timezone_name: str) -> dict[str, Any] | None:
    if preflight.status != "awaiting_confirmation":
        return None
    zone = ZoneInfo(timezone_name)
    def fixed_wall(value: datetime) -> str:
        value = value.astimezone(zone)
        base = value.strftime("%Y-%m-%d %H:%M:%S")
        return f"{base}.{value.microsecond:06d}" if value.microsecond else base

    start = fixed_wall(window.start)
    end = fixed_wall(window.end)
    return {
        "token": preflight.confirmation_token,
        "request": f"从 {start} 到 {end}",
        "estimated_rows": preflight.estimated_rows,
        "available_bytes": preflight.available_bytes,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one fail-closed, read-only Codex-first WeChat window scan"
    )
    parser.add_argument("--source-config", type=Path, help="private current-source.json created by wechat-local-setup")
    parser.add_argument("--live-db-root", type=Path)
    parser.add_argument("--message-root", type=Path)
    parser.add_argument("--passphrase", type=Path)
    parser.add_argument("--event-store", type=Path)
    parser.add_argument("--agent-store", type=Path)
    parser.add_argument("--account-id")
    parser.add_argument("--source-generation")
    parser.add_argument("--self-username")
    contact_sources = parser.add_mutually_exclusive_group()
    contact_sources.add_argument("--contact-database", type=Path, help="explicit read-only plaintext contact.db for exact username-to-name lookup")
    contact_sources.add_argument("--encrypted-contact-database", type=Path, help="explicit read-only encrypted contact.db under --live-db-root")
    parser.add_argument("--include-public-accounts", action="store_true")
    target = parser.add_mutually_exclusive_group()
    target.add_argument("--conversation-username", help="exact Name2Id username; restricts sources before preflight and content reads")
    target.add_argument("--contact-query", help="resolve a current contact by username, remark, nickname or alias")
    target.add_argument("--group-query", help="resolve a group name to its stable conversation")
    parser.add_argument("--include-related-groups", action="store_true", help="include this contact's own messages in verified groups")
    parser.add_argument("--locate-related-groups", action="store_true", help="locate groups with this contact's messages without reading message content")
    when = parser.add_mutually_exclusive_group()
    when.add_argument("--request")
    when.add_argument("--latest", nargs='?', const=20, type=int, choices=range(1, 101),
                      metavar='N', help='latest 1–100 messages in the resolved private conversation (default 20)')
    parser.add_argument("--timezone")
    parser.add_argument("--check-config", action="store_true")
    parser.add_argument("--display-limit", type=int, default=50)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--voice-transcripts", action="store_true")
    parser.add_argument("--voice-database", type=Path, action="append", default=[])
    parser.add_argument("--voice-runtime-root", type=Path)
    parser.add_argument("--voice-python", type=Path)
    parser.add_argument("--voice-jsonl", action="store_true")
    parser.add_argument("--image-database", type=Path)
    parser.add_argument("--image-attachments-root", type=Path)
    parser.add_argument("--image-output-root", type=Path)
    parser.add_argument("--image-key-config", type=Path, help="private JSON file with account_scope and key_codes")
    parser.add_argument("--image-ffmpeg", type=Path)
    parser.add_argument("--configured-images", action="store_true", help="opt in to image sources recorded by --source-config")
    parser.add_argument('--file-attachments-root', type=Path,
                        help='opt in to read-only document, spreadsheet, presentation and archive location within this account; no download or parsing')
    parser.add_argument("--confirmation-token", help="confirmation token for the same fixed absolute large-window request")
    parser.add_argument("--json", action="store_true")
    return parser


def _attachment_resolver_for_args(args, live_db_root):
    path = args.file_attachments_root
    if path is None:
        return None
    root = Path(path).absolute()
    account = live_db_root.parent.resolve()
    if root == account or not root.is_relative_to(account) or root.is_relative_to(live_db_root):
        raise ValueError('attachment_root_outside_account')
    return AttachmentResolver(root, account_id=args.account_id, source_generation=args.source_generation)


def _run_cli(args: argparse.Namespace) -> int:
    args._diagnostic_stage = "configuration"
    if args.latest is not None and (args.contact_query is None or args.include_related_groups or args.check_config):
        raise ContactQueryError('latest_requires_private_contact')
    if args.locate_related_groups:
        if args.contact_query is None or not args.include_related_groups:
            raise ContactQueryError('locate_requires_contact_and_groups')
        if args.check_config or args.voice_transcripts or args.voice_database or args.voice_runtime_root or args.voice_python or args.configured_images or args.image_database or args.image_attachments_root or args.image_output_root or args.image_key_config or args.image_ffmpeg or args.file_attachments_root:
            raise ContactQueryError('locate_media_or_check_unsupported')
    if args.include_related_groups and args.contact_query is None:
        raise ContactQueryError('contact_query_required')
    name_query = args.contact_query if args.contact_query is not None else args.group_query
    if name_query is not None and not name_query.strip():
        raise ContactQueryError('contact_query_empty')
    if args.check_config and name_query is not None:
        raise ContactQueryError('contact_query_with_check_config')
    apply_source_config_to_args(args)
    if any(
        getattr(args, field, None) is None
        for field in ("live_db_root", "message_root", "passphrase", "event_store", "agent_store")
    ):
        raise LiveReadonlyError("A complete --source-config or explicit source paths are required")
    voice_options = bool(
        args.voice_database
        or args.voice_runtime_root
        or args.voice_python
        or args.voice_jsonl
    )
    if voice_options and not args.voice_transcripts and not args.locate_related_groups:
        raise LiveReadonlyError("Voice options require --voice-transcripts")
    if bool(args.voice_runtime_root) != bool(args.voice_python):
        raise LiveReadonlyError(
            "Local voice runtime and Python executable must be configured together"
        )
    if args.voice_jsonl and args.json:
        raise LiveReadonlyError("Choose either --json or --voice-jsonl")
    if args.voice_runtime_root is not None and not args.voice_jsonl:
        raise LiveReadonlyError(
            "Local voice transcription requires --voice-jsonl so messages are emitted first"
        )
    image_options = (
        args.image_database,
        args.image_attachments_root,
        args.image_output_root,
    )
    if any(image_options) and not all(image_options):
        raise LiveReadonlyError("Image database, attachments root and output root must be configured together")
    if (args.image_key_config or args.image_ffmpeg) and not all(image_options):
        raise LiveReadonlyError("Image decoder options require the image database, attachments and output roots")
    if args.event_store.resolve() == args.agent_store.resolve():
        raise LiveReadonlyError("EventStore and Agent Store must be separate files")
    message_root = args.message_root.resolve()
    if not message_root.is_dir() or message_root.is_symlink():
        raise LiveReadonlyError("Message root must be a non-symlink directory")
    source_paths = discover_numeric_shards(message_root)
    root, _sources, event_store_path = validate_live_storage_paths(
        args.live_db_root,
        source_paths,
        args.event_store,
    )
    try:
        message_root.relative_to(root)
        args.agent_store.resolve().relative_to(root)
    except ValueError:
        pass
    else:
        raise LiveReadonlyError("Agent Store must be outside the live DB root")
    passphrase = read_private_passphrase(args.passphrase)
    if args.check_config:
        checked = []
        args._diagnostic_stage = "source_open"
        try:
            for path in source_paths:
                connection, _raw_key_bytes = open_encrypted_readonly(path, passphrase)
                try:
                    checked.append(path.name)
                finally:
                    connection.close()
        except Exception as exc:
            raise LiveReadonlyError("Live source configuration is unavailable") from exc
        result = {"status": "ready", "query_only": True, "shard_count": len(checked)}
        print(json.dumps(result, sort_keys=True) if args.json else "CONFIGURATION_READY")
        return 0
    if not args.account_id or not args.source_generation or (not args.request and args.latest is None) or not args.timezone:
        raise LiveReadonlyError("Account, source generation, request and timezone are required for a query")
    now = datetime.now(timezone.utc)
    window = TimeWindow.from_utc(now, now, 'latest') if args.latest is not None else parse_time_window(
        args.request,
        now=now,
        timezone=ZoneInfo(args.timezone),
    )
    catalog = None
    voice_connections: list[Any] = []
    image_connection = None
    contact_connection = None
    image_connection_owned = False
    contact_connection_owned = False
    contact_source_unavailable = False
    voice_source_unavailable = bool(getattr(args, "voice_source_unavailable", False))
    image_source_unavailable = bool(getattr(args, "configured_image_unavailable", False))
    target_resolution = None
    contact_epoch = None
    resolved_contact_path = None
    preopened = None
    try:
        if name_query is not None:
            # Resolve against the current contact source before opening message shards.
            try:
                if args.contact_database is not None:
                    contact_path = args.contact_database
                    if contact_path.is_symlink() or not contact_path.is_file():
                        raise OSError('contact source unavailable')
                    before = contact_path.lstat()
                    contact_connection = sqlite3.connect(readonly_database_uri(contact_path), uri=True)
                    contact_connection_owned = True
                    contact_connection.execute('PRAGMA query_only=ON')
                    info = contact_path.lstat()
                elif args.encrypted_contact_database is not None:
                    contact_path = args.encrypted_contact_database
                    resolved_contact_path = contact_path.resolve(strict=True)
                    resolved_contact_path.relative_to(root)
                    if contact_path.is_symlink() or not resolved_contact_path.is_file():
                        raise OSError('contact source unavailable')
                    before = resolved_contact_path.lstat()
                    if not stat.S_ISREG(before.st_mode):
                        raise OSError('contact source unavailable')
                    contact_connection, _ = open_encrypted_readonly(resolved_contact_path, passphrase, _thread_handoff=True)
                    contact_connection_owned = True
                    info = resolved_contact_path.lstat()
                else:
                    before = info = None
                if before is not None and (not stat.S_ISREG(info.st_mode) or (before.st_dev,before.st_ino)!=(info.st_dev,info.st_ino)):
                    raise OSError('contact source changed')
            except Exception:
                if contact_connection is not None and contact_connection_owned:
                    contact_connection.close()
                contact_connection = None
                contact_connection_owned = False
                contact_source_unavailable = True
            early_identity = LiveIdentityAdapter(_ContactOnlyCatalog(), contact_connection=contact_connection,
                                                 contact_source_unavailable=contact_source_unavailable)
            if contact_connection is not None:
                early_identity._initial_shards[id(contact_connection)] = (info.st_dev,info.st_ino)
            early_identity.begin_scan()
            target_resolution, contact_epoch = early_identity.resolve_contact(name_query, _kind='private' if args.contact_query is not None else 'group')
            if (args.contact_query is not None and args.contact_query.strip().endswith('@chatroom')
                    and target_resolution['status'] in ('not_found', 'suggestions')):
                target_resolution = {'status':'unavailable', 'reason':'contact_target_type_mismatch', 'hint':'group-query'}
            args._target_resolution = target_resolution
            state = target_resolution['status']
            if state == 'resolved':
                selected = target_resolution['selected_username']
                if args.contact_query is not None and (selected.endswith('@chatroom') or selected.startswith('gh_')):
                    raise ContactQueryError('contact_target_unsupported')
                if args.group_query is not None:
                    args.conversation_username = selected
            else:
                code = 3 if state == 'ambiguous' else 2
                result = {'status': state, 'target_resolution': target_resolution}
                if state == 'unavailable':result['error_code'] = target_resolution['reason']
                if args.voice_jsonl:args._stream_completion = {'phase': 'complete', **result}
                else:args._contact_resolution_output = result
                return code
            if resolved_contact_path is not None and resolved_contact_path in source_paths:
                preopened = {resolved_contact_path: contact_connection}
        attachment_resolver = _attachment_resolver_for_args(args, root)
        resolved_voice_paths = tuple(path.resolve() for path in args.voice_database)
        if len(set(resolved_voice_paths)) != len(resolved_voice_paths):
            raise LiveReadonlyError("Voice database paths must be unique")
        args._diagnostic_stage = "result_processing"
        with EventStore(event_store_path) as event_store, (nullcontext(None) if args.locate_related_groups else AgentStore(
            args.agent_store
        )) as agent_store:
            args._diagnostic_stage = "source_open"
            catalog = LiveCatalog.dynamic_encrypted(
                message_root,
                passphrase,
                store=event_store,
                account_id=args.account_id,
                source_generation=args.source_generation,
                _thread_handoff=True,
                **({'_preopened': preopened} if preopened is not None else {}),
            )
            catalog_identities = {}
            for shard in catalog.shards:
                info = shard.path.lstat()
                catalog_identities[shard.path] = (id(shard.connection), info.st_dev, info.st_ino)
            args._diagnostic_stage = "result_processing"
            borrowed_voice_paths = {}
            owned_voice_paths = {}
            owned_voice_identities = {}
            unavailable_source_paths: set[Path] = set()
            for path in resolved_voice_paths:
                args._diagnostic_stage = "configuration"
                try:
                    path.relative_to(root)
                except ValueError as exc:
                    raise LiveReadonlyError(
                        "Voice database must be under the live DB root"
                    ) from exc
                args._diagnostic_stage = "result_processing"
                if not path.is_file():
                    # Voice data is an optional enrichment source. Preserve this
                    # coverage gap so base messages can still be emitted first.
                    voice_source_unavailable = True
                    unavailable_source_paths.add(path)
                    continue
                shared = next((shard.connection for shard in catalog.shards if shard.path == path), None)
                if shared is not None:
                    info = path.lstat()
                    if (not stat.S_ISREG(info.st_mode)
                            or (id(shared), info.st_dev, info.st_ino) != catalog_identities.get(path)):
                        voice_source_unavailable = True
                        unavailable_source_paths.add(path)
                        continue
                    borrowed_voice_paths[path] = (id(shared), info.st_dev, info.st_ino)
                    continue
                if (name_query is not None and resolved_contact_path == path and contact_connection is not None):
                    info = path.lstat()
                    if not stat.S_ISREG(info.st_mode) or (info.st_dev,info.st_ino) != early_identity._initial_shards.get(id(contact_connection)):
                        voice_source_unavailable = True
                        unavailable_source_paths.add(path)
                        continue
                    owned_voice_paths[path] = contact_connection
                    owned_voice_identities[path] = (info.st_dev,info.st_ino)
                    continue
                connection = None
                try:
                    before = path.lstat()
                    if not stat.S_ISREG(before.st_mode):
                        raise OSError("voice source is unavailable")
                    connection, _raw_key_bytes = open_encrypted_readonly(
                        path, passphrase
                    )
                    after = path.lstat()
                    if not stat.S_ISREG(after.st_mode) or (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
                        raise OSError("voice source changed")
                except Exception:
                    if connection is not None:
                        connection.close()
                    # An explicitly selected but unreadable/wrong-key voice shard
                    # is an incomplete coverage set: never infer a unique audio
                    # match from the remaining readable shards.
                    voice_source_unavailable = True
                    unavailable_source_paths.add(path)
                    continue
                voice_connections.append(connection)
                owned_voice_paths[path] = connection
                owned_voice_identities[path] = (after.st_dev, after.st_ino)
            def shared_source(path: Path) -> Any | None:
                if path in unavailable_source_paths:
                    raise OSError("shared source is unavailable")
                for shard in catalog.shards:
                    if shard.path == path:
                        info = path.lstat()
                        if (not stat.S_ISREG(info.st_mode)
                                or (id(shard.connection), info.st_dev, info.st_ino) != catalog_identities.get(path)):
                            raise OSError("shared source changed")
                        return shard.connection
                if path in owned_voice_paths:
                    info = path.lstat()
                    if not stat.S_ISREG(info.st_mode) or (info.st_dev, info.st_ino) != owned_voice_identities[path]:
                        raise OSError("shared source changed")
                    return owned_voice_paths[path]
                return None

            transcriber = None
            if args.voice_runtime_root is not None:
                transcriber = LocalWhisperTranscriber(
                    args.voice_runtime_root, args.voice_python
                )
            voice_enricher = (
                VoiceTranscriptEnricher(
                    voice_connections,
                    transcriber=transcriber,
                    source_unavailable=voice_source_unavailable,
                )
                if args.voice_transcripts
                else None
            )
            def refresh_voice_sources():
                if voice_enricher is None:
                    return
                current = {shard.path: shard.connection for shard in catalog.shards}
                usable = {}
                for path, expected in borrowed_voice_paths.items():
                    try:
                        info = path.lstat()
                        if stat.S_ISREG(info.st_mode) and path in current and (id(current[path]), info.st_dev, info.st_ino) == expected:
                            usable[path] = current[path]
                    except OSError:
                        pass
                owned_usable = {}
                for path, connection in owned_voice_paths.items():
                    try:
                        info = path.lstat()
                        if stat.S_ISREG(info.st_mode) and (info.st_dev, info.st_ino) == owned_voice_identities[path]:
                            owned_usable[path] = connection
                    except OSError:
                        pass
                voice_enricher.voice_connections = tuple(
                    usable[path] if path in usable else owned_usable[path]
                    for path in resolved_voice_paths
                    if path in usable or path in owned_usable
                )
                voice_enricher.source_unavailable = (
                    voice_source_unavailable or len(usable) != len(borrowed_voice_paths)
                    or len(owned_usable) != len(owned_voice_paths)
                )
            refresh_voice_sources()
            identity = LiveIdentityAdapter(
                catalog,
                self_username=args.self_username,
                include_public_accounts=args.include_public_accounts,
                conversation_username=args.conversation_username,
            )
            for connection_id, device, inode in catalog_identities.values():
                identity._initial_shards[connection_id] = (device, inode)
            if name_query is not None:
                identity.contact_connection = contact_connection
                identity.contact_source_unavailable = contact_source_unavailable
                if contact_connection is not None:
                    identity._initial_shards[id(contact_connection)] = early_identity._initial_shards[id(contact_connection)]
            elif args.contact_database is not None:
                contact_path = args.contact_database
                try:
                    if contact_path.is_symlink() or not contact_path.is_file():
                        raise OSError("contact source is unavailable")
                    before = contact_path.lstat()
                    contact_connection = sqlite3.connect(
                        readonly_database_uri(contact_path), uri=True
                    )
                    contact_connection_owned = True
                    contact_connection.execute("PRAGMA query_only=ON")
                    after = contact_path.lstat()
                    if not stat.S_ISREG(after.st_mode) or (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
                        raise OSError('contact source changed')
                    identity._initial_shards.setdefault(id(contact_connection), (after.st_dev, after.st_ino))
                    identity.contact_connection = contact_connection
                except (OSError, sqlite3.Error):
                    if contact_connection is not None:
                        contact_connection.close()
                        contact_connection = None
                    contact_source_unavailable = True
                    identity.contact_source_unavailable = True
            elif args.encrypted_contact_database is not None:
                contact_path = args.encrypted_contact_database
                resolved_contact_path = None
                try:
                    resolved_contact_path = contact_path.resolve(strict=True)
                    resolved_contact_path.relative_to(root)
                    if contact_path.is_symlink() or not resolved_contact_path.is_file():
                        raise OSError("contact source is unavailable")
                    before = resolved_contact_path.lstat()
                    if not stat.S_ISREG(before.st_mode):
                        raise OSError("contact source is unavailable")
                    contact_connection = shared_source(resolved_contact_path)
                    if contact_connection is None:
                        contact_connection, _raw_key_bytes = open_encrypted_readonly(
                            resolved_contact_path, passphrase
                        )
                        contact_connection_owned = True
                    info = resolved_contact_path.lstat()
                    if not stat.S_ISREG(info.st_mode) or (before.st_dev, before.st_ino) != (info.st_dev, info.st_ino):
                        raise OSError("contact source changed")
                    identity._initial_shards.setdefault(id(contact_connection), (info.st_dev, info.st_ino))
                    identity.contact_connection = contact_connection
                except Exception:
                    if contact_connection is not None and contact_connection_owned:
                        contact_connection.close()
                    contact_connection = None
                    if resolved_contact_path is not None:
                        unavailable_source_paths.add(resolved_contact_path)
                    contact_source_unavailable = True
                    identity.contact_source_unavailable = True

            def ensure_contact_stable() -> None:
                if contact_epoch is not None and (identity.contact_connection is None or identity._epoch(identity.contact_connection) != contact_epoch):
                    raise ContactQueryError('contact_source_changed')
                if any(runtime._voice_epoch(shard) != epoch for shard,epoch in runtime._scope_epochs.values()):
                    raise LiveReadonlyError('Live message source changed')

            unknown_sources: set[SourceKey] = set()
            def selected_source(source: SourceKey) -> bool:
                username = identity.conversation_username(source)
                if username is None:
                    if args.include_related_groups:
                        unknown_sources.add(source)
                    return False
                return ((not args.locate_related_groups and username == target_resolution['selected_username']) or (args.include_related_groups and username.endswith('@chatroom')))
            image_key_codes: tuple[int, ...] = ()
            image_account_scope = ""
            if args.image_key_config is not None:
                args._diagnostic_stage = "configuration"
                config_path = args.image_key_config
                if (
                    config_path.is_symlink()
                    or not config_path.is_file()
                    or (config_path.stat().st_mode & 0o077)
                    or config_path.stat().st_size > 16_384
                ):
                    raise LiveReadonlyError("Image key configuration must be a private regular file")
                try:
                    image_config = json.loads(config_path.read_text(encoding="utf-8"))
                    codes = image_config["key_codes"]
                    scope = image_config["account_scope"]
                    if not isinstance(codes, list) or not codes or len(codes) > 256 or any(type(code) is not int or code < 0 for code in codes):
                        raise ValueError
                    if not isinstance(scope, str) or not scope or len(scope) > 256:
                        raise ValueError
                except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
                    raise LiveReadonlyError("Image key configuration is invalid") from exc
                image_key_codes = tuple(codes)
                image_account_scope = scope
            args._diagnostic_stage = "result_processing"
            image_enricher = None
            if image_source_unavailable:
                image_enricher = lambda _source, _message, _conversation: {
                    "status": "unavailable", "reason": "image_source_unavailable"
                }
            if all(image_options):
                args._diagnostic_stage = "configuration"
                image_db = args.image_database.resolve()
                attachments_root = args.image_attachments_root.resolve()
                image_output_root = args.image_output_root.resolve()
                try:
                    image_db.relative_to(root)
                except ValueError as exc:
                    raise LiveReadonlyError("Image resource database must be under the live DB root") from exc
                if args.image_database.is_symlink() or not image_db.is_file():
                    image_source_unavailable = True
                elif args.image_attachments_root.is_symlink() or not attachments_root.is_dir():
                    image_source_unavailable = True
                elif image_output_root == root or image_output_root.is_relative_to(root):
                    raise LiveReadonlyError("Image output must be outside the live DB root")
                else:
                    args._diagnostic_stage = "result_processing"
                    try:
                        image_before = image_db.lstat()
                        if not stat.S_ISREG(image_before.st_mode):
                            raise OSError("image source is unavailable")
                        image_connection = shared_source(image_db)
                        if (image_connection is None and contact_connection is not None
                                and args.encrypted_contact_database is not None
                                and image_db == resolved_contact_path):
                            if identity._initial_shards.get(id(contact_connection)) != (image_before.st_dev, image_before.st_ino):
                                raise OSError("shared source changed")
                            image_connection = contact_connection
                        if image_connection is None:
                            image_connection, _raw_key_bytes = open_encrypted_readonly(image_db, passphrase)
                            image_connection_owned = True
                        image_info = image_db.lstat()
                        if not stat.S_ISREG(image_info.st_mode) or (image_before.st_dev, image_before.st_ino) != (image_info.st_dev, image_info.st_ino):
                            raise OSError("image source changed")
                        image_identity = (image_info.st_dev, image_info.st_ino)
                        image_from_catalog = any(shard.path == image_db and shard.connection is image_connection for shard in catalog.shards)
                    except Exception:
                        if image_connection is not None and image_connection_owned:
                            image_connection.close()
                        image_connection = None
                        image_source_unavailable = True
                if image_source_unavailable:
                    image_enricher = lambda _source, _message, _conversation: {
                        "status": "unavailable", "reason": "image_source_unavailable"
                    }
                elif image_connection is not None:
                    from .image_runtime import ImageMessageIdentity, enrich_bound_images

                    def image_source_epoch():
                        try:
                            info = image_db.lstat()
                            if (not stat.S_ISREG(info.st_mode) or (info.st_dev, info.st_ino) != image_identity
                                    or (image_from_catalog and not any(shard.path == image_db and shard.connection is image_connection for shard in catalog.shards))):
                                return None
                            if image_connection_owned:
                                return (info.st_dev, info.st_ino)
                            stamps = []
                            for path in (image_db, Path(str(image_db) + '-wal')):
                                try:
                                    stamp = path.lstat()
                                except FileNotFoundError:
                                    stamps.append(None)
                                    continue
                                if not stat.S_ISREG(stamp.st_mode):
                                    return None
                                stamps.append((stamp.st_dev, stamp.st_ino, stamp.st_size, stamp.st_mtime_ns, stamp.st_ctime_ns))
                            return (int(image_connection.execute('PRAGMA data_version').fetchone()[0]), tuple(stamps))
                        except Exception:
                            return None

                    def image_enricher(source: SourceKey, message: RawMessage, _conversation_id: str):
                        before = image_source_epoch()
                        if before is None:
                            return {"status": "unavailable", "reason": "image_source_unavailable"}
                        username = identity.conversation_username(source)
                        if username is None or message.server_id is None:
                            return {"status": "unavailable", "reason": "image_message_identity_unavailable"}
                        image = enrich_bound_images(
                            image_connection,
                            ImageMessageIdentity(
                                username,
                                message.server_id,
                                message.local_id,
                                message.create_time,
                                message.local_type,
                                ZoneInfo(args.timezone),
                            ),
                            attachments_root=attachments_root,
                            output_root=image_output_root,
                            key_codes=image_key_codes,
                            account_scope=image_account_scope,
                            ffmpeg=args.image_ffmpeg,
                        )
                        if image_source_epoch() != before:
                            return {"status": "unavailable", "reason": "image_source_unavailable"}
                        return image
            args._diagnostic_stage = "result_processing"
            runtime = CodexFirstRuntime(
                catalog,
                event_store,
                agent_store,
                account_id=args.account_id,
                source_generation=args.source_generation,
                capability_check=lambda _catalog: identity.evidence(),
                voice_enricher=voice_enricher,
                image_enricher=image_enricher,
                attachment_resolver=attachment_resolver,
                source_filter=(
                    selected_source if args.contact_query is not None else
                    (lambda source: identity.matches_conversation(source, args.conversation_username))
                    if args.conversation_username else None
                ),
                batch_size=args.batch_size,
                voice_sources_refresh=refresh_voice_sources,
                scan_start=identity.begin_scan,
                _locate_only=args.locate_related_groups,
            )
            runtime._scan_workers = _WINDOW_SHARD_WORKERS
            runtime._empty_window_enabled = target_resolution is None and args.conversation_username is None
            if args.conversation_username is not None:
                runtime._conversation_target = args.conversation_username
                runtime._contact_guard = ensure_contact_stable
            if args.contact_query is not None:
                runtime._contact_target = target_resolution['selected_username']
                runtime._contact_related = args.include_related_groups
                runtime._contact_unknown_sources = unknown_sources
                runtime._contact_guard = ensure_contact_stable
                runtime._contact_binding = contact_epoch[2] if contact_epoch is not None else None
                runtime._contact_group_source = lambda source: bool((identity.conversation_username(source) or '').endswith('@chatroom'))
            for connection_id, device, inode in catalog_identities.values():
                runtime._bound_shards[connection_id] = (device, inode)

            if args.locate_related_groups:
                identity.begin_scan()
                ensure_contact_stable()
                initial_source_state = runtime._source_state()
                if initial_source_state is None:
                    raise LiveReadonlyError('Live message source changed')
                runtime._prepare_contact_scope(window)
                ensure_contact_stable()
                if runtime._source_state() != initial_source_state:
                    raise LiveReadonlyError('Live message source changed')
                found = {}
                for source in runtime._scope_senders:
                    username = identity.conversation_username(source)
                    if not isinstance(username, str) or not username.endswith('@chatroom'):
                        raise ContactQueryError('contact_source_changed')
                    name = identity._contact_display_name(username)
                    if name == username:
                        name = None
                    found[username] = {'conversation_username': username, 'display_name': name}
                gaps = ['conversation_attribution_unverified', 'timestamp_integrity_unverified',
                        'source_catalog_coverage_unverified', *runtime._scope_gaps]
                if any(group['display_name'] is None for group in found.values()):
                    gaps.append('group_name_unknown')
                if unknown_sources:
                    gaps.append('related_source_identity_unknown')
                if any(source.account_id == args.account_id and source.source_generation == args.source_generation for source in event_store.source_catalog_sources('unavailable')):
                    gaps.append('known_source_missing')
                ensure_contact_stable()
                if runtime._source_state() != initial_source_state:
                    raise LiveReadonlyError('Live message source changed')
                result = {'status': 'partial', 'coverage_status': 'partial', 'target_resolution': target_resolution,
                          'requested': {'start': _iso(window.start), 'end': _iso(window.end)},
                          'groups': [found[key] for key in sorted(found)], 'gaps': list(dict.fromkeys(gaps))}
                if args.voice_jsonl:
                    args._stream_completion = {'phase': 'complete', **result}
                else:
                    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
                return 0

            def ensure_latest_stable():
                if args.latest is not None:
                    runtime._ensure_latest_source_state()
                    if any(runtime._voice_epoch(shard) != epoch for shard, epoch in runtime._scope_epochs.values()):
                        raise ContactQueryError('source_changed')

            def mark_latest(payload):
                if args.latest is not None:
                    payload['selected_time_bounds'] = payload['requested'] if payload['total_count'] else {'start': None, 'end': None}
                    payload['requested'] = {'start': None, 'end': None}
                    payload['query_mode'] = {'kind': 'latest', 'limit': args.latest,
                                             'same_second_order': 'not_guaranteed',
                                             'scope': 'configured_current_sources'}

            def emit_messages(initial: ScanResult) -> None:
                ensure_contact_stable()
                ensure_latest_stable()
                initial_payload = _result_payload(initial)
                mark_latest(initial_payload)
                if target_resolution is not None:
                    initial_payload['target_resolution'] = target_resolution
                if runtime._query_failures:
                    initial_payload["diagnostics"] = {"failures": list(runtime._query_failures)}
                print(
                    json.dumps(
                        {"phase": "messages", "result": initial_payload},
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    flush=True,
                )
                args._stream_started = True

            def emit_voice(event_id: str, voice: Mapping[str, Any]) -> None:
                ensure_contact_stable()
                ensure_latest_stable()
                print(
                    json.dumps(
                        {"phase": "voice", "event_id": event_id, "voice": voice},
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    flush=True,
                )

            def emit_image(event_id: str, image: Mapping[str, Any]) -> None:
                ensure_contact_stable()
                ensure_latest_stable()
                print(
                    json.dumps(
                        {"phase": "image", "event_id": event_id, "image": image},
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    flush=True,
                )

            result = runtime.scan(
                window,
                display_limit=max(args.display_limit, args.latest or 0),
                confirmation_token=args.confirmation_token,
                on_base_messages=emit_messages if args.voice_jsonl else None,
                on_voice_result=emit_voice if args.voice_jsonl else None,
                on_image_result=emit_image if args.voice_jsonl else None,
                _latest_limit=args.latest,
            )
            ensure_contact_stable()
            ensure_latest_stable()
            payload = _result_payload(result)
            mark_latest(payload)
            if target_resolution is not None:
                payload['target_resolution'] = target_resolution
            if runtime._query_failures:
                payload["diagnostics"] = {"failures": list(runtime._query_failures)}
            payload["contact_name_source"] = identity.contact_source_status()
            output_contact_epoch = getattr(identity, '_contact_schema_epoch', None)
            preflight = runtime.output_preflight(window) if result.status != 'unavailable' else None
            confirmation = _confirmation_payload(preflight, window, args.timezone) if preflight is not None else None
            ensure_contact_stable()
            ensure_latest_stable()
            if args.voice_jsonl:
                if output_contact_epoch is not None and (identity.contact_connection is None or identity._epoch(identity.contact_connection) != output_contact_epoch):
                    raise ContactQueryError('contact_source_changed')
                args._stream_completion = {
                    "phase": "complete",
                    "status": result.status,
                    **({'error_code': result.error_code, 'gaps': list(result.coverage.gaps)} if result.status == 'unavailable' else {}),
                    "confirmation": confirmation,
                    "contact_name_source": payload["contact_name_source"],
                    **({'query_mode': payload['query_mode']} if args.latest is not None else {}),
                    **({'target_resolution': target_resolution} if target_resolution is not None else {}),
                    **({"diagnostics": {"failures": list(runtime._query_failures)}} if runtime._query_failures else {}),
                }
                ensure_contact_stable()
            elif args.json:
                payload["confirmation"] = confirmation
                print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            else:
                print("CODEX_FIRST_SCAN " + result.status.upper())
                if args.latest is not None:
                    print('QUERY_MODE latest; same-second order is not guaranteed')
                if confirmation is not None:
                    print("CONFIRMATION_REQUIRED " + json.dumps(confirmation, ensure_ascii=False, sort_keys=True))
                print(
                    "REQUESTED "
                    + str(payload["requested"]["start"])
                    + " "
                    + str(payload["requested"]["end"])
                )
                print(
                    "ACTUAL "
                    + str(payload["actual"]["start"])
                    + " "
                    + str(payload["actual"]["end"])
                )
                print("TOTAL " + str(payload["total_count"]))
                if payload["gaps"]:
                    print("GAPS " + ",".join(payload["gaps"]))
                for failure in runtime._query_failures:
                    print("DIAGNOSTIC " + failure["stage"] + " " + failure["reason"])
                for group in payload["groups"]:
                    print(json.dumps(group, ensure_ascii=False, sort_keys=True))
            return 0
    finally:
        active_error = sys.exc_info()[0] is not None
        cleanup_error = None
        closers = [connection.close for connection in voice_connections]
        if image_connection is not None and image_connection_owned:
            closers.append(image_connection.close)
        if catalog is not None:
            closers.append(catalog.close)
        if contact_connection is not None and contact_connection_owned:
            closers.append(contact_connection.close)
        for close in closers:
            try:
                close()
            except BaseException as error:
                if cleanup_error is None:
                    cleanup_error = error
        if cleanup_error is not None and not active_error:
            raise cleanup_error


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    identity_error: str | None = None
    try:
        result = _run_cli(args)
        resolution_output = getattr(args, '_contact_resolution_output', None)
        if resolution_output is not None:
            text = json.dumps(resolution_output, ensure_ascii=False, sort_keys=True)
            print(text if args.json else 'TARGET_RESOLUTION ' + text)
        completion = getattr(args, '_stream_completion', None)
        if completion is not None:
            print(json.dumps(completion, sort_keys=True), flush=True)
            args._stream_complete = True
        return result
    except ContactQueryError as error:
        category = str(error) if str(error) in {'contact_query_required', 'contact_query_empty', 'contact_query_with_check_config', 'contact_source_changed', 'contact_target_unsupported', 'source_changed', 'locate_requires_contact_and_groups', 'locate_media_or_check_unsupported', 'latest_requires_private_contact', 'latest_order_unavailable', 'latest_source_unavailable'} else 'contact_source_unavailable'
        reason = category
        args._target_resolution = {'status': 'unavailable', 'reason': category}
    except SourceSetupError as error:
        category = "configuration_unavailable"
        if str(error) in {"self_identity_binding_mismatch", "self_identity_conflict", "self_identity_invalid", "self_username_invalid"}:
            identity_error = str(error)
        reason = _diagnostic_reason(error)
    except (
        AgentStoreError,
        CodexFirstError,
        EventStoreError,
        FileNotFoundError,
        LiveReadonlyError,
        OSError,
        ValueError,
    ) as error:
        category = "configuration_unavailable"
        reason = _diagnostic_reason(error)
    except Exception as error:
        category = "runtime_unavailable"
        reason = _diagnostic_reason(error)
    failure = {"stage": getattr(args, "_diagnostic_stage", "configuration"), "reason": reason}
    if args.voice_jsonl and (getattr(args, '_stream_started', False) or args.contact_query is not None or args.group_query is not None or args.conversation_username is not None or args.locate_related_groups) and not getattr(args, '_stream_complete', False):
        print(json.dumps({'phase': 'complete', 'status': 'unavailable', 'error_code': category,
                          **({'target_resolution': args._target_resolution} if hasattr(args, '_target_resolution') else {}),
                          'diagnostics': {'failures': [failure]}}, sort_keys=True), flush=True)
        args._stream_complete = True
    if args.json and not (args.locate_related_groups and args.voice_jsonl and getattr(args, '_stream_complete', False)):
        print(
            json.dumps(
                {
                    "status": "unavailable", "error_code": category,
                    "diagnostics": {"failures": [failure]},
                    **({'target_resolution': args._target_resolution} if hasattr(args, '_target_resolution') else {}),
                    **({"configuration_detail": identity_error, "action": "confirm_self_identity"} if identity_error else {}),
                },
                sort_keys=True,
            )
        )
    else:
        print("CODEX_FIRST_SCAN UNAVAILABLE", file=sys.stderr)
        print("ERROR " + category, file=sys.stderr)
        print("DIAGNOSTIC " + failure["stage"] + " " + failure["reason"], file=sys.stderr)
        if identity_error:
            print("IDENTITY " + identity_error + "; confirm identity with wechat-local-setup", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
