"""Resolve only the identity rows needed by one bounded live query.

The adapter reads a chat's exact Name2Id row and the current message's sender
row from its already query-only shard. It never reads message bodies or dumps a
contact/session inventory. Direction stays unknown without an explicit self
username or a unique sender mapping.
"""

from __future__ import annotations

import hashlib
import math
import os
import stat
import xml.etree.ElementTree as ET
from typing import Any

from .common import quote_identifier
from .event_normalizer import _standard_id
from .event_store import RawMessage, SourceKey
from .live_catalog import LiveCatalog
from .target_resolver import _kind


MAX_SOURCE_INPUT_BYTES = 262_144
MAX_SOURCE_OUTPUT_BYTES = 2_000_000


def _name2id_columns(connection: Any) -> bool:
    try:
        row = connection.execute("PRAGMA query_only").fetchone()
        if not row or int(row[0]) != 1:
            return False
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='Name2Id'"
        ).fetchone()
        if not exists:
            return False
        columns = {str(item[1]) for item in connection.execute("PRAGMA table_info(Name2Id)").fetchall()}
        return "user_name" in columns
    except Exception:
        return False


def _username_for_table(connection: Any, table_name: str, *, schema_verified: bool = False) -> str | None:
    if (not schema_verified and not _name2id_columns(connection)) or len(table_name) != 36 or not table_name.startswith("Msg_"):
        return None
    try:
        connection.create_function(
            "wa_message_table",
            1,
            lambda value: "Msg_" + hashlib.md5(value.encode("utf-8")).hexdigest()
            if isinstance(value, str)
            else "",
            deterministic=True,
        )
        rows = connection.execute(
            "SELECT user_name FROM Name2Id WHERE wa_message_table(user_name)=? LIMIT 2",
            (table_name,),
        ).fetchall()
    except Exception:
        return None
    if len(rows) != 1 or not isinstance(rows[0][0], str) or not rows[0][0]:
        return None
    return rows[0][0]


class LiveIdentityAdapter:
    """Build safe capability callbacks for one explicitly scoped live scan."""

    def __init__(
        self,
        catalog: LiveCatalog,
        self_username: str | None = None,
        *,
        include_public_accounts: bool = True,
        conversation_username: str | None = None,
        contact_connection: Any | None = None,
        contact_source_unavailable: bool = False,
    ) -> None:
        self.catalog = catalog
        self.self_username = self_username
        self.include_public_accounts = include_public_accounts
        self.selected_conversation_username = conversation_username
        self.contact_connection = contact_connection
        self.contact_source_unavailable = contact_source_unavailable
        self._contact_schema_usable: bool | None = None
        self._conversation_names: dict[str, str] = {}
        self._sender_cache: dict[tuple[Any, ...], str | None] = {}
        self._initial_shards = {}
        for shard in catalog.shards:
            try:
                info = shard.path.lstat()
                self._initial_shards[id(shard.connection)] = (info.st_dev, info.st_ino)
            except OSError:
                pass

    def begin_scan(self) -> None:
        self._scan_cache_active = True
        self._lookup_cache = {}
        self._connection_main_id = dict(self._initial_shards)
        self._connection_paths = {}
        self._sender_cache.clear()
        self._conversation_names.clear()
        self._contact_schema_usable = None
        self._contact_schema_epoch = None
        self._contact_columns = set()

    def _epoch(self, connection: Any) -> tuple | None:
        try:
            version = int(connection.execute("PRAGMA data_version").fetchone()[0])
            if connection not in self._connection_paths:
                self._connection_paths[connection] = next(
                    str(row[2]) for row in connection.execute("PRAGMA database_list").fetchall() if row[1] == 'main'
                )
            main = self._connection_paths[connection]
            stamps = []
            for path in ((main, main + '-wal') if main else ()):
                try:
                    info = os.lstat(path)
                except FileNotFoundError:
                    stamps.append(None)
                    continue
                if not stat.S_ISREG(info.st_mode):
                    return None
                stamps.append((info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns))
            if main:
                identity = stamps[0][:2] if stamps[0] else None
                initial = self._connection_main_id.setdefault(id(connection), identity)
                if identity is None or initial != identity:
                    return None
            return (id(connection), version, tuple(stamps))
        except Exception:
            return None

    def _once(self, connection: Any, key: tuple, operation: Any, *, epoch: tuple | None = None) -> Any:
        if not getattr(self, '_scan_cache_active', False):
            return operation()
        before = epoch if epoch is not None else self._epoch(connection)
        if before is None:
            return None
        cache_key = (before, key)
        if cache_key not in self._lookup_cache:
            value = operation()
            if self._epoch(connection) != before:
                return None
            self._lookup_cache[cache_key] = value
        return self._lookup_cache[cache_key]

    def _username(self, source: SourceKey, connection: Any) -> str | None:
        def read() -> str | None:
            if not self._name2id_schema(connection):
                return None
            return _username_for_table(connection, source.table_name, schema_verified=True)

        routes = getattr(self.catalog, '_routes', None)
        if not getattr(self, '_scan_cache_active', False) or not isinstance(routes, dict):
            return self._once(connection, ('conversation', source.table_name), read)
        if self.selected_conversation_username is not None:
            try:
                selected = 'Msg_' + hashlib.md5(self.selected_conversation_username.encode('utf-8')).hexdigest()
            except (AttributeError, UnicodeError):
                return None
            if source.table_name != selected:
                return self._once(connection, ('conversation', source.table_name), read)

        def read_batch() -> dict[str, str | None] | None:
            if not self._name2id_schema(connection):
                return None
            targets = {
                table for table, shards in routes.items()
                if any(shard.connection is connection for shard in shards)
            }
            found: dict[str, str | None] = {}
            try:
                if self.selected_conversation_username is not None:
                    selected = 'Msg_' + hashlib.md5(self.selected_conversation_username.encode('utf-8')).hexdigest()
                    targets.intersection_update((selected,))
                for (value,) in connection.execute('SELECT user_name FROM Name2Id'):
                    if not isinstance(value, str):
                        continue
                    table = 'Msg_' + hashlib.md5(value.encode('utf-8')).hexdigest()
                    if table in targets:
                        found[table] = None if table in found or not value else value
            except Exception:
                return None
            return found

        names = self._once(connection, ('conversation-batch',), read_batch)
        return names.get(source.table_name) if names is not None else None

    def _name2id_schema(self, connection: Any) -> bool:
        return bool(self._once(connection, ('name2id-schema',), lambda: _name2id_columns(connection)))

    def _connection(self, source: SourceKey) -> Any | None:
        shard = self.catalog.shard_for_source(source)
        if shard is None:
            return None
        expected = self._initial_shards.get(id(shard.connection))
        if expected is not None:
            try:
                info = shard.path.lstat()
                if not stat.S_ISREG(info.st_mode) or (info.st_dev, info.st_ino) != expected:
                    return None
            except OSError:
                return None
        return shard.connection

    def direction(self, source: SourceKey, message: RawMessage) -> str:
        connection = self._connection(source)
        if connection is None:
            return "unknown"
        conversation_username = self._username(source, connection)
        if conversation_username is not None:
            conversation_id = _standard_id(
                "conv_",
                "standard-conversation-v1",
                source.account_id,
                source.source_generation,
                source.table_name,
            )
            self._conversation_names[conversation_id] = conversation_username
        if message.real_sender_id is None:
            return "unknown"
        sender = self.sender_identity(source, message)
        if sender is None or self.self_username is None:
            return "unknown"
        return "self" if sender == self.self_username else "inbound"

    def sender_identity(self, source: SourceKey, message: RawMessage) -> str | None:
        """Return a sender only when one exact Name2Id row proves it."""
        connection = self._connection(source)
        if connection is None or message.real_sender_id is None:
            return None
        key = ('sender', source.shard_name, source.table_name, message.real_sender_id)
        def read() -> str | None:
            if not self._name2id_schema(connection):
                return None
            try:
                rows = connection.execute(
                    "SELECT user_name FROM Name2Id WHERE rowid=? LIMIT 2",
                    (message.real_sender_id,),
                ).fetchall()
                return str(rows[0][0]) if len(rows) == 1 and isinstance(rows[0][0], str) and rows[0][0] else None
            except Exception:
                return None
        if getattr(self, '_scan_cache_active', False):
            return self._once(connection, key, read)
        if key not in self._sender_cache:
            self._sender_cache[key] = read()
        return self._sender_cache[key]

    def sender_display_name(self, source: SourceKey, message: RawMessage) -> str | None:
        username = self.sender_identity(source, message)
        return self._contact_display_name(username) if username is not None else None

    @staticmethod
    def _mention_facts(message: RawMessage, self_username: str | None) -> dict[str, Any]:
        unknown = {
            "status": "unknown", "at_self": "unknown",
            "at_all": "unknown", "other_count": "unknown",
        }
        raw = message.raw_source
        if raw is None or message.source_compression_type not in (None, 0, 4):
            return unknown
        try:
            if isinstance(raw, str):
                decoded = raw
            else:
                packed = bytes(raw)
                if len(packed) > MAX_SOURCE_INPUT_BYTES:
                    return unknown
                if message.source_compression_type == 4:
                    import zstandard
                    frame_size = zstandard.frame_content_size(packed)
                    if frame_size == zstandard.CONTENTSIZE_ERROR:
                        return unknown
                    if (
                        frame_size != zstandard.CONTENTSIZE_UNKNOWN
                        and frame_size > MAX_SOURCE_OUTPUT_BYTES
                    ):
                        return unknown
                    raw_decoded = zstandard.ZstdDecompressor().decompress(
                        packed,
                        max_output_size=MAX_SOURCE_OUTPUT_BYTES + 1,
                        allow_extra_data=False,
                    )
                    if len(raw_decoded) > MAX_SOURCE_OUTPUT_BYTES:
                        return unknown
                    decoded = raw_decoded.decode("utf-8")
                else:
                    if len(packed) > MAX_SOURCE_OUTPUT_BYTES:
                        return unknown
                    decoded = packed.decode("utf-8")
            if len(decoded) > MAX_SOURCE_OUTPUT_BYTES:
                return unknown
            from .event_normalizer import _parse_xml
            parsed = _parse_xml(decoded)
            root = parsed.root
            if root is None:
                return unknown
            tokens: list[str] = []
            for tag in ("atuserlist", "at_user_list"):
                for node in root.iter(tag):
                    if node.text:
                        tokens.extend(item.strip() for item in node.text.split(",") if item.strip())
            unique_tokens = list(dict.fromkeys(tokens))
            has_self = self_username is not None and self_username in unique_tokens
            has_all = "notify@all" in unique_tokens
            other_count = sum(
                token not in {self_username, "notify@all"}
                for token in unique_tokens
            )
            return {
                "status": "available",
                "at_self": ("yes" if has_self else "no") if self_username is not None else "unknown",
                "at_all": "yes" if has_all else "no",
                "other_count": other_count,
            }
        except Exception:
            return unknown

    @staticmethod
    def _json_safe_source_value(value: Any) -> int | float | bool | str:
        if type(value) in (int, bool):
            return value
        if type(value) is float and math.isfinite(value):
            return value
        if isinstance(value, str) and len(value) <= 256:
            try:
                value.encode("utf-8")
            except UnicodeEncodeError:
                return "unknown"
            return value
        return "unknown"

    def source_facts(self, source: SourceKey, message: RawMessage) -> dict[str, Any]:
        username = self.conversation_username(source)
        status, epoch = self._contact_status_and_epoch()
        contact = {
            "source_status": status,
            "chat_room_notify_raw": "unknown",
            "verify_flag_raw": "unknown",
            "contact_local_type_raw": "unknown",
            "mute_status": "unknown",
            "conversation_subtype": "unknown",
        }
        if username is not None and status == "configured":
            record = self._contact_record(username, status=status, epoch=epoch)
            if record is not None:
                for column, output in (("chat_room_notify", "chat_room_notify_raw"),
                                       ("verify_flag", "verify_flag_raw"),
                                       ("local_type", "contact_local_type_raw")):
                    if column in record:
                        contact[output] = self._json_safe_source_value(record[column])
                notify = record.get('chat_room_notify')
                if username.endswith('@chatroom') and type(notify) is int and notify in (0, 1):
                    contact['mute_status'] = 'muted' if notify == 0 else 'unmuted'
        return {"contact": contact, "mentions": self._mention_facts(message, self.self_username)}

    def contact_source_status(self) -> str:
        return self._contact_status_and_epoch()[0]

    def _contact_status_and_epoch(self) -> tuple[str, tuple | None]:
        if self.contact_source_unavailable:
            return "unavailable", None
        connection = self.contact_connection
        if connection is None:
            return "not_configured", None
        epoch = None
        if getattr(self, '_scan_cache_active', False):
            epoch = self._epoch(connection)
            if epoch is None:
                return "unavailable", None
            if epoch != self._contact_schema_epoch:
                self._contact_schema_usable = None
                self._contact_schema_epoch = epoch
        if self._contact_schema_usable is None:
            try:
                query_only = connection.execute("PRAGMA query_only").fetchone()
                exists = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='contact'"
                ).fetchone()
                columns = {
                    str(row[1])
                    for row in connection.execute("PRAGMA table_info(\"contact\")").fetchall()
                }
                self._contact_schema_usable = bool(
                    query_only and int(query_only[0]) == 1 and exists
                    and {"username", "alias", "remark", "nick_name"}.issubset(columns)
                )
                self._contact_columns = columns
            except Exception:
                self._contact_schema_usable = False
        return ("configured" if self._contact_schema_usable else "unsupported"), epoch

    def _contact_record(self, username: str, *, status: str | None = None, epoch: tuple | None = None) -> dict[str, Any] | None:
        connection = self.contact_connection
        if status is None:
            status, epoch = self._contact_status_and_epoch()
        if connection is None or status != "configured":
            return None
        def read() -> dict[str, Any] | None:
            try:
                columns = (self._contact_columns if getattr(self, '_scan_cache_active', False)
                           else {str(row[1]) for row in connection.execute('PRAGMA table_info("contact")').fetchall()})
                selected = [name for name in ("alias", "remark", "nick_name", "chat_room_notify", "verify_flag", "local_type") if name in columns]
                rows = connection.execute(
                    "SELECT " + ", ".join(quote_identifier(name) for name in selected)
                    + ' FROM "contact" WHERE username=? LIMIT 2', (username,),
                ).fetchall()
                return dict(zip(selected, rows[0])) if len(rows) == 1 else None
            except Exception:
                return None
        return self._once(connection, ('contact', username), read, epoch=epoch)

    def _contact_display_name(self, username: str) -> str | None:
        status, epoch = self._contact_status_and_epoch()
        if status in {"unavailable", "unsupported"}:
            return None
        if status == "not_configured":
            return username
        record = self._contact_record(username, status=status, epoch=epoch)
        if record is None:
            return None
        return next((value.strip() for value in (record.get("remark"), record.get("nick_name"), record.get("alias"))
                     if isinstance(value, str) and value.strip()), username)

    def display_name(self, conversation_id: str) -> str | None:
        username = self._conversation_names.get(conversation_id)
        if username is None:
            return None
        return self._contact_display_name(username)

    def conversation_username(self, source: SourceKey) -> str | None:
        connection = self._connection(source)
        username = (
            self._username(source, connection)
            if connection is not None
            else None
        )
        if username is not None:
            conversation_id = _standard_id(
                "conv_",
                "standard-conversation-v1",
                source.account_id,
                source.source_generation,
                source.table_name,
            )
            self._conversation_names[conversation_id] = username
        return username

    def conversation_kind(self, conversation_id: str) -> str:
        username = self._conversation_names.get(conversation_id)
        if username is None:
            return "未知"
        kind = _kind(username)
        return "私聊或其他（未区分）" if kind == "私聊/其他" else kind

    def include_source(self, source: SourceKey) -> bool:
        connection = self._connection(source)
        username = (
            self._username(source, connection)
            if connection is not None
            else None
        )
        # Unknown conversation identity must not discard original text. It is
        # retained as unknown, while media adapters independently fail closed.
        if username is None:
            return True
        return self.include_public_accounts or _kind(username) != "公众号"

    def matches_conversation(self, source: SourceKey, username: str) -> bool:
        """Exact conversation bound evaluated before preflight/content reads."""
        if isinstance(username, str):
            try:
                if source.table_name != 'Msg_' + hashlib.md5(username.encode('utf-8')).hexdigest():
                    return False
            except UnicodeError:
                return False
        return self.conversation_username(source) == username

    def evidence(self) -> SourceCapabilityEvidence:
        from .codex_first_runtime import SourceCapabilityEvidence
        # Capability is available only when the source schema can support a
        # bounded lookup; each individual unmapped row remains explicitly unknown.
        schema_available = any(
            self._name2id_schema(shard.connection) for shard in self.catalog.shards
        )
        return SourceCapabilityEvidence(
            direction_proven=self.self_username is not None and schema_available,
            contact_mapping_proven=schema_available,
            display_name=self.display_name,
            direction_for_message=self.direction,
            sender_identity=self.sender_identity,
            sender_display_name=self.sender_display_name,
            source_facts=self.source_facts,
            conversation_identity_for_source=self.conversation_username,
            conversation_kind=self.conversation_kind,
            include_source=self.include_source,
        )
