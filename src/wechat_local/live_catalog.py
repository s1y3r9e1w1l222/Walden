"""Read-only static and dynamically refreshable live message catalogs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from .event_store import EventStore, SourceKey
from .live_readonly import open_encrypted_readonly
from .live_tailer import LiveTailerError, _table_columns


SHARD_NAME_PATTERN = re.compile(r"(?:biz_)?message_[0-9]+\.db")
TABLE_NAME_PATTERN = re.compile(r"Msg_[0-9a-f]{32}")


class LiveCatalogError(RuntimeError):
    """Raised when an authorized shard cannot be catalogued safely."""


def _require_query_only(conn: Any) -> None:
    row = conn.execute("PRAGMA query_only").fetchone()
    if not row or int(row[0]) != 1:
        raise LiveCatalogError("Message source connection must have query_only enabled")


@dataclass(frozen=True)
class LiveShard:
    path: Path
    connection: Any

    @property
    def shard_name(self) -> str:
        return self.path.name


@dataclass(frozen=True)
class CatalogRefreshResult:
    shard_count: int
    table_count: int
    new_shards: int
    disappeared_shards: int


ConnectionFactory = Callable[[Path], Any]


def _is_within(path: Path, directory: Path) -> bool:
    try:
        path.relative_to(directory)
    except ValueError:
        return False
    return True


def discover_numeric_shards(live_root: Path) -> tuple[Path, ...]:
    """Enumerate direct numeric message shard files in one explicit live root."""

    root = live_root.resolve()
    if not root.is_dir():
        raise LiveCatalogError("Live shard root must be an existing directory")
    found: list[Path] = []
    for candidate in live_root.iterdir():
        if SHARD_NAME_PATTERN.fullmatch(candidate.name) is None:
            continue
        if candidate.is_symlink():
            resolved = candidate.resolve()
            if not _is_within(resolved, root):
                raise LiveCatalogError("Message shard symbolic link escapes live root")
            raise LiveCatalogError("Message shard candidates must not be symbolic links")
        if not candidate.is_file():
            continue
        resolved = candidate.resolve()
        if not _is_within(resolved, root):
            raise LiveCatalogError("Message shard resolves outside live root")
        found.append(resolved)
    if len(set(found)) != len(found):
        raise LiveCatalogError("Discovered message shard paths must be unique")
    return tuple(sorted(found, key=lambda path: path.name))


class LiveCatalog:
    """A P2-compatible catalog with an optional dynamic discovery root."""

    def __init__(
        self,
        shards: Iterable[LiveShard] = (),
        *,
        live_root: Path | None = None,
        connection_factory: ConnectionFactory | None = None,
        store: EventStore | None = None,
        account_id: str | None = None,
        source_generation: str | None = None,
        _borrowed_connections: tuple[Any, ...] = (),
    ):
        items = tuple(shards)
        dynamic = live_root is not None or connection_factory is not None
        if dynamic and (live_root is None or connection_factory is None):
            raise LiveCatalogError(
                "Dynamic catalog requires both live_root and connection_factory"
            )
        if not items and not dynamic:
            raise LiveCatalogError("At least one message shard is required")
        persistence = (store, account_id, source_generation)
        if any(value is not None for value in persistence) and any(
            value is None for value in persistence
        ):
            raise LiveCatalogError(
                "Catalog persistence requires store, account_id, and source_generation"
            )

        self._live_root = live_root.resolve() if live_root is not None else None
        self._connection_factory = connection_factory
        self._store = store
        self._account_id = account_id
        self._source_generation = source_generation
        self._owns_connections = dynamic
        self._borrowed_connection_ids = {id(connection) for connection in _borrowed_connections}
        self._shards: tuple[LiveShard, ...] = ()
        self._routes: dict[str, tuple[LiveShard, ...]] = {}
        self._set_shards(items)
        if dynamic:
            self.refresh_catalog()

    @classmethod
    def dynamic(
        cls,
        live_root: Path,
        connection_factory: ConnectionFactory,
        *,
        store: EventStore,
        account_id: str,
        source_generation: str,
    ) -> "LiveCatalog":
        return cls(
            live_root=live_root,
            connection_factory=connection_factory,
            store=store,
            account_id=account_id,
            source_generation=source_generation,
        )

    @classmethod
    def dynamic_encrypted(
        cls,
        live_root: Path,
        passphrase: bytes,
        *,
        store: EventStore,
        account_id: str,
        source_generation: str,
        _thread_handoff: bool = False,
        _preopened: dict[Path, Any] | None = None,
    ) -> "LiveCatalog":
        """Build a dynamic catalog using the existing verified RO opener."""

        def open_source(path):
            if _preopened is not None and path in _preopened:
                return _preopened[path]
            return (open_encrypted_readonly(path, passphrase, _thread_handoff=True)
                    if _thread_handoff else open_encrypted_readonly(path, passphrase))

        return cls(
            live_root=live_root,
            connection_factory=open_source,
            store=store,
            account_id=account_id,
            source_generation=source_generation,
            _borrowed_connections=tuple(_preopened.values()) if _preopened else (),
        )

    def _set_shards(self, items: tuple[LiveShard, ...]) -> None:
        resolved_paths = tuple(item.path.resolve() for item in items)
        if len(set(resolved_paths)) != len(resolved_paths):
            raise LiveCatalogError("Message shard paths must be unique")
        shard_names = tuple(path.name for path in resolved_paths)
        if len(set(shard_names)) != len(shard_names):
            raise LiveCatalogError("Message shard basenames must be unique")
        if any(
            SHARD_NAME_PATTERN.fullmatch(name) is None
            for name in shard_names
        ):
            raise LiveCatalogError(
                "Only numeric message_<n>.db or biz_message_<n>.db shards are accepted"
            )

        routes: dict[str, list[LiveShard]] = {}
        normalized: list[LiveShard] = []
        for item, path in zip(items, resolved_paths):
            _require_query_only(item.connection)
            shard = LiveShard(path=path, connection=item.connection)
            normalized.append(shard)
            rows = item.connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table' AND name GLOB 'Msg_*'
                ORDER BY name
                """
            ).fetchall()
            for (table_name,) in rows:
                name = str(table_name)
                if TABLE_NAME_PATTERN.fullmatch(name) is None:
                    continue
                try:
                    _table_columns(item.connection, name)
                except LiveTailerError:
                    continue
                routes.setdefault(name, []).append(shard)

        self._shards = tuple(normalized)
        self._routes = {name: tuple(found) for name, found in routes.items()}

    def refresh_catalog(self) -> CatalogRefreshResult:
        """Perform one deterministic, authoritative shard/table refresh."""

        old_by_path = {shard.path: shard for shard in self._shards}
        if self._live_root is None:
            self._set_shards(self._shards)
            return CatalogRefreshResult(len(self._shards), len(self._routes), 0, 0)

        paths = discover_numeric_shards(self._live_root)
        new_items: list[LiveShard] = []
        opened: list[Any] = []
        try:
            for path in paths:
                existing = old_by_path.get(path)
                if existing is not None:
                    new_items.append(existing)
                    continue
                opened_value = self._connection_factory(path)
                connection = (
                    opened_value[0]
                    if isinstance(opened_value, tuple)
                    else opened_value
                )
                opened.append(connection)
                new_items.append(LiveShard(path, connection))
            previous = self._shards
            previous_routes = self._routes
            self._set_shards(tuple(new_items))
        except BaseException:
            for connection in reversed(opened):
                if id(connection) not in self._borrowed_connection_ids:
                    try:connection.close()
                    except BaseException:pass
            raise

        try:
            if self._store is not None:
                discovered = [
                    (shard.shard_name, table_name)
                    for table_name, shards in self._routes.items()
                    for shard in shards
                ]
                self._store.record_catalog_refresh(
                    self._account_id, self._source_generation, discovered
                )
        except BaseException:
            self._shards = previous
            self._routes = previous_routes
            for connection in reversed(opened):
                if id(connection) not in self._borrowed_connection_ids:
                    try:connection.close()
                    except BaseException:pass
            raise

        current_paths = {shard.path for shard in self._shards}
        disappeared = [
            shard for shard in previous if shard.path not in current_paths
        ]
        for shard in disappeared:
            if id(shard.connection) not in self._borrowed_connection_ids:
                shard.connection.close()
        return CatalogRefreshResult(
            shard_count=len(self._shards),
            table_count=len(self._routes),
            new_shards=sum(path not in old_by_path for path in paths),
            disappeared_shards=len(disappeared),
        )

    @property
    def shards(self) -> tuple[LiveShard, ...]:
        return self._shards

    def routes_for(self, table_name: str) -> tuple[LiveShard, ...]:
        routes = self.discovered_routes_for(table_name)
        if self._store is None:
            return routes
        return tuple(
            shard
            for shard in routes
            if self._store.source_is_runnable(
                SourceKey(
                    self._account_id,
                    self._source_generation,
                    shard.shard_name,
                    table_name,
                )
            )
        )

    def discovered_routes_for(self, table_name: str) -> tuple[LiveShard, ...]:
        return self._routes.get(table_name, ())

    def shard_for_source(self, source: SourceKey) -> LiveShard | None:
        for shard in self.discovered_routes_for(source.table_name):
            if shard.shard_name == source.shard_name:
                return shard
        return None

    def table_count(self) -> int:
        return len(self._routes)

    def close(self) -> None:
        if self._owns_connections:
            failure = None
            for shard in reversed(self._shards):
                if id(shard.connection) not in self._borrowed_connection_ids:
                    try:shard.connection.close()
                    except BaseException as error:
                        if failure is None:failure = error
            self._shards = ()
            self._routes = {}
            if failure is not None:raise failure
