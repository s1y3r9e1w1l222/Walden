"""Durably tail one live WeChat message table by monotonically increasing id."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal

from .common import quote_identifier
from .event_store import EventStore, Parser, RawMessage, ScanLeaseFence, SourceKey
from .live_readonly import (
    LiveReadonlyError,
    open_encrypted_readonly,
    read_private_passphrase,
    validate_live_storage_paths,
)


InitialPosition = Literal["beginning", "latest"]
REQUIRED_COLUMNS = {
    "local_id",
    "local_type",
    "create_time",
    "real_sender_id",
    "message_content",
    "WCDB_CT_message_content",
}
SOURCE_TIMESTAMP_LOWER_BOUND = 946_684_800  # 2000-01-01T00:00:00Z
SOURCE_TIMESTAMP_UPPER_BOUND = 4_102_444_800  # 2100-01-01T00:00:00Z, exclusive


def _optional_source_compression_type(value: Any) -> int | None:
    """Keep malformed optional metadata row-local without weakening parsing."""
    if value is None:
        return None
    if type(value) is int:
        return value
    # Unsupported sentinel: source facts will be unknown, while the message's
    # required body columns remain available for the normal event path.
    return -1


class LiveTailerError(RuntimeError):
    """Raised when one table cannot be tailed without weakening guarantees."""


class SourceCursorRegression(LiveTailerError):
    """Raised when a source maximum is below its persisted cursor."""


@dataclass(frozen=True)
class TailResult:
    initialized: bool
    initial_position: str
    scanned_rows: int
    inserted_events: int
    inserted_revisions: int
    parse_errors: int
    cursor: int
    observed_max_local_id: int


@dataclass(frozen=True)
class WindowReadResult:
    """Read-only result for one bounded source-table time window.

    Time bounds describe rows returned for this request only.  Invalid rows
    elsewhere in the source are deliberately not counted by a window read.
    """

    messages: list[RawMessage]
    source_min_time: int | None
    source_max_time: int | None
    examined_count: int
    invalid_timestamp_count: int | None
    voice_packed: dict[int, tuple[Any, ...]] = field(default_factory=dict)


def _require_query_only(conn: Any) -> None:
    row = conn.execute("PRAGMA query_only").fetchone()
    if not row or int(row[0]) != 1:
        raise LiveTailerError("Source connection must have query_only enabled")


def _table_columns(conn: Any, table_name: str) -> dict[str, tuple[Any, ...]]:
    rows = conn.execute(
        f"PRAGMA table_info({quote_identifier(table_name)})"
    ).fetchall()
    if not rows:
        raise LiveTailerError("Message table does not exist")
    columns = {str(row[1]): tuple(row) for row in rows}
    missing = REQUIRED_COLUMNS - set(columns)
    if missing:
        raise LiveTailerError(
            f"Message table is missing required columns: {sorted(missing)}"
        )
    if int(columns["local_id"][5] or 0) < 1:
        raise LiveTailerError("local_id must be the message table primary key")
    return columns


def _source_max_local_id(conn: Any, table_name: str) -> int:
    row = conn.execute(
        f"SELECT max(local_id) FROM {quote_identifier(table_name)}"
    ).fetchone()
    if row is None:
        raise LiveTailerError("Source maximum observation returned no result")
    return int(row[0] or 0)


def observe_source_max_local_id(conn: Any, table_name: str) -> int:
    """Make one validated read-only MAX observation for explicit initialization."""

    _require_query_only(conn)
    _table_columns(conn, table_name)
    observed = _source_max_local_id(conn, table_name)
    if observed is None:
        raise LiveTailerError("Source maximum observation returned no result")
    return observed


def _read_after_cursor(
    conn: Any,
    table_name: str,
    columns: dict[str, tuple[Any, ...]],
    cursor: int,
    batch_size: int,
) -> list[RawMessage]:
    server_id_expression = (
        quote_identifier("server_id") if "server_id" in columns else "NULL"
    )
    rows = conn.execute(
        f"""
        SELECT local_id, {server_id_expression}, local_type, create_time,
               real_sender_id, message_content, WCDB_CT_message_content
        FROM {quote_identifier(table_name)}
        WHERE local_id > ?
        ORDER BY local_id
        LIMIT ?
        """,
        (cursor, batch_size),
    ).fetchall()
    return [
        RawMessage(
            local_id=int(local_id),
            server_id=int(server_id) if server_id is not None else None,
            local_type=int(local_type),
            create_time=int(create_time or 0),
            real_sender_id=int(real_sender_id) if real_sender_id is not None else None,
            raw_content=(
                bytes(raw_content)
                if isinstance(raw_content, memoryview)
                else raw_content
            ),
            compression_type=(
                int(compression_type) if compression_type is not None else None
            ),
        )
        for (
            local_id,
            server_id,
            local_type,
            create_time,
            real_sender_id,
            raw_content,
            compression_type,
        ) in rows
    ]


def _empty_window_probe(conn: Any, table_name: str, *, start: int, end: int) -> tuple[bool, bool]:
    """Return only empty-window and no-future proofs from valid timestamps."""
    _require_query_only(conn)
    _table_columns(conn, table_name)
    table = quote_identifier(table_name)
    row = conn.execute(
        f"SELECT MAX(create_time) FROM {table} WHERE typeof(create_time)='integer' "
        f"AND create_time >= {SOURCE_TIMESTAMP_LOWER_BOUND} AND create_time < {SOURCE_TIMESTAMP_UPPER_BOUND}"
    ).fetchone()
    if row is None:
        raise LiveTailerError('Timestamp metadata unavailable')
    maximum = row[0]
    return (maximum is None or maximum < start, maximum is None or maximum <= end)


def read_message_window(
    conn: Any,
    table_name: str,
    *,
    start_unix_seconds: int,
    end_unix_seconds: int,
    batch_size: int,
    collect_voice_packed: bool = False,
    _cancelled: Callable[[], bool] | None = None,
    _sender_id: int | None = None,
) -> WindowReadResult:
    """Read source rows in an inclusive create-time window without mutation.

    The source connection must already be query-only. Rows are fetched in
    stable ``local_id`` keyset batches so a large result never requires an
    unbounded query or offset scan. No cursor or event-store state is read or
    changed by this operation.
    """

    if not isinstance(start_unix_seconds, int) or not isinstance(
        end_unix_seconds, int
    ):
        raise ValueError("window bounds must be integer Unix seconds")
    if start_unix_seconds > end_unix_seconds:
        raise ValueError("start_unix_seconds must not exceed end_unix_seconds")
    if batch_size < 1 or batch_size > 10_000:
        raise ValueError("batch_size must be between 1 and 10000")

    _require_query_only(conn)
    columns = _table_columns(conn, table_name)
    quoted_table = quote_identifier(table_name)
    server_id_expression = (
        quote_identifier("server_id") if "server_id" in columns else "NULL"
    )
    source_available = {"source", "WCDB_CT_source"}.issubset(columns)
    source_expression = quote_identifier("source") if source_available else "NULL"
    source_compression_expression = (
        quote_identifier("WCDB_CT_source") if source_available else "NULL"
    )
    packed_required = {"local_id", "server_id", "local_type", "create_time", "real_sender_id", "packed_info_data"}
    packed_available = collect_voice_packed and packed_required.issubset(columns)
    if packed_available:
        from .voice_transcript import MAX_PACKED_INFO_BYTES

        voice_condition = '(local_type & 4294967295)=34'
        packed_expression = (f'CASE WHEN {voice_condition} AND typeof("packed_info_data")=\'blob\' '
                             f'AND length("packed_info_data")<={MAX_PACKED_INFO_BYTES} THEN "packed_info_data" END')
        packed_type = f'CASE WHEN {voice_condition} THEN typeof("packed_info_data") END'
        packed_length = f'CASE WHEN {voice_condition} THEN length("packed_info_data") END'
    else:
        packed_expression = packed_type = packed_length = 'NULL'

    messages: list[RawMessage] = []
    voice_packed: dict[int, tuple[Any, ...]] = {}
    last_local_id: int | None = None
    while True:
        if _cancelled is not None and _cancelled():
            raise InterruptedError('Window read cancelled')
        local_id_predicate = "" if last_local_id is None else " AND local_id > ?"
        query_params: tuple[int, ...]
        if last_local_id is None:
            query_params = (start_unix_seconds, end_unix_seconds, batch_size)
        else:
            query_params = (
                start_unix_seconds,
                end_unix_seconds,
                last_local_id,
                batch_size,
            )
        sender_predicate = ''
        if _sender_id is not None:
            sender_predicate = " AND typeof(real_sender_id)='integer' AND real_sender_id=?"
            query_params = (*query_params[:-1], _sender_id, query_params[-1])
        rows = conn.execute(
            f"""
            SELECT local_id, {server_id_expression}, local_type, create_time,
                   real_sender_id, message_content, WCDB_CT_message_content,
                   {source_expression}, {source_compression_expression}, {packed_expression}, {packed_type}, {packed_length}
            FROM {quoted_table}
            WHERE typeof(create_time)='integer'
              AND create_time >= {SOURCE_TIMESTAMP_LOWER_BOUND}
              AND create_time < {SOURCE_TIMESTAMP_UPPER_BOUND}
              AND create_time >= ? AND create_time <= ?{local_id_predicate}{sender_predicate}
            ORDER BY local_id ASC
            LIMIT ?
            """,
            query_params,
        ).fetchall()
        if not rows:
            break
        for (
            local_id,
            server_id,
            local_type,
            create_time,
            real_sender_id,
            raw_content,
            compression_type,
            raw_source,
            source_compression_type,
            packed_info,
            packed_kind,
            packed_size,
        ) in rows:
            if collect_voice_packed and (int(local_type) & 0xFFFFFFFF) == 34:
                voice_packed[int(local_id)] = (packed_available, server_id, local_type, create_time, real_sender_id, packed_info, packed_kind, packed_size)
            messages.append(
                RawMessage(
                    local_id=int(local_id),
                    server_id=int(server_id) if server_id is not None else None,
                    local_type=int(local_type),
                    create_time=int(create_time or 0),
                    real_sender_id=(
                        int(real_sender_id) if real_sender_id is not None else None
                    ),
                    raw_content=(
                        bytes(raw_content)
                        if isinstance(raw_content, memoryview)
                        else raw_content
                    ),
                    compression_type=(
                        int(compression_type)
                        if compression_type is not None
                        else None
                    ),
                    raw_source=(
                        bytes(raw_source)
                        if isinstance(raw_source, memoryview)
                        else raw_source
                    ),
                    source_compression_type=(
                        _optional_source_compression_type(source_compression_type)
                    ),
                )
            )
        last_local_id = int(rows[-1][0])
        if len(rows) < batch_size:
            break

    return WindowReadResult(
        messages=messages,
        source_min_time=min((message.create_time for message in messages), default=None),
        source_max_time=max((message.create_time for message in messages), default=None),
        examined_count=len(messages),
        invalid_timestamp_count=None,
        voice_packed=voice_packed,
    )


def tail_message_table(
    conn: Any,
    store: EventStore,
    source: SourceKey,
    *,
    initial_position: InitialPosition,
    batch_size: int = 500,
    parser: Parser | None = None,
    scan_fence: ScanLeaseFence | None = None,
) -> TailResult:
    """Read and atomically persist one batch from one physical message table.

    ``initial_position`` is mandatory even when a cursor already exists so the
    first-run policy is never selected implicitly.  Existing cursors always win.
    """

    if initial_position not in ("beginning", "latest"):
        raise ValueError("initial_position must be 'beginning' or 'latest'")
    if batch_size < 1 or batch_size > 10_000:
        raise ValueError("batch_size must be between 1 and 10000")
    _require_query_only(conn)
    columns = _table_columns(conn, source.table_name)
    observed_max = _source_max_local_id(conn, source.table_name)
    if observed_max is None:
        raise LiveTailerError("Source maximum observation returned no result")

    cursor = store.get_cursor(source)
    initialized = cursor is None
    if cursor is None:
        seed = observed_max if initial_position == "latest" else 0
        cursor = store.initialize_cursor(source, seed, initial_position)
        if initial_position == "latest":
            return TailResult(
                initialized=True,
                initial_position=initial_position,
                scanned_rows=0,
                inserted_events=0,
                inserted_revisions=0,
                parse_errors=0,
                cursor=cursor,
                observed_max_local_id=observed_max,
            )

    if observed_max < cursor:
        store.mark_replacement_suspected(
            source,
            "SourceCursorRegression",
            scan_fence=scan_fence,
        )
        raise SourceCursorRegression(
            "Source maximum local_id is below the persisted cursor; "
            "use a new source generation after verifying database replacement"
        )

    messages = _read_after_cursor(
        conn, source.table_name, columns, cursor, batch_size
    )
    stored = store.commit_messages(
        source, messages, parser, scan_fence=scan_fence
    )
    return TailResult(
        initialized=initialized,
        initial_position=initial_position,
        scanned_rows=len(messages),
        inserted_events=stored.inserted_events,
        inserted_revisions=stored.inserted_revisions,
        parse_errors=stored.parse_errors,
        cursor=stored.cursor,
        observed_max_local_id=observed_max,
    )


def _inferred_live_root(database: Path) -> Path:
    resolved = database.resolve()
    for parent in resolved.parents:
        if parent.name == "db_storage":
            return parent
    return resolved.parent


def _validate_event_store_location(
    database: Path,
    event_store: Path,
    live_db_root: Path | None = None,
) -> None:
    source = database.resolve()
    destination = event_store.resolve()
    if destination == source:
        raise LiveTailerError("Event store must be separate from the source database")
    root = live_db_root.resolve() if live_db_root is not None else _inferred_live_root(source)
    try:
        validate_live_storage_paths(root, [source], destination)
    except LiveReadonlyError as exc:
        raise LiveTailerError(str(exc)) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Capture one batch from one live message table into a private event store"
    )
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--passphrase", type=Path, required=True)
    parser.add_argument("--event-store", type=Path, required=True)
    parser.add_argument(
        "--live-db-root",
        type=Path,
        help=(
            "Optional compatibility hardening for Phase 1; defaults to the "
            "nearest db_storage ancestor or the source directory"
        ),
    )
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--source-generation", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument(
        "--initial-position",
        choices=("beginning", "latest"),
        required=True,
        help="Required first-run choice: backfill the table or start after its current maximum",
    )
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    database = args.database.resolve()
    event_store_path = args.event_store.resolve()
    _validate_event_store_location(database, event_store_path, args.live_db_root)
    passphrase = read_private_passphrase(args.passphrase)
    conn, _raw_key_form = open_encrypted_readonly(database, passphrase)
    try:
        source = SourceKey(
            account_id=args.account_id,
            source_generation=args.source_generation,
            shard_name=database.name,
            table_name=args.table,
        )
        with EventStore(event_store_path) as store:
            result = tail_message_table(
                conn,
                store,
                source,
                initial_position=args.initial_position,
                batch_size=args.batch_size,
            )
    finally:
        conn.close()

    payload = asdict(result)
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print("LIVE_TABLE_TAIL OK")
        print(f"INITIALIZED {int(result.initialized)}")
        print(f"SCANNED_ROWS {result.scanned_rows}")
        print(f"INSERTED_EVENTS {result.inserted_events}")
        print(f"INSERTED_REVISIONS {result.inserted_revisions}")
        print(f"PARSE_ERRORS {result.parse_errors}")
        print(f"CURSOR {result.cursor}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
