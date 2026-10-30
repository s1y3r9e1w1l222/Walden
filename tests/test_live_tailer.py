from __future__ import annotations

import hashlib
import sqlite3
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from wechat_local.event_store import EventStore, EventStoreError, RawMessage, SourceKey
from wechat_local.live_tailer import (
    LiveTailerError,
    SourceCursorRegression,
    read_message_window,
    tail_message_table,
)


TABLE = "Msg_" + hashlib.md5(b"synthetic-contact").hexdigest()


class LiveTailerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.source_path = self.root / "message_7.db"
        self.store_path = self.root / "private" / "live_events.db"
        self.source = SourceKey(
            account_id="synthetic-account",
            source_generation="generation-1",
            shard_name=self.source_path.name,
            table_name=TABLE,
        )
        self._create_source()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _create_source(self) -> None:
        with sqlite3.connect(self.source_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE \"{TABLE}\" (
                    local_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    server_id INTEGER,
                    local_type INTEGER NOT NULL,
                    create_time INTEGER NOT NULL,
                    real_sender_id INTEGER,
                    message_content BLOB,
                    WCDB_CT_message_content INTEGER
                )
                """
            )

    def _insert(self, timestamp: int, content: str, local_type: int = 1) -> int:
        with sqlite3.connect(self.source_path) as conn:
            cursor = conn.execute(
                f"""
                INSERT INTO \"{TABLE}\" (
                    server_id, local_type, create_time, real_sender_id,
                    message_content, WCDB_CT_message_content
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (timestamp * 10, local_type, timestamp, 1, content, 0),
            )
            return int(cursor.lastrowid)

    def _readonly_source(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            f"file:{self.source_path.resolve()}?mode=ro", uri=True
        )
        conn.execute("PRAGMA query_only = ON")
        return conn

    def _source_digest(self) -> str:
        return hashlib.sha256(self.source_path.read_bytes()).hexdigest()

    def test_beginning_reads_once_and_restart_does_not_duplicate(self) -> None:
        for timestamp in (100, 101, 102):
            self._insert(timestamp, f"message-{timestamp}")
        digest_before = self._source_digest()

        with self._readonly_source() as source_conn, EventStore(self.store_path) as store:
            first = tail_message_table(
                source_conn,
                store,
                self.source,
                initial_position="beginning",
            )
            second = tail_message_table(
                source_conn,
                store,
                self.source,
                initial_position="beginning",
            )
            self.assertEqual(first.inserted_events, 3)
            self.assertEqual(first.cursor, 3)
            self.assertEqual(second.inserted_events, 0)
            self.assertEqual(store.count_events(), 3)
            self.assertEqual(store.count_revisions(), 3)
            self.assertEqual(store.count_pending_tasks(), 3)

        with self._readonly_source() as source_conn, EventStore(self.store_path) as store:
            restarted = tail_message_table(
                source_conn,
                store,
                self.source,
                initial_position="beginning",
            )
            self.assertEqual(restarted.inserted_events, 0)
            self.assertEqual(store.get_cursor(self.source), 3)
            self.assertEqual(store.count_events(), 3)

        self.assertEqual(self._source_digest(), digest_before)

    def test_latest_is_an_explicit_non_backfill_initialization(self) -> None:
        self._insert(100, "existing-1")
        self._insert(101, "existing-2")
        with self._readonly_source() as source_conn, EventStore(self.store_path) as store:
            initialized = tail_message_table(
                source_conn,
                store,
                self.source,
                initial_position="latest",
            )
            self.assertTrue(initialized.initialized)
            self.assertEqual(initialized.inserted_events, 0)
            self.assertEqual(initialized.cursor, 2)

        self._insert(102, "future")
        with self._readonly_source() as source_conn, EventStore(self.store_path) as store:
            captured = tail_message_table(
                source_conn,
                store,
                self.source,
                initial_position="latest",
            )
            self.assertEqual(captured.inserted_events, 1)
            self.assertEqual(captured.cursor, 3)

    def test_old_timestamp_with_new_local_id_is_captured(self) -> None:
        self._insert(500, "newer-time")
        with self._readonly_source() as source_conn, EventStore(self.store_path) as store:
            tail_message_table(
                source_conn,
                store,
                self.source,
                initial_position="beginning",
            )

        old_time_id = self._insert(50, "old-time-new-id")
        with self._readonly_source() as source_conn, EventStore(self.store_path) as store:
            result = tail_message_table(
                source_conn,
                store,
                self.source,
                initial_position="beginning",
            )
            self.assertEqual(old_time_id, 2)
            self.assertEqual(result.inserted_events, 1)
            self.assertEqual(result.cursor, 2)

    def test_parser_failure_saves_raw_row_and_advances_cursor(self) -> None:
        self._insert(100, "good")
        self._insert(101, "bad")
        self._insert(102, "still-good")

        def parser(message):
            if message.local_id == 2:
                raise ValueError("message contents must not enter diagnostics")
            return {"kind": "text", "length": len(message.raw_content or "")}

        with self._readonly_source() as source_conn, EventStore(self.store_path) as store:
            result = tail_message_table(
                source_conn,
                store,
                self.source,
                initial_position="beginning",
                parser=parser,
            )
            revisions = store.revision_statuses()
            self.assertEqual(result.inserted_events, 3)
            self.assertEqual(result.parse_errors, 1)
            self.assertEqual(result.cursor, 3)
            self.assertEqual(
                [row["parse_status"] for row in revisions],
                ["parsed", "error", "parsed"],
            )
            self.assertEqual(revisions[1]["parse_error"], "ValueError")

        with sqlite3.connect(
            f"file:{self.store_path.resolve()}?mode=ro", uri=True
        ) as event_conn:
            raw_content, raw_kind = event_conn.execute(
                """
                SELECT raw_content, raw_content_kind
                FROM event_revisions ORDER BY id LIMIT 1 OFFSET 1
                """
            ).fetchone()
        self.assertEqual(raw_content, "bad")
        self.assertEqual(raw_kind, "text")

    def test_cursor_failure_rolls_back_events_revisions_and_tasks(self) -> None:
        self._insert(100, "one")
        with self._readonly_source() as source_conn, EventStore(self.store_path) as store:
            store.initialize_cursor(self.source, 0, "beginning")
            with patch.object(
                store,
                "_advance_cursor",
                side_effect=RuntimeError("synthetic crash"),
            ):
                with self.assertRaisesRegex(RuntimeError, "synthetic crash"):
                    tail_message_table(
                        source_conn,
                        store,
                        self.source,
                        initial_position="beginning",
                    )
            self.assertEqual(store.get_cursor(self.source), 0)
            self.assertEqual(store.count_events(), 0)
            self.assertEqual(store.count_revisions(), 0)
            self.assertEqual(store.count_pending_tasks(), 0)

    def test_event_identity_includes_source_generation(self) -> None:
        self._insert(100, "same-local-id")
        generation_two = SourceKey(
            account_id=self.source.account_id,
            source_generation="generation-2",
            shard_name=self.source.shard_name,
            table_name=self.source.table_name,
        )
        with self._readonly_source() as source_conn, EventStore(self.store_path) as store:
            tail_message_table(
                source_conn,
                store,
                self.source,
                initial_position="beginning",
            )
            tail_message_table(
                source_conn,
                store,
                generation_two,
                initial_position="beginning",
            )
            self.assertEqual(store.count_events(), 2)

    def test_changed_raw_row_creates_revision_without_changing_event_identity(self) -> None:
        self._insert(100, "original")
        with self._readonly_source() as source_conn, EventStore(self.store_path) as store:
            tail_message_table(
                source_conn,
                store,
                self.source,
                initial_position="beginning",
            )
            store.commit_messages(
                self.source,
                [
                    RawMessage(
                        local_id=1,
                        server_id=1000,
                        local_type=10002,
                        create_time=100,
                        real_sender_id=1,
                        raw_content="recalled",
                        compression_type=0,
                    )
                ],
            )
            self.assertEqual(store.count_events(), 1)
            self.assertEqual(store.count_revisions(), 2)
            self.assertEqual(store.count_pending_tasks(), 2)
            self.assertEqual(store.get_cursor(self.source), 1)
            self.assertEqual(store.integrity_check(), "ok")

    def test_event_store_permissions_and_unrelated_database_rejection(self) -> None:
        with EventStore(self.store_path) as store:
            self.assertEqual(store.integrity_check(), "ok")
        self.assertEqual(stat.S_IMODE(self.store_path.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.store_path.parent.stat().st_mode), 0o700)

        unrelated = self.root / "unrelated.db"
        with sqlite3.connect(unrelated) as conn:
            conn.execute("CREATE TABLE unrelated (value TEXT)")
        with self.assertRaisesRegex(EventStoreError, "non-empty"):
            EventStore(unrelated)
        with sqlite3.connect(unrelated) as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        self.assertEqual(tables, {"unrelated"})

    def test_source_connection_must_already_be_query_only(self) -> None:
        with sqlite3.connect(self.source_path) as source_conn, EventStore(
            self.store_path
        ) as store:
            with self.assertRaisesRegex(LiveTailerError, "query_only"):
                tail_message_table(
                    source_conn,
                    store,
                    self.source,
                    initial_position="beginning",
                )

    def test_window_read_uses_inclusive_create_time_bounds_without_cursor_mutation(self) -> None:
        base = 1_700_000_000
        self._insert(base - 1, "before")
        self._insert(base, "inside")
        self._insert(base + 1, "after")
        digest_before = self._source_digest()

        source = self._readonly_source()
        statements = []
        source.set_trace_callback(statements.append)
        result = read_message_window(
            source,
            TABLE,
            start_unix_seconds=base,
            end_unix_seconds=base,
            batch_size=10,
        )

        self.assertEqual([message.raw_content for message in result.messages], ["inside"])
        self.assertEqual(result.source_min_time, base)
        self.assertEqual(result.source_max_time, base)
        self.assertEqual(result.examined_count, 1)
        self.assertIsNone(result.invalid_timestamp_count)
        selects = [statement.lower() for statement in statements if "from \"" + TABLE.lower() + "\"" in statement.lower()]
        self.assertTrue(selects)
        self.assertTrue(all("where" in statement for statement in selects))
        self.assertFalse(any("min(" in statement or "sum(" in statement for statement in selects))

    def test_window_read_carries_optional_source_only_for_rows_in_window(self) -> None:
        with sqlite3.connect(self.source_path) as conn:
            conn.execute(f'ALTER TABLE "{TABLE}" ADD COLUMN source BLOB')
            conn.execute(f'ALTER TABLE "{TABLE}" ADD COLUMN WCDB_CT_source INTEGER')
        before = self._insert(1_699_999_999, "before")
        inside = self._insert(1_700_000_000, "inside")
        after = self._insert(1_700_000_001, "after")
        with sqlite3.connect(self.source_path) as conn:
            conn.execute(f'UPDATE "{TABLE}" SET source=?, WCDB_CT_source=4 WHERE local_id=?', (b"before-source", before))
            conn.execute(f'UPDATE "{TABLE}" SET source=?, WCDB_CT_source=4 WHERE local_id=?', (b"inside-source", inside))
            conn.execute(f'UPDATE "{TABLE}" SET source=?, WCDB_CT_source=4 WHERE local_id=?', (b"after-source", after))
        digest_before = self._source_digest()

        result = read_message_window(
            self._readonly_source(), TABLE,
            start_unix_seconds=1_700_000_000,
            end_unix_seconds=1_700_000_000,
            batch_size=10,
        )

        self.assertEqual(len(result.messages), 1)
        self.assertEqual(result.messages[0].raw_content, "inside")
        self.assertEqual(result.messages[0].raw_source, b"inside-source")
        self.assertEqual(result.messages[0].source_compression_type, 4)
        self.assertEqual(self._source_digest(), digest_before)

    def test_invalid_optional_source_compression_flag_does_not_drop_message(self) -> None:
        with sqlite3.connect(self.source_path) as conn:
            conn.execute(f'ALTER TABLE "{TABLE}" ADD COLUMN source BLOB')
            conn.execute(f'ALTER TABLE "{TABLE}" ADD COLUMN WCDB_CT_source')
        local_id = self._insert(1_700_000_000, "original text")
        with sqlite3.connect(self.source_path) as conn:
            conn.execute(
                f'UPDATE "{TABLE}" SET source=?, WCDB_CT_source=? WHERE local_id=?',
                (b"<msg />", "invalid-source-flag", local_id),
            )

        result = read_message_window(
            self._readonly_source(), TABLE,
            start_unix_seconds=1_700_000_000,
            end_unix_seconds=1_700_000_000,
            batch_size=10,
        )

        self.assertEqual(len(result.messages), 1)
        self.assertEqual(result.messages[0].raw_content, "original text")
        self.assertEqual(result.messages[0].raw_source, b"<msg />")
        self.assertEqual(result.messages[0].source_compression_type, -1)

    def test_window_read_orders_equal_timestamps_by_local_id_and_excludes_old_timestamp_new_id(self) -> None:
        base = 1_700_000_000
        self._insert(base, "first")
        self._insert(base, "second")
        self._insert(base - 100, "old-time-new-id")

        result = read_message_window(
            self._readonly_source(),
            TABLE,
            start_unix_seconds=base,
            end_unix_seconds=base,
            batch_size=1,
        )

        self.assertEqual([message.local_id for message in result.messages], [1, 2])
        self.assertEqual([message.raw_content for message in result.messages], ["first", "second"])
        self.assertEqual(result.examined_count, 2)

    def test_window_read_includes_schema_valid_zero_local_id(self) -> None:
        base = 1_700_000_000
        with sqlite3.connect(self.source_path) as conn:
            conn.execute(
                f"""
                INSERT INTO "{TABLE}" (
                    local_id, server_id, local_type, create_time, real_sender_id,
                    message_content, WCDB_CT_message_content
                ) VALUES (0, 0, 1, ?, 1, 'zero', 0)
                """,
                (base,),
            )
            conn.commit()

        result = read_message_window(
            self._readonly_source(),
            TABLE,
            start_unix_seconds=base,
            end_unix_seconds=base,
            batch_size=1,
        )

        self.assertEqual([message.local_id for message in result.messages], [0])
        self.assertEqual(result.examined_count, 1)

    def test_window_read_counts_null_text_and_out_of_domain_timestamps(self) -> None:
        invalid_table = "Msg_" + hashlib.md5(b"invalid-time-fixture").hexdigest()
        base = 1_700_000_000
        with sqlite3.connect(self.source_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE "{invalid_table}" (
                    local_id INTEGER PRIMARY KEY,
                    server_id INTEGER,
                    local_type INTEGER NOT NULL,
                    create_time,
                    real_sender_id INTEGER,
                    message_content BLOB,
                    WCDB_CT_message_content INTEGER
                )
                """
            )
            conn.executemany(
                f"INSERT INTO \"{invalid_table}\" VALUES (?, ?, 1, ?, 1, ?, 0)",
                (
                    (1, 1, base, "valid"),
                    (2, 2, None, "null-time"),
                    (3, 3, "not-a-time", "text-time"),
                    (4, 4, 0, "out-of-domain-time"),
                ),
            )

        result = read_message_window(
            self._readonly_source(),
            invalid_table,
            start_unix_seconds=base,
            end_unix_seconds=base,
            batch_size=1,
        )

        self.assertEqual([message.raw_content for message in result.messages], ["valid"])
        self.assertEqual(result.source_min_time, base)
        self.assertEqual(result.source_max_time, base)
        self.assertIsNone(result.invalid_timestamp_count)

    def test_window_read_requires_query_only_and_valid_arguments(self) -> None:
        with sqlite3.connect(self.source_path) as source_conn:
            for kwargs in (
                {"start_unix_seconds": 201, "end_unix_seconds": 200, "batch_size": 10},
                {"start_unix_seconds": 200, "end_unix_seconds": 200, "batch_size": 0},
                {"start_unix_seconds": 200, "end_unix_seconds": 200, "batch_size": 10_001},
            ):
                with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                    read_message_window(source_conn, TABLE, **kwargs)

        with sqlite3.connect(self.source_path) as source_conn:
            with self.assertRaisesRegex(LiveTailerError, "query_only"):
                read_message_window(
                    source_conn,
                    TABLE,
                    start_unix_seconds=200,
                    end_unix_seconds=200,
                    batch_size=10,
                )

    def test_window_read_rejects_malformed_table(self) -> None:
        malformed = "Malformed"
        with sqlite3.connect(self.source_path) as conn:
            conn.execute(f'CREATE TABLE "{malformed}" (local_id INTEGER PRIMARY KEY)')
            conn.commit()
        with self._readonly_source() as source_conn:
            with self.assertRaisesRegex(LiveTailerError, "missing required columns"):
                read_message_window(
                    source_conn,
                    malformed,
                    start_unix_seconds=0,
                    end_unix_seconds=1,
                    batch_size=10,
                )

    def test_source_regression_is_rejected_without_cursor_rewind(self) -> None:
        self._insert(100, "one")
        self._insert(101, "two")
        with self._readonly_source() as source_conn, EventStore(self.store_path) as store:
            tail_message_table(
                source_conn,
                store,
                self.source,
                initial_position="beginning",
            )

        with sqlite3.connect(self.source_path) as conn:
            conn.execute(f'DELETE FROM "{TABLE}" WHERE local_id=2')
            conn.execute("DELETE FROM sqlite_sequence WHERE name=?", (TABLE,))

        with self._readonly_source() as source_conn, EventStore(self.store_path) as store:
            with self.assertRaises(SourceCursorRegression):
                tail_message_table(
                    source_conn,
                    store,
                    self.source,
                    initial_position="beginning",
                )
            self.assertEqual(store.get_cursor(self.source), 2)
            self.assertEqual(
                store.source_catalog_entry(self.source)["lifecycle"],
                "replacement_suspected",
            )

    def test_max_observation_errors_never_create_replacement_suspicion(self) -> None:
        self._insert(100, "one")
        self._insert(101, "two")
        with self._readonly_source() as source_conn, EventStore(self.store_path) as store:
            tail_message_table(
                source_conn,
                store,
                self.source,
                initial_position="beginning",
            )
            failures = (
                sqlite3.OperationalError("database is busy"),
                sqlite3.OperationalError("database is locked"),
                TimeoutError("source timeout"),
                sqlite3.DatabaseError("query failed"),
            )
            for failure in failures:
                with self.subTest(error=type(failure).__name__, detail=str(failure)):
                    with patch(
                        "wechat_local.live_tailer._source_max_local_id",
                        side_effect=failure,
                    ):
                        with self.assertRaises(type(failure)):
                            tail_message_table(
                                source_conn,
                                store,
                                self.source,
                                initial_position="beginning",
                            )
                    self.assertEqual(
                        store.source_catalog_entry(self.source)["lifecycle"],
                        "available",
                    )
                    self.assertEqual(store.get_cursor(self.source), 2)

            with patch(
                "wechat_local.live_tailer._source_max_local_id",
                return_value=None,
            ):
                with self.assertRaisesRegex(LiveTailerError, "no result"):
                    tail_message_table(
                        source_conn,
                        store,
                        self.source,
                        initial_position="beginning",
                    )
            self.assertEqual(
                store.source_catalog_entry(self.source)["lifecycle"], "available"
            )

            with patch(
                "wechat_local.live_tailer._table_columns",
                side_effect=sqlite3.DatabaseError("schema enumeration failed"),
            ):
                with self.assertRaises(sqlite3.DatabaseError):
                    tail_message_table(
                        source_conn,
                        store,
                        self.source,
                        initial_position="beginning",
                    )
            self.assertEqual(
                store.source_catalog_entry(self.source)["lifecycle"], "available"
            )


if __name__ == "__main__":
    unittest.main()
