from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from wechat_local.event_store import (
    APPLICATION_ID,
    CORE_SCHEMA,
    NORMALIZATION_SCHEMA,
    SCHEMA_VERSION,
    SESSION_SCHEMA,
    SOURCE_CATALOG_SCHEMA,
    EventStore,
    SourceKey,
)
from wechat_local.live_tailer import tail_message_table


class EventStoreV4MigrationTests(unittest.TestCase):
    def test_migrated_existing_cursor_remains_runnable_and_continues_tail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store_path = root / "events.db"
            source_path = root / "message_4.db"
            table = "Msg_" + hashlib.md5(b"existing-source").hexdigest()
            source = SourceKey(
                "account", "generation", source_path.name, table
            )
            with sqlite3.connect(source_path) as conn:
                conn.execute(
                    f"""
                    CREATE TABLE "{table}" (
                        local_id INTEGER PRIMARY KEY,
                        server_id INTEGER,
                        local_type INTEGER NOT NULL,
                        create_time INTEGER NOT NULL,
                        real_sender_id INTEGER,
                        message_content BLOB,
                        WCDB_CT_message_content INTEGER
                    )
                    """
                )
                conn.executemany(
                    f"""
                    INSERT INTO "{table}" VALUES (?, ?, 1, ?, 1, ?, 0)
                    """,
                    (
                        (1, 10, 10, "old-one"),
                        (2, 20, 20, "old-two"),
                        (3, 30, 30, "new-three"),
                    ),
                )
            with sqlite3.connect(store_path) as conn:
                conn.executescript(CORE_SCHEMA + SESSION_SCHEMA + NORMALIZATION_SCHEMA)
                conn.execute(f"PRAGMA application_id = {APPLICATION_ID}")
                conn.execute("PRAGMA user_version = 3")
                conn.execute(
                    "INSERT INTO cursors VALUES (?, ?, ?, ?, 2, 'beginning', 'init', 'updated')",
                    (source.account_id, source.source_generation, source.shard_name, table),
                )

            source_conn = sqlite3.connect(
                f"file:{source_path.resolve()}?mode=ro", uri=True
            )
            source_conn.execute("PRAGMA query_only = ON")
            try:
                with EventStore(store_path) as store:
                    catalog_row = store.source_catalog_entry(source)
                    self.assertEqual(store.get_cursor(source), 2)
                    self.assertEqual(catalog_row["lifecycle"], "available")
                    self.assertEqual(catalog_row["initialization_policy"], "beginning")
                    self.assertEqual(catalog_row["source_generation"], "generation")
                    self.assertTrue(store.source_is_runnable(source))

                    result = tail_message_table(
                        source_conn,
                        store,
                        source,
                        initial_position="latest",
                    )
                    self.assertFalse(result.initialized)
                    self.assertEqual(result.inserted_events, 1)
                    self.assertEqual(result.cursor, 3)
                    self.assertEqual(store.count_events(), 1)
            finally:
                source_conn.close()

    def test_v3_migration_failure_rolls_back_and_retry_is_safe(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.db"
            with sqlite3.connect(path) as conn:
                conn.executescript(CORE_SCHEMA + SESSION_SCHEMA + NORMALIZATION_SCHEMA)
                conn.execute(f"PRAGMA application_id = {APPLICATION_ID}")
                conn.execute("PRAGMA user_version = 3")
                conn.execute(
                    "INSERT INTO cursors VALUES (?, ?, ?, ?, 7, 'beginning', 'init', 'updated')",
                    ("account", "generation", "message_0.db", "Msg_hash"),
                )

            invalid = """
            CREATE TABLE migration_must_rollback (value TEXT);
            THIS IS NOT VALID SQL;
            """
            with patch("wechat_local.event_store.SOURCE_CATALOG_SCHEMA", invalid):
                with self.assertRaises(sqlite3.OperationalError):
                    EventStore(path)

            with sqlite3.connect(path) as conn:
                self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 3)
                self.assertIsNone(
                    conn.execute(
                        "SELECT name FROM sqlite_master "
                        "WHERE name='migration_must_rollback'"
                    ).fetchone()
                )
                self.assertEqual(conn.execute("SELECT last_local_id FROM cursors").fetchone()[0], 7)

            with EventStore(path) as store:
                source = SourceKey("account", "generation", "message_0.db", "Msg_hash")
                self.assertEqual(store.get_cursor(source), 7)
                self.assertTrue(store.source_is_runnable(source))

    def test_v3_to_v4_preserves_all_frozen_rows_and_existing_cursor(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.db"
            with sqlite3.connect(path) as conn:
                conn.executescript(CORE_SCHEMA + SESSION_SCHEMA + NORMALIZATION_SCHEMA)
                conn.execute(f"PRAGMA application_id = {APPLICATION_ID}")
                conn.execute("PRAGMA user_version = 3")
                conn.execute(
                    "INSERT INTO cursors VALUES (?, ?, ?, ?, 7, 'beginning', 'init', 'updated')",
                    ("account", "generation", "message_0.db", "Msg_hash"),
                )
                event_id = conn.execute(
                    """
                    INSERT INTO events (
                        event_uid, account_id, source_generation, shard_name,
                        table_name, local_id, first_seen_at
                    ) VALUES ('uid', 'account', 'generation', 'message_0.db',
                              'Msg_hash', 7, 'seen')
                    """
                ).lastrowid
                revision_id = conn.execute(
                    """
                    INSERT INTO event_revisions (
                        event_id, revision_number, content_fingerprint, server_id,
                        local_type, create_time, real_sender_id, raw_content,
                        raw_content_kind, compression_type, parse_status,
                        parse_error, parsed_payload_json, observed_at
                    ) VALUES (?, 1, 'fingerprint', '8', 1, 9, 1, 'raw',
                              'text', 0, 'parsed', '', '{}', 'observed')
                    """,
                    (event_id,),
                ).lastrowid
                conn.execute(
                    "UPDATE events SET current_revision_id=? WHERE id=?",
                    (revision_id, event_id),
                )
                outbox_id = conn.execute(
                    """
                    INSERT INTO processing_outbox (
                        revision_id, task_kind, status, attempt_count,
                        available_at, created_at, updated_at
                    ) VALUES (?, 'process_event', 'pending', 0, 'a', 'c', 'u')
                    """,
                    (revision_id,),
                ).lastrowid
                conn.execute(
                    """
                    INSERT INTO session_observations VALUES (
                        'account', 'generation', 'session.db', 'user', 'Msg_hash',
                        7, 70, 6, 60, 'init', 'updated'
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO session_scan_queue VALUES (
                        'account', 'generation', 'session.db', 'user', 'Msg_hash',
                        7, 70, 'pending', 0, 'available', NULL, NULL, '', 'c', 'u'
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO outbox_consumer_state (
                        outbox_id, consumer_name, contract_version,
                        normalizer_version, status, attempt_count, available_at,
                        lease_owner, lease_expires_at, last_error_type,
                        completed_at, created_at, updated_at
                    ) VALUES (?, 'consumer', 'contract-v1', 'normalizer-v1',
                              'pending', 0, 'available', NULL, NULL, '', NULL,
                              'created', 'updated')
                    """,
                    (outbox_id,),
                )
                payload = '{}'
                conn.execute(
                    """
                    INSERT INTO standard_event_materializations (
                        source_revision_id, event_id, subject_id,
                        contract_version, normalizer_name, normalizer_version,
                        canonical_payload_json, payload_sha256, quality_status,
                        created_at
                    ) VALUES (?, 'standard-event', 'subject', 'contract-v1',
                              'normalizer', 'normalizer-v1', ?, ?, 'normalized',
                              'created')
                    """,
                    (
                        revision_id,
                        payload,
                        hashlib.sha256(payload.encode()).hexdigest(),
                    ),
                )
                frozen_before = {
                    name: conn.execute(f"SELECT * FROM {name}").fetchall()
                    for name in (
                        "cursors", "events", "event_revisions", "processing_outbox",
                        "session_observations", "session_scan_queue",
                        "outbox_consumer_state", "standard_event_materializations",
                    )
                }

            with EventStore(path) as store:
                source = SourceKey("account", "generation", "message_0.db", "Msg_hash")
                self.assertEqual(store.get_cursor(source), 7)
                self.assertTrue(store.source_is_runnable(source))
                self.assertEqual(store.source_catalog_entry(source)["lifecycle"], "available")
            with EventStore(path):
                pass

            with sqlite3.connect(path) as conn:
                self.assertEqual(
                    conn.execute("PRAGMA user_version").fetchone()[0],
                    SCHEMA_VERSION,
                )
                frozen_after = {
                    name: conn.execute(f"SELECT * FROM {name}").fetchall()
                    for name in frozen_before
                }
                self.assertEqual(
                    conn.execute("SELECT count(*) FROM source_catalog").fetchone()[0], 1
                )
            self.assertEqual(frozen_after, frozen_before)
            self.assertGreaterEqual(SCHEMA_VERSION, 4)
            self.assertGreater(outbox_id, 0)

    def test_fresh_database_initializes_at_latest_schema(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "private" / "events.db"
            with EventStore(path):
                pass
            with sqlite3.connect(path) as conn:
                self.assertEqual(
                    conn.execute("PRAGMA user_version").fetchone()[0],
                    SCHEMA_VERSION,
                )
                self.assertIsNotNone(
                    conn.execute(
                        "SELECT name FROM sqlite_master WHERE name='source_catalog'"
                    ).fetchone()
                )

    def test_v4_to_latest_adds_window_ingest_metadata_without_rewriting_frozen_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.db"
            with sqlite3.connect(path) as conn:
                conn.executescript(
                    CORE_SCHEMA + SESSION_SCHEMA + NORMALIZATION_SCHEMA + SOURCE_CATALOG_SCHEMA
                )
                conn.execute(f"PRAGMA application_id = {APPLICATION_ID}")
                conn.execute("PRAGMA user_version = 4")
                conn.execute(
                    "INSERT INTO cursors VALUES (?, ?, ?, ?, 7, 'beginning', 'init', 'updated')",
                    ("account", "generation", "message_0.db", "Msg_hash"),
                )
                frozen_before = {
                    table: conn.execute(f"SELECT * FROM {table}").fetchall()
                    for table in ("cursors", "events", "event_revisions", "processing_outbox")
                }

            with EventStore(path):
                pass

            with sqlite3.connect(path) as conn:
                self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], SCHEMA_VERSION)
                self.assertEqual(
                    {
                        table: conn.execute(f"SELECT * FROM {table}").fetchall()
                        for table in frozen_before
                    },
                    frozen_before,
                )
                for table in ("window_ingest_runs", "window_ingest_outcomes"):
                    self.assertIsNotNone(
                        conn.execute(
                            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                            (table,),
                        ).fetchone()
                    )


if __name__ == "__main__":
    unittest.main()
