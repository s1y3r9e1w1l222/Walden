from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from wechat_local.event_store import (
    APPLICATION_ID,
    CORE_SCHEMA,
    SCHEMA_VERSION,
    SESSION_SCHEMA,
    EventStore,
    EventStoreError,
)


def _insert_phase1_fixture(conn: sqlite3.Connection) -> tuple[int, int, int]:
    conn.execute(
        """
        INSERT INTO cursors VALUES (
            'account', 'generation', 'message_0.db', 'Msg_hash',
            7, 'beginning', 'init', 'update'
        )
        """
    )
    event_id = int(
        conn.execute(
            """
            INSERT INTO events (
                event_uid, account_id, source_generation, shard_name,
                table_name, local_id, first_seen_at
            ) VALUES ('stable-uid', 'account', 'generation',
                      'message_0.db', 'Msg_hash', 7, 'seen')
            """
        ).lastrowid
    )
    revision_id = int(
        conn.execute(
            """
            INSERT INTO event_revisions (
                event_id, revision_number, content_fingerprint,
                server_id, local_type, create_time, real_sender_id,
                raw_content, raw_content_kind, compression_type,
                parse_status, parse_error, parsed_payload_json, observed_at
            ) VALUES (?, 1, 'stable-fingerprint', '8', 1, 9, 1,
                      'raw', 'text', 0, 'pending', '', NULL, 'observed')
            """,
            (event_id,),
        ).lastrowid
    )
    conn.execute(
        "UPDATE events SET current_revision_id=? WHERE id=?",
        (revision_id, event_id),
    )
    outbox_id = int(
        conn.execute(
            """
            INSERT INTO processing_outbox (
                revision_id, task_kind, status, attempt_count,
                available_at, created_at, updated_at
            ) VALUES (?, 'process_event', 'pending', 0, 'a', 'c', 'u')
            """,
            (revision_id,),
        ).lastrowid
    )
    return event_id, revision_id, outbox_id


def _table_names(path: Path) -> set[str]:
    with sqlite3.connect(path) as conn:
        return {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }


class EventStoreV3MigrationTests(unittest.TestCase):
    def test_fresh_database_initializes_directly_at_v3(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "private" / "events.db"
            with EventStore(path) as store:
                self.assertEqual(store.integrity_check(), "ok")

            with sqlite3.connect(path) as conn:
                self.assertEqual(
                    int(conn.execute("PRAGMA user_version").fetchone()[0]),
                    SCHEMA_VERSION,
                )
                self.assertEqual(
                    int(conn.execute("PRAGMA application_id").fetchone()[0]),
                    APPLICATION_ID,
                )
            tables = _table_names(path)
            self.assertIn("outbox_consumer_state", tables)
            self.assertIn("standard_event_materializations", tables)

    def test_v1_to_v3_is_additive_and_preserves_phase1_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "private" / "events.db"
            path.parent.mkdir()
            with sqlite3.connect(path) as conn:
                conn.executescript(CORE_SCHEMA)
                conn.execute(f"PRAGMA application_id = {APPLICATION_ID}")
                conn.execute("PRAGMA user_version = 1")
                event_id, revision_id, outbox_id = _insert_phase1_fixture(conn)
                outbox_before = conn.execute(
                    "SELECT * FROM processing_outbox WHERE id=?", (outbox_id,)
                ).fetchone()

            with EventStore(path) as store:
                self.assertEqual(store.count_events(), 1)
                self.assertEqual(store.count_revisions(), 1)
                self.assertEqual(store.count_pending_tasks(), 1)
                self.assertEqual(store.integrity_check(), "ok")

            with sqlite3.connect(path) as conn:
                self.assertEqual(
                    int(conn.execute("PRAGMA user_version").fetchone()[0]),
                    SCHEMA_VERSION,
                )
                event = conn.execute(
                    "SELECT id, event_uid, current_revision_id FROM events"
                ).fetchone()
                revision = conn.execute(
                    "SELECT id, content_fingerprint FROM event_revisions"
                ).fetchone()
                cursor = int(
                    conn.execute("SELECT last_local_id FROM cursors").fetchone()[0]
                )
                outbox_after = conn.execute(
                    "SELECT * FROM processing_outbox WHERE id=?", (outbox_id,)
                ).fetchone()

            self.assertEqual(event, (event_id, "stable-uid", revision_id))
            self.assertEqual(revision, (revision_id, "stable-fingerprint"))
            self.assertEqual(cursor, 7)
            self.assertEqual(outbox_after, outbox_before)
            tables = _table_names(path)
            self.assertIn("session_observations", tables)
            self.assertIn("session_scan_queue", tables)
            self.assertIn("outbox_consumer_state", tables)
            self.assertIn("standard_event_materializations", tables)

    def test_v2_to_v3_preserves_phase1_and_phase2_rows_and_reopens_safely(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "private" / "events.db"
            path.parent.mkdir()
            with sqlite3.connect(path) as conn:
                conn.executescript(CORE_SCHEMA + SESSION_SCHEMA)
                conn.execute(f"PRAGMA application_id = {APPLICATION_ID}")
                conn.execute("PRAGMA user_version = 2")
                event_id, revision_id, outbox_id = _insert_phase1_fixture(conn)
                conn.execute(
                    """
                    INSERT INTO session_observations (
                        account_id, source_generation, session_database_name,
                        username, table_name, seen_last_local_id,
                        seen_last_timestamp, completed_last_local_id,
                        completed_last_timestamp, initialized_at, updated_at
                    ) VALUES (
                        'account', 'generation', 'session.db', 'synthetic-user',
                        'Msg_hash', 7, 70, 6, 60, 'init', 'update'
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO session_scan_queue (
                        account_id, source_generation, session_database_name,
                        username, table_name, target_local_id, target_timestamp,
                        status, attempt_count, available_at, lease_owner,
                        lease_expires_at, last_error, created_at, updated_at
                    ) VALUES (
                        'account', 'generation', 'session.db', 'synthetic-user',
                        'Msg_hash', 7, 70, 'pending', 2, 'available', NULL,
                        NULL, 'SyntheticError', 'created', 'updated'
                    )
                    """
                )
                frozen_before = {
                    "cursor": conn.execute("SELECT * FROM cursors").fetchone(),
                    "event": conn.execute("SELECT * FROM events").fetchone(),
                    "revision": conn.execute(
                        "SELECT * FROM event_revisions"
                    ).fetchone(),
                    "outbox": conn.execute(
                        "SELECT * FROM processing_outbox"
                    ).fetchone(),
                    "observation": conn.execute(
                        "SELECT * FROM session_observations"
                    ).fetchone(),
                    "queue": conn.execute(
                        "SELECT * FROM session_scan_queue"
                    ).fetchone(),
                }

            with EventStore(path) as first:
                self.assertEqual(first.integrity_check(), "ok")
            with EventStore(path) as reopened:
                self.assertEqual(reopened.integrity_check(), "ok")

            with sqlite3.connect(path) as conn:
                frozen_after = {
                    "cursor": conn.execute("SELECT * FROM cursors").fetchone(),
                    "event": conn.execute("SELECT * FROM events").fetchone(),
                    "revision": conn.execute(
                        "SELECT * FROM event_revisions"
                    ).fetchone(),
                    "outbox": conn.execute(
                        "SELECT * FROM processing_outbox"
                    ).fetchone(),
                    "observation": conn.execute(
                        "SELECT * FROM session_observations"
                    ).fetchone(),
                    "queue": conn.execute(
                        "SELECT * FROM session_scan_queue"
                    ).fetchone(),
                }
                self.assertEqual(
                    int(conn.execute("PRAGMA user_version").fetchone()[0]),
                    SCHEMA_VERSION,
                )

            self.assertEqual(frozen_after, frozen_before)
            self.assertEqual(frozen_after["event"][0], event_id)
            self.assertEqual(frozen_after["revision"][0], revision_id)
            self.assertEqual(frozen_after["outbox"][0], outbox_id)

    def test_v2_migration_failure_rolls_back_and_can_be_retried(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "private" / "events.db"
            path.parent.mkdir()
            with sqlite3.connect(path) as conn:
                conn.executescript(CORE_SCHEMA + SESSION_SCHEMA)
                conn.execute(f"PRAGMA application_id = {APPLICATION_ID}")
                conn.execute("PRAGMA user_version = 2")
                _, _, outbox_id = _insert_phase1_fixture(conn)
                outbox_before = conn.execute(
                    "SELECT * FROM processing_outbox WHERE id=?", (outbox_id,)
                ).fetchone()

            invalid_migration = """
            CREATE TABLE migration_must_rollback (value TEXT);
            THIS IS NOT VALID SQL;
            """
            with patch(
                "wechat_local.event_store.NORMALIZATION_SCHEMA",
                invalid_migration,
            ):
                with self.assertRaises(sqlite3.OperationalError):
                    EventStore(path)

            with sqlite3.connect(path) as conn:
                self.assertEqual(
                    int(conn.execute("PRAGMA user_version").fetchone()[0]), 2
                )
                self.assertIsNone(
                    conn.execute(
                        """
                        SELECT name FROM sqlite_master
                        WHERE type='table' AND name='migration_must_rollback'
                        """
                    ).fetchone()
                )
                self.assertEqual(
                    conn.execute(
                        "SELECT * FROM processing_outbox WHERE id=?", (outbox_id,)
                    ).fetchone(),
                    outbox_before,
                )

            with EventStore(path) as retried:
                self.assertEqual(retried.integrity_check(), "ok")
            self.assertIn("outbox_consumer_state", _table_names(path))

    def test_unrelated_nonempty_database_is_still_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "unrelated.db"
            with sqlite3.connect(path) as conn:
                conn.execute("CREATE TABLE unrelated (value TEXT)")
            with self.assertRaisesRegex(EventStoreError, "non-empty"):
                EventStore(path)
            self.assertEqual(_table_names(path), {"unrelated"})


if __name__ == "__main__":
    unittest.main()
