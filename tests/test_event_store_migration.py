from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from wechat_local.event_store import (
    APPLICATION_ID,
    CORE_SCHEMA,
    EventStore,
    SCHEMA_VERSION,
)


class EventStoreMigrationTests(unittest.TestCase):
    def test_v1_to_v2_is_additive_and_preserves_phase1_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "private" / "events.db"
            path.parent.mkdir()
            with sqlite3.connect(path) as conn:
                conn.executescript(CORE_SCHEMA)
                conn.execute(f"PRAGMA application_id = {APPLICATION_ID}")
                conn.execute("PRAGMA user_version = 1")
                conn.execute(
                    """
                    INSERT INTO cursors VALUES (
                        'account', 'generation', 'message_0.db', 'Msg_hash',
                        7, 'beginning', 'init', 'update'
                    )
                    """
                )
                event_id = conn.execute(
                    """
                    INSERT INTO events (
                        event_uid, account_id, source_generation, shard_name,
                        table_name, local_id, first_seen_at
                    ) VALUES ('stable-uid', 'account', 'generation',
                              'message_0.db', 'Msg_hash', 7, 'seen')
                    """
                ).lastrowid
                revision_id = conn.execute(
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
                conn.execute(
                    "UPDATE events SET current_revision_id=? WHERE id=?",
                    (revision_id, event_id),
                )
                conn.execute(
                    """
                    INSERT INTO processing_outbox (
                        revision_id, task_kind, status, attempt_count,
                        available_at, created_at, updated_at
                    ) VALUES (?, 'process_event', 'pending', 0, 'a', 'c', 'u')
                    """,
                    (revision_id,),
                )

            with EventStore(path) as store:
                self.assertEqual(store.count_events(), 1)
                self.assertEqual(store.count_revisions(), 1)
                self.assertEqual(store.count_pending_tasks(), 1)
                self.assertEqual(store.integrity_check(), "ok")

            with sqlite3.connect(path) as conn:
                self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], SCHEMA_VERSION)
                event = conn.execute(
                    "SELECT id, event_uid, current_revision_id FROM events"
                ).fetchone()
                revision = conn.execute(
                    "SELECT id, content_fingerprint FROM event_revisions"
                ).fetchone()
                cursor = conn.execute(
                    "SELECT last_local_id FROM cursors"
                ).fetchone()[0]
                tables = {
                    row[0]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
            self.assertEqual(event, (event_id, "stable-uid", revision_id))
            self.assertEqual(revision, (revision_id, "stable-fingerprint"))
            self.assertEqual(cursor, 7)
            self.assertIn("session_observations", tables)
            self.assertIn("session_scan_queue", tables)


if __name__ == "__main__":
    unittest.main()
