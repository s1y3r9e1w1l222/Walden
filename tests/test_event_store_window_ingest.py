from __future__ import annotations

from datetime import datetime, timezone
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from wechat_local.event_store import (
    EventStore,
    EventStoreError,
    RawMessage,
    SourceKey,
    WindowIngestRun,
)


class EventStoreWindowIngestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.store_path = Path(self.temporary.name) / "private" / "events.db"
        self.store = EventStore(self.store_path)
        self.source = SourceKey(
            "synthetic-account",
            "synthetic-generation",
            "message_0.db",
            "Msg_00000000000000000000000000000000",
        )
        self.store.record_catalog_refresh(
            self.source.account_id,
            self.source.source_generation,
            [(self.source.shard_name, self.source.table_name)],
        )
        self.message = RawMessage(7, 70, 1, 1_700_000_001, 1, "secret body", 0)
        self.run = WindowIngestRun(
            requested_start=datetime(2023, 11, 14, 0, 0, tzinfo=timezone.utc),
            requested_end=datetime(2023, 11, 15, 0, 0, tzinfo=timezone.utc),
            source_coverage={
                "source_min_time": 1_699_999_000,
                "source_max_time": 1_700_001_000,
                "examined_count": 1,
            },
        )

    def tearDown(self) -> None:
        self.store.close()
        self.temporary.cleanup()

    def test_window_ingest_is_idempotent_and_never_creates_or_advances_a_cursor(self) -> None:
        result = self.store.commit_window_messages(self.source, [self.message], self.run)
        repeated = self.store.commit_window_messages(self.source, [self.message], self.run)

        self.assertEqual((result.inserted_events, repeated.inserted_events), (1, 0))
        self.assertEqual((result.inserted_revisions, repeated.inserted_revisions), (1, 0))
        self.assertIsNone(self.store.get_cursor(self.source))
        self.assertEqual(
            self.store.source_catalog_entry(self.source)["lifecycle"],
            "pending_initialization",
        )
        self.assertNotEqual(result.run_id, repeated.run_id)
        self.assertEqual(self.store.count_events(), 1)

    def test_window_ingest_preserves_an_existing_normal_cursor_and_catalog_lifecycle(self) -> None:
        self.store.initialize_catalog_source(self.source, "beginning", observed_max_local_id=3)
        before_catalog = tuple(self.store.source_catalog_entry(self.source))

        self.store.commit_window_messages(self.source, [self.message], self.run)

        self.assertEqual(self.store.get_cursor(self.source), 0)
        self.assertEqual(tuple(self.store.source_catalog_entry(self.source)), before_catalog)

    def test_window_ingest_refuses_uncatalogued_and_replacement_suspected_sources(self) -> None:
        uncatalogued = SourceKey(
            "synthetic-account", "synthetic-generation", "message_1.db", "Msg_other"
        )
        with self.assertRaisesRegex(EventStoreError, "catalogued"):
            self.store.commit_window_messages(uncatalogued, [self.message], self.run)
        self.assertEqual(self.store.count_events(), 0)

        self.store._conn.execute(
            "UPDATE source_catalog SET lifecycle='replacement_suspected', "
            "suspicion_reason='test' WHERE account_id=? AND source_generation=? "
            "AND shard_name=? AND table_name=?",
            self.store._source_values(self.source),
        )
        self.store._conn.commit()
        with self.assertRaisesRegex(EventStoreError, "replacement-suspected"):
            self.store.commit_window_messages(self.source, [self.message], self.run)
        self.assertEqual(self.store.count_events(), 0)

    def test_window_ingest_metadata_is_bounded_and_redacted(self) -> None:
        invalid_coverage = (
            {"source_min_time": 0, "source_max_time": 1, "examined_count": 1, "content": "secret"},
            {"source_min_time": 0, "source_max_time": 1, "examined_count": 1, "source_key": "secret"},
            {"source_min_time": 0, "source_max_time": 1, "examined_count": "one"},
        )
        for coverage in invalid_coverage:
            with self.subTest(coverage=coverage):
                with self.assertRaises((TypeError, ValueError)):
                    WindowIngestRun(self.run.requested_start, self.run.requested_end, coverage)

        result = self.store.commit_window_messages(self.source, [self.message], self.run)
        metadata = self.store.window_ingest_run(result.run_id)
        self.assertIsNotNone(metadata)
        self.assertEqual(
            set(metadata.keys()),
            {
                "run_id", "requested_start", "requested_end", "source_min_time",
                "source_max_time", "examined_count", "received_messages",
                "inserted_events", "inserted_revisions", "parse_errors", "completed_at",
            },
        )
        self.assertNotIn("secret body", repr(dict(metadata)))
        self.assertNotIn(self.source.account_id, repr(dict(metadata)))
        self.assertNotIn("raw_content", metadata.keys())

    def test_window_run_coverage_is_a_frozen_copy_of_controlled_facts(self) -> None:
        coverage = {
            "source_min_time": 1,
            "source_max_time": 2,
            "examined_count": 3,
        }
        run = WindowIngestRun(self.run.requested_start, self.run.requested_end, coverage)
        coverage["examined_count"] = 99

        self.assertEqual(run.source_coverage["examined_count"], 3)
        with self.assertRaises(TypeError):
            run.source_coverage["examined_count"] = 4

    def test_window_ingest_rolls_back_events_and_run_facts_together(self) -> None:
        with patch.object(
            self.store, "_persist_messages", side_effect=sqlite3.DatabaseError("boom")
        ):
            with self.assertRaisesRegex(sqlite3.DatabaseError, "boom"):
                self.store.commit_window_messages(self.source, [self.message], self.run)

        self.assertEqual(self.store.count_events(), 0)
        self.assertEqual(
            self.store._conn.execute("SELECT count(*) FROM window_ingest_runs").fetchone()[0],
            0,
        )
        self.assertEqual(
            self.store._conn.execute("SELECT count(*) FROM window_ingest_outcomes").fetchone()[0],
            0,
        )

    def test_window_ingested_events_normalize_through_the_standard_outbox(self) -> None:
        self.store.commit_window_messages(self.source, [self.message], self.run)

        row = self.store._conn.execute(
            "SELECT revision_id, task_kind, status FROM processing_outbox"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual((row["task_kind"], row["status"]), ("process_event", "pending"))


if __name__ == "__main__":
    unittest.main()
