from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from wechat_local.agent_store import AgentStore
from wechat_local.event_search_projection import (
    EventSearchService,
    ProjectionError,
    ProjectionWorker,
)
from wechat_local.event_store import APPLICATION_ID, EventStore, RawMessage, SourceKey
from wechat_local.normalization_worker import NormalizationWorker
from wechat_local.standard_event_repository import (
    EventRepositoryError,
    ProjectionProfile,
    StandardEventRepository,
)


class ProjectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.event_path = root / "events.db"
        self.agent_path = root / "agent.db"
        self.event = EventStore(self.event_path)
        self.agent = AgentStore(self.agent_path)
        self.source_a = SourceKey(
            "synthetic-account",
            "synthetic-generation",
            "message_0.db",
            "Msg_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        self.source_b = SourceKey(
            "synthetic-account",
            "synthetic-generation",
            "message_0.db",
            "Msg_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        )
        for source in (self.source_a, self.source_b):
            self.event.initialize_cursor(source, 0, "beginning")
        self.event.commit_messages(
            self.source_a,
            [
                RawMessage(1, 101, 1, 1_700_000_000, 41, "alpha needle one", 0),
                RawMessage(2, 102, 1, 1_700_000_100, 41, "beta needle two", 0),
            ],
        )
        self.event.commit_messages(
            self.source_b,
            [
                RawMessage(1, 201, 1, 1_700_000_200, 42, "gamma other", 0),
                RawMessage(
                    2, 202, 777, 1_700_000_300, 42, "delta needle degraded", 0
                ),
            ],
        )
        normalized = NormalizationWorker(
            self.event, "p5-synthetic-normalizer"
        ).run_once()
        self.assertEqual(normalized.claimed, 4)
        rows = self.event._conn.execute(
            "SELECT canonical_payload_json FROM standard_event_materializations "
            "ORDER BY id"
        ).fetchall()
        self.payloads = [json.loads(row[0]) for row in rows]
        self.by_text = {
            payload["content"]["text"]: payload for payload in self.payloads
        }
        self.conversation_a = self.by_text["alpha needle one"]["conversation"]["id"]
        self.conversation_b = self.by_text["gamma other"]["conversation"]["id"]

    def tearDown(self) -> None:
        self.event.close()
        self.agent.close()
        self.temporary.cleanup()

    def _project(self, profile: ProjectionProfile | None = None) -> ProjectionWorker:
        repository = StandardEventRepository(self.event_path)
        self.addCleanup(repository.close)
        worker = ProjectionWorker(repository, self.agent, profile)
        result = worker.run_once()
        self.assertEqual(result.read, 4)
        self.assertEqual(result.projected, 4)
        return worker

    @staticmethod
    def _utc(timestamp: int) -> str:
        return datetime.fromtimestamp(timestamp, timezone.utc).isoformat(
            timespec="microseconds"
        )

    def test_repository_is_read_only_and_projection_is_idempotent(self) -> None:
        source_hash_before = hashlib.sha256(self.event_path.read_bytes()).hexdigest()
        repository = StandardEventRepository(self.event_path)
        statements: list[str] = []
        try:
            with self.assertRaises(sqlite3.OperationalError):
                repository._conn.execute(
                    "UPDATE standard_event_materializations SET quality_status='degraded'"
                )
            repository.set_trace_callback(statements.append)
            worker = ProjectionWorker(repository, self.agent)
            first = worker.run_once(batch_size=10)
            repeated = worker.run_once(batch_size=10)
        finally:
            repository.close()
        source_hash_after = hashlib.sha256(self.event_path.read_bytes()).hexdigest()
        self.assertEqual((first.read, first.projected), (4, 4))
        self.assertEqual((repeated.read, repeated.projected), (0, 0))
        self.assertEqual(source_hash_before, source_hash_after)
        traced = [statement.lstrip().upper() for statement in statements]
        self.assertTrue(any(statement.startswith("SELECT") for statement in traced))
        self.assertFalse(any(statement.startswith("UPDATE") for statement in traced))

    def test_search_filters_stable_pagination_and_cursor_validation(self) -> None:
        self._project()
        service = EventSearchService(self.agent)
        self.assertEqual(len(service.search(q="needle").items), 2)
        self.assertEqual(
            len(
                service.search(
                    q="needle", conversation_id=self.conversation_a
                ).items
            ),
            2,
        )
        time_page = service.search(
            from_time=self._utc(1_700_000_100),
            to_time=self._utc(1_700_000_200),
        )
        self.assertEqual(len(time_page.items), 2)
        degraded = service.search(quality_status="degraded")
        self.assertEqual(len(degraded.items), 1)
        self.assertEqual(degraded.items[0]["conversation_id"], self.conversation_b)

        first = service.search(limit=2)
        self.assertEqual(len(first.items), 2)
        self.assertIsNotNone(first.next_cursor)
        second = service.search(limit=2, cursor=first.next_cursor)
        self.assertEqual(len(second.items), 2)
        self.assertIsNone(second.next_cursor)
        first_ids = {item["event_id"] for item in first.items}
        second_ids = {item["event_id"] for item in second.items}
        self.assertFalse(first_ids & second_ids)
        with self.assertRaisesRegex(ValueError, "cursor"):
            service.search(limit=2, cursor=first.next_cursor[:-1] + "x")
        with self.assertRaisesRegex(ValueError, "query"):
            service.search(q="needle", limit=2, cursor=first.next_cursor)
        alternate_service = EventSearchService(
            self.agent,
            ProjectionProfile(redaction_profile="p5-safe-v1-alternate"),
        )
        with self.assertRaisesRegex(ValueError, "query"):
            alternate_service.search(limit=2, cursor=first.next_cursor)
        for invalid_limit in (0, 101):
            with self.assertRaisesRegex(ValueError, "limit"):
                service.search(limit=invalid_limit)
        special = service.search(q="needle' \" OR * ( )")
        self.assertEqual(special.items, [])

    def test_event_refs_are_bounded_and_exclude_physical_or_provenance_data(self) -> None:
        self._project()
        service = EventSearchService(self.agent)
        item = service.search(q="alpha").items[0]
        self.assertEqual(
            set(item),
            {
                "event_id",
                "subject_id",
                "conversation_id",
                "occurred_at",
                "occurred_at_source",
                "quality_status",
                "citation_text",
                "content_kind",
                "event_type",
            },
        )
        self.assertLessEqual(len(item["citation_text"]), 500)
        serialized = json.dumps(item, ensure_ascii=False)
        for forbidden in (
            "raw_event_uid",
            "raw_revision_fingerprint",
            "message_0.db",
            "Msg_aaaaaaaa",
            "local_id",
            "materialization_id",
        ):
            self.assertNotIn(forbidden, serialized)
        self.assertEqual(service.get_event(item["event_id"]), item)
        self.assertIsNone(service.get_event("evt_missing"))

    def test_conversation_search_and_pagination(self) -> None:
        self._project()
        service = EventSearchService(self.agent)
        first = service.conversations(limit=1)
        second = service.conversations(limit=1, cursor=first.next_cursor)
        self.assertEqual(len(first.items), 1)
        self.assertEqual(len(second.items), 1)
        self.assertNotEqual(
            first.items[0]["conversation_id"], second.items[0]["conversation_id"]
        )
        selected = service.conversations(q=self.conversation_a[-8:])
        self.assertEqual(selected.items[0]["conversation_id"], self.conversation_a)

    def test_invalid_hash_fails_closed_without_checkpoint_advance(self) -> None:
        self.event._conn.execute(
            "UPDATE standard_event_materializations SET payload_sha256=? WHERE id=1",
            ("0" * 64,),
        )
        self.event._conn.commit()
        repository = StandardEventRepository(self.event_path)
        try:
            with self.assertRaisesRegex(ProjectionError, "hash"):
                ProjectionWorker(repository, self.agent).run_once()
        finally:
            repository.close()
        self.assertEqual(
            int(
                self.agent._conn.execute(
                    "SELECT count(*) FROM event_search_projection"
                ).fetchone()[0]
            ),
            0,
        )
        checkpoint = self.agent._conn.execute(
            "SELECT last_materialization_id FROM projection_checkpoints"
        ).fetchone()
        self.assertEqual(int(checkpoint[0]), 0)

    def test_unexpected_payload_shape_fails_closed(self) -> None:
        row = self.event._conn.execute(
            "SELECT canonical_payload_json FROM standard_event_materializations "
            "WHERE id=1"
        ).fetchone()
        payload = json.loads(row[0])
        payload["unexpected"] = "must fail"
        serialized = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        self.event._conn.execute(
            "UPDATE standard_event_materializations "
            "SET canonical_payload_json=?, payload_sha256=? WHERE id=1",
            (serialized, digest),
        )
        self.event._conn.commit()
        repository = StandardEventRepository(self.event_path)
        try:
            with self.assertRaisesRegex(ProjectionError, "shape"):
                ProjectionWorker(repository, self.agent).run_once()
        finally:
            repository.close()
        self.assertEqual(
            int(
                self.agent._conn.execute(
                    "SELECT count(*) FROM event_search_projection"
                ).fetchone()[0]
            ),
            0,
        )

    def test_malformed_json_fails_closed(self) -> None:
        malformed = "{not-json"
        digest = hashlib.sha256(malformed.encode("utf-8")).hexdigest()
        self.event._conn.execute(
            "UPDATE standard_event_materializations "
            "SET canonical_payload_json=?, payload_sha256=? WHERE id=1",
            (malformed, digest),
        )
        self.event._conn.commit()
        repository = StandardEventRepository(self.event_path)
        try:
            with self.assertRaisesRegex(ProjectionError, "Malformed"):
                ProjectionWorker(repository, self.agent).run_once()
        finally:
            repository.close()
        self.assertEqual(
            int(
                self.agent._conn.execute(
                    "SELECT count(*) FROM event_search_projection"
                ).fetchone()[0]
            ),
            0,
        )

    def test_event_and_subject_identity_mismatch_each_fail_closed(self) -> None:
        row = self.event._conn.execute(
            "SELECT event_id, subject_id FROM standard_event_materializations WHERE id=1"
        ).fetchone()
        repository = StandardEventRepository(self.event_path)
        try:
            self.event._conn.execute(
                "UPDATE standard_event_materializations SET event_id=? WHERE id=1",
                ("evt_wrong_identity",),
            )
            self.event._conn.commit()
            with self.assertRaisesRegex(ProjectionError, "identity"):
                ProjectionWorker(repository, self.agent).run_once()
            self.event._conn.execute(
                "UPDATE standard_event_materializations "
                "SET event_id=?, subject_id=? WHERE id=1",
                (row["event_id"], "msg_wrong_identity"),
            )
            self.event._conn.commit()
            with self.assertRaisesRegex(ProjectionError, "identity"):
                ProjectionWorker(repository, self.agent).run_once()
        finally:
            repository.close()
        self.assertEqual(
            int(
                self.agent._conn.execute(
                    "SELECT count(*) FROM event_search_projection"
                ).fetchone()[0]
            ),
            0,
        )

    def test_oversized_source_text_is_rejected_before_projection(self) -> None:
        row = self.event._conn.execute(
            "SELECT canonical_payload_json FROM standard_event_materializations "
            "WHERE id=1"
        ).fetchone()
        payload = json.loads(row[0])
        payload["content"]["text"] = "x" * 100_001
        serialized = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        self.event._conn.execute(
            "UPDATE standard_event_materializations "
            "SET canonical_payload_json=?, payload_sha256=? WHERE id=1",
            (serialized, digest),
        )
        self.event._conn.commit()
        repository = StandardEventRepository(self.event_path)
        try:
            with self.assertRaisesRegex(ProjectionError, "source bound"):
                ProjectionWorker(repository, self.agent).run_once()
        finally:
            repository.close()

    def test_invalid_source_timestamp_uses_observed_fallback(self) -> None:
        self.event.commit_messages(
            self.source_a,
            [RawMessage(3, 103, 1, 0, 41, "fallback timestamp", 0)],
        )
        result = NormalizationWorker(
            self.event, "fallback-synthetic-normalizer"
        ).run_once()
        self.assertEqual(result.normalized, 1)
        repository = StandardEventRepository(self.event_path)
        try:
            projected = ProjectionWorker(repository, self.agent).run_once()
        finally:
            repository.close()
        self.assertEqual(projected.projected, 5)
        item = EventSearchService(self.agent).search(q="fallback").items[0]
        self.assertEqual(item["occurred_at_source"], "observed_fallback")

    def test_degraded_unknown_event_is_visible_without_invented_body(self) -> None:
        self._project()
        service = EventSearchService(self.agent)
        degraded = service.search(quality_status="degraded").items
        self.assertEqual(len(degraded), 1)
        self.assertEqual(degraded[0]["citation_text"], "unknown")
        self.assertEqual(service.search(q="delta").items, [])

    def test_incremental_restart_replay_and_fts_integrity(self) -> None:
        worker = self._project()
        self.event.commit_messages(
            self.source_a,
            [RawMessage(3, 103, 1, 1_700_000_400, 41, "epsilon increment", 0)],
        )
        NormalizationWorker(self.event, "incremental-normalizer").run_once()
        added = worker.run_once()
        self.assertEqual((added.read, added.projected), (1, 1))
        self.assertEqual(worker.run_once().projected, 0)
        orphan_count = int(
            self.agent._conn.execute(
                """
                SELECT count(*) FROM event_search_fts AS f
                LEFT JOIN event_search_projection AS p ON p.rowid=f.rowid
                WHERE p.rowid IS NULL
                """
            ).fetchone()[0]
        )
        self.assertEqual(orphan_count, 0)
        self.assertEqual(
            int(self.agent._conn.execute("SELECT count(*) FROM event_search_fts").fetchone()[0]),
            5,
        )
        self.agent.close()
        self.agent = AgentStore(self.agent_path)
        restarted_repository = StandardEventRepository(self.event_path)
        try:
            replayed = ProjectionWorker(restarted_repository, self.agent).run_once()
        finally:
            restarted_repository.close()
        self.assertEqual((replayed.read, replayed.projected), (0, 0))

    def test_interrupted_rebuild_rolls_back_projection_fts_and_checkpoint(self) -> None:
        worker = self._project()
        before = (
            int(
                self.agent._conn.execute(
                    "SELECT count(*) FROM event_search_projection"
                ).fetchone()[0]
            ),
            int(
                self.agent._conn.execute(
                    "SELECT count(*) FROM event_search_fts"
                ).fetchone()[0]
            ),
            int(
                self.agent._conn.execute(
                    "SELECT last_materialization_id FROM projection_checkpoints"
                ).fetchone()[0]
            ),
        )
        self.agent._conn.execute(
            """
            CREATE TRIGGER synthetic_rebuild_failure
            BEFORE DELETE ON event_search_projection
            BEGIN SELECT RAISE(ABORT, 'synthetic rebuild failure'); END
            """
        )
        self.agent._conn.commit()
        with self.assertRaises(sqlite3.IntegrityError):
            worker.rebuild()
        after = (
            int(
                self.agent._conn.execute(
                    "SELECT count(*) FROM event_search_projection"
                ).fetchone()[0]
            ),
            int(
                self.agent._conn.execute(
                    "SELECT count(*) FROM event_search_fts"
                ).fetchone()[0]
            ),
            int(
                self.agent._conn.execute(
                    "SELECT last_materialization_id FROM projection_checkpoints"
                ).fetchone()[0]
            ),
        )
        self.assertEqual(after, before)

    def test_projection_transaction_rolls_back_rows_fts_and_checkpoint(self) -> None:
        self.agent._conn.execute(
            """
            CREATE TRIGGER synthetic_projection_failure
            BEFORE INSERT ON event_search_projection
            BEGIN SELECT RAISE(ABORT, 'synthetic projection failure'); END
            """
        )
        self.agent._conn.commit()
        repository = StandardEventRepository(self.event_path)
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                ProjectionWorker(repository, self.agent).run_once()
        finally:
            repository.close()
        projection_count = int(
            self.agent._conn.execute(
                "SELECT count(*) FROM event_search_projection"
            ).fetchone()[0]
        )
        fts_count = int(
            self.agent._conn.execute("SELECT count(*) FROM event_search_fts").fetchone()[0]
        )
        checkpoint = int(
            self.agent._conn.execute(
                "SELECT last_materialization_id FROM projection_checkpoints"
            ).fetchone()[0]
        )
        self.assertEqual((projection_count, fts_count, checkpoint), (0, 0, 0))

    def test_rebuild_is_profile_scoped_and_recoverable(self) -> None:
        default_worker = self._project()
        alternate = ProjectionProfile(redaction_profile="p5-safe-v1-alternate")
        alternate_repository = StandardEventRepository(self.event_path)
        self.addCleanup(alternate_repository.close)
        alternate_worker = ProjectionWorker(
            alternate_repository, self.agent, alternate
        )
        self.assertEqual(alternate_worker.run_once().projected, 4)
        self.assertEqual(
            int(
                self.agent._conn.execute(
                    "SELECT count(*) FROM event_search_projection"
                ).fetchone()[0]
            ),
            8,
        )
        default_worker.rebuild()
        counts = {
            row["profile_id"]: int(row["count"])
            for row in self.agent._conn.execute(
                "SELECT profile_id, count(*) AS count "
                "FROM event_search_projection GROUP BY profile_id"
            )
        }
        self.assertNotIn(ProjectionProfile().profile_id, counts)
        self.assertEqual(counts[alternate.profile_id], 4)
        self.assertEqual(default_worker.run_once().projected, 4)


class RepositoryBoundaryTests(unittest.TestCase):
    def test_rejects_wrong_event_store_identity_and_schema(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "wrong.db"
            connection = sqlite3.connect(path)
            connection.execute("CREATE TABLE standard_event_materializations(id INTEGER)")
            connection.execute("PRAGMA application_id = 123")
            connection.execute("PRAGMA user_version = 6")
            connection.commit()
            connection.close()
            with self.assertRaisesRegex(EventRepositoryError, "application id"):
                StandardEventRepository(path)
            future = Path(directory) / "future.db"
            connection = sqlite3.connect(future)
            connection.execute("CREATE TABLE standard_event_materializations(id INTEGER)")
            connection.execute(f"PRAGMA application_id = {APPLICATION_ID}")
            connection.execute("PRAGMA user_version = 999")
            connection.commit()
            connection.close()
            with self.assertRaisesRegex(EventRepositoryError, "schema"):
                StandardEventRepository(future)

    def test_batches_are_bounded_ordered_and_profile_filtered(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            event = EventStore(Path(directory) / "events.db")
            source = SourceKey("account", "generation", "message.db", "Msg_synthetic")
            event.initialize_cursor(source, 0, "beginning")
            event.commit_messages(
                source,
                [
                    RawMessage(1, 1, 1, 1_700_000_000, 1, "one", 0),
                    RawMessage(2, 2, 1, 1_700_000_001, 1, "two", 0),
                    RawMessage(3, 3, 1, 1_700_000_002, 1, "three", 0),
                ],
            )
            NormalizationWorker(event, "repository-normalizer").run_once()
            event.close()
            repository = StandardEventRepository(Path(directory) / "events.db")
            try:
                self.assertEqual(
                    int(repository._conn.execute("PRAGMA query_only").fetchone()[0]), 1
                )
                rows = repository.batch_after(ProjectionProfile(), 0, 2)
                self.assertEqual([int(row["id"]) for row in rows], [1, 2])
                self.assertEqual(
                    repository.batch_after(
                        ProjectionProfile(normalizer_version="999"), 0, 10
                    ),
                    [],
                )
            finally:
                repository.close()


if __name__ == "__main__":
    unittest.main()
