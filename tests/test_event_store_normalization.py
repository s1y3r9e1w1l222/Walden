from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from wechat_local.event_store import (
    EventStore,
    EventStoreError,
    MaterializationConflict,
    RawMessage,
    SourceKey,
    StandardEventMaterializationInput,
)


CONSUMER = "standard-event-normalizer"
CONTRACT = "wechat-local.standard-event/1"
NORMALIZER = "wechat-macos-v4"
LEASE_EXPIRY = "2099-01-01T00:00:00.000000+00:00"
READY_NOW = "2000-01-01T00:00:00.000000+00:00"


class EventStoreNormalizationPersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.store_path = Path(self.temporary.name) / "private" / "events.db"
        self.store = EventStore(self.store_path)
        self.source = SourceKey(
            account_id="synthetic-account",
            source_generation="synthetic-generation",
            shard_name="message_0.db",
            table_name="Msg_00000000000000000000000000000000",
        )
        self.store.initialize_cursor(self.source, 0, "beginning")
        self.store.commit_messages(
            self.source,
            [
                RawMessage(1, 10, 1, 100, 1, "synthetic-one", 0),
                RawMessage(2, 20, 1, 200, 1, "synthetic-two", 0),
            ],
        )
        rows = self.store._conn.execute(
            """
            SELECT outbox.id AS outbox_id, outbox.revision_id
            FROM processing_outbox AS outbox ORDER BY outbox.id
            """
        ).fetchall()
        self.outbox_revision_pairs = [
            (int(row["outbox_id"]), int(row["revision_id"])) for row in rows
        ]

    def tearDown(self) -> None:
        self.store.close()
        self.temporary.cleanup()

    def _consumer_state(
        self,
        outbox_id: int | None = None,
        *,
        consumer_name: str = CONSUMER,
        normalizer_version: str = "1",
    ):
        selected_outbox = outbox_id or self.outbox_revision_pairs[0][0]
        return self.store.ensure_outbox_consumer_state(
            selected_outbox,
            consumer_name,
            CONTRACT,
            normalizer_version,
        )

    def _materialization(
        self,
        revision_id: int | None = None,
        *,
        normalizer_version: str = "1",
        payload_marker: str = "first",
        event_id: str = "evt_synthetic",
        subject_id: str = "msg_synthetic",
    ) -> StandardEventMaterializationInput:
        selected_revision = revision_id or self.outbox_revision_pairs[0][1]
        payload = json.dumps(
            {
                "id": event_id,
                "marker": payload_marker,
                "subject_id": subject_id,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return StandardEventMaterializationInput(
            source_revision_id=selected_revision,
            event_id=event_id,
            subject_id=subject_id,
            contract_version=CONTRACT,
            normalizer_name=NORMALIZER,
            normalizer_version=normalizer_version,
            canonical_payload_json=payload,
            payload_sha256=hashlib.sha256(payload.encode("utf-8")).hexdigest(),
            quality_status="normalized",
        )

    def _outbox_row(self, outbox_id: int) -> tuple[object, ...]:
        row = self.store._conn.execute(
            "SELECT * FROM processing_outbox WHERE id=?", (outbox_id,)
        ).fetchone()
        return tuple(row)

    def test_consumer_key_is_idempotent_without_mutating_global_outbox(self) -> None:
        outbox_id = self.outbox_revision_pairs[0][0]
        before = self._outbox_row(outbox_id)
        first = self._consumer_state(outbox_id)
        second = self._consumer_state(outbox_id)

        self.assertEqual(first.id, second.id)
        self.assertEqual(first.status, "pending")
        self.assertEqual(first.attempt_count, 0)
        self.assertEqual(self._outbox_row(outbox_id), before)
        self.assertEqual(
            int(
                self.store._conn.execute(
                    "SELECT count(*) FROM outbox_consumer_state"
                ).fetchone()[0]
            ),
            1,
        )

    def test_consumer_profile_refactor_preserves_i1_input_validation(self) -> None:
        outbox_id = self.outbox_revision_pairs[0][0]
        invalid_profiles = (
            ("", CONTRACT, "1", "consumer_name"),
            (" standard-event-normalizer", CONTRACT, "1", "consumer_name"),
            (CONSUMER, "", "1", "contract_version"),
            (CONSUMER, CONTRACT, "1\n", "normalizer_version"),
            ("x" * 101, CONTRACT, "1", "consumer_name"),
        )
        for api in (
            self.store.ensure_outbox_consumer_state,
            self.store.get_outbox_consumer_state,
        ):
            for consumer_name, contract, version, field_name in invalid_profiles:
                with self.subTest(api=api.__name__, field_name=field_name):
                    with self.assertRaisesRegex((TypeError, ValueError), field_name):
                        api(outbox_id, consumer_name, contract, version)
            with self.subTest(api=api.__name__, field_name="outbox_id"):
                with self.assertRaisesRegex(ValueError, "outbox_id must be positive"):
                    api(0, CONSUMER, CONTRACT, "1")

    def test_bootstrap_is_idempotent_and_ignores_global_outbox_lifecycle(self) -> None:
        first_outbox = self.outbox_revision_pairs[0][0]
        other = self._consumer_state(
            first_outbox, consumer_name="synthetic-indexer"
        )
        self.store._conn.execute(
            "UPDATE processing_outbox SET status='complete' WHERE id=?",
            (first_outbox,),
        )
        self.store._conn.execute(
            "UPDATE processing_outbox SET status='failed' WHERE id=?",
            (self.outbox_revision_pairs[1][0],),
        )
        self.store._conn.commit()
        outbox_before = [
            tuple(row)
            for row in self.store._conn.execute(
                "SELECT * FROM processing_outbox ORDER BY id"
            )
        ]

        inserted = self.store.bootstrap_outbox_consumer_states(
            CONSUMER, CONTRACT, "1"
        )
        repeated = self.store.bootstrap_outbox_consumer_states(
            CONSUMER, CONTRACT, "1"
        )
        upgraded = self.store.bootstrap_outbox_consumer_states(
            CONSUMER, CONTRACT, "2"
        )

        self.assertEqual(inserted, 2)
        self.assertEqual(repeated, 0)
        self.assertEqual(upgraded, 2)
        self.assertEqual(
            self.store.get_outbox_consumer_state(
                first_outbox, "synthetic-indexer", CONTRACT, "1"
            ),
            other,
        )
        self.assertEqual(
            [
                tuple(row)
                for row in self.store._conn.execute(
                    "SELECT * FROM processing_outbox ORDER BY id"
                )
            ],
            outbox_before,
        )

    def test_claim_is_oldest_ready_and_excludes_unexpired_processing(self) -> None:
        outbox_before = [
            tuple(row)
            for row in self.store._conn.execute(
                "SELECT * FROM processing_outbox ORDER BY id"
            )
        ]
        self.store.bootstrap_outbox_consumer_states(CONSUMER, CONTRACT, "1")

        first = self.store.claim_next_outbox_consumer_state(
            CONSUMER, CONTRACT, "1", "worker-one", LEASE_EXPIRY
        )
        second = self.store.claim_next_outbox_consumer_state(
            CONSUMER, CONTRACT, "1", "worker-two", LEASE_EXPIRY
        )
        none_ready = self.store.claim_next_outbox_consumer_state(
            CONSUMER, CONTRACT, "1", "worker-three", LEASE_EXPIRY
        )

        self.assertEqual(first.outbox_id, self.outbox_revision_pairs[0][0])
        self.assertEqual(second.outbox_id, self.outbox_revision_pairs[1][0])
        self.assertEqual(first.attempt_count, 1)
        self.assertEqual(second.attempt_count, 1)
        self.assertIsNone(none_ready)
        self.assertEqual(
            [
                tuple(row)
                for row in self.store._conn.execute(
                    "SELECT * FROM processing_outbox ORDER BY id"
                )
            ],
            outbox_before,
        )

    def test_claim_honors_available_at_and_excludes_failed_and_complete(self) -> None:
        first_outbox, first_revision = self.outbox_revision_pairs[0]
        second_outbox, second_revision = self.outbox_revision_pairs[1]
        delayed = self.store.ensure_outbox_consumer_state(
            first_outbox,
            CONSUMER,
            CONTRACT,
            "1",
            available_at=LEASE_EXPIRY,
        )
        self.store.bootstrap_outbox_consumer_states(CONSUMER, CONTRACT, "1")
        ready = self.store.claim_next_outbox_consumer_state(
            CONSUMER, CONTRACT, "1", "ready-worker", LEASE_EXPIRY
        )
        self.assertEqual(ready.outbox_id, second_outbox)
        self.store.complete_outbox_consumer_state_with_materialization(
            ready.id,
            "ready-worker",
            self._materialization(second_revision, event_id="evt_second"),
        )

        self.store._conn.execute(
            "UPDATE outbox_consumer_state SET available_at=? WHERE id=?",
            (READY_NOW, delayed.id),
        )
        self.store._conn.commit()
        claimed_delayed = self.store.claim_next_outbox_consumer_state(
            CONSUMER, CONTRACT, "1", "failed-worker", LEASE_EXPIRY
        )
        self.assertEqual(claimed_delayed.outbox_id, first_outbox)
        self.store.fail_outbox_consumer_state(
            claimed_delayed.id, "failed-worker", "SyntheticInvariant"
        )

        self.assertIsNone(
            self.store.claim_next_outbox_consumer_state(
                CONSUMER, CONTRACT, "1", "none-worker", LEASE_EXPIRY
            )
        )
        self.assertIsNone(
            self.store.get_standard_event_materialization(
                first_revision, CONTRACT, NORMALIZER, "1"
            )
        )

    def test_expired_processing_is_recovered_and_attempted_again(self) -> None:
        outbox_id = self.outbox_revision_pairs[0][0]
        outbox_before = self._outbox_row(outbox_id)
        state = self._consumer_state(outbox_id)
        first_attempt = self.store.begin_outbox_consumer_attempt(
            state.id, "expired-worker", READY_NOW
        )

        recovered = self.store.claim_next_outbox_consumer_state(
            CONSUMER, CONTRACT, "1", "recovery-worker", LEASE_EXPIRY
        )

        self.assertEqual(recovered.id, first_attempt.id)
        self.assertEqual(recovered.outbox_id, outbox_id)
        self.assertEqual(recovered.status, "processing")
        self.assertEqual(recovered.attempt_count, 2)
        self.assertEqual(recovered.lease_owner, "recovery-worker")
        self.assertEqual(recovered.last_error_type, "LeaseExpired")
        self.assertEqual(self._outbox_row(outbox_id), outbox_before)

    def test_lease_renewal_preserves_attempt_and_requires_exact_owner(self) -> None:
        outbox_id = self.outbox_revision_pairs[0][0]
        outbox_before = self._outbox_row(outbox_id)
        state = self._consumer_state()
        processing = self.store.begin_outbox_consumer_attempt(
            state.id, "lease-worker", LEASE_EXPIRY
        )
        renewed_expiry = "2099-01-02T00:00:00.000000+00:00"

        renewed = self.store.renew_outbox_consumer_lease(
            processing.id, "lease-worker", renewed_expiry
        )

        self.assertEqual(renewed.attempt_count, processing.attempt_count)
        self.assertEqual(renewed.lease_expires_at, renewed_expiry)
        with self.assertRaisesRegex(EventStoreError, "unowned"):
            self.store.renew_outbox_consumer_lease(
                processing.id, "other-worker", LEASE_EXPIRY
            )
        self.assertEqual(self._outbox_row(outbox_id), outbox_before)

    def test_reclaimed_state_rejects_every_stale_owner_transition(self) -> None:
        outbox_id, revision_id = self.outbox_revision_pairs[0]
        outbox_before = self._outbox_row(outbox_id)
        state = self._consumer_state(outbox_id)
        self.store.begin_outbox_consumer_attempt(
            state.id, "stale-worker", READY_NOW
        )
        reclaimed = self.store.claim_next_outbox_consumer_state(
            CONSUMER, CONTRACT, "1", "current-worker", LEASE_EXPIRY
        )
        candidate = self._materialization(revision_id)

        with self.assertRaisesRegex(EventStoreError, "unowned"):
            self.store.renew_outbox_consumer_lease(
                reclaimed.id, "stale-worker", LEASE_EXPIRY
            )
        with self.assertRaisesRegex(EventStoreError, "unowned"):
            self.store.retry_outbox_consumer_state(
                reclaimed.id,
                "stale-worker",
                "StorageContention",
                available_at=READY_NOW,
            )
        with self.assertRaisesRegex(EventStoreError, "unowned"):
            self.store.fail_outbox_consumer_state(
                reclaimed.id, "stale-worker", "WorkerInvariant"
            )
        with self.assertRaisesRegex(EventStoreError, "unowned"):
            self.store.complete_outbox_consumer_state_with_materialization(
                reclaimed.id, "stale-worker", candidate
            )

        current = self.store.get_outbox_consumer_state(
            outbox_id, CONSUMER, CONTRACT, "1"
        )
        self.assertEqual(current.status, "processing")
        self.assertEqual(current.lease_owner, "current-worker")
        self.assertEqual(current.attempt_count, 2)
        self.assertIsNone(
            self.store.get_standard_event_materialization(
                revision_id, CONTRACT, NORMALIZER, "1"
            )
        )
        self.assertEqual(self._outbox_row(outbox_id), outbox_before)

    def test_claimed_load_uses_outbox_revision_not_current_revision(self) -> None:
        first_outbox, first_revision = self.outbox_revision_pairs[0]
        self.store.commit_messages(
            self.source,
            [RawMessage(1, 11, 1, 101, 1, "synthetic-one-revised", 0)],
        )
        current_revision = int(
            self.store._conn.execute(
                "SELECT current_revision_id FROM events WHERE local_id=1"
            ).fetchone()[0]
        )
        self.assertNotEqual(current_revision, first_revision)
        self.store.bootstrap_outbox_consumer_states(CONSUMER, CONTRACT, "1")
        claimed = self.store.claim_next_outbox_consumer_state(
            CONSUMER, CONTRACT, "1", "exact-worker", LEASE_EXPIRY
        )

        loaded = self.store.load_claimed_process_event(
            claimed.id, "exact-worker"
        )

        self.assertEqual(claimed.outbox_id, first_outbox)
        self.assertEqual(loaded.source_revision_id, first_revision)
        self.assertEqual(loaded.revision_number, 1)
        self.assertEqual(loaded.raw_content, "synthetic-one")
        self.assertNotIn("synthetic-one", repr(loaded))
        with self.assertRaisesRegex(EventStoreError, "unowned"):
            self.store.load_claimed_process_event(claimed.id, "other-worker")

    def test_consumer_state_counts_are_profile_scoped_and_aggregate_only(self) -> None:
        first_outbox, _ = self.outbox_revision_pairs[0]
        self.store.bootstrap_outbox_consumer_states(CONSUMER, CONTRACT, "1")
        self._consumer_state(first_outbox, consumer_name="synthetic-indexer")
        claimed = self.store.claim_next_outbox_consumer_state(
            CONSUMER, CONTRACT, "1", "count-worker", LEASE_EXPIRY
        )
        self.store.fail_outbox_consumer_state(
            claimed.id, "count-worker", "SyntheticInvariant"
        )

        counts = self.store.outbox_consumer_state_counts(
            CONSUMER, CONTRACT, "1"
        )

        self.assertEqual(counts.pending, 1)
        self.assertEqual(counts.processing, 0)
        self.assertEqual(counts.complete, 0)
        self.assertEqual(counts.failed, 1)
        self.assertFalse(hasattr(counts, "raw_content"))

    def test_consumers_and_normalizer_versions_have_independent_state(self) -> None:
        outbox_id = self.outbox_revision_pairs[0][0]
        standard_v1 = self._consumer_state(outbox_id)
        other_consumer = self._consumer_state(
            outbox_id, consumer_name="synthetic-indexer"
        )
        standard_v2 = self._consumer_state(outbox_id, normalizer_version="2")

        self.assertEqual(len({standard_v1.id, other_consumer.id, standard_v2.id}), 3)
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.ensure_outbox_consumer_state(
                999_999, CONSUMER, CONTRACT, "1"
            )

        with self.assertRaises(sqlite3.IntegrityError):
            self.store._conn.execute(
                "DELETE FROM processing_outbox WHERE id=?", (outbox_id,)
            )
        self.store._conn.rollback()
        self.assertIsNotNone(
            self.store.get_outbox_consumer_state(
                outbox_id, CONSUMER, CONTRACT, "1"
            )
        )

    def test_same_outbox_consumers_have_independent_lifecycles(self) -> None:
        outbox_id, revision_id = self.outbox_revision_pairs[0]
        outbox_before = self._outbox_row(outbox_id)
        standard = self._consumer_state(outbox_id)
        indexer = self._consumer_state(
            outbox_id, consumer_name="synthetic-indexer"
        )
        agent = self._consumer_state(
            outbox_id, consumer_name="synthetic-agent"
        )

        self.store.begin_outbox_consumer_attempt(
            indexer.id, "indexer-worker", LEASE_EXPIRY
        )
        retried_indexer = self.store.retry_outbox_consumer_state(
            indexer.id,
            "indexer-worker",
            "SyntheticRetry",
            available_at=READY_NOW,
        )
        self.assertEqual(retried_indexer.status, "pending")
        self.assertEqual(
            self.store.get_outbox_consumer_state(
                outbox_id, "synthetic-agent", CONTRACT, "1"
            ),
            agent,
        )

        self.store.begin_outbox_consumer_attempt(
            agent.id, "agent-worker", LEASE_EXPIRY
        )
        failed_agent = self.store.fail_outbox_consumer_state(
            agent.id, "agent-worker", "SyntheticInvariant"
        )
        self.assertEqual(failed_agent.status, "failed")
        self.assertEqual(
            self.store.get_outbox_consumer_state(
                outbox_id, "synthetic-indexer", CONTRACT, "1"
            ),
            retried_indexer,
        )

        self.store.begin_outbox_consumer_attempt(
            standard.id, "normalizer-worker", LEASE_EXPIRY
        )
        self.store.complete_outbox_consumer_state_with_materialization(
            standard.id,
            "normalizer-worker",
            self._materialization(revision_id),
        )
        completed_standard = self.store.get_outbox_consumer_state(
            outbox_id, CONSUMER, CONTRACT, "1"
        )

        self.assertEqual(completed_standard.status, "complete")
        self.assertEqual(
            self.store.get_outbox_consumer_state(
                outbox_id, "synthetic-indexer", CONTRACT, "1"
            ),
            retried_indexer,
        )
        self.assertEqual(
            self.store.get_outbox_consumer_state(
                outbox_id, "synthetic-agent", CONTRACT, "1"
            ),
            failed_agent,
        )
        self.assertEqual(self._outbox_row(outbox_id), outbox_before)

    def test_exact_row_attempt_and_retry_persist_lease_fields(self) -> None:
        outbox_id = self.outbox_revision_pairs[0][0]
        outbox_before = self._outbox_row(outbox_id)
        state = self._consumer_state()
        processing = self.store.begin_outbox_consumer_attempt(
            state.id, "synthetic-worker", LEASE_EXPIRY
        )
        self.assertEqual(processing.status, "processing")
        self.assertEqual(processing.attempt_count, 1)
        self.assertEqual(processing.lease_owner, "synthetic-worker")
        self.assertEqual(processing.lease_expires_at, LEASE_EXPIRY)

        retried = self.store.retry_outbox_consumer_state(
            state.id,
            "synthetic-worker",
            "SyntheticRetry",
            available_at=READY_NOW,
        )
        self.assertEqual(retried.status, "pending")
        self.assertIsNone(retried.lease_owner)
        self.assertIsNone(retried.lease_expires_at)
        self.assertEqual(retried.last_error_type, "SyntheticRetry")
        self.assertEqual(retried.available_at, READY_NOW)
        self.assertEqual(self._outbox_row(outbox_id), outbox_before)

    def test_consumer_error_persists_only_exception_type(self) -> None:
        secret = "private raw content from a synthetic exception"
        state = self._consumer_state()
        self.store.begin_outbox_consumer_attempt(
            state.id, "synthetic-worker", LEASE_EXPIRY
        )

        with self.assertRaisesRegex(ValueError, "controlled consumer error type"):
            self.store.retry_outbox_consumer_state(
                state.id,
                "synthetic-worker",
                f"RuntimeError: {secret}",
                available_at=READY_NOW,
            )
        still_processing = self.store.get_outbox_consumer_state(
            state.outbox_id, CONSUMER, CONTRACT, "1"
        )
        self.assertEqual(still_processing.status, "processing")

        try:
            raise RuntimeError(secret)
        except RuntimeError as error:
            retried = self.store.retry_outbox_consumer_state(
                state.id,
                "synthetic-worker",
                error,
                available_at=READY_NOW,
            )
        self.assertEqual(retried.last_error_type, "RuntimeError")
        database_dump = "\n".join(self.store._conn.iterdump())
        self.assertNotIn(secret, database_dump)
        self.assertNotIn("Traceback (most recent call last)", database_dump)

    def test_terminal_failure_is_consumer_local(self) -> None:
        outbox_id = self.outbox_revision_pairs[0][0]
        before = self._outbox_row(outbox_id)
        state = self._consumer_state(outbox_id)
        self.store.begin_outbox_consumer_attempt(
            state.id, "synthetic-worker", LEASE_EXPIRY
        )
        failed = self.store.fail_outbox_consumer_state(
            state.id, "synthetic-worker", "SyntheticInvariant"
        )

        self.assertEqual(failed.status, "failed")
        self.assertEqual(failed.last_error_type, "SyntheticInvariant")
        self.assertEqual(self._outbox_row(outbox_id), before)

    def test_other_consumer_cannot_complete_standard_event_materialization(self) -> None:
        outbox_id, revision_id = self.outbox_revision_pairs[0]
        state = self._consumer_state(
            outbox_id, consumer_name="synthetic-indexer"
        )
        self.store.begin_outbox_consumer_attempt(
            state.id, "synthetic-worker", LEASE_EXPIRY
        )
        candidate = self._materialization(revision_id)

        with self.assertRaisesRegex(
            EventStoreError, "standard-event-normalizer consumer"
        ):
            self.store.complete_outbox_consumer_state_with_materialization(
                state.id, "synthetic-worker", candidate
            )
        self.assertIsNone(
            self.store.get_standard_event_materialization(
                revision_id, CONTRACT, NORMALIZER, "1"
            )
        )

    def test_materialization_insert_is_idempotent_and_readable(self) -> None:
        candidate = self._materialization()
        first = self.store.insert_standard_event_materialization(candidate)
        second = self.store.insert_standard_event_materialization(candidate)
        stored = self.store.get_standard_event_materialization(
            candidate.source_revision_id,
            candidate.contract_version,
            candidate.normalizer_name,
            candidate.normalizer_version,
        )

        self.assertTrue(first.inserted)
        self.assertFalse(second.inserted)
        self.assertEqual(first.materialization.id, second.materialization.id)
        self.assertEqual(stored, first.materialization)
        self.assertEqual(stored.canonical_payload_json, candidate.canonical_payload_json)
        self.assertEqual(stored.payload_sha256, candidate.payload_sha256)

    def test_same_pinned_versions_with_different_hash_is_invariant_conflict(self) -> None:
        original = self._materialization(payload_marker="original")
        conflicting = self._materialization(payload_marker="conflicting")
        inserted = self.store.insert_standard_event_materialization(original)

        with self.assertRaises(MaterializationConflict):
            self.store.insert_standard_event_materialization(conflicting)
        stored = self.store.get_standard_event_materialization(
            original.source_revision_id,
            original.contract_version,
            original.normalizer_name,
            original.normalizer_version,
        )
        self.assertEqual(stored.id, inserted.materialization.id)
        self.assertEqual(stored.payload_sha256, original.payload_sha256)

    def test_versions_and_source_revisions_can_coexist_append_only(self) -> None:
        first_revision = self.outbox_revision_pairs[0][1]
        second_revision = self.outbox_revision_pairs[1][1]
        candidates = (
            self._materialization(first_revision, normalizer_version="1"),
            self._materialization(
                first_revision,
                normalizer_version="2",
                payload_marker="normalizer-upgrade",
            ),
            self._materialization(
                second_revision,
                normalizer_version="1",
                payload_marker="second-revision",
                event_id="evt_synthetic_second",
            ),
        )
        results = [
            self.store.insert_standard_event_materialization(candidate)
            for candidate in candidates
        ]

        self.assertEqual(len({result.materialization.id for result in results}), 3)
        self.assertEqual(results[0].materialization.event_id, "evt_synthetic")
        self.assertEqual(results[1].materialization.event_id, "evt_synthetic")
        self.assertFalse(
            hasattr(self.store, "update_standard_event_materialization")
        )
        self.assertFalse(
            hasattr(self.store, "delete_standard_event_materialization")
        )

    def test_atomic_completion_commits_projection_and_consumer_only(self) -> None:
        outbox_id, revision_id = self.outbox_revision_pairs[0]
        outbox_before = self._outbox_row(outbox_id)
        state = self._consumer_state(outbox_id)
        self.store.begin_outbox_consumer_attempt(
            state.id, "synthetic-worker", LEASE_EXPIRY
        )
        candidate = self._materialization(revision_id)

        result = self.store.complete_outbox_consumer_state_with_materialization(
            state.id, "synthetic-worker", candidate
        )
        completed = self.store.get_outbox_consumer_state(
            outbox_id, CONSUMER, CONTRACT, "1"
        )

        self.assertTrue(result.inserted)
        self.assertEqual(completed.status, "complete")
        self.assertIsNotNone(completed.completed_at)
        self.assertIsNone(completed.lease_owner)
        self.assertEqual(self._outbox_row(outbox_id), outbox_before)

    def test_atomic_completion_rolls_back_both_writes_on_failure(self) -> None:
        outbox_id, revision_id = self.outbox_revision_pairs[1]
        outbox_before = self._outbox_row(outbox_id)
        state = self._consumer_state(outbox_id)
        self.store.begin_outbox_consumer_attempt(
            state.id, "synthetic-worker", LEASE_EXPIRY
        )
        candidate = self._materialization(
            revision_id,
            payload_marker="rollback",
            event_id="evt_rollback",
            subject_id="msg_rollback",
        )

        complete_state = self.store._mark_outbox_consumer_state_complete

        def complete_then_fail(*args) -> None:
            complete_state(*args)
            inserted_inside_transaction = (
                self.store.get_standard_event_materialization(
                    revision_id, CONTRACT, NORMALIZER, "1"
                )
            )
            completed_inside_transaction = self.store.get_outbox_consumer_state(
                outbox_id, CONSUMER, CONTRACT, "1"
            )
            self.assertIsNotNone(inserted_inside_transaction)
            self.assertEqual(completed_inside_transaction.status, "complete")
            self.assertIsNotNone(completed_inside_transaction.completed_at)
            self.assertIsNone(completed_inside_transaction.lease_owner)
            raise RuntimeError("synthetic failure before commit")

        with patch.object(
            self.store,
            "_mark_outbox_consumer_state_complete",
            side_effect=complete_then_fail,
        ):
            with self.assertRaisesRegex(RuntimeError, "synthetic failure"):
                self.store.complete_outbox_consumer_state_with_materialization(
                    state.id, "synthetic-worker", candidate
                )

        stored = self.store.get_standard_event_materialization(
            revision_id, CONTRACT, NORMALIZER, "1"
        )
        consumer = self.store.get_outbox_consumer_state(
            outbox_id, CONSUMER, CONTRACT, "1"
        )
        self.assertIsNone(stored)
        self.assertEqual(consumer.status, "processing")
        self.assertIsNone(consumer.completed_at)
        self.assertEqual(consumer.lease_owner, "synthetic-worker")
        self.assertEqual(consumer.lease_expires_at, LEASE_EXPIRY)
        self.assertEqual(self._outbox_row(outbox_id), outbox_before)

    def test_schema_has_required_foreign_keys_constraints_and_indexes(self) -> None:
        consumer_fk = self.store._conn.execute(
            "PRAGMA foreign_key_list(outbox_consumer_state)"
        ).fetchall()
        materialization_fk = self.store._conn.execute(
            "PRAGMA foreign_key_list(standard_event_materializations)"
        ).fetchall()
        indexes = {
            str(row[0])
            for row in self.store._conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='index' AND name NOT LIKE 'sqlite_autoindex_%'
                """
            )
        }

        self.assertEqual(consumer_fk[0][2], "processing_outbox")
        self.assertEqual(consumer_fk[0][6], "RESTRICT")
        self.assertEqual(materialization_fk[0][2], "event_revisions")
        self.assertEqual(materialization_fk[0][6], "RESTRICT")
        self.assertIn("outbox_consumer_state_ready_idx", indexes)
        self.assertIn("outbox_consumer_state_expired_lease_idx", indexes)
        self.assertIn("standard_event_materializations_event_idx", indexes)
        self.assertIn("standard_event_materializations_subject_idx", indexes)


if __name__ == "__main__":
    unittest.main()
