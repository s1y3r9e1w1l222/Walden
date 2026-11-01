from __future__ import annotations

import hashlib
import inspect
import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from wechat_local.event_normalizer import (
    NormalizerConfig,
    RawEventInput,
    RawRevisionInput,
    normalize_event as pure_normalize_event,
)
from wechat_local.event_store import (
    EventStore,
    MissingSourceRevision,
    RawMessage,
    SourceKey,
    StandardEventMaterializationInput,
)
from wechat_local import normalization_worker as worker_module
from wechat_local.normalization_worker import (
    CONSUMER_NAME,
    CONTRACT_VERSION,
    NORMALIZER_NAME,
    NORMALIZER_VERSION,
    NormalizationWorker,
)


LEASE_EXPIRY = "2099-01-01T00:00:00.000000+00:00"
READY_NOW = "2000-01-01T00:00:00.000000+00:00"


class NormalizationWorkerTests(unittest.TestCase):
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

    def tearDown(self) -> None:
        if self.store is not None:
            self.store.close()
        self.temporary.cleanup()

    def _worker(self, owner: str = "synthetic-worker") -> NormalizationWorker:
        return NormalizationWorker(
            self.store,
            owner,
            lease_seconds=120.0,
            retry_base_seconds=1.0,
            retry_max_seconds=4.0,
        )

    def _commit(
        self,
        local_id: int,
        *,
        server_id: int = 100,
        local_type: int = 1,
        create_time: int = 1_700_000_000,
        raw_content: str | bytes | None = "synthetic text",
        compression_type: int | None = 0,
    ) -> None:
        self.store.commit_messages(
            self.source,
            [
                RawMessage(
                    local_id=local_id,
                    server_id=server_id,
                    local_type=local_type,
                    create_time=create_time,
                    real_sender_id=42,
                    raw_content=raw_content,
                    compression_type=compression_type,
                )
            ],
        )

    def _outbox_revision_pairs(self) -> list[tuple[int, int]]:
        rows = self.store._conn.execute(
            "SELECT id, revision_id FROM processing_outbox ORDER BY id"
        ).fetchall()
        return [(int(row["id"]), int(row["revision_id"])) for row in rows]

    def _outbox_rows(self) -> list[tuple[object, ...]]:
        return [
            tuple(row)
            for row in self.store._conn.execute(
                "SELECT * FROM processing_outbox ORDER BY id"
            )
        ]

    def _state(self, outbox_id: int):
        return self.store.get_outbox_consumer_state(
            outbox_id,
            CONSUMER_NAME,
            CONTRACT_VERSION,
            NORMALIZER_VERSION,
        )

    @staticmethod
    def _materialization_from_source(source) -> StandardEventMaterializationInput:
        event = RawEventInput(
            event_uid=source.event_uid,
            account_id=source.account_id,
            source_generation=source.source_generation,
            shard_name=source.shard_name,
            table_name=source.table_name,
            local_id=source.local_id,
        )
        revision = RawRevisionInput(
            content_fingerprint=source.content_fingerprint,
            revision_number=source.revision_number,
            server_id=source.server_id,
            local_type=source.local_type,
            create_time=source.create_time,
            real_sender_id=source.real_sender_id,
            raw_content=source.raw_content,
            raw_content_kind=source.raw_content_kind,
            compression_type=source.compression_type,
            observed_at=source.observed_at,
        )
        normalized = pure_normalize_event(
            event,
            revision,
            NormalizerConfig(
                contract_version=CONTRACT_VERSION,
                normalizer_name=NORMALIZER_NAME,
                normalizer_version=NORMALIZER_VERSION,
            ),
        )
        return StandardEventMaterializationInput(
            source_revision_id=source.source_revision_id,
            event_id=normalized.event_id,
            subject_id=normalized.subject_id,
            contract_version=normalized.contract_version,
            normalizer_name=normalized.normalizer_name,
            normalizer_version=normalized.normalizer_version,
            canonical_payload_json=normalized.canonical_payload_json,
            payload_sha256=normalized.payload_sha256,
            quality_status=normalized.quality_status,
        )

    def _prepare_existing_materialization(self):
        self._commit(1)
        worker = self._worker()
        worker.bootstrap()
        state = self.store.claim_next_outbox_consumer_state(
            CONSUMER_NAME,
            CONTRACT_VERSION,
            NORMALIZER_VERSION,
            worker.lease_owner,
            LEASE_EXPIRY,
        )
        source = self.store.load_claimed_process_event(
            state.id, worker.lease_owner
        )
        candidate = self._materialization_from_source(source)
        self.store.insert_standard_event_materialization(candidate)
        self.store.retry_outbox_consumer_state(
            state.id,
            worker.lease_owner,
            "SyntheticSetup",
            available_at=READY_NOW,
        )
        return worker, state, candidate

    def test_successful_normalized_event_is_materialized_and_completed(self) -> None:
        self._commit(1, raw_content="synthetic normalized")
        outbox_before = self._outbox_rows()
        outbox_id, revision_id = self._outbox_revision_pairs()[0]

        result = self._worker().run_once()
        state = self._state(outbox_id)
        materialization = self.store.get_standard_event_materialization(
            revision_id,
            CONTRACT_VERSION,
            NORMALIZER_NAME,
            NORMALIZER_VERSION,
        )

        self.assertEqual(result.registered, 1)
        self.assertEqual(result.claimed, 1)
        self.assertEqual(result.normalized, 1)
        self.assertEqual(result.degraded, 0)
        self.assertEqual(state.status, "complete")
        self.assertEqual(materialization.quality_status, "normalized")
        self.assertEqual(
            json.loads(materialization.canonical_payload_json)["content"]["text"],
            "synthetic normalized",
        )
        self.assertEqual(self._outbox_rows(), outbox_before)

    def test_successful_degraded_event_completes_without_retry(self) -> None:
        self._commit(1, local_type=777, raw_content="synthetic unknown")
        outbox_before = self._outbox_rows()
        outbox_id, revision_id = self._outbox_revision_pairs()[0]

        result = self._worker().run_once()
        materialization = self.store.get_standard_event_materialization(
            revision_id,
            CONTRACT_VERSION,
            NORMALIZER_NAME,
            NORMALIZER_VERSION,
        )
        payload = json.loads(materialization.canonical_payload_json)

        self.assertEqual(result.degraded, 1)
        self.assertEqual(result.retried, 0)
        self.assertEqual(self._state(outbox_id).status, "complete")
        self.assertEqual(materialization.quality_status, "degraded")
        self.assertIn("unknown_message_type", payload["quality"]["codes"])
        self.assertEqual(self._outbox_rows(), outbox_before)

    def test_worker_materializes_every_exact_revision_not_only_current(self) -> None:
        self._commit(1, server_id=100, raw_content="revision one")
        first_revision = self._outbox_revision_pairs()[0][1]
        self._commit(1, server_id=101, raw_content="revision two")
        current_revision = int(
            self.store._conn.execute(
                "SELECT current_revision_id FROM events WHERE local_id=1"
            ).fetchone()[0]
        )
        self.assertNotEqual(first_revision, current_revision)

        result = self._worker().run_once()
        first = self.store.get_standard_event_materialization(
            first_revision,
            CONTRACT_VERSION,
            NORMALIZER_NAME,
            NORMALIZER_VERSION,
        )
        current = self.store.get_standard_event_materialization(
            current_revision,
            CONTRACT_VERSION,
            NORMALIZER_NAME,
            NORMALIZER_VERSION,
        )

        self.assertEqual(result.normalized, 2)
        self.assertEqual(
            json.loads(first.canonical_payload_json)["content"]["text"],
            "revision one",
        )
        self.assertEqual(
            json.loads(current.canonical_payload_json)["content"]["text"],
            "revision two",
        )
        self.assertEqual(first.subject_id, current.subject_id)
        self.assertNotEqual(first.event_id, current.event_id)

    def test_duplicate_execution_and_restart_are_idempotent(self) -> None:
        self._commit(1)
        outbox_id, _ = self._outbox_revision_pairs()[0]
        other = self.store.ensure_outbox_consumer_state(
            outbox_id, "synthetic-indexer", CONTRACT_VERSION, "1"
        )
        outbox_before = self._outbox_rows()

        first = self._worker("first-worker").run_once()
        second = self._worker("restart-worker").run_once()
        materialization_count = int(
            self.store._conn.execute(
                "SELECT count(*) FROM standard_event_materializations"
            ).fetchone()[0]
        )

        self.assertEqual(first.normalized, 1)
        self.assertEqual(second.claimed, 0)
        self.assertEqual(materialization_count, 1)
        self.assertEqual(
            self.store.get_outbox_consumer_state(
                outbox_id, "synthetic-indexer", CONTRACT_VERSION, "1"
            ),
            other,
        )
        self.assertEqual(self._outbox_rows(), outbox_before)

    def test_existing_identical_materialization_completes_idempotently(self) -> None:
        worker, state, candidate = self._prepare_existing_materialization()

        result = worker.run_once()
        stored = self.store.get_standard_event_materialization(
            candidate.source_revision_id,
            candidate.contract_version,
            candidate.normalizer_name,
            candidate.normalizer_version,
        )

        self.assertEqual(result.normalized, 1)
        self.assertEqual(self._state(state.outbox_id).status, "complete")
        self.assertEqual(stored.payload_sha256, candidate.payload_sha256)
        self.assertEqual(
            int(
                self.store._conn.execute(
                    "SELECT count(*) FROM standard_event_materializations"
                ).fetchone()[0]
            ),
            1,
        )

    def test_materialization_conflict_is_terminal_and_preserves_existing(self) -> None:
        worker, state, candidate = self._prepare_existing_materialization()
        outbox_before = self._outbox_rows()
        conflicting_payload = '{"synthetic":"conflict"}'
        conflicting = replace(
            candidate,
            canonical_payload_json=conflicting_payload,
            payload_sha256=hashlib.sha256(
                conflicting_payload.encode("utf-8")
            ).hexdigest(),
        )
        self.store._conn.execute(
            "DELETE FROM standard_event_materializations WHERE source_revision_id=?",
            (candidate.source_revision_id,),
        )
        self.store._conn.commit()
        self.store.insert_standard_event_materialization(conflicting)
        fail_state = []
        fail_transition = self.store.fail_outbox_consumer_state

        def observe_fail(consumer_state_id, lease_owner, error):
            current = self._state(state.outbox_id)
            stored_before_fail = self.store.get_standard_event_materialization(
                conflicting.source_revision_id,
                conflicting.contract_version,
                conflicting.normalizer_name,
                conflicting.normalizer_version,
            )
            fail_state.append(
                (
                    current.status,
                    current.lease_owner,
                    current.completed_at,
                    stored_before_fail.payload_sha256,
                )
            )
            return fail_transition(consumer_state_id, lease_owner, error)

        with patch.object(
            self.store,
            "fail_outbox_consumer_state",
            side_effect=observe_fail,
        ):
            result = worker.run_once()
        failed = self._state(state.outbox_id)
        stored = self.store.get_standard_event_materialization(
            conflicting.source_revision_id,
            conflicting.contract_version,
            conflicting.normalizer_name,
            conflicting.normalizer_version,
        )

        self.assertEqual(result.claimed, 1)
        self.assertEqual(result.failed, 1)
        self.assertEqual(result.retried, 0)
        self.assertEqual(
            fail_state,
            [
                (
                    "processing",
                    worker.lease_owner,
                    None,
                    conflicting.payload_sha256,
                )
            ],
        )
        self.assertEqual(failed.status, "failed")
        self.assertEqual(failed.last_error_type, "MaterializationConflict")
        self.assertIsNone(failed.completed_at)
        self.assertIsNone(failed.lease_owner)
        self.assertEqual(stored.payload_sha256, conflicting.payload_sha256)
        self.assertEqual(self._outbox_rows(), outbox_before)

    def test_conflict_fail_contention_recovers_without_stale_owner_mutation(
        self,
    ) -> None:
        worker, state, candidate = self._prepare_existing_materialization()
        conflicting_payload = '{"synthetic":"existing-conflict"}'
        conflicting = replace(
            candidate,
            canonical_payload_json=conflicting_payload,
            payload_sha256=hashlib.sha256(
                conflicting_payload.encode("utf-8")
            ).hexdigest(),
        )
        self.store._conn.execute(
            "DELETE FROM standard_event_materializations WHERE source_revision_id=?",
            (candidate.source_revision_id,),
        )
        self.store._conn.commit()
        self.store.insert_standard_event_materialization(conflicting)
        outbox_before = self._outbox_rows()

        with patch.object(
            self.store,
            "fail_outbox_consumer_state",
            side_effect=sqlite3.OperationalError("database is locked"),
        ):
            first = worker.run_once()

        processing = self._state(state.outbox_id)
        self.assertEqual(first.retried, 1)
        self.assertEqual(first.failed, 0)
        self.assertEqual(processing.status, "processing")
        self.assertEqual(processing.lease_owner, worker.lease_owner)
        self.assertIsNone(processing.completed_at)

        self.store._conn.execute(
            "UPDATE outbox_consumer_state SET lease_expires_at=? WHERE id=?",
            (READY_NOW, processing.id),
        )
        self.store._conn.commit()
        recovered = self._worker("conflict-recovery-worker").run_once()
        terminal = self._state(state.outbox_id)
        stored = self.store.get_standard_event_materialization(
            conflicting.source_revision_id,
            conflicting.contract_version,
            conflicting.normalizer_name,
            conflicting.normalizer_version,
        )

        self.assertEqual(recovered.failed, 1)
        self.assertEqual(terminal.status, "failed")
        self.assertEqual(terminal.last_error_type, "MaterializationConflict")
        self.assertIsNone(terminal.lease_owner)
        self.assertEqual(stored.payload_sha256, conflicting.payload_sha256)
        self.assertEqual(self._outbox_rows(), outbox_before)

    def test_storage_contention_retries_with_controlled_category(self) -> None:
        self._commit(1)
        outbox_id, revision_id = self._outbox_revision_pairs()[0]
        worker = self._worker()

        with patch.object(
            self.store,
            "complete_outbox_consumer_state_with_materialization",
            side_effect=sqlite3.OperationalError("database is locked"),
        ):
            result = worker.run_once()

        state = self._state(outbox_id)
        self.assertEqual(result.retried, 1)
        self.assertEqual(state.status, "pending")
        self.assertEqual(state.last_error_type, "StorageContention")
        self.assertIsNone(state.lease_owner)
        self.assertIsNone(
            self.store.get_standard_event_materialization(
                revision_id,
                CONTRACT_VERSION,
                NORMALIZER_NAME,
                NORMALIZER_VERSION,
            )
        )

    def test_other_operational_errors_are_not_retryable_contention(self) -> None:
        self.assertFalse(
            worker_module._is_storage_contention(
                sqlite3.OperationalError("database is not locked")
            )
        )
        self.assertFalse(
            worker_module._is_storage_contention(
                sqlite3.OperationalError("disk I/O error")
            )
        )
        self._commit(1)
        outbox_id, _ = self._outbox_revision_pairs()[0]

        with patch.object(
            self.store,
            "complete_outbox_consumer_state_with_materialization",
            side_effect=sqlite3.OperationalError("disk I/O error"),
        ):
            result = self._worker().run_once()

        state = self._state(outbox_id)
        self.assertEqual(result.retried, 0)
        self.assertEqual(result.failed, 1)
        self.assertEqual(state.status, "failed")
        self.assertEqual(state.last_error_type, "WorkerInvariant")

    def test_retry_backoff_is_exponential_and_bounded(self) -> None:
        fixed_now = datetime(2026, 8, 29, tzinfo=timezone.utc)
        worker = NormalizationWorker(
            self.store,
            "backoff-worker",
            retry_base_seconds=1.0,
            retry_max_seconds=4.0,
            clock=lambda: fixed_now,
        )

        self.assertEqual(
            worker._retry_available_at(1),
            "2026-08-29T00:00:01.000000+00:00",
        )
        self.assertEqual(
            worker._retry_available_at(2),
            "2026-08-29T00:00:02.000000+00:00",
        )
        self.assertEqual(
            worker._retry_available_at(99),
            "2026-08-29T00:00:04.000000+00:00",
        )
        self.assertEqual(
            worker._retry_available_at(10**1_000),
            "2026-08-29T00:00:04.000000+00:00",
        )

    def test_timing_configuration_rejects_nonfinite_and_invalid_ranges(self) -> None:
        invalid = (
            {"lease_seconds": 0.0},
            {"lease_seconds": float("nan")},
            {"retry_base_seconds": float("inf")},
            {"retry_max_seconds": -1.0},
            {"poll_interval_seconds": float("-inf")},
            {"retry_base_seconds": 2.0, "retry_max_seconds": 1.0},
        )
        for overrides in invalid:
            with self.subTest(overrides=overrides):
                with self.assertRaises(ValueError):
                    NormalizationWorker(self.store, "timing-worker", **overrides)

        for value in ("0", "-1", "nan", "inf", "-inf"):
            with self.subTest(cli_value=value):
                stderr = io.StringIO()
                with redirect_stderr(stderr), self.assertRaises(SystemExit):
                    worker_module.build_parser().parse_args(
                        [
                            "--event-store",
                            "synthetic.db",
                            "--once",
                            f"--lease-seconds={value}",
                        ]
                    )
                self.assertIn("finite and positive", stderr.getvalue())

    def test_missing_revision_is_permanent_without_persisting_detail(self) -> None:
        self._commit(1)
        outbox_id, _ = self._outbox_revision_pairs()[0]
        secret = "private synthetic source detail"

        with patch.object(
            self.store,
            "load_claimed_process_event",
            side_effect=MissingSourceRevision(secret),
        ):
            result = self._worker().run_once()

        state = self._state(outbox_id)
        self.assertEqual(result.failed, 1)
        self.assertEqual(state.status, "failed")
        self.assertEqual(state.last_error_type, "MissingSourceRevision")
        self.assertNotIn(secret, "\n".join(self.store._conn.iterdump()))

    def test_invalid_raw_record_is_terminal(self) -> None:
        self._commit(1)
        outbox_id, revision_id = self._outbox_revision_pairs()[0]
        self.store._conn.execute(
            """
            UPDATE event_revisions
            SET raw_content=?, raw_content_kind='text'
            WHERE id=?
            """,
            (sqlite3.Binary(b"synthetic invalid text storage"), revision_id),
        )
        self.store._conn.commit()

        result = self._worker().run_once()
        state = self._state(outbox_id)

        self.assertEqual(result.failed, 1)
        self.assertEqual(state.status, "failed")
        self.assertEqual(state.last_error_type, "InvalidRawRecord")

    def test_expired_lease_is_recovered_after_restart(self) -> None:
        self._commit(1)
        outbox_id, _ = self._outbox_revision_pairs()[0]
        worker = self._worker("restart-worker")
        worker.bootstrap()
        state = self._state(outbox_id)
        self.store.begin_outbox_consumer_attempt(
            state.id, "crashed-worker", READY_NOW
        )

        result = worker.run_once()
        completed = self._state(outbox_id)

        self.assertEqual(result.normalized, 1)
        self.assertEqual(completed.status, "complete")
        self.assertEqual(completed.attempt_count, 2)

    def test_stale_worker_discards_result_after_lease_loss(self) -> None:
        self._commit(1)
        outbox_before = self._outbox_rows()
        old_worker = self._worker("old-worker")
        new_worker = self._worker("new-worker")
        old_worker.bootstrap()
        nested_results = []
        triggered = False

        def recover_during_normalization(event, revision, config):
            nonlocal triggered
            if not triggered:
                triggered = True
                self.store._conn.execute(
                    """
                    UPDATE outbox_consumer_state
                    SET lease_expires_at=? WHERE lease_owner='old-worker'
                    """,
                    (READY_NOW,),
                )
                self.store._conn.commit()
                nested_results.append(new_worker.process_one())
            return pure_normalize_event(event, revision, config)

        with patch(
            "wechat_local.normalization_worker.normalize_event",
            side_effect=recover_during_normalization,
        ):
            stale = old_worker.process_one()

        self.assertEqual(nested_results[0].outcome, "normalized")
        self.assertEqual(stale.outcome, "lease_lost")
        state = self._state(self._outbox_revision_pairs()[0][0])
        self.assertEqual(state.status, "complete")
        self.assertIsNone(state.lease_owner)
        self.assertEqual(
            int(
                self.store._conn.execute(
                    "SELECT count(*) FROM standard_event_materializations"
                ).fetchone()[0]
            ),
            1,
        )
        self.assertEqual(self._outbox_rows(), outbox_before)

    def test_crash_before_atomic_persistence_recovers_after_expiry(self) -> None:
        self._commit(1)
        outbox_before = self._outbox_rows()
        outbox_id, revision_id = self._outbox_revision_pairs()[0]
        worker = self._worker("crash-worker")
        worker.bootstrap()

        with patch.object(
            self.store,
            "complete_outbox_consumer_state_with_materialization",
            side_effect=KeyboardInterrupt(),
        ):
            with self.assertRaises(KeyboardInterrupt):
                worker.process_one()

        processing = self._state(outbox_id)
        self.assertEqual(processing.status, "processing")
        self.assertIsNone(
            self.store.get_standard_event_materialization(
                revision_id,
                CONTRACT_VERSION,
                NORMALIZER_NAME,
                NORMALIZER_VERSION,
            )
        )
        self.store._conn.execute(
            "UPDATE outbox_consumer_state SET lease_expires_at=? WHERE id=?",
            (READY_NOW, processing.id),
        )
        self.store._conn.commit()
        recovered = self._worker("recovery-worker").process_one()
        self.assertEqual(recovered.outcome, "normalized")
        self.assertEqual(self._outbox_rows(), outbox_before)

    def test_crash_inside_atomic_transaction_rolls_back_both_writes(self) -> None:
        self._commit(1)
        outbox_before = self._outbox_rows()
        outbox_id, revision_id = self._outbox_revision_pairs()[0]
        worker = self._worker("atomic-crash-worker")
        worker.bootstrap()
        complete_state = self.store._mark_outbox_consumer_state_complete

        def complete_then_crash(*args) -> None:
            complete_state(*args)
            raise KeyboardInterrupt()

        with patch.object(
            self.store,
            "_mark_outbox_consumer_state_complete",
            side_effect=complete_then_crash,
        ):
            with self.assertRaises(KeyboardInterrupt):
                worker.process_one()
        self.store._conn.rollback()

        processing = self._state(outbox_id)
        self.assertEqual(processing.status, "processing")
        self.assertIsNone(processing.completed_at)
        self.assertIsNone(
            self.store.get_standard_event_materialization(
                revision_id,
                CONTRACT_VERSION,
                NORMALIZER_NAME,
                NORMALIZER_VERSION,
            )
        )
        self.assertEqual(self._outbox_rows(), outbox_before)
        self.store._conn.execute(
            "UPDATE outbox_consumer_state SET lease_expires_at=? WHERE id=?",
            (READY_NOW, processing.id),
        )
        self.store._conn.commit()
        self.assertEqual(
            self._worker("atomic-recovery-worker").process_one().outcome,
            "normalized",
        )

    def test_failed_earlier_item_does_not_block_later_ready_item(self) -> None:
        self._commit(1, raw_content="first")
        self._commit(2, raw_content="second")
        outbox_before = self._outbox_rows()
        first_outbox = self._outbox_revision_pairs()[0][0]
        worker = self._worker()
        worker.bootstrap()
        original_load = self.store.load_claimed_process_event

        def fail_first(consumer_state_id, lease_owner):
            row = self.store._conn.execute(
                "SELECT outbox_id FROM outbox_consumer_state WHERE id=?",
                (consumer_state_id,),
            ).fetchone()
            if int(row["outbox_id"]) == first_outbox:
                raise MissingSourceRevision("synthetic missing relation")
            return original_load(consumer_state_id, lease_owner)

        with patch.object(
            self.store,
            "load_claimed_process_event",
            side_effect=fail_first,
        ):
            result = worker.run_once()

        self.assertEqual(result.failed, 1)
        self.assertEqual(result.normalized, 1)
        self.assertEqual(self._state(first_outbox).status, "failed")
        self.assertEqual(self._outbox_rows(), outbox_before)

    def test_run_once_uses_finite_bootstrap_snapshot(self) -> None:
        self._commit(1, raw_content="first snapshot")
        worker = self._worker()
        inserted_during_run = False

        def insert_new_outbox(event, revision, config):
            nonlocal inserted_during_run
            if not inserted_during_run:
                inserted_during_run = True
                self._commit(2, raw_content="next snapshot")
            return pure_normalize_event(event, revision, config)

        with patch(
            "wechat_local.normalization_worker.normalize_event",
            side_effect=insert_new_outbox,
        ):
            first = worker.run_once()

        state_count = int(
            self.store._conn.execute(
                "SELECT count(*) FROM outbox_consumer_state WHERE consumer_name=?",
                (CONSUMER_NAME,),
            ).fetchone()[0]
        )
        second = worker.run_once()

        self.assertEqual(first.registered, 1)
        self.assertEqual(first.normalized, 1)
        self.assertEqual(state_count, 1)
        self.assertEqual(second.registered, 1)
        self.assertEqual(second.normalized, 1)

    def test_run_loop_sleeps_only_between_finite_passes(self) -> None:
        self._commit(1)
        emitted = []

        class StopLoop(Exception):
            pass

        def stop_after_first_sleep(seconds: float) -> None:
            self.assertEqual(seconds, 0.25)
            raise StopLoop()

        worker = NormalizationWorker(
            self.store,
            "loop-worker",
            poll_interval_seconds=0.25,
            sleeper=stop_after_first_sleep,
        )

        with self.assertRaises(StopLoop):
            worker.run_loop(emitted.append)

        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0].normalized, 1)

    def test_cli_once_is_aggregate_only_and_uses_only_event_store(self) -> None:
        self._commit(1, raw_content="private synthetic cli content")
        store_path_text = str(self.store_path)
        self.store.close()
        self.store = None
        stdout = io.StringIO()
        stderr = io.StringIO()

        with redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = worker_module.main(
                ["--event-store", store_path_text, "--once", "--json"]
            )

        payload = json.loads(stdout.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr.getvalue(), "")
        self.assertEqual(
            set(payload),
            {
                "registered",
                "claimed",
                "normalized",
                "degraded",
                "retried",
                "failed",
                "lease_lost",
                "pending",
            },
        )
        self.assertNotIn("private synthetic cli content", stdout.getvalue())
        self.assertNotIn(store_path_text, stdout.getvalue())
        with EventStore(self.store_path) as reopened:
            self.assertEqual(
                int(
                    reopened._conn.execute(
                        "SELECT count(*) FROM standard_event_materializations"
                    ).fetchone()[0]
                ),
                1,
            )

    def test_cli_failure_emits_only_controlled_category(self) -> None:
        secret = "private failure detail"
        private_path = "/private/synthetic/events.db"
        stdout = io.StringIO()
        stderr = io.StringIO()

        with patch.object(
            worker_module,
            "EventStore",
            side_effect=RuntimeError(f"{secret}: {private_path}"),
        ), redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = worker_module.main(
                ["--event-store", private_path, "--once", "--json"]
            )

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(
            stderr.getvalue(), "NORMALIZATION_WORKER ERROR WorkerInvariant\n"
        )
        self.assertNotIn(secret, stderr.getvalue())
        self.assertNotIn(private_path, stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())

    def test_worker_profile_matches_current_i2_supported_values(self) -> None:
        self.assertEqual(CONSUMER_NAME, "standard-event-normalizer")
        self.assertEqual(CONTRACT_VERSION, "wechat-local.standard-event/1")
        self.assertEqual(NORMALIZER_NAME, "wechat-macos-v4")
        self.assertEqual(NORMALIZER_VERSION, "2")
        worker = self._worker()
        self.assertEqual(worker.config.contract_version, CONTRACT_VERSION)
        self.assertEqual(worker.config.normalizer_name, NORMALIZER_NAME)
        self.assertEqual(worker.config.normalizer_version, NORMALIZER_VERSION)

    def test_module_has_no_live_session_agent_model_or_network_dependency(self) -> None:
        source = inspect.getsource(worker_module)
        forbidden_import_fragments = (
            "live_tailer",
            "live_sessions",
            "session_watcher",
            "live_readonly",
            "wechat db reader",
            "sqlcipher",
            "sessiontable",
            "openai",
            "anthropic",
            "requests",
            "urllib",
            "socket",
            "llm",
            "agent",
            "memory",
            "from .rag",
            "embedding",
        )
        lowered = source.lower()
        for fragment in forbidden_import_fragments:
            with self.subTest(fragment=fragment):
                self.assertNotIn(fragment, lowered)

        parser_destinations = {
            action.dest for action in worker_module.build_parser()._actions
        }
        for forbidden_option in (
            "live_db",
            "live_db_root",
            "session_db",
            "message_db",
            "passphrase",
            "agent",
            "model",
            "api_key",
            "delivery",
        ):
            self.assertNotIn(forbidden_option, parser_destinations)


if __name__ == "__main__":
    unittest.main()
