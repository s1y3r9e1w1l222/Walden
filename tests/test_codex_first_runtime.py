from __future__ import annotations

import ast
import sqlite3
import shutil
import socket
import subprocess
import tempfile
import unittest
import urllib.request
import webbrowser
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from wechat_local.agent_store import AgentStore
import wechat_local.codex_first_runtime as runtime_module
from wechat_local.codex_first import TimeWindow
from wechat_local.codex_first_runtime import (
    CodexFirstRuntime,
    SourceCapabilityEvidence,
    inspect_source_capabilities,
    main,
    _result_payload,
)
from wechat_local.event_store import EventStore, RawMessage, SourceKey, WindowIngestRun
from wechat_local.live_catalog import LiveCatalog


TABLE = "Msg_00000000000000000000000000000000"
START = datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc)
END = datetime(2023, 11, 14, 22, 15, 0, tzinfo=timezone.utc)


def _open_query_only(path: Path):
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA query_only = ON")
    return connection


class CodexFirstRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.message_root = self.root / "live" / "message"
        self.message_root.mkdir(parents=True)
        self.source_path = self.message_root / "message_0.db"
        source = sqlite3.connect(self.source_path)
        source.execute(
            f"""
            CREATE TABLE {TABLE} (
                local_id INTEGER PRIMARY KEY,
                server_id INTEGER,
                local_type INTEGER NOT NULL,
                create_time INTEGER,
                real_sender_id INTEGER,
                message_content BLOB,
                WCDB_CT_message_content INTEGER
            )
            """
        )
        source.executemany(
            f"INSERT INTO {TABLE} VALUES (?, ?, 1, ?, 7, ?, 0)",
            (
                (1, 101, 1_699_999_999, "outside-before"),
                (2, 102, 1_700_000_000, "fixture-one"),
                (3, 103, 1_700_000_050, "fixture-two"),
                (4, 104, 1_700_000_101, "outside-after"),
            ),
        )
        source.commit()
        source.close()

        self.event_store = EventStore(self.root / "private" / "events.db")
        self.agent_store = AgentStore(self.root / "private" / "agent.db")
        self.catalog = LiveCatalog.dynamic(
            self.message_root,
            _open_query_only,
            store=self.event_store,
            account_id="synthetic-account",
            source_generation="synthetic-generation",
        )
        self.source = SourceKey(
            "synthetic-account",
            "synthetic-generation",
            "message_0.db",
            TABLE,
        )
        self.capability_checks = 0

    def tearDown(self) -> None:
        self.catalog.close()
        self.agent_store.close()
        self.event_store.close()
        self.temporary.cleanup()

    def _proven_capability(self, _catalog: LiveCatalog) -> SourceCapabilityEvidence:
        self.capability_checks += 1
        return SourceCapabilityEvidence(
            direction_proven=True,
            contact_mapping_proven=True,
            inbound_message=lambda _source, message: message.real_sender_id == 7,
            inbound_event_ref=lambda _event_ref: True,
            display_name=lambda _conversation_id: "Fixture Contact",
        )

    def _runtime(self, capability_check=None) -> CodexFirstRuntime:
        return CodexFirstRuntime(
            self.catalog,
            self.event_store,
            self.agent_store,
            account_id="synthetic-account",
            source_generation="synthetic-generation",
            capability_check=capability_check or self._proven_capability,
            batch_size=1,
        )

    def _insert_source_rows(self, rows) -> None:
        with sqlite3.connect(self.source_path) as source:
            source.executemany(
                f"INSERT INTO {TABLE} VALUES (?, ?, 1, ?, 7, ?, 0)", rows
            )

    def test_scan_ingests_only_requested_window_and_returns_grouped_event_refs(self) -> None:
        result = self._runtime().scan(TimeWindow.from_utc(START, END), display_limit=50)

        self.assertEqual(result.requested_range, (START, END))
        self.assertEqual(result.groups[0].total_messages, 2)
        self.assertEqual(
            [message.text for message in result.groups[0].messages],
            ["fixture-one", "fixture-two"],
        )
        self.assertTrue(all(message.event_ref["direction"] == "inbound" for message in result.groups[0].messages))
        self.assertIsNone(self.event_store.get_cursor(self.source))
        self.assertEqual(self.capability_checks, 1)

    def test_attachment_reference_uses_existing_attributed_window_and_preserves_projection(self):
        from wechat_local.attachment_resolver import AttachmentResolver
        import hashlib
        data = b'%PDF-1.7\nfixture'
        files = self.root.resolve() / 'attachments'
        files.mkdir()
        path = files / 'report.pdf'
        path.write_bytes(data)
        xml = ('<msg><appmsg><type>6</type><title>report.pdf</title><appattach>'
               f'<totallen>{len(data)}</totallen><md5>{hashlib.md5(data).hexdigest()}</md5>'
               '</appattach></appmsg></msg>')
        with sqlite3.connect(self.source_path) as source:
            source.execute(f'UPDATE {TABLE} SET local_type=49,message_content=? WHERE local_id=2', (xml,))
        runtime = self._runtime()
        window = TimeWindow.from_utc(START, END)
        without = runtime.scan(window, display_limit=50)
        self.assertNotIn('attachment', without.groups[0].messages[0].event_ref)
        runtime.attachment_resolver = AttachmentResolver(files, account_id='synthetic-account',
                                                       source_generation='synthetic-generation')
        first = runtime.scan(window, display_limit=50)
        second = runtime.scan(window, display_limit=50)
        ref = first.groups[0].messages[0].event_ref
        self.assertEqual(ref['attachment']['status'], 'resolved')
        self.assertEqual(ref['attachment']['path'], str(path))
        self.assertEqual(first.groups[0].messages[0].event_id, without.groups[0].messages[0].event_id)
        self.assertEqual(ref, second.groups[0].messages[0].event_ref)
        self.assertNotIn('attachment', first.groups[0].messages[1].event_ref)
        self.assertEqual(runtime.scan(window, display_limit=1, display_offset=1).total_messages, 2)
        self.assertIsNone(self.event_store.get_cursor(self.source))

    def test_attachment_cli_rejects_root_from_another_account_and_location_mode(self):
        from wechat_local.codex_first_runtime import _attachment_resolver_for_args, build_parser
        parser = build_parser()
        args = parser.parse_args(['--file-attachments-root', str(self.root / 'another-account')])
        with self.assertRaisesRegex(ValueError, 'attachment_root_outside_account'):
            _attachment_resolver_for_args(args, self.root / 'selected-account' / 'db_storage')
        payload = StringIO()
        with redirect_stdout(payload):
            exit_code = main(['--file-attachments-root', str(self.root), '--locate-related-groups',
                              '--contact-query', 'fixture', '--include-related-groups', '--json'])
        self.assertEqual(exit_code, 2)
        self.assertIn('locate_media_or_check_unsupported', payload.getvalue())

    def test_source_changed_during_attachment_hashing_is_not_emitted(self):
        from wechat_local.attachment_resolver import AttachmentResolver
        import hashlib
        data = b'%PDF-1.7\nfixture'
        files = self.root.resolve() / 'attachments'
        files.mkdir()
        (files / 'report.pdf').write_bytes(data)
        xml = ('<msg><appmsg><type>6</type><title>report.pdf</title><appattach>'
               f'<totallen>{len(data)}</totallen><md5>{hashlib.md5(data).hexdigest()}</md5>'
               '</appattach></appmsg></msg>')
        with sqlite3.connect(self.source_path) as source:
            source.execute(f'UPDATE {TABLE} SET local_type=49,message_content=? WHERE local_id=2', (xml,))
        runtime = self._runtime()
        resolver = AttachmentResolver(files, account_id='synthetic-account', source_generation='synthetic-generation')
        runtime.attachment_resolver = resolver
        verify = resolver._verify
        def changed(*args):
            result = verify(*args)
            with sqlite3.connect(self.source_path) as source:
                source.execute(f'UPDATE {TABLE} SET real_sender_id=999,message_content=? WHERE local_id=2', ('changed fixture',))
            return result
        emitted = []
        with patch.object(resolver, '_verify', changed):
            result = runtime.scan(TimeWindow.from_utc(START, END), display_limit=50, on_base_messages=emitted.append)
        self.assertEqual(result.status, 'unavailable')
        self.assertEqual(result.groups, ())
        self.assertEqual(emitted, [])

    def test_source_filter_is_applied_before_preflight_and_message_read(self) -> None:
        statements: list[str] = []
        shard = self.catalog.shard_for_source(self.source)
        shard.connection.set_trace_callback(statements.append)
        runtime = CodexFirstRuntime(
            self.catalog, self.event_store, self.agent_store,
            account_id="synthetic-account", source_generation="synthetic-generation",
            capability_check=self._proven_capability,
            source_filter=lambda _source: False,
        )
        result = runtime.scan(TimeWindow.from_utc(START, END), display_limit=50)
        self.assertEqual(result.status, "unavailable")
        reads = [s.lower() for s in statements if TABLE.lower() in s.lower() and s.lstrip().lower().startswith("select")]
        self.assertEqual(reads, [])

    def test_unique_sender_identity_is_projected_outside_frozen_event(self) -> None:
        evidence = SourceCapabilityEvidence(
            direction_proven=True,
            contact_mapping_proven=True,
            direction_for_message=lambda _source, _message: "inbound",
            display_name=lambda _conversation_id: "Fixture Contact",
            sender_identity=lambda _source, message: "synthetic-sender" if message.real_sender_id == 7 else None,
            sender_display_name=lambda _source, message: "Synthetic Sender Name" if message.real_sender_id == 7 else None,
        )
        result = self._runtime(lambda _catalog: evidence).scan(TimeWindow.from_utc(START, END), display_limit=50)
        ref = _result_payload(result)["groups"][0]["event_refs"][0]
        self.assertEqual(ref["sender_identity"], "synthetic-sender")
        self.assertEqual(ref["sender_display_name"], "Synthetic Sender Name")

    def test_image_enricher_runs_only_for_visible_bound_image_messages(self) -> None:
        self._insert_source_rows([(6, 106, 1_700_000_075, "image-placeholder")])
        with sqlite3.connect(self.source_path) as source:
            source.execute(
                f"UPDATE {TABLE} SET local_type=3 WHERE local_id=6"
            )
        calls = []
        runtime = CodexFirstRuntime(
            self.catalog,
            self.event_store,
            self.agent_store,
            account_id="synthetic-account",
            source_generation="synthetic-generation",
            capability_check=self._proven_capability,
            image_enricher=lambda source, message, conversation: calls.append(
                (source, message.local_id, conversation)
            ) or {"status": "bound", "representations": [4, 5]},
        )

        window = TimeWindow.from_utc(START, END)
        first_page = runtime.scan(window, display_limit=2)
        self.assertEqual(calls, [])
        self.assertEqual(first_page.total_messages, 3)
        self.assertEqual([message.text for message in first_page.groups[0].messages], ["fixture-one", "fixture-two"])
        result = runtime.scan(window, display_limit=1, display_offset=2)

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][1], 6)
        self.assertEqual(len(result.groups[0].messages), 1)
        self.assertEqual(result.groups[0].messages[0].event_ref["image"]["representations"], [4, 5])

    def test_reuses_preexisting_agent_store_and_projection_is_idempotent(self) -> None:
        runtime = self._runtime()
        first = runtime.scan(TimeWindow.from_utc(START, END), display_limit=50)
        second = runtime.scan(TimeWindow.from_utc(START, END), display_limit=50)

        self.assertEqual(second.total_messages, first.total_messages)
        projected = self.agent_store._conn.execute(
            "SELECT count(*) FROM event_search_projection"
        ).fetchone()[0]
        self.assertEqual(projected, 2)
        self.assertEqual(self.event_store.count_events(), 2)

    def test_keyword_search_is_bounded_by_window_and_conversation_id(self) -> None:
        runtime = self._runtime()
        scanned = runtime.scan(TimeWindow.from_utc(START, END), display_limit=50)

        page = runtime.search(
            TimeWindow.from_utc(START, END),
            query="fixture",
            conversation_id=scanned.groups[0].conversation_id,
            limit=50,
        )

        self.assertEqual([item["citation_text"] for item in page.items], ["fixture-two", "fixture-one"])
        self.assertIsNone(page.next_cursor)

    def test_incomplete_source_coverage_is_reported_without_expanding_range(self) -> None:
        wider_start = datetime(2023, 11, 14, 22, 11, 40, tzinfo=timezone.utc)
        wider_end = datetime(2023, 11, 14, 22, 16, 40, tzinfo=timezone.utc)

        result = self._runtime().scan(
            TimeWindow.from_utc(wider_start, wider_end), display_limit=50
        )

        self.assertEqual(result.requested_range, (wider_start, wider_end))
        self.assertFalse(result.coverage.complete)
        self.assertTrue(result.coverage.gaps)

    def test_result_payload_exposes_explicit_coverage_status(self) -> None:
        result = self._runtime().scan(TimeWindow.from_utc(START, END), display_limit=50)
        self.assertEqual(_result_payload(result)["coverage_status"], "partial")
        self.assertIn("timestamp_integrity_unknown_outside_requested_window", _result_payload(result)["gaps"])
        self.assertGreaterEqual(result.total_messages, 2)

    def test_no_available_live_source_returns_controlled_unavailable_result(self) -> None:
        empty_root = self.root / "empty-message-root"
        empty_root.mkdir()
        empty_catalog = LiveCatalog.dynamic(
            empty_root,
            _open_query_only,
            store=self.event_store,
            account_id="synthetic-account",
            source_generation="empty-generation",
        )
        try:
            runtime = CodexFirstRuntime(
                empty_catalog,
                self.event_store,
                self.agent_store,
                account_id="synthetic-account",
                source_generation="empty-generation",
                capability_check=self._proven_capability,
            )
            result = runtime.scan(TimeWindow.from_utc(START, END), display_limit=50)
        finally:
            empty_catalog.close()

        self.assertEqual(result.error_code, "source_unavailable")
        self.assertEqual(result.groups, ())
        self.assertEqual(result.requested_range, (START, END))

    def test_known_missing_current_shard_is_reported_as_incomplete(self) -> None:
        self.source_path.unlink()
        result = self._runtime().scan(TimeWindow.from_utc(START, END), display_limit=50)
        self.assertEqual(result.error_code, "source_unavailable")
        self.assertIn("known_source_missing", result.coverage.gaps)
        self.assertFalse(result.coverage.complete)

    def test_large_window_requires_preflight_confirmation_before_content_read(self) -> None:
        runtime = self._runtime()
        wide_end = END + timedelta(days=2)
        preflight = runtime.preflight(TimeWindow.from_utc(START, wide_end))
        self.assertEqual(preflight.status, "awaiting_confirmation")
        self.assertGreaterEqual(preflight.estimated_rows, 0)
        self.assertTrue(preflight.confirmation_token)
        result = runtime.scan(TimeWindow.from_utc(START, wide_end), display_limit=50)
        self.assertEqual(result.status, "confirmation_required")
        self.assertEqual(self.event_store.count_events(), 0)

    def test_voice_epoch_rejects_same_path_replaced_object(self):
        runtime = self._runtime()
        shard = self.catalog.shard_for_source(self.source)
        self.assertIsNotNone(runtime._voice_epoch(shard))
        replacement = self.source_path.with_name('replacement.db')
        shutil.copy2(self.source_path, replacement)
        replacement.replace(self.source_path)
        self.assertIsNone(runtime._voice_epoch(shard))

    def test_small_window_avoids_confirmation_sampling(self):
        statements = []
        self.catalog.shard_for_source(self.source).connection.set_trace_callback(statements.append)
        runtime = self._runtime()
        self.assertNotEqual(runtime.scan(TimeWindow.from_utc(START, END), display_limit=50).status, "confirmation_required")
        self.assertIsNone(runtime.output_preflight(TimeWindow.from_utc(START, END)))
        samples = [s for s in statements if s.lstrip().lower().startswith('select 1 from "msg_')]
        self.assertEqual(samples, [])

    def test_large_output_reuses_sample_but_changed_source_rechecks(self):
        statements = []
        self.catalog.shard_for_source(self.source).connection.set_trace_callback(statements.append)
        runtime = self._runtime()
        window = TimeWindow.from_utc(START, END + timedelta(days=2))
        self.assertEqual(runtime.scan(window, display_limit=50).status, "confirmation_required")
        first = runtime.output_preflight(window)
        samples = lambda: [s for s in statements if s.lstrip().lower().startswith('select 1 from "msg_')]
        self.assertEqual(len(samples()), 1)
        self._insert_source_rows([(5, 105, 1_700_000_060, 'synthetic changed')])
        second = runtime.output_preflight(window)
        self.assertEqual(len(samples()), 2)
        self.assertNotEqual(first.confirmation_token, second.confirmation_token)

    def test_preflight_samples_only_requested_window_not_entire_history(self) -> None:
        wider_end = START + timedelta(days=2)
        statements = []
        shard = self.catalog.shard_for_source(self.source)
        shard.connection.set_trace_callback(statements.append)
        preflight = self._runtime().preflight(TimeWindow.from_utc(START, wider_end))
        self.assertEqual(preflight.status, "awaiting_confirmation")
        selects = [
            statement.lower()
            for statement in statements
            if statement.lstrip().lower().startswith("select")
            and TABLE.lower() in statement.lower()
        ]
        self.assertTrue(selects)
        self.assertTrue(all("where" in statement and "limit" in statement for statement in selects))
        self.assertFalse(any("count(*)" in statement for statement in selects))

    def test_display_limit_reports_continuation_and_unavailable_display_name(self) -> None:
        evidence = SourceCapabilityEvidence(
            direction_proven=True,
            contact_mapping_proven=True,
            inbound_message=lambda _source, _message: True,
            inbound_event_ref=lambda _event_ref: True,
            display_name=lambda _conversation_id: None,
        )
        result = self._runtime(lambda _catalog: evidence).scan(
            TimeWindow.from_utc(START, END), display_limit=1
        )

        self.assertEqual(result.groups[0].total_messages, 2)
        self.assertEqual(len(result.groups[0].messages), 1)
        self.assertTrue(result.groups[0].has_more)
        self.assertEqual(result.groups[0].display_name, "会话 1")

    def test_runtime_passes_display_offset_to_result_contract(self) -> None:
        result = self._runtime().scan(TimeWindow.from_utc(START, END), display_limit=1, display_offset=1)
        self.assertEqual([message.text for message in result.groups[0].messages], ["fixture-two"])
        self.assertIsNone(result.groups[0].continuation_offset)

    def test_default_schema_check_degrades_without_withholding_message_rows(self) -> None:
        statements: list[str] = []
        self.catalog.shards[0].connection.set_trace_callback(statements.append)

        result = self._runtime(inspect_source_capabilities).scan(
            TimeWindow.from_utc(START, END), display_limit=50
        )

        self.assertIsNone(result.error_code)
        self.assertEqual(result.total_messages, 2)
        self.assertEqual(result.groups[0].display_name, "会话 1")
        self.assertTrue(all(message.direction == "unknown" for message in result.groups[0].messages))
        self.assertEqual(result.requested_range, (START, END))
        self.assertTrue(any("message_content" in sql.lower() and "select" in sql.lower() for sql in statements))
        self.assertEqual(self.event_store.count_events(), 2)

    def test_scan_does_not_create_provider_action_approval_or_execution_records(self) -> None:
        protected = (
            "agent_tasks",
            "agent_runs",
            "action_intents",
            "approvals",
            "action_outbox",
            "executions",
            "execution_results",
        )
        before = {
            table: self.agent_store._conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in protected
        }

        self._runtime().scan(TimeWindow.from_utc(START, END), display_limit=50)

        after = {
            table: self.agent_store._conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in protected
        }
        self.assertEqual(after, before)

    def test_cli_path_failure_is_bounded_and_does_not_print_the_path(self) -> None:
        missing = self.root / "sensitive-missing-event-store.db"
        output = StringIO()
        errors = StringIO()

        with redirect_stdout(output), redirect_stderr(errors):
            status = main(
                [
                    "--live-db-root", str(self.root / "live"),
                    "--message-root", str(self.message_root),
                    "--passphrase", str(self.root / "private" / "passphrase.bin"),
                    "--event-store", str(missing),
                    "--agent-store", str(self.agent_store.path),
                    "--account-id", "synthetic-account",
                    "--source-generation", "synthetic-generation",
                    "--request", "最近1小时",
                    "--timezone", "Asia/Shanghai",
                ]
            )

        combined = output.getvalue() + errors.getvalue()
        self.assertEqual(status, 2)
        self.assertIn("configuration_unavailable", combined)
        self.assertNotIn(str(missing), combined)

    def test_explicit_check_config_does_not_require_query_identity_labels(self) -> None:
        passphrase = self.root / "private" / "passphrase.bin"
        passphrase.parent.mkdir(parents=True, exist_ok=True)
        passphrase.write_bytes(b"p" * 32)
        passphrase.chmod(0o600)

        def open_source(path: Path, _passphrase: bytes):
            connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            connection.execute("PRAGMA query_only=ON")
            return connection, 32

        argv = [
            "--live-db-root", str(self.root / "live"),
            "--message-root", str(self.message_root),
            "--passphrase", str(passphrase),
            "--event-store", str(self.event_store.path),
            "--agent-store", str(self.agent_store.path),
            "--check-config", "--json",
        ]
        output = StringIO()
        with patch.object(runtime_module, "open_encrypted_readonly", side_effect=open_source):
            with patch.object(runtime_module, "read_private_passphrase", return_value=b"p" * 32):
                with redirect_stdout(output):
                    status = main(argv)
        self.assertEqual(status, 0)
        self.assertIn('"status": "ready"', output.getvalue())

        query_output = StringIO()
        query_argv = [
            "--live-db-root", str(self.root / "live"),
            "--message-root", str(self.message_root),
            "--passphrase", str(passphrase),
            "--event-store", str(self.event_store.path),
            "--agent-store", str(self.agent_store.path),
            "--request", "最近1小时", "--timezone", "UTC", "--json",
        ]
        with patch.object(runtime_module, "open_encrypted_readonly", side_effect=open_source):
            with patch.object(runtime_module, "read_private_passphrase", return_value=b"p" * 32):
                with redirect_stdout(query_output):
                    query_status = main(query_argv)
        self.assertEqual(query_status, 2)
        self.assertIn('"error_code": "configuration_unavailable"', query_output.getvalue())

    def test_scan_excludes_foreign_or_stale_agent_store_event_refs(self) -> None:
        runtime = self._runtime()
        first = runtime.scan(TimeWindow.from_utc(START, END), display_limit=50)
        template = self.agent_store._conn.execute(
            "SELECT * FROM event_search_projection LIMIT 1"
        ).fetchone()
        self.agent_store._conn.execute(
            """
            INSERT INTO event_search_projection (
                profile_id, materialization_id, event_id, subject_id,
                conversation_id, occurred_at, occurred_at_fallback,
                observed_at, event_type, content_kind, citation_text,
                searchable_text, quality_status, quality_codes_json,
                payload_sha256, indexed_at
            ) VALUES (?, ?, 'evt_foreign', 'msg_foreign', 'conv_foreign',
                      ?, 0, ?, 'message.created', 'text', 'foreign-body',
                      'foreign-body', 'normalized', '[]', ?, ?)
            """,
            (
                template["profile_id"],
                int(template["materialization_id"]) + 10_000,
                START.isoformat(timespec="microseconds"),
                START.isoformat(timespec="microseconds"),
                "f" * 64,
                START.isoformat(timespec="microseconds"),
            ),
        )
        self.agent_store._conn.commit()

        second = runtime.scan(TimeWindow.from_utc(START, END), display_limit=50)
        event_ids = {
            message.event_id for group in second.groups for message in group.messages
        }

        self.assertEqual(second.total_messages, first.total_messages)
        self.assertNotIn("evt_foreign", event_ids)

    def test_scan_binds_same_event_id_to_exact_canonical_materialization(self) -> None:
        runtime = self._runtime()
        first = runtime.scan(TimeWindow.from_utc(START, END), display_limit=50)
        canonical = next(
            message
            for group in first.groups
            for message in group.messages
            if message.text == "fixture-one"
        )
        projection = self.agent_store._conn.execute(
            "SELECT * FROM event_search_projection WHERE event_id=?",
            (canonical.event_id,),
        ).fetchone()
        forged_time = (END - timedelta(seconds=1)).isoformat(timespec="microseconds")
        forged_hash = "f" * 64
        self.agent_store._conn.execute(
            """
            INSERT INTO event_search_projection (
                profile_id, materialization_id, event_id, subject_id,
                conversation_id, occurred_at, occurred_at_fallback,
                observed_at, event_type, content_kind, citation_text,
                searchable_text, quality_status, quality_codes_json,
                payload_sha256, indexed_at
            ) VALUES (?, ?, ?, 'forged-subject', 'foreign-conversation',
                      ?, 0, ?, 'message.created', 'text', 'forged-body',
                      'forged-body', 'normalized', '[]', ?, ?)
            """,
            (
                projection["profile_id"],
                int(projection["materialization_id"]) + 10_000,
                canonical.event_id,
                forged_time,
                forged_time,
                forged_hash,
                forged_time,
            ),
        )
        self.agent_store._conn.commit()

        second = runtime.scan(TimeWindow.from_utc(START, END), display_limit=50)
        rebound = next(
            message
            for group in second.groups
            for message in group.messages
            if message.event_id == canonical.event_id
        )

        self.assertEqual(second.total_messages, first.total_messages)
        self.assertEqual(rebound.text, "fixture-one")
        for field in ("subject_id", "conversation_id", "occurred_at"):
            with self.subTest(field=field):
                self.assertEqual(
                    rebound.event_ref[field],
                    canonical.event_ref[field],
                )
        self.assertNotEqual(projection["payload_sha256"], forged_hash)

    def test_scan_rejects_mutated_metadata_on_exact_projection_row(self) -> None:
        runtime = self._runtime()
        first = runtime.scan(TimeWindow.from_utc(START, END), display_limit=50)
        canonical = next(
            message
            for group in first.groups
            for message in group.messages
            if message.text == "fixture-one"
        )
        forged_time = (END - timedelta(seconds=1)).isoformat(timespec="microseconds")
        updated = self.agent_store._conn.execute(
            """
            UPDATE event_search_projection
            SET subject_id='forged-subject',
                conversation_id='foreign-conversation',
                occurred_at=?
            WHERE event_id=?
            """,
            (forged_time, canonical.event_id),
        )
        self.assertEqual(updated.rowcount, 1)
        self.agent_store._conn.commit()

        result = runtime.scan(TimeWindow.from_utc(START, END), display_limit=50)
        event_ids = {
            message.event_id for group in result.groups for message in group.messages
        }

        self.assertNotIn(canonical.event_id, event_ids)
        self.assertIn("event_projection_unavailable", result.coverage.gaps)
        self.assertTrue(
            all(
                group.conversation_id != "foreign-conversation"
                for group in result.groups
            )
        )

    def test_scan_isolates_a_materialized_foreign_account_and_generation(self) -> None:
        foreign_source = SourceKey(
            "foreign-account",
            "foreign-generation",
            "message_9.db",
            TABLE,
        )
        self.event_store.record_catalog_refresh(
            foreign_source.account_id,
            foreign_source.source_generation,
            [(foreign_source.shard_name, foreign_source.table_name)],
        )
        self.event_store.commit_window_messages(
            foreign_source,
            [RawMessage(99, 999, 1, 1_700_000_025, 7, "foreign-secret", 0)],
            WindowIngestRun(
                START,
                END,
                {
                    "source_min_time": 1_700_000_000,
                    "source_max_time": 1_700_000_050,
                    "examined_count": 1,
                },
            ),
        )

        result = self._runtime().scan(
            TimeWindow.from_utc(START, END), display_limit=50
        )
        texts = [
            message.text for group in result.groups for message in group.messages
        ]

        self.assertEqual(result.total_messages, 2)
        self.assertNotIn("foreign-secret", texts)

    def test_scan_excludes_same_event_when_agent_projection_hash_is_stale(self) -> None:
        runtime = self._runtime()
        first = runtime.scan(TimeWindow.from_utc(START, END), display_limit=50)
        target = next(
            message
            for group in first.groups
            for message in group.messages
            if message.text == "fixture-one"
        )
        stale_hash = self.agent_store._conn.execute(
            "SELECT payload_sha256 FROM event_search_projection WHERE event_id=?",
            (target.event_id,),
        ).fetchone()[0]
        with sqlite3.connect(self.source_path) as source:
            source.execute(
                f"UPDATE {TABLE} SET message_content='fixture-one-revised' "
                "WHERE local_id=2"
            )

        revised = runtime.scan(TimeWindow.from_utc(START, END), display_limit=50)
        revised_target = next(
            message
            for group in revised.groups
            for message in group.messages
            if message.text == "fixture-one-revised"
        )
        self.agent_store._conn.execute(
            "UPDATE event_search_projection SET payload_sha256=? WHERE event_id=?",
            (stale_hash, revised_target.event_id),
        )
        self.agent_store._conn.commit()

        result = runtime.scan(TimeWindow.from_utc(START, END), display_limit=50)
        event_ids = {
            message.event_id for group in result.groups for message in group.messages
        }

        self.assertNotIn(revised_target.event_id, event_ids)
        self.assertIn("event_projection_unavailable", result.coverage.gaps)

    def test_scan_restores_full_canonical_text_instead_of_silent_citation_truncation(self) -> None:
        long_text = "x" * 600
        self._insert_source_rows(((5, 105, 1_700_000_075, long_text),))

        result = self._runtime().scan(
            TimeWindow.from_utc(START, END), display_limit=50
        )
        rendered = [
            message.text
            for group in result.groups
            for message in group.messages
            if message.text.startswith("x")
        ]

        self.assertEqual(rendered, [long_text])

    def test_display_offset_retrieves_every_page_without_overlap(self) -> None:
        self._insert_source_rows(
            tuple(
                (local_id, 1_000 + local_id, 1_700_000_075, f"bulk-{local_id}")
                for local_id in range(5, 110)
            )
        )
        runtime = self._runtime()

        first = runtime.scan(
            TimeWindow.from_utc(START, END), display_limit=50, display_offset=0
        )
        second = runtime.scan(
            TimeWindow.from_utc(START, END), display_limit=50, display_offset=50
        )
        third = runtime.scan(
            TimeWindow.from_utc(START, END), display_limit=50, display_offset=100
        )
        pages = [
            {message.event_id for group in page.groups for message in group.messages}
            for page in (first, second, third)
        ]

        self.assertEqual(first.total_messages, 107)
        self.assertTrue(first.groups[0].has_more)
        self.assertEqual(first.groups[0].continuation_offset, 50)
        self.assertTrue(pages[0].isdisjoint(pages[1]))
        self.assertTrue(pages[1].isdisjoint(pages[2]))
        self.assertEqual(len(set().union(*pages)), 107)

    def test_operator_hold_remains_cursor_free_and_unchanged(self) -> None:
        self.event_store.initialize_catalog_source(self.source, "operator-hold")
        before = dict(self.event_store.source_catalog_entry(self.source))

        self._runtime().scan(TimeWindow.from_utc(START, END), display_limit=50)

        after = dict(self.event_store.source_catalog_entry(self.source))
        self.assertIsNone(self.event_store.get_cursor(self.source))
        self.assertEqual(after["lifecycle"], "pending_initialization")
        self.assertEqual(after["initialization_policy"], "operator-hold")
        self.assertEqual(
            (after["lifecycle"], after["initialization_policy"]),
            (before["lifecycle"], before["initialization_policy"]),
        )

    def test_runtime_has_no_forbidden_calls_and_help_has_no_forbidden_options(self) -> None:
        allowed_stdlib = {
            "__future__",
            "argparse",
            "collections",
            "concurrent",
            "contextlib",
            "dataclasses",
            "datetime",
            "hashlib",
            "json",
            "os",
            "pathlib",
            "re",
            "secrets",
            "sqlite3",
            "sys",
            "typing",
            "stat",
            "threading",
            "zoneinfo",
        }
        for module_path in (
            Path(runtime_module.__file__),
            Path(runtime_module.__file__).with_name("codex_first.py"),
        ):
            tree = ast.parse(module_path.read_text())
            imports: set[str] = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imports.update(alias.name.split(".")[0] for alias in node.names)
                elif (
                    isinstance(node, ast.ImportFrom)
                    and node.level == 0
                    and node.module is not None
                ):
                    imports.add(node.module.split(".")[0])
            self.assertLessEqual(imports, allowed_stdlib)

        forbidden = AssertionError("forbidden surface called")
        with patch.object(socket, "socket", side_effect=forbidden), patch.object(
            subprocess, "run", side_effect=forbidden
        ), patch.object(urllib.request, "urlopen", side_effect=forbidden), patch.object(
            webbrowser, "open", side_effect=forbidden
        ):
            result = self._runtime().scan(
                TimeWindow.from_utc(START, END), display_limit=50
            )

        self.assertEqual(result.total_messages, 2)
        help_text = runtime_module.build_parser().format_help().lower()
        for forbidden_option in (
            "--send", "--provider", "--action", "--monitor", "--clipboard"
        ):
            self.assertNotIn(forbidden_option, help_text)

    def test_codex_first_console_entry_point_is_not_shipped_without_real_capability(self) -> None:
        pyproject = (Path(__file__).parents[1] / "pyproject.toml").read_text()
        self.assertNotIn("wechat-codex-first =", pyproject)


if __name__ == "__main__":
    unittest.main()
