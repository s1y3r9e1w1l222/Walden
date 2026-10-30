from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from sqlcipher3 import dbapi2 as sqlcipher
import zstandard

from wechat_local.agent_store import AgentStore
from wechat_local.event_store import EventStore, SourceKey
from wechat_local.live_catalog import LiveCatalog


GROUP = "synthetic-cli-group@chatroom"
TABLE = "Msg_" + hashlib.md5(GROUP.encode("utf-8")).hexdigest()
PUBLIC = "gh_synthetic_cli_account"
PUBLIC_TABLE = "Msg_" + hashlib.md5(PUBLIC.encode("utf-8")).hexdigest()
PASSPHRASE = b"s" * 32


def _field(number: int, value: bytes) -> bytes:
    return bytes([(number << 3) | 2, len(value)]) + value


def _native(value: str) -> bytes:
    return _field(5, _field(2, value.encode("utf-8")))


def _open_encrypted_writer(path: Path, key: str = "s" * 32):
    connection = sqlcipher.connect(path)
    connection.execute("PRAGMA key = '" + key + "'")
    connection.execute("PRAGMA cipher_compatibility = 4")
    return connection


class CodexFirstVoiceCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.live_root = self.root / "live"
        self.message_root = self.live_root / "message"
        self.voice_root = self.live_root / "voice"
        self.message_root.mkdir(parents=True)
        self.voice_root.mkdir()
        self.message_path = self.message_root / "message_0.db"
        with _open_encrypted_writer(self.message_path) as source:
            source.execute("CREATE TABLE Name2Id (user_name TEXT)")
            source.execute(
                "INSERT INTO Name2Id(rowid, user_name) VALUES (7, ?)", (GROUP,)
            )
            source.execute(
                f"""
                CREATE TABLE {TABLE} (
                    local_id INTEGER PRIMARY KEY, server_id INTEGER,
                    local_type INTEGER NOT NULL, create_time INTEGER,
                    real_sender_id INTEGER, message_content BLOB,
                    WCDB_CT_message_content INTEGER, packed_info_data BLOB
                )
                """
            )
            source.executemany(
                f"INSERT INTO {TABLE} VALUES (?, ?, 34, ?, 9, NULL, 0, ?)",
                (
                    (1, 101, 1_700_000_000, _native("native cli transcript")),
                    (2, 102, 1_700_000_010, _field(1, b"abcde")),
                ),
            )
        self.voice_path = self.voice_root / "voice.db"
        with _open_encrypted_writer(self.voice_path) as voice:
            voice.execute("CREATE TABLE Name2Id (user_name TEXT)")
            voice.execute(
                "INSERT INTO Name2Id(rowid, user_name) VALUES (7, ?)", (GROUP,)
            )
            voice.execute(
                """
                CREATE TABLE VoiceInfo (
                    chat_name_id INTEGER, create_time INTEGER,
                    local_id INTEGER, svr_id INTEGER,
                    voice_data BLOB, data_index BLOB
                )
                """
            )
            voice.execute(
                "INSERT INTO VoiceInfo VALUES (7, 1700000010, 2, 102, ?, NULL)",
                (b"\x02#!SILK_V3" + b"x" * 32,),
            )
        self.passphrase = self.root / "passphrase.bin"
        self.passphrase.write_bytes(PASSPHRASE)
        os.chmod(self.passphrase, 0o600)
        self.event_store_path = self.root / "events.db"
        self.agent_store_path = self.root / "agent.db"
        with EventStore(self.event_store_path) as store:
            catalog = LiveCatalog.dynamic_encrypted(
                self.message_root,
                PASSPHRASE,
                store=store,
                account_id="synthetic-account",
                source_generation="synthetic-generation",
            )
            try:
                store.initialize_catalog_source(
                    SourceKey(
                        "synthetic-account",
                        "synthetic-generation",
                        "message_0.db",
                        TABLE,
                    ),
                    "beginning",
                )
            finally:
                catalog.close()
        AgentStore(self.agent_store_path).close()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_public_account_rule_mapping_changes_only_source_filter(self):
        with _open_encrypted_writer(self.message_path) as source:
            source.execute("INSERT INTO Name2Id(rowid, user_name) VALUES (8, ?)", (PUBLIC,))
            source.execute(
                f"CREATE TABLE {PUBLIC_TABLE} ("
                "local_id INTEGER PRIMARY KEY, server_id INTEGER, local_type INTEGER NOT NULL, "
                "create_time INTEGER, real_sender_id INTEGER, message_content BLOB, "
                "WCDB_CT_message_content INTEGER)"
            )
            source.execute(
                f"INSERT INTO {PUBLIC_TABLE} VALUES (1, 801, 1, 1700000020, 8, ?, 0)",
                (b"synthetic public text",),
            )
        with EventStore(self.event_store_path) as store:
            catalog = LiveCatalog.dynamic_encrypted(
                self.message_root,
                PASSPHRASE,
                store=store,
                account_id="synthetic-account",
                source_generation="synthetic-generation",
            )
            try:
                catalog.refresh_catalog()
                store.initialize_catalog_source(
                    SourceKey(
                        "synthetic-account", "synthetic-generation", "message_0.db", PUBLIC_TABLE
                    ),
                    "beginning",
                )
            finally:
                catalog.close()
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
        base = [
            sys.executable, "-m", "wechat_local.codex_first_runtime",
            "--live-db-root", str(self.live_root),
            "--message-root", str(self.message_root),
            "--passphrase", str(self.passphrase),
            "--event-store", str(self.event_store_path),
            "--agent-store", str(self.agent_store_path),
            "--account-id", "synthetic-account",
            "--source-generation", "synthetic-generation",
            "--request", "从 2023-11-14 22:13 到 2023-11-14 22:15",
            "--timezone", "UTC", "--json",
        ]

        # Four current-policy situations map to the existing include switch.
        # This tests CLI/filter behavior, not a model's interpretation of Markdown.
        states = (
            ("default-excluded", False, 2),
            ("current-rule-includes", True, 3),
            ("explicit-request-includes", True, 3),
            ("rule-edited-back-to-exclude", False, 2),
        )
        for _name, include_public, expected_count in states:
            command = base + (["--include-public-accounts"] if include_public else [])
            completed = subprocess.run(
                command, cwd=Path(__file__).parents[1], env=environment,
                text=True, capture_output=True, timeout=30,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            payload = json.loads(completed.stdout)
            self.assertEqual(payload["total_count"], expected_count)
            public_groups = [
                group for group in payload["groups"]
                if group["conversation_kind"] == "公众号"
            ]
            self.assertEqual(bool(public_groups), include_public)

    def test_explicit_contact_database_labels_exact_conversation_only(self):
        import sqlite3

        with _open_encrypted_writer(self.message_path) as source:
            source.execute(
                "INSERT INTO Name2Id(rowid, user_name) VALUES (9, ?)",
                ("synthetic-cli-sender",),
            )
        contact_database = self.root / "contacts.db"
        with sqlite3.connect(contact_database) as contacts:
            contacts.execute(
                "CREATE TABLE contact (username TEXT, alias TEXT, remark TEXT, nick_name TEXT)"
            )
            contacts.execute(
                "INSERT INTO contact VALUES (?, ?, ?, ?)",
                (GROUP, "synthetic-alias", "Synthetic group label", "Synthetic nick"),
            )
            contacts.execute(
                "INSERT INTO contact VALUES (?, ?, ?, ?)",
                ("unrequested-contact", "private-alias", "Private name", "Private nick"),
            )
            contacts.execute(
                "INSERT INTO contact VALUES (?, ?, ?, ?)",
                ("synthetic-cli-sender", "sender-alias", "Synthetic sender label", "Sender nick"),
            )
        environment = os.environ.copy()
        installed_cli = os.environ.get("WECHAT_LOCAL_WINDOW")
        if installed_cli:
            environment.pop("PYTHONPATH", None)
        else:
            environment["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
        completed = subprocess.run(
            [
                *([installed_cli] if installed_cli else [sys.executable, "-m", "wechat_local.codex_first_runtime"]),
                "--live-db-root", str(self.live_root),
                "--message-root", str(self.message_root),
                "--passphrase", str(self.passphrase),
                "--event-store", str(self.event_store_path),
                "--agent-store", str(self.agent_store_path),
                "--account-id", "synthetic-account",
                "--source-generation", "synthetic-generation",
                "--request", "从 2023-11-14 22:13 到 2023-11-14 22:15",
                "--timezone", "UTC", "--conversation-username", GROUP,
                "--contact-database", str(contact_database), "--json",
            ],
            cwd=Path(__file__).parents[1], env=environment,
            text=True, capture_output=True, timeout=30,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["contact_name_source"], "configured")
        self.assertEqual(len(payload["groups"]), 1)
        self.assertEqual(payload["groups"][0]["display_name"], "Synthetic group label")
        self.assertEqual(payload["groups"][0]["conversation_id"], payload["groups"][0]["event_refs"][0]["conversation_id"])
        first_ref = payload["groups"][0]["event_refs"][0]
        self.assertEqual(first_ref["sender_identity"], "synthetic-cli-sender")
        self.assertEqual(first_ref["sender_display_name"], "Synthetic sender label")

    def test_encrypted_contact_and_bounded_source_mentions_are_separate_facts(self):
        source_xml = (
            b"<msg><atuserlist>other-user,synthetic-self,notify@all</atuserlist>"
            b"<at_user_list>other-user</at_user_list></msg>"
        )
        packed_source = zstandard.ZstdCompressor().compress(source_xml)
        with _open_encrypted_writer(self.message_path) as messages:
            messages.execute(
                "INSERT INTO Name2Id(rowid, user_name) VALUES (9, ?)",
                ("synthetic-cli-sender",),
            )
            messages.execute(f'ALTER TABLE "{TABLE}" ADD COLUMN source BLOB')
            messages.execute(f'ALTER TABLE "{TABLE}" ADD COLUMN WCDB_CT_source INTEGER')
            messages.execute(
                f'UPDATE "{TABLE}" SET source=?, WCDB_CT_source=4 WHERE local_id=1',
                (packed_source,),
            )
        encrypted_contact = self.live_root / "contact" / "contact.db"
        encrypted_contact.parent.mkdir(parents=True)
        with _open_encrypted_writer(encrypted_contact) as contacts:
            contacts.execute(
                "CREATE TABLE contact (username TEXT, alias TEXT, remark TEXT, nick_name TEXT, "
                "chat_room_notify INTEGER, verify_flag INTEGER, local_type INTEGER)"
            )
            contacts.execute(
                "INSERT INTO contact VALUES (?, ?, ?, ?, ?, ?, ?)",
                (GROUP, "synthetic alias", "Synthetic encrypted group", "Synthetic nick", 0, 8, 9),
            )

        environment = os.environ.copy()
        installed_cli = os.environ.get("WECHAT_LOCAL_WINDOW")
        if installed_cli:
            environment.pop("PYTHONPATH", None)
        else:
            environment["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
        completed = subprocess.run(
            [
                *([installed_cli] if installed_cli else [sys.executable, "-m", "wechat_local.codex_first_runtime"]),
                "--live-db-root", str(self.live_root),
                "--message-root", str(self.message_root),
                "--passphrase", str(self.passphrase),
                "--event-store", str(self.event_store_path),
                "--agent-store", str(self.agent_store_path),
                "--account-id", "synthetic-account",
                "--source-generation", "synthetic-generation",
                "--request", "从 2023-11-14 22:13 到 2023-11-14 22:15",
                "--timezone", "UTC", "--conversation-username", GROUP,
                "--self-username", "synthetic-self",
                "--encrypted-contact-database", str(encrypted_contact),
                "--json",
            ],
            cwd=Path(__file__).parents[1], env=environment,
            text=True, capture_output=True, timeout=30,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["contact_name_source"], "configured")
        self.assertEqual(payload["groups"][0]["display_name"], "Synthetic encrypted group")
        refs = payload["groups"][0]["event_refs"]
        self.assertEqual(len(refs), 2)
        first = next(
            ref for ref in refs
            if ref.get("source_facts", {}).get("mentions", {}).get("status") == "available"
        )
        self.assertEqual(first["sender_identity"], "synthetic-cli-sender")
        self.assertEqual(first["source_facts"]["mentions"], {
            "status": "available", "at_self": "yes", "at_all": "yes", "other_count": 1,
        })
        self.assertEqual(first["source_facts"]["contact"], {
            "source_status": "configured", "chat_room_notify_raw": 0,
            "verify_flag_raw": 8, "contact_local_type_raw": 9,
            "mute_status": "muted", "conversation_subtype": "unknown",
        })

    def test_wrong_key_encrypted_contact_keeps_messages_and_reports_unavailable(self):
        encrypted_contact = self.live_root / "contact" / "contact.db"
        encrypted_contact.parent.mkdir(parents=True)
        with _open_encrypted_writer(encrypted_contact, key="different synthetic key") as contacts:
            contacts.execute(
                "CREATE TABLE contact (username TEXT, alias TEXT, remark TEXT, nick_name TEXT)"
            )
            contacts.execute(
                "INSERT INTO contact VALUES (?, ?, ?, ?)",
                (GROUP, "should-not-appear", "should-not-appear", "should-not-appear"),
            )
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
        completed = subprocess.run(
            [
                sys.executable, "-m", "wechat_local.codex_first_runtime",
                "--live-db-root", str(self.live_root),
                "--message-root", str(self.message_root),
                "--passphrase", str(self.passphrase),
                "--event-store", str(self.event_store_path),
                "--agent-store", str(self.agent_store_path),
                "--account-id", "synthetic-account",
                "--source-generation", "synthetic-generation",
                "--request", "从 2023-11-14 22:13 到 2023-11-14 22:15",
                "--timezone", "UTC", "--conversation-username", GROUP,
                "--encrypted-contact-database", str(encrypted_contact), "--json",
            ],
            cwd=Path(__file__).parents[1], env=environment,
            text=True, capture_output=True, timeout=30,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["total_count"], 2)
        self.assertEqual(payload["contact_name_source"], "unavailable")
        refs = payload["groups"][0]["event_refs"]
        self.assertEqual(len(refs), 2)
        self.assertNotIn("should-not-appear", completed.stdout)
        self.assertTrue(all(ref["source_facts"]["contact"]["source_status"] == "unavailable" for ref in refs))

    def test_binary_optional_contact_fact_stays_unknown_and_keeps_all_messages(self):
        import sqlite3

        contact_database = self.root / "opaque-contact.db"
        with sqlite3.connect(contact_database) as contacts:
            contacts.execute(
                "CREATE TABLE contact (username TEXT, alias TEXT, remark TEXT, nick_name TEXT, "
                "chat_room_notify, verify_flag, local_type)"
            )
            contacts.execute(
                "INSERT INTO contact VALUES (?, ?, ?, ?, ?, ?, ?)",
                (GROUP, "synthetic alias", "Synthetic group", "Synthetic nick", bytes((255,)), bytes((255,)), bytes((255,))),
            )
        environment = os.environ.copy()
        installed_cli = os.environ.get("WECHAT_LOCAL_WINDOW")
        if installed_cli:
            environment.pop("PYTHONPATH", None)
        else:
            environment["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
        completed = subprocess.run(
            [
                *([installed_cli] if installed_cli else [sys.executable, "-m", "wechat_local.codex_first_runtime"]),
                "--live-db-root", str(self.live_root),
                "--message-root", str(self.message_root),
                "--passphrase", str(self.passphrase),
                "--event-store", str(self.event_store_path),
                "--agent-store", str(self.agent_store_path),
                "--account-id", "synthetic-account",
                "--source-generation", "synthetic-generation",
                "--request", "从 2023-11-14 22:13 到 2023-11-14 22:15",
                "--timezone", "UTC", "--conversation-username", GROUP,
                "--contact-database", str(contact_database), "--json",
            ],
            cwd=Path(__file__).parents[1], env=environment,
            text=True, capture_output=True, timeout=30,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["total_count"], 2)
        refs = payload["groups"][0]["event_refs"]
        self.assertEqual(len(refs), 2)
        for ref in refs:
            contact = ref["source_facts"]["contact"]
            self.assertEqual(contact["chat_room_notify_raw"], "unknown")
            self.assertEqual(contact["verify_flag_raw"], "unknown")
            self.assertEqual(contact["contact_local_type_raw"], "unknown")

    def test_module_entry_emits_text_before_bound_voice_results(self) -> None:
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "wechat_local.codex_first_runtime",
                "--live-db-root", str(self.live_root),
                "--message-root", str(self.message_root),
                "--passphrase", str(self.passphrase),
                "--event-store", str(self.event_store_path),
                "--agent-store", str(self.agent_store_path),
                "--account-id", "synthetic-account",
                "--source-generation", "synthetic-generation",
                "--request", "从 2023-11-14 22:13 到 2023-11-14 22:15",
                "--timezone", "UTC",
                "--voice-transcripts",
                "--voice-database", str(self.voice_path),
                "--voice-jsonl",
            ],
            cwd=Path(__file__).parents[1],
            env=environment,
            text=True,
            capture_output=True,
            check=False,
            timeout=30,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        records = [json.loads(line) for line in completed.stdout.splitlines()]
        self.assertEqual(records[0]["phase"], "messages")
        initial_refs = records[0]["result"]["groups"][0]["event_refs"]
        self.assertTrue(all("voice" not in ref for ref in initial_refs))
        voice_records = [record for record in records if record["phase"] == "voice"]
        self.assertEqual(len(voice_records), 2)
        self.assertEqual(
            {record["voice"]["status"] for record in voice_records},
            {"native_transcript", "asr_unavailable"},
        )
        self.assertTrue(all(record["event_id"] for record in voice_records))
        self.assertEqual(records[-1]["phase"], "complete")

    def test_voice_media_is_not_opened_without_opt_in_and_is_enriched_when_enabled(self):
        base = [
            "--live-db-root", str(self.live_root),
            "--message-root", str(self.message_root),
            "--passphrase", str(self.passphrase),
            "--event-store", str(self.event_store_path),
            "--agent-store", str(self.agent_store_path),
            "--account-id", "synthetic-account",
            "--source-generation", "synthetic-generation",
            "--request", "从 2023-11-14 22:13 到 2023-11-14 22:15",
            "--timezone", "UTC",
        ]
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")

        plain = subprocess.run(
            [sys.executable, "-m", "wechat_local.codex_first_runtime", *base, "--json"],
            cwd=Path(__file__).parents[1], env=environment,
            text=True, capture_output=True, timeout=30,
        )
        self.assertEqual(plain.returncode, 0, plain.stderr)
        plain_payload = json.loads(plain.stdout)
        self.assertEqual(plain_payload["total_count"], 2)
        self.assertTrue(
            all("voice" not in ref for group in plain_payload["groups"] for ref in group["event_refs"])
        )

        enriched = subprocess.run(
            [
                sys.executable, "-m", "wechat_local.codex_first_runtime",
                *base, "--voice-transcripts", "--voice-database", str(self.voice_path),
                "--voice-jsonl",
            ],
            cwd=Path(__file__).parents[1], env=environment,
            text=True, capture_output=True, timeout=30,
        )
        self.assertEqual(enriched.returncode, 0, enriched.stderr)
        records = [json.loads(line) for line in enriched.stdout.splitlines()]
        self.assertEqual(records[0]["phase"], "messages")
        self.assertEqual(len([item for item in records if item["phase"] == "voice"]), 2)

    def test_large_window_continuation_and_conversation_bound_are_public(self):
        environment = os.environ.copy()
        installed_command = environment.get("WECHAT_LOCAL_WINDOW")
        if installed_command:
            environment.pop("PYTHONPATH", None)
        else:
            environment["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
        command = ([installed_command] if installed_command else [
            sys.executable, "-m", "wechat_local.codex_first_runtime",
        ]) + [
            "--live-db-root", str(self.live_root),
            "--message-root", str(self.message_root),
            "--passphrase", str(self.passphrase),
            "--event-store", str(self.event_store_path),
            "--agent-store", str(self.agent_store_path),
            "--account-id", "synthetic-account",
            "--source-generation", "synthetic-generation",
            "--request", "从 2023-11-14 22:13:20 到 2023-11-16 22:13:20",
            "--timezone", "UTC", "--conversation-username", GROUP, "--json",
        ]
        image_key_config = self.root / "image-keys.json"
        image_key_config.write_text(json.dumps({"account_scope": "synthetic", "key_codes": [1, 2]}))
        os.chmod(image_key_config, 0o600)
        image_database = self.live_root / "image.db"
        image_database.write_bytes(b"synthetic-not-a-database")
        command += [
            "--image-database", str(image_database),
            "--image-attachments-root", str(self.root / "attachments"),
            "--image-output-root", str(self.root / "decoded"),
            "--image-key-config", str(image_key_config),
        ]
        first = subprocess.run(command, cwd=Path(__file__).parents[1], env=environment, text=True, capture_output=True, timeout=30)
        self.assertEqual(first.returncode, 0, first.stderr)
        first_payload = json.loads(first.stdout)
        confirmation = first_payload["confirmation"]
        self.assertEqual(first_payload["status"], "confirmation_required")
        self.assertEqual(confirmation["estimated_rows"], 2)
        self.assertEqual(confirmation["request"], "从 2023-11-14 22:13:20 到 2023-11-16 22:13:20")
        self.assertEqual(first_payload["total_count"], 0)

        resumed = subprocess.run(
            command + ["--confirmation-token", confirmation["token"]],
            cwd=Path(__file__).parents[1], env=environment, text=True, capture_output=True, timeout=30,
        )
        self.assertEqual(resumed.returncode, 0, resumed.stderr)
        payload = json.loads(resumed.stdout)
        self.assertEqual(payload["total_count"], 2)
        self.assertEqual(payload["groups"][0]["conversation_kind"], "群聊")

    def test_relative_large_window_confirmation_replays_exact_bounds_and_rejects_changes(self):
        environment = os.environ.copy()
        installed_command = environment.get("WECHAT_LOCAL_WINDOW")
        if installed_command:
            environment.pop("PYTHONPATH", None)
        else:
            environment["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
        command = ([installed_command] if installed_command else [
            sys.executable, "-m", "wechat_local.codex_first_runtime",
        ]) + [
            "--live-db-root", str(self.live_root),
            "--message-root", str(self.message_root),
            "--passphrase", str(self.passphrase),
            "--event-store", str(self.event_store_path),
            "--agent-store", str(self.agent_store_path),
            "--account-id", "synthetic-account",
            "--source-generation", "synthetic-generation",
            "--request", "最近3天",
            "--timezone", "UTC", "--conversation-username", GROUP, "--json",
        ]

        def run(argv):
            completed = subprocess.run(
                argv, cwd=Path(__file__).parents[1], env=environment,
                text=True, capture_output=True, timeout=30,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            return json.loads(completed.stdout)

        first = run(command)
        confirmation = first["confirmation"]
        self.assertEqual(first["status"], "confirmation_required")
        self.assertRegex(confirmation["request"], r"\.\d{6} 到 .+\.\d{6}$")

        replay_command = command.copy()
        replay_command[replay_command.index("最近3天")] = confirmation["request"]
        exact_replay = run(replay_command + ["--confirmation-token", confirmation["token"]])
        self.assertNotEqual(exact_replay["status"], "confirmation_required")

        start_text, end_text = confirmation["request"].removeprefix("从 ").split(" 到 ", 1)
        changed_start = datetime.fromisoformat(start_text).replace(microsecond=(datetime.fromisoformat(start_text).microsecond + 1) % 1_000_000)
        changed_request = f"从 {changed_start.strftime('%Y-%m-%d %H:%M:%S.%f')} 到 {end_text}"
        changed_bounds_command = replay_command.copy()
        changed_bounds_command[changed_bounds_command.index(confirmation["request"])] = changed_request
        changed_bounds = run(changed_bounds_command + ["--confirmation-token", confirmation["token"]])
        self.assertEqual(changed_bounds["status"], "confirmation_required")

        # Change the bounded estimate while keeping the exact fixed request.
        start = datetime.fromisoformat(start_text)
        end = datetime.fromisoformat(end_text)
        inserted_time = int((start + (end - start) / 2).timestamp())
        with _open_encrypted_writer(self.message_path) as source:
            source.execute(
                f"INSERT INTO {TABLE} VALUES (?, ?, 34, ?, 9, NULL, 0, ?)",
                (3, 103, inserted_time, _native("new synthetic row")),
            )
        changed_estimate = run(replay_command + ["--confirmation-token", confirmation["token"]])
        self.assertEqual(changed_estimate["status"], "confirmation_required")

    def test_cli_subprocess_assembles_fake_only_at_local_model_boundary(self) -> None:
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
        boundary = (
            "import sys; "
            "import wechat_local.codex_first_runtime as runtime; "
            "from wechat_local.voice_transcript import LocalTranscription; "
            "Fake=type('Fake',(),{"
            "'__init__':lambda self,*args,**kwargs:None,"
            "'transcribe':lambda self,payload:LocalTranscription("
            "'synthetic boundary transcript',None,'fake-whisper-test','f'*64)}); "
            "runtime.LocalWhisperTranscriber=Fake; "
            "raise SystemExit(runtime.main(sys.argv[1:]))"
        )
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                boundary,
                "--live-db-root", str(self.live_root),
                "--message-root", str(self.message_root),
                "--passphrase", str(self.passphrase),
                "--event-store", str(self.event_store_path),
                "--agent-store", str(self.agent_store_path),
                "--account-id", "synthetic-account",
                "--source-generation", "synthetic-generation",
                "--request", "从 2023-11-14 22:13 到 2023-11-14 22:15",
                "--timezone", "UTC",
                "--voice-transcripts",
                "--voice-database", str(self.voice_path),
                "--voice-runtime-root", str(self.root),
                "--voice-python", sys.executable,
                "--voice-jsonl",
            ],
            cwd=Path(__file__).parents[1],
            env=environment,
            text=True,
            capture_output=True,
            check=False,
            timeout=30,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        records = [json.loads(line) for line in completed.stdout.splitlines()]
        self.assertEqual(records[0]["phase"], "messages")
        local = next(
            record["voice"]
            for record in records
            if record.get("phase") == "voice"
            and record["voice"]["status"] == "local_transcript"
        )
        self.assertEqual(local["transcript"]["source"], "local_whisper_cpp")
        self.assertEqual(local["transcript"]["model_sha256"], "f" * 64)
        self.assertEqual(
            local["audio"]["audio_payload_sha256"],
            hashlib.sha256(b"\x02#!SILK_V3" + b"x" * 32).hexdigest(),
        )

    def test_local_asr_rejects_single_json_mode(self) -> None:
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "wechat_local.codex_first_runtime",
                "--live-db-root", str(self.live_root),
                "--message-root", str(self.message_root),
                "--passphrase", str(self.passphrase),
                "--event-store", str(self.event_store_path),
                "--agent-store", str(self.agent_store_path),
                "--account-id", "synthetic-account",
                "--source-generation", "synthetic-generation",
                "--request", "最近1小时",
                "--timezone", "UTC",
                "--voice-transcripts",
                "--voice-runtime-root", str(self.root),
                "--voice-python", sys.executable,
                "--json",
            ],
            cwd=Path(__file__).parents[1],
            env=environment,
            text=True,
            capture_output=True,
            check=False,
            timeout=30,
        )

        self.assertEqual(completed.returncode, 2)
        self.assertEqual(
            json.loads(completed.stdout),
            {"error_code": "configuration_unavailable", "status": "unavailable"},
        )

    def test_missing_local_runtime_keeps_initial_messages_and_fails_voice_only(self) -> None:
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
        completed = subprocess.run(
            [
                sys.executable, "-m", "wechat_local.codex_first_runtime",
                "--live-db-root", str(self.live_root),
                "--message-root", str(self.message_root),
                "--passphrase", str(self.passphrase),
                "--event-store", str(self.event_store_path),
                "--agent-store", str(self.agent_store_path),
                "--account-id", "synthetic-account",
                "--source-generation", "synthetic-generation",
                "--request", "从 2023-11-14 22:13 到 2023-11-14 22:15",
                "--timezone", "UTC",
                "--voice-transcripts",
                "--voice-database", str(self.voice_path),
                "--voice-runtime-root", str(self.root / "missing-runtime"),
                "--voice-python", sys.executable,
                "--voice-jsonl",
            ],
            cwd=Path(__file__).parents[1],
            env=environment,
            text=True,
            capture_output=True,
            check=False,
            timeout=30,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        records = [json.loads(line) for line in completed.stdout.splitlines()]
        self.assertEqual(records[0]["phase"], "messages")
        statuses = {
            record["voice"]["status"]
            for record in records
            if record.get("phase") == "voice"
        }
        self.assertEqual(
            statuses, {"native_transcript", "local_transcription_unavailable"}
        )

    def test_wrong_key_voice_database_is_media_unavailable_not_scan_failure(self) -> None:
        wrong_voice = self.voice_root / "wrong-key.db"
        with _open_encrypted_writer(wrong_voice, "t" * 32) as voice:
            voice.execute("CREATE TABLE Name2Id (user_name TEXT)")
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
        completed = subprocess.run(
            [
                sys.executable, "-m", "wechat_local.codex_first_runtime",
                "--live-db-root", str(self.live_root),
                "--message-root", str(self.message_root),
                "--passphrase", str(self.passphrase),
                "--event-store", str(self.event_store_path),
                "--agent-store", str(self.agent_store_path),
                "--account-id", "synthetic-account",
                "--source-generation", "synthetic-generation",
                "--request", "从 2023-11-14 22:13 到 2023-11-14 22:15",
                "--timezone", "UTC",
                "--voice-transcripts",
                "--voice-database", str(wrong_voice),
                "--voice-jsonl",
            ],
            cwd=Path(__file__).parents[1],
            env=environment,
            text=True,
            capture_output=True,
            check=False,
            timeout=30,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        records = [json.loads(line) for line in completed.stdout.splitlines()]
        self.assertEqual(records[0]["phase"], "messages")
        statuses = {
            record["voice"]["status"]
            for record in records
            if record.get("phase") == "voice"
        }
        self.assertEqual(statuses, {"native_transcript", "voice_source_unavailable"})


    def test_missing_voice_database_keeps_base_messages_and_reports_voice_failure(self) -> None:
        missing_voice = self.voice_root / "missing.db"
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
        completed = subprocess.run(
            [
                sys.executable, "-m", "wechat_local.codex_first_runtime",
                "--live-db-root", str(self.live_root),
                "--message-root", str(self.message_root),
                "--passphrase", str(self.passphrase),
                "--event-store", str(self.event_store_path),
                "--agent-store", str(self.agent_store_path),
                "--account-id", "synthetic-account",
                "--source-generation", "synthetic-generation",
                "--request", "从 2023-11-14 22:13 到 2023-11-14 22:15",
                "--timezone", "UTC",
                "--voice-transcripts",
                "--voice-database", str(missing_voice),
                "--voice-jsonl",
            ],
            cwd=Path(__file__).parents[1],
            env=environment,
            text=True,
            capture_output=True,
            check=False,
            timeout=30,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        records = [json.loads(line) for line in completed.stdout.splitlines()]
        self.assertEqual(records[0]["phase"], "messages")
        self.assertEqual(records[0]["result"]["total_count"], 2)
        voice_records = [record for record in records if record.get("phase") == "voice"]
        self.assertEqual(len(voice_records), 2)
        self.assertEqual(
            {record["voice"]["status"] for record in voice_records},
            {"native_transcript", "voice_source_unavailable"},
        )

    def test_good_and_wrong_key_shards_fail_closed_before_local_asr(self) -> None:
        wrong_voice = self.voice_root / "wrong-key-mixed.db"
        with _open_encrypted_writer(wrong_voice, "t" * 32) as voice:
            voice.execute("CREATE TABLE Name2Id (user_name TEXT)")
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
        marker = self.root / "asr-called.marker"
        environment["ASR_TEST_MARKER"] = str(marker)
        boundary = (
            "import sys,os; "
            "import wechat_local.codex_first_runtime as runtime; "
            "from wechat_local.voice_transcript import LocalTranscription; "
            "def_transcribe=lambda self,payload:(open(os.environ['ASR_TEST_MARKER'],'w').close() or "
            "LocalTranscription('should not be called',None,'fake-whisper-test','f'*64)); "
            "Fake=type('Fake',(),{"
            "'__init__':lambda self,*args,**kwargs:None,"
            "'transcribe':def_transcribe}); "
            "runtime.LocalWhisperTranscriber=Fake; "
            "raise SystemExit(runtime.main(sys.argv[1:]))"
        )
        completed = subprocess.run(
            [
                sys.executable, "-c", boundary,
                "--live-db-root", str(self.live_root),
                "--message-root", str(self.message_root),
                "--passphrase", str(self.passphrase),
                "--event-store", str(self.event_store_path),
                "--agent-store", str(self.agent_store_path),
                "--account-id", "synthetic-account",
                "--source-generation", "synthetic-generation",
                "--request", "从 2023-11-14 22:13 到 2023-11-14 22:15",
                "--timezone", "UTC",
                "--voice-transcripts",
                "--voice-database", str(self.voice_path),
                "--voice-database", str(wrong_voice),
                "--voice-runtime-root", str(self.root),
                "--voice-python", sys.executable,
                "--voice-jsonl",
            ],
            cwd=Path(__file__).parents[1],
            env=environment,
            text=True,
            capture_output=True,
            check=False,
            timeout=30,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        records = [json.loads(line) for line in completed.stdout.splitlines()]
        self.assertEqual(records[0]["phase"], "messages")
        voice_records = [record for record in records if record.get("phase") == "voice"]
        self.assertEqual(
            {record["voice"]["status"] for record in voice_records},
            {"native_transcript", "voice_source_unavailable"},
        )
        self.assertFalse(marker.exists(), "ASR must not run when any explicit shard is unavailable")


if __name__ == "__main__":
    unittest.main()
