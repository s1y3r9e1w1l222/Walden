from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
import argparse
import copy
import contextlib
import io
import sys
from pathlib import Path
from unittest.mock import patch

from wechat_local import source_setup


class SourceSetupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.documents = self.base / "Documents"
        self.account = self.documents / "xwechat_files" / "account_test_123"
        self.db_storage = self.account / "db_storage"
        (self.db_storage / "message").mkdir(parents=True)
        (self.db_storage / "contact").mkdir()
        (self.account / "msg" / "attach").mkdir(parents=True)
        self._sqlite(self.db_storage / "message" / "message_0.db", "CREATE TABLE Message(x)")
        self._sqlite(self.db_storage / "contact" / "contact.db", "CREATE TABLE contact(username, alias, remark, nick_name)")
        self._sqlite(self.db_storage / "message" / "message_resource.db", "CREATE TABLE MessageResourceInfo(x)")
        self.voice_db = self.db_storage / "cache" / "unknown-name.db"
        self._sqlite(
            self.voice_db,
            "CREATE TABLE VoiceInfo(chat_name_id, create_time, local_id, svr_id, voice_data)",
        )
        with sqlite3.connect(self.voice_db) as connection:
            connection.execute("CREATE TABLE Name2Id(user_name)")
        self.key = self.base / "synthetic-key"
        self.key.write_bytes(b"k" * 32)
        self.key.chmod(0o600)
        self.config = self.base / "config" / "wechat-local" / "current-source.json"
        self.data = self.base / "data"
        self.cli_bin = self.base / "installed-venv" / "bin"
        self.cli_bin.mkdir(parents=True)
        for name in ("wechat-local-setup", "wechat-local-window", "wechat-local-reply"):
            command = self.cli_bin / name
            command.write_text("#!/bin/sh\nexit 0\n")
            command.chmod(0o700)
        for relative in ("net/kvcomm", "ilink/kvcomm"):
            directory = self.documents / "app_data" / relative
            directory.mkdir(parents=True)
        (self.documents / "app_data" / "net" / "kvcomm" / "key_17_main.statistic").touch()
        (self.documents / "app_data" / "ilink" / "kvcomm" / "key_29_aux.statistic").touch()
        self.tools = {
            "runtime_python": Path("/venv/bin/python"),
            "window_cli": Path("/venv/bin/wechat-local-window"),
            "reply_cli": Path("/venv/bin/wechat-local-reply"),
        }

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def _sqlite(path: Path, ddl: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(path) as connection:
            connection.execute(ddl)

    @staticmethod
    def _open(path: Path, _key: bytes):
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        connection.execute("PRAGMA query_only=ON")
        return connection, 32

    def _run(self, key_path: Path | None = None):
        return source_setup.prepare_account_source(
            self.account,
            passphrase_file=key_path,
            config_path=self.config,
            data_home=self.data,
            **self.tools,
        )

    def test_complete_root_discovers_only_verified_resources_and_writes_private_profile(self) -> None:
        with patch.object(source_setup, "open_encrypted_readonly", side_effect=self._open):
            result = self._run(self.key)
        self.assertEqual(result["status"], "configured")
        profile = json.loads(self.config.read_text())
        self.assertEqual(profile["message_root"], str((self.db_storage / "message").resolve()))
        self.assertEqual(profile["encrypted_contact_database"], str((self.db_storage / "contact" / "contact.db").resolve()))
        self.assertEqual(profile["image_database"], str((self.db_storage / "message" / "message_resource.db").resolve()))
        self.assertEqual(profile["image_attachments_root"], str((self.account / "msg" / "attach").resolve()))
        self.assertEqual(profile["image_credentials_status"], "ready")
        image_key_config = Path(profile["image_key_config"])
        self.assertEqual(image_key_config.stat().st_mode & 0o777, 0o600)
        self.assertEqual(json.loads(image_key_config.read_text()), {
            "account_scope": "account_test",
            "key_codes": [17, 29],
        })
        self.assertEqual(profile["voice_databases"], [str(self.voice_db.resolve())])
        self.assertTrue(profile["voice_discovery_complete"])
        self.assertEqual(profile["account_id"].split("-")[0], "local")
        self.assertEqual(profile["source_generation"].split("-")[0], "generation")
        self.assertEqual(self.config.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.config.parent.stat().st_mode & 0o777, 0o700)
        self.assertTrue(self.data.is_relative_to(self.base))
        self.assertFalse(self.data.is_relative_to(self.account))

    def test_setup_opens_each_unchanged_database_once_for_source_and_voice(self):
        from collections import Counter
        second = self.db_storage / 'message/message_1.db'
        self._sqlite(second, 'CREATE TABLE Message(x)')
        message = self.db_storage / 'message/message_0.db'
        with sqlite3.connect(message) as db:
            db.execute('CREATE TABLE VoiceInfo(chat_name_id, create_time, local_id, svr_id, voice_data)')
            db.execute('CREATE TABLE Name2Id(user_name)')
        opens = []
        def counted(path, key):
            opens.append(Path(path).resolve()); return self._open(path, key)
        with patch.object(source_setup, 'open_encrypted_readonly', side_effect=counted):
            result = self._run(self.key)
        self.assertEqual(result['status'], 'configured', result)
        profile = json.loads(self.config.read_text())
        self.assertIn(str(message.resolve()), profile['voice_databases'])
        self.assertIn(str(self.voice_db.resolve()), profile['voice_databases'])
        counts = Counter(opens)
        self.assertEqual(counts[message.resolve()], 1)
        self.assertEqual(counts[second.resolve()], 1)
        self.assertEqual(counts[self.db_storage.joinpath('contact/contact.db').resolve()], 1)
        self.assertEqual(counts[self.db_storage.joinpath('message/message_resource.db').resolve()], 1)

    def test_wal_change_before_voice_scan_reopens_and_uses_current_schema(self):
        message = self.db_storage / 'message/message_0.db'
        writer = sqlite3.connect(message)
        writer.execute('PRAGMA journal_mode=WAL')
        original_scan = source_setup._scan_database_files
        opens = []
        def counted(path, key):
            opens.append(Path(path).resolve()); return self._open(path, key)
        def change(root):
            writer.execute('CREATE TABLE VoiceInfo(chat_name_id, create_time, local_id, svr_id, voice_data)')
            writer.execute('CREATE TABLE Name2Id(user_name)')
            writer.commit()
            self.assertTrue(Path(str(message)+'-wal').exists())
            return original_scan(root)
        try:
            with patch.object(source_setup, 'open_encrypted_readonly', side_effect=counted), \
                 patch.object(source_setup, '_scan_database_files', side_effect=change):
                result = self._run(self.key)
            self.assertEqual(result['status'], 'configured', result)
            profile = json.loads(self.config.read_text())
            self.assertIn(str(message.resolve()), profile['voice_databases'])
            self.assertEqual(opens.count(message.resolve()), 2)
        finally:
            writer.close()

    def test_new_message_shard_before_voice_scan_is_not_ignored(self):
        original_scan = source_setup._scan_database_files
        def add(root):
            self._sqlite(self.db_storage/'message/message_1.db', 'CREATE TABLE Message(x)')
            return original_scan(root)
        with patch.object(source_setup, 'open_encrypted_readonly', side_effect=self._open), \
             patch.object(source_setup, '_scan_database_files', side_effect=add):
            result = self._run(self.key)
        self.assertEqual(result['status'], 'message_source_changed', result)
        self.assertFalse(self.config.exists())

    def test_replaced_message_database_is_reopened_for_current_schema(self):
        message = self.db_storage/'message/message_0.db'
        old = message.with_name('previous.db')
        original_scan = source_setup._scan_database_files
        opens = []
        def counted(path, key):
            opens.append(Path(path).resolve()); return self._open(path, key)
        def replace(root):
            message.rename(old)
            self._sqlite(message, 'CREATE TABLE VoiceInfo(chat_name_id, create_time, local_id, svr_id, voice_data)')
            with sqlite3.connect(message) as db: db.execute('CREATE TABLE Name2Id(user_name)')
            old.unlink()
            return original_scan(root)
        with patch.object(source_setup, 'open_encrypted_readonly', side_effect=counted), \
             patch.object(source_setup, '_scan_database_files', side_effect=replace):
            result = self._run(self.key)
        self.assertEqual(result['status'], 'configured', result)
        self.assertIn(str(message.resolve()), json.loads(self.config.read_text())['voice_databases'])
        self.assertEqual(opens.count(message.resolve()), 2)

    def test_message_symlink_replacement_stops_before_config(self):
        message = self.db_storage/'message/message_0.db'
        outside = self.base/'outside.db'
        self._sqlite(outside, 'CREATE TABLE Message(x)')
        original_scan = source_setup._scan_database_files
        def replace(root):
            message.unlink(); message.symlink_to(outside)
            return original_scan(root)
        with patch.object(source_setup, 'open_encrypted_readonly', side_effect=self._open), \
             patch.object(source_setup, '_scan_database_files', side_effect=replace):
            result = self._run(self.key)
        self.assertEqual(result['status'], 'message_source_changed', result)
        self.assertFalse(self.config.exists())
        self.assertTrue(message.is_symlink())

    def test_metadata_connection_closes_when_query_only_check_raises(self):
        class Broken:
            closed = False
            def execute(self, _query): raise RuntimeError('synthetic failure')
            def close(self): self.closed = True
        connection = Broken()
        with patch.object(source_setup, 'open_encrypted_readonly', return_value=(connection, 32)):
            with self.assertRaises(RuntimeError):
                source_setup._metadata_connection(self.voice_db, b'k'*32)
        self.assertTrue(connection.closed)

    def test_voice_schema_error_keeps_incomplete_warning_and_closes(self):
        message = self.db_storage/'message/message_0.db'
        original_columns = source_setup._table_columns
        def columns(connection, table):
            if table == 'VoiceInfo': raise RuntimeError('synthetic schema unavailable')
            return original_columns(connection, table)
        with patch.object(source_setup, 'open_encrypted_readonly', side_effect=self._open), \
             patch.object(source_setup, '_table_columns', side_effect=columns):
            result = self._run(self.key)
        self.assertEqual(result['status'], 'configured_with_voice_incomplete', result)
        profile = json.loads(self.config.read_text())
        self.assertEqual(profile['voice_databases'], [])
        self.assertFalse(profile['voice_discovery_complete'])
        self.assertIn('voice_scan_incomplete', profile['setup_warnings'])

    def test_profile_preserves_selected_venv_python_symlink(self) -> None:
        base_python = self.base / "base-python"
        base_python.write_text("synthetic base executable")
        selected_python = self.base / "venv" / "bin" / "python"
        selected_python.parent.mkdir(parents=True)
        selected_python.symlink_to(base_python)
        tools = {**self.tools, "runtime_python": selected_python}
        with patch.object(source_setup, "open_encrypted_readonly", side_effect=self._open):
            result = source_setup.prepare_account_source(
                self.account,
                passphrase_file=self.key,
                config_path=self.config,
                data_home=self.data,
                **tools,
            )
        self.assertEqual(result["status"], "configured")
        profile = json.loads(self.config.read_text())
        self.assertEqual(profile["runtime_python"], str(selected_python.absolute()))
        self.assertNotEqual(profile["runtime_python"], str(selected_python.resolve()))

    def test_missing_key_reports_credentials_required_without_open_or_config(self) -> None:
        with patch.object(source_setup, "open_encrypted_readonly") as opener:
            result = self._run()
        self.assertEqual(result["status"], "credentials_required")
        self.assertFalse(self.config.exists())
        opener.assert_not_called()

    def test_cli_does_not_report_missing_credentials_as_success(self) -> None:
        output = io.StringIO()
        with patch.object(sys, "argv", [str(self.cli_bin / "wechat-local-setup"), "--account-root", str(self.account)]):
            with patch.object(source_setup, "prepare_account_source", return_value={"status": "credentials_required"}):
                with contextlib.redirect_stdout(output):
                    result = source_setup.main()
        self.assertEqual(result, 2)
        self.assertIn('"status": "credentials_required"', output.getvalue())

    def test_cli_refuses_to_borrow_window_and_reply_entrypoints_from_path(self) -> None:
        external_bin = self.base / "external-bin"
        external_bin.mkdir()
        for name in ("wechat-local-window", "wechat-local-reply"):
            command = external_bin / name
            command.write_text("#!/bin/sh\nexit 0\n")
            command.chmod(0o700)
        installed_bin = self.base / "package-venv" / "bin"
        output = io.StringIO()
        with patch.object(
            sys,
            "argv",
            [str(installed_bin / "wechat-local-setup"), "--account-root", str(self.account)],
        ), patch.dict(os.environ, {"PATH": str(external_bin)}), patch.object(
            source_setup, "prepare_account_source", return_value={"status": "credentials_required"}
        ) as prepare, contextlib.redirect_stdout(output):
            result = source_setup.main()
        self.assertEqual(result, 2)
        self.assertEqual(json.loads(output.getvalue()), {
            "status": "entrypoint_missing",
            "missing": ["wechat-local-window", "wechat-local-reply"],
        })
        prepare.assert_not_called()

    def test_missing_message_directory_returns_json_without_writing_config(self) -> None:
        account = self.base / "copied-account"
        (account / "db_storage").mkdir(parents=True)
        config = self.base / "missing-config" / "current-source.json"
        output = io.StringIO()
        errors = io.StringIO()
        with patch.object(
            sys, "argv",
            [str(self.cli_bin / "wechat-local-setup"), "--account-root", str(account), "--config", str(config)],
        ):
            with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
                result = source_setup.main()
        self.assertEqual(result, 2)
        self.assertEqual(json.loads(output.getvalue())["status"], "message_shards_missing")
        self.assertEqual(errors.getvalue(), "")
        self.assertFalse(config.exists())

    def test_unreadable_database_prevents_ready_profile_but_preserves_explicit_incomplete_voice_status(self) -> None:
        broken = self.db_storage / "misc" / "opaque.db"
        broken.parent.mkdir()
        broken.write_bytes(b"not a database")

        def open_with_one_failure(path: Path, key: bytes):
            if path == broken:
                raise OSError("synthetic unreadable shard")
            return self._open(path, key)

        with patch.object(source_setup, "open_encrypted_readonly", side_effect=open_with_one_failure):
            result = self._run(self.key)
        self.assertEqual(result["status"], "configured_with_voice_incomplete")
        profile = json.loads(self.config.read_text())
        self.assertFalse(profile["voice_discovery_complete"])
        self.assertIn("voice_scan_incomplete", profile["setup_warnings"])
        with self.assertRaises(source_setup.SourceSetupError):
            source_setup.require_complete_voice_source(profile)

    def test_actual_permission_denied_voice_directory_marks_discovery_incomplete(self) -> None:
        blocked = self.db_storage / "unreadable"
        blocked.mkdir()
        self._sqlite(
            blocked / "second.db",
            "CREATE TABLE VoiceInfo(chat_name_id, create_time, local_id, svr_id, voice_data)",
        )
        blocked.chmod(0)
        try:
            with self.assertRaises(PermissionError):
                next(blocked.iterdir(), None)
            with patch.object(source_setup, "open_encrypted_readonly", side_effect=self._open):
                result = self._run(self.key)
            self.assertEqual(result["status"], "configured_with_voice_incomplete")
            profile = json.loads(self.config.read_text())
            self.assertFalse(profile["voice_discovery_complete"])
            self.assertEqual(profile["voice_discovery_status"], "incomplete")
            self.assertEqual(profile["voice_databases"], [])
            self.assertIn("voice_scan_incomplete", profile["setup_warnings"])
            with self.assertRaises(source_setup.SourceSetupError):
                source_setup.require_complete_voice_source(profile)
        finally:
            blocked.chmod(0o700)

    def test_multi_account_parent_is_rejected_without_selecting_child(self) -> None:
        parent = self.base / "xwechat_files"
        for name in ("acct-a", "acct-b"):
            (parent / name / "db_storage" / "message").mkdir(parents=True)
        result = source_setup.inspect_account_root(parent)
        self.assertEqual(result["status"], "account_root_ambiguous")
        self.assertFalse(self.config.exists())

    def test_existing_config_is_not_overwritten(self) -> None:
        self.config.parent.mkdir(parents=True, mode=0o700)
        self.config.write_text("existing")
        self.config.chmod(0o600)
        with patch.object(source_setup, "open_encrypted_readonly", side_effect=self._open):
            result = self._run(self.key)
        self.assertEqual(result["status"], "config_exists")
        self.assertEqual(self.config.read_text(), "existing")

    def test_existing_image_key_config_is_never_deleted_or_overwritten(self) -> None:
        image_key = self.config.with_name("current-image-key.json")
        image_key.parent.mkdir(parents=True, mode=0o700)
        image_key.write_text("preserve-existing")
        image_key.chmod(0o600)
        with patch.object(source_setup, "open_encrypted_readonly", side_effect=self._open):
            result = self._run(self.key)
        self.assertEqual(result["status"], "config_exists")
        self.assertEqual(image_key.read_text(), "preserve-existing")
        self.assertFalse(self.config.exists())

    def test_data_home_inside_account_root_is_rejected_before_writing_any_config(self) -> None:
        with patch.object(source_setup, "open_encrypted_readonly", side_effect=self._open):
            result = source_setup.prepare_account_source(
                self.account,
                passphrase_file=self.key,
                config_path=self.config,
                data_home=self.account / "derived-output",
                **self.tools,
            )
        self.assertEqual(result["status"], "data_home_must_be_outside_account_root")
        self.assertFalse(self.config.exists())
        self.assertFalse((self.config.parent / "current-image-key.json").exists())

    def test_symlink_database_is_not_followed_and_marks_voice_scan_incomplete(self) -> None:
        outside = self.base / "outside.db"
        self._sqlite(outside, "CREATE TABLE VoiceInfo(chat_name_id, create_time, local_id, svr_id, voice_data)")
        link = self.db_storage / "escape.db"
        link.symlink_to(outside)
        with patch.object(source_setup, "open_encrypted_readonly", side_effect=self._open):
            result = self._run(self.key)
        self.assertEqual(result["status"], "configured_with_voice_incomplete")
        self.assertNotIn(str(outside), result.get("voice_databases", []))

    def test_source_config_maps_paths_and_keeps_media_opt_in(self) -> None:
        profile = {
            "runtime_python": "/venv/bin/python",
            "window_cli": "/venv/bin/wechat-local-window",
            "reply_cli": "/venv/bin/wechat-local-reply",
            "live_db_root": "/copy/acct/db_storage",
            "message_root": "/copy/acct/db_storage/message",
            "passphrase_file": "/private/key",
            "event_store": "/private/events.sqlite",
            "agent_store": "/private/agents.sqlite",
            "account_id": "local-example",
            "source_generation": "generation-example",
            "encrypted_contact_database": "/copy/acct/db_storage/contact/contact.db",
            "image_database": "/copy/acct/db_storage/message/message_resource.db",
            "image_attachments_root": "/copy/acct/msg/attach",
            "image_output_root": "/private/images",
            "voice_databases": ["/copy/acct/db_storage/VoiceInfo.db"],
            "voice_discovery_complete": True,
            "voice_discovery_status": "complete",
        }
        self.config.parent.mkdir(parents=True, mode=0o700)
        self.config.write_text(json.dumps(profile))
        self.config.chmod(0o600)
        args = argparse.Namespace(
            source_config=self.config,
            configured_images=False,
            voice_transcripts=False,
            live_db_root=None,
            message_root=None,
            passphrase=None,
            event_store=None,
            agent_store=None,
            account_id=None,
            source_generation=None,
            contact_database=None,
            encrypted_contact_database=None,
            voice_database=[],
            image_database=None,
            image_attachments_root=None,
            image_output_root=None,
        )
        source_setup.apply_source_config_to_args(args)
        self.assertEqual(args.message_root, Path(profile["message_root"]))
        self.assertEqual(args.encrypted_contact_database, Path(profile["encrypted_contact_database"]))
        self.assertIsNone(args.image_database)
        self.assertEqual(args.voice_database, [])

        image_args = copy.copy(args)
        image_args.live_db_root = image_args.message_root = image_args.passphrase = None
        image_args.event_store = image_args.agent_store = None
        image_args.account_id = image_args.source_generation = None
        image_args.encrypted_contact_database = None
        image_args.image_database = image_args.image_attachments_root = image_args.image_output_root = None
        image_args.configured_images = True
        source_setup.apply_source_config_to_args(image_args)
        self.assertEqual(image_args.image_database, Path(profile["image_database"]))

    def test_configured_image_paths_remain_enabled_without_v2_credentials(self) -> None:
        profile = {
            "runtime_python": "/venv/bin/python",
            "window_cli": "/venv/bin/wechat-local-window",
            "reply_cli": "/venv/bin/wechat-local-reply",
            "live_db_root": "/copy/acct/db_storage",
            "message_root": "/copy/acct/db_storage/message",
            "passphrase_file": "/private/key",
            "event_store": "/private/events.sqlite",
            "agent_store": "/private/agents.sqlite",
            "account_id": "local-example",
            "source_generation": "generation-example",
            "image_database": "/copy/acct/db_storage/message/message_resource.db",
            "image_attachments_root": "/copy/acct/msg/attach",
            "image_output_root": "/private/images",
            "image_credentials_status": "not_configured",
        }
        self.config.parent.mkdir(parents=True, mode=0o700)
        self.config.write_text(json.dumps(profile))
        self.config.chmod(0o600)
        args = argparse.Namespace(
            source_config=self.config,
            configured_images=True,
            voice_transcripts=False,
            live_db_root=None,
            message_root=None,
            passphrase=None,
            event_store=None,
            agent_store=None,
            account_id=None,
            source_generation=None,
            contact_database=None,
            encrypted_contact_database=None,
            voice_database=[],
            image_database=None,
            image_attachments_root=None,
            image_output_root=None,
            image_key_config=None,
        )
        source_setup.apply_source_config_to_args(args)
        self.assertFalse(args.configured_image_unavailable)
        self.assertEqual(args.image_database, Path(profile["image_database"]))
        self.assertEqual(args.image_attachments_root, Path(profile["image_attachments_root"]))
        self.assertIsNone(args.image_key_config)

    def test_image_key_discovery_is_limited_to_account_sibling_app_data(self) -> None:
        (self.documents / "app_data" / "net" / "kvcomm" / "unrelated.statistic").touch()
        with patch.object(source_setup, "open_encrypted_readonly", side_effect=self._open):
            result = self._run(self.key)
        self.assertEqual(result["status"], "configured")
        profile = json.loads(self.config.read_text())
        self.assertEqual(profile["image_credentials_status"], "ready")
        key_data = json.loads(Path(profile["image_key_config"]).read_text())
        self.assertEqual(key_data["key_codes"], [17, 29])
        self.assertEqual(key_data["account_scope"], "account_test")

    def test_net_only_image_key_directory_is_supported(self) -> None:
        ilink = self.documents / "app_data" / "ilink" / "kvcomm"
        (ilink / "key_29_aux.statistic").unlink()
        ilink.rmdir()
        with patch.object(source_setup, "open_encrypted_readonly", side_effect=self._open):
            result = self._run(self.key)
        self.assertEqual(result["status"], "configured")
        profile = json.loads(self.config.read_text())
        self.assertEqual(profile["image_credentials_status"], "ready")
        key_data = json.loads(Path(profile["image_key_config"]).read_text())
        self.assertEqual(key_data["key_codes"], [17])

    def test_ilink_only_image_key_directory_is_supported(self) -> None:
        net = self.documents / "app_data" / "net" / "kvcomm"
        (net / "key_17_main.statistic").unlink()
        net.rmdir()
        with patch.object(source_setup, "open_encrypted_readonly", side_effect=self._open):
            result = self._run(self.key)
        self.assertEqual(result["status"], "configured")
        profile = json.loads(self.config.read_text())
        self.assertEqual(profile["image_credentials_status"], "ready")
        key_data = json.loads(Path(profile["image_key_config"]).read_text())
        self.assertEqual(key_data["key_codes"], [29])

    def test_image_key_directory_overflow_stops_after_one_sentinel(self) -> None:
        directory = self.documents / "app_data" / "net" / "kvcomm"
        for entry in directory.iterdir():
            entry.unlink()
        for index in range(source_setup.MAX_IMAGE_KEY_DIRECTORY_ENTRIES + 45):
            (directory / f"entry-{index:03}.statistic").touch()
        original_scandir = os.scandir
        consumed = 0
        iterator_closed = False

        class CountingScandir:
            def __init__(self, path):
                self.path = path
                self.iterator = None

            def __enter__(self):
                self.iterator = original_scandir(self.path)
                return self

            def __exit__(self, exc_type, exc, traceback):
                self.close()

            def __iter__(self):
                return self

            def __next__(self):
                nonlocal consumed
                item = next(self.iterator)
                if os.fspath(self.path) == os.fspath(directory):
                    consumed += 1
                return item

            def close(self):
                nonlocal iterator_closed
                if self.iterator is not None:
                    self.iterator.close()
                    iterator_closed = True

        with patch.object(source_setup.os, "scandir", CountingScandir):
            result = source_setup._discover_image_keys(self.account)
        self.assertEqual(result, (None, None))
        self.assertEqual(consumed, source_setup.MAX_IMAGE_KEY_DIRECTORY_ENTRIES + 1)
        self.assertTrue(iterator_closed)

    def test_image_key_discovery_rejects_symlink_and_wrong_account_layout(self) -> None:
        external = self.base / "external.statistic"
        external.touch()
        (self.documents / "app_data" / "net" / "kvcomm" / "key_88_escape.statistic").symlink_to(external)
        with patch.object(source_setup, "open_encrypted_readonly", side_effect=self._open):
            result = self._run(self.key)
        self.assertEqual(result["status"], "configured")
        profile = json.loads(self.config.read_text())
        key_data = json.loads(Path(profile["image_key_config"]).read_text())
        self.assertEqual(key_data["key_codes"], [17, 29])
        self.assertNotIn(88, key_data["key_codes"])

    def test_incomplete_configured_voice_preserves_text_and_marks_voice_unavailable(self) -> None:
        profile = {
            "runtime_python": "/venv/bin/python",
            "window_cli": "/venv/bin/wechat-local-window",
            "reply_cli": "/venv/bin/wechat-local-reply",
            "live_db_root": "/copy/acct/db_storage",
            "message_root": "/copy/acct/db_storage/message",
            "passphrase_file": "/private/key",
            "event_store": "/private/events.sqlite",
            "agent_store": "/private/agents.sqlite",
            "account_id": "local-example",
            "source_generation": "generation-example",
            "voice_databases": [],
            "voice_discovery_complete": False,
            "voice_discovery_status": "incomplete",
        }
        self.config.parent.mkdir(parents=True, mode=0o700)
        self.config.write_text(json.dumps(profile))
        self.config.chmod(0o600)
        args = argparse.Namespace(
            source_config=self.config,
            configured_images=False,
            voice_transcripts=True,
            live_db_root=None,
            message_root=None,
            passphrase=None,
            event_store=None,
            agent_store=None,
            account_id=None,
            source_generation=None,
            contact_database=None,
            encrypted_contact_database=None,
            voice_database=[],
            image_database=None,
            image_attachments_root=None,
            image_output_root=None,
        )
        source_setup.apply_source_config_to_args(args)
        self.assertTrue(args.voice_source_unavailable)
        self.assertEqual(args.voice_database, [])

    def test_source_config_rejects_mixed_explicit_source_paths(self) -> None:
        args = argparse.Namespace(
            source_config=self.config,
            configured_images=False,
            voice_transcripts=False,
            live_db_root=Path("/override"),
            message_root=None,
            passphrase=None,
            event_store=None,
            agent_store=None,
            account_id=None,
            source_generation=None,
            contact_database=None,
            encrypted_contact_database=None,
            voice_database=[],
            image_database=None,
            image_attachments_root=None,
            image_output_root=None,
        )
        with self.assertRaises(source_setup.SourceSetupError):
            source_setup.apply_source_config_to_args(args)


if __name__ == "__main__":
    unittest.main()
