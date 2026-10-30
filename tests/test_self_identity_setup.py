"""Synthetic regressions for confirmed identity persistence and source binding."""
from __future__ import annotations

import contextlib
import copy
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from wechat_local import source_setup
from wechat_local.codex_first_runtime import build_parser


class SelfIdentitySetupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.private = self.root / 'private'
        self.private.mkdir(mode=0o700)
        self.config = self.private / 'current-source.json'
        self.live = self.root / 'misleading_wxid_directory' / 'db_storage'
        self.profile = {
            'runtime_python': sys.executable,
            'window_cli': str(self.root / 'bin/wechat-local-window'),
            'reply_cli': str(self.root / 'bin/wechat-local-reply'),
            'live_db_root': str(self.live),
            'message_root': str(self.live / 'message'),
            'passphrase_file': str(self.private / 'missing-passphrase'),
            'event_store': str(self.root / 'events.sqlite'),
            'agent_store': str(self.root / 'agents.sqlite'),
            'account_id': 'synthetic-account',
            'source_generation': 'synthetic-generation',
            'setup_warnings': ['preserve-this-warning'],
            'voice_databases': [],
            'voice_discovery_complete': True,
            'voice_discovery_status': 'absent',
        }
        self.write(self.profile)

    def tearDown(self):
        self.temp.cleanup()

    def write(self, value):
        self.config.write_text(json.dumps(value))
        self.config.chmod(0o600)

    def setup_cli(self, *args):
        output, errors = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            try:
                code = source_setup.main(['--config', str(self.config), *args])
            except SystemExit as exc:
                code = exc.code
        self.assertTrue(output.getvalue(), errors.getvalue())
        return code, output.getvalue(), errors.getvalue()

    def args(self, *extra):
        return build_parser().parse_args(['--source-config', str(self.config), *extra])

    def confirm(self, username='synthetic_self', *extra):
        code, output, errors = self.setup_cli('--confirm-self-username', username, *extra)
        self.assertEqual(code, 0, output + errors)
        self.assertEqual(json.loads(output)['status'], 'self_identity_confirmed')
        self.assertNotIn(username, output + errors)
        return json.loads(self.config.read_text())

    def test_existing_config_confirmation_works_without_database_or_key_and_preserves_fields(self):
        profile = self.confirm()
        self.assertFalse(self.live.exists())
        self.assertFalse(Path(self.profile['passphrase_file']).exists())
        self.assertEqual({k: v for k, v in profile.items() if k != 'self_identity'}, self.profile)
        self.assertEqual(self.config.stat().st_mode & 0o777, 0o600)
        args = self.args()
        source_setup.apply_source_config_to_args(args)
        self.assertEqual(args.self_username, 'synthetic_self')
        self.assertEqual(args.account_id, 'synthetic-account')

    def test_old_config_retains_unknown_and_explicit_legacy_cli_remains_available(self):
        args = self.args()
        source_setup.apply_source_config_to_args(args)
        self.assertIsNone(args.self_username)
        args = self.args('--self-username', 'synthetic_self')
        source_setup.apply_source_config_to_args(args)
        self.assertEqual(args.self_username, 'synthetic_self')
        self.assertNotIn('self_identity', json.loads(self.config.read_text()))

    def test_confirmed_identity_accepts_same_explicit_value_and_rejects_conflict(self):
        self.confirm()
        args = self.args('--self-username', 'synthetic_self')
        source_setup.apply_source_config_to_args(args)
        self.assertEqual(args.self_username, 'synthetic_self')
        with self.assertRaisesRegex(source_setup.SourceSetupError, 'self_identity_conflict'):
            source_setup.apply_source_config_to_args(self.args('--self-username', 'synthetic_other'))

    def test_account_generation_database_and_message_roots_cannot_silently_reuse_identity(self):
        original = self.confirm()
        for field, new in (
            ('account_id', 'different-account'),
            ('source_generation', 'different-generation'),
            ('live_db_root', str(self.root / 'other/db_storage')),
            ('message_root', str(self.root / 'other/db_storage/message')),
        ):
            with self.subTest(field=field):
                changed = copy.deepcopy(original)
                changed[field] = new
                self.write(changed)
                with self.assertRaisesRegex(source_setup.SourceSetupError, 'self_identity_binding_mismatch'):
                    source_setup.apply_source_config_to_args(self.args())
                before = self.config.read_bytes()
                code, output, _ = self.setup_cli('--confirm-self-username', 'synthetic_self')
                self.assertEqual(code, 2)
                self.assertEqual(json.loads(output)['status'], 'self_identity_binding_mismatch')
                self.assertEqual(self.config.read_bytes(), before)

    def test_tampered_binding_is_rejected_and_explicit_replace_rebinds_current_profile(self):
        profile = self.confirm()
        profile['self_identity']['account_id'] = 'tampered-binding'
        self.write(profile)
        with self.assertRaisesRegex(source_setup.SourceSetupError, 'self_identity_binding_mismatch'):
            source_setup.load_source_config(self.config)
        self.confirm('synthetic_new', '--replace-self-identity')
        args = self.args()
        source_setup.apply_source_config_to_args(args)
        self.assertEqual(args.self_username, 'synthetic_new')

    def test_correction_requires_explicit_replace_and_retains_unrelated_config(self):
        first = self.confirm()
        before = self.config.read_bytes()
        code, output, _ = self.setup_cli('--confirm-self-username', 'synthetic_new')
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(output)['status'], 'self_identity_replace_required')
        self.assertEqual(self.config.read_bytes(), before)
        changed = self.confirm('synthetic_new', '--replace-self-identity')
        self.assertEqual({k: v for k, v in changed.items() if k != 'self_identity'},
                         {k: v for k, v in first.items() if k != 'self_identity'})

    def test_invalid_identifiers_and_incompatible_update_options_leave_original_bytes(self):
        before = self.config.read_bytes()
        for username in ('', ' ', ' name', 'name\nsecret', 'group@chatroom', 'notify@all', '../name', '显示名称', 'x'*129):
            with self.subTest(username=repr(username)):
                code, output, errors = self.setup_cli('--confirm-self-username', username)
                self.assertEqual(code, 2)
                self.assertEqual(json.loads(output)['status'], 'self_username_invalid')
                self.assertNotIn(username, output + errors) if username.strip() else None
                self.assertEqual(self.config.read_bytes(), before)
        for extra in (['--acquire-passphrase'], ['--passphrase-file', str(self.private / 'key')],
                      ['--data-home', str(self.root / 'output')], ['--capture-timeout', '1']):
            with self.subTest(extra=extra):
                code, output, _ = self.setup_cli('--confirm-self-username', 'synthetic_self', *extra)
                self.assertEqual(code, 2)
                self.assertEqual(json.loads(output)['status'], 'identity_update_arguments_invalid')
                self.assertEqual(self.config.read_bytes(), before)

    def test_symlink_and_public_config_or_parent_do_not_confirm_identity(self):
        before = self.config.read_bytes()
        target = self.private / 'target.json'
        self.config.rename(target)
        self.config.symlink_to(target)
        code, _, _ = self.setup_cli('--confirm-self-username', 'synthetic_self')
        self.assertEqual(code, 2)
        self.assertEqual(target.read_bytes(), before)
        self.config.unlink()
        target.rename(self.config)
        self.config.chmod(0o644)
        code, _, _ = self.setup_cli('--confirm-self-username', 'synthetic_self')
        self.assertEqual(code, 2)
        self.assertEqual(self.config.read_bytes(), before)
        self.config.chmod(0o600)
        self.private.chmod(0o755)
        code, _, _ = self.setup_cli('--confirm-self-username', 'synthetic_self')
        self.assertEqual(code, 2)
        self.assertEqual(self.config.read_bytes(), before)
        self.private.chmod(0o700)

    def test_confirmed_config_requires_exact_private_mode_and_canonical_binding(self):
        profile = self.confirm()
        self.config.chmod(0o640)
        with self.assertRaises(source_setup.SourceSetupError):
            source_setup.apply_source_config_to_args(self.args())
        self.config.chmod(0o400)
        with self.assertRaises(source_setup.SourceSetupError):
            source_setup.apply_source_config_to_args(self.args())
        self.config.chmod(0o600)
        profile['self_identity']['live_db_root'] += '/..'
        self.write(profile)
        with self.assertRaises(source_setup.SourceSetupError):
            source_setup.apply_source_config_to_args(self.args())

    def test_symlink_in_config_ancestry_is_rejected_for_update_and_reuse(self):
        self.confirm()
        link = self.root / 'alias'
        link.symlink_to(self.root, target_is_directory=True)
        actual = self.config
        self.config = link / 'private' / actual.name
        before = actual.read_bytes()
        with self.assertRaises(source_setup.SourceSetupError):
            source_setup.apply_source_config_to_args(self.args())
        code, _, _ = self.setup_cli('--confirm-self-username', 'synthetic_self')
        self.assertEqual(code, 2)
        self.assertEqual(actual.read_bytes(), before)

    def test_invalid_bound_path_reports_error_without_ever_confirming(self):
        profile = copy.deepcopy(self.profile)
        profile['live_db_root'] = '/synthetic/invalid\x00path'
        self.write(profile)
        before = self.config.read_bytes()
        code, output, _ = self.setup_cli('--confirm-self-username', 'synthetic_self')
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(output)['status'], 'self_identity_binding_mismatch')
        self.assertEqual(self.config.read_bytes(), before)

    def test_query_binding_and_conflict_errors_are_actionable_without_identity_values(self):
        from wechat_local.codex_first_runtime import main as query_main
        original = self.confirm()
        for detail, extra in (
            ('self_identity_binding_mismatch', []),
            ('self_identity_conflict', ['--self-username', 'synthetic_other']),
        ):
            changed = copy.deepcopy(original)
            if not extra:
                changed['source_generation'] = 'new-generation'
            self.write(changed)
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = query_main(['--source-config', str(self.config), '--check-config', '--json', *extra])
            self.assertEqual(code, 2)
            result = json.loads(output.getvalue())
            self.assertEqual(result.get('configuration_detail'), detail)
            self.assertEqual(result.get('action'), 'confirm_self_identity')
            self.assertNotIn('synthetic_self', output.getvalue())
            self.assertNotIn('synthetic_other', output.getvalue())

    def test_write_failure_keeps_original_and_does_not_leave_private_temporary_files(self):
        before = self.config.read_bytes()
        files_before = set(self.private.iterdir())
        for operation in ('replace', 'fsync'):
            with self.subTest(operation=operation), patch.object(source_setup.os, operation, side_effect=OSError('synthetic failure')):
                code, output, _ = self.setup_cli('--confirm-self-username', 'synthetic_self')
                self.assertEqual(code, 2)
                self.assertEqual(json.loads(output)['status'], 'self_identity_save_failed')
                self.assertEqual(self.config.read_bytes(), before)
                self.assertEqual(set(self.private.iterdir()), files_before)

    def test_existing_temporary_name_is_not_deleted_after_failed_exclusive_creation(self):
        from types import SimpleNamespace
        existing = self.private / '.self-identity-fixed.tmp'
        existing.write_bytes(b'unrelated-existing-file')
        before = self.config.read_bytes()
        with patch.object(source_setup.uuid, 'uuid4', return_value=SimpleNamespace(hex='fixed')):
            code, output, _ = self.setup_cli('--confirm-self-username', 'synthetic_self')
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(output)['status'], 'self_identity_save_failed')
        self.assertEqual(self.config.read_bytes(), before)
        self.assertTrue(existing.exists(), 'An unrelated collision file must not be removed')
        self.assertEqual(existing.read_bytes(), b'unrelated-existing-file')

    def test_replaced_temporary_file_is_never_published_as_confirmed(self):
        before = self.config.read_bytes()
        original_fsync = source_setup.os.fsync
        replacement = self.private / 'replacement'
        replacement.write_bytes(b'unrelated-content')
        def swap(fd):
            original_fsync(fd)
            temporary = next(self.private.glob('.self-identity-*.tmp'))
            temporary.unlink()
            temporary.symlink_to(replacement)
        with patch.object(source_setup.os, 'fsync', side_effect=swap):
            code, output, _ = self.setup_cli('--confirm-self-username', 'synthetic_self')
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(output)['status'], 'self_identity_config_changed')
        self.assertFalse(self.config.is_symlink())
        self.assertEqual(self.config.read_bytes(), before)
        self.assertEqual(replacement.read_bytes(), b'unrelated-content')

    def test_malformed_identity_objects_and_replaced_source_root_do_not_load(self):
        original = self.confirm()
        for bad in (None, [], {}, {'username': 'synthetic_self'},
                    {**original['self_identity'], 'username': 'group@chatroom'},
                    {**original['self_identity'], 'unexpected': True}):
            with self.subTest(identity=bad):
                changed = copy.deepcopy(original)
                changed['self_identity'] = bad
                self.write(changed)
                with self.assertRaises(source_setup.SourceSetupError):
                    source_setup.apply_source_config_to_args(self.args())
        self.write(original)
        self.live.parent.mkdir(parents=True)
        other = self.root / 'other-root'
        other.mkdir()
        self.live.symlink_to(other, target_is_directory=True)
        with self.assertRaisesRegex(source_setup.SourceSetupError, 'self_identity_binding_mismatch'):
            source_setup.apply_source_config_to_args(self.args())

    def test_setup_creation_accepts_confirmed_identity_and_restores_it(self):
        from sqlcipher3 import dbapi2 as sqlcipher
        self.config.unlink()
        message_root = self.live / 'message'
        message_root.mkdir(parents=True)
        key = self.private / 'key.bin'
        key.write_bytes(b'k' * 32)
        key.chmod(0o600)
        connection = sqlcipher.connect(str(message_root / 'message_0.db'))
        from wechat_local.wcdb_crypto import derive_database_key
        salt = b's' * 16
        material = derive_database_key(b'k' * 32, salt) + salt
        connection.execute('PRAGMA key = "x\'' + material.hex() + '\'"')
        connection.execute('PRAGMA cipher_compatibility = 4')
        connection.execute('CREATE TABLE Name2Id(user_name)')
        connection.commit()
        connection.close()
        bin_dir = self.root / 'bin'
        bin_dir.mkdir()
        for name in ('wechat-local-window', 'wechat-local-reply'):
            path = bin_dir / name
            path.write_text('#!/bin/sh\nexit 0\n')
            path.chmod(0o700)
        with patch.object(sys, 'argv', [str(bin_dir / 'wechat-local-setup')]):
            code, output, errors = self.setup_cli(
                '--account-root', str(self.live.parent), '--passphrase-file', str(key),
                '--data-home', str(self.root / 'data'), '--confirm-self-username', 'synthetic_self')
        self.assertEqual(code, 0, output + errors)
        self.assertEqual(json.loads(output)['status'], 'configured')
        self.assertNotIn('synthetic_self', output + errors)
        args = self.args()
        source_setup.apply_source_config_to_args(args)
        self.assertEqual(args.self_username, 'synthetic_self')


if __name__ == '__main__':
    unittest.main()
