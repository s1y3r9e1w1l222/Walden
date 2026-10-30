"""Run with the package installed in the test interpreter's isolated venv.

Uses only newly generated SQLCipher fixtures and the installed public commands.
No debugger, WeChat app, external credentials, media or send action is used.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from sqlcipher3 import dbapi2 as sqlcipher


class InstalledSelfIdentityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.bin = Path(sys.executable).parent
        for command in ('wechat-local-setup', 'wechat-local-window'):
            self.assertTrue((self.bin / command).is_file(), 'Install the wheel in this isolated interpreter first')
        self.config = self.root / 'private/current-source.json'
        self.key = self.root / 'private/key.bin'
        self.key.parent.mkdir(mode=0o700)
        # Exactly 32 ASCII bytes so the fixture uses real SQLCipher's passphrase KDF.
        self.key.write_bytes(b'synthetic-passphrase-for-tests!!')
        self.assertEqual(len(self.key.read_bytes()), 32)
        self.key.chmod(0o600)
        self.account = self.root / 'directory_named_for_someone_else'
        self.messages = self.account / 'db_storage/message/message_0.db'
        self.contact = self.account / 'db_storage/contact/contact.db'
        self._fixture()
        self.db_hash = self.hash(self.messages)
        self.runs = []

    def tearDown(self):
        destination = os.environ.get('WECHAT_SELF_IDENTITY_EVIDENCE')
        if destination:
            out = Path(destination)
            out.mkdir(parents=True, exist_ok=True)
            (out / (self._testMethodName + '.json')).write_text(json.dumps(self.runs, ensure_ascii=False, indent=2))
        self.temp.cleanup()

    @staticmethod
    def hash(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def connection(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlcipher.connect(str(path))
        connection.execute("PRAGMA key = 'synthetic-passphrase-for-tests!!'")
        connection.execute('PRAGMA cipher_compatibility = 4')
        return connection

    def _fixture(self):
        db = self.connection(self.messages)
        db.execute('CREATE TABLE Name2Id(user_name TEXT)')
        db.executemany('INSERT INTO Name2Id(rowid, user_name) VALUES (?, ?)', [
            (1, 'synthetic_self'), (2, 'synthetic_peer'), (3, 'synthetic_team@chatroom'),
        ])
        table = 'Msg_' + hashlib.md5(b'synthetic_team@chatroom').hexdigest()
        db.execute(f'''CREATE TABLE {table} (
            local_id INTEGER PRIMARY KEY, server_id INTEGER, local_type INTEGER,
            create_time INTEGER, real_sender_id INTEGER, message_content BLOB,
            WCDB_CT_message_content INTEGER, source BLOB, WCDB_CT_source INTEGER)''')
        db.executemany(f'INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, 0, ?, 0)', [
            (1, 101, 1, 1700000001, 1, 'self original', '<msgsource/>'),
            (2, 102, 1, 1700000002, 2, 'peer direct mention original', '<msgsource><atuserlist>synthetic_self</atuserlist></msgsource>'),
            (3, 103, 1, 1700000003, 2, 'peer all mention original', '<msgsource><atuserlist>notify@all</atuserlist></msgsource>'),
            (4, 104, 10000, 1700000004, None, 'system original', '<msgsource/>'),
        ])
        db.commit()
        db.close()
        contacts = self.connection(self.contact)
        contacts.execute('CREATE TABLE contact(username, alias, remark, nick_name)')
        contacts.executemany('INSERT INTO contact VALUES (?, ?, ?, ?)', [
            ('synthetic_self', '', 'Same displayed name', 'Original self nickname'),
            ('synthetic_peer', '', 'Same displayed name', 'Original peer nickname'),
            ('synthetic_team@chatroom', '', 'Synthetic group', ''),
        ])
        contacts.commit()
        contacts.close()

    def run_command(self, command, args, expected=0):
        env = dict(os.environ)
        env.pop('PYTHONPATH', None)
        result = subprocess.run([str(self.bin / command), *args], cwd=self.root, env=env,
                                text=True, capture_output=True, timeout=30)
        self.runs.append({'command': command, 'arguments': args, 'exit': result.returncode,
                          'stdout': result.stdout, 'stderr': result.stderr})
        self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
        self.assertEqual(result.stderr, '')
        return json.loads(result.stdout)

    def setup(self, confirmed=False):
        args = ['--account-root', str(self.account), '--passphrase-file', str(self.key),
                '--config', str(self.config), '--data-home', str(self.root / 'data')]
        if confirmed:
            args += ['--confirm-self-username', 'synthetic_self']
        result = self.run_command('wechat-local-setup', args)
        self.assertEqual(result['status'], 'configured')
        self.assertNotIn('synthetic_self', json.dumps(result))

    def query(self, *extra, expected=0):
        return self.run_command('wechat-local-window', [
            '--source-config', str(self.config), '--request', '从 2023-11-14 22:13:20 到 2023-11-14 22:13:30',
            '--timezone', 'UTC', '--json', *extra], expected)

    def refs(self, result):
        self.assertEqual(result['total_count'], 4)
        return {ref['citation_text']: ref for group in result['groups'] for ref in group['event_refs']}

    def assert_confirmed_facts(self, result):
        refs = self.refs(result)
        self.assertEqual(set(refs), {'self original', 'peer direct mention original', 'peer all mention original', 'system original'})
        self.assertEqual(refs['self original']['direction'], 'self')
        self.assertEqual(refs['peer direct mention original']['direction'], 'inbound')
        self.assertEqual(refs['peer direct mention original']['source_facts']['mentions']['at_self'], 'yes')
        self.assertEqual(refs['peer all mention original']['source_facts']['mentions']['at_self'], 'no')
        self.assertEqual(refs['peer all mention original']['source_facts']['mentions']['at_all'], 'yes')
        self.assertEqual(refs['system original']['direction'], 'unknown')
        self.assertEqual(refs['system original']['content_kind'], 'system')
        self.assertEqual(refs['self original']['sender_identity'], 'synthetic_self')
        self.assertEqual(refs['peer direct mention original']['sender_identity'], 'synthetic_peer')
        self.assertEqual(self.hash(self.messages), self.db_hash)
        return refs

    def test_old_config_confirmation_reuse_name_change_and_explicit_correction(self):
        self.setup()
        before = self.refs(self.query())
        self.assertTrue(all(ref['direction'] == 'unknown' for ref in before.values()))
        self.assertEqual(before['peer direct mention original']['source_facts']['mentions']['at_self'], 'unknown')
        # Confirmation does not need a readable key or database at all.
        offline_account = self.account.with_name('offline-account')
        self.account.rename(offline_account)
        offline_key = self.key.with_name('offline-key')
        self.key.rename(offline_key)
        result = self.run_command('wechat-local-setup', ['--config', str(self.config), '--confirm-self-username', 'synthetic_self'])
        self.assertEqual(result, {'status': 'self_identity_confirmed'})
        offline_account.rename(self.account)
        offline_key.rename(self.key)
        self.assertEqual(self.config.stat().st_mode & 0o777, 0o600)
        self.assert_confirmed_facts(self.query())
        contacts = self.connection(self.contact)
        contacts.execute("UPDATE contact SET remark='Renamed visible contact', nick_name='Renamed nickname' WHERE username='synthetic_self'")
        contacts.commit()
        contacts.close()
        renamed = self.assert_confirmed_facts(self.query())
        self.assertEqual(renamed['self original']['sender_display_name'], 'Renamed visible contact')
        conflict = self.query('--self-username', 'synthetic_peer', expected=2)
        self.assertEqual(conflict['configuration_detail'], 'self_identity_conflict')
        self.assert_confirmed_facts(self.query('--self-username', 'synthetic_self'))
        replaced = self.run_command('wechat-local-setup', ['--config', str(self.config), '--confirm-self-username', 'synthetic_peer', '--replace-self-identity'])
        self.assertEqual(replaced['status'], 'self_identity_confirmed')
        corrected = self.refs(self.query())
        self.assertEqual(corrected['self original']['direction'], 'inbound')
        self.assertEqual(corrected['peer direct mention original']['direction'], 'self')

    def test_new_setup_confirmation_and_binding_errors_in_public_query(self):
        self.setup(confirmed=True)
        self.assert_confirmed_facts(self.query())
        original = json.loads(self.config.read_text())
        for field, value in [('account_id', 'changed-account'), ('source_generation', 'changed-generation'),
                             ('live_db_root', str(self.root / 'changed-db-root')),
                             ('message_root', str(self.root / 'changed-message-root'))]:
            profile = copy.deepcopy(original)
            profile[field] = value
            self.config.write_text(json.dumps(profile))
            self.config.chmod(0o600)
            result = self.query(expected=2)
            self.assertEqual(result['configuration_detail'], 'self_identity_binding_mismatch')
            self.assertEqual(result['action'], 'confirm_self_identity')
        self.config.write_text(json.dumps(original))
        self.config.chmod(0o644)
        self.assertEqual(self.query(expected=2)['error_code'], 'configuration_unavailable')


if __name__ == '__main__':
    unittest.main()
