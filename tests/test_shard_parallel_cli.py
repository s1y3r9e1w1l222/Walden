from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from sqlcipher3 import dbapi2 as cipher
from wechat_local.agent_store import AgentStore
from wechat_local.event_store import EventStore
import wechat_local.live_catalog as catalog_module
import wechat_local.codex_first_runtime as runtime


class InstalledParallelWindowTests(unittest.TestCase):
    def test_public_window_serial_and_two_workers_return_same_results_once_opened(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            live = root / 'live'
            messages = live / 'message'
            messages.mkdir(parents=True)
            passphrase = root / 'key'
            passphrase.write_bytes(b'p' * 32)
            os.chmod(passphrase, 0o600)
            for i in range(2):
                user = f'fixture_{i}'
                table = 'Msg_' + hashlib.md5(user.encode()).hexdigest()
                db = cipher.connect(str(messages / f'message_{i}.db'))
                db.execute("PRAGMA key='" + 'p' * 32 + "'")
                db.execute('PRAGMA cipher_compatibility=4')
                db.execute('CREATE TABLE Name2Id (user_name TEXT)')
                db.execute('INSERT INTO Name2Id(rowid,user_name) VALUES (7,?)', (user,))
                db.execute(f'CREATE TABLE {table} (local_id INTEGER PRIMARY KEY, server_id INTEGER, local_type INTEGER NOT NULL, create_time INTEGER, real_sender_id INTEGER, message_content BLOB, WCDB_CT_message_content INTEGER)')
                db.execute(f'INSERT INTO {table} VALUES (1,?,1,1700000000,7,?,0)', (101+i, b'fixture'))
                db.commit()
                db.close()
            results = []
            for workers in (1, 2):
                event = root / f'events-{workers}.db'
                agent = root / f'agent-{workers}.db'
                EventStore(event).close()
                AgentStore(agent).close()
                argv = ['--live-db-root', str(live), '--message-root', str(messages), '--passphrase', str(passphrase), '--event-store', str(event), '--agent-store', str(agent), '--account-id', 'fixture', '--source-generation', 'fixture', '--request', '从 2023-11-14 22:13 到 2023-11-14 22:15', '--timezone', 'UTC', '--json']
                opened = []
                real_open = catalog_module.open_encrypted_readonly
                def counted(path, *args, **kwargs):
                    opened.append(path.name)
                    return real_open(path, *args, **kwargs)
                output = StringIO()
                with patch.object(runtime, '_WINDOW_SHARD_WORKERS', workers), patch.object(catalog_module, 'open_encrypted_readonly', counted), redirect_stdout(output):
                    code = runtime.main(argv)
                self.assertEqual(code, 0, output.getvalue())
                self.assertEqual(sorted(opened), ['message_0.db', 'message_1.db'])
                results.append(json.loads(output.getvalue()))
            self.assertEqual(results[0], results[1])
            self.assertEqual(results[0]['total_count'], 2)


if __name__ == '__main__':
    unittest.main()
