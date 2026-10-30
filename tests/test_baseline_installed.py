from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

from sqlcipher3 import dbapi2 as cipher


@unittest.skipUnless(os.environ.get('WECHAT_BASELINE_SETUP'), 'requires fresh installed entrypoints')
class InstalledBaselineTests(unittest.TestCase):
    def test_setup_then_rolling_window_uses_same_installed_flow(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);account=root/'account';messages=account/'db_storage'/'message';messages.mkdir(parents=True)
            key=root/'key';key.write_bytes(b'p'*32);key.chmod(0o600)
            now=int(time.time())
            for index in range(2):
                user=f'fixture_{index}';table='Msg_'+hashlib.md5(user.encode()).hexdigest()
                db=cipher.connect(str(messages/f'message_{index}.db'));db.execute("PRAGMA key='"+'p'*32+"'");db.execute('PRAGMA cipher_compatibility=4')
                db.execute('CREATE TABLE Name2Id (user_name TEXT)');db.execute('INSERT INTO Name2Id(rowid,user_name) VALUES(7,?)',(user,))
                db.execute(f'CREATE TABLE {table} (local_id INTEGER PRIMARY KEY,server_id INTEGER,local_type INTEGER NOT NULL,create_time INTEGER,real_sender_id INTEGER,message_content BLOB,WCDB_CT_message_content INTEGER)')
                db.execute(f'INSERT INTO {table} VALUES (1,101,1,?,7,?,0)',(now-(20000 if index==0 else 60),b'synthetic baseline body'))
                db.commit();db.close()
            config=root/'private'/'current.json';data=root/'data'
            trace=root/'trace';hook=root/'hook';hook.mkdir()
            (hook/'sitecustomize.py').write_text('''import os,threading\nimport wechat_local.codex_first_runtime as r\nlock=threading.Lock()\ndef wrap(obj,name,label):\n old=getattr(obj,name)\n def f(*a,**kw):\n  with lock:\n   with open(os.environ["WECHAT_TEST_TRACE"],"a") as out:out.write(label+"\\n")\n  return old(*a,**kw)\n setattr(obj,name,f)\nwrap(r,"read_message_window","read")\nwrap(r,"_empty_window_probe","probe")\nwrap(r.EventStore,"commit_window_messages","ingest")\n''')
            env=os.environ.copy();env['PYTHONPATH']=str(hook);env['WECHAT_TEST_TRACE']=str(trace)
            started=time.monotonic()
            setup=subprocess.run([os.environ['WECHAT_BASELINE_SETUP'],'--account-root',str(account),'--passphrase-file',str(key),'--config',str(config),'--data-home',str(data)],capture_output=True,text=True,env=env)
            first_seconds=time.monotonic()-started
            self.assertEqual(setup.returncode,0,setup.stdout+setup.stderr)
            self.assertNotIn('synthetic baseline body',setup.stdout)
            result=json.loads(setup.stdout);self.assertEqual(result['status'],'configured');self.assertEqual(result['message_baseline']['status'],'ready')
            self.assertEqual(result['self_identity']['status'],'incomplete');self.assertEqual(result['mute_status']['status'],'incomplete')
            profile=json.loads(config.read_text());events=Path(profile['event_store'])
            with sqlite3.connect(events) as db:before=db.execute('SELECT count(*) FROM window_ingest_runs').fetchone()[0]
            first_trace=trace.read_text().splitlines();trace.unlink()
            started=time.monotonic()
            query=subprocess.run([os.environ['WECHAT_BASELINE_WINDOW'],'--source-config',str(config),'--request','最近3小时','--timezone','UTC','--json'],capture_output=True,text=True,env=env)
            second_seconds=time.monotonic()-started
            self.assertEqual(query.returncode,0,query.stdout+query.stderr)
            payload=json.loads(query.stdout);self.assertEqual(payload['total_count'],1)
            with sqlite3.connect(events) as db:after=db.execute('SELECT count(*) FROM window_ingest_runs').fetchone()[0]
            self.assertEqual(before,2);self.assertEqual(after-before,1)
            second_trace=trace.read_text().splitlines()
            self.assertEqual({k:first_trace.count(k) for k in ('probe','read','ingest')},{'probe':2,'read':1,'ingest':2})
            self.assertEqual({k:second_trace.count(k) for k in ('probe','read','ingest')},{'probe':0,'read':1,'ingest':1})
            print(json.dumps({'synthetic_first_seconds':round(first_seconds,3),'synthetic_next_seconds':round(second_seconds,3),'first':{k:first_trace.count(k) for k in ('probe','read','ingest')},'next':{k:second_trace.count(k) for k in ('probe','read','ingest')},'status':'PASS'}))


if __name__ == '__main__':unittest.main()
