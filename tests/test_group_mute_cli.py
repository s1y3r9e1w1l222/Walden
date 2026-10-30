from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from sqlcipher3 import dbapi2 as cipher

from wechat_local.agent_store import AgentStore
from wechat_local.event_store import EventStore


class GroupMutePublicTests(unittest.TestCase):
    def test_installed_window_group_states_private_unknown_and_updates(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);live=root/'live';message=live/'message';message.mkdir(parents=True)
            path=message/'message_0.db';db=cipher.connect(str(path));db.execute("PRAGMA key='"+'p'*32+"'");db.execute('PRAGMA cipher_compatibility=4')
            db.execute('CREATE TABLE Name2Id(user_name TEXT)')
            users=[('quiet@chatroom','QuietGroup',0),('active@chatroom','ActiveGroup',1),('private_peer','PrivatePeer',0)]
            for i,(username,_,_) in enumerate(users,1):
                db.execute('INSERT INTO Name2Id(rowid,user_name) VALUES (?,?)',(i,username))
                table='Msg_'+hashlib.md5(username.encode()).hexdigest()
                db.execute(f'CREATE TABLE {table}(local_id INTEGER PRIMARY KEY,server_id INTEGER,local_type INTEGER NOT NULL,create_time INTEGER,real_sender_id INTEGER,message_content BLOB,WCDB_CT_message_content INTEGER)')
                db.execute(f'INSERT INTO {table} VALUES (1,?,1,1700000000,?,?,0)',(100+i,i,b'synthetic content'))
            db.commit();db.close()
            contact=root/'contacts.db';writer=sqlite3.connect(contact)
            writer.execute('CREATE TABLE contact(username,alias,remark,nick_name,chat_room_notify)')
            writer.executemany('INSERT INTO contact VALUES (?,?,?,?,?)',[(username,'',label,'',value) for username,label,value in users]);writer.commit()
            key=root/'key';key.write_bytes(b'p'*32);key.chmod(0o600)
            event=root/'event.db';agent=root/'agent.db';EventStore(event).close();AgentStore(agent).close()
            args=['--live-db-root',str(live),'--message-root',str(message),'--passphrase',str(key),'--event-store',str(event),'--agent-store',str(agent),'--account-id','fixture','--source-generation','fixture','--request','从 2023-11-14 22:13 到 2023-11-14 22:15','--timezone','UTC','--contact-database',str(contact),'--json']
            env=os.environ.copy();cli=env.get('WECHAT_MUTE_WINDOW')
            command=[cli] if cli else [sys.executable,'-m','wechat_local.codex_first_runtime']
            if cli:env.pop('PYTHONPATH',None)
            def run(command):
                result=subprocess.run([*command,*args],capture_output=True,text=True,env=env,timeout=45)
                self.assertEqual(result.returncode,0,result.stderr)
                return json.loads(result.stdout)
            try:
                first=run(command)
                groups={group['display_name']:group for group in first['groups']}
                self.assertEqual(first['total_count'],3)
                self.assertEqual(set(groups),{'QuietGroup','ActiveGroup','PrivatePeer'})
                for label,expected in [('QuietGroup','muted'),('ActiveGroup','unmuted'),('PrivatePeer','unknown')]:
                    self.assertEqual(groups[label]['event_refs'][0]['source_facts']['contact']['mute_status'],expected)
                old=env.get('WECHAT_MUTE_BASE')
                if old:
                    previous=run([old])
                    def strip(value):
                        for group in value['groups']:
                            for event_ref in group['event_refs']:
                                event_ref['source_facts']['contact'].pop('mute_status',None)
                        return value
                    self.assertEqual(strip(previous),strip(json.loads(json.dumps(first))))
                writer.execute("UPDATE contact SET chat_room_notify=1 WHERE username='quiet@chatroom'");writer.commit()
                second=run(command);updated={g['display_name']:g for g in second['groups']}
                self.assertEqual(updated['QuietGroup']['event_refs'][0]['source_facts']['contact']['mute_status'],'unmuted')
                self.assertEqual({e['event_id'] for g in first['groups'] for e in g['event_refs']},{e['event_id'] for g in second['groups'] for e in g['event_refs']})
            finally:writer.close()


if __name__=='__main__':unittest.main()
