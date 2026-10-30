from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_live_identity import _fixture
from wechat_local.event_store import RawMessage
from wechat_local.live_identity import LiveIdentityAdapter


class GroupMuteTests(unittest.TestCase):
    def test_only_exact_group_integer_values_are_interpreted(self):
        for username, value, expected in (
            ('muted@chatroom',0,'muted'),('active@chatroom',1,'unmuted'),
            ('muted@chatroom',True,'unknown'),('muted@chatroom',False,'unknown'),
            ('muted@chatroom','0','unknown'),('muted@chatroom',0.0,'unknown'),
            ('muted@chatroom',2,'unknown'),('muted@chatroom',None,'unknown'),
            ('ordinary',0,'unknown'),('ordinary',1,'unknown'),
        ):
            with self.subTest(username=username,value=repr(value)):
                connection,catalog,source=_fixture(username)
                self.addCleanup(connection.close)
                adapter=LiveIdentityAdapter(catalog)
                message=RawMessage(1,101,1,1700000000,7,'fixture',None)
                with patch.object(adapter,'_contact_status_and_epoch',return_value=('configured',None)), patch.object(adapter,'_contact_record',return_value={'chat_room_notify':value}):
                    self.assertEqual(adapter.source_facts(source,message)['contact']['mute_status'],expected)

    def test_contact_lookup_is_reused_and_change_is_seen_within_and_across_scans(self):
        connection,catalog,source=_fixture('fixture@chatroom')
        self.addCleanup(connection.close)
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'contacts.db'
            writer=sqlite3.connect(path)
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('CREATE TABLE contact(username,alias,remark,nick_name,chat_room_notify)')
            writer.execute('INSERT INTO contact VALUES (?,?,?,?,?)',('fixture@chatroom','','group','',0));writer.commit()
            reader=sqlite3.connect(path);reader.execute('PRAGMA query_only=ON')
            try:
                adapter=LiveIdentityAdapter(catalog,contact_connection=reader);adapter.begin_scan()
                message=RawMessage(1,101,1,1700000000,7,'fixture',None);statements=[];reader.set_trace_callback(statements.append)
                self.assertEqual([adapter.source_facts(source,message)['contact']['mute_status'] for _ in range(3)],['muted']*3)
                self.assertEqual(sum('FROM "contact" WHERE username=' in s for s in statements),1)
                writer.execute('UPDATE contact SET chat_room_notify=1');writer.commit()
                self.assertEqual(adapter.source_facts(source,message)['contact']['mute_status'],'unmuted')
                adapter.begin_scan()
                self.assertEqual(adapter.source_facts(source,message)['contact']['mute_status'],'unmuted')
            finally:reader.close();writer.close()

    def test_missing_duplicate_or_unavailable_contact_remains_unknown(self):
        connection,catalog,source=_fixture('fixture@chatroom');self.addCleanup(connection.close)
        contacts=sqlite3.connect(':memory:');self.addCleanup(contacts.close)
        contacts.execute('CREATE TABLE contact(username,alias,remark,nick_name,chat_room_notify)')
        contacts.execute('PRAGMA query_only=ON')
        adapter=LiveIdentityAdapter(catalog,contact_connection=contacts);message=RawMessage(1,101,1,1700000000,7,'fixture',None)
        self.assertEqual(adapter.source_facts(source,message)['contact']['mute_status'],'unknown')
        contacts.execute('PRAGMA query_only=OFF')
        contacts.executemany('INSERT INTO contact VALUES(?,?,?,?,?)',[('fixture@chatroom','','','',0),('fixture@chatroom','','','',1)]);contacts.commit();contacts.execute('PRAGMA query_only=ON')
        self.assertEqual(adapter.source_facts(source,message)['contact']['mute_status'],'unknown')
        adapter.contact_source_unavailable=True
        self.assertEqual(adapter.source_facts(source,message)['contact']['mute_status'],'unknown')

    def test_contact_change_during_lookup_does_not_return_stale_mute(self):
        connection,catalog,source=_fixture('fixture@chatroom');self.addCleanup(connection.close)
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'contacts.db';writer=sqlite3.connect(path)
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('CREATE TABLE contact(username,alias,remark,nick_name,chat_room_notify)')
            writer.execute('INSERT INTO contact VALUES (?,?,?,?,?)',('fixture@chatroom','','group','',0));writer.commit()
            reader=sqlite3.connect(path);reader.execute('PRAGMA query_only=ON')
            try:
                adapter=LiveIdentityAdapter(catalog,contact_connection=reader);adapter.begin_scan()
                message=RawMessage(1,101,1,1700000000,7,'fixture',None);original=adapter._epoch;calls=[]
                def epoch(candidate):
                    if candidate is reader:
                        calls.append(True)
                        if len(calls)==2:
                            writer.execute('UPDATE contact SET chat_room_notify=1');writer.commit()
                    return original(candidate)
                with patch.object(adapter,'_epoch',epoch):
                    self.assertEqual(adapter.source_facts(source,message)['contact']['mute_status'],'unknown')
                    self.assertEqual(adapter.source_facts(source,message)['contact']['mute_status'],'unmuted')
            finally:reader.close();writer.close()


if __name__=='__main__':unittest.main()
