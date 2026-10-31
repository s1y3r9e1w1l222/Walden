"""Location-only tests use synthetic encrypted sources and observe actual column access."""
import unittest,os,shutil,json,io,contextlib
from unittest.mock import patch
from sqlcipher3 import dbapi2 as sql
from tests.test_contact_query import ContactQueryTests,TARGET,OTHER,GROUP2,table
from tests import test_codex_first_voice_cli as fixture
import wechat_local.codex_first_runtime as runtime
import wechat_local.live_catalog as catalog
from wechat_local.event_store import EventStore,SourceKey,RawMessage,WindowIngestRun
from wechat_local.live_catalog import LiveCatalog
from datetime import datetime,timezone

class LocateGroupsTests(ContactQueryTests):
    def test_locate_jsonl_terminal_matrix(self):
        f=self.f
        base=['--live-db-root',str(f.live_root),'--message-root',str(f.message_root),'--passphrase',str(f.passphrase),'--event-store',str(f.event_store_path),'--agent-store',str(f.agent_store_path),'--account-id','synthetic-account','--source-generation','synthetic-generation','--request','从 2023-11-14 22:13 到 2023-11-14 22:15','--timezone','UTC','--voice-jsonl','--encrypted-contact-database',str(f.message_path),'--locate-related-groups']
        target=['--contact-query','Target Remark','--include-related-groups']
        for extra in ([], ['--contact-query','Target Remark'], target+['--check-config'], target+['--voice-transcripts']):
            with self.subTest(extra=extra):
                out,err=io.StringIO(),io.StringIO()
                with contextlib.redirect_stdout(out),contextlib.redirect_stderr(err): code=runtime.main(base+extra)
                rows=[json.loads(line) for line in out.getvalue().splitlines()]
                self.assertEqual(code,2);self.assertEqual(len(rows),1);self.assertEqual(rows[0]['phase'],'complete')
        resolution={'status':'ambiguous','candidates':[]}
        out,err=io.StringIO(),io.StringIO()
        with patch.object(runtime.LiveIdentityAdapter,'resolve_contact',return_value=(resolution,None)),contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):
            code=runtime.main(base+target)
        rows=[json.loads(line) for line in out.getvalue().splitlines()]
        self.assertEqual(code,3);self.assertEqual(len(rows),1);self.assertEqual(rows[0]['phase'],'complete');self.assertEqual(rows[0]['status'],'ambiguous')
        original=runtime.CodexFirstRuntime._prepare_contact_scope
        def mutate(instance,window):
            original(instance,window)
            c=fixture._open_encrypted_writer(self.f.message_path)
            try:c.execute('INSERT INTO Name2Id(user_name) VALUES(?)',('synthetic-change',));c.commit()
            finally:c.close()
        out,err=io.StringIO(),io.StringIO()
        with patch.object(runtime.CodexFirstRuntime,'_prepare_contact_scope',new=mutate),contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):
            code=runtime.main(base+target)
        rows=[json.loads(line) for line in out.getvalue().splitlines()]
        self.assertEqual(code,2);self.assertEqual(len(rows),1);self.assertEqual(rows[0]['phase'],'complete');self.assertEqual(rows[0]['status'],'unavailable')

    def test_locate_does_not_consume_existing_pending_or_touch_agent_store(self):
        source=SourceKey('synthetic-account','synthetic-generation','message_0.db',table(GROUP2))
        with EventStore(self.f.event_store_path) as store:
            cat=LiveCatalog.dynamic_encrypted(self.f.message_root,fixture.PASSPHRASE,store=store,account_id='synthetic-account',source_generation='synthetic-generation');cat.close()
            msg=RawMessage(99,999,1,1700000005,10,b'private old pending',0)
            window=WindowIngestRun(datetime.fromtimestamp(1699999990,timezone.utc),datetime.fromtimestamp(1700000010,timezone.utc),{'source_min_time':1700000005,'source_max_time':1700000005,'examined_count':1})
            store.commit_window_messages(source,[msg],window)
            before=[tuple(row) for row in store._conn.execute('SELECT * FROM processing_outbox ORDER BY id')]
        with patch.object(runtime,'AgentStore',side_effect=AssertionError('agent store opened')),patch.object(runtime.CodexFirstRuntime,'_project',side_effect=AssertionError('projection called')):
            code,result,_=self.run_cli('--contact-query','Target Remark','--include-related-groups','--locate-related-groups')
        self.assertEqual(code,0);self.assertEqual(result['status'],'partial')
        with EventStore(self.f.event_store_path) as store:
            self.assertEqual([tuple(row) for row in store._conn.execute('SELECT * FROM processing_outbox ORDER BY id')],before)
            self.assertEqual(store._conn.execute('SELECT count(*) FROM outbox_consumer_state').fetchone()[0],0)
            self.assertEqual(store._conn.execute('SELECT count(*) FROM standard_event_materializations').fetchone()[0],0)

    def test_locate_jsonl_error_has_one_terminal(self):
        f=self.f;argv=['--live-db-root',str(f.live_root),'--message-root',str(f.message_root),'--passphrase',str(f.passphrase),'--event-store',str(f.event_store_path),'--agent-store',str(f.agent_store_path),'--account-id','synthetic-account','--source-generation','synthetic-generation','--request','从 2023-11-14 22:13 到 2023-11-14 22:15','--timezone','UTC','--voice-jsonl','--encrypted-contact-database',str(f.root/'missing.db'),'--contact-query','Target Remark','--include-related-groups','--locate-related-groups']
        out,err=io.StringIO(),io.StringIO()
        with contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):code=runtime.main(argv)
        rows=[json.loads(line) for line in out.getvalue().splitlines()]
        self.assertEqual(code,2);self.assertEqual(len(rows),1);self.assertEqual(rows[0]['phase'],'complete');self.assertEqual(rows[0]['status'],'unavailable')

    def test_locate_jsonl_with_missing_contact_has_one_terminal(self):
        out,err=io.StringIO(),io.StringIO()
        with contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):code=runtime.main(['--locate-related-groups','--voice-jsonl','--json'])
        rows=[json.loads(line) for line in out.getvalue().splitlines()]
        self.assertEqual(code,2);self.assertEqual(len(rows),1);self.assertEqual(rows[0]['phase'],'complete')

    def test_locate_jsonl_missing_target_has_one_terminal(self):
        out,err=io.StringIO(),io.StringIO()
        with contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):code=runtime.main(['--locate-related-groups','--voice-jsonl'])
        rows=[json.loads(line) for line in out.getvalue().splitlines()]
        self.assertEqual(code,2);self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]['phase'],'complete');self.assertEqual(rows[0]['status'],'unavailable')
        self.assertEqual(rows[0]['error_code'],'locate_requires_contact_and_groups')

    def test_locate_contact_change_rejected_without_body(self):
        contact=self.f.voice_path
        c=fixture._open_encrypted_writer(contact)
        try:c.execute('CREATE TABLE contact(username TEXT,alias TEXT,remark TEXT,nick_name TEXT)');c.execute('INSERT INTO contact VALUES(?,?,?,?)',(TARGET,'','Target Remark',''));c.commit()
        finally:c.close()
        original=runtime.LiveIdentityAdapter.resolve_contact
        def swap(adapter,*args,**kwargs):
            result=original(adapter,*args,**kwargs)
            replacement=contact.with_name('contact-replacement.db');shutil.copyfile(contact,replacement);os.replace(replacement,contact)
            return result
        with patch.object(runtime.LiveIdentityAdapter,'resolve_contact',new=swap),patch.object(runtime,'read_message_window',side_effect=AssertionError('body called')):
            code,result,_=self.run_cli('--contact-query','Target Remark','--include-related-groups','--locate-related-groups',contact_path=contact)
        self.assertEqual(code,2);self.assertEqual(result['status'],'unavailable');self.assertFalse(result.get('groups'))

    def test_locate_source_change_and_missing_or_duplicate_mapping_fail_closed(self):
        original=runtime.CodexFirstRuntime._prepare_contact_scope
        def mutate(instance,window):
            original(instance,window)
            c=fixture._open_encrypted_writer(self.f.message_path)
            try:c.execute('INSERT INTO Name2Id(user_name) VALUES(?)',('unrelated-synthetic',));c.commit()
            finally:c.close()
        with patch.object(runtime.CodexFirstRuntime,'_prepare_contact_scope',new=mutate),patch.object(runtime,'read_message_window',side_effect=AssertionError('body called')):
            code,result,_=self.run_cli('--contact-query','Target Remark','--include-related-groups','--locate-related-groups')
        self.assertEqual(code,2);self.assertEqual(result['status'],'unavailable');self.assertFalse(result.get('groups'))
        for mode in ('missing','duplicate'):
            c=fixture._open_encrypted_writer(self.f.message_path)
            try:
                c.execute('DELETE FROM Name2Id WHERE user_name=?',(TARGET,))
                if mode=='duplicate':c.executemany('INSERT INTO Name2Id(user_name) VALUES(?)',[(TARGET,),(TARGET,)])
                c.commit()
            finally:c.close()
            code,result,_=self.run_cli('--contact-query','Target Remark','--include-related-groups','--locate-related-groups')
            self.assertEqual(code,0);self.assertEqual(result['status'],'partial');self.assertIn('related_sender_unknown',result['gaps']);self.assertFalse(result['groups'])

    def test_locate_jsonl_has_one_terminal_and_no_media(self):
        f=self.f;argv=['--live-db-root',str(f.live_root),'--message-root',str(f.message_root),'--passphrase',str(f.passphrase),'--event-store',str(f.event_store_path),'--agent-store',str(f.agent_store_path),'--account-id','synthetic-account','--source-generation','synthetic-generation','--request','从 2023-11-14 22:13 到 2023-11-14 22:15','--timezone','UTC','--voice-jsonl','--encrypted-contact-database',str(f.message_path),'--contact-query','Target Remark','--include-related-groups','--locate-related-groups']
        out,err=io.StringIO(),io.StringIO()
        with contextlib.redirect_stdout(out),contextlib.redirect_stderr(err),patch.object(runtime,'read_message_window',side_effect=AssertionError('body called')):code=runtime.main(argv)
        rows=[json.loads(line) for line in out.getvalue().splitlines()]
        self.assertEqual(code,0);self.assertEqual(len(rows),1);self.assertEqual(rows[0]['phase'],'complete');self.assertEqual(rows[0]['status'],'partial')

    def test_locate_returns_verified_groups_without_reading_body_or_derived_stores(self):
        unrelated='locate-unrelated@chatroom';name=table(unrelated)
        c=fixture._open_encrypted_writer(self.f.message_path)
        try:
            c.execute('INSERT INTO Name2Id(rowid,user_name) VALUES(?,?)',(55,unrelated))
            c.execute('CREATE TABLE "'+name+'" (local_id INTEGER PRIMARY KEY,server_id INTEGER,local_type INTEGER,create_time INTEGER,real_sender_id INTEGER,message_content BLOB,WCDB_CT_message_content INTEGER,packed_info_data BLOB)')
            c.executemany('INSERT INTO "'+name+'" VALUES(?,?,?,?,?,?,?,?)',[(1,901,1,1700000000,10,b'other sender',0,None),(2,902,1,1699990000,9,b'outside window',0,None)])
            c.execute('INSERT INTO contact VALUES(?,?,?,?)',(GROUP2,'','Known Group',''));c.commit()
        finally:c.close()
        seen=[];opener=catalog.open_encrypted_readonly
        def open_source(*args,**kwargs):
            connection,key=opener(*args,**kwargs)
            def authorizer(action,table,column,db,trigger):
                if action==sql.SQLITE_READ and table and table.startswith('Msg_') and column in ('message_content','source','packed_info_data'):
                    seen.append(column);return sql.SQLITE_DENY
                return sql.SQLITE_OK
            connection.set_authorizer(authorizer);return connection,key
        with patch.object(catalog,'open_encrypted_readonly',new=open_source),patch.object(runtime,'AgentStore',side_effect=AssertionError('agent store opened')),patch.object(runtime.CodexFirstRuntime,'_project',side_effect=AssertionError('projection called')),patch.object(runtime,'read_message_window',side_effect=AssertionError('body called')):
            code,result,_=self.run_cli('--contact-query','Target Remark','--include-related-groups','--locate-related-groups')
        self.assertEqual(code,0);self.assertEqual(result['status'],'partial')
        self.assertIn('conversation_attribution_unverified',result['gaps'])
        groups={g['conversation_username']:g for g in result['groups']}
        self.assertIn(GROUP2,groups);self.assertEqual(groups[GROUP2]['display_name'],'Known Group')
        self.assertIn(fixture.GROUP,groups);self.assertIsNone(groups[fixture.GROUP]['display_name'])
        self.assertNotIn(unrelated,groups)
        self.assertFalse(seen)
        with EventStore(self.f.event_store_path) as store:
            self.assertEqual(store._conn.execute('SELECT count(*) FROM event_revisions').fetchone()[0],0)
            self.assertEqual(store._conn.execute('SELECT count(*) FROM current_window_baseline').fetchone()[0],0)
            self.assertEqual(store._conn.execute('SELECT count(*) FROM current_empty_window_coverage').fetchone()[0],0)

    def test_locate_rejects_media_and_requires_related_target(self):
        for args in (('--locate-related-groups',),('--contact-query','Target Remark','--locate-related-groups'),('--contact-query','Target Remark','--include-related-groups','--locate-related-groups','--voice-transcripts')):
            with self.subTest(args=args):
                code,result,_=self.run_cli(*args);self.assertEqual(code,2);self.assertFalse(result.get('groups'))
