import sqlite3
import unittest
import os
from unittest.mock import patch

from test_codex_first_runtime import CodexFirstRuntimeTests, START, END
from wechat_local.codex_first import TimeWindow, CodexFirstError
from wechat_local.codex_first_runtime import CodexFirstRuntime
from wechat_local.event_store import SourceKey, RawMessage, WindowIngestRun, _content_fingerprint


class AssociationConflictTests(unittest.TestCase):
    def setUp(self):
        self.f = CodexFirstRuntimeTests(methodName='test_scan_ingests_only_requested_window_and_returns_grouped_event_refs')
        self.f.setUp()
        self.table = 'Msg_' + '1' * 32
        self.window = TimeWindow.from_utc(START, END)

    def tearDown(self):
        self.f.tearDown()

    def add_table(self, server=102, timestamp=1700000000, sender=7, content='fixture-one', local=2, kind=1):
        with sqlite3.connect(self.f.source_path) as conn:
            conn.execute(f'''CREATE TABLE "{self.table}" (local_id INTEGER PRIMARY KEY,server_id INTEGER,
                local_type INTEGER NOT NULL,create_time INTEGER,real_sender_id INTEGER,
                message_content BLOB,WCDB_CT_message_content INTEGER)''')
            conn.execute(f'INSERT INTO "{self.table}" VALUES (?,?,?,?,?,?,0)',(local,server,kind,timestamp,sender,content))

    def runtime(self, only=None):
        return CodexFirstRuntime(self.f.catalog,self.f.event_store,self.f.agent_store,
            account_id='synthetic-account',source_generation='synthetic-generation',
            capability_check=self.f._proven_capability,batch_size=1,
            source_filter=(lambda source:source.table_name==only) if only else None)

    def test_conflict_blocks_before_callback_and_single_source_reuse_and_search(self):
        self.add_table()
        callbacks=[]
        runtime=self.runtime()
        result=runtime.scan(self.window,display_limit=1,on_base_messages=callbacks.append)
        self.assertEqual((result.status,result.error_code),('unavailable','conversation_attribution_conflict'))
        self.assertIn('conversation_attribution_conflict',result.coverage.gaps)
        self.assertEqual(callbacks,[])
        self.assertEqual(self.f.event_store.count_events(),3)
        result=self.runtime(self.table).scan(self.window,display_limit=1)
        self.assertEqual(result.error_code,'conversation_attribution_conflict')
        with self.assertRaisesRegex(CodexFirstError,'conversation_attribution_conflict'):
            runtime.search(self.window,query='one',limit=1)

    def test_historical_counterpart_revision_still_blocks(self):
        self.add_table()
        runtime=self.runtime()
        runtime.scan(self.window,display_limit=1)
        with sqlite3.connect(self.f.source_path) as conn:
            conn.execute(f'UPDATE "{self.table}" SET message_content=? WHERE local_id=2',('changed',))
        self.runtime(self.table).scan(self.window,display_limit=1)
        result=self.runtime('Msg_'+'0'*32).scan(self.window,display_limit=1)
        self.assertEqual(result.error_code,'conversation_attribution_conflict')

    def test_unavailable_counterpart_retains_conflict(self):
        self.add_table()
        self.runtime().scan(self.window,display_limit=10)
        with sqlite3.connect(self.f.source_path) as conn:
            conn.execute(f'DROP TABLE "{self.table}"')
        result=self.runtime('Msg_'+'0'*32).scan(self.window,display_limit=1)
        self.assertEqual(result.error_code,'conversation_attribution_conflict')

    def test_same_text_with_different_server_does_not_conflict(self):
        self.add_table(server=999)
        self.assertEqual(self.runtime().scan(self.window,display_limit=10).status,'ok')

    def test_exception_does_not_emit_callback(self):
        self.add_table(server=999)
        callbacks=[]
        with patch.object(self.f.event_store,'_association_conflict',side_effect=sqlite3.DatabaseError('private content')):
            result=self.runtime().scan(self.window,display_limit=10,on_base_messages=callbacks.append)
        self.assertEqual(result.status,'unavailable')
        self.assertEqual(callbacks,[])

    def test_independent_fields_and_null_server_do_not_conflict(self):
        for kwargs in ({'server':None}, {'local':5}, {'kind':3},
                       {'timestamp':1700000001}, {'content':'other'}):
            with self.subTest(kwargs=kwargs):
                self.add_table(**kwargs)
                self.assertEqual(self.runtime().scan(self.window,display_limit=10).status,'ok')
                with sqlite3.connect(self.f.source_path) as conn:
                    conn.execute(f'DROP TABLE "{self.table}"')

    def test_store_change_during_check_and_source_change_fail_closed(self):
        self.add_table(server=999)
        original=self.f.event_store._association_conflict
        def change_store(candidates):
            result=original(candidates)
            self.f.event_store._conn.execute('UPDATE events SET first_seen_at=first_seen_at WHERE id=(SELECT min(id) FROM events)')
            return result
        with patch.object(self.f.event_store,'_association_conflict',side_effect=change_store):
            self.assertEqual(self.runtime().scan(self.window,display_limit=10).error_code,'conversation_attribution_unavailable')
        self.f.event_store._conn.rollback()
        def change_source(candidates):
            result=original(candidates)
            current=self.f.source_path.stat()
            os.utime(self.f.source_path,ns=(current.st_atime_ns,current.st_mtime_ns+1000000))
            return result
        with patch.object(self.f.event_store,'_association_conflict',side_effect=change_source):
            self.assertEqual(self.runtime().scan(self.window,display_limit=10).error_code,'conversation_attribution_unavailable')

    def test_existing_old_revision_reappearing_is_not_treated_as_current(self):
        runtime=self.runtime()
        self.assertEqual(runtime.scan(self.window,display_limit=10).status,'ok')
        with sqlite3.connect(self.f.source_path) as conn:
            conn.execute('UPDATE "Msg_'+'0'*32+'" SET message_content=? WHERE local_id=2',('changed',))
        self.assertEqual(runtime.scan(self.window,display_limit=10).status,'ok')
        with sqlite3.connect(self.f.source_path) as conn:
            conn.execute('UPDATE "Msg_'+'0'*32+'" SET message_content=? WHERE local_id=2',('fixture-one',))
        self.assertEqual(runtime.scan(self.window,display_limit=10).error_code,'conversation_attribution_unavailable')

    def test_no_base_or_media_callback_on_conflict(self):
        self.add_table(kind=3)
        with sqlite3.connect(self.f.source_path) as conn:
            conn.execute('UPDATE "Msg_'+'0'*32+'" SET local_type=3 WHERE local_id=2')
        calls=[]
        runtime=self.runtime()
        runtime.image_enricher=lambda *a: calls.append('enrich')
        result=runtime.scan(self.window,display_limit=10,on_base_messages=lambda *a:calls.append('base'),
                            on_image_result=lambda *a:calls.append('image'),on_voice_result=lambda *a:calls.append('voice'))
        self.assertEqual(result.error_code,'conversation_attribution_conflict')
        self.assertEqual(calls,[])

    def test_other_account_generation_and_shard_are_distinct_namespaces(self):
        self.assertEqual(self.runtime().scan(self.window,display_limit=10).status,'ok')
        msg=RawMessage(2,102,1,1700000000,7,'fixture-one',0)
        original=self.f.source
        candidates=[(original,2,102,1,1700000000,7,_content_fingerprint(msg))]
        variants=[('other-account','synthetic-generation','message_0.db'),
                  ('synthetic-account','other-generation','message_0.db'),
                  ('synthetic-account','synthetic-generation','message_1.db')]
        for account,generation,shard in variants:
            self.f.event_store.record_catalog_refresh(account,generation,[(shard,self.table)])
            source=SourceKey(account,generation,shard,self.table)
            self.f.event_store.commit_window_messages(source,[msg],WindowIngestRun(START,END,
                {'source_min_time':1700000000,'source_max_time':1700000000,'examined_count':1}))
        self.assertFalse(self.f.event_store._association_conflict(candidates))

    def test_unselected_shard_change_does_not_invalidate_candidate(self):
        other=self.f.message_root/'message_1.db'
        with sqlite3.connect(other) as conn:
            conn.execute('CREATE TABLE "Msg_'+'2'*32+'" (local_id INTEGER PRIMARY KEY,server_id INTEGER,local_type INTEGER,create_time INTEGER,real_sender_id INTEGER,message_content BLOB,WCDB_CT_message_content INTEGER)')
        original=self.f.event_store._association_conflict
        def change_unselected(candidates):
            result=original(candidates)
            s=other.stat()
            os.utime(other,ns=(s.st_atime_ns,s.st_mtime_ns+1000000))
            return result
        with patch.object(self.f.event_store,'_association_conflict',side_effect=change_unselected):
            result=self.runtime('Msg_'+'0'*32).scan(self.window,display_limit=10)
        self.assertEqual(result.status,'ok')


if __name__=='__main__': unittest.main()
