"""Synthetic SQLCipher source and v1 EventStore migrate only selected messages."""
from datetime import datetime,timezone
import json,sqlite3,sys
from pathlib import Path
from unittest.mock import patch
import zstandard
from tests.test_contact_query import ContactQueryTests,TARGET,SELF,table
from tests import test_codex_first_voice_cli as fixture
from wechat_local.event_store import EventStore,SourceKey,RawMessage,WindowIngestRun
from wechat_local.agent_store import AgentStore
from wechat_local.event_search_projection import ProjectionWorker
from wechat_local.standard_event_repository import ProjectionProfile
from wechat_local import normalization_worker as norm
from wechat_local import event_normalizer
from wechat_local import reply_source_binding as binding
from wechat_local import live_catalog as catalog
from wechat_local import codex_first_runtime as runtime
from wechat_local.reply_workflow import PendingReply,confirm_reply
from test_system_template import TEMPLATE

class SystemCacheUpgradeTests(ContactQueryTests):
    def test_old_completed_materialization_is_rebuilt_once_and_old_binding_expires(self):
        f=self.f;packed=zstandard.ZstdCompressor().compress(TEMPLATE.encode())
        raw=RawMessage(70,701,10000,1700000004,9,packed,4)
        c=fixture._open_encrypted_writer(f.message_path)
        try:
            c.execute('INSERT INTO "'+table(TARGET)+'" VALUES (?,?,?,?,?,?,?,?)',(70,701,10000,1700000004,9,packed,4,None))
            c.execute('INSERT INTO "'+table(TARGET)+'" VALUES (?,?,?,?,?,?,?,?)',(71,702,10000,1700000005,9,b'<sysmsg><private>secret-not-display</private></sysmsg>',0,None))
            c.commit()
        finally:c.close()
        src=SourceKey('synthetic-account','synthetic-generation','message_0.db',table(TARGET))
        win=WindowIngestRun(datetime.fromtimestamp(1699999980,timezone.utc),datetime.fromtimestamp(1700000100,timezone.utc),{'source_min_time':1700000004,'source_max_time':1700000004,'examined_count':1})
        with EventStore(f.event_store_path) as store:
            store.record_catalog_refresh('synthetic-account','synthetic-generation',[('message_0.db',table(TARGET))]);store.commit_window_messages(src,[raw],win)
            with patch.object(norm,'NORMALIZER_VERSION','1'),patch.object(event_normalizer,'_system_profile_template',return_value=None):
                outcome=norm.NormalizationWorker(store,'legacy-synthetic').process_exact(store._exact_window_outboxes(src,[raw])[0]);self.assertEqual(outcome.outcome,'normalized')
            old=next(iter(store.standard_event_materializations_for_messages(src,[raw],contract_version='wechat-local.standard-event/1',normalizer_name='wechat-macos-v4',normalizer_version='1',_exact=True).values()))
            self.assertIsNone(json.loads(old.canonical_payload_json)['content']['text'])
            with AgentStore(f.agent_store_path) as agent:ProjectionWorker(None,agent,ProjectionProfile(normalizer_version='1')).run_exact([old])
        config=Path(f.root)/'synthetic-source.json'
        config.write_text(json.dumps({'runtime_python':sys.executable,'window_cli':str(Path(sys.executable).parent/'wechat-local-window'),'reply_cli':str(Path(sys.executable).parent/'wechat-local-reply'),'live_db_root':str(f.live_root.resolve()),'message_root':str(f.message_root.resolve()),'passphrase_file':str(f.passphrase.resolve()),'event_store':str(f.event_store_path.resolve()),'agent_store':str(f.agent_store_path.resolve()),'account_id':'synthetic-account','source_generation':'synthetic-generation','encrypted_contact_database':str(f.message_path.resolve())}));config.chmod(0o600)
        with patch.object(binding,'ProjectionProfile',return_value=ProjectionProfile(normalizer_version='1')):
            old_binding=binding.verify_reply_sources(str(config),[old.event_id],TARGET,'Target Remark')
        pending=PendingReply('synthetic-operation',TARGET,'Target Remark','Synthetic reply',str(config),(old.event_id,),old_binding)
        code,result,_=self.run_cli('--contact-query','Target Remark');self.assertEqual(code,0)
        refs=[x for g in result['groups'] for x in g['event_refs']];system=next(x for x in refs if x['event_id']==old.event_id)
        self.assertEqual(system['citation_text'],'"Alpha" scanned "Beta" and joined')
        self.assertIn('系统消息内容不可用',[x['citation_text'] for x in refs]);self.assertNotIn('secret-not-display',json.dumps(result))
        with self.assertRaisesRegex(binding.ReplySourceError,'source_binding_changed'):
            confirm_reply(pending,target_id=TARGET,target_label='Target Remark',body='Synthetic reply',confirmed=True)
        with sqlite3.connect(f.event_store_path) as db:
            versions=db.execute('SELECT normalizer_version,count(*) FROM standard_event_materializations WHERE event_id=? GROUP BY normalizer_version ORDER BY normalizer_version',(old.event_id,)).fetchall()
            self.assertEqual(versions,[('1',1),('2',1)])
        opened=[];opener=catalog.open_encrypted_readonly
        def counted(*args,**kwargs):
            opened.append(1);return opener(*args,**kwargs)
        with patch.object(catalog,'open_encrypted_readonly',side_effect=counted),patch.object(runtime,'open_encrypted_readonly',side_effect=counted),patch.object(norm.NormalizationWorker,'_process_claimed',side_effect=AssertionError('unexpected rematerialization')):
            code,second,_=self.run_cli('--contact-query','Target Remark')
        self.assertEqual(code,0);self.assertEqual(len(opened),1)
        self.assertEqual(next(x for g in second['groups'] for x in g['event_refs'] if x['event_id']==old.event_id)['citation_text'],system['citation_text'])
        with sqlite3.connect(f.event_store_path) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM standard_event_materializations WHERE event_id=?',(old.event_id,)).fetchone()[0],2)

class ProfileSearchCompatibilityTests(ContactQueryTests):
    def test_search_selects_current_profile_when_old_materialization_exists(self):
        from tests.test_association_conflict import AssociationConflictTests
        case=AssociationConflictTests(methodName='test_same_text_with_different_server_does_not_conflict');case.setUp()
        try:
            current=case.runtime();result=current.scan(case.window,display_limit=50);self.assertEqual(result.status,'ok')
            outboxes=[row[0] for row in case.f.event_store._conn.execute('SELECT id FROM processing_outbox')]
            with patch.object(norm,'NORMALIZER_VERSION','1'):
                worker=norm.NormalizationWorker(case.f.event_store,'synthetic-legacy')
                for oid in outboxes:self.assertEqual(worker.process_exact(oid).outcome,'normalized')
            page=current.search(case.window,query='fixture',limit=20)
            self.assertGreater(len(page.items),0)
        finally:case.tearDown()
