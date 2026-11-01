"""Real synthetic SQLCipher CLI exercises output precedence and terminal shape."""
import contextlib,io,json
from unittest.mock import patch
from tests.test_contact_query import ContactQueryTests,SELF
from tests import test_codex_first_voice_cli as fixture
from wechat_local import codex_first_runtime as runtime
from wechat_local import live_catalog as catalog

class OutputFormatResolutionCliTests(ContactQueryTests):
    def args(self):
        f=self.f
        return ['--live-db-root',str(f.live_root),'--message-root',str(f.message_root),'--passphrase',str(f.passphrase),'--event-store',str(f.event_store_path),'--agent-store',str(f.agent_store_path),'--account-id','synthetic-account','--source-generation','synthetic-generation','--self-username',SELF,'--request','从 2023-11-14 22:13 到 2023-11-14 22:15','--timezone','UTC','--encrypted-contact-database',str(f.message_path)]
    def invoke(self,extra):
        out,err=io.StringIO(),io.StringIO();opens=[];original=catalog.open_encrypted_readonly
        def counted(*args,**kwargs):opens.append(1);return original(*args,**kwargs)
        with patch.object(catalog,'open_encrypted_readonly',side_effect=counted),patch.object(runtime,'open_encrypted_readonly',side_effect=counted),contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):code=runtime.main(self.args()+extra)
        return code,[json.loads(line) for line in out.getvalue().splitlines()],err.getvalue(),len(opens)
    def test_query_both_flags_match_jsonl_and_base_precedes_complete(self):
        single=self.invoke(['--contact-query','Target Remark','--voice-transcripts','--voice-jsonl'])
        dual=self.invoke(['--contact-query','Target Remark','--voice-transcripts','--voice-jsonl','--json'])
        for code,rows,err,opens in (single,dual):
            self.assertEqual(code,0);self.assertEqual(opens,1);self.assertEqual([x['phase'] for x in rows],['messages','complete']);self.assertEqual(rows[-1]['status'],'ok')
        def ids(rows):return sorted(x['event_id'] for group in rows[0]['result']['groups'] for x in group['event_refs'])
        self.assertEqual(ids(single[1]),ids(dual[1]))
    def test_config_both_flags_is_single_json_ready(self):
        f=self.f;common=['--live-db-root',str(f.live_root),'--message-root',str(f.message_root),'--passphrase',str(f.passphrase),'--event-store',str(f.event_store_path),'--agent-store',str(f.agent_store_path),'--check-config','--json']
        results=[]
        for extra in ([],['--voice-jsonl']):
            out,err=io.StringIO(),io.StringIO()
            with contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):code=runtime.main(common+extra)
            self.assertEqual(code,0);rows=out.getvalue().splitlines();self.assertEqual(len(rows),1);results.append(json.loads(rows[0]))
        self.assertEqual(results[0],results[1]);self.assertEqual(results[0]['status'],'ready')
    def test_ambiguous_contact_yields_one_jsonl_complete(self):
        c=fixture._open_encrypted_writer(self.f.message_path)
        try:c.execute('UPDATE contact SET remark=? WHERE username=?',('Target Remark','synthetic-other'));c.commit()
        finally:c.close()
        code,rows,err,opens=self.invoke(['--contact-query','Target Remark','--voice-transcripts','--voice-jsonl','--json'])
        self.assertEqual(code,3);self.assertEqual(len(rows),1);self.assertEqual(rows[0]['phase'],'complete');self.assertEqual(rows[0]['status'],'ambiguous')
    def test_metadata_locator_both_flags_yields_one_jsonl_complete(self):
        code,rows,err,opens=self.invoke(['--contact-query','Target Remark','--include-related-groups','--locate-related-groups','--voice-jsonl','--json'])
        self.assertEqual(code,0);self.assertEqual(len(rows),1);self.assertEqual(rows[0]['phase'],'complete');self.assertEqual(rows[0]['status'],'partial')
    def test_single_json_remains_single_result(self):
        code,rows,err,opens=self.invoke(['--contact-query','Target Remark','--json'])
        self.assertEqual(code,0);self.assertEqual(len(rows),1);self.assertNotIn('phase',rows[0]);self.assertEqual(rows[0]['status'],'ok')

class OutputFormatVoiceTests(fixture.CodexFirstVoiceCliTests):
    def test_both_flags_emit_messages_then_voice_then_one_complete(self):
        args=['--live-db-root',str(self.live_root),'--message-root',str(self.message_root),'--passphrase',str(self.passphrase),'--event-store',str(self.event_store_path),'--agent-store',str(self.agent_store_path),'--account-id','synthetic-account','--source-generation','synthetic-generation','--request','从 2023-11-14 22:13 到 2023-11-14 22:15','--timezone','UTC','--voice-transcripts','--voice-database',str(self.voice_path),'--voice-jsonl','--json']
        out,err=io.StringIO(),io.StringIO()
        with contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):code=runtime.main(args)
        rows=[json.loads(line) for line in out.getvalue().splitlines()]
        self.assertEqual(code,0);self.assertEqual(rows[0]['phase'],'messages');self.assertEqual(rows[-1]['phase'],'complete')
        self.assertEqual(sum(x['phase']=='complete' for x in rows),1);self.assertGreater(sum(x['phase']=='voice' for x in rows),0)
