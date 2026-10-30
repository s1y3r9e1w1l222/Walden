import contextlib, errno, io, json, os, shutil, sqlite3, tempfile, unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import test_codex_first_voice_runtime as voice_fixture
import test_query_source_reuse as query_fixture
from wechat_local.codex_first import TimeWindow
import wechat_local.codex_first_runtime as rt
import wechat_local.image_runtime as im
import wechat_local.reply_workflow as reply

class FourFixTests(unittest.TestCase):
    def test_voice_reassociation_after_metadata_not_sent_to_asr(self):
        f=voice_fixture.CodexFirstVoiceRuntimeTests();f.setUp();self.addCleanup(f.tearDown)
        altered=b'\x02#!SILK_V3'+b'y'*32;changed=[]
        class ChangingVoice:
            def execute(self,sql,*args,**kwargs):
                if 'SELECT voice_data FROM VoiceInfo' in sql and not changed:
                    with sqlite3.connect(f.voice_path) as writer:
                        writer.execute('UPDATE VoiceInfo SET local_id=999,svr_id=999,create_time=1700000999,voice_data=?',(altered,))
                    changed.append(True)
                return f.voice.execute(sql,*args,**kwargs)
            def __getattr__(self,name):return getattr(f.voice,name)
        transcriber=voice_fixture._RecordingTranscriber();runtime=f.runtime(transcriber)
        runtime.voice_enricher.voice_connections=(ChangingVoice(),)
        result=runtime.scan(TimeWindow.from_utc(voice_fixture.START,voice_fixture.END),display_limit=50)
        self.assertTrue(changed);self.assertEqual(transcriber.calls,[])
        self.assertEqual(result.status,'ok')

    def test_reply_receipt_failure_after_action_never_retries(self):
        with tempfile.TemporaryDirectory() as tmp:
            state=Path(tmp)/'state';common=['--state-dir',str(state),'--target-id','synthetic-target','--target-label','Synthetic target','--body','synthetic body','--json'];calls=[]
            def runner(args,**kwargs):calls.append(args[3]);return SimpleNamespace(returncode=0,stdout='',stderr='')
            output=io.StringIO()
            with patch.object(reply,'verify_reply_sources',return_value='synthetic-binding'),contextlib.redirect_stdout(output):
                reply.reply_cli_main(['prepare',*common,'--source-config','/synthetic/config','--source-event-id','synthetic-event'],runner=runner)
            op=json.loads(output.getvalue())['operation_id']; output=io.StringIO()
            with patch.object(reply,'verify_reply_sources',return_value='synthetic-binding'),patch.object(reply,'_write_state',side_effect=OSError(errno.ENOSPC,'synthetic full')),contextlib.redirect_stdout(output):
                code=reply.reply_cli_main(['confirm',*common,'--operation-id',op,'--confirmed'],runner=runner)
            self.assertEqual(code,2);self.assertEqual(json.loads(output.getvalue()),{'status':'outcome_unknown','reason':'receipt_persistence_unavailable'})
            output=io.StringIO()
            with patch.object(reply,'verify_reply_sources',return_value='synthetic-binding'),contextlib.redirect_stdout(output):
                reply.reply_cli_main(['confirm',*common,'--operation-id',op,'--confirmed'],runner=runner)
            self.assertEqual(json.loads(output.getvalue())['reason'],'operation_already_attempted')
            self.assertEqual(calls,['connect','act','assert'])

    def test_jsonl_late_change_has_exactly_one_failure_terminal(self):
        g=query_fixture.QuerySourceReuseTests();g.setUp();self.addCleanup(g.tearDown)
        path=g.f.message_path;g.add_roles(path,voice=True)
        argv=['--live-db-root',str(g.f.live_root),'--message-root',str(g.f.message_root),'--passphrase',str(g.f.passphrase),'--event-store',str(g.f.event_store_path),'--agent-store',str(g.f.agent_store_path),'--account-id','synthetic-account','--source-generation','synthetic-generation','--request','从 2023-11-14 22:13 到 2023-11-14 22:15','--timezone','UTC','--voice-transcripts','--voice-jsonl','--image-database',str(path),'--image-attachments-root',str(g.attachments),'--image-output-root',str(g.output)]
        original=im.enrich_bound_images
        def swap(*args,**kwargs):
            result=original(*args,**kwargs);replacement=path.with_name('replacement.db');shutil.copyfile(path,replacement);os.replace(replacement,path);return result
        out=io.StringIO();err=io.StringIO()
        with patch.object(im,'enrich_bound_images',side_effect=swap),contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):code=rt.main(argv)
        lines=[json.loads(x) for x in out.getvalue().splitlines()]; complete=[x for x in lines if x.get('phase')=='complete']
        self.assertEqual(code,2);self.assertEqual(len(complete),1);self.assertIs(lines[-1],complete[0])
        self.assertEqual(complete[0]['status'],'unavailable');self.assertIn('diagnostics',complete[0]);self.assertIn('ERROR',err.getvalue())

    def test_jsonl_cleanup_failure_does_not_emit_success_terminal(self):
        g=query_fixture.QuerySourceReuseTests();g.setUp();self.addCleanup(g.tearDown)
        argv=['--live-db-root',str(g.f.live_root),'--message-root',str(g.f.message_root),'--passphrase',str(g.f.passphrase),'--event-store',str(g.f.event_store_path),'--agent-store',str(g.f.agent_store_path),'--account-id','synthetic-account','--source-generation','synthetic-generation','--request','从 2023-11-14 22:13 到 2023-11-14 22:15','--timezone','UTC','--voice-transcripts','--voice-jsonl']
        close=rt.LiveCatalog.close
        def fail_after_close(catalog):
            close(catalog);raise OSError('synthetic cleanup failure')
        out=io.StringIO();err=io.StringIO()
        with patch.object(rt.LiveCatalog,'close',new=fail_after_close),contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):code=rt.main(argv)
        lines=[json.loads(x) for x in out.getvalue().splitlines()];complete=[x for x in lines if x.get('phase')=='complete']
        self.assertEqual(code,2);self.assertEqual(len(complete),1);self.assertEqual(complete[0]['status'],'unavailable')
