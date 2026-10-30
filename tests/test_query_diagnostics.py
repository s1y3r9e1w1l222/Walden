"""Controlled failures through the public query entry, using synthetic data."""
import contextlib,io,json,sqlite3,unittest
from unittest.mock import patch
from tests import test_codex_first_voice_cli as fixtures
import wechat_local.codex_first_runtime as runtime
from wechat_local.event_store import EventStore,EventStoreError,RawMessage,SourceKey,WindowIngestRun
from datetime import datetime,timezone
from wechat_local.live_readonly import LiveReadonlyError

class QueryDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.fixture=fixtures.CodexFirstVoiceCliTests();self.fixture.setUp();f=self.fixture
        self.argv=['--live-db-root',str(f.live_root),'--message-root',str(f.message_root),'--passphrase',str(f.passphrase),'--event-store',str(f.event_store_path),'--agent-store',str(f.agent_store_path),'--account-id','synthetic-account','--source-generation','synthetic-generation','--request','从 2023-11-14 22:13 到 2023-11-14 22:15','--timezone','UTC','--json']
    def tearDown(self):self.fixture.tearDown()
    def run_query(self):
        output=io.StringIO();err=io.StringIO()
        with contextlib.redirect_stdout(output),contextlib.redirect_stderr(err):code=runtime.main(self.argv)
        data=json.loads(output.getvalue());self.assertNotIn('PRIVATE_ERROR_SENTINEL',output.getvalue());self.assertNotIn('PRIVATE_ERROR_SENTINEL',err.getvalue());return code,data
    def test_missing_configuration_reports_configuration_stage(self):
        self.argv=['--source-config',str(self.fixture.root/'missing.json'),'--json']
        code,data=self.run_query()
        self.assertEqual((code,data['status'],data['error_code']),(2,'unavailable','configuration_unavailable'))
        self.assertEqual(data['diagnostics']['failures'],[{'stage':'configuration','reason':'configuration_error'}])
    def test_check_config_wrong_key_reports_source_open(self):
        self.fixture.passphrase.write_bytes(b'x'*32)
        self.argv=['--live-db-root',str(self.fixture.live_root),'--message-root',str(self.fixture.message_root),'--passphrase',str(self.fixture.passphrase),'--event-store',str(self.fixture.event_store_path),'--agent-store',str(self.fixture.agent_store_path),'--check-config','--json']
        code,data=self.run_query()
        self.assertEqual((code,data['status'],data['error_code']),(2,'unavailable','configuration_unavailable'))
        self.assertEqual(data['diagnostics']['failures'],[{'stage':'source_open','reason':'read_only_source_error'}])

    def test_check_config_correct_key_remains_ready(self):
        self.argv=['--live-db-root',str(self.fixture.live_root),'--message-root',str(self.fixture.message_root),'--passphrase',str(self.fixture.passphrase),'--event-store',str(self.fixture.event_store_path),'--agent-store',str(self.fixture.agent_store_path),'--check-config','--json']
        code,data=self.run_query()
        self.assertEqual((code,data['status'],data['query_only'],data['shard_count']),(0,'ready',True,1))
        self.assertNotIn('diagnostics',data)

    def test_late_voice_path_validation_is_configuration(self):
        self.argv+=['--voice-transcripts','--voice-database',str(self.fixture.root/'outside.db')]
        code,data=self.run_query()
        self.assertEqual((code,data['status'],data['error_code']),(2,'unavailable','configuration_unavailable'))
        self.assertEqual(data['diagnostics']['failures'],[{'stage':'configuration','reason':'read_only_source_error'}])

    def test_late_image_database_validation_is_configuration(self):
        self.argv+=['--image-database',str(self.fixture.root/'outside.db'),'--image-attachments-root',str(self.fixture.root),'--image-output-root',str(self.fixture.root/'out')]
        code,data=self.run_query()
        self.assertEqual((code,data['status'],data['error_code']),(2,'unavailable','configuration_unavailable'))
        self.assertEqual(data['diagnostics']['failures'],[{'stage':'configuration','reason':'read_only_source_error'}])

    def test_late_image_output_validation_is_configuration(self):
        self.argv+=['--image-database',str(self.fixture.voice_path),'--image-attachments-root',str(self.fixture.root),'--image-output-root',str(self.fixture.live_root/'out')]
        code,data=self.run_query()
        self.assertEqual((code,data['status'],data['error_code']),(2,'unavailable','configuration_unavailable'))
        self.assertEqual(data['diagnostics']['failures'],[{'stage':'configuration','reason':'read_only_source_error'}])

    def test_valid_late_media_configuration_restores_processing_stage(self):
        self.argv+=['--voice-transcripts','--voice-database',str(self.fixture.voice_path),'--image-database',str(self.fixture.voice_path),'--image-attachments-root',str(self.fixture.root),'--image-output-root',str(self.fixture.root/'out')]
        with patch.object(runtime.CodexFirstRuntime,'_project',side_effect=RuntimeError('PRIVATE_ERROR_SENTINEL')):
            code,data=self.run_query()
        self.assertEqual((code,data['status'],data['error_code']),(2,'unavailable','runtime_unavailable'))
        self.assertEqual(data['diagnostics']['failures'],[{'stage':'result_processing','reason':'unknown'}])

    def test_source_open_failure_keeps_safe_specific_stage(self):
        with patch.object(runtime.LiveCatalog,'dynamic_encrypted',side_effect=LiveReadonlyError('PRIVATE_ERROR_SENTINEL /private/path')):
            code,data=self.run_query()
        self.assertEqual((code,data['status'],data['error_code']),(2,'unavailable','configuration_unavailable'))
        self.assertEqual(data['diagnostics']['failures'],[{'stage':'source_open','reason':'read_only_source_error'}])
    def test_source_read_failure_keeps_existing_gap_and_reason(self):
        with patch.object(runtime,'read_message_window',side_effect=sqlite3.OperationalError('PRIVATE_ERROR_SENTINEL SQL text')):
            code,data=self.run_query()
        self.assertEqual((code,data['status']),(0,'unavailable'))
        self.assertIn('source_read_unavailable',data['gaps'])
        self.assertEqual(data['diagnostics']['failures'],[{'stage':'source_read','reason':'database_error'}])
    def test_plain_source_read_failure_reports_safe_stage(self):
        self.argv.remove('--json')
        output=io.StringIO();err=io.StringIO()
        with patch.object(runtime,'read_message_window',side_effect=sqlite3.OperationalError('PRIVATE_ERROR_SENTINEL SQL text')),contextlib.redirect_stdout(output),contextlib.redirect_stderr(err):
            code=runtime.main(self.argv)
        self.assertEqual(code,0)
        self.assertIn('source_read_unavailable',output.getvalue())
        self.assertIn('DIAGNOSTIC source_read database_error',output.getvalue())
        self.assertNotIn('PRIVATE_ERROR_SENTINEL',output.getvalue()+err.getvalue())
    def test_commit_failure_reports_processing_not_source_read(self):
        with patch.object(EventStore,'commit_window_messages',side_effect=EventStoreError('PRIVATE_ERROR_SENTINEL')):
            code,data=self.run_query()
        self.assertIn('source_read_unavailable',data['gaps'])
        self.assertEqual(data['diagnostics']['failures'],[{'stage':'result_processing','reason':'event_store_error'}])
    def test_projection_failure_reports_processing_and_unknown_never_leaks(self):
        with patch.object(runtime.CodexFirstRuntime,'_project',side_effect=RuntimeError('PRIVATE_ERROR_SENTINEL')):
            code,data=self.run_query()
        self.assertEqual((code,data['status'],data['error_code']),(2,'unavailable','runtime_unavailable'))
        self.assertEqual(data['diagnostics']['failures'],[{'stage':'result_processing','reason':'unknown'}])

    def test_voice_jsonl_reports_association_change_at_completion(self):
        self.argv.remove('--json')
        self.argv+=['--voice-transcripts','--voice-database',str(self.fixture.voice_path),'--voice-jsonl']
        original=runtime.CodexFirstRuntime.scan
        def scan(instance,*args,**kwargs):
            emit=kwargs['on_base_messages']
            def callback(result):
                emit(result)
                source=SourceKey('synthetic-account','synthetic-generation','message_0.db',fixtures.TABLE)
                start=datetime(2023,11,14,22,13,tzinfo=timezone.utc);end=datetime(2023,11,14,22,15,tzinfo=timezone.utc)
                instance.event_store.commit_window_messages(source,[RawMessage(1,101,34,1700000000,9,'changed synthetic data',0)],WindowIngestRun(start,end,{'source_min_time':1700000000,'source_max_time':1700000000,'examined_count':1}))
            kwargs['on_base_messages']=callback
            return original(instance,*args,**kwargs)
        output=io.StringIO()
        with patch.object(runtime.CodexFirstRuntime,'scan',new=scan),contextlib.redirect_stdout(output):code=runtime.main(self.argv)
        events=[json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(code,0);self.assertEqual(events[0]['phase'],'messages');self.assertEqual(events[-1]['phase'],'complete')
        self.assertFalse(any(event['phase']=='voice' for event in events))
        self.assertEqual(events[-1]['diagnostics']['failures'],[{'stage':'voice_association','reason':'state_changed'}])
