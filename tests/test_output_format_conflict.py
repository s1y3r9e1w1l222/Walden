"""Dual output flags follow the requested streaming or configuration path."""
import contextlib,io,json,unittest
from unittest.mock import patch
from wechat_local import codex_first_runtime as runtime

class OutputFormatResolutionTests(unittest.TestCase):
    def test_query_dual_flags_normalize_to_jsonl_before_run(self):
        out=io.StringIO();err=io.StringIO();seen={}
        def run(args):
            seen.update(json=args.json,voice_jsonl=args.voice_jsonl);args._stream_completion={'phase':'complete','status':'ok'};return 0
        with patch.object(runtime,'_run_cli',side_effect=run),contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):
            code=runtime.main(['--json','--voice-jsonl','--voice-transcripts'])
        self.assertEqual(code,0);self.assertEqual(seen,{'json':False,'voice_jsonl':True});self.assertEqual(json.loads(out.getvalue()),{'phase':'complete','status':'ok'})
    def test_config_dual_flags_preserve_json_check_mode(self):
        out=io.StringIO();seen={}
        def run(args):
            seen.update(json=args.json,voice_jsonl=args.voice_jsonl);print(json.dumps({'status':'ready'}));return 0
        with patch.object(runtime,'_run_cli',side_effect=run),contextlib.redirect_stdout(out):code=runtime.main(['--check-config','--json','--voice-jsonl'])
        self.assertEqual(code,0);self.assertEqual(seen,{'json':True,'voice_jsonl':False});self.assertEqual(json.loads(out.getvalue())['status'],'ready')
    def test_source_failure_before_stream_has_one_jsonl_terminal_with_or_without_json(self):
        for extra in ([],['--json']):
            with self.subTest(extra=extra):
                out,err=io.StringIO(),io.StringIO()
                with contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):
                    code=runtime.main(['--source-config','/synthetic/not-found','--voice-transcripts','--voice-jsonl']+extra)
                rows=[json.loads(line) for line in out.getvalue().splitlines()]
                self.assertEqual(code,2);self.assertEqual(len(rows),1);self.assertEqual(rows[0]['phase'],'complete')
                self.assertEqual(rows[0]['status'],'unavailable');self.assertEqual(rows[0]['error_code'],'configuration_unavailable')
                self.assertNotIn('/synthetic',out.getvalue()+err.getvalue())
    def test_check_config_failure_keeps_original_single_json_diagnostics(self):
        for extra in ([],['--voice-jsonl']):
            with self.subTest(extra=extra):
                out,err=io.StringIO(),io.StringIO()
                with contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):
                    code=runtime.main(['--check-config','--source-config','/synthetic/not-found','--json']+extra)
                rows=[json.loads(line) for line in out.getvalue().splitlines()]
                self.assertEqual(code,2);self.assertEqual(len(rows),1);self.assertEqual(rows[0]['error_code'],'configuration_unavailable')
                self.assertNotIn('phase',rows[0]);self.assertNotIn('/synthetic',out.getvalue()+err.getvalue())
    def test_check_config_with_only_jsonl_retains_failure_behavior(self):
        out,err=io.StringIO(),io.StringIO()
        with contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):
            code=runtime.main(['--check-config','--source-config','/synthetic/not-found','--voice-transcripts','--voice-jsonl'])
        self.assertEqual(code,2);self.assertEqual(out.getvalue(),'');self.assertIn('configuration_unavailable',err.getvalue())
