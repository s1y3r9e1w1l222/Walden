"""Public argument errors are reported before source configuration is touched."""
import contextlib
import io
import json
import unittest
from unittest.mock import patch
from wechat_local.codex_first_runtime import main

class OutputFormatConflictTests(unittest.TestCase):
    def test_conflicting_outputs_return_one_safe_argument_error_without_source_read(self):
        out=io.StringIO();err=io.StringIO()
        with patch('wechat_local.codex_first_runtime.apply_source_config_to_args',side_effect=AssertionError('config touched')),contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):
            code=main(['--source-config','/synthetic/nonexistent','--request','recent','--timezone','UTC','--voice-transcripts','--voice-jsonl','--json'])
        self.assertEqual(code,2);lines=out.getvalue().splitlines();self.assertEqual(len(lines),1)
        payload=json.loads(lines[0]);self.assertEqual(payload['status'],'unavailable');self.assertEqual(payload['error_code'],'invalid_arguments')
        self.assertEqual(payload['diagnostics']['failures'],[{'stage':'arguments','reason':'conflicting_output_formats'}])
        self.assertNotIn('/synthetic',out.getvalue()+err.getvalue())
