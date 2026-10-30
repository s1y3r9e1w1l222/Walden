from __future__ import annotations

import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from wechat_local import source_setup
import wechat_local.codex_first_runtime as runtime


class InitialBaselineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.bin = self.root / 'bin'; self.bin.mkdir()
        for name in ('wechat-local-setup', 'wechat-local-window', 'wechat-local-reply'):
            path = self.bin / name; path.write_text('#!/bin/sh\nexit 0\n'); path.chmod(0o700)
        self.config = self.root / 'config.json'

    def run_setup(self, extra=(), status='ok', exit_code=0):
        output = io.StringIO(); calls = []
        def window(argv):
            calls.append(argv)
            print(json.dumps({'status': status, 'error_code': 'source_changed' if exit_code else None, 'coverage_status':'partial', 'gaps':[], 'groups':[{'private':'fixture body must not escape'}]}))
            return exit_code
        argv = [str(self.bin/'wechat-local-setup'), '--account-root', str(self.root), '--config', str(self.config), *extra]
        with patch.object(sys, 'argv', argv), patch.object(source_setup, 'prepare_account_source', return_value={'status':'configured'}), patch.object(runtime, 'main', window), contextlib.redirect_stdout(output):
            code = source_setup.main()
        self.assertNotIn('fixture body must not escape', output.getvalue())
        return code, json.loads(output.getvalue()), calls

    def test_default_window_and_three_independent_states(self):
        code, result, calls = self.run_setup()
        self.assertEqual(code, 0)
        self.assertEqual(calls[0][calls[0].index('--request')+1], '最近3小时')
        self.assertEqual(result['message_baseline']['status'], 'ready')
        self.assertEqual(result['message_baseline']['reuse_scope'], 'empty_sources_only')
        self.assertEqual(result['self_identity']['status'], 'incomplete')
        self.assertEqual(result['mute_status']['status'], 'incomplete')
        self.assertEqual(result['mute_status']['reason'], 'mute_coverage_incomplete')

    def test_custom_window_and_baseline_failure_keep_configured(self):
        code, result, calls = self.run_setup(('--baseline-request', '最近1小时', '--baseline-timezone', 'Asia/Shanghai'), 'unavailable', 2)
        self.assertEqual(code, 0)
        self.assertEqual(calls[0][calls[0].index('--request')+1], '最近1小时')
        self.assertEqual(calls[0][calls[0].index('--timezone')+1], 'Asia/Shanghai')
        self.assertEqual(result['status'], 'configured')
        self.assertEqual(result['message_baseline']['status'], 'incomplete')
        self.assertEqual(result['message_baseline']['reason'], 'source_changed')

    def test_identity_only_mode_never_runs_baseline(self):
        output = io.StringIO()
        argv = [str(self.bin/'wechat-local-setup'), '--config', str(self.config), '--confirm-self-username', 'fixture']
        with patch.object(sys, 'argv', argv), patch.object(source_setup, 'confirm_source_identity', return_value={'status':'self_identity_confirmed'}), patch.object(runtime, 'main') as window, contextlib.redirect_stdout(output):
            self.assertEqual(source_setup.main(), 0)
        window.assert_not_called()

    def test_missing_baseline_state_is_incomplete_without_exposing_messages(self):
        def window(_argv):
            print(json.dumps({'status':'ok','coverage_status':'partial','gaps':['baseline_state_unavailable'],'groups':[{'private':'not for output'}]}))
            return 0
        with patch.object(runtime, 'main', window):
            result = source_setup._initial_message_baseline(self.config, '最近3小时')
        self.assertEqual(result['status'], 'incomplete')
        self.assertIn('baseline_state_unavailable', result['gaps'])
        self.assertNotIn('not for output', str(result))

    def test_missing_or_invalid_timezone_rejected_before_acquisition_or_config(self):
        for extra in ((), ('--baseline-timezone', 'Invalid/Zone')):
            output=io.StringIO()
            argv=[str(self.bin/'wechat-local-setup'),'--account-root',str(self.root),'--acquire-passphrase','--baseline-request','从 2026-10-30 10:00 到 2026-10-30 11:00',*extra]
            with patch.object(sys,'argv',argv), patch.object(source_setup,'_preflight_capture_outputs') as preflight, patch.object(source_setup,'prepare_account_source') as prepare, contextlib.redirect_stdout(output):
                self.assertEqual(source_setup.main(),2)
            preflight.assert_not_called();prepare.assert_not_called()
            self.assertFalse(self.config.exists())

    def test_cancel_reports_saved_config_and_returns_130_without_traceback(self):
        output=io.StringIO()
        def configured(*args, **kwargs):
            self.config.write_text('{}');return {'status':'configured'}
        argv=[str(self.bin/'wechat-local-setup'),'--account-root',str(self.root),'--config',str(self.config)]
        with patch.object(sys,'argv',argv), patch.object(source_setup,'prepare_account_source',configured), patch.object(runtime,'main',side_effect=KeyboardInterrupt), contextlib.redirect_stdout(output):
            code=source_setup.main()
        self.assertEqual(code,130)
        self.assertTrue(self.config.exists())
        payload=json.loads(output.getvalue())
        self.assertEqual(payload['status'],'configured')
        self.assertEqual(payload['message_baseline']['status'],'incomplete')
        self.assertEqual(payload['message_baseline']['reason'],'cancelled')
        self.assertNotIn('Traceback',output.getvalue())


if __name__ == '__main__':
    unittest.main()
