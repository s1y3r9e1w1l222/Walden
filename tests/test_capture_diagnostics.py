"""Execute the generated driver; only the external LLDB API is a controlled fake."""
import ast
import json
import os
from pathlib import Path
import shlex
import stat
import sys
import tempfile
import types
import unittest
from unittest.mock import patch
from test_first_key_capture import load_renderer
from wechat_local import credential_capture, source_setup

SENTINEL = 'SECRET_ERROR_ACCOUNT_PATH_SENTINEL'
STAGES = ['target_create', 'environment', 'breakpoint', 'launch', 'breakpoint_hit', 'capture_write', 'target_termination']

# Also executed in a separate fake debugger process by the existing acquisition tests.
def execute_driver(script, payload=b'k'*32, fail=None, mutation=None, *, require_outer_run=False):
    # Only LLDB itself executes ordinary breakpoint commands in the triggering context.
    # Any Python memory-read callback is deliberately rejected by this boundary.
    commands = []; launched = [False]; capture_block = []; target_streams = {}
    class Result:
        ok = True
        command = ''
        def Succeeded(self): return self.ok
        def GetError(self):
            return ('error: memory read failed: synthetic access denied ' if self.command.startswith('memory read')
                    else 'error: command failed: synthetic access denied ') + SENTINEL
        def GetOutput(self): raise AssertionError('memory/command output must not be read')
    class Thread:
        def __init__(self, index): self.index = index
        def IsValid(self): return True
        def GetStopReason(self):
            if mutation == 'signal_stop' or fail == 'breakpoint_hit': return 5
            if mutation == 'non_target_stop': return 3
            if mutation == 'selected_thread_differs' and self.index == 0: return 5
            return 3
        def GetStopReasonDataCount(self): return 1 if self.GetStopReason() == 5 else 2
        def GetStopReasonDataAtIndex(self, index):
            if self.GetStopReason() == 5: return 11
            return (2 if mutation == 'non_target_stop' else 1) if index == 0 else 7
    class Process:
        state = 5
        def IsValid(self): return launched[0]
        def GetProcessID(self): return (2147483000 if mutation == 'attach_failure' else os.getpid() + 10000) if launched[0] else 0
        def GetState(self): return self.state
        def GetBroadcaster(self): return self
        def GetExitStatus(self): return -1 if mutation == 'attach_failure' else 9
        def GetExitDescription(self): return ('attach failed (attached to process, but could not pause execution; attach failed)' if mutation == 'attach_failure' else 'process exited with status 9 ' + SENTINEL)
        def GetSelectedThread(self): raise AssertionError('selected thread is not the triggering execution context')
        def GetNumThreads(self): return 2 if mutation == 'selected_thread_differs' else 1
        def GetThreadAtIndex(self, index): return Thread(index)
        def Kill(self):
            result = types.SimpleNamespace(Success=lambda: fail != 'target_termination', GetCString=lambda: 'synthetic kill denied /private/secret 01 23 45 67 89 ab cd ef')
            if mutation == 'unknown_state': self.state = 42
            elif fail != 'target_termination' and mutation not in ('delayed_exit', 'never_exit'): self.state = 10
            return result
    process = Process()
    class Breakpoint:
        def IsValid(self): return fail != 'breakpoint'
        def GetID(self): return 1
    class Target:
        def IsValid(self): return fail != 'target_create'
        def GetProcess(self): return process
        def GetNumBreakpoints(self): return 1
        def GetBreakpointAtIndex(self, index): return Breakpoint()
    def apply_command(command, result, ordinary=False):
        commands.append(('outer:' if ordinary else '') + command)
        result.command = command
        for setting in ('output-path', 'error-path'):
            if command == 'settings set target.' + setting + ' /dev/null':
                if fail != setting:
                    target_streams[setting] = '/dev/null'
                else:
                    result.ok = False
                    return
        if mutation == 'byte_error':
            result.GetError = lambda: 'error: synthetic access denied 01 23 45 67 89 ab cd ef'
        if command.startswith('breakpoint command add -F'):
            raise AssertionError('Python capture callbacks must not replace ordinary breakpoint commands')
        stage = ('target_create' if command.startswith('target create') else
                 'environment' if 'target.env-vars' in command else
                 'breakpoint' if command.startswith('breakpoint set') else
                 'capture_write' if command.startswith('memory read') else None)
        result.ok = fail is None or stage != fail
        if mutation == 'exception' and stage == fail:
            raise RuntimeError('first line\nsynthetic access denied /private/secret/account ' + SENTINEL)
        if stage == 'capture_write':
            assert ordinary, 'memory read must run in the ordinary triggering context'
            if result.ok:
                tokens = shlex.split(command)
                out = Path(tokens[tokens.index('--outfile') + 1])
                assert tokens[:4] == ['memory','read','--force','--outfile']
                assert tokens[-6:] == ['--binary','--size','1','--count','32','$rsi']
                if mutation == 'replace_capture':
                    replacement = out.with_suffix('.replacement')
                    replacement.write_bytes(payload); replacement.chmod(0o600); os.replace(replacement, out)
                elif mutation == 'fifo_capture':
                    out.unlink(); os.mkfifo(out, 0o600)
                else: out.write_bytes(payload)
    class Interpreter:
        def HandleCommand(self, command, result): apply_command(command, result)
    class Listener:
        def WaitForEventForBroadcasterWithType(self, timeout, broadcaster, mask, event):
            assert broadcaster is process and mask == 1
            if mutation == 'delayed_exit':
                process.state = 10
                return True
            import time
            time.sleep(timeout)
            return False
    class Debugger:
        def GetListener(self): return Listener()
        def SetAsync(self, value): assert value is False
        def GetCommandInterpreter(self): return Interpreter()
        def GetSelectedTarget(self): return Target()
    module = types.SimpleNamespace(debugger=Debugger(), SBCommandReturnObject=Result,
        eStateStopped=5,eStateRunning=6,eStateExited=10,eStateCrashed=8,eStateDetached=9,
        eStopReasonBreakpoint=3,eStopReasonSignal=5, SBEvent=lambda: object(),
        SBProcess=types.SimpleNamespace(eBroadcastBitStateChanged=1))
    namespace = {}
    def python_command(line):
        exec(compile(line[7:], '<generated-capture-source>', 'exec'), namespace)
    def outer_run():
        commands.append('outer:run')
        if mutation == 'sleep':
            import time
            time.sleep(30)
        launched[0] = True
        if mutation == 'target_output':
            if target_streams.get('output-path') != '/dev/null': print('error: chat body: meet at station at 9')
            if target_streams.get('error-path') != '/dev/null': print('error: key bytes: 01 23 45 67 89 ab cd ef')
        if fail == 'launch' or mutation in ('unexpected_exit', 'attach_failure'): process.state = 10
        elif mutation == 'crashed_target': process.state = 8
        elif mutation not in ('signal_stop','non_target_stop') and fail != 'breakpoint_hit':
            assert capture_block, 'ordinary breakpoint command block is missing'
            for line in capture_block:
                try:
                    if line.startswith('script '): python_command(line)
                    else:
                        result=Result(); apply_command(line,result,ordinary=True)
                        if not result.Succeeded():
                            print(result.GetError())
                            break
                except Exception as error:
                    print(type(error).__name__ + ': ' + str(error))
                    break
    with patch.dict(sys.modules, {'lldb': module}):
        collecting = False
        try:
            for line in Path(script).read_text().splitlines():
                if collecting:
                    if line == 'DONE': collecting = False
                    else: capture_block.append(line)
                elif line == 'breakpoint command add 1': collecting = True
                elif line.startswith('script '): python_command(line)
                elif line == 'run': outer_run()
                elif line: raise AssertionError('unexpected source command: ' + line)
        except RuntimeError:
            pass  # ordinary command source stops at its first error
        exec('_capture_session.finish()', namespace)  # queued stdin, including on source failure
    return commands

class DiagnosticDriverTests(unittest.TestCase):
    def render(self, root):
        output = root/'capture.bin'; output.touch(mode=0o600)
        diagnostic = root/'diagnostic.json'; diagnostic.touch(mode=0o600)
        script = root/'capture.lldb'
        renderer = load_renderer()
        self.assertTrue(hasattr(renderer, 'render_diagnostic'), 'actual-stage driver is missing')
        script.write_text(renderer.render_diagnostic(root/'app', root/'home', output, diagnostic, 'a'*64))
        return script, diagnostic

    def test_mixed_output_never_promotes_app_log_or_byte_dump_to_debugger_error(self):
        read_fd, write_fd = os.pipe()
        reader = credential_capture._DebuggerErrors(os.fdopen(read_fd, 'rb'))
        os.write(write_fd, (b'error: chat body: meet at station at 9\n'
                            b'error: key bytes: 01 23 45 67 89 ab cd ef 10 32 54 76 98 ba dc fe\n'))
        os.close(write_fd)
        reader.close()
        self.assertEqual(reader.errors, [])

    def test_generated_commands_isolate_target_streams_before_run(self):
        from contextlib import redirect_stdout
        from io import StringIO
        with tempfile.TemporaryDirectory() as tmp:
            script, diagnostic = self.render(Path(tmp))
            output = StringIO()
            with redirect_stdout(output):
                commands = execute_driver(script, mutation='target_output')
            self.assertEqual(output.getvalue(), '')
            run = commands.index('outer:run')
            for setting in ('output-path', 'error-path'):
                self.assertLess(commands.index('settings set target.' + setting + ' /dev/null'), run)

    def test_failed_target_stream_isolation_stops_before_run(self):
        for setting in ('output-path', 'error-path'):
            with self.subTest(setting=setting), tempfile.TemporaryDirectory() as tmp:
                script, diagnostic = self.render(Path(tmp))
                commands = execute_driver(script, fail=setting)
                result = json.loads(diagnostic.read_text())
                self.assertEqual(result['failed_stage'], 'environment')
                self.assertNotIn('outer:run', commands)
                self.assertNotIn('outer:memory read', commands)

    def test_trusted_command_error_redacts_separated_bytes_but_keeps_reason(self):
        with tempfile.TemporaryDirectory() as tmp:
            script, diagnostic = self.render(Path(tmp))
            execute_driver(script, fail='target_create', mutation='byte_error')
            result = json.loads(diagnostic.read_text())
            self.assertIn('synthetic access denied', result['command_error'])
            self.assertNotIn('01 23 45', result['command_error'])
            self.assertIn('[redacted]', result['command_error'])

    def test_generated_driver_records_actual_stage_results_and_no_raw_errors(self):
        for failed in [None] + STAGES:
            with self.subTest(failed=failed), tempfile.TemporaryDirectory() as tmp:
                script, diagnostic = self.render(Path(tmp))
                execute_driver(script, fail=failed, require_outer_run=True)
                result = json.loads(diagnostic.read_text())
                self.assertEqual(result['failed_stage'], failed)
                if failed is None:
                    self.assertEqual(result['last_completed'], 'target_termination')
                    self.assertEqual(result['target_exit'], 'intentional')
                    self.assertTrue(all(result['stages'][s] == 'completed' for s in STAGES))
                else:
                    self.assertEqual(result['stages'][failed], 'failed')
                self.assertNotIn(SENTINEL, diagnostic.read_text())

    def test_termination_confirms_delayed_bound_process_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            script, diagnostic = self.render(Path(tmp))
            execute_driver(script, mutation='delayed_exit')
            result = json.loads(diagnostic.read_text())
            self.assertEqual(result['stages']['target_termination'], 'completed')
            self.assertEqual(result['target_state'], 'exited')
            self.assertEqual(result['target_exit'], 'intentional')

    def test_kill_error_preserves_reason_and_state_without_confirming(self):
        with tempfile.TemporaryDirectory() as tmp:
            script, diagnostic = self.render(Path(tmp))
            execute_driver(script, fail='target_termination')
            result = json.loads(diagnostic.read_text())
            self.assertEqual(result['stages']['target_termination'], 'failed')
            self.assertEqual(result['target_state'], 'stopped')
            self.assertEqual(result['target_exit'], 'unknown')
            self.assertIn('synthetic kill denied', result['command_error'])
            self.assertIn('state=5', result['command_error'])
            self.assertNotIn('01 23 45', result['command_error'])
            self.assertNotIn('/private/secret', result['command_error'])

    def test_unconfirmed_exit_stays_failed_with_last_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            script, diagnostic = self.render(Path(tmp))
            execute_driver(script, mutation='never_exit')
            result = json.loads(diagnostic.read_text())
            self.assertEqual(result['stages']['target_termination'], 'failed')
            self.assertEqual(result['target_state'], 'stopped')
            self.assertEqual(result['target_exit'], 'unknown')
            self.assertIn('termination unconfirmed', result['command_error'])
            self.assertIn('state=5', result['command_error'])

    def test_unrecognized_state_number_is_retained_without_confirmation(self):
        with tempfile.TemporaryDirectory() as tmp:
            script, diagnostic = self.render(Path(tmp))
            execute_driver(script, mutation='unknown_state')
            result = json.loads(diagnostic.read_text())
            self.assertEqual(result['stages']['target_termination'], 'failed')
            self.assertEqual(result['target_state'], 'unknown')
            self.assertEqual(result['target_exit'], 'unknown')
            self.assertIn('state=42', result['command_error'])

    def test_exception_preserves_failure_even_after_target_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp:
            script, diagnostic = self.render(Path(tmp))
            execute_driver(script, fail='capture_write', mutation='exception')
            result=json.loads(diagnostic.read_text())
            self.assertEqual(result['failed_stage'], 'capture_write')
            self.assertEqual(result['last_completed'], 'target_termination')
            self.assertNotIn(SENTINEL, diagnostic.read_text())

    def test_setup_exception_preserves_sanitized_final_line(self):
        with tempfile.TemporaryDirectory() as tmp:
            script, diagnostic = self.render(Path(tmp))
            execute_driver(script, fail='target_create', mutation='exception')
            result = json.loads(diagnostic.read_text())
            self.assertIn('RuntimeError: synthetic access denied', result['command_error'])
            self.assertNotIn('first line', result['command_error'])
            self.assertNotIn('/private/secret/account', diagnostic.read_text())
            self.assertNotIn(SENTINEL, diagnostic.read_text())

    def test_unexpected_exit_does_not_claim_intentional_kill_or_capture(self):
        with tempfile.TemporaryDirectory() as tmp:
            script, diagnostic = self.render(Path(tmp))
            execute_driver(script, mutation='unexpected_exit')
            result=json.loads(diagnostic.read_text())
            self.assertEqual(result['target_exit'], 'unexpected')
            self.assertEqual(result['failed_stage'], 'launch')
            self.assertEqual((Path(tmp)/'capture.bin').stat().st_size,0)


    def test_observed_crash_survives_subsequent_target_cleanup(self):
        for cleanup_failure in (None, 'target_termination'):
            with self.subTest(cleanup_failure=cleanup_failure), tempfile.TemporaryDirectory() as tmp:
                script, diagnostic = self.render(Path(tmp))
                execute_driver(script, fail=cleanup_failure, mutation='crashed_target')
                result=json.loads(diagnostic.read_text())
                self.assertEqual(result['target_exit'], 'unexpected')
                self.assertEqual(result['failed_stage'], 'launch')
                self.assertEqual(result['stages']['capture_write'], 'not_started')
                self.assertEqual(result['stages']['target_termination'], 'completed' if cleanup_failure is None else 'failed')
                self.assertEqual(result['target_state'], 'exited' if cleanup_failure is None else 'crashed')
                self.assertEqual((Path(tmp)/'capture.bin').stat().st_size, 0)

    def test_outer_launch_error_preserves_exit_facts_and_stops_capture(self):
        with tempfile.TemporaryDirectory() as tmp:
            script, diagnostic = self.render(Path(tmp))
            commands = execute_driver(script, fail='launch', require_outer_run=True)
            result = json.loads(diagnostic.read_text())
            self.assertEqual(result['failed_stage'], 'launch')
            self.assertEqual(result['target_exit'], 'unexpected')
            self.assertEqual(result['target_exit_status'], 9)
            self.assertIn('status 9', result['target_exit_description'])
            self.assertNotIn(SENTINEL, diagnostic.read_text())
            self.assertFalse(any(c.startswith('memory read') for c in commands))
            self.assertNotIn('run', commands, 'ordinary run must not re-enter HandleCommand')


    def test_plain_commands_use_trigger_context_when_selected_thread_differs(self):
        with tempfile.TemporaryDirectory() as tmp:
            script, diagnostic = self.render(Path(tmp))
            commands = execute_driver(script, mutation='selected_thread_differs', require_outer_run=True)
            result = json.loads(diagnostic.read_text())
            self.assertEqual((Path(tmp)/'capture.bin').read_bytes(), b'k'*32)
            self.assertEqual(result['breakpoint_commands'], 'completed')
            self.assertTrue(any(c.startswith('outer:memory read ') for c in commands))
            self.assertIn('outer:breakpoint disable 1', commands)
            self.assertTrue(all(result['stages'][s] == 'completed' for s in STAGES))
            self.assertEqual(result['stop_facts'][1]['breakpoint_id'], 1)
            self.assertEqual(result['stop_facts'][1]['location_id'], 7)

    def test_non_target_and_signal_stops_are_recorded_before_kill(self):
        for mutation in ('signal_stop','non_target_stop'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmp:
                script, diagnostic = self.render(Path(tmp))
                commands = execute_driver(script, mutation=mutation, require_outer_run=True)
                result=json.loads(diagnostic.read_text())
                self.assertEqual(result['breakpoint_commands'], 'not_entered')
                self.assertEqual(result['failure_detail'], 'breakpoint_commands_not_entered')
                self.assertEqual(result['stop_source'], 'run_return')
                self.assertEqual(result['stop_state'], 'stopped')
                self.assertEqual(result['target_state'], 'exited')
                fact=result['stop_facts'][0]
                self.assertEqual(fact['signal'], 11 if mutation == 'signal_stop' else None)
                self.assertEqual(fact['breakpoint_id'], 2 if mutation == 'non_target_stop' else None)
                self.assertFalse(any('memory read' in c for c in commands))

    def test_ordinary_capture_command_failure_is_distinct_from_never_entering(self):
        with tempfile.TemporaryDirectory() as tmp:
            script, diagnostic = self.render(Path(tmp))
            commands=execute_driver(script, fail='capture_write', require_outer_run=True)
            result=json.loads(diagnostic.read_text())
            self.assertEqual(result['breakpoint_commands'], 'failed')
            self.assertEqual(result['failure_detail'], 'capture_commands_incomplete')
            self.assertEqual(result['failed_stage'], 'capture_write')
            self.assertFalse(any(c == 'outer:breakpoint disable 1' for c in commands))


class DiagnosticReaderTests(unittest.TestCase):
    def test_valid_channel_is_whitelisted_and_rejects_corruption_identity_and_forgery(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); script, diagnostic=DiagnosticDriverTests().render(root)
            execute_driver(script)
            original=diagnostic.read_bytes(); info=diagnostic.stat(); identity=(info.st_dev,info.st_ino)
            self.assertTrue(hasattr(credential_capture,'_read_capture_diagnostic'), 'bounded parser missing')
            read=lambda: credential_capture._read_capture_diagnostic(diagnostic,identity,'a'*64,os.getpid())
            valid=read(); self.assertEqual(valid['channel'],'valid'); self.assertNotIn('token',valid)
            value=json.loads(original)
            for key,bad in [('token','b'*64),('debugger_pid',os.getpid()+1),('elapsed_ms',True),('elapsed_ms',-1),('target_pid',-1),('failed_stage',SENTINEL),('extra',SENTINEL),('last_started','target_create'),('last_completed','target_create'),('stages',dict(value['stages'],breakpoint='not_started'))]:
                with self.subTest(key=key,bad=bad):
                    changed=dict(value); changed[key]=bad;diagnostic.write_text(json.dumps(changed))
                    self.assertEqual(read()['channel'],'invalid')
                    self.assertNotIn(SENTINEL,json.dumps(read()))
            for bad in (b'',b'{',b'x'*4097,b'['*2000+b']'*2000):
                diagnostic.write_bytes(bad);self.assertEqual(read()['channel'],'invalid')
            diagnostic.write_bytes(original);diagnostic.chmod(0o644);self.assertEqual(read()['channel'],'invalid')
            diagnostic.chmod(0o600); replacement=root/'replacement'; replacement.write_bytes(original);replacement.chmod(0o600);os.replace(replacement,diagnostic)
            self.assertEqual(read()['channel'],'invalid')
            diagnostic.unlink();diagnostic.symlink_to(replacement);self.assertEqual(read()['channel'],'invalid')

import test_first_key_capture as capture_tests
class DiagnosticCaptureTests(unittest.TestCase):
    setUp = capture_tests.FirstKeyCaptureTests.setUp
    tearDown = capture_tests.FirstKeyCaptureTests.tearDown
    _make_db = staticmethod(capture_tests.FirstKeyCaptureTests._make_db)
    _write_debugger = capture_tests.FirstKeyCaptureTests._write_debugger
    _run = capture_tests.FirstKeyCaptureTests._run
    _run_setup = capture_tests.FirstKeyCaptureTests._run_setup
    # Reuse real account/SQLCipher fixtures, not inherited test cases.
    def test_nonzero_preserves_code_without_reading_capture(self):
        self._write_debugger(self.key,7)
        with patch.object(credential_capture,'_verify_all_shards',side_effect=AssertionError('must not validate')):
            result=self._run()
        self.assertEqual(result['status'],'debugger_failed')
        self.assertIn('diagnostics',result,'exit diagnostics missing')
        d=result['diagnostics'];self.assertEqual(d['debugger_returncode'],7)
        self.assertEqual(d['last_completed'],'target_termination')
        self.assertEqual(d['validation'],'not_started')
        self.assertFalse(self.config.with_name('passphrase.bin').exists())

    def test_python_exception_preserves_sanitized_final_line_and_stops(self):
        with patch.object(credential_capture, '_verify_all_shards',
                          side_effect=RuntimeError('earlier line\nsynthetic verification failure /private/secret/account ' + SENTINEL)):
            result = self._run()
        self.assertNotEqual(result['status'], 'captured_and_verified')
        detail = result['diagnostics']['python_error']
        self.assertIn('RuntimeError: synthetic verification failure', detail)
        self.assertNotIn('earlier line', detail)
        self.assertNotIn('/private/secret/account', detail)
        self.assertNotIn(SENTINEL, detail)
        self.assertFalse(self.config.with_name('passphrase.bin').exists())

    def test_isolated_debugger_error_keeps_reason_and_removes_byte_dump(self):
        code = self.lldb.read_text().replace('execute_driver(script, payload=',
              "execute_driver(script, fail='capture_write', mutation='byte_error', payload=")
        code = code.replace('mutation=mutation, require_outer_run=True', 'require_outer_run=True')
        self.lldb.write_text(code)
        result = self._run()
        self.assertEqual(result['status'], 'capture_stage_failed')
        self.assertEqual(result['diagnostics']['failure_detail'], 'capture_commands_incomplete')
        errors = result['diagnostics']['debugger_errors']
        self.assertTrue(any('synthetic access denied' in line for line in errors))
        self.assertNotIn('01 23 45', json.dumps(result))
        self.assertFalse(self.config.with_name('passphrase.bin').exists())

    def test_missing_diagnostic_never_creates_readiness(self):
        self.lldb.write_text('#!/bin/sh\nexit 0\n');self.lldb.chmod(0o700)
        result=self._run()
        self.assertEqual(result['status'],'capture_diagnostic_invalid')
        self.assertIn(result['diagnostics']['channel'],('missing','invalid'))
        self.assertFalse(self.config.exists())

    def test_signal_exit_is_reported_and_group_cleanup_does_not_prove_target_exit(self):
        self.lldb.write_text(f'#!{sys.executable}\nimport os,signal\nos.kill(os.getpid(),signal.SIGTERM)\n');self.lldb.chmod(0o700)
        result=self._run();self.assertIn('diagnostics',result,'signal diagnostics missing')
        d=result['diagnostics'];self.assertEqual(d['debugger_signal'],15)
        self.assertEqual(d['group_cleanup'],'confirmed')
        self.assertEqual(d['target_exit'],'unknown')

    def test_setup_propagates_validation_persistence_and_configuration(self):
        code, output = self._run_setup()
        self.assertEqual(code,0)
        result=json.loads(output)
        self.assertIn('diagnostics',result,'configuration stage missing')
        self.assertEqual(result['diagnostics']['validation'],'completed')
        self.assertEqual(result['diagnostics']['persistence'],'completed')
        self.assertEqual(result['diagnostics']['configuration'],'completed')

    def test_setup_failure_marks_configuration_failed_and_removes_owned_key(self):
        with patch.object(source_setup,'prepare_account_source',side_effect=OSError(SENTINEL)):
            code, output=self._run_setup()
        self.assertEqual(code,2)
        result=json.loads(output)
        self.assertIn('diagnostics',result,'configuration failure stage missing')
        self.assertEqual(result['diagnostics']['configuration'],'failed')
        self.assertFalse(self.config.with_name('passphrase.bin').exists())
        self.assertNotIn(SENTINEL,output)

    def test_timeout_in_actual_launch_leaves_last_started_and_target_unknown(self):
        text=self.lldb.read_text().replace('mutation=mutation, require_outer_run=True)', "mutation='sleep', require_outer_run=True)")
        self.lldb.write_text(text)
        before=set(Path(tempfile.gettempdir()).glob('wechat-local-capture-*'))
        try:
            result=self._run(timeout=2.0)
            d=result['diagnostics']
            self.assertEqual(d['last_started'],'launch')
            self.assertEqual(d['last_completed'],'breakpoint')
            self.assertIsNone(d['target_pid'])
            self.assertTrue(d['debugger_timeout'])
            self.assertEqual(d['target_cleanup'],'unknown')
            self.assertEqual(d['group_cleanup'],'confirmed')
            self.assertFalse(self.config.with_name('passphrase.bin').exists())
        finally:
            import shutil
            for stage in set(Path(tempfile.gettempdir()).glob('wechat-local-capture-*'))-before:
                shutil.rmtree(stage)

    def test_cleanup_uncertainty_preserves_exit_and_prevents_value_processing(self):
        before=set(Path(tempfile.gettempdir()).glob('wechat-local-capture-*'))
        try:
            with patch.object(credential_capture,'_stop_owned_group',return_value=False), patch.object(credential_capture,'_verify_all_shards',side_effect=AssertionError('must not validate')):
                result=self._run()
            self.assertEqual(result['status'],'debugger_cleanup_uncertain')
            self.assertEqual(result['diagnostics']['group_cleanup'],'unknown')
            self.assertEqual(result['diagnostics']['debugger_returncode'],0)
            self.assertEqual(result['diagnostics']['validation'],'not_started')
        finally:
            import shutil
            for stage in set(Path(tempfile.gettempdir()).glob('wechat-local-capture-*'))-before:
                shutil.rmtree(stage)

    def test_oversized_channel_after_real_driver_blocks_validation_and_leaks_nothing(self):
        text=self.lldb.read_text().replace('print(', "Path(script).with_name('diagnostic.json').write_text('SECRET_ERROR_ACCOUNT_PATH_SENTINEL' * 500)\nprint(")
        self.lldb.write_text(text)
        with patch.object(credential_capture,'_verify_all_shards',side_effect=AssertionError('must not validate')):
            result=self._run()
        self.assertEqual(result['status'],'capture_diagnostic_invalid')
        self.assertNotIn(SENTINEL,json.dumps(result))
        self.assertIsNone(result['diagnostics']['last_completed'])
        self.assertFalse(self.config.with_name('passphrase.bin').exists())

    def test_persistence_failure_follows_successful_validation(self):
        original=credential_capture._write_secret_exclusive
        def write(path, material):
            return (False,None) if path.name == 'passphrase.bin' else original(path,material)
        with patch.object(credential_capture,'_write_secret_exclusive',side_effect=write):
            result=self._run()
        self.assertEqual(result['status'],'credential_save_failed')
        self.assertEqual(result['diagnostics']['validation'],'completed')
        self.assertEqual(result['diagnostics']['persistence'],'failed')
        self.assertEqual(result['diagnostics']['configuration'],'not_started')

    def test_late_channel_corruption_cannot_return_verified_or_keep_saved_value(self):
        original=credential_capture._write_secret_exclusive
        channel=[]
        def write(path,material):
            result=original(path,material)
            if path.name=='diagnostic.json':channel.append(path)
            if path.name=='passphrase.bin':channel[0].write_bytes(b'{')
            return result
        with patch.object(credential_capture,'_write_secret_exclusive',side_effect=write):
            result=self._run()
        self.assertEqual(result['status'],'capture_diagnostic_invalid')
        self.assertNotIn('passphrase_file',result)
        self.assertFalse(self.config.with_name('passphrase.bin').exists())

if __name__ == '__main__': unittest.main()
