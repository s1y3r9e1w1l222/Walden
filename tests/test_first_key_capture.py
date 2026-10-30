from __future__ import annotations

import importlib.util
import json
import os
import plistlib
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import unittest
import contextlib
import io
import math
from pathlib import Path
from unittest.mock import patch

from sqlcipher3 import dbapi2 as sqlcipher

from wechat_local import credential_capture
from wechat_local import source_setup


def load_renderer():
    path = Path(__file__).parents[1] / "tools" / "render_lldb_capture.py"
    spec = importlib.util.spec_from_file_location("render_lldb_capture_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class RendererSafetyTests(unittest.TestCase):
    def test_environment_assignment_is_one_quoted_lldb_argument(self) -> None:
        renderer = load_renderer()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data_home = root / 'home with "quotes"'
            script = renderer.render(Path("/Applications/WeChat.app/Contents/MacOS/WeChat"), data_home, root / "key.bin")
        assignment = next(line for line in script.splitlines() if "CFFIXED_USER_HOME" in line)
        import shlex
        tokens = shlex.split(assignment)
        self.assertEqual(tokens[:3], ["settings", "set", "target.env-vars"])
        self.assertEqual(tokens[3], f"CFFIXED_USER_HOME={data_home}")

    def test_renderer_refuses_control_characters_and_insecure_existing_parent(self) -> None:
        renderer = load_renderer()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            executable = root / "WeChat"
            executable.touch()
            data_home = root / "Data"
            data_home.mkdir()
            with self.assertRaises(ValueError):
                renderer.render(executable, Path(str(data_home) + "\tbad"), root / "key")
            with self.assertRaises(ValueError):
                renderer.render(executable, Path(str(data_home) + "`id`"), root / "key")
            shared = root / "shared"
            shared.mkdir(mode=0o755)
            shared.chmod(0o755)
            output = shared / "script.lldb"
            with patch.object(sys, "argv", ["render_lldb_capture.py", "--wechat-executable", str(executable),
                    "--wechat-data-home", str(data_home), "--passphrase-output", str(shared / "key"),
                    "--output-script", str(output)]):
                result = renderer.main()
            self.assertEqual(result, 2)
            self.assertEqual(stat.S_IMODE(shared.stat().st_mode), 0o755)

    def test_renderer_rejects_backticks_in_each_lldb_argument_context(self) -> None:
        renderer = load_renderer()
        normal = Path("/tmp/ordinary path")
        with self.assertRaisesRegex(ValueError, "backticks"):
            renderer.render(Path("/tmp/We`Chat"), normal, Path("/tmp/key"))
        with self.assertRaisesRegex(ValueError, "backticks"):
            renderer.render(Path("/tmp/WeChat"), Path("/tmp/home`x"), Path("/tmp/key"))
        with self.assertRaisesRegex(ValueError, "backticks"):
            renderer.render(Path("/tmp/WeChat"), normal, Path("/tmp/key`x"))

    def test_source_and_packaged_query_skills_describe_the_same_first_use(self) -> None:
        root = Path(__file__).parents[1]
        source = (root / "skills/wechat-local-query/SKILL.md").read_text()
        packaged = (root / "src/wechat_local/resources/skills/wechat-local-query/SKILL.md").read_text()
        self.assertEqual(source, packaged)
        self.assertIn("--acquire-passphrase", source)
        self.assertNotIn("no automatic first-key acquisition path", source)


class FirstKeyCaptureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.data_home = self.root / "Client Data"
        self.account = self.data_home / "Documents" / "xwechat_files" / "synthetic_account_1"
        self.message_root = self.account / "db_storage" / "message"
        self.message_root.mkdir(parents=True)
        self.output_home = self.root / "app data"
        self.config = self.output_home / "config" / "current-source.json"
        self.cli_bin = self.root / "installed-venv" / "bin"
        self.cli_bin.mkdir(parents=True)
        for name in ("wechat-local-setup", "wechat-local-window", "wechat-local-reply"):
            command = self.cli_bin / name
            command.write_text("#!/bin/sh\nexit 0\n")
            command.chmod(0o700)
        self.key = b"k" * 32
        self._make_db(self.message_root / "message_0.db", self.key)
        self._make_db(self.message_root / "message_1.db", self.key)
        self.app = self.root / "WeChat Copy.app"
        self.contents = self.app / "Contents"
        self.mac = self.contents / "MacOS"
        self.mac.mkdir(parents=True)
        self.executable = self.mac / "WeChat"
        shutil.copyfile("/usr/bin/true", self.executable)
        self.executable.chmod(0o700)
        (self.contents / "Info.plist").write_bytes(plistlib.dumps({
            "CFBundleIdentifier": "com.tencent.xinWeChat",
            "CFBundleExecutable": "WeChat",
        }))
        self.lldb = self.root / "fake lldb"
        self._write_debugger(self.key, 0)
        self.active_policy_config = self.root / 'policy-config'
        self.active_policy_config.write_text(str(self.config.parent))
        self.policy_mode = self.root / 'policy-mode'
        self.policy_mode.write_text('early')
        self.policy_program = self.root / 'synthetic-policy-stream'
        self.policy_program.write_text(f'''#!{sys.executable}
import datetime, os, pathlib, signal, sys, time
root=pathlib.Path({str(self.root)!r})
mode=(root/'policy-mode').read_text()
if mode == 'exit': sys.exit(2)
if mode == 'overflow': print('A'*20000,flush=True);time.sleep(30);sys.exit(0)
if mode == 'drop': print('messages dropped',flush=True);time.sleep(30);sys.exit(0)
copy=next((pathlib.Path((root/'policy-config').read_text())/'.wechat-local-client').rglob('WeChat-debug.app'))
while not (root/'debugger-ran').exists():time.sleep(0.01)
if mode == 'noqueue':time.sleep(30);sys.exit(0)
def emit(message,level='I'):
 when=datetime.datetime.now(datetime.timezone.utc)
 if mode == 'old':when-=datetime.timedelta(seconds=20)
 stamp=when.strftime('%Y-%m-%d %H:%M:%S.%f%z')
 print(stamp+' '+level+'  syspolicyd[77:abc] [com.apple.syspolicy.exec:default] '+message,flush=True)
print('SYNTHETIC_PRIVATE_LOG_SECRET 01 23 45 67',flush=True)
url=(copy.with_name('Other.app') if mode=='other' else copy).as_uri()+'/'
if mode=='alias':url=(pathlib.Path('/tmp')/copy.relative_to('/private/tmp')).as_uri()+'/'
if mode == 'redacted':url='<private>'
pid=999 if mode=='other_pid' else (2147482999 if mode=='history_wrong_pid' else 2147483000)
emit(f'Skipping TCC check due to process: {{pid}}, 0, {{pid}}')
emit('queueing up scan for code: PST: (path: a12b), (team: (null))')
if mode=='duplicate':emit('queueing up scan for code: PST: (path: a12b), (team: (null))')
if mode in ('history_denied','history_denied_E','history_nonzero','history_wrong_pid'):
 if mode in ('history_denied','history_denied_E'):emit('policy denied: PST: (path: a12b), (team: (null))','E' if mode=='history_denied_E' else 'I')
 elif mode=='history_nonzero':emit('GK scan complete: PST: (path: a12b), (team: (null)), 1, 0, 0')
 else:
  emit('GK Xprotect results: PST: (path: a12b), (team: (null)), XPScan: 0,99,now,(null),(null),'+url)
  emit('GK scan complete: PST: (path: a12b), (team: (null)), 0, 0, 0')
  emit('scan finished, waking up any waiters: PST: (path: a12b), (team: (null))')
 emit('Skipping TCC check due to process: 2147483000, 0, 2147483000')
 emit('queueing up scan for code: PST: (path: cdef), (team: (null))')
 emit('GK Xprotect results: PST: (path: cdef), (team: (null)), XPScan: 0,99,now,(null),(null),'+url)
 emit('GK scan complete: PST: (path: cdef), (team: (null)), 0, 0, 0')
 emit('scan finished, waking up any waiters: PST: (path: cdef), (team: (null))')
 time.sleep(30);sys.exit(0)
if mode=='queue_only':time.sleep(30);sys.exit(0)
if mode=='reject_before_url':
 emit('policy denied: PST: (path: a12b), (team: (null))')
 time.sleep(30);sys.exit(0)
if mode=='nonzero_before_url':
 emit('GK scan complete: PST: (path: a12b), (team: (null)), 1, 0, 0')
 time.sleep(30);sys.exit(0)
if mode=='late':
 while not (root/'first-driver-finished').exists():time.sleep(0.01)
 debug_pid=int((root/'debugger-ran').read_text())
 until=time.monotonic()+20
 while time.monotonic()<until:
  try:os.kill(debug_pid,0)
  except ProcessLookupError:break
  time.sleep(0.01)
 time.sleep(0.15)
emit('GK Xprotect results: PST: (path: a12b), (team: (null)), XPScan: 0,99,now,(null),(null),'+url)
if mode=='reject':emit('policy denied: PST: (path: a12b), (team: (null))')
else:
 emit('GK scan complete: PST: (path: a12b), (team: (null)), 0, 0, 0')
 emit('scan finished, waking up any waiters: PST: (path: a12b), (team: (null))')
if mode=='completed_exit':sys.exit(0)
if mode in ('close_denied','close_lost'):
 while not (root/'close-gate').exists():time.sleep(.01)
 if mode=='close_denied':emit('policy denied: PST: (path: a12b), (team: (null))')
 else:print('messages dropped',flush=True)
time.sleep(30)
''')
        self.policy_program.chmod(0o700)
        self.policy_patch = patch.object(credential_capture, '_POLICY_LOG', self.policy_program)
        self.policy_patch.start()

    def tearDown(self) -> None:
        self.policy_patch.stop()
        self.temp.cleanup()

    @staticmethod
    def _make_db(path: Path, key: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlcipher.connect(str(path))
        db.execute(f"PRAGMA key = '{key.decode('ascii')}'")
        db.execute("PRAGMA cipher_compatibility = 4")
        db.execute("CREATE TABLE synthetic_metadata(x)")
        db.close()

    def _write_debugger(self, key: bytes, exit_code: int, *, sleep: bool = False, short: bool = False,
                        long: bool = False, spawn_child: bool = False,
                        spawn_detached_child: bool = False, fifo_capture: bool = False,
                        replace_capture: bool = False) -> None:
        payload = self.root / "synthetic-input.bin"
        payload.write_bytes(key[:8] if short else (key + b"x" if long else key))
        data = repr(payload.read_bytes())
        marker = self.root / "debugger-ran"
        code = f'''#!{sys.executable}
import ast, os, subprocess, sys, time
from pathlib import Path
sys.path.insert(0, {str(Path(__file__).parent)!r})
sys.path.insert(0, {str(Path(credential_capture.__file__).parent.parent)!r})
from test_capture_diagnostics import execute_driver
Path({str(marker)!r}).write_text(str(os.getpid()))
args=sys.argv[1:]
assert args[0] == "-s" and len(args) == 2
assert os.environ.get("TERM") == "dumb"
script=args[1]
assert sys.stdin.read() == "script _capture_session.finish()\\nquit\\n"
assert (Path(script).stat().st_mode & 0o777)==0o600
assert (Path(script).parent.stat().st_mode & 0o777)==0o700
mutation = 'fifo_capture' if {fifo_capture!r} else 'replace_capture' if {replace_capture!r} else None
execute_driver(script, payload={data}, mutation=mutation, require_outer_run=True)
print({data}.hex())
''' + ("child=subprocess.Popen(['/bin/sleep','30'])\nPath(" + repr(str(self.root / "child-pid")) + ").write_text(str(child.pid))\n" if spawn_child else "") + ("time.sleep(30)\n" if sleep else f"sys.exit({exit_code})\n")
        if spawn_detached_child:
            code = code.replace(
                "time.sleep(30)\n" if sleep else f"sys.exit({exit_code})\n",
                "child=subprocess.Popen(['/bin/sleep','30'],start_new_session=True)\nPath(" + repr(str(self.root / "detached-child-pid")) + ").write_text(str(child.pid))\ntime.sleep(30)\n",
            )
        self.lldb.write_text(code)
        self.lldb.chmod(0o700)
        self.synthetic_capture = payload

    def _run(self, timeout: float = 3.0) -> dict[str, object]:
        return credential_capture.capture_and_save_passphrase(
            account_root=self.account,
            wechat_executable=self.executable,
            wechat_data_home=self.data_home,
            lldb_executable=self.lldb,
            config_path=self.config,
            timeout_seconds=timeout,
        )

    def _run_setup(self, *, config: Path | None = None, data_home: Path | None = None, timeout: str = "3") -> tuple[int, str]:
        output = io.StringIO()
        argv = [
            str(self.cli_bin / "wechat-local-setup"), "--account-root", str(self.account),
            "--config", str(config or self.config), "--data-home", str(data_home or self.output_home),
            "--acquire-passphrase", "--wechat-executable", str(self.executable),
            "--wechat-data-home", str(self.data_home), "--lldb-executable", str(self.lldb),
            "--capture-timeout", timeout,
        ]
        with patch.object(sys, "argv", argv), contextlib.redirect_stdout(output):
            result = source_setup.main()
        return result, output.getvalue()

    def _install_two_attempt_debugger(self, first='attach_failure', second='success', extra_first=''):
        (self.root/'debugger-ran').unlink(missing_ok=True)
        (self.root/'first-driver-finished').unlink(missing_ok=True)
        self._write_debugger(self.key, 0)
        marker = self.root / 'attempts.jsonl'
        code = self.lldb.read_text()
        line = next(x for x in code.splitlines() if x.startswith('execute_driver('))
        code = code.replace(line, f"""marker=Path({str(marker)!r})
previous=marker.read_text().splitlines() if marker.exists() else []
stage=Path(script).parent
copy=(Path(previous[0].split('|')[1]) if previous else next(Path({str(self.config.parent / '.wechat-local-client')!r}).rglob('WeChat-debug.app')))
assert copy.is_dir() and str(copy) in Path(script).read_text()
with marker.open('a') as f:f.write(str(stage)+'|'+str(copy)+'|'+str(copy.stat().st_ino)+'\\n')
mode={first!r} if not previous else {second!r}
if mode=='attach_failure':
 execute_driver(script, payload={self.key!r}, fail='launch', mutation='attach_failure', require_outer_run=True)
 print('error: attach failed (attached to process, but could not pause execution; attach failed)')
elif mode=='other_failure': execute_driver(script, payload={self.key!r}, fail='launch', mutation='unexpected_exit', require_outer_run=True)
elif mode=='timeout': time.sleep(30)
else: execute_driver(script, payload={self.key!r}, require_outer_run=True)
if not previous:
 time.sleep(0.2)
 {extra_first or 'pass'}
 Path({str(self.root/'first-driver-finished')!r}).write_text('done')
""")
        self.lldb.write_text(code)
        return marker

    def test_one_retry_reuses_copy_with_fresh_diagnostics_and_preserves_error(self):
        marker = self._install_two_attempt_debugger()
        result = self._run_prepared()
        self.assertEqual(result['status'], 'captured_and_verified', result)
        attempts = result['diagnostics']['attempts']
        self.assertEqual(len(attempts), 2)
        self.assertEqual(attempts[0]['status'], 'capture_stage_failed')
        self.assertIn('could not pause execution', attempts[0]['diagnostics']['target_exit_description'])
        self.assertEqual(attempts[1]['status'], 'captured_and_verified')
        rows = [line.split('|') for line in marker.read_text().splitlines()]
        self.assertEqual(len(rows), 2)
        self.assertNotEqual(rows[0][0], rows[1][0])
        self.assertEqual(rows[0][1:], rows[1][1:])
        self.assertTrue(Path(rows[0][1]).is_dir())

    def test_default_public_setup_exposes_both_attempts_on_success(self):
        marker = self._install_two_attempt_debugger()
        resolved = dict(status='resolved', wechat_executable=str(self.executable),
                        wechat_data_home=str(self.data_home), lldb_executable=str(self.lldb))
        argv = [str(self.cli_bin/'wechat-local-setup'), '--account-root', str(self.account),
                '--config', str(self.config), '--data-home', str(self.output_home), '--acquire-passphrase']
        with patch.object(credential_capture, 'resolve_default_capture_inputs', return_value=resolved), \
             patch.object(sys, 'argv', argv), contextlib.redirect_stdout(io.StringIO()) as output:
            result = source_setup.main()
        value = json.loads(output.getvalue())
        self.assertEqual(result, 0, value)
        self.assertTrue(value['status'].startswith('configured'))
        self.assertEqual([a['status'] for a in value['diagnostics']['attempts']],
                         ['capture_stage_failed', 'captured_and_verified'])
        self.assertEqual(len(marker.read_text().splitlines()), 2)
        self.assertNotIn(self.key.hex(), output.getvalue())

    def test_tmp_alias_event_retries_same_owned_copy(self):
        self.policy_mode.write_text('alias')
        marker=self._install_two_attempt_debugger(extra_first='time.sleep(0.3)')
        result=self._run_prepared(timeout=6)
        self.assertEqual(result['status'],'captured_and_verified',result)
        self.assertEqual(result['diagnostics']['policy_observation'],{'state':'completed','retry':'completed'})
        rows=marker.read_text().splitlines()
        self.assertEqual(len(rows),2)
        self.assertEqual(rows[0].split('|')[1:],rows[1].split('|')[1:])

    def test_tmp_alias_does_not_bypass_replaced_copy_guard(self):
        self.policy_mode.write_text('alias')
        extra="import shutil;os.rename(copy,copy.with_name('replaced.app'));shutil.copytree(copy.with_name('replaced.app'),copy,symlinks=True);time.sleep(0.3)"
        marker=self._install_two_attempt_debugger(extra_first=extra)
        result=self._run_prepared(timeout=6)
        self.assertNotEqual(result['status'],'captured_and_verified',result)
        self.assertEqual(len(marker.read_text().splitlines()),1)
        self.assertTrue((self.config.parent/'.wechat-local-client'/'lock').is_dir())

    def test_late_scan_event_triggers_same_copy_once(self):
        self.policy_mode.write_text('late')
        marker = self._install_two_attempt_debugger()
        result = self._run_prepared(timeout=6)
        self.assertEqual(result['status'], 'captured_and_verified', result)
        self.assertEqual(result['diagnostics']['policy_observation'], {'state':'completed','retry':'completed'})
        self.assertEqual(len(marker.read_text().splitlines()), 2)
        self.assertNotIn(str(self.config.parent), json.dumps(result['diagnostics']['policy_observation']))
        self.assertNotIn('SYNTHETIC_PRIVATE_LOG_SECRET', json.dumps(result))

    def test_observation_rejected_or_lost_during_close_does_not_retry(self):
        original_config=self.config
        for number,(mode,expected) in enumerate((('close_denied','rejected'),('close_lost','unavailable'))):
            with self.subTest(mode=mode):
                self.config=original_config.parent/('case'+str(number))/original_config.name
                self.policy_mode.write_text(mode)
                (self.root/'attempts.jsonl').unlink(missing_ok=True)
                marker=self._install_two_attempt_debugger(extra_first='time.sleep(0.3)')
                (self.root/'close-gate').unlink(missing_ok=True)
                original=credential_capture._PolicyObserver.close;seen=[]
                def close_at_gate(observer,*args,**kwargs):
                    if kwargs.get('require_active'):
                        terminate=observer._process.terminate
                        def delayed_terminate():
                            (self.root/'close-gate').touch()
                            until=time.monotonic()+3
                            while time.monotonic()<until and observer._matcher.state=='completed':time.sleep(.01)
                            seen.append(observer._matcher.state)
                            terminate()
                        observer._process.terminate=delayed_terminate
                    return original(observer,*args,**kwargs)
                with patch.object(credential_capture._PolicyObserver,'close',new=close_at_gate):
                    result=self._run_prepared(timeout=6)
                self.assertEqual(seen,[expected])
                self.assertEqual(result['status'],'capture_stage_failed',result)
                self.assertEqual(result['failure_reason'],'scan_wait_'+expected)
                self.assertEqual(result['diagnostics']['policy_observation'],{'state':expected,'retry':'not_started'})
                self.assertEqual(len(marker.read_text().splitlines()),1)
                self.assertFalse(self.config.exists())
                self.assertFalse(self.config.with_name('passphrase.bin').exists())
                marker.unlink()

    def test_cancel_while_stopping_observer_does_not_retry(self):
        marker=self._install_two_attempt_debugger(extra_first='time.sleep(0.3)')
        original=credential_capture._PolicyObserver.close;interrupted=[]
        def cancel_once(observer,*args,**kwargs):
            if kwargs.get('require_active') and not interrupted:
                interrupted.append(True)
                raise KeyboardInterrupt
            return original(observer,*args,**kwargs)
        with patch.object(credential_capture._PolicyObserver,'close',new=cancel_once):
            result=self._run_prepared(timeout=6)
        self.assertEqual(result['status'],'capture_stage_failed',result)
        self.assertEqual(result['failure_reason'],'scan_wait_cancelled')
        self.assertEqual(len(marker.read_text().splitlines()),1)
        self.assertFalse(self.config.exists())
        self.assertFalse(self.config.with_name('passphrase.bin').exists())

    def test_observer_is_reaped_before_automatic_retry(self):
        marker = self._install_two_attempt_debugger()
        original = credential_capture._PolicyObserver.close
        closed = []
        def record(observer, *args, **kwargs):
            result = original(observer, *args, **kwargs)
            closed.append(observer._process.poll())
            return result
        with patch.object(credential_capture._PolicyObserver, 'close', new=record):
            result = self._run_prepared(timeout=6)
        self.assertEqual(result['status'], 'captured_and_verified', result)
        self.assertEqual(len(marker.read_text().splitlines()), 2)
        self.assertEqual(len(closed), 1)
        self.assertIsNotNone(closed[0])

    def test_completed_but_exited_observer_cannot_trigger_retry(self):
        self.policy_mode.write_text('completed_exit')
        marker = self._install_two_attempt_debugger(extra_first='time.sleep(0.2)')
        result = self._run_prepared(timeout=6)
        self.assertEqual(result['status'], 'capture_stage_failed', result)
        self.assertEqual(result['diagnostics']['policy_observation']['state'], 'unavailable')
        self.assertEqual(len(marker.read_text().splitlines()), 1)

    def test_bad_observation_never_retries(self):
        original = self.config
        for mode in ('other', 'other_pid', 'duplicate', 'old', 'redacted', 'drop', 'exit', 'overflow', 'reject'):
            with self.subTest(mode=mode):
                self.config = original.parent / mode / original.name
                self.policy_mode.write_text(mode)
                (self.root/'attempts.jsonl').unlink(missing_ok=True)
                marker = self._install_two_attempt_debugger()
                result = self._run_prepared(timeout=6)
                self.assertEqual(result['status'], 'capture_stage_failed', result)
                self.assertEqual(len(marker.read_text().splitlines()), 1)
                self.assertNotEqual(result['diagnostics']['policy_observation']['state'], 'completed')
                self.assertEqual(result['diagnostics']['policy_observation']['retry'], 'not_started')
                self.assertIn('could not pause execution', result['diagnostics']['target_exit_description'])
                self.assertFalse(self.config.exists())
                self.assertFalse(self.config.with_name('passphrase.bin').exists())
                marker.unlink()

    def test_observer_failure_does_not_block_initial_success_or_explicit_target(self):
        self.policy_mode.write_text('exit')
        result = self._run_prepared(timeout=6)
        self.assertEqual(result['status'], 'captured_and_verified', result)
        self.assertEqual(result['diagnostics']['policy_observation']['state'], 'not_needed')
        self.config = self.root/'another'/'current.json'
        with patch.object(credential_capture._PolicyObserver, 'start', side_effect=AssertionError('must not observe')):
            result = self._run(timeout=6)
        self.assertEqual(result['status'], 'captured_and_verified', result)

    def test_observer_cleanup_uncertain_preserves_lock_without_saving_key(self):
        original = credential_capture._PolicyObserver.close
        def closed_but_report_uncertain(observer, *args, **kwargs):
            original(observer, *args, **kwargs)
            raise OSError('synthetic cleanup uncertain')
        with patch.object(credential_capture._PolicyObserver, 'close', new=closed_but_report_uncertain):
            result = self._run_prepared(timeout=6)
        self.assertEqual(result['status'], 'policy_observer_cleanup_uncertain')
        self.assertEqual(result['diagnostics']['policy_observation']['state'], 'cleanup_uncertain')
        self.assertTrue((self.config.parent/'.wechat-local-client'/'lock').is_dir())
        self.assertFalse(self.config.with_name('passphrase.bin').exists())
        self.assertFalse(self.config.exists())

    def test_cancel_before_debugger_launch_reaps_observer_without_saving(self):
        with patch.object(credential_capture._PolicyObserver, 'arm', side_effect=KeyboardInterrupt):
            result = self._run_prepared(timeout=0)
        self.assertEqual(result['status'], 'capture_cancelled', result)
        self.assertEqual(result['failure_reason'], 'debugger_cancelled')
        self.assertEqual(result['diagnostics']['policy_observation']['state'], 'cancelled')
        self.assertFalse((self.root/'debugger-ran').exists())
        self.assertFalse(self.config.with_name('passphrase.bin').exists())
        self.assertFalse(self.config.exists())

    def test_deadline_expiry_before_launch_does_not_start_debugger(self):
        original = credential_capture._PolicyObserver.arm
        def delayed_arm(observer):
            original(observer)
            time.sleep(0.15)
        with patch.object(credential_capture._PolicyObserver, 'arm', new=delayed_arm):
            result = self._run_prepared(timeout=0.05)
        self.assertEqual(result['status'], 'capture_timeout', result)
        self.assertEqual(result['failure_reason'], 'debugger_timeout')
        self.assertFalse((self.root/'debugger-ran').exists())
        self.assertFalse(self.config.with_name('passphrase.bin').exists())
        self.assertFalse(self.config.exists())

    def test_unbound_queue_stops_immediately_with_zero_timeout_and_live_stream(self):
        self.policy_mode.write_text('noqueue')
        marker = self._install_two_attempt_debugger()
        original = credential_capture._PolicyObserver.close
        before_close = []
        def record(observer, *args, **kwargs):
            before_close.append(observer._process.poll())
            return original(observer, *args, **kwargs)
        started = time.monotonic()
        with patch.object(credential_capture._PolicyObserver, 'close', new=record):
            result = self._run_prepared(timeout=0)
        self.assertEqual(result['status'], 'capture_stage_failed', result)
        self.assertEqual(result['failure_reason'], 'scan_wait_unavailable')
        self.assertEqual(result['diagnostics']['policy_observation']['retry'], 'not_started')
        self.assertEqual(len(marker.read_text().splitlines()), 1)
        self.assertEqual(before_close, [None])
        self.assertLess(time.monotonic() - started, 12)

    def test_bound_rejection_before_url_stops_with_zero_timeout_and_live_stream(self):
        original_config = self.config
        original_close = credential_capture._PolicyObserver.close
        for mode, expected in (('reject_before_url', 'rejected'), ('nonzero_before_url', 'unavailable')):
            with self.subTest(mode=mode):
                self.config = original_config.parent / mode / original_config.name
                self.policy_mode.write_text(mode)
                (self.root/'attempts.jsonl').unlink(missing_ok=True)
                marker = self._install_two_attempt_debugger()
                before_close = []
                def record(observer, *args, **kwargs):
                    before_close.append(observer._process.poll())
                    return original_close(observer, *args, **kwargs)
                started = time.monotonic()
                with patch.object(credential_capture._PolicyObserver, 'close', new=record):
                    result = self._run_prepared(timeout=0)
                self.assertEqual(result['status'], 'capture_stage_failed', result)
                self.assertEqual(result['failure_reason'], 'scan_wait_' + expected)
                self.assertEqual(result['diagnostics']['policy_observation']['retry'], 'not_started')
                self.assertEqual(len(marker.read_text().splitlines()), 1)
                self.assertEqual(before_close, [None])
                self.assertLess(time.monotonic() - started, 12)
                self.assertFalse(self.config.exists())
                self.assertFalse(self.config.with_name('passphrase.bin').exists())
                marker.unlink()

    def test_multiple_early_psts_with_rejection_or_conflict_never_retry(self):
        original_config=self.config
        for mode, expected in (('history_denied','rejected'), ('history_denied_E','rejected'), ('history_nonzero','unavailable'), ('history_wrong_pid','unavailable')):
            with self.subTest(mode=mode):
                self.config=original_config.parent/mode/original_config.name
                self.policy_mode.write_text(mode)
                (self.root/'attempts.jsonl').unlink(missing_ok=True)
                marker=self._install_two_attempt_debugger(extra_first='time.sleep(0.3)')
                result=self._run_prepared(timeout=6)
                self.assertEqual(result['status'],'capture_stage_failed',result)
                self.assertEqual(result['failure_reason'],'scan_wait_'+expected)
                self.assertEqual(result['diagnostics']['policy_observation']['retry'],'not_started')
                self.assertEqual(len(marker.read_text().splitlines()),1)
                self.assertFalse(self.config.exists())
                self.assertFalse(self.config.with_name('passphrase.bin').exists())
                marker.unlink()

    def test_wait_cancellation_stops_without_second_launch(self):
        self.policy_mode.write_text('queue_only')
        marker = self._install_two_attempt_debugger()
        def cancel_wait(observer, deadline):
            self.assertIsNone(deadline)
            self.assertEqual(observer._matcher.state, 'watching')
            raise KeyboardInterrupt
        with patch.object(credential_capture._PolicyObserver, 'wait', new=cancel_wait):
            result = self._run_prepared(timeout=0)
        self.assertEqual(result['status'], 'capture_stage_failed')
        self.assertEqual(result['failure_reason'], 'scan_wait_cancelled')
        self.assertEqual(result['diagnostics']['policy_observation']['state'], 'cancelled')
        self.assertEqual(len(marker.read_text().splitlines()), 1)
        self.assertFalse(self.config.with_name('passphrase.bin').exists())

    def test_one_shared_deadline_covers_wait_and_second_debugger(self):
        self.policy_mode.write_text('late')
        marker = self._install_two_attempt_debugger(second='timeout')
        original_wait = subprocess.Popen.wait
        limits = []
        def record_wait(process, timeout=None):
            if Path(process.args[0]).resolve() == self.lldb.resolve() and timeout is not None:
                limits.append(timeout)
            return original_wait(process, timeout=timeout)
        with patch.object(subprocess.Popen, 'wait', new=record_wait):
            result = self._run_prepared(timeout=8)
        self.assertEqual(result['status'], 'debugger_cleanup_uncertain', result)
        self.assertEqual(result['diagnostics']['policy_observation']['retry'], 'failed')
        self.assertEqual(len(marker.read_text().splitlines()), 2)
        self.assertGreaterEqual(len(limits), 2)
        self.assertLess(limits[1], limits[0] - 0.1)
        self.assertTrue((self.config.parent/'.wechat-local-client'/'lock').is_dir())

    def test_retry_rejects_alive_or_inaccessible_first_pid(self):
        for error in (None, PermissionError()):
            with self.subTest(error=error):
                marker = self._install_two_attempt_debugger()
                real_kill = credential_capture.os.kill
                def probe(pid, sig):
                    if pid != 2147483000:
                        return real_kill(pid, sig)
                    if error: raise error
                    return None
                with patch.object(credential_capture.os, 'kill', side_effect=probe):
                    result = self._run_prepared()
                self.assertEqual(result['status'], 'capture_stage_failed')
                self.assertEqual(len(marker.read_text().splitlines()), 1)
                marker.unlink()

    def test_other_failure_or_possible_capture_never_retries(self):
        for first, extra in (('other_failure', ''), ('attach_failure', "(stage/'captured.bin').write_bytes(b'X')"), ('attach_failure', "(stage/'replacement').write_bytes(b'');os.chmod(stage/'replacement',0o600);os.replace(stage/'replacement',stage/'captured.bin')")):
            with self.subTest(first=first, extra=extra):
                marker = self._install_two_attempt_debugger(first=first, extra_first=extra)
                result = self._run_prepared()
                self.assertEqual(result['status'], 'capture_stage_failed')
                self.assertEqual(len(marker.read_text().splitlines()), 1)
                marker.unlink()

    def test_replaced_copy_never_retries(self):
        extra = "import shutil;os.rename(copy,copy.with_name('replaced.app'));shutil.copytree(copy.with_name('replaced.app'),copy,symlinks=True)"
        marker = self._install_two_attempt_debugger(extra_first=extra)
        result = self._run_prepared()
        self.assertNotEqual(result['status'], 'captured_and_verified', result)
        rows = marker.read_text().splitlines()
        self.assertEqual(len(rows), 1)
        copy = Path(rows[0].split('|')[1])
        self.assertTrue(copy.is_dir())
        self.assertTrue((self.config.parent/'.wechat-local-client'/'lock').is_dir())
        shutil.rmtree(copy.parent)  # Synthetic replacement after preservation was checked.

    def test_copy_internal_directory_replaced_by_external_link_stops(self):
        outside = self.root/'moved-contents'
        extra = "os.rename(copy/'Contents'," + repr(str(outside)) + ");os.symlink(" + repr(str(outside)) + ",copy/'Contents')"
        marker = self._install_two_attempt_debugger(extra_first=extra)
        result = self._run_prepared()
        self.assertEqual(result['status'], 'capture_stage_failed', result)
        self.assertNotIn('attempts', result['diagnostics'])
        rows = marker.read_text().splitlines()
        self.assertEqual(len(rows), 1)
        copy = Path(rows[0].split('|')[1])
        self.assertTrue(copy.is_dir())
        shutil.rmtree(copy.parent)

    def test_existing_credential_after_first_failure_never_retries_or_overwrites(self):
        key = self.config.with_name('passphrase.bin')
        extra = "Path(" + repr(str(key.parent)) + ").mkdir(parents=True,exist_ok=True);Path(" + repr(str(key)) + ").write_bytes(b'foreign')"
        marker = self._install_two_attempt_debugger(extra_first=extra)
        result = self._run_prepared()
        self.assertEqual(result['status'], 'capture_stage_failed')
        self.assertEqual(len(marker.read_text().splitlines()), 1)
        self.assertEqual(key.read_bytes(), b'foreign')

    def test_first_timeout_or_cancel_does_not_retry(self):
        original_config = self.config
        for mode in ('timeout', 'cancel'):
            with self.subTest(mode=mode):
                self.config = original_config.parent / mode / original_config.name
                marker = self._install_two_attempt_debugger(first='timeout')
                before = set(Path(tempfile.gettempdir()).glob('wechat-local-capture-*'))
                real_wait = subprocess.Popen.wait; waits = []
                def wait(process, timeout=None):
                    if Path(process.args[0]).resolve() == self.lldb.resolve() and not waits:
                        waits.append(True)
                        if mode == 'cancel': raise KeyboardInterrupt()
                    return real_wait(process, timeout=timeout)
                try:
                    with patch.object(subprocess.Popen, 'wait', wait):
                        result = self._run_prepared(timeout=1)
                    self.assertEqual(result['status'], 'debugger_cleanup_uncertain')
                    self.assertEqual(result['failure_reason'], 'debugger_cancelled' if mode == 'cancel' else 'debugger_timeout')
                    self.assertEqual(len(waits), 1)
                    self.assertLessEqual(len(marker.read_text().splitlines()) if marker.exists() else 0, 1)
                    self.assertTrue((self.config.parent/'.wechat-local-client'/'lock').is_dir())
                finally:
                    if marker.exists(): marker.unlink()
                    for stage in set(Path(tempfile.gettempdir()).glob('wechat-local-capture-*')) - before:
                        shutil.rmtree(stage)

    def test_exact_failure_on_explicit_target_never_retries(self):
        self._write_debugger(self.key, 0)
        marker = self.root / 'explicit-count'
        code = self.lldb.read_text().replace('args=sys.argv[1:]',
            "args=sys.argv[1:]\nwith Path(" + repr(str(marker)) + ").open('a') as f:f.write('1\\n')")
        code = code.replace('mutation=mutation, require_outer_run=True', "fail='launch', mutation='attach_failure', require_outer_run=True")
        self.lldb.write_text(code)
        result = self._run()
        self.assertEqual(result['status'], 'capture_stage_failed')
        self.assertEqual(marker.read_text().splitlines(), ['1'])

    def test_unconfirmed_first_group_cleanup_never_retries(self):
        marker = self._install_two_attempt_debugger()
        original = credential_capture._stop_owned_group; calls = []
        def stop(pid):
            calls.append(pid)
            if len(calls) == 4: return False
            return original(pid)
        before = set(Path(tempfile.gettempdir()).glob('wechat-local-capture-*'))
        try:
            with patch.object(credential_capture, '_stop_owned_group', side_effect=stop):
                result = self._run_prepared()
            self.assertEqual(result['status'], 'debugger_cleanup_uncertain')
            self.assertTrue((self.config.parent/'.wechat-local-client'/'lock').is_dir())
            self.assertEqual(len(marker.read_text().splitlines()), 1)
        finally:
            for stage in set(Path(tempfile.gettempdir()).glob('wechat-local-capture-*')) - before:
                shutil.rmtree(stage)

    def test_second_failure_stops_at_two_and_preserves_copy(self):
        marker = self._install_two_attempt_debugger(second='attach_failure')
        result = self._run_prepared()
        self.assertNotEqual(result['status'], 'captured_and_verified')
        self.assertEqual(len(result['diagnostics']['attempts']), 2)
        self.assertIn('could not pause execution', ' '.join(result['diagnostics']['debugger_errors']), result)
        rows = [line.split('|') for line in marker.read_text().splitlines()]
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][1:], rows[1][1:])
        self.assertTrue(Path(rows[0][1]).is_dir())
        shutil.rmtree(Path(rows[0][1]).parent)

    def test_second_timeout_retains_same_copy_and_never_launches_third(self):
        marker = self._install_two_attempt_debugger(second='timeout')
        result = self._run_prepared(timeout=4)
        self.assertEqual(result['status'], 'debugger_cleanup_uncertain')
        rows = [line.split('|') for line in marker.read_text().splitlines()]
        self.assertEqual(len(rows), 2)
        self.assertTrue(Path(rows[0][1]).is_dir())
        self.assertEqual(result['diagnostics']['attempts'][1]['diagnostics']['target_cleanup'], 'unknown')
        self.assertTrue((self.config.parent/'.wechat-local-client'/'lock').is_dir())
        shutil.rmtree(Path(rows[0][1]).parent)

    def test_public_default_waits_until_cancel_and_explicit_timeout_remains(self):
        resolved = dict(status='resolved', wechat_executable=str(self.executable),
                        wechat_data_home=str(self.data_home), lldb_executable=str(self.lldb))
        for extra, expected in (([], 0), (['--capture-timeout', '23'], 23)):
            argv = [str(self.cli_bin/'wechat-local-setup'), '--account-root', str(self.account),
                    '--config', str(self.config), '--data-home', str(self.output_home), '--acquire-passphrase', *extra]
            with patch.object(credential_capture, 'resolve_default_capture_inputs', return_value=resolved), \
                 patch.object(credential_capture, 'capture_and_save_passphrase', return_value={'status':'synthetic_stop'}) as capture, \
                 patch.object(sys, 'argv', argv), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(source_setup.main(), 2)
            self.assertEqual(capture.call_args.kwargs['timeout_seconds'], expected)

    def test_default_setup_prepares_owned_copy_before_capture(self):
        marker = self.root / 'copy-path'
        self.lldb.write_text(self.lldb.read_text().replace('args=sys.argv[1:]',
            "args=sys.argv[1:]\ncopy=next(Path(" + repr(str(self.config.parent / '.wechat-local-client')) + ").rglob('WeChat-debug.app'))\nPath(" + repr(str(marker)) + ").write_text(str(copy))\nassert (copy/'Contents/MacOS/WeChat').is_file()"))
        resolved = dict(status='resolved', wechat_executable=str(self.executable),
                        wechat_data_home=str(self.data_home), lldb_executable=str(self.lldb))
        argv = [str(self.cli_bin / 'wechat-local-setup'), '--account-root', str(self.account),
                '--config', str(self.config), '--data-home', str(self.output_home), '--acquire-passphrase']
        with patch.object(credential_capture, 'resolve_default_capture_inputs', return_value=resolved), \
             patch.object(sys, 'argv', argv), contextlib.redirect_stdout(io.StringIO()) as output:
            exit_code = source_setup.main()
        result = json.loads(output.getvalue())
        self.assertEqual(exit_code, 0, result)
        self.assertTrue(result['status'].startswith('configured'))
        self.assertTrue(Path(marker.read_text()).is_dir())

    def _run_prepared(self, timeout=8):
        self.active_policy_config.write_text(str(self.config.parent))
        return credential_capture.capture_and_save_passphrase(
            account_root=self.account, wechat_executable=self.executable,
            wechat_data_home=self.data_home, lldb_executable=self.lldb,
            config_path=self.config, timeout_seconds=timeout, prepare_copy=True)

    def test_reuses_private_copy_after_capture_failure(self):
        marker = self.root / 'preparation-commands'
        original = credential_capture._run_preparation_command
        def track(step, argv):
            with marker.open('a') as out: out.write(step + '\n')
            return original(step, argv)
        code = self.lldb.read_text().replace("mutation=mutation, require_outer_run=True", "mutation='unexpected_exit', fail='launch', require_outer_run=True")
        self.lldb.write_text(code)
        with patch.object(credential_capture, '_run_preparation_command', side_effect=track):
            first = self._run_prepared()
            self.assertEqual(first['status'], 'capture_stage_failed', first)
            copies = list((self.config.parent / '.wechat-local-client').rglob('WeChat-debug.app'))
            self.assertEqual(len(copies), 1)
            identity = copies[0].stat().st_ino
            self._write_debugger(self.key, 0)
            second = self._run_prepared()
        self.assertEqual(second['status'], 'captured_and_verified', second)
        self.assertEqual(copies[0].stat().st_ino, identity)
        self.assertEqual(marker.read_text().splitlines().count('copy'), 1)
        self.assertEqual(marker.read_text().splitlines().count('sign'), 1)

    def _failed_reusable_copy(self):
        code = self.lldb.read_text().replace("mutation=mutation, require_outer_run=True", "mutation='unexpected_exit', fail='launch', require_outer_run=True")
        self.lldb.write_text(code)
        result = self._run_prepared()
        self.assertEqual(result['status'], 'capture_stage_failed', result)
        self._write_debugger(self.key, 0)
        copies = list((self.config.parent / '.wechat-local-client').rglob('WeChat-debug.app'))
        self.assertEqual(len(copies), 1)
        return copies[0]

    def test_reusable_copy_missing_or_source_changed_rebuilds(self):
        for change in ('missing', 'slot_missing', 'source'):
            with self.subTest(change=change):
                copy = self._failed_reusable_copy()
                if change == 'missing': shutil.rmtree(copy)
                elif change == 'slot_missing': shutil.rmtree(copy.parent)
                else:
                    info = self.contents / 'Info.plist'
                    data = plistlib.loads(info.read_bytes()); data['CFBundleVersion'] = 'synthetic-new'
                    info.write_bytes(plistlib.dumps(data))
                result = self._run_prepared()
                self.assertEqual(result['status'], 'captured_and_verified', result)
                copies = list((self.config.parent / '.wechat-local-client').rglob('WeChat-debug.app'))
                self.assertTrue(any(item != copy for item in copies))
                self.config.with_name('passphrase.bin').unlink()
                shutil.rmtree(self.config.parent / '.wechat-local-client')

    def test_replaced_copy_symlink_is_rejected_without_removal(self):
        copy = self._failed_reusable_copy()
        shutil.rmtree(copy)
        copy.symlink_to(self.app)
        result = self._run_prepared()
        self.assertEqual(result['status'], 'reusable_client_ambiguous', result)
        self.assertTrue(copy.is_symlink())
        self.assertTrue(self.app.exists())

    def test_replaced_copy_directory_is_rejected_without_removal(self):
        copy = self._failed_reusable_copy()
        displaced = copy.with_name('displaced.app')
        copy.rename(displaced)
        shutil.copytree(displaced, copy, symlinks=True)
        identity = copy.stat().st_ino
        result = self._run_prepared()
        self.assertEqual(result['status'], 'reusable_client_ambiguous', result)
        self.assertEqual(copy.stat().st_ino, identity)
        self.assertTrue(displaced.exists())

    def test_public_import_does_not_prepare_client(self):
        self.synthetic_capture.chmod(0o600)
        argv = [str(self.cli_bin/'wechat-local-setup'), '--account-root', str(self.account),
                '--config', str(self.config), '--data-home', str(self.output_home),
                '--passphrase-file', str(self.synthetic_capture)]
        with patch.object(credential_capture, '_client_root', side_effect=AssertionError('client preparation on import')), \
             patch.object(sys, 'argv', argv), contextlib.redirect_stdout(io.StringIO()) as out:
            result = source_setup.main()
        self.assertEqual(result, 0, out.getvalue())
        self.assertTrue(json.loads(out.getvalue())['status'].startswith('configured'))
        self.assertFalse((self.config.parent/'.wechat-local-client').exists())

    def test_prelaunch_failure_releases_lock_and_allows_next_default_capture(self):
        marker = self.root/'preparation-steps'
        original = credential_capture._run_preparation_command
        def track(step, argv):
            with marker.open('a') as stream: stream.write(step + '\n')
            return original(step, argv)
        code = self.lldb.read_text().replace('mutation=mutation, require_outer_run=True', "fail='environment', mutation=mutation, require_outer_run=True")
        self.lldb.write_text(code)
        with patch.object(credential_capture, '_run_preparation_command', side_effect=track):
            first = self._run_prepared()
            diagnostic = first['diagnostics']
            self.assertEqual(first['status'], 'capture_stage_failed', first)
            self.assertEqual(diagnostic['channel'], 'valid')
            self.assertEqual(diagnostic['stages']['launch'], 'not_started')
            self.assertIsNone(diagnostic['target_pid'])
            self.assertEqual(diagnostic['group_cleanup'], 'confirmed')
            self.assertFalse((self.config.parent/'.wechat-local-client'/'lock').exists())
            self._write_debugger(self.key, 0)
            second = self._run_prepared()
        self.assertEqual(second['status'], 'captured_and_verified', second)
        self.assertEqual(marker.read_text().splitlines().count('copy'), 1)
        self.assertEqual(marker.read_text().splitlines().count('sign'), 1)

    def test_unknown_diagnostic_keeps_reusable_client_lock(self):
        with patch.object(credential_capture, '_read_capture_diagnostic', return_value=credential_capture._unknown_capture_diagnostic('missing')):
            first = self._run_prepared()
        self.assertEqual(first['status'], 'capture_diagnostic_invalid', first)
        self.assertTrue((self.config.parent/'.wechat-local-client'/'lock').is_dir())
        self.assertEqual(self._run_prepared()['status'], 'reusable_client_locked')

    def test_uncertain_preparation_retains_lock_for_next_default_capture(self):
        original_config = self.config
        original = credential_capture._run_preparation_command
        for mode in ('copy', 'sign', 'reuse_verify'):
            with self.subTest(mode=mode):
                self.config = original_config.parent / mode / 'current-source.json'
                if mode == 'reuse_verify':
                    self._failed_reusable_copy()
                marker = self.root / 'debugger-ran'
                marker.unlink(missing_ok=True)
                calls = []
                def uncertain(step, argv):
                    calls.append(step)
                    if step == ('verify' if mode == 'reuse_verify' else mode):
                        raise credential_capture._PreparationCleanupUncertain('synthetic process group cleanup uncertain')
                    return original(step, argv)
                with patch.object(credential_capture, '_run_preparation_command', side_effect=uncertain):
                    first = self._run_prepared()
                    self.assertEqual(first['status'], 'preparation_cleanup_uncertain', first)
                    before = list(calls)
                    self.assertFalse(marker.exists())
                    self.assertFalse(self.config.exists())
                    self.assertFalse(self.config.with_name('passphrase.bin').exists())
                    second = self._run_prepared()
                    self.assertEqual(second['status'], 'reusable_client_locked', second)
                    self.assertEqual(calls, before)
                    self.assertFalse(marker.exists())
                    self.assertTrue((self.config.parent/'.wechat-local-client'/'lock').is_dir())
        self.config = original_config

    def test_lock_and_unconfirmed_cleanup_refuse_next_launch(self):
        root, lock, error = credential_capture._client_root(self.config)
        self.assertIsNone(error)
        result = self._run_prepared()
        self.assertEqual(result['status'], 'reusable_client_locked', result)
        self.assertFalse((self.root / 'debugger-ran').exists())
        credential_capture._release_client_lock(root, lock)
        code = self.lldb.read_text().replace('mutation=mutation, require_outer_run=True', "fail='target_termination', mutation=mutation, require_outer_run=True")
        self.lldb.write_text(code)
        result = self._run_prepared()
        self.assertEqual(result['status'], 'capture_stage_failed', result)
        self.assertTrue((root / 'lock').is_dir())
        (self.root / 'debugger-ran').unlink()
        self._write_debugger(self.key, 0)
        again = self._run_prepared()
        self.assertEqual(again['status'], 'reusable_client_locked', again)
        self.assertFalse((self.root / 'debugger-ran').exists())

    def test_default_preparation_copies_signs_and_uses_owned_bundle(self):
        import hashlib
        before = hashlib.sha256(self.executable.read_bytes()).digest()
        (self.contents / 'internal-link').symlink_to('MacOS/WeChat')
        marker = self.root / 'copy-path'
        code = self.lldb.read_text().replace('args=sys.argv[1:]',
            "args=sys.argv[1:]\ncopy=next(Path(" + repr(str(self.config.parent / '.wechat-local-client')) + ").rglob('WeChat-debug.app'))\nPath(" + repr(str(marker)) + ").write_text(str(copy))\nassert (copy/'Contents/MacOS/WeChat').is_file()")
        self.lldb.write_text(code)
        result = self._run_prepared()
        self.assertEqual(result['status'], 'captured_and_verified', result)
        self.assertEqual(hashlib.sha256(self.executable.read_bytes()).digest(), before)
        self.assertTrue(Path(marker.read_text()).is_dir())

    def test_external_symlink_is_rejected_before_signing_or_debugger(self):
        (self.contents / 'external').symlink_to('/usr/bin/true')
        result = self._run_prepared()
        self.assertEqual(result['status'], 'preparation_unsafe_bundle', result)
        self.assertFalse((self.root / 'debugger-ran').exists())
        self.assertFalse(self.config.exists())
        self.assertFalse(self.config.with_name('passphrase.bin').exists())

    def test_preparation_command_failure_stops_before_debugger(self):
        from unittest.mock import patch
        for name in ('copy', 'sign', 'verify'):
            with self.subTest(step=name):
                original = credential_capture._run_preparation_command
                def intercept(step, argv):
                    if step == name: return False, 'synthetic access denied /private/secret/fixture'
                    return original(step, argv)
                with patch.object(credential_capture, '_run_preparation_command', side_effect=intercept):
                    result = self._run_prepared()
                self.assertEqual(result['status'], 'preparation_' + name + '_failed', result)
                self.assertIn('synthetic access denied', result['error'])
                self.assertNotIn('/private/secret', result['error'])
                self.assertFalse((self.root / 'debugger-ran').exists())
                self.assertFalse(self.config.exists())
                self.assertFalse(self.config.with_name('passphrase.bin').exists())

    def test_preparation_outcome_uses_exit_status_not_error_text(self):
        original = credential_capture._run_preparation_command
        def warning(step, argv):
            result = original(step, argv)
            if step == 'sign':
                return (True, 'synthetic warning')
            return result
        with patch.object(credential_capture, '_run_preparation_command', side_effect=warning):
            result = self._run_prepared()
        self.assertEqual(result['status'], 'captured_and_verified', result)
        self.config.with_name('passphrase.bin').unlink()
        def empty_failure(step, argv):
            if step == 'sign': return False, ''
            return original(step, argv)
        with patch.object(credential_capture, '_run_preparation_command', side_effect=empty_failure):
            result = self._run_prepared()
        self.assertEqual(result['status'], 'preparation_sign_failed', result)
        self.assertEqual(result['error'], 'sign failed')

    def test_failed_command_with_nonprintable_error_still_fails(self):
        ok, error = credential_capture._run_preparation_command(
            'sign', [sys.executable, '-c', "import os; os.write(2, bytes([0])); raise SystemExit(7)"])
        self.assertFalse(ok)
        self.assertTrue(error)
        self.assertIn('7', error)

    def test_preparation_reader_failure_with_unknown_child_cleanup_is_not_silent(self):
        import types
        read_fd, write_fd = os.pipe(); os.close(write_fd)
        stream = os.fdopen(read_fd, 'rb')
        child = types.SimpleNamespace(pid=999999, stderr=stream)
        try:
            with patch.object(credential_capture.subprocess, 'Popen', return_value=child), \
                 patch.object(credential_capture.threading.Thread, 'start', side_effect=RuntimeError('reader failed')), \
                 patch.object(credential_capture, '_stop_owned_group', return_value=False):
                with self.assertRaises(credential_capture._PreparationCleanupUncertain):
                    credential_capture._run_preparation_command('copy', ['/bin/cp'])
        finally:
            stream.close()

    def test_uncertain_preparation_cleanup_preserves_copy_and_stops(self):
        marker = self.root / 'uncertain-copy-path'
        def uncertain(step, argv):
            copied = Path(argv[-1]); copied.mkdir()
            marker.write_text(str(copied))
            raise credential_capture._PreparationCleanupUncertain('synthetic interruption')
        with patch.object(credential_capture, '_run_preparation_command', side_effect=uncertain):
            result = self._run_prepared()
        self.assertEqual(result['status'], 'preparation_cleanup_uncertain')
        retained = Path(marker.read_text())
        self.assertTrue(retained.is_dir())
        self.assertFalse((self.root / 'debugger-ran').exists())
        shutil.rmtree(retained.parent)

    def test_unknown_target_cleanup_preserves_owned_bundle(self):
        self._write_debugger(self.key, 0, sleep=True)
        marker = self.root / 'copy-path'
        self.lldb.write_text(self.lldb.read_text().replace('args=sys.argv[1:]',
            "args=sys.argv[1:]\nPath(" + repr(str(marker)) + ").write_text(str(Path(args[1]).parent / 'WeChat-debug.app'))"))
        result = self._run_prepared(timeout=1)
        self.assertEqual(result['status'], 'debugger_cleanup_uncertain')
        retained = Path(marker.read_text())
        self.assertTrue(retained.is_dir())
        shutil.rmtree(retained.parent)  # Synthetic owned fixture after checking retention.

    def test_fake_debugger_capture_real_hmac_validate_all_shards_then_save_private(self) -> None:
        result = self._run()
        self.assertEqual(result["status"], "captured_and_verified")
        key_path = Path(str(result["passphrase_file"]))
        self.assertEqual(key_path.read_bytes(), self.key)
        self.assertEqual(stat.S_IMODE(key_path.stat().st_mode), 0o600)
        self.assertFalse(self.config.exists())
        self.assertNotIn(self.key.hex(), json.dumps(result))

    def test_capture_and_setup_validate_stable_shards_with_one_open_each(self):
        opens = []
        capture_open = credential_capture.open_encrypted_readonly
        setup_open = source_setup.open_encrypted_readonly
        real_run = subprocess.run
        def counted(stage, opener):
            def call(path, key): opens.append((stage, Path(path).name)); return opener(path, key)
            return call
        def lipo(argv, *args, **kwargs):
            if str(argv[0]) == '/usr/bin/lipo': return subprocess.CompletedProcess(argv, 0, b'x86_64\n', b'')
            return real_run(argv, *args, **kwargs)
        with patch.object(credential_capture, 'open_encrypted_readonly', side_effect=counted('capture', capture_open)), \
             patch.object(source_setup, 'open_encrypted_readonly', side_effect=counted('setup', setup_open)), \
             patch.object(credential_capture.subprocess, 'run', side_effect=lipo):
            rc, output = self._run_setup(timeout='10')
        self.assertEqual(rc, 0, output)
        for number in (0, 1):
            self.assertEqual(sum(name == f'message_{number}.db' for _,name in opens), 1, opens)
        self.assertNotIn(self.key.hex(), output)

    def test_changed_shard_after_capture_is_revalidated_in_setup(self):
        opens = []
        original_prepare = source_setup.prepare_account_source
        original_open = source_setup.open_encrypted_readonly
        def changed(*args, **kwargs):
            shard = self.message_root/'message_1.db'
            st = shard.stat(); os.utime(shard, ns=(st.st_atime_ns, st.st_mtime_ns + 1000000000))
            return original_prepare(*args, **kwargs)
        def counted(path, key):
            opens.append(Path(path).name); return original_open(path,key)
        with patch.object(source_setup, 'prepare_account_source', side_effect=changed), \
             patch.object(source_setup, 'open_encrypted_readonly', side_effect=counted):
            rc, output = self._run_setup(timeout='10')
        self.assertEqual(rc, 0, output)
        self.assertEqual(opens.count('message_1.db'), 1, opens)
        self.assertEqual(opens.count('message_0.db'), 0, opens)

    def test_wal_change_after_capture_reopens_changed_shard_and_sees_voice_schema(self):
        original_prepare = source_setup.prepare_account_source
        original_open = source_setup.open_encrypted_readonly
        opens = []; writers = []
        def changed(*args, **kwargs):
            shard = self.message_root/'message_1.db'
            writer = sqlcipher.connect(str(shard)); writers.append(writer)
            writer.execute("PRAGMA key='" + 'k'*32 + "'")
            writer.execute('PRAGMA cipher_compatibility=4')
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('CREATE TABLE VoiceInfo(chat_name_id, create_time, local_id, svr_id, voice_data)')
            writer.execute('CREATE TABLE Name2Id(user_name)'); writer.commit()
            self.assertTrue(Path(str(shard)+'-wal').exists())
            return original_prepare(*args, **kwargs)
        def counted(path, key): opens.append(Path(path).name); return original_open(path,key)
        try:
            with patch.object(source_setup, 'prepare_account_source', side_effect=changed), \
                 patch.object(source_setup, 'open_encrypted_readonly', side_effect=counted):
                rc, output = self._run_setup(timeout='10')
            self.assertEqual(rc, 0, output)
            self.assertEqual(opens.count('message_0.db'), 0, opens)
            self.assertEqual(opens.count('message_1.db'), 1, opens)
            self.assertIn(str((self.message_root/'message_1.db').resolve()), json.loads(self.config.read_text())['voice_databases'])
            self.assertNotIn('_capture_observations', output + self.config.read_text())
        finally:
            for writer in writers: writer.close()

    def test_replaced_database_after_capture_is_reopened_and_revalidated(self):
        original_prepare = source_setup.prepare_account_source
        original_open = source_setup.open_encrypted_readonly
        opens = []
        def changed(*args, **kwargs):
            shard = self.message_root/'message_1.db'; replacement = shard.with_suffix('.replacement')
            shutil.copyfile(shard, replacement); os.replace(replacement, shard)
            return original_prepare(*args, **kwargs)
        def counted(path, key): opens.append(Path(path).name); return original_open(path,key)
        with patch.object(source_setup, 'prepare_account_source', side_effect=changed), \
             patch.object(source_setup, 'open_encrypted_readonly', side_effect=counted):
            rc, output = self._run_setup(timeout='10')
        self.assertEqual(rc, 0, output)
        self.assertEqual(opens.count('message_0.db'), 0, opens)
        self.assertEqual(opens.count('message_1.db'), 1, opens)

    def test_changed_saved_capture_is_not_reused_or_deleted(self):
        original = source_setup.prepare_account_source
        def changed(*args, **kwargs):
            Path(kwargs['passphrase_file']).write_bytes(b'x'*32)
            return original(*args, **kwargs)
        with patch.object(source_setup, 'prepare_account_source', side_effect=changed):
            rc, output = self._run_setup(timeout='10')
        self.assertEqual(rc, 2, output)
        self.assertEqual(json.loads(output)['status'], 'credential_source_changed')
        self.assertFalse(self.config.exists())
        self.assertEqual(self.config.with_name('passphrase.bin').read_bytes(), b'x'*32)
        self.assertNotIn((b'x'*32).hex(), output)

    def test_changed_shard_before_persistence_fails_without_saving_key(self):
        original = credential_capture._verify_all_shards
        def changed(*args, **kwargs):
            result = original(*args, **kwargs)
            shard = self.message_root/'message_1.db'
            st = shard.stat(); os.utime(shard, ns=(st.st_atime_ns, st.st_mtime_ns + 1000000000))
            return result
        with patch.object(credential_capture, '_verify_all_shards', side_effect=changed):
            rc, output = self._run_setup(timeout='10')
        self.assertEqual(rc, 2, output)
        self.assertEqual(json.loads(output)['status'], 'message_source_changed')
        self.assertFalse(self.config.exists())
        self.assertFalse(self.config.with_name('passphrase.bin').exists())

    def test_replaced_credential_parent_symlink_is_preserved_and_rejected(self):
        original = source_setup.prepare_account_source
        parent = self.config.parent
        moved = parent.with_name(parent.name + '-moved')
        def changed(*args, **kwargs):
            parent.rename(moved)
            parent.symlink_to(moved, target_is_directory=True)
            return original(*args, **kwargs)
        with patch.object(source_setup, 'prepare_account_source', side_effect=changed):
            rc, output = self._run_setup(timeout='10')
        self.assertEqual(rc, 2, output)
        self.assertEqual(json.loads(output)['status'], 'credential_source_changed')
        self.assertTrue(parent.is_symlink())
        self.assertTrue((moved/'passphrase.bin').is_file())
        self.assertFalse((moved/self.config.name).exists())

    def test_replaced_saved_capture_symlink_is_preserved_and_rejected(self):
        original = source_setup.prepare_account_source
        outside = self.root/'foreign-key'; outside.write_bytes(b'f'*32); outside.chmod(0o600)
        def changed(*args, **kwargs):
            path = Path(kwargs['passphrase_file']); path.unlink(); path.symlink_to(outside)
            return original(*args, **kwargs)
        with patch.object(source_setup, 'prepare_account_source', side_effect=changed):
            rc, output = self._run_setup(timeout='10')
        self.assertEqual(rc, 2, output)
        self.assertEqual(json.loads(output)['status'], 'credential_source_changed')
        self.assertTrue(self.config.with_name('passphrase.bin').is_symlink())
        self.assertEqual(outside.read_bytes(), b'f'*32)
        self.assertFalse(self.config.exists())

    def test_public_setup_cli_resumes_after_capture_and_verification(self) -> None:
        output = io.StringIO()
        argv = [
            str(self.cli_bin / "wechat-local-setup"), "--account-root", str(self.account),
            "--config", str(self.config), "--data-home", str(self.output_home),
            "--acquire-passphrase", "--wechat-executable", str(self.executable),
            "--wechat-data-home", str(self.data_home), "--lldb-executable", str(self.lldb),
            "--capture-timeout", "3",
        ]
        with patch.object(sys, "argv", argv), contextlib.redirect_stdout(output):
            result = source_setup.main()
        self.assertEqual(result, 0)
        response = json.loads(output.getvalue())
        self.assertIn(response["status"], {"configured", "configured_with_voice_incomplete"})
        profile = json.loads(self.config.read_text())
        passphrase = Path(profile["passphrase_file"])
        self.assertEqual(passphrase.read_bytes(), self.key)
        self.assertEqual(stat.S_IMODE(passphrase.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.config.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.config.parent.stat().st_mode), 0o700)
        self.assertNotIn(self.key.hex(), output.getvalue())

    def test_setup_failure_after_verified_capture_removes_only_its_key(self) -> None:
        output = io.StringIO()
        argv = [
            str(self.cli_bin / "wechat-local-setup"), "--account-root", str(self.account),
            "--config", str(self.config), "--data-home", str(self.output_home),
            "--acquire-passphrase", "--wechat-executable", str(self.executable),
            "--wechat-data-home", str(self.data_home), "--lldb-executable", str(self.lldb),
            "--capture-timeout", "3",
        ]
        with patch.object(sys, "argv", argv), patch.object(
            source_setup, "prepare_account_source", return_value={"status": "message_source_unavailable"}
        ), contextlib.redirect_stdout(output):
            result = source_setup.main()
        self.assertEqual(result, 2)
        self.assertEqual(json.loads(output.getvalue())["status"], "message_source_unavailable")
        self.assertFalse(self.config.exists())
        self.assertFalse((self.config.parent / "passphrase.bin").exists())

    def test_resume_failure_does_not_delete_replacement_key_path(self) -> None:
        original_capture = credential_capture.capture_and_save_passphrase
        self.config.parent.mkdir(parents=True)
        self.config.parent.chmod(0o700)
        replacement = self.config.parent / "replacement.bin"
        replacement.write_bytes(b"foreign file")
        def capture_then_replace(**kwargs):
            result = original_capture(**kwargs)
            self.assertEqual(result["status"], "captured_and_verified")
            key_path = Path(result["passphrase_file"])
            os.replace(replacement, key_path)
            return result
        argv = [
            str(self.cli_bin / "wechat-local-setup"), "--account-root", str(self.account), "--config", str(self.config),
            "--data-home", str(self.output_home), "--acquire-passphrase", "--wechat-executable", str(self.executable),
            "--wechat-data-home", str(self.data_home), "--lldb-executable", str(self.lldb), "--capture-timeout", "3",
        ]
        output = io.StringIO()
        with patch.object(sys, "argv", argv), patch.object(
            credential_capture, "capture_and_save_passphrase", side_effect=capture_then_replace
        ), patch.object(source_setup, "prepare_account_source", return_value={"status": "message_source_unavailable"}), contextlib.redirect_stdout(output):
            result = source_setup.main()
        self.assertEqual(result, 2)
        self.assertEqual((self.config.parent / "passphrase.bin").read_bytes(), b"foreign file")

    def test_capture_writer_exception_does_not_delete_replaced_file(self) -> None:
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            root = Path(temporary); root.chmod(0o700)
            target = root / "secret.bin"
            replacement = root / "replacement.bin"
            replacement.write_bytes(b"foreign")
            def replace_then_fail(fd):
                os.replace(replacement, target)
                raise OSError("synthetic fsync failure")
            with patch.object(credential_capture.os, "fsync", side_effect=replace_then_fail):
                created, identity = credential_capture._write_secret_exclusive(target, self.key)
            self.assertFalse(created)
            self.assertIsNone(identity)
            self.assertEqual(target.read_bytes(), b"foreign")

    def test_config_write_io_error_returns_failure_and_removes_owned_key(self) -> None:
        real_prepare = source_setup.prepare_account_source
        replacement = self.config.parent / "foreign-config.json"
        self.config.parent.mkdir(parents=True, exist_ok=True)
        self.config.parent.chmod(0o700)
        replacement.write_text("foreign config")
        def fail_config_fsync(*args, **kwargs):
            def replace_then_fail(fd):
                os.replace(replacement, self.config)
                raise OSError("synthetic config fsync failure")
            with patch.object(source_setup.os, "fsync", side_effect=replace_then_fail):
                return real_prepare(*args, **kwargs)
        with patch.object(source_setup, "prepare_account_source", side_effect=fail_config_fsync):
            result, output = self._run_setup()
        self.assertEqual(result, 2)
        self.assertEqual(json.loads(output)["status"], "config_write_failed")
        self.assertEqual(self.config.read_text(), "foreign config")
        self.assertFalse((self.config.parent / "passphrase.bin").exists())

    def test_invalid_output_locations_are_rejected_before_debugger_or_source_writes(self) -> None:
        inside_config = self.account / "private" / "current-source.json"
        result, output = self._run_setup(config=inside_config)
        self.assertEqual(result, 2)
        self.assertEqual(json.loads(output)["status"], "config_must_be_outside_account_root")
        self.assertFalse((self.root / "debugger-ran").exists())
        self.assertFalse(inside_config.parent.exists())
        (self.root / "debugger-ran").unlink(missing_ok=True)
        inside_data_home = self.account / "output"
        result, output = self._run_setup(data_home=inside_data_home)
        self.assertEqual(result, 2)
        self.assertEqual(json.loads(output)["status"], "data_home_must_be_outside_account_root")
        self.assertFalse((self.root / "debugger-ran").exists())
        self.assertFalse(inside_data_home.exists())

    def test_explicit_zero_timeout_waits_without_deadline(self) -> None:
        self.lldb.write_text(self.lldb.read_text().replace('execute_driver(script, payload=', 'time.sleep(0.15)\nexecute_driver(script, payload='))
        code, output = self._run_setup(timeout="0")
        self.assertEqual(code, 0)
        self.assertIn(json.loads(output)["status"], {"configured", "configured_with_voice_incomplete"})

    def test_isolated_launch_error_is_drained_and_specific_reason_retained(self) -> None:
        code = self.lldb.read_text()
        code = code.replace('execute_driver(script, payload=', "print('ordinary application log ' * 300000)\nprint('error: Cannot launch /private/secret/account: process exited with status 9 ' + 'ab'*32)\nexecute_driver(script, fail='launch', payload=")
        self.lldb.write_text(code)
        result = self._run(timeout=3)
        self.assertEqual(result['status'], 'capture_stage_failed')
        diagnostics = result['diagnostics']
        self.assertIn('Cannot launch', ' '.join(diagnostics['debugger_errors']))
        self.assertNotIn('/private/secret/account', json.dumps(result))
        self.assertNotIn('ab'*32, json.dumps(result))
        self.assertNotIn('ordinary application log', json.dumps(result))
        self.assertEqual(diagnostics['target_exit_status'], 9)
        self.assertFalse(self.config.exists())
        self.assertFalse(self.config.with_name('passphrase.bin').exists())

    def test_default_wait_remains_bounded_and_cancellation_stops_owned_group(self) -> None:
        real_wait = subprocess.Popen.wait
        waits = []
        def observe_wait(process, timeout=None):
            if Path(process.args[0]).resolve() == self.lldb.resolve():
                waits.append(timeout)
            return real_wait(process, timeout=timeout)
        with patch.object(subprocess.Popen, 'wait', observe_wait):
            result = credential_capture.capture_and_save_passphrase(
                account_root=self.account, wechat_executable=self.executable,
                wechat_data_home=self.data_home, lldb_executable=self.lldb, config_path=self.config)
        self.assertEqual(result['status'], 'captured_and_verified')
        self.assertEqual(waits[0], 60.0)
        self.config.with_name('passphrase.bin').unlink()
        interrupted = []
        def cancel_wait(process, timeout=None):
            if Path(process.args[0]).resolve() == self.lldb.resolve() and not interrupted:
                interrupted.append(True)
                raise KeyboardInterrupt()
            return real_wait(process, timeout=timeout)
        before = set(Path(tempfile.gettempdir()).glob('wechat-local-capture-*'))
        try:
            with patch.object(subprocess.Popen, 'wait', cancel_wait):
                result = self._run(timeout=0)
            self.assertEqual(result['status'], 'debugger_cleanup_uncertain')
            self.assertEqual(result['failure_reason'], 'debugger_cancelled')
            self.assertTrue(result['diagnostics']['debugger_cancelled'])
            self.assertFalse(result['diagnostics']['debugger_timeout'])
            self.assertEqual(result['diagnostics']['group_cleanup'], 'confirmed')
            self.assertFalse(self.config.exists())
            self.assertFalse(self.config.with_name('passphrase.bin').exists())
        finally:
            for stage in set(Path(tempfile.gettempdir()).glob('wechat-local-capture-*')) - before:
                shutil.rmtree(stage)

    def test_nan_timeout_is_rejected_before_debugger(self) -> None:
        result, output = self._run_setup(timeout="nan")
        self.assertEqual(result, 2)
        self.assertEqual(json.loads(output)["status"], "capture_timeout_invalid")
        self.assertFalse((self.root / "debugger-ran").exists())

    def test_timeout_with_detached_child_is_reported_uncertain_and_secret_removed(self) -> None:
        self._write_debugger(self.key, 0, sleep=True, spawn_detached_child=True)
        before = set(Path(tempfile.gettempdir()).glob("wechat-local-capture-*"))
        child_pid = None
        try:
            result = self._run(timeout=1.0)
            self.assertEqual(result["status"], "debugger_cleanup_uncertain")
            self.assertEqual(result["failure_reason"], "debugger_timeout")
            self.assertTrue(result["diagnostics"]["debugger_timeout"])
            self.assertEqual(result["diagnostics"]["target_cleanup"], "unknown")
            self.assertEqual(result["cleanup_state"], "private_stage_retained_without_capture")
            child_pid = int((self.root / "detached-child-pid").read_text())
            os.kill(child_pid, 0)
            self.assertFalse((self.config.parent / "passphrase.bin").exists())
            retained = list(Path(tempfile.gettempdir()).glob("wechat-local-capture-*"))
            self.assertTrue(any(path.is_dir() and (path / "capture.lldb").is_file() and not (path / "captured.bin").exists() for path in retained))
        finally:
            if child_pid is None and (self.root / "detached-child-pid").exists():
                child_pid = int((self.root / "detached-child-pid").read_text())
            if child_pid is not None:
                try: os.kill(child_pid, 9)
                except ProcessLookupError: pass
            after = set(Path(tempfile.gettempdir()).glob("wechat-local-capture-*"))
            for stage in after - before:
                if stage.is_dir():
                    import shutil
                    shutil.rmtree(stage)

    def test_timeout_replaced_capture_is_preserved_and_reported_as_unverified(self) -> None:
        self._write_debugger(self.key, 0, sleep=True, replace_capture=True)
        before = set(Path(tempfile.gettempdir()).glob("wechat-local-capture-*"))
        try:
            result = self._run(timeout=1.0)
            self.assertEqual(result["status"], "debugger_cleanup_uncertain")
            self.assertEqual(result["failure_reason"], "debugger_timeout")
            self.assertTrue(result["diagnostics"]["debugger_timeout"])
            self.assertEqual(result["diagnostics"]["target_cleanup"], "unknown")
            self.assertEqual(result["cleanup_state"], "private_stage_retained_capture_identity_changed")
            self.assertNotIn(self.key.hex(), json.dumps(result))
            stages = set(Path(tempfile.gettempdir()).glob("wechat-local-capture-*")) - before
            self.assertEqual(len(stages), 1)
            stage = next(iter(stages))
            self.assertEqual((stage / "captured.bin").read_bytes(), self.key)
            self.assertEqual(stat.S_IMODE(stage.stat().st_mode), 0o700)
            self.assertFalse((self.config.parent / "passphrase.bin").exists())
        finally:
            after = set(Path(tempfile.gettempdir()).glob("wechat-local-capture-*"))
            for stage in after - before:
                if stage.is_dir():
                    import shutil
                    shutil.rmtree(stage)

    def test_fifo_capture_output_is_rejected_without_blocking(self) -> None:
        self._write_debugger(self.key, 0, fifo_capture=True)
        code = (
            "import json; from pathlib import Path; from wechat_local.credential_capture import capture_and_save_passphrase; "
            f"r=capture_and_save_passphrase(account_root=Path({str(self.account)!r}), "
            f"wechat_executable=Path({str(self.executable)!r}), wechat_data_home=Path({str(self.data_home)!r}), "
            f"lldb_executable=Path({str(self.lldb)!r}), config_path=Path({str(self.config)!r}), timeout_seconds=2); "
            "print(json.dumps(r))"
        )
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).parents[1] / "src"))
        process = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env, timeout=3)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(json.loads(process.stdout)["status"], "capture_stage_failed")
        self.assertEqual(json.loads(process.stdout)["diagnostics"]["failed_stage"], "capture_write")

    def test_wrong_key_and_short_capture_do_not_persist(self) -> None:
        self._write_debugger(b"x" * 32, 0)
        result = self._run()
        self.assertEqual(result["status"], "capture_key_invalid")
        self.assertFalse(self.config.exists())
        self.assertFalse((self.config.parent / "passphrase.bin").exists())
        self._write_debugger(self.key, 0, short=True)
        result = self._run()
        self.assertEqual(result["status"], "capture_stage_failed")
        self.assertEqual(result["diagnostics"]["failed_stage"], "capture_write")
        self.assertFalse((self.config.parent / "passphrase.bin").exists())
        self._write_debugger(self.key, 0, long=True)
        result = self._run()
        self.assertEqual(result["status"], "capture_stage_failed")
        self.assertEqual(result["diagnostics"]["failed_stage"], "capture_write")
        self.assertFalse((self.config.parent / "passphrase.bin").exists())

    def test_one_wrong_message_shard_rejects_capture(self) -> None:
        (self.message_root / "message_1.db").unlink()
        self._make_db(self.message_root / "message_1.db", b"x" * 32)
        result = self._run()
        self.assertEqual(result["status"], "capture_key_invalid")
        self.assertFalse(self.config.exists())
        self.assertFalse((self.config.parent / "passphrase.bin").exists())

    def test_timeout_cleans_only_owned_debugger_group_and_stage(self) -> None:
        self._write_debugger(self.key, 0, sleep=True)
        result = self._run(timeout=0.2)
        self.assertEqual(result["status"], "debugger_cleanup_uncertain")
        self.assertEqual(result["failure_reason"], "debugger_timeout")
        self.assertEqual(result["cleanup_state"], "private_stage_retained_without_capture")
        self.assertFalse(self.config.exists())

    def test_target_and_source_layout_must_be_bound_before_spawn(self) -> None:
        outside = self.root / "unrelated home"
        outside.mkdir()
        result = credential_capture.capture_and_save_passphrase(
            account_root=self.account,
            wechat_executable=self.executable,
            wechat_data_home=outside,
            lldb_executable=self.lldb,
            config_path=self.config,
            timeout_seconds=1,
        )
        self.assertEqual(result["status"], "target_source_unbound")
        self.assertFalse(self.config.exists())
        self.assertFalse((self.root / "debugger-ran").exists())

    def test_default_data_home_derivation_uses_only_selected_canonical_layout(self) -> None:
        resolved, error = credential_capture._data_home_from_selected_account(self.account)
        self.assertIsNone(error)
        self.assertEqual(resolved, self.data_home.resolve())
        copied = self.root / "copied-account"
        copied.mkdir()
        resolved, error = credential_capture._data_home_from_selected_account(copied)
        self.assertIsNone(resolved)
        self.assertEqual(error, "target_source_unbound")

    def test_existing_config_and_insecure_parent_are_never_modified(self) -> None:
        self.config.parent.mkdir(parents=True)
        self.config.write_text("existing")
        result = self._run()
        self.assertEqual(result["status"], "config_exists")
        self.assertEqual(self.config.read_text(), "existing")
        self.assertFalse((self.root / "debugger-ran").exists())
        self.config.unlink()
        self.config.parent.chmod(0o755)
        result = self._run()
        self.assertEqual(result["status"], "credential_destination_unavailable")
        self.assertEqual(stat.S_IMODE(self.config.parent.stat().st_mode), 0o755)
        self.assertFalse((self.root / "debugger-ran").exists())

    def test_timeout_kills_only_owned_process_group_and_removes_stage(self) -> None:
        self._write_debugger(self.key, 0, sleep=True, spawn_child=True)
        unrelated = subprocess.Popen(["/bin/sleep", "30"])
        before = set(Path(tempfile.gettempdir()).glob("wechat-local-capture-*"))
        try:
            result = self._run(timeout=1.0)
            self.assertEqual(result["status"], "debugger_cleanup_uncertain")
            child_pid = int((self.root / "child-pid").read_text())
            import time
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                try:
                    os.kill(child_pid, 0)
                except ProcessLookupError:
                    break
                time.sleep(0.05)
            else:
                self.fail("owned debugger child survived process-group cleanup")
            self.assertIsNone(unrelated.poll())
            after = set(Path(tempfile.gettempdir()).glob("wechat-local-capture-*"))
            retained = after - before
            self.assertEqual(len(retained), 1)
            self.assertEqual({p.name for p in next(iter(retained)).iterdir()}, {"capture.lldb", "diagnostic.json"})
            self.assertFalse(self.config.exists())
        finally:
            unrelated.terminate()
            unrelated.wait()
            after = set(Path(tempfile.gettempdir()).glob("wechat-local-capture-*"))
            for stage in after - before:
                if stage.is_dir():
                    import shutil
                    shutil.rmtree(stage)


if __name__ == "__main__":
    unittest.main()
