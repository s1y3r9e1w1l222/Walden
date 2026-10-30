#!/usr/bin/env python3
"""Render a private LLDB capture script without repository-specific paths."""

from __future__ import annotations

import argparse
import os
import stat
from pathlib import Path
from typing import Union


def lldb_quote(path: Union[str, Path]) -> str:
    value = str(path)
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise ValueError("LLDB values containing control characters are not supported")
    if "`" in value:
        raise ValueError("LLDB values containing backticks are not supported")
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _private_directory(path: Path) -> bool:
    try:
        info = path.lstat()
    except OSError:
        return False
    return stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode) and info.st_uid == os.getuid() and (
        stat.S_IMODE(info.st_mode) & 0o077
    ) == 0


def render(executable: Path, data_home: Path, passphrase_output: Path) -> str:
    return "\n".join(
        [
            "settings set target.preload-symbols false",
            f"target create {lldb_quote(executable)}",
            "settings set target.env-vars "
            + lldb_quote(f"CFFIXED_USER_HOME={data_home}"),
            (
                "breakpoint set -n CCKeyDerivationPBKDF "
                "-c '$rdi == 2 && $rdx == 32 && $r8 == 16 && $r9 == 5 "
                "&& *(unsigned int*)($rsp + 8) == 256000'"
            ),
            "breakpoint command add 1",
            (
                "memory read --force --outfile "
                f"{lldb_quote(passphrase_output)} "
                "--binary --size 1 --count 32 $rsi"
            ),
            "breakpoint disable 1",
            "process kill",
            "DONE",
            "run",
            "",
        ]
    )


def _safe_diagnostic_text(value):
    """Bound debugger errors; remove paths, addresses and possible secret tokens."""
    import re
    text = ''.join(c if 32 <= ord(c) < 127 else ' ' for c in str(value)[:2048])
    text = re.sub(r'''(?:[A-Za-z]:)?/[^\s'"<>]*''', '[path]', text)
    text = re.sub(r'(?<![A-Za-z0-9])(?:[0-9a-fA-F]{2}[\s,:-]+)+[0-9a-fA-F]{2}(?![A-Za-z0-9])', '[redacted]', text)
    text = re.sub(r'\b(?:0x)?[0-9a-fA-F]{8,}\b', '[redacted]', text)
    text = re.sub(r'[A-Za-z0-9_+=-]{24,}', '[redacted]', text)
    return ' '.join(text.split())[:384]


def _safe_exception(error):
    lines = str(error).splitlines()
    detail = lines[-1] if lines else ''
    return _safe_diagnostic_text(type(error).__name__ + (': ' + detail if detail else ''))


class _CaptureSession:
    # Embedded verbatim. Only ordinary LLDB source commands may launch the target.
    def __init__(self, executable, data_home, output, diagnostic, token, diagnostic_identity, output_identity):
        import json, os, stat, time, lldb
        self.json, self.os, self.stat, self.time, self.lldb = json, os, stat, time, lldb
        self.executable, self.data_home, self.output = executable, data_home, output
        self.diagnostic, self.diagnostic_identity, self.output_identity = diagnostic, diagnostic_identity, output_identity
        self.record = dict(version=1, token=token, debugger_pid=os.getpid(),
            stages={s: 'not_started' for s in ('target_create', 'environment', 'breakpoint', 'launch', 'breakpoint_hit', 'capture_write', 'target_termination')},
            last_started=None, last_completed=None, failed_stage=None, elapsed_ms=0,
            target_pid=None, target_state='unknown', target_exit='unknown',
            target_exit_status=None, target_exit_description=None, command_error=None,
            breakpoint_commands='not_entered', failure_detail=None, stop_source='none',
            stop_state='unknown', stop_facts=[], stop_threads_omitted=0, stop_error=None)
        self.started = time.monotonic()
        self.target = self.process = None
        self.fd = os.open(diagnostic, os.O_WRONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
        self.publish()

    def publish(self):
        os, stat = self.os, self.stat
        self.record['elapsed_ms'] = max(0, int((self.time.monotonic() - self.started) * 1000))
        info, current = os.fstat(self.fd), os.lstat(self.diagnostic)
        if ((info.st_dev, info.st_ino) != self.diagnostic_identity
                or (current.st_dev, current.st_ino) != self.diagnostic_identity
                or not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1):
            raise RuntimeError('diagnostic unavailable')
        data = self.json.dumps(self.record, separators=(',', ':')).encode('ascii')
        if len(data) > 4096:
            raise RuntimeError('diagnostic too large')
        os.lseek(self.fd, 0, os.SEEK_SET)
        os.ftruncate(self.fd, 0)
        if os.write(self.fd, data) != len(data):
            raise RuntimeError('diagnostic incomplete')
        os.fsync(self.fd)

    def begin(self, name):
        self.record['last_started'] = name
        self.record['stages'][name] = 'started'
        self.publish()

    def complete(self, name):
        self.record['stages'][name] = 'completed'
        self.record['last_completed'] = name
        self.publish()

    def fail(self, name, detail):
        self.record['stages'][name] = 'failed'
        if self.record['failed_stage'] is None:
            self.record['failed_stage'] = name
            self.record['failure_detail'] = detail
        self.publish()

    def command(self, text):
        result = self.lldb.SBCommandReturnObject()
        self.lldb.debugger.GetCommandInterpreter().HandleCommand(text, result)
        if not result.Succeeded():
            # Never read command output: memory read writes only to the private file.
            error = _safe_diagnostic_text(result.GetError() or 'command failed')
            self.record['command_error'] = error
            raise RuntimeError('capture command failed')

    def observe(self):
        if self.target is not None:
            self.process = self.target.GetProcess()
        if self.process is None or not self.process.IsValid():
            return
        pid = self.process.GetProcessID()
        if type(pid) is int and 0 < pid < 2147483648:
            self.record['target_pid'] = pid
        state = self.process.GetState()
        lldb = self.lldb
        self.record['target_state'] = {lldb.eStateStopped: 'stopped', lldb.eStateRunning: 'running',
            lldb.eStateExited: 'exited', lldb.eStateCrashed: 'crashed', lldb.eStateDetached: 'detached'}.get(state, 'unknown')
        if state in (lldb.eStateExited, lldb.eStateCrashed):
            self.record['target_exit'] = 'unexpected'
            if state == lldb.eStateExited:
                code = self.process.GetExitStatus()
                if type(code) is int and -2147483648 <= code < 2147483648:
                    self.record['target_exit_status'] = code
                description = self.process.GetExitDescription()
                if description:
                    self.record['target_exit_description'] = _safe_diagnostic_text(description)

    def prepare(self):
        lldb = self.lldb
        try:
            lldb.debugger.SetAsync(False)
            self.begin('target_create')
            self.command('settings set target.preload-symbols false')
            self.command('target create ' + self.executable)
            self.target = lldb.debugger.GetSelectedTarget()
            if not self.target.IsValid():
                raise RuntimeError('invalid target')
            self.complete('target_create')
            self.begin('environment')
            self.command('settings set target.env-vars ' + self.data_home)
            self.command('settings set target.output-path /dev/null')
            self.command('settings set target.error-path /dev/null')
            self.complete('environment')
            self.begin('breakpoint')
            self.command("breakpoint set -n CCKeyDerivationPBKDF -c '$rdi == 2 && $rdx == 32 && $r8 == 16 && $r9 == 5 && *(unsigned int*)($rsp + 8) == 256000'")
            if self.target.GetNumBreakpoints() != 1:
                raise RuntimeError('ambiguous breakpoint')
            breakpoint = self.target.GetBreakpointAtIndex(0)
            if not breakpoint.IsValid() or breakpoint.GetID() <= 0:
                raise RuntimeError('invalid breakpoint')
            self.breakpoint_id = breakpoint.GetID()
            if self.breakpoint_id != 1:
                raise RuntimeError('invalid breakpoint')
        except Exception as error:
            if self.record['command_error'] is None:
                self.record['command_error'] = _safe_exception(error)
            self.fail(self.record['last_started'], 'setup_command_failed')
            # An error stops ordinary command-source processing before its run line.
            raise RuntimeError('capture setup failed') from None

    def armed(self):
        self.complete('breakpoint')
        self.begin('launch')

    def snapshot_stop(self, source):
        # Observe all bounded threads before Kill. Selected thread is not a gate
        # and need not be the thread whose ordinary breakpoint commands are running.
        if self.record['stop_source'] != 'none':
            return
        self.record['stop_source'] = source
        self.record['stop_state'] = self.record['target_state']
        try:
            if self.process is None or not self.process.IsValid():
                return
            count = self.process.GetNumThreads()
            if type(count) is not int or not 0 <= count < 2147483648:
                raise ValueError('invalid thread count')
            self.record['stop_threads_omitted'] = max(0, count - 8)
            for index in range(min(count, 8)):
                thread = self.process.GetThreadAtIndex(index)
                if not thread.IsValid():
                    continue
                reason = thread.GetStopReason()
                if type(reason) is not int or not 0 <= reason <= 255:
                    raise ValueError('invalid stop reason')
                fact = dict(thread_index=index, reason=reason, breakpoint_id=None, location_id=None, signal=None)
                size = thread.GetStopReasonDataCount()
                if reason == self.lldb.eStopReasonBreakpoint and size >= 2:
                    for field, position in (('breakpoint_id', 0), ('location_id', 1)):
                        value = thread.GetStopReasonDataAtIndex(position)
                        if type(value) is int and 0 <= value < 2147483648:
                            fact[field] = value
                elif reason == self.lldb.eStopReasonSignal and size >= 1:
                    value = thread.GetStopReasonDataAtIndex(0)
                    if type(value) is int and 0 <= value <= 255:
                        fact['signal'] = value
                self.record['stop_facts'].append(fact)
        except Exception as error:
            self.record['stop_error'] = _safe_exception(error)

    def command_entered(self):
        # This marker never selects/checks a thread or performs the memory read.
        # LLDB executes the following ordinary commands in its triggering context.
        self.record['breakpoint_commands'] = 'entered'
        try:
            self.complete('launch')
            self.begin('breakpoint_hit')
            self.complete('breakpoint_hit')
            self.begin('capture_write')
            try:
                self.observe()
            except Exception as error:
                self.record['stop_error'] = _safe_exception(error)
            self.snapshot_stop('breakpoint_commands')
            self.publish()
        except Exception as error:
            # A diagnostic failure must not replace the original LLDB action.
            # The parent still rejects missing/invalid/incomplete diagnostics.
            self.record['command_error'] = _safe_exception(error)

    def capture_checked(self):
        try:
            info = self.os.lstat(output_path)
            if (not self.stat.S_ISREG(info.st_mode) or (info.st_dev, info.st_ino) != self.output_identity
                    or info.st_uid != self.os.getuid() or self.stat.S_IMODE(info.st_mode) != 0o600
                    or info.st_nlink != 1):
                self.record['breakpoint_commands'] = 'failed'
                self.fail('capture_write', 'capture_output_identity_invalid')
                return
            if info.st_size != 32:
                self.record['breakpoint_commands'] = 'failed'
                self.fail('capture_write', 'capture_output_length_invalid')
                return
            self.record['breakpoint_commands'] = 'completed'
            self.complete('capture_write')
        except Exception as error:
            self.record['breakpoint_commands'] = 'failed'
            self.record['command_error'] = _safe_exception(error)
            self.fail('capture_write', 'capture_check_internal_error')

    def finish(self):
        if self.fd is None:
            return
        try:
            try:
                self.observe()
            except Exception as error:
                self.record['command_error'] = _safe_exception(error)
            self.snapshot_stop('run_return')
            if self.record['stages']['breakpoint'] == 'started':
                self.fail('breakpoint', 'breakpoint_setup_incomplete')
            if self.record['stages']['launch'] == 'started':
                if self.record['target_pid'] is None or self.record['target_state'] in ('unknown', 'exited', 'crashed'):
                    self.fail('launch', 'launch_unconfirmed')
                else:
                    self.complete('launch')
                    self.begin('breakpoint_hit')
                    self.fail('breakpoint_hit', 'breakpoint_commands_not_entered')
            if self.record['stages']['capture_write'] == 'started':
                self.record['breakpoint_commands'] = 'failed'
                self.fail('capture_write', 'capture_commands_incomplete')
            if self.process is not None and self.record['target_pid'] is not None:
                try:
                    self.begin('target_termination')
                    if self.record['target_state'] == 'exited':
                        self.complete('target_termination')
                    elif self.record['target_state'] in ('stopped', 'running', 'crashed'):
                        killed = self.process.Kill()
                        if not killed.Success():
                            detail = _safe_diagnostic_text(killed.GetCString() or 'unknown error')
                            previous = self.record['command_error']
                            self.record['command_error'] = _safe_diagnostic_text(
                                'Kill failed: ' + detail + ('; ' + previous if previous else ''))
                        state = self.process.GetState()
                        if killed.Success() and state != self.lldb.eStateExited:
                            # Public state may advance only when this process's state event is consumed.
                            listener = self.lldb.debugger.GetListener()
                            event = self.lldb.SBEvent()
                            deadline = self.time.monotonic() + 2.0
                            while state != self.lldb.eStateExited and self.time.monotonic() < deadline:
                                listener.WaitForEventForBroadcasterWithType(
                                    1, self.process.GetBroadcaster(),
                                    self.lldb.SBProcess.eBroadcastBitStateChanged, event)
                                state = self.process.GetState()
                        self.record['target_state'] = {
                            self.lldb.eStateStopped: 'stopped', self.lldb.eStateRunning: 'running',
                            self.lldb.eStateExited: 'exited', self.lldb.eStateCrashed: 'crashed',
                            self.lldb.eStateDetached: 'detached'}.get(state, 'unknown')
                        if not killed.Success() or state != self.lldb.eStateExited:
                            state_number = str(state) if type(state) is int and -2147483648 <= state < 2147483648 else 'unavailable'
                            prior = self.record['command_error']
                            self.record['command_error'] = _safe_diagnostic_text(
                                'termination unconfirmed; state=' + state_number + ('; ' + prior if prior else ''))
                            raise RuntimeError('termination unconfirmed')
                        if self.record['target_exit'] != 'unexpected':
                            self.record['target_exit'] = 'intentional'
                        self.complete('target_termination')
                    else:
                        raise RuntimeError('target ownership state unknown')
                except Exception as error:
                    if self.record['command_error'] is None:
                        self.record['command_error'] = _safe_exception(error)
                    self.fail('target_termination', 'target_cleanup_unconfirmed')
        finally:
            self.os.close(self.fd)
            self.fd = None


def render_diagnostic(executable: Path, data_home: Path, passphrase_output: Path,
                      diagnostic: Path, token: str) -> str:
    import inspect
    output_info = passphrase_output.lstat()
    diagnostic_info = diagnostic.lstat()
    code = inspect.getsource(_safe_diagnostic_text) + '\n' + inspect.getsource(_safe_exception) + '\n' + inspect.getsource(_CaptureSession)
    code += '\noutput_path = ' + repr(str(passphrase_output)) + '\n'
    arguments = (lldb_quote(executable), lldb_quote(f'CFFIXED_USER_HOME={data_home}'),
                 lldb_quote(passphrase_output), str(diagnostic), token,
                 (diagnostic_info.st_dev, diagnostic_info.st_ino),
                 (output_info.st_dev, output_info.st_ino))
    code += '_capture_session = _CaptureSession(*' + repr(arguments) + ')\n'
    return '\n'.join([
        'script exec(' + repr(code) + ')', 'script _capture_session.prepare()',
        'breakpoint command add 1', 'script _capture_session.command_entered()',
        'memory read --force --outfile ' + lldb_quote(passphrase_output) + ' --binary --size 1 --count 32 $rsi',
        'breakpoint disable 1', 'script _capture_session.capture_checked()', 'DONE',
        'script _capture_session.armed()', 'run', '',
    ])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wechat-executable", type=Path, required=True)
    parser.add_argument("--wechat-data-home", type=Path, required=True)
    parser.add_argument("--passphrase-output", type=Path, required=True)
    parser.add_argument("--output-script", type=Path, required=True)
    parser.add_argument("--diagnostic-output", type=Path)
    parser.add_argument("--diagnostic-token")
    args = parser.parse_args()

    try:
        executable = args.wechat_executable.resolve(strict=True)
        data_home = args.wechat_data_home.resolve(strict=True)
        passphrase_output = args.passphrase_output.absolute()
        output_script = args.output_script.absolute()
    except OSError:
        return 2
    if not executable.is_file():
        raise SystemExit(f"WeChat executable not found: {executable}")
    if not data_home.is_dir():
        raise SystemExit(f"WeChat data home not found: {data_home}")
    if output_script.exists():
        raise SystemExit(f"Refusing to overwrite LLDB script: {output_script}")
    if passphrase_output.exists():
        raise SystemExit(f"Refusing to overwrite passphrase output: {passphrase_output}")

    if any(not _private_directory(parent) for parent in {output_script.parent, passphrase_output.parent}):
        return 2
    output_info = None
    script_info = None
    try:
        data = render(executable, data_home, passphrase_output).encode("utf-8")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        output_fd = os.open(passphrase_output, flags, 0o600)
        output_info = os.fstat(output_fd)
        if not stat.S_ISREG(output_info.st_mode) or stat.S_IMODE(output_info.st_mode) != 0o600:
            os.close(output_fd)
            raise OSError("capture output permissions could not be verified")
        os.close(output_fd)
        if args.diagnostic_output is not None:
            if not args.diagnostic_token or len(args.diagnostic_token) != 64:
                raise ValueError("invalid diagnostic binding")
            data = render_diagnostic(executable, data_home, passphrase_output, args.diagnostic_output, args.diagnostic_token).encode("utf-8")
        fd = os.open(output_script, flags, 0o600)
        script_info = os.fstat(fd)
        if not stat.S_ISREG(script_info.st_mode) or stat.S_IMODE(script_info.st_mode) != 0o600:
            os.close(fd)
            raise OSError("LLDB script permissions could not be verified")
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except (OSError, ValueError):
        for path, identity in ((output_script, script_info), (passphrase_output, output_info)):
            if identity is None:
                continue
            try:
                info = path.lstat()
                if (info.st_dev, info.st_ino) == (identity.st_dev, identity.st_ino):
                    path.unlink()
            except OSError:
                pass
        return 2
    print(f"Private LLDB script written: {output_script}")
    print("Run it only against your own authorized WeChat debug copy.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

