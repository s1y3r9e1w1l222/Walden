"""One-account, opt-in first-key capture orchestration.

This module deliberately supports only an explicitly selected WeChat bundle
and the established Documents/xwechat_files account-root layout. It does not
discover clients or attach to an already running process.
"""

from __future__ import annotations

import os
import json
import hashlib
import secrets
import re
import select
import threading
import math
import plistlib
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import unquote, urlsplit

from .live_catalog import LiveCatalogError, discover_numeric_shards
from .live_readonly import open_encrypted_readonly


_POLICY_LOG = Path('/usr/bin/log')
_POLICY_LINE_LIMIT = 16_384
_POLICY_EVENT_LIMIT = 64


class _PolicyEventMatcher:
    """Recognize only bounded, same-copy scan observations in this invocation."""

    _prefix = re.compile(r'^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d+(?:[+-]\d{2}:?\d{2}|Z)?)\s+(?:I|D|Df|E|F)\s+syspolicyd\[(\d+):([0-9a-fA-F]+)\]\s+\[com\.apple\.syspolicy\.exec:default\]\s+(.*)$')
    _pst = re.compile(r'PST: \(path: ([0-9a-fA-F]{1,32})\)')

    def __init__(self, bundle: Path, started_at: float):
        self.bundle = str(bundle.absolute())
        self.started_at = started_at
        self.state = 'watching'
        self._daemon = None
        self._target_pid = None
        self._pending_threads: dict[str, tuple[int, float]] = {}
        self._events: dict[str, dict[str, Any]] = {}

    def bind_target(self, pid: int) -> None:
        if self.state in {'unavailable', 'rejected'}:
            return
        if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
            self.state = 'unavailable'
            return
        self._target_pid = pid
        bound = [event for event in self._events.values() if event.get('pid') == pid and 'queue' in event]
        if not bound or all('same' in event and not event['same'] for event in bound):
            self.state = 'unavailable'
            return
        for event in self._events.values():
            self._consider(event)

    def _consider(self, event: dict[str, Any]) -> None:
        if self.state in {'unavailable', 'rejected'}:
            return
        if self._target_pid is not None and event.get('pid') == self._target_pid:
            if event.get('denied'):
                self.state = 'rejected'
                return
            if event.get('complete_ok') is False:
                self.state = 'unavailable'
                return
        if not event.get('same'):
            return
        if event.get('denied'):
            self.state = 'rejected'
        elif event.get('xp_ok') is False or event.get('complete_ok') is False:
            self.state = 'unavailable'
        elif all(key in event for key in ('xp', 'complete', 'finished')) and self._target_pid is not None:
            if (event.get('pid') != self._target_pid or 'queue' not in event
                    or not event['queue'] <= event['xp'] <= event['complete'] <= event['finished']):
                self.state = 'unavailable'
            else:
                self.state = 'completed'

    def feed(self, line: str) -> None:
        if self.state in {'unavailable', 'rejected'}:
            return
        if re.search(r'\b(?:dropped|lost)\b.*\b(?:messages|events)\b|\b(?:messages|events)\b.*\b(?:dropped|lost)\b', line, re.IGNORECASE):
            self.state = 'unavailable'
            return
        match = self._prefix.match(line)
        if match is None:
            lower = line.lower()
            if 'syspolicyd' in lower and any(name in lower for name in ('xprotect results', 'scan complete', 'scan finished', 'queueing up scan for code',
                                                         'denied', 'rejected', 'blocked', 'malware detected')):
                self.state = 'unavailable'
            return
        try:
            stamp = match.group(1).replace('Z', '+00:00')
            stamp = re.sub(r'([+-]\d\d)(\d\d)$', r'\1:\2', stamp)
            timestamp = datetime.fromisoformat(stamp).timestamp()
        except ValueError:
            self.state = 'unavailable'
            return
        if timestamp < self.started_at:
            return
        daemon, thread, message = match.group(2), match.group(3), match.group(4)
        if self._daemon is None:
            self._daemon = daemon
        elif daemon != self._daemon:
            self.state = 'unavailable'
            return
        lower = message.lower()
        tcc = re.search(r'^Skipping TCC check due to process:\s*(\d+)\s*,', message)
        if tcc:
            if len(self._pending_threads) >= _POLICY_EVENT_LIMIT and thread not in self._pending_threads:
                self.state = 'unavailable'
            else:
                self._pending_threads[thread] = (int(tcc.group(1)), timestamp)
            return
        if not lower.startswith('queueing up scan for code:'):
            self._pending_threads.pop(thread, None)
        relevant = any(x in lower for x in ('queueing up scan for code:', 'xprotect results', 'scan complete', 'scan finished', 'denied', 'rejected', 'blocked', 'malware detected'))
        if not relevant:
            return
        pst = self._pst.search(message)
        if pst is None:
            self.state = 'unavailable'
            return
        token = pst.group(1).lower()
        if token not in self._events:
            if len(self._events) >= _POLICY_EVENT_LIMIT:
                self.state = 'unavailable'
                return
            self._events[token] = {}
        event = self._events[token]
        if lower.startswith('queueing up scan for code:'):
            prior = self._pending_threads.pop(thread, None)
            if 'queue' in event:
                self.state = 'unavailable'
                return
            event['queue'] = timestamp
            event['pid'] = prior[0] if prior is not None and prior[1] <= timestamp else None
        elif any(word in lower for word in ('denied', 'rejected', 'blocked', 'malware detected')):
            event['denied'] = True
        elif 'xprotect results' in lower:
            if 'xp' in event:
                self.state = 'unavailable'
                return
            address = re.search(r'file://[^\s,]+', message)
            if address is None:
                self.state = 'unavailable'
                return
            try:
                url = urlsplit(address.group(0))
                path = unquote(url.path, errors='strict')
                same = (url.scheme == 'file' and not url.netloc and not url.query and not url.fragment
                        and Path(path).resolve(strict=True) == Path(self.bundle).resolve(strict=True))
            except (OSError, RuntimeError, ValueError, UnicodeError):
                self.state = 'unavailable'
                return
            previous = event.get('same')
            if previous is not None and previous != same:
                self.state = 'unavailable'
                return
            event['same'] = same
            event['xp'] = timestamp
            event['xp_ok'] = bool(re.search(r'XPScan:\s*0\s*,', message))
        elif 'scan complete' in lower:
            if 'complete' in event:
                self.state = 'unavailable'
                return
            event['complete'] = timestamp
            event['complete_ok'] = bool(re.search(r'\),\s*0\s*,\s*0\s*,\s*0\s*$', message))
        elif 'scan finished' in lower:
            if 'finished' in event:
                self.state = 'unavailable'
                return
            event['finished'] = timestamp
        self._consider(event)
        if self._target_pid is not None and self.state == 'watching':
            bound = [item for item in self._events.values() if item.get('pid') == self._target_pid and 'queue' in item]
            if bound and all('same' in item and not item['same'] for item in bound):
                self.state = 'unavailable'


class _PolicyObservationLost(Exception):
    pass


class _PolicyObserver:
    """Drain a private observation process; never retain its raw output."""

    def __init__(self, bundle: Path):
        self._matcher = _PolicyEventMatcher(bundle, time.time())
        self._condition = threading.Condition()
        self._process = None
        self._thread = None
        self._closing = False

    def start(self) -> None:
        predicate = 'processImagePath == "/usr/libexec/syspolicyd" AND subsystem == "com.apple.syspolicy.exec"'
        try:
            self._process = subprocess.Popen([str(_POLICY_LOG), 'stream', '--level', 'info', '--style', 'compact', '--type', 'log', '--predicate', predicate],
                                             stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                             bufsize=0, start_new_session=True)
            self._thread = threading.Thread(target=self._drain, daemon=True)
            self._thread.start()
        except (OSError, RuntimeError):
            self._matcher.state = 'unavailable'

    def _drain(self) -> None:
        pending = bytearray()
        lost = False
        try:
            while True:
                chunk = os.read(self._process.stdout.fileno(), 4096)
                if not chunk:
                    lost = bool(pending)
                    break
                pending.extend(chunk)
                while b'\n' in pending:
                    line, _, rest = pending.partition(b'\n')
                    pending = bytearray(rest)
                    if len(line) > _POLICY_LINE_LIMIT:
                        raise ValueError('oversized event')
                    with self._condition:
                        self._matcher.feed(line.decode('utf-8', errors='replace'))
                        self._condition.notify_all()
                if len(pending) > _POLICY_LINE_LIMIT:
                    raise ValueError('oversized event')
        except (OSError, ValueError):
            lost = True
        finally:
            with self._condition:
                if lost or not self._closing:
                    self._matcher.state = 'unavailable'
                self._condition.notify_all()

    def arm(self) -> None:
        with self._condition:
            if self._process is not None and self._process.poll() is None and self._matcher.state != 'unavailable':
                self._matcher = _PolicyEventMatcher(Path(self._matcher.bundle), time.time())

    def bind_target(self, pid: int) -> None:
        with self._condition:
            self._matcher.bind_target(pid)
            self._condition.notify_all()

    def wait(self, deadline: float | None) -> str:
        with self._condition:
            while self._matcher.state == 'watching':
                if self._process is None or self._process.poll() is not None:
                    return 'unavailable'
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return 'timeout'
                self._condition.wait(timeout=0.5 if remaining is None else min(0.5, remaining))
            if self._process is None or self._process.poll() is not None:
                return 'unavailable'
            return self._matcher.state

    def close(self, *, require_active: bool = False) -> str:
        if self._process is None:
            if require_active:
                raise _PolicyObservationLost()
            return 'unavailable'
        with self._condition:
            was_active = self._process.poll() is None
            if was_active:
                self._closing = True
        if was_active:
            try:
                self._process.terminate()
                self._process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=2)
            except ProcessLookupError:
                was_active = False
        if self._thread is not None:
            self._thread.join(timeout=1)
        if self._process.stdout is not None:
            self._process.stdout.close()
        if self._thread is not None and self._thread.is_alive():
            raise OSError('policy observation cleanup unconfirmed')
        if require_active and not was_active:
            raise _PolicyObservationLost()
        with self._condition:
            return self._matcher.state


def _no_symlink_path(path: Path) -> bool:
    absolute = path.expanduser().absolute()
    current = Path(absolute.anchor)
    for component in absolute.parts[1:]:
        current = current / component
        try:
            if stat.S_ISLNK(current.lstat().st_mode):
                return False
        except OSError:
            return False
    return True


def _private_directory(path: Path) -> bool:
    try:
        info = path.lstat()
    except OSError:
        return False
    return (
        stat.S_ISDIR(info.st_mode)
        and not stat.S_ISLNK(info.st_mode)
        and info.st_uid == os.getuid()
        and (stat.S_IMODE(info.st_mode) & 0o077) == 0
    )


def _destination_parent_available(path: Path) -> bool:
    if path.exists() or path.is_symlink():
        return _private_directory(path)
    current = path
    while not current.exists():
        if current.is_symlink() or current.parent == current:
            return False
        current = current.parent
    try:
        info = current.lstat()
    except OSError:
        return False
    return stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode) and info.st_uid == os.getuid()


def _ensure_private_directory(path: Path) -> bool:
    """Create missing components privately; never chmod an existing one."""

    missing: list[Path] = []
    current = path
    while not current.exists():
        if current.is_symlink():
            return False
        missing.append(current)
        if current.parent == current:
            return False
        current = current.parent
    try:
        info = current.lstat()
    except OSError:
        return False
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        return False
    for item in reversed(missing):
        try:
            item.mkdir(mode=0o700)
        except OSError:
            return False
        if not _private_directory(item):
            return False
    return _private_directory(path)


def _selected_bundle(executable: Path, *, deadline: float | None = None) -> tuple[Path | None, str | None]:
    try:
        if stat.S_ISLNK(executable.expanduser().lstat().st_mode):
            return None, "target_ambiguous"
    except OSError:
        return None, "target_unavailable"
    try:
        resolved = executable.expanduser().resolve(strict=True)
    except OSError:
        return None, "target_unavailable"
    if not resolved.is_file() or not os.access(resolved, os.X_OK):
        return None, "target_unavailable"
    bundle = resolved.parents[2] if len(resolved.parents) > 2 else None
    if bundle is None:
        return None, "target_ambiguous"
    info_path = bundle / "Contents" / "Info.plist"
    try:
        if not _no_symlink_path(info_path) or not info_path.is_file():
            return None, "target_ambiguous"
        with info_path.open("rb") as handle:
            metadata = plistlib.load(handle)
    except (OSError, plistlib.InvalidFileException, ValueError):
        return None, "target_ambiguous"
    if (
        not isinstance(metadata, dict)
        or metadata.get("CFBundleIdentifier") != "com.tencent.xinWeChat"
        or metadata.get("CFBundleExecutable") != resolved.name
    ):
        return None, "target_ambiguous"
    lipo = Path("/usr/bin/lipo")
    if not lipo.is_file() or not os.access(lipo, os.X_OK):
        return None, "architecture_check_unavailable"
    remaining = None if deadline is None else deadline - time.monotonic()
    if remaining is not None and remaining <= 0:
        return None, 'capture_timeout'
    try:
        result = subprocess.run(
            [str(lipo), "-archs", str(resolved)],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=5 if remaining is None else min(5, remaining),
        )
    except subprocess.TimeoutExpired:
        if deadline is not None and time.monotonic() >= deadline:
            return None, 'capture_timeout'
        return None, "architecture_check_unavailable"
    except OSError:
        return None, "architecture_check_unavailable"
    if result.returncode != 0 or b"x86_64" not in result.stdout.split():
        return None, "target_architecture_unsupported"
    return resolved, None


def _bound_account_root(account_root: Path, wechat_data_home: Path) -> tuple[Path | None, str | None]:
    try:
        root = account_root.expanduser().resolve(strict=True)
        data_home = wechat_data_home.expanduser().resolve(strict=True)
    except OSError:
        return None, "target_source_unbound"
    expected_parent = data_home / "Documents" / "xwechat_files"
    expected = expected_parent / root.name
    current = data_home
    for component in ("Documents", "xwechat_files", root.name):
        current = current / component
        try:
            if stat.S_ISLNK(current.lstat().st_mode):
                return None, "target_source_unbound"
        except OSError:
            return None, "target_source_unbound"
    if not expected_parent.is_dir() or root != expected.resolve(strict=False):
        return None, "target_source_unbound"
    return root, None


def _data_home_from_selected_account(account_root: Path) -> tuple[Path | None, str | None]:
    """Derive only the parent in the exact Data/Documents/xwechat_files layout."""

    try:
        root = account_root.expanduser().resolve(strict=True)
    except OSError:
        return None, "target_source_unbound"
    if root.parent.name != "xwechat_files" or root.parent.parent.name != "Documents":
        return None, "target_source_unbound"
    data_home = root.parent.parent.parent
    bound, error = _bound_account_root(root, data_home)
    if error is not None or bound != root:
        return None, "target_source_unbound"
    return data_home, None


def resolve_default_capture_inputs(account_root: Path) -> dict[str, Any]:
    """Resolve one conventional app path without scanning other apps/accounts."""

    data_home, error = _data_home_from_selected_account(account_root)
    if error:
        return {"status": error}
    executable = Path("/Applications/WeChat.app/Contents/MacOS/WeChat")
    if not executable.is_file():
        return {"status": "target_unavailable"}
    xcrun = shutil.which("xcrun")
    if not xcrun:
        return {"status": "debugger_unavailable"}
    try:
        found = subprocess.run(
            [xcrun, "--find", "lldb"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return {"status": "debugger_unavailable"}
    if found.returncode != 0 or len(found.stdout) > 4096:
        return {"status": "debugger_unavailable"}
    try:
        lldb = Path(found.stdout.decode("utf-8", errors="strict").strip())
    except UnicodeDecodeError:
        return {"status": "debugger_unavailable"}
    if not lldb.is_absolute() or not lldb.is_file() or not os.access(lldb, os.X_OK):
        return {"status": "debugger_unavailable"}
    return {
        "status": "resolved",
        "wechat_executable": str(executable),
        "wechat_data_home": str(data_home),
        "lldb_executable": str(lldb),
    }


def _write_secret_exclusive(path: Path, material: bytes) -> tuple[bool, tuple[int, int] | None]:
    if not _ensure_private_directory(path.parent):
        return False, None
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags, 0o600)
    except OSError:
        return False, None
    identity: tuple[int, int] | None = None
    try:
        with os.fdopen(fd, "wb") as handle:
            opened = os.fstat(handle.fileno())
            if not stat.S_ISREG(opened.st_mode) or stat.S_IMODE(opened.st_mode) != 0o600:
                raise OSError("private credential file could not be verified")
            identity = (opened.st_dev, opened.st_ino)
            handle.write(material)
            handle.flush()
            os.fsync(handle.fileno())
        info = path.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(info.st_mode) != 0o600
            or (info.st_dev, info.st_ino) != identity
        ):
            raise OSError("private credential file could not be verified")
        return True, identity
    except BaseException:
        _unlink_owned(path, identity)
        return False, None


def _remove_owned(path: Path, identity: tuple[int, int] | None) -> str:
    if identity is None:
        return "identity_unavailable"
    try:
        info = path.lstat()
    except FileNotFoundError:
        return "already_absent"
    except OSError:
        return "state_unreadable"
    if not stat.S_ISREG(info.st_mode) or (info.st_dev, info.st_ino) != identity:
        return "identity_changed"
    try:
        path.unlink()
    except OSError:
        return "remove_failed"
    return "removed"


def _unlink_owned(path: Path, identity: tuple[int, int] | None) -> None:
    _remove_owned(path, identity)


def _timeout_capture_cleanup_state(path: Path, identity: tuple[int, int] | None) -> str:
    outcome = _remove_owned(path, identity)
    if outcome in {"removed", "already_absent"}:
        return "private_stage_retained_without_capture"
    if outcome == "identity_changed":
        return "private_stage_retained_capture_identity_changed"
    return "private_stage_retained_capture_state_unknown"


def _verify_all_shards(shards: tuple[Path, ...], passphrase: bytes, *,
                       root: Path | None = None, observations: dict | None = None) -> bool:
    if not shards:
        return False
    if observations is not None:
        from .source_setup import _database_stamp, _voice_schema_matches
    for shard in shards:
        connection: Any | None = None
        try:
            before = _database_stamp(shard, root) if observations is not None and root is not None else None
            if observations is not None and before is None:
                observations['_source_changed'] = True
                return False
            opened = open_encrypted_readonly(shard, passphrase)
            connection = opened[0] if isinstance(opened, tuple) else opened
            row = connection.execute("PRAGMA query_only").fetchone()
            if not row or int(row[0]) != 1:
                return False
            if observations is not None:
                try:
                    voice = _voice_schema_matches(connection)
                except Exception:
                    voice = None
                if _database_stamp(shard, root) != before:
                    observations['_source_changed'] = True
                    return False
                observations[shard] = (before, voice)
        except Exception:
            return False
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
    return True


def _owned_group_exists(process_group_id: int) -> bool:
    try:
        os.killpg(process_group_id, 0)
    except ProcessLookupError:
        return False
    except OSError:
        return True
    return True


def _signal_owned_group(process_group_id: int, sig: int) -> bool:
    try:
        os.killpg(process_group_id, sig)
        return True
    except ProcessLookupError:
        return True
    except OSError:
        return False


def _stop_owned_group(process_group_id: int) -> bool:
    """Boundedly stop only the new session created for this LLDB invocation."""

    if not _signal_owned_group(process_group_id, signal.SIGTERM):
        return False
    deadline = time.monotonic() + 0.5
    while time.monotonic() < deadline:
        if not _owned_group_exists(process_group_id):
            return True
        time.sleep(0.02)
    if not _signal_owned_group(process_group_id, signal.SIGKILL):
        return False
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        if not _owned_group_exists(process_group_id):
            return True
        time.sleep(0.02)
    return not _owned_group_exists(process_group_id)


def _safe_diagnostic_text(value):
    """Bound debugger errors; remove paths, addresses and possible secret tokens."""
    text = ''.join(c if 32 <= ord(c) < 127 else ' ' for c in str(value)[:2048])
    text = re.sub(r"(?:[A-Za-z]:)?/[^\s'\"<>]*", '[path]', text)
    text = re.sub(r'(?<![A-Za-z0-9])(?:[0-9a-fA-F]{2}[\s,:-]+)+[0-9a-fA-F]{2}(?![A-Za-z0-9])', '[redacted]', text)
    text = re.sub(r'\b(?:0x)?[0-9a-fA-F]{8,}\b', '[redacted]', text)
    text = re.sub(r'[A-Za-z0-9_+=-]{24,}', '[redacted]', text)
    return ' '.join(text.split())[:384]


class _DebuggerErrors:
    """Drain output continuously; expose errors only after target stdio isolation."""

    def __init__(self, stream):
        self.stream = stream
        self._errors: list[str] = []
        self.done = threading.Event()
        self.thread = threading.Thread(target=self._drain, daemon=True)
        self.thread.start()

    @property
    def errors(self):
        # No source guarantee until the caller validates both target settings.
        return []

    def isolated_errors(self):
        return list(self._errors)

    def _line(self, line):
        # Application output, command echoes and memory output are never returned.
        text = line.decode('utf-8', errors='replace').strip()
        if re.match(r"(?:error:\s|[A-Za-z_][A-Za-z0-9_.]*(?:Error|Exception):|KeyboardInterrupt:|SystemExit:)", text, re.I):
            safe = _safe_diagnostic_text(text)
            if safe and safe not in self._errors and len(self._errors) < 4:
                self._errors.append(safe)

    def _drain(self):
        pending = b''
        discard = False
        try:
            while not self.done.is_set():
                if not select.select([self.stream], [], [], 0.1)[0]:
                    continue
                chunk = os.read(self.stream.fileno(), 4096)
                if not chunk:
                    break
                for piece in chunk.splitlines(keepends=True):
                    ended = piece.endswith((b'\n', b'\r'))
                    if not discard:
                        pending += piece
                        if len(pending) > 2048:
                            pending = b''
                            discard = True
                    if ended:
                        if not discard:
                            self._line(pending)
                        pending, discard = b'', False
            if pending and not discard:
                self._line(pending)
        except (OSError, ValueError):
            pass

    def close(self):
        self.thread.join(timeout=0.3)
        self.done.set()
        self.thread.join(timeout=0.3)
        self.stream.close()


_CAPTURE_STAGES = ('target_create', 'environment', 'breakpoint', 'launch', 'breakpoint_hit', 'capture_write', 'target_termination')


def _unknown_capture_diagnostic(channel: str) -> dict[str, Any]:
    return dict(version=1, channel=channel, stages={s: 'unknown' for s in _CAPTURE_STAGES},
                last_started=None, last_completed=None, failed_stage=None, elapsed_ms=None,
                target_pid=None, target_state='unknown', target_exit='unknown',
                target_exit_status=None, target_exit_description=None, command_error=None,
                breakpoint_commands='not_entered', failure_detail=None, stop_source='none',
                stop_state='unknown', stop_facts=[], stop_threads_omitted=0, stop_error=None)


def _read_capture_diagnostic(path: Path, identity: tuple[int, int] | None,
                             token: str, debugger_pid: int | None) -> dict[str, Any]:
    """Read only the bounded, owned per-invocation channel. Never expose input text."""
    def invalid():
        return _unknown_capture_diagnostic('invalid')

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate diagnostic field')
            result[key] = value
        return result

    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
        with os.fdopen(fd, 'rb') as handle:
            info = os.fstat(handle.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
                    or (info.st_dev, info.st_ino) != identity or not 0 < info.st_size <= 4096):
                return invalid()
            data = handle.read(4097)
            after = path.lstat()
            if ((after.st_dev, after.st_ino) != identity or len(data) != info.st_size
                    or len(data) > 4096):
                return invalid()
        value = json.loads(data, object_pairs_hook=unique)
        expected = {'version','token','debugger_pid','stages','last_started','last_completed','failed_stage',
                    'elapsed_ms','target_pid','target_state','target_exit','target_exit_status',
                    'target_exit_description','command_error','breakpoint_commands','failure_detail',
                    'stop_source','stop_state','stop_facts','stop_threads_omitted','stop_error'}
        if not isinstance(value, dict) or set(value) != expected:
            return invalid()
        if type(value['version']) is not int or value['version'] != 1 or value['token'] != token:
            return invalid()
        if type(value['debugger_pid']) is not int or value['debugger_pid'] != debugger_pid:
            return invalid()
        if type(value['elapsed_ms']) is not int or not 0 <= value['elapsed_ms'] < 9223372036854775808:
            return invalid()
        states = value['stages']
        if not isinstance(states, dict) or set(states) != set(_CAPTURE_STAGES):
            return invalid()
        if any(type(v) is not str or v not in {'not_started','started','completed','failed'} for v in states.values()):
            return invalid()
        for field, allowed in [('last_started', {'started','completed','failed'}),
                               ('last_completed', {'completed'}), ('failed_stage', {'failed'})]:
            item = value[field]
            if item is not None and (type(item) is not str or item not in states or states[item] not in allowed):
                return invalid()
        # Main operations are sequential. Termination is a separately attempted cleanup.
        incomplete = False
        for stage in _CAPTURE_STAGES[:-1]:
            if incomplete and states[stage] != 'not_started':
                return invalid()
            if states[stage] != 'completed':
                incomplete = True
        if (any(v == 'failed' for v in states.values()) != (value['failed_stage'] is not None)
                or any(v != 'not_started' for v in states.values()) != (value['last_started'] is not None)
                or any(v == 'completed' for v in states.values()) != (value['last_completed'] is not None)):
            return invalid()
        began = [s for s in _CAPTURE_STAGES if states[s] != 'not_started']
        completed = [s for s in _CAPTURE_STAGES if states[s] == 'completed']
        failed = [s for s in _CAPTURE_STAGES if states[s] == 'failed']
        if (value['last_started'] != (began[-1] if began else None)
                or value['last_completed'] != (completed[-1] if completed else None)
                or value['failed_stage'] != (failed[0] if failed else None)):
            return invalid()
        pid = value['target_pid']
        if pid is not None and (type(pid) is not int or not 0 < pid < 2147483648 or states['launch'] == 'not_started'):
            return invalid()
        if type(value['target_state']) is not str or value['target_state'] not in {'unknown','stopped','running','exited','crashed','detached'}:
            return invalid()
        if type(value['target_exit']) is not str or value['target_exit'] not in {'unknown','intentional','unexpected'}:
            return invalid()
        if pid is None and (value['target_state'] != 'unknown' or value['target_exit'] != 'unknown'):
            return invalid()
        if value['target_exit'] == 'intentional' and (states['target_termination'] != 'completed' or value['target_state'] != 'exited'):
            return invalid()
        exit_status = value['target_exit_status']
        if exit_status is not None and (type(exit_status) is not int or not -2147483648 <= exit_status < 2147483648
                                        or value['target_exit'] != 'unexpected'):
            return invalid()
        for field in ('target_exit_description', 'command_error', 'stop_error'):
            text = value[field]
            if text is not None and (type(text) is not str or not text or len(text) > 384
                                     or _safe_diagnostic_text(text) != text):
                return invalid()
        if value['target_exit_description'] is not None and value['target_exit'] != 'unexpected':
            return invalid()
        details = {'setup_command_failed', 'breakpoint_setup_incomplete', 'launch_unconfirmed',
                   'breakpoint_commands_not_entered', 'capture_commands_incomplete',
                   'capture_output_identity_invalid', 'capture_output_length_invalid',
                   'capture_check_internal_error', 'target_cleanup_unconfirmed'}
        if value['failure_detail'] is not None and value['failure_detail'] not in details:
            return invalid()
        if (value['failed_stage'] is None) != (value['failure_detail'] is None):
            return invalid()
        if value['breakpoint_commands'] not in {'not_entered','entered','completed','failed'}:
            return invalid()
        if value['breakpoint_commands'] == 'completed' and states['capture_write'] != 'completed':
            return invalid()
        if value['breakpoint_commands'] == 'not_entered' and states['capture_write'] != 'not_started':
            return invalid()
        if value['stop_source'] not in {'none','breakpoint_commands','run_return'}:
            return invalid()
        if value['stop_state'] not in {'unknown','stopped','running','exited','crashed','detached'}:
            return invalid()
        omitted = value['stop_threads_omitted']
        if type(omitted) is not int or not 0 <= omitted < 2147483648:
            return invalid()
        facts = value['stop_facts']
        if not isinstance(facts, list) or len(facts) > 8:
            return invalid()
        seen_threads = set()
        for fact in facts:
            if not isinstance(fact, dict) or set(fact) != {'thread_index','reason','breakpoint_id','location_id','signal'}:
                return invalid()
            index = fact['thread_index']
            if type(index) is not int or not 0 <= index < 8 or index in seen_threads:
                return invalid()
            seen_threads.add(index)
            if type(fact['reason']) is not int or not 0 <= fact['reason'] <= 255:
                return invalid()
            for key, limit in (('breakpoint_id',2147483648),('location_id',2147483648),('signal',256)):
                item = fact[key]
                if item is not None and (type(item) is not int or not 0 <= item < limit):
                    return invalid()
        if value['stop_source'] == 'none' and (facts or omitted or value['stop_state'] != 'unknown' or value['stop_error'] is not None):
            return invalid()
        # Return a new fixed-schema object. The nonce and debugger identity are internal only.
        return {key: value[key] for key in expected - {'token','debugger_pid'}} | {'channel': 'valid'}
    except FileNotFoundError:
        return _unknown_capture_diagnostic('missing')
    except (OSError, ValueError, TypeError, UnicodeError, RecursionError):
        return invalid()


class _PreparationCleanupUncertain(Exception):
    pass


def _run_preparation_command(step: str, argv: list[str]) -> tuple[bool, str | None]:
    """Run one owned command and retain only bounded stderr on failure."""
    process = None
    try:
        process = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                   stderr=subprocess.PIPE, bufsize=0, start_new_session=True)
        output = bytearray()
        def drain():
            try:
                while True:
                    chunk = os.read(process.stderr.fileno(), 4096)
                    if not chunk:
                        break
                    if len(output) < 2048:
                        output.extend(chunk[:2048-len(output)])
            except OSError:
                pass
        try:
            reader = threading.Thread(target=drain, daemon=True)
            reader.start()
            code = process.wait()
        except BaseException:
            if not _stop_owned_group(process.pid):
                raise _PreparationCleanupUncertain(step + ' process cleanup unconfirmed') from None
            process.wait()
            raise
        if not _stop_owned_group(process.pid):
            raise _PreparationCleanupUncertain(step + ' process cleanup unconfirmed')
        reader.join(timeout=0.5)
        process.stderr.close()
        if code == 0:
            return True, None
        detail = output.decode('utf-8', errors='replace').strip()
        return False, _safe_diagnostic_text(detail) or (step + ' failed with exit ' + str(code))
    except _PreparationCleanupUncertain:
        raise
    except (OSError, KeyboardInterrupt) as error:
        return False, _safe_diagnostic_text(type(error).__name__ + ': ' + str(error)) or (step + ' failed')


def _safe_copied_bundle(bundle: Path) -> bool:
    """Never sign a copy containing an external symlink or shared regular file."""
    try:
        info = bundle.lstat()
        if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
            return False
        root = bundle.resolve(strict=True)
        def onerror(error):
            raise error
        for directory, directories, files in os.walk(bundle, followlinks=False, onerror=onerror):
            for name in directories + files:
                path = Path(directory) / name
                entry = path.lstat()
                if stat.S_ISLNK(entry.st_mode):
                    resolved = path.resolve(strict=True)
                    if resolved != root and root not in resolved.parents:
                        return False
                elif stat.S_ISREG(entry.st_mode):
                    if entry.st_nlink != 1:
                        return False
                elif not stat.S_ISDIR(entry.st_mode):
                    return False
        return True
    except (OSError, ValueError, RuntimeError):
        return False


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def _client_root(config: Path) -> tuple[Path | None, tuple[int, int] | None, str | None]:
    """Acquire a private exclusive lock. An existing lock is never guessed stale."""
    if not _ensure_private_directory(config.parent) or not _no_symlink_path(config.parent):
        return None, None, 'reusable_client_unavailable'
    root = config.parent / '.wechat-local-client'
    try:
        try:
            root.mkdir(mode=0o700)
            created = True
        except FileExistsError:
            created = False
        if not _private_directory(root) or not _no_symlink_path(root):
            return None, None, 'reusable_client_ambiguous'
        lock = root / 'lock'
        try:
            lock.mkdir(mode=0o700)
        except FileExistsError:
            return None, None, 'reusable_client_locked'
        info = lock.lstat()
        identity = (info.st_dev, info.st_ino)
        record = root / 'owner.json'
        if created:
            rootinfo = root.lstat()
            ok, _ = _write_secret_exclusive(record, json.dumps({'version': 1, 'root': [rootinfo.st_dev, rootinfo.st_ino], 'client': None}).encode())
            if not ok:
                return root, identity, 'reusable_client_ambiguous'
        return root, identity, None
    except OSError:
        return None, None, 'reusable_client_unavailable'


def _release_client_lock(root: Path, identity: tuple[int, int]) -> None:
    try:
        lock = root / 'lock'
        info = lock.lstat()
        if stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode) and (info.st_dev, info.st_ino) == identity:
            lock.rmdir()
    except OSError:
        pass


def _prepare_reusable_client(root: Path, source: Path) -> tuple[Path | None, str | None, str | None]:
    """Reuse only a private, bound and signature-verified copy; never delete unknown objects."""
    record = root / 'owner.json'
    try:
        flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0)
        fd = os.open(record, flags)
        with os.fdopen(fd, 'rb') as stream:
            info = os.fstat(stream.fileno())
            if not (stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and info.st_nlink == 1
                    and stat.S_IMODE(info.st_mode) == 0o600 and info.st_size <= 8192):
                return None, 'reusable_client_ambiguous', None
            owner = json.load(stream)
        rootinfo = root.lstat()
        if not isinstance(owner, dict) or set(owner) != {'version','root','client'} or owner['version'] != 1 or owner['root'] != [rootinfo.st_dev, rootinfo.st_ino]:
            return None, 'reusable_client_ambiguous', None
        seal = source.parents[1] / '_CodeSignature' / 'CodeResources'
        source_version = hashlib.sha256((_file_digest(source) + _file_digest(source.parents[1] / 'Info.plist')
                                         + (_file_digest(seal) if seal.is_file() else '')).encode()).hexdigest()
        old = owner['client']
        if old is not None:
            if not isinstance(old, dict) or set(old) != {'slot','source','directory','bundle','executable','digest'} or not re.fullmatch(r'client-[0-9a-f]{32}', old.get('slot', '')):
                return None, 'reusable_client_ambiguous', None
            slot = root / old['slot']
            try:
                slot_info = slot.lstat()
            except FileNotFoundError:
                slot_info = None
            if slot_info is not None and (not _private_directory(slot) or [slot_info.st_dev, slot_info.st_ino] != old['directory']):
                return None, 'reusable_client_ambiguous', None
            bundle = slot / 'WeChat-debug.app'
            try:
                bundle_info = bundle.lstat() if slot_info is not None else None
            except FileNotFoundError:
                bundle_info = None
            if bundle_info is not None:
                executable = bundle / 'Contents' / 'MacOS' / source.name
                binary_info = executable.lstat()
                if (not stat.S_ISDIR(bundle_info.st_mode) or stat.S_ISLNK(bundle_info.st_mode)
                    or [bundle_info.st_dev, bundle_info.st_ino] != old['bundle']
                    or not stat.S_ISREG(binary_info.st_mode) or binary_info.st_nlink != 1
                    or [binary_info.st_dev, binary_info.st_ino] != old['executable']
                    or not _no_symlink_path(executable) or not _safe_copied_bundle(bundle)
                    or _file_digest(executable) != old['digest']):
                    return None, 'reusable_client_ambiguous', None
                if old['source'] == source_version:
                    ok, error = _run_preparation_command('verify', ['/usr/bin/codesign', '--verify', '--deep', '--strict', str(bundle)])
                    if not ok:
                        return None, 'preparation_verify_failed', error or 'verify failed'
                    checked, error = _selected_bundle(executable)
                    if error: return None, 'reusable_client_ambiguous', None
                    return checked, None, None
        slot = root / ('client-' + secrets.token_hex(16))
        slot.mkdir(mode=0o700)
        bundle = slot / 'WeChat-debug.app'
        ok, error = _run_preparation_command('copy', ['/bin/cp', '-cR', str(source.parents[2]), str(bundle)])
        if not ok: return None, 'preparation_copy_failed', error or 'copy failed'
        if not _safe_copied_bundle(bundle): return None, 'preparation_unsafe_bundle', None
        for step, argv in (('sign', ['/usr/bin/codesign', '--force', '--deep', '--sign', '-', '--timestamp=none', str(bundle)]),
                           ('verify', ['/usr/bin/codesign', '--verify', '--deep', '--strict', str(bundle)])):
            ok, error = _run_preparation_command(step, argv)
            if not ok: return None, 'preparation_' + step + '_failed', error or (step + ' failed')
        executable = bundle / 'Contents' / 'MacOS' / source.name
        checked, error = _selected_bundle(executable)
        if error: return None, 'preparation_target_invalid', error
        slot_info, bundle_info, binary_info = slot.lstat(), bundle.lstat(), checked.lstat()
        owner['client'] = {'slot':slot.name, 'source':source_version, 'directory':[slot_info.st_dev,slot_info.st_ino],
                           'bundle':[bundle_info.st_dev,bundle_info.st_ino], 'executable':[binary_info.st_dev,binary_info.st_ino],
                           'digest':_file_digest(checked)}
        current = record.lstat()
        if (current.st_dev,current.st_ino) != (info.st_dev,info.st_ino):
            return None, 'reusable_client_ambiguous', None
        temporary = root / ('record-' + secrets.token_hex(16))
        ok, temp_id = _write_secret_exclusive(temporary, json.dumps(owner).encode())
        if not ok: return None, 'reusable_client_unavailable', None
        os.replace(temporary, record)
        return checked, None, None
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return None, 'reusable_client_ambiguous', None


def _pid_is_absent(pid: object) -> bool:
    if type(pid) is not int or not 0 < pid < 2147483648:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except OSError:
        return False
    return False


def _retryable_initial_launch(evidence: dict[str, Any], captured_path: Path,
                              identity: tuple[int, int] | None, config: Path,
                              passphrase_path: Path) -> bool:
    """Admit only the observed first-launch failure before any possible capture."""
    stages = evidence.get('stages', {})
    if not (evidence.get('channel') == 'valid'
            and evidence.get('failed_stage') == 'launch'
            and evidence.get('failure_detail') == 'launch_unconfirmed'
            and evidence.get('target_exit') == 'unexpected'
            and evidence.get('target_exit_status') == -1
            and evidence.get('target_exit_description') == 'attach failed (attached to process, but could not pause execution; attach failed)'
            and evidence.get('breakpoint_commands') == 'not_entered'
            and stages.get('breakpoint_hit') == stages.get('capture_write') == 'not_started'
            and all(evidence.get(name) == 'not_started' for name in ('validation', 'persistence', 'configuration'))
            and evidence.get('group_cleanup') == evidence.get('target_cleanup') == 'confirmed'
            and evidence.get('debugger_timeout') is False and evidence.get('debugger_cancelled') is False
            and evidence.get('debugger_returncode') == 0):
        return False
    try:
        info = captured_path.lstat()
        if not (identity is not None and (info.st_dev, info.st_ino) == identity
                and stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o600
                and info.st_uid == os.getuid() and info.st_nlink == 1 and info.st_size == 0):
            return False
        if config.exists() or config.is_symlink() or passphrase_path.exists() or passphrase_path.is_symlink():
            return False
    except OSError:
        return False
    return _pid_is_absent(evidence.get('target_pid'))


def capture_and_save_passphrase(
    *,
    account_root: Path,
    wechat_executable: Path,
    wechat_data_home: Path,
    lldb_executable: Path,
    config_path: Path,
    timeout_seconds: float = 0.0,
    prepare_copy: bool = False,
    _validation_observations: dict | None = None,
    _capture_deadline: float | None = None,
) -> dict[str, Any]:
    """Capture one key from an explicitly selected target and verify all shards.

    The target/source layout must be structurally bound before LLDB is started.
    Successful capture creates only the private passphrase file; the caller
    resumes the existing setup function and removes that owned file on failure.
    """

    target = Path(wechat_executable).expanduser()
    lldb = Path(lldb_executable).expanduser()
    config = Path(config_path).expanduser().absolute()
    if config.exists() or config.is_symlink():
        return {"status": "config_exists"}
    passphrase_path = config.with_name("passphrase.bin")
    if passphrase_path.exists() or passphrase_path.is_symlink():
        return {"status": "credentials_exist"}
    if not _destination_parent_available(config.parent):
        return {"status": "credential_destination_unavailable"}
    root, error = _bound_account_root(Path(account_root), Path(wechat_data_home))
    if error:
        return {"status": error}
    assert root is not None
    selected_executable, error = _selected_bundle(target, deadline=_capture_deadline)
    if error:
        return {"status": error, **({'failure_reason': 'capture_timeout'} if error == 'capture_timeout' else {})}
    assert selected_executable is not None
    try:
        lldb_resolved = lldb.resolve(strict=True)
    except OSError:
        return {"status": "debugger_unavailable"}
    if not lldb_resolved.is_file() or not os.access(lldb_resolved, os.X_OK):
        return {"status": "debugger_unavailable"}

    from .source_setup import inspect_account_root

    inspected = inspect_account_root(root)
    if inspected.get("status") in {"account_root_ambiguous", "account_root_invalid", "account_root_unavailable", "message_shards_missing"}:
        return {"status": str(inspected["status"])}
    message_root = Path(str(inspected["message_root"]))
    try:
        shards = discover_numeric_shards(message_root)
    except LiveCatalogError:
        return {"status": "message_source_unavailable"}
    if not shards:
        return {"status": "message_shards_missing"}
    if not math.isfinite(timeout_seconds) or timeout_seconds < 0 or timeout_seconds > 300:
        return {"status": "capture_timeout_invalid"}

    stage_parent = Path(tempfile.gettempdir())
    try:
        stage = Path(tempfile.mkdtemp(prefix="wechat-local-capture-", dir=stage_parent))
        stage_identity = (stage.lstat().st_dev, stage.lstat().st_ino)
        stage.chmod(0o700)
    except OSError:
        return {"status": "capture_stage_unavailable"}
    script_path = stage / "capture.lldb"
    captured_path = stage / "captured.bin"
    diagnostic_path = stage / "diagnostic.json"
    diagnostic_token = secrets.token_hex(32)
    diagnostic_identity = None
    diagnostics = _unknown_capture_diagnostic('missing')
    processing = dict(validation='not_started', persistence='not_started', configuration='not_started')
    invocation_started = time.monotonic()
    timed_out = False
    cancelled = False
    output_reader = None
    policy_observer = None
    policy_observation = dict(state='not_started', retry='not_started')
    group_cleanup = 'not_attempted'
    python_error = None

    def response(result_status: str, **fields: Any) -> dict[str, Any]:
        if result_status not in {'captured_and_verified', 'diagnostic_checked'}:
            for name in processing:
                if processing[name] == 'started':
                    processing[name] = 'failed'
        evidence = _read_capture_diagnostic(diagnostic_path, diagnostic_identity, diagnostic_token,
                                            process.pid if process is not None else None)
        diagnostics.clear()
        diagnostics.update(evidence)
        code = process.returncode if process is not None else None
        diagnostics.update(processing)
        if prepare_copy:
            if policy_observation['state'] == 'watching' and result_status != 'diagnostic_checked':
                policy_observation['state'] = 'not_needed'
            diagnostics['policy_observation'] = dict(policy_observation)
        diagnostics.update(debugger_returncode=code, debugger_signal=-code if code is not None and code < 0 else None,
                           debugger_timeout=timed_out, debugger_cancelled=cancelled, group_cleanup=group_cleanup,
                           debugger_errors=(output_reader.isolated_errors() if output_reader is not None
                                            and evidence['channel'] == 'valid'
                                            and evidence['stages']['environment'] == 'completed' else []),
                           python_error=python_error,
                           target_cleanup=('confirmed' if not timed_out and evidence['channel'] == 'valid'
                                           and evidence['target_state'] == 'exited' else 'unknown'),
                           total_elapsed_ms=max(0, int((time.monotonic()-invocation_started)*1000)))
        return dict(status=result_status, diagnostics=diagnostics, **fields)

    persistent_identity: tuple[int, int] | None = None
    persistent_created = False
    process: subprocess.Popen[bytes] | None = None
    process_cleanup_confirmed = True
    captured_identity: tuple[int, int] | None = None
    timeout_cleanup_state: str | None = None
    copy_identity = executable_identity = None
    client_root = client_lock_identity = None
    client_lock_reusable = True
    client_had_retry = False
    status = "capture_failed"
    try:
        if prepare_copy:
            client_root, client_lock_identity, error = _client_root(config)
            if error:
                status = error
                return dict(status=status)
            selected_executable, error, detail = _prepare_reusable_client(client_root, selected_executable)
            if error:
                status = error
                return dict(status=status, **({'error': _safe_diagnostic_text(detail)} if detail else {}))
            copy_bundle = selected_executable.parents[2]
            copy_info, executable_info = copy_bundle.lstat(), selected_executable.lstat()
            copy_identity = (copy_info.st_dev, copy_info.st_ino)
            executable_identity = (executable_info.st_dev, executable_info.st_ino)
        created, diagnostic_identity = _write_secret_exclusive(diagnostic_path, b'')
        if not created:
            return response('capture_diagnostic_invalid')
        from importlib.resources import as_file, files

        renderer_resource = files("wechat_local").joinpath("resources/tools/render_lldb_capture.py")
        renderer_remaining = None if _capture_deadline is None else _capture_deadline - time.monotonic()
        if renderer_remaining is not None and renderer_remaining <= 0:
            return response('capture_timeout', failure_reason='capture_timeout')
        with as_file(renderer_resource) as renderer_path:
            try:
                rendered = subprocess.run(
                    [
                        str(Path(sys.executable).absolute()), str(renderer_path),
                        "--wechat-executable", str(selected_executable),
                        "--wechat-data-home", str(Path(wechat_data_home).expanduser().absolute()),
                        "--passphrase-output", str(captured_path),
                        "--output-script", str(script_path),
                        "--diagnostic-output", str(diagnostic_path),
                        "--diagnostic-token", diagnostic_token,
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                    timeout=10 if renderer_remaining is None else min(10, renderer_remaining),
                )
            except subprocess.TimeoutExpired:
                if _capture_deadline is not None and time.monotonic() >= _capture_deadline:
                    return response('capture_timeout', failure_reason='capture_timeout')
                return response('capture_script_failed')
        if rendered.returncode != 0 or not script_path.is_file():
            status = "capture_script_failed"
            return response(status)
        try:
            capture_info = captured_path.lstat()
            if not stat.S_ISREG(capture_info.st_mode) or stat.S_IMODE(capture_info.st_mode) != 0o600:
                return response("capture_output_invalid")
            captured_identity = (capture_info.st_dev, capture_info.st_ino)
        except OSError:
            return response("capture_output_missing")

        child_env = os.environ.copy()
        for name in ("PYTHONPATH", "PYTHONHOME", "PYTHONUSERBASE", "VIRTUAL_ENV"):
            child_env.pop(name, None)
        child_env["TERM"] = "dumb"
        argv = [str(lldb_resolved), "-s", str(script_path)]
        capture_deadline = (_capture_deadline if _capture_deadline is not None else
                            (time.monotonic() + timeout_seconds if timeout_seconds > 0 else None))
        if prepare_copy:
            policy_observer = _PolicyObserver(copy_bundle)
            try:
                policy_observer.start()
            except KeyboardInterrupt:
                policy_observation['state'] = 'cancelled'
                return response('capture_cancelled', failure_reason='debugger_cancelled')
            policy_observation['state'] = 'watching'
        try:
            if policy_observer is not None:
                policy_observer.arm()
            if capture_deadline is not None and time.monotonic() >= capture_deadline:
                raise subprocess.TimeoutExpired(argv, timeout_seconds)
            process = subprocess.Popen(
                argv,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=child_env,
                start_new_session=True,
            )
            client_lock_reusable = False
            output_reader = _DebuggerErrors(process.stdout)
            # Source failures return to the command interpreter. These queued commands
            # also finish a successful run; only this invocation's SBTarget is owned.
            try:
                process.stdin.write(b"script _capture_session.finish()\nquit\n")
                process.stdin.close()
            except BrokenPipeError:
                pass
            remaining = None if capture_deadline is None else max(0.0, capture_deadline - time.monotonic())
            process.wait(timeout=remaining)
        except (subprocess.TimeoutExpired, KeyboardInterrupt) as interrupted:
            timed_out = isinstance(interrupted, subprocess.TimeoutExpired)
            cancelled = not timed_out
            if process is None:
                if cancelled and policy_observer is not None:
                    policy_observation['state'] = 'cancelled'
                status = 'capture_cancelled' if cancelled else 'capture_timeout'
                return response(status, failure_reason='debugger_cancelled' if cancelled else 'debugger_timeout')
            process_cleanup_confirmed = _signal_owned_group(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                process_cleanup_confirmed = _signal_owned_group(process.pid, signal.SIGKILL) and process_cleanup_confirmed
                try:
                    process.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    process_cleanup_confirmed = _stop_owned_group(process.pid) and process_cleanup_confirmed
                    group_cleanup = 'unknown'
                    process_cleanup_confirmed = False
                    status = "debugger_cleanup_uncertain"
                    timeout_cleanup_state = _timeout_capture_cleanup_state(captured_path, captured_identity)
                    return response(status, failure_reason='debugger_timeout' if timed_out else 'debugger_cancelled', cleanup_state=timeout_cleanup_state)
            process_cleanup_confirmed = _stop_owned_group(process.pid) and process_cleanup_confirmed
            group_cleanup = 'confirmed' if process_cleanup_confirmed else 'unknown'
            # Reaping LLDB and its process group does not prove that an
            # launched target escaped into another process group has exited.
            # Always report this lifecycle boundary as uncertain on timeout.
            process_cleanup_confirmed = False
            status = "debugger_cleanup_uncertain"
            timeout_cleanup_state = _timeout_capture_cleanup_state(captured_path, captured_identity)
            return response(status, failure_reason='debugger_timeout' if timed_out else 'debugger_cancelled', cleanup_state=timeout_cleanup_state)
        except OSError as error:
            python_error = _safe_diagnostic_text(type(error).__name__ + ': ' + (str(error).splitlines() or [''])[-1])
            status = "debugger_unavailable"
            return response(status)
        if output_reader is not None:
            output_reader.close()
        process_cleanup_confirmed = _stop_owned_group(process.pid)
        group_cleanup = 'confirmed' if process_cleanup_confirmed else 'unknown'
        if not process_cleanup_confirmed:
            status = "debugger_cleanup_uncertain"
            return response(status)
        if process.returncode != 0:
            status = "debugger_failed"
            return response(status)
        evidence = dict(response('diagnostic_checked')['diagnostics'])
        client_lock_reusable = evidence['channel'] == 'valid' and evidence['target_cleanup'] == 'confirmed' and group_cleanup == 'confirmed'
        if evidence['channel'] != 'valid':
            return response('capture_diagnostic_invalid')
        if (any(evidence['stages'][name] != 'completed' for name in _CAPTURE_STAGES)
                or evidence['failed_stage'] is not None or evidence['target_exit'] != 'intentional'):
            status = 'capture_stage_failed'
            if prepare_copy and _retryable_initial_launch(evidence, captured_path, captured_identity, config, passphrase_path):
                policy_observation['state'] = 'waiting'
                print('等待本次私有副本的扫描完成观察；可按 Ctrl-C 取消。', file=sys.stderr, flush=True)
                try:
                    policy_observer.bind_target(evidence['target_pid'])
                    observed = policy_observer.wait(capture_deadline)
                except KeyboardInterrupt:
                    policy_observation['state'] = 'cancelled'
                    return response(status, failure_reason='scan_wait_cancelled')
                policy_observation['state'] = observed
                if observed != 'completed':
                    return response(status, failure_reason='scan_wait_' + observed)
                try:
                    observed = policy_observer.close(require_active=True)
                    policy_observer = None
                    policy_observation['state'] = observed
                    if observed != 'completed':
                        return response(status, failure_reason='scan_wait_' + observed)
                except KeyboardInterrupt:
                    policy_observation['state'] = 'cancelled'
                    return response(status, failure_reason='scan_wait_cancelled')
                except _PolicyObservationLost:
                    policy_observation['state'] = 'unavailable'
                    return response(status, failure_reason='scan_wait_unavailable')
                except (OSError, subprocess.TimeoutExpired):
                    process_cleanup_confirmed = False
                    client_lock_reusable = False
                    policy_observation['state'] = 'cleanup_uncertain'
                    return response('policy_observer_cleanup_uncertain')
                if not _retryable_initial_launch(evidence, captured_path, captured_identity, config, passphrase_path):
                    policy_observation['state'] = 'source_changed'
                    return response(status, failure_reason='scan_wait_source_changed')
                try:
                    stage_info, copy_info, executable_info = stage.lstat(), copy_bundle.lstat(), selected_executable.lstat()
                    same_copy = (stat.S_ISDIR(stage_info.st_mode) and (stage_info.st_dev, stage_info.st_ino) == stage_identity
                                 and stat.S_ISDIR(copy_info.st_mode) and (copy_info.st_dev, copy_info.st_ino) == copy_identity
                                 and stat.S_ISREG(executable_info.st_mode) and executable_info.st_nlink == 1
                                 and (executable_info.st_dev, executable_info.st_ino) == executable_identity
                                 and _no_symlink_path(selected_executable)
                                 and copy_bundle.resolve(strict=True) in selected_executable.parents)
                except OSError:
                    same_copy = False
                if not same_copy:
                    # A replaced/unknown object is not ours to remove or launch.
                    process_cleanup_confirmed = False
                    client_lock_reusable = False
                    policy_observation['state'] = 'source_changed'
                    return response(status, failure_reason='scan_wait_source_changed')
                if capture_deadline is not None and time.monotonic() >= capture_deadline:
                    policy_observation['state'] = 'timeout'
                    return response(status, failure_reason='scan_wait_timeout')
                # The outer private stage owns the copy until the second invocation returns.
                # A normal inner call creates a distinct stage and cannot retry recursively.
                client_lock_reusable = False
                client_had_retry = True
                policy_observation['retry'] = 'started'
                second = capture_and_save_passphrase(
                    account_root=root, wechat_executable=selected_executable,
                    wechat_data_home=wechat_data_home, lldb_executable=lldb_resolved,
                    config_path=config, timeout_seconds=timeout_seconds, prepare_copy=False,
                    _validation_observations=_validation_observations, _capture_deadline=capture_deadline)
                second_evidence = second.get('diagnostics')
                snapshot = dict(second_evidence) if isinstance(second_evidence, dict) else None
                client_lock_reusable = bool(snapshot and snapshot.get('channel') == 'valid' and snapshot.get('group_cleanup') == 'confirmed' and snapshot.get('target_cleanup') == 'confirmed')
                process_cleanup_confirmed = (second.get('status') == 'captured_and_verified'
                                             and snapshot is not None
                                             and snapshot.get('channel') == 'valid'
                                             and snapshot.get('group_cleanup') == 'confirmed'
                                             and snapshot.get('target_cleanup') == 'confirmed')
                status = str(second.get('status', 'capture_failed'))
                if snapshot is None:
                    second['diagnostics'] = {}
                second['diagnostics']['attempts'] = [
                    dict(status='capture_stage_failed', diagnostics=evidence),
                    dict(status=status, diagnostics=snapshot),
                ]
                policy_observation['retry'] = 'completed' if status == 'captured_and_verified' else 'failed'
                second['diagnostics']['policy_observation'] = dict(policy_observation)
                return second
            return response(status)
        if prepare_copy:
            policy_observation['state'] = 'not_needed'
            try:
                policy_observer.close()
                policy_observer = None
            except (OSError, subprocess.TimeoutExpired):
                process_cleanup_confirmed = False
                client_lock_reusable = False
                policy_observation['state'] = 'cleanup_uncertain'
                return response('policy_observer_cleanup_uncertain')
        processing['validation'] = 'started'
        try:
            fd = os.open(
                captured_path,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
            )
            with os.fdopen(fd, "rb") as handle:
                file_info = os.fstat(handle.fileno())
                if (
                    not stat.S_ISREG(file_info.st_mode)
                    or stat.S_IMODE(file_info.st_mode) & 0o077
                    or captured_identity != (file_info.st_dev, file_info.st_ino)
                ):
                    return response("capture_output_invalid")
                if file_info.st_size != 32:
                    return response("capture_length_invalid")
                captured = handle.read(33)
        except OSError:
            return response("capture_output_missing")
        if len(captured) != 32:
            return response("capture_length_invalid")
        try:
            if discover_numeric_shards(message_root) != shards:
                return response("message_source_changed")
        except LiveCatalogError:
            return response("message_source_unavailable")
        observations: dict = {}
        if _validation_observations is None:
            verified = _verify_all_shards(shards, captured)
        else:
            verified = _verify_all_shards(shards, captured, root=root / 'db_storage', observations=observations)
        if not verified:
            status = "message_source_changed" if observations.get('_source_changed') else "capture_key_invalid"
            return response(status)
        if _validation_observations is not None:
            from .source_setup import _database_stamp
            try:
                if discover_numeric_shards(message_root) != shards or any(
                        _database_stamp(path, root / 'db_storage') != entry[0]
                        for path, entry in observations.items()):
                    return response("message_source_changed")
            except LiveCatalogError:
                return response("message_source_changed")

        processing['validation'] = 'completed'
        processing['persistence'] = 'started'
        created, persistent_identity = _write_secret_exclusive(passphrase_path, captured)
        if not created:
            status = "credential_save_failed"
            return response(status)
        processing['persistence'] = 'completed'
        persistent_created = True
        status = "captured_and_verified"
        final_response = response(status, passphrase_file=str(passphrase_path), _passphrase_identity=persistent_identity)
        if any(final_response['diagnostics'][key] != evidence[key] for key in _unknown_capture_diagnostic('valid')):
            status = 'capture_diagnostic_invalid'
            return response(status)
        if _validation_observations is not None:
            parent_info = passphrase_path.parent.lstat()
            _validation_observations.update(root=root / 'db_storage', shards=shards, entries=observations,
                                            credential_identity=persistent_identity, credential=captured,
                                            credential_parent_identity=(parent_info.st_dev, parent_info.st_ino))
        return final_response
    except _PreparationCleanupUncertain as error:
        process_cleanup_confirmed = False
        client_lock_reusable = False
        status = 'preparation_cleanup_uncertain'
        return dict(status=status, error=_safe_diagnostic_text(str(error)))
    except KeyboardInterrupt:
        cancelled = True
        if process is not None and group_cleanup != 'confirmed':
            process_cleanup_confirmed = False
            client_lock_reusable = False
        if prepare_copy and policy_observation['state'] in {'watching', 'waiting'}:
            policy_observation['state'] = 'cancelled'
        status = 'capture_cancelled'
        return response(status, failure_reason='debugger_cancelled')
    except Exception as error:
        python_error = _safe_diagnostic_text(type(error).__name__ + ': ' + (str(error).splitlines() or [''])[-1])
        if status == 'captured_and_verified':
            status = 'capture_failed'
        return response(status)
    finally:
        if policy_observer is not None:
            try:
                policy_observer.close()
            except (OSError, subprocess.TimeoutExpired):
                process_cleanup_confirmed = False
                client_lock_reusable = False
        if output_reader is not None and not output_reader.stream.closed:
            output_reader.close()
        if process is not None and process.stdin is not None and not process.stdin.closed:
            process.stdin.close()
        if status != "captured_and_verified" and persistent_created:
            _unlink_owned(passphrase_path, persistent_identity)
        if client_root is not None and process is not None and not client_had_retry and not timed_out and not cancelled and process_cleanup_confirmed and group_cleanup == 'confirmed' and process.poll() is not None:
            final_evidence = _read_capture_diagnostic(diagnostic_path, diagnostic_identity, diagnostic_token, process.pid)
            if final_evidence['channel'] == 'valid' and (final_evidence['target_state'] == 'exited' or final_evidence['stages']['launch'] == 'not_started'):
                client_lock_reusable = True
        try:
            if status != "captured_and_verified":
                if timeout_cleanup_state is None:
                    _unlink_owned(captured_path, captured_identity)
            info = stage.lstat()
            process_reaped = (process is None or process.poll() is not None) and process_cleanup_confirmed
            if prepare_copy and process is not None:
                evidence = _read_capture_diagnostic(diagnostic_path, diagnostic_identity, diagnostic_token, process.pid)
                target_safe = (evidence['channel'] == 'valid' and
                               (evidence['target_state'] == 'exited' or evidence['stages']['launch'] == 'not_started'))
                process_reaped = process_reaped and target_safe
            if process_reaped and stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode) and (info.st_dev, info.st_ino) == stage_identity:
                shutil.rmtree(stage)
        except OSError:
            pass
        if client_root is not None and client_lock_identity is not None and client_lock_reusable:
            _release_client_lock(client_root, client_lock_identity)
