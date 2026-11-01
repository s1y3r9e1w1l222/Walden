"""Read-only file references for exact, already-attributed message revisions.

No source DB access, download, content parser, extraction or persistent cache.
ResourceBinding is accepted only from an adapter with verified DB semantics;
untrusted XML paths and download flags are deliberately not resource bindings.
"""
from __future__ import annotations

import hashlib
import errno
import os
import re
import stat
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from pathlib import Path, PurePosixPath

import zstandard

from .event_store import RawMessage, SourceKey, _content_fingerprint

MAX_XML_BYTES = 1_048_576
MAX_FILE_BYTES = 256 * 1024 * 1024
MAX_HASH_BYTES = 512 * 1024 * 1024
ZIP_EXTENSIONS = {'.xlsx', '.docx', '.docm', '.xlsm', '.xltm', '.numbers', '.pptx', '.zip'}
OLE_EXTENSIONS = {'.doc', '.xls', '.wps'}
SUPPORTED = ZIP_EXTENSIONS | OLE_EXTENSIONS | {'.pdf', '.rar', '.7z'}


def _stamp(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def _signature_matches(header, extension):
    if extension in ZIP_EXTENSIONS:
        return header[:4] in {b'PK\x03\x04', b'PK\x05\x06', b'PK\x07\x08'}
    if extension in OLE_EXTENSIONS:
        return header.startswith(b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1')
    if extension == '.rar':
        return header.startswith((b'Rar!\x1a\x07\x00', b'Rar!\x1a\x07\x01\x00'))
    if extension == '.7z':
        return header.startswith(b'7z\xbc\xaf\x27\x1c')
    return extension == '.pdf' and header.startswith(b'%PDF-')


@dataclass
class _VerificationAudit:
    remaining_bytes: int
    observed: dict[str, tuple] = field(default_factory=dict)


@dataclass(frozen=True)
class ResourceBinding:
    source: SourceKey
    local_id: int
    raw_fingerprint: str
    relative_path: str | None
    downloaded: bool | None = None


def _metadata(message: RawMessage) -> tuple[str, int | None, str | None, str | None] | None:
    raw = message.raw_content
    if isinstance(raw, bytes):
        if len(raw) > MAX_XML_BYTES:
            raise ValueError('metadata_too_large')
        if message.compression_type == 4:
            # stream_reader bounds frames even when the declared size is huge.
            with zstandard.ZstdDecompressor().stream_reader(raw) as reader:
                raw = reader.read(MAX_XML_BYTES + 1)
        elif message.compression_type not in (None, 0):
            raise ValueError('unsupported_compression')
        raw = raw.decode('utf-8')
    if not isinstance(raw, str) or len(raw.encode('utf-8')) > MAX_XML_BYTES:
        raise ValueError('invalid_xml')
    if '<!DOCTYPE' in raw.upper() or '<!ENTITY' in raw.upper():
        raise ValueError('invalid_xml')
    # Group-message sender prefixes are recognized by the existing normalizer.
    start = raw.find('<')
    root = ET.fromstring(raw[start:] if start >= 0 else raw)
    apps = [root] if root.tag == 'appmsg' else list(root.findall('appmsg'))
    if len(apps) != 1:
        raise ValueError('invalid_appmsg')
    app = apps[0]

    def field(parent, name):
        nodes = parent.findall(name)
        if len(nodes) > 1 or any(list(node) for node in nodes):
            raise ValueError('ambiguous_metadata')
        return (nodes[0].text or '').strip() if nodes else ''

    if field(app, 'type') != '6':
        return None
    name = field(app, 'title')
    if not name or name in {'.', '..'} or any(c in name for c in '/\\') or any(ord(c) < 32 for c in name):
        raise ValueError('invalid_filename')
    attaches = app.findall('appattach')
    if len(attaches) != 1:
        raise ValueError('invalid_appattach')
    size_text = field(attaches[0], 'totallen')
    if size_text and (len(size_text) > 20 or not size_text.isascii() or not size_text.isdecimal()):
        raise ValueError('invalid_size')
    size = int(size_text) if size_text else None
    digests = []
    # Only direct file-message fields; nested thumbnail/card digests are not
    # evidence for the file. Preserve the observed path for real acceptance.
    for node, path in ((attaches[0], 'appmsg/appattach/md5'), (app, 'appmsg/md5')):
        value = field(node, 'md5').lower()
        if value:
            if not re.fullmatch('[0-9a-f]{32}', value):
                raise ValueError('invalid_md5')
            digests.append((value, path))
    if len({value for value, _path in digests}) > 1:
        raise ValueError('conflicting_md5')
    md5, md5_path = digests[0] if digests else (None, None)
    return name, size, md5, md5_path


class AttachmentResolver:
    def __init__(self, root: Path, *, account_id: str, source_generation: str,
                 max_entries: int = 20_000):
        self.root = Path(root).absolute()
        self.account_id = account_id
        self.source_generation = source_generation
        if max_entries < 1:
            raise ValueError('max_entries must be positive')
        self.max_entries = max_entries
        # Remember directory identity; a replaced account root cannot reuse it.
        self._root_identity = self._root_stamp()
        self._directory_index = None

    def _begin_query(self):
        self._directory_index = None

    def _root_stamp(self):
        if self.root.resolve() != self.root:
            raise ValueError('unsafe_attachment_root')
        info = self.root.lstat()
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError('invalid_attachment_root')
        return info.st_dev, info.st_ino

    def _open(self, relative: str):
        path = PurePosixPath(relative)
        if (not relative or path.is_absolute() or '\\' in relative
                or any(p in {'', '.', '..'} for p in relative.split('/'))):
            raise ValueError('unsafe_path')
        directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            info = os.fstat(directory)
            if (info.st_dev, info.st_ino) != self._root_identity:
                raise ValueError('root_changed')
            for part in path.parts[:-1]:
                next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
                os.close(directory)
                directory = next_fd
            fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
            return os.fdopen(fd, 'rb')
        finally:
            os.close(directory)

    def _check_directory_stamps(self, stamps):
        """Reopen every enumerated directory without following links."""
        try:
            if self._root_stamp() != self._root_identity:
                raise ValueError('directory_changed')
            root_fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                for folder, expected in stamps:
                    current = os.dup(root_fd)
                    try:
                        for part in folder.relative_to(self.root).parts:
                            following = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current)
                            os.close(current)
                            current = following
                        if _stamp(os.fstat(current)) != expected:
                            raise ValueError('directory_changed')
                    finally:
                        os.close(current)
            finally:
                os.close(root_fd)
        except (OSError, ValueError) as error:
            raise ValueError('directory_changed') from error

    def _check_candidate_type(self, relative):
        path = PurePosixPath(relative)
        directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for part in path.parts[:-1]:
                following = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
                os.close(directory)
                directory = following
            if not stat.S_ISREG(os.stat(path.name, dir_fd=directory, follow_symlinks=False).st_mode):
                raise ValueError('unsafe_path')
        finally:
            os.close(directory)

    def _candidates(self, name):
        if self._directory_index is not None:
            index, stamps = self._directory_index
            try:
                self._check_directory_stamps(stamps)
            except ValueError:
                # The previous resolve's snapshot is no longer current. Start
                # a new complete scan before selecting this resolve's files.
                self._directory_index = None
            else:
                candidates = index.get(name, ())
                if len(candidates) > 100:
                    raise ValueError('candidate_limit')
                for relative in candidates:
                    self._check_candidate_type(relative)
                self._check_directory_stamps(stamps)
                return list(candidates), stamps
        candidates = []
        index = {}
        entries = 0
        stamps = []
        before_enumeration = {self.root: _stamp(self.root.stat())}
        def failed(_error):
            raise ValueError('directory_unavailable')
        for folder_name, dirs, files, directory in os.fwalk(self.root, follow_symlinks=False, onerror=failed):
            folder = Path(folder_name)
            before = before_enumeration.get(folder)
            if before is None or _stamp(os.fstat(directory)) != before:
                raise ValueError('directory_changed')
            if len(folder.relative_to(self.root).parts) > 8:
                raise ValueError('depth_limit')
            entries += len(dirs) + len(files)
            if entries > self.max_entries:
                raise ValueError('scan_limit')
            stamps.append((folder, before))
            for child in dirs:
                info = os.stat(child, dir_fd=directory, follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    before_enumeration[folder / child] = _stamp(info)
            for child in files:
                relative = str((folder / child).relative_to(self.root))
                index.setdefault(child, []).append(relative)
            if name in files:
                info = os.stat(name, dir_fd=directory, follow_symlinks=False)
                if not stat.S_ISREG(info.st_mode):
                    raise ValueError('unsafe_path')
                candidates.append(str((folder / name).relative_to(self.root)))
                if len(candidates) > 100:
                    raise ValueError('candidate_limit')
        self._check_directory_stamps(stamps)
        complete = {key: tuple(sorted(values)) for key, values in index.items()}
        self._directory_index = (complete, tuple(stamps))
        return sorted(candidates), self._directory_index[1]

    def _verify(self, relative, size, expected_md5, extension, audit):
        with self._open(relative) as handle:
            before = os.fstat(handle.fileno())
            if not stat.S_ISREG(before.st_mode):
                raise ValueError('unsafe_path')
            audit.observed[relative] = _stamp(before)
            if size is not None and before.st_size != size:
                raise ValueError('size_mismatch')
            if before.st_size > MAX_FILE_BYTES:
                raise ValueError('file_limit')
            if before.st_size > audit.remaining_bytes:
                raise ValueError('hash_budget')
            md5, sha256 = hashlib.md5(), hashlib.sha256()
            header = b''
            total = 0
            while True:
                block = handle.read(min(1024 * 1024, MAX_FILE_BYTES + 1 - total,
                                        audit.remaining_bytes + 1))
                if not block:
                    break
                if not header:
                    header = block[:8]
                total += len(block)
                if total > MAX_FILE_BYTES:
                    raise ValueError('file_limit')
                if len(block) > audit.remaining_bytes:
                    raise ValueError('hash_budget')
                audit.remaining_bytes -= len(block)
                md5.update(block)
                sha256.update(block)
            after = os.fstat(handle.fileno())
        if _stamp(before) != _stamp(after) or total != before.st_size:
            raise ValueError('file_changed')
        # Re-open through the same no-symlink traversal before returning a path.
        with self._open(relative) as current:
            if _stamp(os.fstat(current.fileno())) != _stamp(after):
                raise ValueError('file_changed')
        if expected_md5 and md5.hexdigest() != expected_md5:
            raise ValueError('hash_mismatch')
        if not _signature_matches(header, extension):
            raise ValueError('format_mismatch')
        return {'sha256': sha256.hexdigest(), 'byte_count': total}

    def resolve(self, source: SourceKey, message: RawMessage, *, resource: ResourceBinding | None = None) -> dict:
        binding = {**asdict(source), 'local_id': message.local_id, 'server_id': message.server_id,
                   'sender_id': message.real_sender_id, 'create_time': message.create_time,
                   'raw_fingerprint': _content_fingerprint(message)}
        base = {'kind': 'file', 'binding': binding, 'download_state': 'unknown'}
        def result(status, reason=None, **extra):
            return {**base, 'status': status, **({'reason': reason} if reason else {}), **extra}
        if (source.account_id, source.source_generation) != (self.account_id, self.source_generation):
            return result('binding_mismatch')
        if (message.local_type & 0xFFFFFFFF) != 49:
            return result('not_file')
        try:
            metadata = _metadata(message)
        except (ValueError, UnicodeError, ET.ParseError, zstandard.ZstdError):
            return result('invalid_metadata')
        if metadata is None:
            return result('not_file')
        name, size, md5, md5_path = metadata
        extension = Path(name).suffix.lower()
        if extension not in SUPPORTED:
            return result('unsupported_format')
        base.update({'filename': name, 'expected_size': size, 'expected_md5': md5,
                     'md5_xml_path': md5_path})
        if resource is not None:
            if (resource.source != source or resource.local_id != message.local_id
                    or resource.raw_fingerprint != binding['raw_fingerprint']):
                return result('binding_mismatch')
            if resource.downloaded is False:
                return result('not_downloaded', download_state='not_downloaded')
        try:
            if self._root_stamp() != self._root_identity:
                return result('unavailable', 'root_changed')
            stamps = []
            if resource is not None:
                if resource.relative_path is None:
                    return result('insufficient_evidence', 'resource_path_unknown')
                candidates = [resource.relative_path]
            else:
                candidates, stamps = self._candidates(name)
            if not candidates:
                return result('not_found')
            if resource is None and md5 is None:
                return result('insufficient_evidence', 'message_hash_unavailable')
            verified, failures = [], []
            audit = _VerificationAudit(MAX_HASH_BYTES)
            for relative in candidates:
                try:
                    verified.append((relative, self._verify(relative, size, md5, extension, audit)))
                except FileNotFoundError:
                    if resource is not None:
                        return result('missing', download_state='downloaded' if resource.downloaded else 'unknown')
                    return result('scan_incomplete', 'candidate_disappeared')
                except ValueError as exc:
                    reason = str(exc)
                    if reason == 'hash_budget':
                        return result('scan_incomplete', reason)
                    if reason not in {'size_mismatch', 'hash_mismatch', 'format_mismatch'}:
                        return result('unsafe_path' if reason == 'unsafe_path' else 'unavailable', reason)
                    failures.append(reason)
            if self._root_stamp() != self._root_identity:
                return result('scan_incomplete', 'directory_changed')
            if stamps:
                self._check_directory_stamps(stamps)
            for relative, observed in audit.observed.items():
                with self._open(relative) as current:
                    if _stamp(os.fstat(current.fileno())) != observed:
                        return result('unavailable', 'file_changed')
            if len(verified) > 1:
                return result('ambiguous', match_count=len(verified))
            if not verified:
                return result('verification_failed', failures[0])
            relative, facts = verified[0]
            return result('resolved', path=str(self.root / relative), **facts,
                          evidence='resource_binding' if resource is not None else 'filename_and_message_md5',
                          download_state='local_verified', format_check='signature_only')
        except ValueError as exc:
            reason = str(exc)
            return result('unsafe_path' if reason in {'unsafe_path', 'unsafe_attachment_root'} else 'scan_incomplete', reason)
        except OSError as exc:
            # Fixed reasons only: OS exceptions can contain private filenames.
            if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                return result('unsafe_path', 'symlink_or_non_directory')
            return result('unavailable', 'filesystem_unavailable')
