"""Bounded, source-bound image enrichment for local WeChat messages.

This module is intentionally a narrow adapter. A caller must first apply the
query's user-selected conversation/time bounds and pass the exact message
identity. It never searches all attachments or selects a file by size alone.
"""

from __future__ import annotations

import hashlib
import io
import os
import re
import stat
import struct
import subprocess
import tempfile
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timezone, tzinfo
from pathlib import Path
from typing import Any, Iterable

try:
    from PIL import Image, UnidentifiedImageError
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.primitives.padding import PKCS7
except ImportError:  # optional image extras
    Image = None  # type: ignore[assignment]
    UnidentifiedImageError = Exception  # type: ignore[assignment,misc]
    Cipher = algorithms = modes = PKCS7 = None  # type: ignore[assignment]


MAX_INPUT_BYTES = 32 * 1024 * 1024
MAX_OUTPUT_BYTES = 32 * 1024 * 1024
MAX_PIXELS = 25_000_000
MAX_FFMPEG_ALLOC = 64 * 1024 * 1024
FFMPEG_TIMEOUT_SECONDS = 25
_IMAGE_TOKEN = re.compile(rb"[a-fA-F0-9]{32}")
_STATIC_SIGNATURES = (b"\x89PNG\r\n\x1a\n", b"\xff\xd8\xff", b"GIF87a", b"GIF89a")
_V2_MAGIC = b"\x07\x08V2\x08\x07"


@dataclass(frozen=True)
class ImageMessageIdentity:
    conversation_id: str
    server_id: int
    local_id: int
    create_time: int
    local_type: int
    display_timezone: tzinfo = timezone.utc

    def __post_init__(self) -> None:
        if not self.conversation_id or self.server_id <= 0 or self.local_id < 0:
            raise ValueError("invalid_image_message_identity")
        if self.local_type & 0xFFFFFFFF != 3:
            raise ValueError("not_an_image_message")


class ImageUnavailable(RuntimeError):
    """A controlled media failure; callers should retain the original event."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class ImageRepresentations:
    expected_sizes: tuple[int, ...]
    files: tuple[tuple[int, Path], ...]
    missing_sizes: tuple[int, ...]
    _hardlink_bytes: tuple[bytes | None, ...] = field(default=(), repr=False, compare=False)


def _query_only(connection: Any) -> None:
    row = connection.execute("PRAGMA query_only").fetchone()
    if not row or int(row[0]) != 1:
        raise ImageUnavailable("image_source_unavailable")


def _read_same_inode(paths: list[Path], expected_size: int,
                     directories: list[tuple[Path, os.stat_result]],
                     candidates: list[os.stat_result]) -> bytes:
    """Capture one verified object shared by multiple hardlink paths."""
    def signature(value: os.stat_result) -> tuple[int, ...]:
        return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
    try:
        def directory_identity(value: os.stat_result) -> tuple[int, int]:
            return value.st_dev, value.st_ino
        handles: list[int] = []
        def open_directories() -> int:
            parent = None
            for path, prior in directories:
                fd = os.open(path if parent is None else path.name,
                             os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                             **({} if parent is None else {'dir_fd': parent}))
                handles.append(fd)
                now = os.fstat(fd)
                if not stat.S_ISDIR(now.st_mode) or directory_identity(now) != directory_identity(prior):
                    raise ImageUnavailable('image_attachment_changed')
                parent = fd
            assert parent is not None
            return parent
        parent = open_directories()
        before = [os.stat(path.name, dir_fd=parent, follow_symlinks=False) for path in paths]
        if any(not stat.S_ISREG(item.st_mode) or item.st_size != expected_size for item in before):
            raise ImageUnavailable('image_attachment_changed')
        if any(signature(actual) != signature(prior) for actual, prior in zip(before, candidates)):
            raise ImageUnavailable('image_attachment_changed')
        if len({(item.st_dev,item.st_ino) for item in before}) != 1:
            raise ImageUnavailable('image_attachment_ambiguous')
        expected = signature(before[0])
        if any(signature(item) != expected for item in before):
            raise ImageUnavailable('image_attachment_changed')
        fd = os.open(paths[0].name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        try:
            opened = os.fstat(fd)
            if not stat.S_ISREG(opened.st_mode) or signature(opened) != expected:
                raise ImageUnavailable('image_attachment_changed')
            chunks = []
            remaining = expected_size + 1
            while remaining:
                chunk = os.read(fd, min(1024 * 1024, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            raw = b''.join(chunks)
            if (len(raw) != expected_size or signature(os.fstat(fd)) != expected
                    or any(signature(os.stat(path.name, dir_fd=parent, follow_symlinks=False)) != expected for path in paths)):
                raise ImageUnavailable('image_attachment_changed')
            open_directories()
            return raw
        finally:
            os.close(fd)
    except ImageUnavailable:
        raise
    except OSError as exc:
        raise ImageUnavailable('image_attachment_changed') from exc
    finally:
        for handle in reversed(locals().get('handles', [])):
            os.close(handle)


def _bound_file(
    resource_connection: Any,
    identity: ImageMessageIdentity,
    attachments_root: Path,
) -> ImageRepresentations:
    """Resolve all metadata-sized representations for one exact message identity."""

    _query_only(resource_connection)
    rows = resource_connection.execute(
        """SELECT i.message_id, i.packed_info
             FROM MessageResourceInfo AS i
             JOIN ChatName2Id AS n ON n.rowid = i.chat_id
            WHERE n.user_name = ? AND i.message_svr_id = ?
              AND i.message_local_id = ? AND i.message_create_time = ?
              AND i.message_local_type = ?""",
        (
            identity.conversation_id,
            identity.server_id,
            identity.local_id,
            identity.create_time,
            identity.local_type & 0xFFFFFFFF,
        ),
    ).fetchall()
    if len(rows) != 1:
        raise ImageUnavailable("image_resource_ambiguous" if rows else "image_resource_missing")
    message_id, packed_info = rows[0]
    if not isinstance(message_id, (str, int)) or packed_info is None:
        raise ImageUnavailable("image_resource_unavailable")
    tokens = {match.decode("ascii").lower() for match in _IMAGE_TOKEN.findall(bytes(packed_info))}
    if len(tokens) != 1:
        raise ImageUnavailable("image_resource_ambiguous")
    sizes = {
        int(row[0])
        for row in resource_connection.execute(
            "SELECT size FROM MessageResourceDetail WHERE message_id = ?",
            (message_id,),
        ).fetchall()
        if row and row[0] is not None and int(row[0]) > 0
    }
    if not sizes or any(size > MAX_INPUT_BYTES for size in sizes):
        raise ImageUnavailable("image_resource_unavailable")
    month = datetime.fromtimestamp(identity.create_time, identity.display_timezone).strftime("%Y-%m")
    if attachments_root.is_symlink():
        raise ImageUnavailable("image_attachment_missing")
    try:
        root = attachments_root.resolve(strict=True)
    except OSError as exc:
        raise ImageUnavailable("image_attachment_missing") from exc
    folder = root / hashlib.md5(identity.conversation_id.encode("utf-8")).hexdigest() / month / "Img"
    try:
        folder.relative_to(root)
        # Reject symlinks at every directory component before resolving any
        # candidate or reading its bytes.
        current = root
        directories = [(root, root.lstat())]
        for component in folder.relative_to(root).parts:
            current = current / component
            if current.is_symlink():
                raise ImageUnavailable("image_attachment_missing")
            directory_stat = current.lstat()
            if not stat.S_ISDIR(directory_stat.st_mode):
                raise ImageUnavailable("image_attachment_missing")
            directories.append((current, directory_stat))
        resolved_folder = folder.resolve(strict=True)
        resolved_folder.relative_to(root)
    except (OSError, ValueError):
        raise ImageUnavailable("image_attachment_missing")
    if not resolved_folder.is_dir():
        raise ImageUnavailable("image_attachment_missing")
    folder = resolved_folder
    token = next(iter(tokens))
    representations: list[tuple[int, Path]] = []
    hardlink_bytes: list[bytes | None] = []
    missing_sizes: list[int] = []
    for expected_size in sorted(sizes):
        matches: list[Path] = []
        candidate_stats: dict[Path, os.stat_result] = {}
        for candidate in folder.glob(token + "*"):
            if candidate.is_symlink() or not candidate.is_file():
                continue
            try:
                resolved_candidate = candidate.resolve(strict=True)
                resolved_candidate.relative_to(root)
                size = candidate.stat().st_size
                candidate_stat = candidate.lstat()
            except (OSError, ValueError):
                continue
            if size == expected_size and 0 < size <= MAX_INPUT_BYTES:
                matches.append(candidate)
                candidate_stats[candidate] = candidate_stat
        captured = None
        if matches:
            matches.sort(key=lambda path: path.name)
            captured = _read_same_inode(matches, expected_size, directories,
                                        [candidate_stats[path] for path in matches])
        if matches:
            representations.append((expected_size, matches[0]))
            hardlink_bytes.append(captured)
        else:
            missing_sizes.append(expected_size)
    if not representations:
        raise ImageUnavailable("image_attachment_missing")
    return ImageRepresentations(tuple(sorted(sizes)), tuple(representations), tuple(missing_sizes), tuple(hardlink_bytes))


def _decrypt_v2(data: bytes, key_codes: Iterable[int], account_scope: str) -> bytes:
    if Cipher is None or algorithms is None or modes is None or PKCS7 is None:
        raise ImageUnavailable("image_decoder_unavailable")
    if not data.startswith(_V2_MAGIC) or len(data) < 15:
        raise ImageUnavailable("image_container_unsupported")
    plain_size, tail_size = struct.unpack("<II", data[6:14])
    encrypted_size = plain_size + 16 - (plain_size % 16) if plain_size else 0
    if not encrypted_size or encrypted_size > MAX_INPUT_BYTES or 15 + encrypted_size + tail_size > len(data):
        raise ImageUnavailable("image_container_invalid")
    decrypted: list[bytes] = []
    for code in key_codes:
        if not isinstance(code, int) or code < 0:
            continue
        key = hashlib.md5((str(code) + account_scope).encode("utf-8")).hexdigest()[:16].encode("ascii")
        try:
            decryptor = Cipher(algorithms.AES(key), modes.ECB()).decryptor()
            block = decryptor.update(data[15 : 15 + encrypted_size]) + decryptor.finalize()
            unpadder = PKCS7(algorithms.AES.block_size).unpadder()
            body = unpadder.update(block) + unpadder.finalize()
            if len(body) != plain_size:
                continue
        except ValueError:
            continue
        tail = data[15 + encrypted_size :]
        checksum = bytes(value ^ (code & 0xFF) for value in tail[-tail_size:]) if tail_size else b""
        decrypted.append(body + (tail[:-tail_size] if tail_size else tail) + checksum)
    if len(decrypted) != 1:
        raise ImageUnavailable("image_key_ambiguous" if decrypted else "image_decrypt_unavailable")
    return decrypted[0]


def _hevc_first_frame(data: bytes, ffmpeg: Path) -> bytes:
    positions = [match.start() for match in re.finditer(b"\x00\x00\x00\x01\x40|\x00\x00\x01\x40", data)]
    if not positions:
        raise ImageUnavailable("image_hevc_header_missing")
    try:
        if Image is None:
            raise ImageUnavailable("image_decoder_unavailable")
        result = subprocess.run(
            [
                str(ffmpeg), "-hide_banner", "-loglevel", "error", "-max_alloc", str(MAX_FFMPEG_ALLOC),
                "-protocol_whitelist", "pipe", "-threads", "1", "-f", "hevc", "-i", "pipe:0",
                "-map", "0:v:0", "-frames:v", "1", "-an", "-threads", "1", "-c:v", "png",
                "-f", "image2pipe", "-fs", str(MAX_OUTPUT_BYTES), "pipe:1",
            ],
            input=data[min(positions):],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=FFMPEG_TIMEOUT_SECONDS,
            env={"PATH": "/usr/bin:/bin:/usr/sbin:/sbin"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ImageUnavailable("image_decode_unavailable") from exc
    if result.returncode != 0 or not result.stdout or len(result.stdout) > MAX_OUTPUT_BYTES:
        raise ImageUnavailable("image_decode_unavailable")
    return result.stdout


def decode_image(
    source: Path,
    *,
    output_root: Path,
    key_codes: Iterable[int] = (),
    account_scope: str = "",
    ffmpeg: Path | None = None,
) -> dict[str, Any]:
    """Decode one already-bound attachment into a private PNG file."""

    try:
        if source.is_symlink() or not source.is_file() or source.stat().st_size > MAX_INPUT_BYTES:
            raise ImageUnavailable("image_input_unavailable")
        raw = source.read_bytes()
        if not raw or len(raw) > MAX_INPUT_BYTES:
            raise ImageUnavailable("image_input_unavailable")
    except ImageUnavailable as exc:
        return {"status": "unavailable", "reason": exc.code}
    except (OSError, ValueError):
        return {"status": "unavailable", "reason": "image_decode_unavailable"}
    return _decode_image_bytes(raw, output_root=output_root, key_codes=key_codes,
                               account_scope=account_scope, ffmpeg=ffmpeg)


def _decode_image_bytes(
    raw: bytes,
    *,
    output_root: Path,
    key_codes: Iterable[int] = (),
    account_scope: str = "",
    ffmpeg: Path | None = None,
) -> dict[str, Any]:
    """Decode already-read, bounded bytes without reopening a source path."""
    try:
        if not raw or len(raw) > MAX_INPUT_BYTES:
            raise ImageUnavailable('image_input_unavailable')
        if raw.startswith(_V2_MAGIC):
            decoded = _decrypt_v2(raw, key_codes, account_scope)
        else:
            decoded = raw
        first_frame_only = False
        if decoded.startswith(b"wxgf"):
            if ffmpeg is None or ffmpeg.is_symlink() or not ffmpeg.is_file():
                raise ImageUnavailable("image_decoder_unavailable")
            decoded = _hevc_first_frame(decoded, ffmpeg)
            first_frame_only = True
        if len(decoded) > MAX_OUTPUT_BYTES:
            raise ImageUnavailable("image_output_too_large")
        Image.MAX_IMAGE_PIXELS = MAX_PIXELS
        warnings.simplefilter("error", Image.DecompressionBombWarning)
        with Image.open(io.BytesIO(decoded)) as image:
            if image.format not in {"PNG", "JPEG", "GIF"}:
                raise ImageUnavailable("image_format_unsupported")
            image.load()
            if image.width * image.height > MAX_PIXELS:
                raise ImageUnavailable("image_pixel_limit")
            if image.format == "GIF":
                image.seek(0)
            output_root.mkdir(mode=0o700, parents=True, exist_ok=True)
            if output_root.is_symlink():
                raise ImageUnavailable("image_output_unavailable")
            with tempfile.NamedTemporaryFile(prefix="wechat-image-", suffix=".png", dir=output_root, delete=False) as handle:
                output = Path(handle.name)
            os.chmod(output, 0o600)
            image.convert("RGB").save(output, format="PNG", optimize=False)
        if output.stat().st_size > MAX_OUTPUT_BYTES:
            output.unlink(missing_ok=True)
            raise ImageUnavailable("image_output_too_large")
        return {"status": "decoded", "path": str(output), "size": [image.width, image.height], "first_frame_only": first_frame_only}
    except ImageUnavailable as exc:
        return {"status": "unavailable", "reason": exc.code}
    except (OSError, ValueError, UnidentifiedImageError, struct.error):
        return {"status": "unavailable", "reason": "image_decode_unavailable"}


def enrich_bound_images(
    resource_connection: Any,
    identity: ImageMessageIdentity,
    *,
    attachments_root: Path,
    output_root: Path,
    key_codes: Iterable[int] = (),
    account_scope: str = "",
    ffmpeg: Path | None = None,
) -> dict[str, Any]:
    """Decode each uniquely matched resource-size representation of one message."""

    try:
        bound = _bound_file(resource_connection, identity, attachments_root)
    except ImageUnavailable as exc:
        return {"status": "unavailable", "reason": exc.code}
    decoded = []
    for index, (resource_size, source) in enumerate(bound.files):
        captured = bound._hardlink_bytes[index] if index < len(bound._hardlink_bytes) else None
        if captured is None:
            result = decode_image(source, output_root=output_root, key_codes=key_codes,
                                  account_scope=account_scope, ffmpeg=ffmpeg)
        else:
            result = _decode_image_bytes(captured, output_root=output_root, key_codes=key_codes,
                                         account_scope=account_scope, ffmpeg=ffmpeg)
        decoded.append({"resource_size_bytes": resource_size, **result})
    successes = sum(item.get("status") == "decoded" for item in decoded)
    if successes == len(decoded) and not bound.missing_sizes:
        status = "decoded"
    elif successes:
        status = "partial"
    else:
        status = "unavailable"
    return {
        "status": status,
        "expected_resource_sizes": list(bound.expected_sizes),
        "missing_resource_sizes": list(bound.missing_sizes),
        "representations": decoded,
    }
