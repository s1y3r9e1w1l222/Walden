"""Resolve one explicitly selected WeChat account root into current-source paths.

Voice discovery inspects schema metadata. Successful first setup separately
invokes the ordinary bounded window entry to prepare its initial baseline.
"""

from __future__ import annotations

import json
import contextlib
import io
import hmac
import math
import os
import re
import sqlite3
import stat
import uuid
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .live_catalog import LiveCatalogError, discover_numeric_shards
from .live_readonly import LiveReadonlyError, open_encrypted_readonly, read_private_passphrase


MAX_DATABASE_DEPTH = 6
MAX_DATABASE_FILES = 256
MAX_DATABASE_SCAN_BYTES = 64 * 1024
MAX_IMAGE_KEY_DIRECTORY_ENTRIES = 256
_VOICE_COLUMNS = {"chat_name_id", "create_time", "local_id", "svr_id", "voice_data"}
_NAME_COLUMNS = {"user_name"}
_REQUIRED_CONFIG_FIELDS = {
    "runtime_python", "window_cli", "reply_cli", "live_db_root", "message_root",
    "passphrase_file", "event_store", "agent_store", "account_id", "source_generation",
}
_OPTIONAL_CONFIG_FIELDS = {
    "session_database", "encrypted_contact_database", "image_database",
    "image_attachments_root", "image_output_root", "image_key_config",
    "image_credentials_status", "voice_databases",
    "voice_discovery_complete", "voice_discovery_status", "setup_warnings",
    "self_identity",
}


class SourceSetupError(ValueError):
    """A selected account source cannot be configured safely."""


def _contained(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _owned_file_identity(path: Path, identity: tuple[int, int] | None) -> None:
    if identity is None:
        return
    try:
        current = path.lstat()
        if stat.S_ISREG(current.st_mode) and (current.st_dev, current.st_ino) == identity:
            path.unlink()
    except OSError:
        pass


def _safe_relative(root: Path, relative: Path) -> Path | None:
    current = root
    for component in relative.parts:
        if component in {"", ".", ".."}:
            return None
        current = current / component
        try:
            if stat.S_ISLNK(current.lstat().st_mode):
                return None
        except OSError:
            return None
    resolved = current.resolve(strict=False)
    return resolved if _contained(resolved, root) else None


def _private_regular(path: Path, *, max_bytes: int | None = None) -> bool:
    try:
        mode = path.lstat()
    except OSError:
        return False
    return (
        stat.S_ISREG(mode.st_mode)
        and not stat.S_ISLNK(mode.st_mode)
        and (stat.S_IMODE(mode.st_mode) & 0o077) == 0
        and (max_bytes is None or mode.st_size <= max_bytes)
    )


def _db_storage_for(account_root: Path) -> tuple[Path | None, dict[str, Any] | None]:
    supplied = Path(account_root).expanduser()
    try:
        status = supplied.lstat()
    except OSError:
        return None, {"status": "account_root_unavailable", "missing": [str(supplied)]}
    if stat.S_ISLNK(status.st_mode) or not stat.S_ISDIR(status.st_mode):
        return None, {"status": "account_root_invalid", "reason": "not_a_real_directory"}
    root = supplied.resolve(strict=True)
    db_storage = root / "db_storage"
    try:
        db_status = db_storage.lstat()
    except OSError:
        # Detect a container holding multiple accounts without entering any
        # account's database subtree or choosing one on the user's behalf.
        candidates: list[str] = []
        try:
            for child in root.iterdir():
                if child.is_symlink() or not child.is_dir():
                    continue
                nested = child / "db_storage"
                if nested.is_dir() and not nested.is_symlink():
                    candidates.append(child.name)
        except OSError:
            pass
        if len(candidates) > 1:
            return None, {"status": "account_root_ambiguous", "candidate_count": len(candidates)}
        return None, {"status": "account_root_invalid", "reason": "db_storage_missing"}
    if stat.S_ISLNK(db_status.st_mode) or not stat.S_ISDIR(db_status.st_mode):
        return None, {"status": "account_root_invalid", "reason": "db_storage_not_a_real_directory"}
    return db_storage.resolve(strict=True), None


def _scan_database_files(db_storage: Path) -> tuple[list[Path], bool, list[str]]:
    found: list[Path] = []
    complete = True
    warnings: list[str] = []

    def on_walk_error(error: OSError) -> None:
        nonlocal complete
        complete = False
        warnings.append(
            "database_scan_permission_denied"
            if isinstance(error, PermissionError)
            else "database_scan_unavailable"
        )

    for current, dirs, files in os.walk(
        db_storage, topdown=True, onerror=on_walk_error, followlinks=False
    ):
        directory = Path(current)
        try:
            depth = len(directory.relative_to(db_storage).parts)
        except ValueError:
            return [], False, ["database_scan_escaped_root"]
        kept_dirs = []
        for name in dirs:
            child = directory / name
            if child.is_symlink():
                complete = False
                warnings.append("database_scan_symlink_skipped")
                continue
            if depth >= MAX_DATABASE_DEPTH:
                complete = False
                warnings.append("database_scan_depth_limit")
                continue
            kept_dirs.append(name)
        dirs[:] = kept_dirs
        for name in files:
            if not name.lower().endswith(".db"):
                continue
            candidate = directory / name
            if candidate.is_symlink():
                complete = False
                warnings.append("database_scan_symlink_skipped")
                continue
            try:
                resolved = candidate.resolve(strict=True)
                if not _contained(resolved, db_storage) or not resolved.is_file():
                    complete = False
                    warnings.append("database_scan_path_rejected")
                    continue
            except OSError:
                complete = False
                warnings.append("database_scan_path_unavailable")
                continue
            found.append(resolved)
            if len(found) > MAX_DATABASE_FILES:
                return [], False, ["database_scan_count_limit"]
    return sorted(set(found)), complete, sorted(set(warnings))


def _table_columns(connection: Any, table: str) -> set[str]:
    names = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchall()
    }
    if not names:
        return set()
    quoted = '"' + table.replace('"', '""') + '"'
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({quoted})").fetchall()}


def _metadata_connection(path: Path, passphrase: bytes):
    opened = open_encrypted_readonly(path, passphrase)
    connection = opened[0] if isinstance(opened, tuple) else opened
    try:
        connection.execute("PRAGMA query_only=ON")
        state = connection.execute("PRAGMA query_only").fetchone()
        if not state or int(state[0]) != 1:
            raise SourceSetupError("source_not_query_only")
        return connection
    except BaseException:
        connection.close()
        raise


def _database_stamp(path: Path, root: Path) -> tuple[Any, ...] | None:
    """Snapshot main and WAL identity; unsafe or unreadable paths cannot be reused."""
    try:
        relative = path.relative_to(root)
        if _safe_relative(root, relative) != path:
            return None
        values = []
        for item in (path, path.with_name(path.name + '-wal')):
            try:
                info = item.lstat()
            except FileNotFoundError:
                if item == path:
                    return None
                values.append(None)
                continue
            if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
                return None
            values.append((info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns))
        return tuple(values)
    except (OSError, ValueError):
        return None


def _owned_capture_parent(path: Path, identity: tuple[int, int]) -> bool:
    from .credential_capture import _private_directory
    try:
        info = path.parent.lstat()
        return _private_directory(path.parent) and (info.st_dev, info.st_ino) == identity
    except (OSError, TypeError):
        return False


def _read_owned_capture(path: Path, identity: tuple[int, int], expected: bytes,
                        parent_identity: tuple[int, int]) -> bytes:
    """Compare this invocation's saved credential without following replacements."""
    try:
        if not _owned_capture_parent(path, parent_identity):
            raise SourceSetupError('credential_source_changed')
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
        with os.fdopen(fd, 'rb') as stream:
            info = os.fstat(stream.fileno())
            if not (stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o600
                    and info.st_uid == os.getuid() and info.st_nlink == 1
                    and (info.st_dev, info.st_ino) == identity and info.st_size == 32):
                raise SourceSetupError('credential_source_changed')
            value = stream.read(33)
            current = path.lstat()
            final = os.fstat(stream.fileno())
            if (not _owned_capture_parent(path, parent_identity) or stat.S_ISLNK(current.st_mode) or (current.st_dev, current.st_ino) != identity
                    or (current.st_size, current.st_mtime_ns, current.st_ctime_ns) != (info.st_size, info.st_mtime_ns, info.st_ctime_ns)
                    or (final.st_size, final.st_mtime_ns, final.st_ctime_ns) != (info.st_size, info.st_mtime_ns, info.st_ctime_ns)
                    or len(value) != 32 or not hmac.compare_digest(value, expected)):
                raise SourceSetupError('credential_source_changed')
            return value
    except (OSError, TypeError, ValueError):
        raise SourceSetupError('credential_source_changed') from None


def _voice_schema_matches(connection: Any) -> bool:
    voice_columns = _table_columns(connection, "VoiceInfo")
    name_columns = _table_columns(connection, "Name2Id")
    return _VOICE_COLUMNS.issubset(voice_columns) and _NAME_COLUMNS.issubset(name_columns)


def inspect_account_root(account_root: Path) -> dict[str, Any]:
    db_storage, error = _db_storage_for(Path(account_root))
    if error is not None:
        return error
    assert db_storage is not None
    root = db_storage.parent
    message_root = _safe_relative(db_storage, Path("message"))
    if message_root is None or not message_root.is_dir():
        return {"status": "message_shards_missing", "account_root": str(root), "live_db_root": str(db_storage)}
    try:
        message_root.lstat()
        shards = discover_numeric_shards(message_root)
    except (OSError, LiveCatalogError):
        shards = ()
    contact = _safe_relative(db_storage, Path("contact/contact.db"))
    session = _safe_relative(db_storage, Path("session/session.db"))
    image_database = _safe_relative(db_storage, Path("message/message_resource.db"))
    attachments = _safe_relative(root, Path("msg/attach"))
    image_ready = (
        image_database is not None and image_database.is_file()
        and attachments is not None and attachments.is_dir()
    )
    image_keys, image_scope = _discover_image_keys(root)
    image_credentials_ready = image_ready and image_keys is not None and image_scope is not None
    return {
        "status": "resources_found" if shards else "message_shards_missing",
        "account_root": str(root),
        "live_db_root": str(db_storage),
        "message_root": str(message_root),
        "message_shard_count": len(shards),
        "session_database": str(session) if session is not None and session.is_file() else None,
        "encrypted_contact_database": str(contact) if contact is not None and contact.is_file() else None,
        "image_database": str(image_database) if image_ready else None,
        "image_attachments_root": str(attachments) if image_ready else None,
        "image_credentials_status": "ready" if image_credentials_ready else "not_configured",
    }


def _discover_image_keys(account_root: Path) -> tuple[list[int] | None, str | None]:
    """Use the verified sibling app_data filename convention, without file reads."""

    root = Path(account_root)
    try:
        root_status = root.lstat()
        xwechat_root = root.parent
        xwechat_status = xwechat_root.lstat()
        documents = xwechat_root.parent
        documents_status = documents.lstat()
    except OSError:
        return None, None
    if (
        stat.S_ISLNK(root_status.st_mode)
        or stat.S_ISLNK(xwechat_status.st_mode)
        or stat.S_ISLNK(documents_status.st_mode)
        or not stat.S_ISDIR(xwechat_status.st_mode)
        or xwechat_root.name != "xwechat_files"
        or not stat.S_ISDIR(documents_status.st_mode)
    ):
        return None, None
    account_name = root.name
    if "_" not in account_name:
        return None, None
    scope = account_name.rsplit("_", 1)[0]
    if not scope:
        return None, None
    key_directories: list[Path] = []
    for relative in (Path("app_data/net/kvcomm"), Path("app_data/ilink/kvcomm")):
        current = documents
        missing = False
        for component in relative.parts:
            current = current / component
            try:
                status = current.lstat()
            except FileNotFoundError:
                missing = True
                break
            except OSError:
                return None, None
            if stat.S_ISLNK(status.st_mode) or not stat.S_ISDIR(status.st_mode):
                return None, None
            try:
                resolved = current.resolve(strict=True)
            except OSError:
                return None, None
            if not _contained(resolved, documents.resolve()):
                return None, None
        if not missing:
            key_directories.append(current)
    if not key_directories:
        return None, None

    codes: set[int] = set()
    for directory in key_directories:
        try:
            with os.scandir(directory) as entries:
                for index, entry in enumerate(entries):
                    if index >= MAX_IMAGE_KEY_DIRECTORY_ENTRIES:
                        return None, None
                    match = re.fullmatch(r"key_(\d+)_.*\.statistic", entry.name)
                    try:
                        if entry.is_symlink():
                            continue
                        if not entry.is_file():
                            continue
                    except OSError:
                        return None, None
                    if match:
                        try:
                            codes.add(int(match.group(1)))
                        except ValueError:
                            return None, None
        except OSError:
            return None, None
    if not 1 <= len(codes) <= 2:
        return None, None
    return sorted(codes), scope


def _write_private_config(path: Path, payload: Mapping[str, Any], account_root: Path) -> tuple[int, int]:
    target = path.expanduser().absolute()
    resolved_target = target.resolve(strict=False)
    if _contained(resolved_target, account_root):
        raise SourceSetupError("config_must_be_outside_account_root")
    if target.exists() or target.is_symlink():
        raise SourceSetupError("config_exists")
    missing: list[Path] = []
    parent = target.parent
    while not parent.exists():
        if parent.is_symlink():
            raise SourceSetupError("config_parent_symlink")
        missing.append(parent)
        parent = parent.parent
    if parent.is_symlink() or not parent.is_dir():
        raise SourceSetupError("config_parent_invalid")
    if parent == account_root or _contained(parent.resolve(), account_root):
        raise SourceSetupError("config_must_be_outside_account_root")
    for directory in reversed(missing):
        directory.mkdir(mode=0o700)
    final_parent = target.parent
    parent_status = final_parent.lstat()
    if stat.S_ISLNK(parent_status.st_mode) or not stat.S_ISDIR(parent_status.st_mode):
        raise SourceSetupError("config_parent_invalid")
    if stat.S_IMODE(parent_status.st_mode) & 0o077:
        raise SourceSetupError("config_parent_not_private")
    data = (json.dumps(dict(payload), ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    identity: tuple[int, int] | None = None
    try:
        with os.fdopen(fd, "wb") as handle:
            opened = os.fstat(handle.fileno())
            if not stat.S_ISREG(opened.st_mode) or stat.S_IMODE(opened.st_mode) != 0o600:
                raise OSError("private config file could not be verified")
            identity = (opened.st_dev, opened.st_ino)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        final = target.lstat()
        if not stat.S_ISREG(final.st_mode) or (final.st_dev, final.st_ino) != identity:
            raise OSError("private config file ownership changed")
        return identity
    except OSError as exc:
        _owned_file_identity(target, identity)
        raise SourceSetupError("config_write_failed") from exc
    except BaseException:
        _owned_file_identity(target, identity)
        raise


def prepare_account_source(
    account_root: Path,
    *,
    passphrase_file: Path | None,
    config_path: Path,
    data_home: Path,
    runtime_python: Path,
    window_cli: Path,
    reply_cli: Path,
    confirm_self_username: str | None = None,
    _capture_observations: dict | None = None,
) -> dict[str, Any]:
    if _capture_observations and passphrase_file is not None:
        if not _owned_capture_parent(Path(passphrase_file), _capture_observations.get('credential_parent_identity')):
            return {"status": "credential_source_changed"}
    if confirm_self_username is not None:
        try:
            _validate_self_username(confirm_self_username)
            _check_identity_config_parents(config_path)
        except SourceSetupError as exc:
            return {"status": str(exc)}
    inspected = inspect_account_root(account_root)
    if inspected["status"] in {"account_root_ambiguous", "account_root_invalid", "account_root_unavailable"}:
        return inspected
    if inspected["status"] == "message_shards_missing":
        return inspected
    if Path(config_path).expanduser().exists() or Path(config_path).expanduser().is_symlink():
        return {"status": "config_exists", "config_path": str(Path(config_path).expanduser())}
    root = Path(inspected["account_root"])
    db_storage = Path(inspected["live_db_root"])
    message_root = Path(inspected["message_root"])
    resolved_data_home = Path(data_home).expanduser().resolve(strict=False)
    if _contained(resolved_data_home, root):
        return {"status": "data_home_must_be_outside_account_root", "resources": inspected}
    try:
        shards = discover_numeric_shards(message_root)
    except LiveCatalogError:
        return {"status": "message_source_unavailable"}
    if not shards:
        return {"status": "message_shards_missing"}
    if passphrase_file is None:
        return {"status": "credentials_required", "resources": inspected}
    try:
        if _capture_observations:
            passphrase = _read_owned_capture(Path(passphrase_file), _capture_observations['credential_identity'],
                                             _capture_observations['credential'], _capture_observations['credential_parent_identity'])
        else:
            passphrase = read_private_passphrase(Path(passphrase_file))
    except SourceSetupError:
        return {"status": "credential_source_changed"}
    except (OSError, LiveReadonlyError, KeyError):
        return {"status": "credentials_invalid", "resources": inspected}

    # These observations live only within this setup; a changed main file or
    # WAL is reopened before voice metadata is used.
    voice_observations: dict[Path, tuple[tuple[Any, ...], bool | None]] = {}
    verified_message_stamps: dict[Path, tuple[Any, ...]] = {}
    if (_capture_observations and _capture_observations.get('root') == db_storage
            and _capture_observations.get('shards') == shards):
        for path, observation in _capture_observations.get('entries', {}).items():
            if path in shards and _database_stamp(path, db_storage) == observation[0]:
                voice_observations[path] = observation
                verified_message_stamps[path] = observation[0]
    def remember_voice(path: Path, connection: Any, before: tuple[Any, ...] | None) -> None:
        try:
            matches = _voice_schema_matches(connection)
        except Exception:
            matches = None
        after = _database_stamp(path, db_storage)
        if before is not None and before == after:
            voice_observations[path] = (after, matches)

    for shard in shards:
        if shard in verified_message_stamps:
            continue
        try:
            before = _database_stamp(shard, db_storage)
            if before is None:
                return {"status": "message_source_changed"}
            connection = _metadata_connection(shard, passphrase)
            try:
                remember_voice(shard, connection, before)
            finally:
                connection.close()
            if _database_stamp(shard, db_storage) != before:
                return {"status": "message_source_changed"}
            verified_message_stamps[shard] = before
        except Exception:
            return {"status": "message_source_unavailable", "resources": inspected}

    warnings: list[str] = []
    if inspected.get("session_database") is None:
        warnings.append("session_database_missing")
    contact_path = Path(inspected["encrypted_contact_database"]) if inspected.get("encrypted_contact_database") else None
    if contact_path is not None:
        try:
            before = _database_stamp(contact_path, db_storage)
            connection = _metadata_connection(contact_path, passphrase)
            try:
                remember_voice(contact_path, connection, before)
                columns = _table_columns(connection, "contact")
                if not {"username", "alias", "remark", "nick_name"}.issubset(columns):
                    warnings.append("contact_schema_unavailable")
                    contact_path = None
            finally:
                connection.close()
        except Exception:
            warnings.append("contact_source_unavailable")
            contact_path = None
    image_path = Path(inspected["image_database"]) if inspected.get("image_database") else None
    image_attachments = Path(inspected["image_attachments_root"]) if inspected.get("image_attachments_root") else None
    if image_path is not None:
        try:
            before = _database_stamp(image_path, db_storage)
            connection = _metadata_connection(image_path, passphrase)
            try:
                remember_voice(image_path, connection, before)
            finally:
                connection.close()
        except Exception:
            warnings.append("image_source_unavailable")
            image_path = None
            image_attachments = None

    database_files, scan_complete, scan_warnings = _scan_database_files(db_storage)
    try:
        if discover_numeric_shards(message_root) != shards:
            return {"status": "message_source_changed"}
    except LiveCatalogError:
        return {"status": "message_source_changed"}
    warnings.extend(scan_warnings)
    voice_paths: list[str] = []
    for path in database_files:
        try:
            stamp = _database_stamp(path, db_storage)
            if stamp is None:
                raise SourceSetupError("source_changed")
            cached = voice_observations.get(path)
            if path in (contact_path, image_path) and (cached is None or cached[0] != stamp):
                return {"status": "source_changed"}
            if cached is not None and cached[0] == stamp:
                matches = cached[1]
                if matches is None:
                    raise SourceSetupError("voice_schema_unavailable")
            else:
                connection = _metadata_connection(path, passphrase)
                try:
                    matches = _voice_schema_matches(connection)
                    if path == contact_path and not {"username", "alias", "remark", "nick_name"}.issubset(_table_columns(connection, "contact")):
                        warnings.append("contact_schema_unavailable")
                        contact_path = None
                finally:
                    connection.close()
                if _database_stamp(path, db_storage) != stamp:
                    raise SourceSetupError("source_changed")
                if path in verified_message_stamps:
                    verified_message_stamps[path] = stamp
            if matches:
                voice_paths.append(str(path))
        except Exception:
            if path in shards and _database_stamp(path, db_storage) != voice_observations.get(path, (None,))[0]:
                return {"status": "message_source_changed"}
            scan_complete = False
            warnings.append("voice_scan_incomplete")
    try:
        if discover_numeric_shards(message_root) != shards or any(
                _database_stamp(path, db_storage) != stamp
                for path, stamp in verified_message_stamps.items()):
            return {"status": "message_source_changed"}
    except LiveCatalogError:
        return {"status": "message_source_changed"}
    for path in (contact_path, image_path):
        if path is not None:
            observed = voice_observations.get(path)
            if observed is None or _database_stamp(path, db_storage) != observed[0]:
                return {"status": "source_changed"}
    if not scan_complete:
        warnings.append("voice_scan_incomplete")
    voice_status = "complete" if scan_complete and voice_paths else (
        "absent" if scan_complete else "incomplete"
    )

    stores = resolved_data_home / "wechat-local"
    profile: dict[str, Any] = {
        # Preserve the selected venv entry point. Resolving a venv symlink
        # points at its base interpreter and loses installed package context.
        "runtime_python": str(Path(runtime_python).expanduser().absolute()),
        "window_cli": str(Path(window_cli).resolve(strict=False)),
        "reply_cli": str(Path(reply_cli).resolve(strict=False)),
        "live_db_root": str(db_storage),
        "message_root": str(message_root),
        "passphrase_file": str(Path(passphrase_file).expanduser().resolve(strict=False)),
        "event_store": str(stores / "events.sqlite"),
        "agent_store": str(stores / "agents.sqlite"),
        "account_id": f"local-{uuid.uuid4()}",
        "source_generation": f"generation-{uuid.uuid4()}",
        "voice_databases": voice_paths if scan_complete else [],
        "voice_discovery_complete": bool(scan_complete),
        "voice_discovery_status": voice_status,
        "setup_warnings": sorted(set(warnings)),
        "image_credentials_status": "not_configured",
    }
    if confirm_self_username is not None:
        profile["self_identity"] = {
            "username": confirm_self_username, **_self_identity_binding(profile),
        }
    if inspected.get("session_database"):
        profile["session_database"] = inspected["session_database"]
    if contact_path is not None:
        profile["encrypted_contact_database"] = str(contact_path)
    image_key_path: Path | None = None
    image_key_identity: tuple[int, int] | None = None
    image_keys, image_scope = _discover_image_keys(root)
    if image_path is not None and image_attachments is not None:
        profile["image_database"] = str(image_path)
        profile["image_attachments_root"] = str(image_attachments)
        profile["image_output_root"] = str(stores / "images")
        if image_keys is not None and image_scope is not None:
            image_key_path = Path(config_path).expanduser().with_name("current-image-key.json")
            profile["image_key_config"] = str(image_key_path.absolute())
            profile["image_credentials_status"] = "ready"
    if not (image_path is not None and image_attachments is not None and image_keys is not None and image_scope is not None):
        profile["image_credentials_status"] = "not_configured"
    try:
        if image_key_path is not None:
            image_key_identity = _write_private_config(
                image_key_path,
                {"key_codes": image_keys, "account_scope": image_scope},
                root,
            )
        _write_private_config(Path(config_path), profile, root)
    except SourceSetupError as exc:
        if image_key_path is not None:
            _owned_file_identity(image_key_path, image_key_identity)
        return {"status": str(exc), "resources": inspected}
    return {
        "status": "configured" if scan_complete else "configured_with_voice_incomplete",
        "config_path": str(Path(config_path).expanduser().absolute()),
        "resources": inspected,
        "voice_databases": voice_paths if scan_complete else [],
    }


def require_complete_voice_source(profile: Mapping[str, Any]) -> None:
    if profile.get("voice_discovery_complete") is not True or profile.get("voice_discovery_status") != "complete":
        raise SourceSetupError("Voice database discovery is incomplete or unavailable")
    paths = profile.get("voice_databases")
    if not isinstance(paths, list) or not paths:
        raise SourceSetupError("No VoiceInfo source was verified")


def _preflight_capture_outputs(account_root: Path, config_path: Path, output_data_home: Path) -> str | None:
    inspected = inspect_account_root(account_root)
    if inspected.get("status") not in {"resources_found", "message_shards_missing"}:
        return str(inspected.get("status", "account_root_invalid"))
    root = Path(str(inspected["account_root"])).resolve(strict=True)
    target = config_path.expanduser().absolute()
    resolved_target = target.resolve(strict=False)
    output_root = output_data_home.expanduser().resolve(strict=False)
    if _contained(resolved_target, root):
        return "config_must_be_outside_account_root"
    if _contained(output_root, root):
        return "data_home_must_be_outside_account_root"
    if target.exists() or target.is_symlink():
        return "config_exists"
    passphrase_path = target.with_name("passphrase.bin")
    if passphrase_path.exists() or passphrase_path.is_symlink():
        return "credentials_exist"
    from .credential_capture import _destination_parent_available

    if not _destination_parent_available(target.parent):
        return "credential_destination_unavailable"
    try:
        parent = target.parent
        while not parent.exists():
            if parent.is_symlink() or parent.parent == parent:
                return "credential_destination_unavailable"
            parent = parent.parent
        if parent.is_symlink() or not os.access(parent, os.W_OK):
            return "credential_destination_unavailable"
    except OSError:
        return "credential_destination_unavailable"
    return None


def _validate_self_username(username: Any) -> None:
    # A stable username, never a display name, room token or filesystem path.
    if not isinstance(username, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", username) is None:
        raise SourceSetupError("self_username_invalid")


def _self_identity_binding(profile: Mapping[str, Any]) -> dict[str, str]:
    roots: dict[str, str] = {}
    for field in ("live_db_root", "message_root"):
        path = Path(profile[field]).expanduser()
        if not path.is_absolute():
            raise SourceSetupError("self_identity_binding_mismatch")
        try:
            roots[field] = str(path.resolve(strict=False))
        except (OSError, RuntimeError, ValueError) as exc:
            raise SourceSetupError("self_identity_binding_mismatch") from exc
    if not _contained(Path(roots["message_root"]), Path(roots["live_db_root"])):
        raise SourceSetupError("self_identity_binding_mismatch")
    return {"account_id": profile["account_id"], "source_generation": profile["source_generation"], **roots}


def _confirmed_self_username(profile: Mapping[str, Any]) -> str | None:
    if "self_identity" not in profile:
        return None
    identity = profile["self_identity"]
    if not isinstance(identity, dict) or set(identity) != {
        "username", "account_id", "source_generation", "live_db_root", "message_root",
    }:
        raise SourceSetupError("self_identity_invalid")
    _validate_self_username(identity["username"])
    if any(identity[field] != value for field, value in _self_identity_binding(profile).items()):
        raise SourceSetupError("self_identity_binding_mismatch")
    return identity["username"]


def _decode_source_config(data: bytes) -> dict[str, Any]:
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise SourceSetupError("Source config is invalid") from exc
    if not isinstance(value, dict) or not _REQUIRED_CONFIG_FIELDS.issubset(value):
        raise SourceSetupError("Source config is missing required fields")
    if set(value) - _REQUIRED_CONFIG_FIELDS - _OPTIONAL_CONFIG_FIELDS:
        raise SourceSetupError("Source config has unsupported fields")
    if any(not isinstance(value[key], str) or not value[key] for key in _REQUIRED_CONFIG_FIELDS):
        raise SourceSetupError("Source config fields are invalid")
    if "voice_databases" in value and (
        not isinstance(value["voice_databases"], list)
        or any(not isinstance(item, str) or not item for item in value["voice_databases"])
    ):
        raise SourceSetupError("Source config voice paths are invalid")
    if "voice_discovery_complete" in value and type(value["voice_discovery_complete"]) is not bool:
        raise SourceSetupError("Source config voice status is invalid")
    if "setup_warnings" in value and (
        not isinstance(value["setup_warnings"], list)
        or any(not isinstance(item, str) for item in value["setup_warnings"])
    ):
        raise SourceSetupError("Source config warnings are invalid")
    return value


def _check_identity_config_parents(path: Path) -> None:
    # Use a canonical, symlink-free reference for confirmed private settings.
    for parent in Path(path).expanduser().absolute().parents:
        try:
            mode = parent.lstat()
        except FileNotFoundError:
            continue  # First setup may create a new private parent.
        if stat.S_ISLNK(mode.st_mode) or not stat.S_ISDIR(mode.st_mode):
            raise SourceSetupError("config_parent_symlink_or_invalid")


def _read_source_config_fd(fd: int) -> tuple[dict[str, Any], bytes, os.stat_result]:
    mode = os.fstat(fd)
    if (
        not stat.S_ISREG(mode.st_mode) or stat.S_IMODE(mode.st_mode) & 0o077
        or mode.st_size > MAX_DATABASE_SCAN_BYTES or mode.st_uid != os.getuid()
    ):
        raise SourceSetupError("Source config must be a private regular file")
    with os.fdopen(os.dup(fd), "rb") as handle:
        data = handle.read(MAX_DATABASE_SCAN_BYTES + 1)
    if len(data) > MAX_DATABASE_SCAN_BYTES:
        raise SourceSetupError("Source config is invalid")
    profile = _decode_source_config(data)
    if "self_identity" in profile and stat.S_IMODE(mode.st_mode) != 0o600:
        raise SourceSetupError("Source identity config must have mode 0600")
    return profile, data, mode


def load_source_config(path: Path) -> dict[str, Any]:
    try:
        fd = os.open(Path(path).expanduser(), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            profile, _, _ = _read_source_config_fd(fd)
        finally:
            os.close(fd)
        if _confirmed_self_username(profile) is not None:
            _check_identity_config_parents(path)
        return profile
    except OSError as exc:
        raise SourceSetupError("Source config must be a private regular file") from exc


def confirm_source_identity(path: Path, username: str, *, replace: bool = False) -> dict[str, Any]:
    """Confirm one private profile without opening its database or key references."""
    _validate_self_username(username)
    target = Path(path).expanduser().absolute()
    _check_identity_config_parents(target)
    directory_fd: int | None = None
    source_fd: int | None = None
    temporary_name: str | None = None
    temporary_identity: tuple[int, int] | None = None
    try:
        directory_fd = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        parent = os.fstat(directory_fd)
        if stat.S_IMODE(parent.st_mode) & 0o077 or parent.st_uid != os.getuid():
            raise SourceSetupError("config_parent_not_private")
        source_fd = os.open(target.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
        profile, original, source = _read_source_config_fd(source_fd)
        if stat.S_IMODE(source.st_mode) != 0o600 or source.st_nlink != 1:
            raise SourceSetupError("Source identity config must have mode 0600 and one link")
        previous = None if replace else _confirmed_self_username(profile)
        if previous is not None and previous != username:
            raise SourceSetupError("self_identity_replace_required")
        profile["self_identity"] = {"username": username, **_self_identity_binding(profile)}
        data = (json.dumps(profile, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        if len(data) > MAX_DATABASE_SCAN_BYTES:
            raise SourceSetupError("self_identity_save_failed")
        temporary_name = ".self-identity-" + uuid.uuid4().hex + ".tmp"
        fd = os.open(temporary_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory_fd)
        with os.fdopen(fd, "wb") as handle:
            opened = os.fstat(handle.fileno())
            temporary_identity = (opened.st_dev, opened.st_ino)
            if not stat.S_ISREG(opened.st_mode) or stat.S_IMODE(opened.st_mode) != 0o600:
                raise SourceSetupError("self_identity_save_failed")
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        current = os.stat(target.name, dir_fd=directory_fd, follow_symlinks=False)
        # A replaced or edited source must never be silently overwritten.
        if (
            (current.st_dev, current.st_ino, current.st_mtime_ns, current.st_size, current.st_mode)
            != (source.st_dev, source.st_ino, source.st_mtime_ns, source.st_size, source.st_mode)
            or target.parent.stat().st_ino != parent.st_ino
        ):
            raise SourceSetupError("self_identity_config_changed")
        os.lseek(source_fd, 0, os.SEEK_SET)
        _, reread, _ = _read_source_config_fd(source_fd)
        if reread != original:
            raise SourceSetupError("self_identity_config_changed")
        staged = os.stat(temporary_name, dir_fd=directory_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(staged.st_mode) or stat.S_IMODE(staged.st_mode) != 0o600
            or (staged.st_dev, staged.st_ino) != temporary_identity
        ):
            raise SourceSetupError("self_identity_config_changed")
        os.replace(temporary_name, target.name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
        temporary_name = None
        return {"status": "self_identity_confirmed"}
    except OSError as exc:
        raise SourceSetupError("self_identity_save_failed") from exc
    finally:
        if temporary_name is not None and temporary_identity is not None and directory_fd is not None:
            try:
                remaining = os.stat(temporary_name, dir_fd=directory_fd, follow_symlinks=False)
                if stat.S_ISREG(remaining.st_mode) and (remaining.st_dev, remaining.st_ino) == temporary_identity:
                    os.unlink(temporary_name, dir_fd=directory_fd)
            except OSError:
                pass
        if source_fd is not None:
            os.close(source_fd)
        if directory_fd is not None:
            os.close(directory_fd)


def apply_source_config_to_args(args: Any) -> dict[str, Any] | None:
    """Load one profile and fill only source arguments the caller omitted."""

    explicit_self = getattr(args, "self_username", None)
    if explicit_self is not None:
        _validate_self_username(explicit_self)
    args.voice_source_unavailable = False
    args.configured_image_unavailable = False
    if args.source_config is None:
        if args.configured_images:
            raise SourceSetupError("--configured-images requires --source-config")
        return None
    explicit_source_fields = (
        "live_db_root", "message_root", "passphrase", "event_store", "agent_store",
        "account_id", "source_generation", "contact_database", "encrypted_contact_database",
        "voice_database", "image_database", "image_attachments_root", "image_output_root",
        "image_key_config",
    )
    if any(getattr(args, field, None) for field in explicit_source_fields):
        raise SourceSetupError("Do not combine --source-config with explicit source paths")
    profile = load_source_config(args.source_config)
    confirmed_self = _confirmed_self_username(profile)
    if confirmed_self is not None:
        if explicit_self is not None and explicit_self != confirmed_self:
            raise SourceSetupError("self_identity_conflict")
        args.self_username = confirmed_self
    args.live_db_root = Path(profile["live_db_root"])
    args.message_root = Path(profile["message_root"])
    args.passphrase = Path(profile["passphrase_file"])
    args.event_store = Path(profile["event_store"])
    args.agent_store = Path(profile["agent_store"])
    args.account_id = profile["account_id"]
    args.source_generation = profile["source_generation"]
    contact = profile.get("encrypted_contact_database")
    args.encrypted_contact_database = Path(contact) if contact else None
    if args.voice_transcripts:
        try:
            require_complete_voice_source(profile)
            args.voice_database = [Path(value) for value in profile["voice_databases"]]
        except SourceSetupError:
            # Continue the text query, but mark voice enrichment unavailable;
            # the existing enricher will preserve the base message and skip ASR.
            args.voice_database = []
            args.voice_source_unavailable = True
    if args.configured_images:
        image_values = (
            profile.get("image_database"),
            profile.get("image_attachments_root"),
            profile.get("image_output_root"),
        )
        if all(isinstance(value, str) and value for value in image_values):
            args.image_database, args.image_attachments_root, args.image_output_root = map(Path, image_values)
        else:
            args.configured_image_unavailable = True
        image_key_config = profile.get("image_key_config")
        if isinstance(image_key_config, str) and image_key_config:
            args.image_key_config = Path(image_key_config)
    args.source_profile = profile
    return profile


def _initial_message_baseline(config_path: Path, request: str, timezone_name: str = 'UTC') -> dict[str, Any]:
    """Run the existing bounded window without exposing its message payload."""
    from .codex_first_runtime import main as window_main

    output = io.StringIO()
    try:
        with contextlib.redirect_stdout(output):
            code = window_main(['--source-config', str(config_path), '--request', request,
                                '--timezone', timezone_name, '--display-limit', '1', '--json'])
        payload = json.loads(output.getvalue())
    except Exception:
        return {'status': 'incomplete', 'stage': 'window', 'reason': 'unknown', 'reuse_scope': 'empty_sources_only'}
    if not isinstance(payload, dict) or not isinstance(payload.get('gaps', []), list):
        return {'status': 'incomplete', 'stage': 'window', 'reason': 'unknown', 'reuse_scope': 'empty_sources_only'}
    allowed_gaps = {'known_source_missing', 'live_source_unavailable', 'source_read_unavailable',
                    'source_changed', 'source_excluded_by_classification_policy',
                    'timestamp_integrity_unknown_outside_requested_window',
                    'direction_or_contact_mapping_unavailable', 'direction_identity_unavailable',
                    'event_projection_unavailable', 'confirmation_required', 'baseline_state_unavailable'}
    gaps = [value if value in allowed_gaps else 'unknown' for value in payload.get('gaps', []) if isinstance(value, str)]
    incomplete = {'known_source_missing', 'live_source_unavailable', 'source_read_unavailable',
                  'source_changed', 'event_projection_unavailable', 'confirmation_required', 'baseline_state_unavailable', 'unknown'}
    if code == 0 and payload.get('status') == 'ok' and not incomplete.intersection(gaps):
        return {'status': 'ready', 'coverage_status': payload.get('coverage_status', 'unknown') if payload.get('coverage_status') in {'complete', 'partial', 'unknown'} else 'unknown', 'gaps': gaps, 'reuse_scope': 'empty_sources_only'}
    reasons = {'source_changed', 'source_unavailable', 'configuration_unavailable',
               'runtime_unavailable', 'confirmation_required', 'source_read_unavailable'}
    reason = payload.get('error_code')
    stage = 'window'
    diagnostics = payload.get('diagnostics')
    failures = diagnostics.get('failures') if isinstance(diagnostics, dict) else None
    if isinstance(failures, list) and failures and isinstance(failures[0], dict):
        actual_stage = failures[0].get('stage')
        if actual_stage in {'configuration', 'source_open', 'source_read', 'result_processing'}:
            stage = actual_stage
    coverage = payload.get('coverage_status')
    return {'status': 'incomplete', 'stage': stage, 'reason': reason if reason in reasons else 'unknown', 'coverage_status': coverage if coverage in {'complete', 'partial', 'unknown'} else 'unknown', 'gaps': gaps, 'reuse_scope': 'empty_sources_only'}


def main(argv: list[str] | None = None) -> int:
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Configure one explicit WeChat account root")
    parser.add_argument("--account-root", type=Path)
    parser.add_argument("--confirm-self-username", help="stable username explicitly confirmed by the user for this source")
    parser.add_argument("--replace-self-identity", action="store_true", help="explicitly correct/rebind an existing saved identity")
    parser.add_argument("--passphrase-file", type=Path)
    parser.add_argument("--acquire-passphrase", action="store_true")
    parser.add_argument("--wechat-executable", type=Path)
    parser.add_argument("--wechat-data-home", type=Path)
    parser.add_argument("--lldb-executable", type=Path)
    parser.add_argument("--capture-timeout", type=float, help="Capture seconds (default: 0, waits until completion or cancellation)")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--data-home", type=Path)
    parser.add_argument("--baseline-request", help="initial bounded window (default: 最近3小时)")
    parser.add_argument("--baseline-timezone", help="IANA time zone required with --baseline-request")
    args = parser.parse_args(argv)
    target_options = (args.wechat_executable, args.wechat_data_home, args.lldb_executable)
    config_home = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    output_data_home = args.data_home or Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    config_path = args.config or config_home / "wechat-local" / "current-source.json"
    if args.baseline_request is not None and args.baseline_timezone is None:
        print(json.dumps({'status': 'baseline_timezone_required'}, sort_keys=True))
        return 2
    if args.baseline_timezone is not None:
        try:
            ZoneInfo(args.baseline_timezone)
        except (ZoneInfoNotFoundError, ValueError):
            print(json.dumps({'status': 'baseline_timezone_invalid'}, sort_keys=True))
            return 2
    if args.confirm_self_username is not None:
        try:
            _validate_self_username(args.confirm_self_username)
        except SourceSetupError as exc:
            print(json.dumps({"status": str(exc)}, sort_keys=True))
            return 2
    if args.account_root is None:
        if args.confirm_self_username is None or (
            args.acquire_passphrase or args.passphrase_file is not None or args.data_home is not None
            or any(item is not None for item in target_options) or args.capture_timeout is not None or args.baseline_request is not None or args.baseline_timezone is not None
        ):
            print(json.dumps({"status": "identity_update_arguments_invalid"}, sort_keys=True))
            return 2
        try:
            result = confirm_source_identity(config_path, args.confirm_self_username, replace=args.replace_self_identity)
        except SourceSetupError as exc:
            result = {"status": str(exc)}
        print(json.dumps(result, sort_keys=True))
        return 0 if result["status"] == "self_identity_confirmed" else 2
    if args.replace_self_identity:
        print(json.dumps({"status": "identity_update_arguments_invalid"}, sort_keys=True))
        return 2
    if args.acquire_passphrase:
        supplied_target_options = sum(item is not None for item in target_options)
        if args.passphrase_file is not None or supplied_target_options not in {0, 3}:
            print(json.dumps({"status": "acquisition_arguments_invalid"}, sort_keys=True))
            return 2
        if args.capture_timeout is not None and (
            not math.isfinite(args.capture_timeout) or args.capture_timeout < 0 or args.capture_timeout > 300
        ):
            print(json.dumps({"status": "capture_timeout_invalid"}, sort_keys=True))
            return 2
        preflight_error = _preflight_capture_outputs(args.account_root, config_path, output_data_home)
        if preflight_error is not None:
            print(json.dumps({"status": preflight_error}, ensure_ascii=False, sort_keys=True))
            return 2
        if supplied_target_options == 0:
            from .credential_capture import resolve_default_capture_inputs

            resolved = resolve_default_capture_inputs(args.account_root)
            if resolved.get("status") != "resolved":
                print(json.dumps(resolved, ensure_ascii=False, sort_keys=True))
                return 2
            target_options = (
                Path(str(resolved["wechat_executable"])),
                Path(str(resolved["wechat_data_home"])),
                Path(str(resolved["lldb_executable"])),
            )
    elif any(item is not None for item in target_options) or args.capture_timeout is not None:
        print(json.dumps({"status": "acquisition_arguments_invalid"}, sort_keys=True))
        return 2
    executable = Path(sys.argv[0]).resolve(strict=False)
    venv_bin = executable.parent
    window_cli = venv_bin / "wechat-local-window"
    reply_cli = venv_bin / "wechat-local-reply"
    missing_entrypoints = [
        path.name
        for path in (window_cli, reply_cli)
        if not path.is_file() or not os.access(path, os.X_OK)
    ]
    if missing_entrypoints:
        print(json.dumps({"status": "entrypoint_missing", "missing": missing_entrypoints}, sort_keys=True))
        return 2
    passphrase_file = args.passphrase_file
    captured_identity: tuple[int, int] | None = None
    capture_observations: dict = {}
    if args.acquire_passphrase:
        from .credential_capture import capture_and_save_passphrase

        print("等待首次捕获；可按 Ctrl-C 取消。调试器内部附加仍可能提前失败。", file=sys.stderr, flush=True)
        captured = capture_and_save_passphrase(
            account_root=args.account_root,
            wechat_executable=target_options[0],
            wechat_data_home=target_options[1],
            lldb_executable=target_options[2],
            config_path=config_path,
            timeout_seconds=args.capture_timeout if args.capture_timeout is not None else 0.0,
            prepare_copy=supplied_target_options == 0,
            _validation_observations=capture_observations,
        )
        if captured.get("status") != "captured_and_verified":
            captured.pop("_passphrase_identity", None)
            print(json.dumps(captured, ensure_ascii=False, sort_keys=True))
            return 2
        passphrase_file = Path(str(captured["passphrase_file"]))
        captured_identity = captured.pop("_passphrase_identity", None)
        if not isinstance(captured_identity, tuple) or len(captured_identity) != 2:
            from .credential_capture import _unlink_owned

            _unlink_owned(passphrase_file, None)
            capture_observations.clear()
            print(json.dumps({"status": "credential_save_failed"}, sort_keys=True))
            return 2
    captured_parent_identity = capture_observations.get('credential_parent_identity')
    try:
        result = prepare_account_source(
            args.account_root,
            passphrase_file=passphrase_file,
            config_path=config_path,
            data_home=output_data_home,
            runtime_python=Path(sys.executable),
            window_cli=window_cli,
            reply_cli=reply_cli,
            confirm_self_username=args.confirm_self_username,
            _capture_observations=capture_observations if args.acquire_passphrase else None,
        )
    except Exception as exc:
        result = {"status": "setup_failed", "error_type": type(exc).__name__}
    finally:
        capture_observations.clear()
    if args.acquire_passphrase:
        result["diagnostics"] = dict(captured["diagnostics"])
        result["diagnostics"]["configuration"] = "completed" if result["status"] in {"configured", "configured_with_voice_incomplete"} else "failed"
        if result["status"] in {"configured", "configured_with_voice_incomplete"}:
            result["credential_capture"] = "captured_and_verified"
        elif result.get('status') != 'credential_source_changed':
            from .credential_capture import _unlink_owned

            if _owned_capture_parent(Path(str(passphrase_file)), captured_parent_identity):
                _unlink_owned(Path(str(passphrase_file)), captured_identity)
    baseline_cancelled = False
    if result['status'] in {'configured', 'configured_with_voice_incomplete'}:
        try:
            result['message_baseline'] = _initial_message_baseline(config_path, args.baseline_request or '最近3小时', args.baseline_timezone or 'UTC')
        except KeyboardInterrupt:
            baseline_cancelled = True
            result['message_baseline'] = {'status': 'incomplete', 'stage': 'window', 'reason': 'cancelled', 'reuse_scope': 'empty_sources_only'}
        result['self_identity'] = {'status': 'confirmed' if args.confirm_self_username is not None else 'incomplete'}
        result['mute_status'] = {'status': 'incomplete', 'reason': 'mute_coverage_incomplete'}
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 130 if baseline_cancelled else (0 if result["status"] in {"configured", "configured_with_voice_incomplete"} else 2)


if __name__ == "__main__":
    raise SystemExit(main())
