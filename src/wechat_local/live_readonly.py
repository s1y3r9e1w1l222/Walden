"""Read-only access and diagnostics for a running encrypted WeChat database.

This module deliberately does not expose arbitrary SQL execution through its
CLI.  It verifies the per-database key, opens the source with SQLite URI
``mode=ro``, enables ``query_only``, and reports only non-message diagnostics.
"""

from __future__ import annotations

import argparse
import json
import stat
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

from .wcdb_crypto import (
    KEY_SIZE,
    PAGE_SIZE,
    SALT_SIZE,
    derive_database_key,
    verify_encryption_key,
)


class LiveReadonlyError(RuntimeError):
    """Raised when a live database cannot be safely opened read-only."""


@dataclass(frozen=True)
class LiveDatabaseProbe:
    """Non-sensitive diagnostics proving that read-only access is active."""

    database_name: str
    raw_key_form_bytes: int
    query_only: bool
    journal_mode: str
    table_count: int
    wal_present: bool
    shm_present: bool


def _sqlcipher_module() -> Any:
    try:
        from sqlcipher3 import dbapi2 as sqlcipher
    except ImportError as exc:
        raise LiveReadonlyError(
            "sqlcipher3 is required; install requirements-macos.txt"
        ) from exc
    return sqlcipher


def read_private_passphrase(path: Path) -> bytes:
    """Read a 32-byte passphrase and reject group/other-readable files."""

    resolved = path.resolve()
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    mode = stat.S_IMODE(resolved.stat().st_mode)
    if mode & 0o077:
        raise LiveReadonlyError(
            "Passphrase file must not be accessible by group or other users"
        )
    material = resolved.read_bytes()
    if len(material) != KEY_SIZE:
        raise LiveReadonlyError("Passphrase material must be exactly 32 bytes")
    return material


def readonly_database_uri(path: Path) -> str:
    """Build a URI that asks SQLite to open an existing file read-only."""

    resolved = path.resolve()
    return f"file:{quote(str(resolved), safe='/')}?mode=ro"


def _is_within(path: Path, directory: Path) -> bool:
    """Return whether a resolved path is inside (or equal to) a directory."""

    try:
        path.relative_to(directory)
    except ValueError:
        return False
    return True


def validate_live_storage_paths(
    live_db_root: Path,
    source_databases: list[Path] | tuple[Path, ...],
    event_store: Path,
) -> tuple[Path, tuple[Path, ...], Path]:
    """Enforce the write boundary around an explicitly scoped live DB tree.

    All returned paths are resolved.  Every source must already be a regular
    file under ``live_db_root`` and the writable event store must be outside
    that entire tree.
    """

    root = live_db_root.resolve()
    if not root.is_dir():
        raise LiveReadonlyError("Live database root must be an existing directory")
    if not source_databases:
        raise LiveReadonlyError("At least one live source database is required")

    resolved_sources = tuple(path.resolve() for path in source_databases)
    if len(set(resolved_sources)) != len(resolved_sources):
        raise LiveReadonlyError("Live source database paths must be unique")
    for source in resolved_sources:
        if not source.is_file():
            raise LiveReadonlyError("Live source database must be an existing file")
        if not _is_within(source, root):
            raise LiveReadonlyError("Every live source database must be under live DB root")

    destination = event_store.resolve()
    if _is_within(destination, root):
        raise LiveReadonlyError("Event store must be outside the entire live DB root")
    return root, resolved_sources, destination


def derive_verified_database_material(
    path: Path, passphrase: bytes
) -> tuple[bytes, bytes]:
    """Derive a per-database key and verify it against encrypted page one."""

    resolved = path.resolve()
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    with resolved.open("rb") as handle:
        page_one = handle.read(PAGE_SIZE)
    if len(page_one) != PAGE_SIZE:
        raise LiveReadonlyError(f"Database is too small: {resolved.name}")
    salt = page_one[:SALT_SIZE]
    key = derive_database_key(passphrase, salt)
    if not verify_encryption_key(key, page_one):
        raise LiveReadonlyError(f"HMAC verification failed: {resolved.name}")
    return key, salt


def _key_pragma(material: bytes) -> str:
    return f'PRAGMA key = "x\'{material.hex()}\'"'


def open_encrypted_readonly(
    path: Path,
    passphrase: bytes,
    *,
    sqlcipher_module: Any | None = None,
    timeout_seconds: float = 2.0,
    busy_timeout_ms: int = 2_000,
    _thread_handoff: bool = False,
):
    """Open one verified encrypted database without requesting write access.

    Returns ``(connection, raw_key_form_bytes)``.  Callers own the connection
    and must keep read transactions short.
    """

    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if busy_timeout_ms < 0:
        raise ValueError("busy_timeout_ms must not be negative")

    resolved = path.resolve()
    key, salt = derive_verified_database_material(resolved, passphrase)
    sqlcipher = sqlcipher_module or _sqlcipher_module()
    failures: list[str] = []

    for material in (key, key + salt):
        options = {'check_same_thread': False} if _thread_handoff else {}
        conn = sqlcipher.connect(
            readonly_database_uri(resolved), uri=True, timeout=timeout_seconds, **options
        )
        try:
            conn.execute(_key_pragma(material))
            conn.execute("PRAGMA cipher_compatibility = 4")
            conn.execute("PRAGMA query_only = ON")
            conn.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
            conn.execute("SELECT count(*) FROM sqlite_master").fetchone()
            query_only_row = conn.execute("PRAGMA query_only").fetchone()
            if not query_only_row or int(query_only_row[0]) != 1:
                raise LiveReadonlyError("SQLite query_only could not be enabled")
            return conn, len(material)
        except Exception as exc:
            failures.append(type(exc).__name__)
            conn.close()

    kinds = ", ".join(failures) if failures else "unknown error"
    raise LiveReadonlyError(
        f"SQLCipher could not open verified database read-only: "
        f"{resolved.name} ({kinds})"
    )


def probe_live_database(
    path: Path,
    passphrase: bytes,
    *,
    sqlcipher_module: Any | None = None,
) -> LiveDatabaseProbe:
    """Open, inspect, and close a live database without reading chat rows."""

    resolved = path.resolve()
    conn, raw_key_form_bytes = open_encrypted_readonly(
        resolved, passphrase, sqlcipher_module=sqlcipher_module
    )
    try:
        query_only_row = conn.execute("PRAGMA query_only").fetchone()
        journal_row = conn.execute("PRAGMA journal_mode").fetchone()
        table_row = conn.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table'"
        ).fetchone()
        return LiveDatabaseProbe(
            database_name=resolved.name,
            raw_key_form_bytes=raw_key_form_bytes,
            query_only=bool(query_only_row and int(query_only_row[0]) == 1),
            journal_mode=str(journal_row[0]) if journal_row else "",
            table_count=int(table_row[0]) if table_row else 0,
            wal_present=Path(f"{resolved}-wal").is_file(),
            shm_present=Path(f"{resolved}-shm").is_file(),
        )
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Safely probe one running encrypted WeChat database read-only"
    )
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--passphrase", type=Path, required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    passphrase = read_private_passphrase(args.passphrase)
    result = probe_live_database(args.database, passphrase)
    payload = asdict(result)
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    print("READONLY_LIVE_OPEN OK")
    print(f"DATABASE {result.database_name}")
    print(f"QUERY_ONLY {int(result.query_only)}")
    print(f"JOURNAL_MODE {result.journal_mode}")
    print(f"TABLE_COUNT {result.table_count}")
    print(f"RAW_KEY_FORM {result.raw_key_form_bytes}B")
    print(f"WAL_PRESENT {int(result.wal_present)}")
    print(f"SHM_PRESENT {int(result.shm_present)}")


if __name__ == "__main__":
    main()
