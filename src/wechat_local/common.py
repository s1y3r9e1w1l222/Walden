"""Shared filesystem and SQLite helpers."""

from __future__ import annotations

import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PRIVATE_DIR = PROJECT_ROOT / "private"


def numeric_message_databases(decrypted: Path) -> list[Path]:
    paths = [
        path
        for path in (decrypted / "message").glob("message_*.db")
        if re.fullmatch(r"message_\d+\.db", path.name)
    ]
    return sorted(paths, key=lambda path: int(path.stem.split("_")[1]))


def connect_readonly(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise FileNotFoundError(path)
    return sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)


def ensure_private_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        os.chmod(path.parent, 0o700)
    except PermissionError:
        pass


def write_private_json(path: Path, payload: Any) -> None:
    ensure_private_parent(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    path.chmod(0o600)


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def require_columns(conn: sqlite3.Connection, table: str, columns: Iterable[str]) -> None:
    actual = {
        row[1]
        for row in conn.execute(f"PRAGMA table_info({quote_identifier(table)})")
    }
    missing = set(columns) - actual
    if missing:
        raise RuntimeError(f"{table} is missing required columns: {sorted(missing)}")

