"""Read SessionTable change hints without treating them as message cursors."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from .common import quote_identifier


SESSION_TABLE = "SessionTable"
SESSION_ID_COLUMN = "last_msg_locald_id"
SESSION_TIME_COLUMN = "last_timestamp"
REQUIRED_SESSION_COLUMNS = {"username", SESSION_ID_COLUMN, SESSION_TIME_COLUMN}


class LiveSessionError(RuntimeError):
    """Raised when SessionTable cannot safely provide Phase 2 hints."""


class SessionHintRegression(LiveSessionError):
    """Raised when a persisted session hint moves backwards."""


@dataclass(frozen=True)
class SessionHint:
    username: str
    last_msg_local_id: int
    last_timestamp: int

    def __post_init__(self) -> None:
        if not self.username or self.username.strip() != self.username:
            raise LiveSessionError("Session username must be a non-empty trimmed string")
        if any(ord(character) < 32 for character in self.username):
            raise LiveSessionError("Session username must not contain control characters")
        if self.last_msg_local_id < 0:
            raise LiveSessionError("Session last message local id must not be negative")

    @property
    def table_name(self) -> str:
        return message_table_name(self.username)


def message_table_name(username: str) -> str:
    if not username:
        raise ValueError("username must not be empty")
    return "Msg_" + hashlib.md5(username.encode("utf-8")).hexdigest()


def _require_query_only(conn: Any) -> None:
    row = conn.execute("PRAGMA query_only").fetchone()
    if not row or int(row[0]) != 1:
        raise LiveSessionError("Session source connection must have query_only enabled")


def _session_columns(conn: Any) -> set[str]:
    rows = conn.execute(
        f"PRAGMA table_info({quote_identifier(SESSION_TABLE)})"
    ).fetchall()
    if not rows:
        raise LiveSessionError("SessionTable does not exist")
    columns = {str(row[1]) for row in rows}
    missing = REQUIRED_SESSION_COLUMNS - columns
    if missing:
        raise LiveSessionError(
            f"SessionTable is missing required columns: {sorted(missing)}"
        )
    return columns


def read_session_hints(conn: Any) -> list[SessionHint]:
    """Read only identifiers and last-message metadata from SessionTable."""

    _require_query_only(conn)
    _session_columns(conn)
    rows = conn.execute(
        f"""
        SELECT {quote_identifier('username')},
               {quote_identifier(SESSION_ID_COLUMN)},
               {quote_identifier(SESSION_TIME_COLUMN)}
        FROM {quote_identifier(SESSION_TABLE)}
        WHERE {quote_identifier('username')} IS NOT NULL
        """
    ).fetchall()

    hints: list[SessionHint] = []
    seen: set[str] = set()
    for username, local_id, timestamp in rows:
        if not isinstance(username, str):
            raise LiveSessionError("Session username must be stored as text")
        value = username
        if value in seen:
            raise LiveSessionError("SessionTable contains duplicate usernames")
        seen.add(value)
        try:
            hint = SessionHint(
                username=value,
                last_msg_local_id=int(local_id or 0),
                last_timestamp=int(timestamp or 0),
            )
        except (TypeError, ValueError) as exc:
            raise LiveSessionError("SessionTable contains invalid hint metadata") from exc
        hints.append(hint)
    return hints
