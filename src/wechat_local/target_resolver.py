"""Resolve a scoped contact query without guessing between duplicate names."""

from __future__ import annotations

import argparse
import hashlib
import json
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from .common import connect_readonly, numeric_message_databases, quote_identifier


CONTACT_FIELDS = ("username", "alias", "remark", "nick_name", "display_name")
EXIT_RESOLVED = 0
EXIT_NOT_RESOLVED = 2
EXIT_AMBIGUOUS = 3


def normalize(value: str | None) -> str:
    return unicodedata.normalize("NFKC", value or "").strip().casefold()


def _display_name(contact: dict[str, Any]) -> str:
    return (
        str(contact.get("remark") or "").strip()
        or str(contact.get("nick_name") or "").strip()
        or str(contact.get("alias") or "").strip()
        or str(contact["username"])
    )


def load_contacts(decrypted: Path) -> list[dict[str, Any]]:
    contact_path = decrypted / "contact" / "contact.db"
    with connect_readonly(contact_path) as conn:
        rows = conn.execute(
            "SELECT username, alias, remark, nick_name FROM contact"
        ).fetchall()
    contacts = []
    for username, alias, remark, nick_name in rows:
        if not username:
            continue
        item = {
            "username": username,
            "alias": alias or "",
            "remark": remark or "",
            "nick_name": nick_name or "",
        }
        item["display_name"] = _display_name(item)
        contacts.append(item)
    return contacts


def load_sessions(decrypted: Path) -> set[str]:
    session_path = decrypted / "session" / "session.db"
    with connect_readonly(session_path) as conn:
        return {
            username
            for (username,) in conn.execute("SELECT username FROM SessionTable")
            if username
        }


def load_inventory(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None:
        return {}
    if not path.is_file():
        raise FileNotFoundError(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        item["username"]: item
        for item in payload.get("chats", [])
        if item.get("username")
    }


def _kind(username: str) -> str:
    if username.endswith("@chatroom"):
        return "群聊"
    if username.startswith("gh_"):
        return "公众号"
    return "私聊/其他"


def _local_time(timestamp: int | None) -> str:
    if not timestamp:
        return ""
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")


def _inspect_history(
    decrypted: Path, usernames: Iterable[str]
) -> dict[str, dict[str, Any]]:
    """Inspect only candidate tables; never enumerate or print all messages."""
    result = {
        username: {"message_count": 0, "first_timestamp": None, "last_timestamp": None}
        for username in usernames
    }
    tables = {
        username: "Msg_" + hashlib.md5(username.encode("utf-8")).hexdigest()
        for username in result
    }
    for db_path in numeric_message_databases(decrypted):
        with connect_readonly(db_path) as conn:
            existing = {
                name
                for (name,) in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'Msg_%'"
                )
            }
            for username, table in tables.items():
                if table not in existing:
                    continue
                count, first_ts, last_ts = conn.execute(
                    "SELECT count(*), min(create_time), max(create_time) FROM "
                    + quote_identifier(table)
                ).fetchone()
                item = result[username]
                item["message_count"] += int(count or 0)
                if first_ts and (
                    item["first_timestamp"] is None or first_ts < item["first_timestamp"]
                ):
                    item["first_timestamp"] = first_ts
                if last_ts and (
                    item["last_timestamp"] is None or last_ts > item["last_timestamp"]
                ):
                    item["last_timestamp"] = last_ts
    return {
        username: {
            "message_count": values["message_count"],
            "first_time": _local_time(values["first_timestamp"]),
            "last_time": _local_time(values["last_timestamp"]),
        }
        for username, values in result.items()
    }


def _matched_fields(contact: dict[str, Any], query: str, exact: bool) -> list[str]:
    fields = []
    for field in CONTACT_FIELDS:
        value = normalize(str(contact.get(field) or ""))
        if not value:
            continue
        if (value == query) if exact else (query in value):
            fields.append(field)
    return fields


def _candidate_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        not item.get("in_session", False),
        -int(item.get("message_count") or 0),
        normalize(item.get("display_name")),
        normalize(item.get("username")),
    )


def _enrich_candidates(
    decrypted: Path,
    contacts: list[dict[str, Any]],
    sessions: set[str],
    inventory: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    missing = [item["username"] for item in contacts if item["username"] not in inventory]
    inspected = _inspect_history(decrypted, missing) if missing else {}
    output = []
    for contact in contacts:
        username = contact["username"]
        chat = inventory.get(username, inspected.get(username, {}))
        output.append(
            {
                **contact,
                "kind": chat.get("kind") or _kind(username),
                "in_session": username in sessions,
                "message_count": int(chat.get("message_count") or 0),
                "first_time": chat.get("first_time") or "",
                "last_time": chat.get("last_time") or "",
            }
        )
    return sorted(output, key=_candidate_sort_key)


def resolve_target(
    decrypted: Path,
    query: str,
    inventory_path: Path | None = None,
    suggestion_limit: int = 20,
) -> dict[str, Any]:
    decrypted = decrypted.resolve()
    normalized_query = normalize(query)
    if not normalized_query:
        raise ValueError("Query must not be empty")
    if suggestion_limit < 1 or suggestion_limit > 100:
        raise ValueError("suggestion_limit must be between 1 and 100")

    contacts = load_contacts(decrypted)
    sessions = load_sessions(decrypted)
    inventory = load_inventory(inventory_path.resolve() if inventory_path else None)

    username_matches = [
        item for item in contacts if normalize(item["username"]) == normalized_query
    ]
    if username_matches:
        candidates = _enrich_candidates(decrypted, username_matches, sessions, inventory)
        candidates[0]["matched_fields"] = ["username"]
        return {
            "status": "resolved",
            "query": query,
            "match_basis": "exact_username",
            "selected_username": candidates[0]["username"],
            "candidates": candidates,
        }

    exact_matches = []
    for item in contacts:
        fields = _matched_fields(item, normalized_query, exact=True)
        if fields:
            exact_matches.append(({**item, "matched_fields": fields}))
    if exact_matches:
        candidates = _enrich_candidates(decrypted, exact_matches, sessions, inventory)
        status = "resolved" if len(candidates) == 1 else "ambiguous"
        return {
            "status": status,
            "query": query,
            "match_basis": "exact_contact_field",
            "selected_username": candidates[0]["username"] if status == "resolved" else None,
            "candidates": candidates,
        }

    partial_matches = []
    if len(normalized_query) >= 2:
        for item in contacts:
            fields = _matched_fields(item, normalized_query, exact=False)
            if fields:
                partial_matches.append(({**item, "matched_fields": fields}))
    if partial_matches:
        partial_matches.sort(
            key=lambda item: (
                normalize(item.get("display_name")),
                normalize(item.get("username")),
            )
        )
        candidates = _enrich_candidates(
            decrypted, partial_matches[:suggestion_limit], sessions, inventory
        )
        return {
            "status": "suggestions",
            "query": query,
            "match_basis": "substring_suggestion_only",
            "selected_username": None,
            "candidates": candidates,
            "truncated": len(partial_matches) > suggestion_limit,
        }

    return {
        "status": "not_found",
        "query": query,
        "match_basis": "none",
        "selected_username": None,
        "candidates": [],
    }


def _print_candidate(item: dict[str, Any]) -> None:
    fields = ",".join(item.get("matched_fields") or [])
    print(
        "\t".join(
            [
                item["username"],
                item.get("display_name") or "",
                item.get("alias") or "",
                item.get("remark") or "",
                item.get("nick_name") or "",
                item.get("kind") or "",
                str(item.get("message_count") or 0),
                item.get("first_time") or "",
                item.get("last_time") or "",
                "是" if item.get("in_session") else "否",
                fields,
            ]
        )
    )


def print_text(result: dict[str, Any]) -> None:
    status = result["status"]
    if status == "resolved":
        print("STATUS\tRESOLVED")
    elif status == "ambiguous":
        print(f"STATUS\tAMBIGUOUS\t{len(result['candidates'])}")
    elif status == "suggestions":
        print(f"STATUS\tSUGGESTIONS_ONLY\t{len(result['candidates'])}")
    else:
        print("STATUS\tNOT_FOUND")
    if result["candidates"]:
        print(
            "username\tdisplay_name\talias\tremark\tnick_name\tkind\t"
            "message_count\tfirst_time\tlast_time\tin_session\tmatched_fields"
        )
        for item in result["candidates"]:
            _print_candidate(item)
    if status == "resolved":
        print(f"RESOLVED_USERNAME\t{result['selected_username']}")
    elif status == "ambiguous":
        print("Refusing to guess. Re-run with one candidate's exact username.")
    elif status == "suggestions":
        print("Suggestions are informational only. Re-run with an exact value.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Resolve a contact name safely; duplicate names are never guessed."
    )
    parser.add_argument("query", help="Internal username, alias, remark, or nickname")
    parser.add_argument("--decrypted", type=Path, required=True)
    parser.add_argument(
        "--inventory",
        type=Path,
        help="Optional inventory JSON used for chat counts and date range",
    )
    parser.add_argument("--limit", type=int, default=20, help="Suggestion limit (1-100)")
    parser.add_argument("--json", action="store_true", help="Print structured JSON")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = resolve_target(args.decrypted, args.query, args.inventory, args.limit)
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Resolver input error: {exc}") from exc
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print_text(result)
    if result["status"] == "resolved":
        return EXIT_RESOLVED
    if result["status"] == "ambiguous":
        return EXIT_AMBIGUOUS
    return EXIT_NOT_RESOLVED


if __name__ == "__main__":
    raise SystemExit(main())
