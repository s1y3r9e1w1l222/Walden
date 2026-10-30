from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import unittest
from pathlib import Path

from wechat_local.target_resolver import resolve_target


class TargetResolverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.decrypted = Path(self.temporary.name)
        (self.decrypted / "contact").mkdir()
        (self.decrypted / "session").mkdir()
        (self.decrypted / "message").mkdir()

        contacts = [
            ("synthetic_contact_a", "alias_a", "duplicate-label", "label-a"),
            ("synthetic_contact_b", "alias_b", "duplicate-label", "label-b"),
            ("synthetic_contact_unique", "alias_unique", "unique-label", "unique-nickname"),
            ("synthetic_contact_secondary", "alias_secondary", "", "secondary-nickname"),
        ]
        with sqlite3.connect(self.decrypted / "contact" / "contact.db") as conn:
            conn.execute(
                "CREATE TABLE contact "
                "(username TEXT, alias TEXT, remark TEXT, nick_name TEXT)"
            )
            conn.executemany("INSERT INTO contact VALUES (?, ?, ?, ?)", contacts)
        with sqlite3.connect(self.decrypted / "session" / "session.db") as conn:
            conn.execute("CREATE TABLE SessionTable (username TEXT)")
            conn.executemany(
                "INSERT INTO SessionTable VALUES (?)",
                [
                    ("synthetic_contact_a",),
                    ("synthetic_contact_unique",),
                    ("synthetic_contact_secondary",),
                ],
            )
        with sqlite3.connect(self.decrypted / "message" / "message_0.db") as conn:
            for username, *_ in contacts:
                table = "Msg_" + hashlib.md5(username.encode()).hexdigest()
                conn.execute(f'CREATE TABLE "{table}" (create_time INTEGER)')
                if username == "synthetic_contact_unique":
                    conn.executemany(
                        f'INSERT INTO "{table}" VALUES (?)', [(100,), (200,)]
                    )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_exact_username_has_priority_over_names(self) -> None:
        result = resolve_target(self.decrypted, "synthetic_contact_a")
        self.assertEqual(result["status"], "resolved")
        self.assertEqual(result["selected_username"], "synthetic_contact_a")
        self.assertEqual(result["match_basis"], "exact_username")

    def test_duplicate_remark_is_ambiguous(self) -> None:
        result = resolve_target(self.decrypted, "duplicate-label")
        self.assertEqual(result["status"], "ambiguous")
        self.assertIsNone(result["selected_username"])
        self.assertEqual(
            {item["username"] for item in result["candidates"]},
            {"synthetic_contact_a", "synthetic_contact_b"},
        )

    def test_unique_remark_resolves_and_has_chat_range(self) -> None:
        result = resolve_target(self.decrypted, "unique-label")
        self.assertEqual(result["status"], "resolved")
        self.assertEqual(result["selected_username"], "synthetic_contact_unique")
        candidate = result["candidates"][0]
        self.assertEqual(candidate["message_count"], 2)
        self.assertTrue(candidate["in_session"])
        self.assertTrue(candidate["first_time"])
        self.assertTrue(candidate["last_time"])

    def test_partial_match_never_auto_selects(self) -> None:
        result = resolve_target(self.decrypted, "unique-nick")
        self.assertEqual(result["status"], "suggestions")
        self.assertIsNone(result["selected_username"])
        self.assertEqual(
            result["candidates"][0]["username"], "synthetic_contact_unique"
        )

    def test_one_character_query_does_not_enumerate_suggestions(self) -> None:
        result = resolve_target(self.decrypted, "x")
        self.assertEqual(result["status"], "not_found")
        self.assertEqual(result["candidates"], [])


if __name__ == "__main__":
    unittest.main()
