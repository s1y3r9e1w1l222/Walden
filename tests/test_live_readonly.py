from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from wechat_local.live_readonly import (
    LiveReadonlyError,
    open_encrypted_readonly,
    probe_live_database,
    read_private_passphrase,
    readonly_database_uri,
)


class FakeCursor:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class FakeConnection:
    def __init__(self):
        self.statements: list[str] = []
        self.closed = False

    def execute(self, statement: str):
        self.statements.append(statement)
        normalized = " ".join(statement.lower().split())
        if normalized == "pragma query_only":
            return FakeCursor((1,))
        if normalized == "pragma journal_mode":
            return FakeCursor(("wal",))
        if "where type='table'" in normalized:
            return FakeCursor((17,))
        if "sqlite_master" in normalized:
            return FakeCursor((42,))
        return FakeCursor(None)

    def close(self):
        self.closed = True


class FakeSqlcipher:
    def __init__(self):
        self.calls = []
        self.connections: list[FakeConnection] = []

    def connect(self, path, **kwargs):
        self.calls.append((path, kwargs))
        conn = FakeConnection()
        self.connections.append(conn)
        return conn


class LiveReadonlyTests(unittest.TestCase):
    def test_readonly_uri_encodes_spaces_and_does_not_mark_live_db_immutable(self):
        uri = readonly_database_uri(Path("/tmp/WeChat Data/message_7.db"))
        self.assertIn("WeChat%20Data", uri)
        self.assertTrue(uri.endswith("?mode=ro"))
        self.assertNotIn("immutable", uri)

    def test_private_passphrase_rejects_permissive_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "passphrase.bin"
            path.write_bytes(b"x" * 32)
            path.chmod(0o644)
            with self.assertRaises(LiveReadonlyError):
                read_private_passphrase(path)
            path.chmod(0o600)
            self.assertEqual(read_private_passphrase(path), b"x" * 32)

    @patch(
        "wechat_local.live_readonly.derive_verified_database_material",
        return_value=(b"k" * 32, b"s" * 16),
    )
    def test_open_requests_readonly_uri_and_enables_query_only(self, _derive):
        fake = FakeSqlcipher()
        conn, form_bytes = open_encrypted_readonly(
            Path("/tmp/message_7.db"),
            b"p" * 32,
            sqlcipher_module=fake,
        )
        self.assertEqual(form_bytes, 32)
        self.assertIs(conn, fake.connections[0])
        uri, kwargs = fake.calls[0]
        self.assertTrue(uri.endswith("?mode=ro"))
        self.assertTrue(kwargs["uri"])
        statements = "\n".join(conn.statements)
        self.assertIn("PRAGMA query_only = ON", statements)
        self.assertIn("PRAGMA cipher_compatibility = 4", statements)

    @patch(
        "wechat_local.live_readonly.derive_verified_database_material",
        return_value=(b"k" * 32, b"s" * 16),
    )
    def test_probe_returns_only_non_message_diagnostics(self, _derive):
        fake = FakeSqlcipher()
        result = probe_live_database(
            Path("/tmp/message_7.db"),
            b"p" * 32,
            sqlcipher_module=fake,
        )
        self.assertEqual(result.database_name, "message_7.db")
        self.assertTrue(result.query_only)
        self.assertEqual(result.journal_mode, "wal")
        self.assertEqual(result.table_count, 17)
        self.assertTrue(fake.connections[0].closed)


if __name__ == "__main__":
    unittest.main()
