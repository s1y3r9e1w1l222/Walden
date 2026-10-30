from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from wechat_local.agent_store import (
    AGENT_STORE_APPLICATION_ID,
    AGENT_STORE_SCHEMA_VERSION,
    AgentStore,
    AgentStoreError,
)
from wechat_local.event_search_projection import main as projection_main


class AgentStoreTests(unittest.TestCase):
    def test_initialize_reopen_identity_fts_integrity_and_permissions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "private" / "agent.db"
            with AgentStore(path) as store:
                self.assertEqual(
                    int(store._conn.execute("PRAGMA application_id").fetchone()[0]),
                    AGENT_STORE_APPLICATION_ID,
                )
                self.assertEqual(
                    int(store._conn.execute("PRAGMA user_version").fetchone()[0]),
                    AGENT_STORE_SCHEMA_VERSION,
                )
                self.assertEqual(store.integrity_check(), "ok")
                self.assertEqual(
                    int(store._conn.execute("PRAGMA foreign_keys").fetchone()[0]), 1
                )
                schema_names = {
                    str(row[0])
                    for row in store._conn.execute(
                        "SELECT name FROM sqlite_master "
                        "WHERE type IN ('table', 'index')"
                    )
                }
                self.assertIn("event_search_projection", schema_names)
                self.assertIn("event_search_fts", schema_names)
                self.assertIn("event_search_projection_query_idx", schema_names)
                store._conn.execute(
                    "INSERT INTO event_search_fts(searchable_text) VALUES (?)",
                    ("synthetic searchable",),
                )
                self.assertEqual(
                    int(
                        store._conn.execute(
                            "SELECT count(*) FROM event_search_fts "
                            "WHERE event_search_fts MATCH 'synthetic'"
                        ).fetchone()[0]
                    ),
                    1,
                )
            with AgentStore(path) as reopened:
                self.assertEqual(reopened.integrity_check(), "ok")
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(path.parent.stat().st_mode & 0o077, 0)

    def test_rejects_unrelated_nonempty_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "unrelated.db"
            connection = sqlite3.connect(path)
            connection.execute("CREATE TABLE user_data(value TEXT)")
            connection.commit()
            connection.close()
            with self.assertRaisesRegex(AgentStoreError, "nonempty"):
                AgentStore(path)

    def test_rejects_future_schema_and_wrong_application_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            future = Path(directory) / "future.db"
            connection = sqlite3.connect(future)
            connection.execute(
                f"PRAGMA user_version = {AGENT_STORE_SCHEMA_VERSION + 1}"
            )
            connection.commit()
            connection.close()
            with self.assertRaisesRegex(AgentStoreError, "future"):
                AgentStore(future)

            wrong = Path(directory) / "wrong.db"
            connection = sqlite3.connect(wrong)
            connection.execute("PRAGMA application_id = 12345")
            connection.commit()
            connection.close()
            with self.assertRaisesRegex(AgentStoreError, "Unrelated"):
                AgentStore(wrong)

    def test_rejects_symlink_destination(self) -> None:
        if not hasattr(os, "symlink"):
            self.skipTest("symlink is unavailable")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "target.db"
            target.touch()
            link = root / "agent.db"
            link.symlink_to(target)
            with self.assertRaisesRegex(AgentStoreError, "symlink"):
                AgentStore(link)

    def test_initialization_failure_rolls_back_schema_and_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "agent.db"
            broken_schema = (
                "CREATE TABLE partial(value TEXT);"
                "THIS IS NOT VALID SQLITE;"
            )
            with patch("wechat_local.agent_store.SCHEMA_V1", broken_schema):
                with self.assertRaises(sqlite3.DatabaseError):
                    AgentStore(path)
            connection = sqlite3.connect(path)
            try:
                self.assertEqual(
                    int(connection.execute("PRAGMA application_id").fetchone()[0]), 0
                )
                self.assertEqual(
                    int(connection.execute("PRAGMA user_version").fetchone()[0]), 0
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT name FROM sqlite_master WHERE name='partial'"
                    ).fetchone(),
                    None,
                )
            finally:
                connection.close()

    def test_projection_cli_rejects_same_event_and_agent_store_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "same.db"
            with self.assertRaisesRegex(SystemExit, "separate files"):
                projection_main(
                    [
                        "--event-store",
                        str(path),
                        "--agent-store",
                        str(path),
                    ]
                )


if __name__ == "__main__":
    unittest.main()
