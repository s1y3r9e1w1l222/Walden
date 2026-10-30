from __future__ import annotations

import sqlite3
import os
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from concurrent.futures import Future, ThreadPoolExecutor

from wechat_local.agent_store import AgentStore
from wechat_local.codex_first import TimeWindow
import wechat_local.codex_first_runtime as module
from wechat_local.event_store import EventStore
from wechat_local.live_catalog import LiveCatalog
from wechat_local.live_readonly import open_encrypted_readonly


class ShardParallelTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.message_root = root / 'message'
        self.message_root.mkdir()
        for index in range(2):
            db = sqlite3.connect(self.message_root / f'message_{index}.db')
            table = 'Msg_' + format(index, '032x')
            db.execute(f'CREATE TABLE {table} (local_id INTEGER PRIMARY KEY, server_id INTEGER, local_type INTEGER NOT NULL, create_time INTEGER, real_sender_id INTEGER, message_content BLOB, WCDB_CT_message_content INTEGER)')
            db.execute(f'INSERT INTO {table} VALUES (1, ?, 1, 1700000000, 7, ?, 0)', (101 + index, b'fixture'))
            db.commit()
            db.close()
        self.store = EventStore(root / 'events.db')
        self.agent = AgentStore(root / 'agent.db')
        def connect(path):
            db = sqlite3.connect(path, check_same_thread=False)
            db.execute('PRAGMA query_only = ON')
            return db
        self.catalog = LiveCatalog.dynamic(self.message_root, connect, store=self.store, account_id='fixture', source_generation='fixture')
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(self.store.close)
        self.addCleanup(self.agent.close)
        self.addCleanup(self.catalog.close)

    def test_two_shards_are_read_concurrently_with_results_preserved(self):
        barrier = threading.Barrier(2)
        original = module.read_message_window
        observed = []
        lock = threading.Lock()
        def read(connection, table, **kwargs):
            with lock:
                observed.append((id(connection), threading.get_ident()))
            barrier.wait(timeout=2)
            return original(connection, table, **kwargs)
        runtime = module.CodexFirstRuntime(self.catalog, self.store, self.agent, account_id='fixture', source_generation='fixture', capability_check=lambda _: module.SourceCapabilityEvidence(direction_proven=False, contact_mapping_proven=False))
        runtime._scan_workers = 2
        window = TimeWindow.from_utc(datetime.fromtimestamp(1699999990, timezone.utc), datetime.fromtimestamp(1700000010, timezone.utc))
        with patch.object(module, 'read_message_window', read):
            result = runtime.scan(window, display_limit=10)
        self.assertEqual(len(observed), 2)
        self.assertEqual(len({thread for _, thread in observed}), 2)
        self.assertNotEqual(result.status, 'unavailable')
        self.assertNotIn('source_read_unavailable', result.coverage.gaps)

    def test_each_table_failure_is_preserved_and_later_sources_still_read(self):
        table = 'Msg_' + format(2, '032x')
        with sqlite3.connect(self.message_root / 'message_0.db') as db:
            db.execute(f'CREATE TABLE {table} (local_id INTEGER PRIMARY KEY, server_id INTEGER, local_type INTEGER NOT NULL, create_time INTEGER, real_sender_id INTEGER, message_content BLOB, WCDB_CT_message_content INTEGER)')
            db.execute(f'INSERT INTO {table} VALUES (1, 103, 1, 1700000000, 7, ?, 0)', (b'fixture',))
        self.catalog.refresh_catalog()
        runtime = module.CodexFirstRuntime(self.catalog, self.store, self.agent, account_id='fixture', source_generation='fixture', capability_check=lambda _: module.SourceCapabilityEvidence(direction_proven=False, contact_mapping_proven=False))
        runtime._scan_workers = 2
        sources = runtime._sources()
        self.assertEqual(len(sources), 3)
        active = set()
        overlap = []
        lock = threading.Lock()
        original = module.read_message_window
        def read(connection, name, **kwargs):
            with lock:
                if id(connection) in active:
                    overlap.append(True)
                active.add(id(connection))
            try:
                time.sleep(.05)
                if name == table:
                    raise RuntimeError('private fixture details')
                return original(connection, name, **kwargs)
            finally:
                with lock:
                    active.remove(id(connection))
        with patch.object(module, 'read_message_window', read):
            results = runtime._parallel_window_reads(sources, {s: True for s in sources}, 1699999990, 1700000010)
        self.assertFalse(overlap)
        self.assertEqual(sum(value[2] is None for value in results.values()), 2)
        self.assertEqual(sum(isinstance(value[2], RuntimeError) for value in results.values()), 1)

    def test_real_sqlcipher_connection_handoff_and_back(self):
        from sqlcipher3 import dbapi2 as cipher
        path = self.message_root / 'encrypted.db'
        db = cipher.connect(str(path))
        db.execute("PRAGMA key='" + 'p' * 32 + "'")
        db.execute('PRAGMA cipher_compatibility=4')
        db.execute('CREATE TABLE fixture (id INTEGER)')
        db.execute('INSERT INTO fixture VALUES (7)')
        db.commit()
        db.close()
        connection, _ = open_encrypted_readonly(path, b'p' * 32, _thread_handoff=True)
        try:
            self.assertEqual(connection.execute('SELECT count(*) FROM fixture').fetchone()[0], 1)
            with ThreadPoolExecutor(max_workers=1) as pool:
                self.assertEqual(pool.submit(lambda: connection.execute('SELECT id FROM fixture').fetchone()[0]).result(), 7)
            self.assertEqual(connection.execute('SELECT id FROM fixture').fetchone()[0], 7)
        finally:
            connection.close()

    def test_scan_replays_partial_reads_on_main_thread_in_source_order(self):
        active = set()
        overlap = []
        commits = []
        lock = threading.Lock()
        original = module.read_message_window
        original_commit = self.store.commit_window_messages
        caller = threading.get_ident()
        bad_table = 'Msg_' + format(1, '032x')
        def read(connection, table, **kwargs):
            with lock:
                active.add(id(connection))
            try:
                time.sleep(.05)
                if table == bad_table:
                    raise RuntimeError('private details')
                return original(connection, table, **kwargs)
            finally:
                with lock:
                    active.remove(id(connection))
        def checked_callback(*_args):
            with lock:
                if active or threading.get_ident() != caller:
                    overlap.append(True)
            return True
        def commit(source, *args, **kwargs):
            checked_callback()
            commits.append(source.table_name)
            return original_commit(source, *args, **kwargs)
        evidence = module.SourceCapabilityEvidence(direction_proven=True, contact_mapping_proven=True, inbound_message=checked_callback, inbound_event_ref=checked_callback, display_name=lambda _: 'fixture', include_source=checked_callback)
        runtime = module.CodexFirstRuntime(self.catalog, self.store, self.agent, account_id='fixture', source_generation='fixture', capability_check=lambda _: evidence)
        runtime._scan_workers = 2
        window = TimeWindow.from_utc(datetime.fromtimestamp(1699999990, timezone.utc), datetime.fromtimestamp(1700000010, timezone.utc))
        with patch.object(module, 'read_message_window', read), patch.object(self.store, 'commit_window_messages', commit):
            result = runtime.scan(window, display_limit=10)
        self.assertFalse(overlap)
        self.assertEqual(commits, ['Msg_' + format(0, '032x')])
        self.assertIn('source_read_unavailable', result.coverage.gaps)
        self.assertEqual(runtime._query_failures[0]['stage'], 'source_read')
        self.assertNotIn('private details', str(runtime._query_failures))

    def test_interrupt_waits_for_active_sql_before_connections_are_reused(self):
        from sqlcipher3 import dbapi2 as cipher
        root = self.message_root.parent / 'encrypted-message'
        root.mkdir()
        for index in range(2):
            db = cipher.connect(str(root / f'message_{index}.db'))
            db.execute("PRAGMA key='" + 'p' * 32 + "'")
            db.execute('PRAGMA cipher_compatibility=4')
            table = 'Msg_' + format(index, '032x')
            db.execute(f'CREATE TABLE {table} (local_id INTEGER PRIMARY KEY, server_id INTEGER, local_type INTEGER NOT NULL, create_time INTEGER, real_sender_id INTEGER, message_content BLOB, WCDB_CT_message_content INTEGER)')
            db.commit()
            db.close()
        catalog = LiveCatalog.dynamic_encrypted(root, b'p' * 32, store=self.store, account_id='encrypted', source_generation='fixture', _thread_handoff=True)
        self.addCleanup(catalog.close)
        runtime = module.CodexFirstRuntime(catalog, self.store, self.agent, account_id='encrypted', source_generation='fixture', capability_check=lambda _: module.SourceCapabilityEvidence(direction_proven=False, contact_mapping_proven=False))
        runtime._scan_workers = 2
        sources = runtime._sources()
        entered = {id(shard.connection): threading.Event() for shard in catalog.shards}
        exited = []
        def long_read(connection, _name, **_kwargs):
            connection.set_progress_handler(lambda: entered[id(connection)].set() or 0, 1000)
            try:
                connection.execute('WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000000) SELECT sum(x) FROM n').fetchone()
            finally:
                connection.set_progress_handler(None, 0)
                exited.append(id(connection))
        def interrupted(_future, *args, **kwargs):
            self.assertTrue(all(event.wait(5) for event in entered.values()))
            raise KeyboardInterrupt()
        with patch.object(module, 'read_message_window', long_read), patch.object(Future, 'result', interrupted):
            with self.assertRaises(KeyboardInterrupt):
                runtime._parallel_window_reads(sources, {s: True for s in sources}, 1699999990, 1700000010)
        self.assertEqual(len(exited), 2)
        for shard in catalog.shards:
            self.assertEqual(shard.connection.execute('SELECT 1').fetchone()[0], 1)

    def test_voice_epoch_does_not_reuse_payload_after_wal_changes(self):
        path = self.message_root / 'message_0.db'
        writer = sqlite3.connect(path, check_same_thread=False)
        self.addCleanup(writer.close)
        writer.execute('PRAGMA journal_mode=WAL')
        runtime = module.CodexFirstRuntime(self.catalog, self.store, self.agent, account_id='fixture', source_generation='fixture', capability_check=lambda _: module.SourceCapabilityEvidence(direction_proven=False, contact_mapping_proven=False))
        runtime.voice_enricher = object()
        runtime._scan_workers = 2
        sources = runtime._sources()
        original = module.read_message_window
        def read(connection, table, **kwargs):
            value = original(connection, table, **kwargs)
            if table == 'Msg_' + format(0, '032x'):
                writer.execute(f'UPDATE {table} SET server_id=123 WHERE local_id=1')
                writer.commit()
            return value
        with patch.object(module, 'read_message_window', read):
            values = runtime._parallel_window_reads(sources, {s: True for s in sources}, 1699999990, 1700000010)
        changed = next(s for s in sources if s.shard_name == 'message_0.db')
        self.assertIsNone(values[changed][1])
        self.assertIsNone(values[changed][2])

    def test_unknown_file_identity_serializes_every_shard(self):
        runtime = module.CodexFirstRuntime(self.catalog, self.store, self.agent, account_id='fixture', source_generation='fixture', capability_check=lambda _: module.SourceCapabilityEvidence(direction_proven=False, contact_mapping_proven=False))
        runtime._scan_workers = 2
        sources = runtime._sources()
        original_stat = Path.lstat
        original_read = module.read_message_window
        threads = set()
        def stat(path, *args, **kwargs):
            if path.name == 'message_0.db':
                raise OSError('synthetic unknown identity')
            return original_stat(path, *args, **kwargs)
        def read(connection, table, **kwargs):
            threads.add(threading.get_ident())
            return original_read(connection, table, **kwargs)
        with patch.object(Path, 'lstat', stat), patch.object(module, 'read_message_window', read):
            values = runtime._parallel_window_reads(sources, {s: True for s in sources}, 1699999990, 1700000010)
        self.assertEqual(len(threads), 1)
        self.assertTrue(all(value[2] is None for value in values.values()))

    def test_hardlink_aliases_do_not_read_same_file_concurrently(self):
        root = self.message_root.parent / 'aliases'
        root.mkdir()
        os.link(self.message_root / 'message_0.db', root / 'message_0.db')
        os.link(self.message_root / 'message_0.db', root / 'message_1.db')
        def connect(path):
            db = sqlite3.connect(path, check_same_thread=False)
            db.execute('PRAGMA query_only=ON')
            return db
        catalog = LiveCatalog.dynamic(root, connect, store=self.store, account_id='alias', source_generation='fixture')
        self.addCleanup(catalog.close)
        runtime = module.CodexFirstRuntime(catalog, self.store, self.agent, account_id='alias', source_generation='fixture', capability_check=lambda _: module.SourceCapabilityEvidence(direction_proven=False, contact_mapping_proven=False))
        runtime._scan_workers = 2
        sources = runtime._sources()
        original = module.read_message_window
        threads = set()
        def read(connection, table, **kwargs):
            threads.add(threading.get_ident())
            return original(connection, table, **kwargs)
        with patch.object(module, 'read_message_window', read):
            values = runtime._parallel_window_reads(sources, {s: True for s in sources}, 1699999990, 1700000010)
        self.assertEqual(len(sources), 2)
        self.assertEqual(len(threads), 1)
        self.assertTrue(all(value[2] is None for value in values.values()))


if __name__ == '__main__':
    unittest.main()
