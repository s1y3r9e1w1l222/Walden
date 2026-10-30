from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from wechat_local.agent_store import AgentStore
from wechat_local.codex_first import TimeWindow
import wechat_local.codex_first_runtime as runtime_module
from wechat_local.event_store import EventStore, SourceKey
from wechat_local.live_catalog import LiveCatalog
from wechat_local import live_tailer


class EmptyWindowProofTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db = sqlite3.connect(self.root / 'source.db')
        self.db.execute('CREATE TABLE Msg_00000000000000000000000000000000 (local_id INTEGER PRIMARY KEY, local_type INTEGER NOT NULL, create_time, real_sender_id INTEGER, message_content BLOB, WCDB_CT_message_content INTEGER)')
        self.table = 'Msg_00000000000000000000000000000000'
        self.addCleanup(self.db.close)

    def test_metadata_probe_respects_inclusive_boundaries_and_future(self):
        def probe():
            return live_tailer._empty_window_probe(self.db, self.table, start=1700000000, end=1700000100)
        def add(identifier, value):
            self.db.execute(f'INSERT INTO {self.table} VALUES (?,1,?,1,?,0)', (identifier, value, b'fixture'))
            self.db.commit()
        self.db.execute('PRAGMA query_only=ON')
        self.assertEqual(probe(), (True, True))
        self.db.execute('PRAGMA query_only=OFF')
        for index, value in enumerate((None, 'not-int', 1.5, 946684799, 4102444800), 1):
            add(index, value)
        self.db.execute('PRAGMA query_only=ON')
        self.assertEqual(probe(), (True, True))
        self.db.execute('PRAGMA query_only=OFF')
        add(10, 1699999999)
        self.db.execute('PRAGMA query_only=ON')
        self.assertEqual(probe(), (True, True))
        self.db.execute('PRAGMA query_only=OFF')
        add(11, 1700000000)
        self.db.execute('PRAGMA query_only=ON')
        self.assertEqual(probe(), (False, True))
        self.db.execute('PRAGMA query_only=OFF')
        add(12, 1700000100)
        self.db.execute('PRAGMA query_only=ON')
        self.assertEqual(probe(), (False, True))
        self.db.execute('PRAGMA query_only=OFF')
        add(13, 1700000101)
        self.db.execute('PRAGMA query_only=ON')
        self.assertEqual(probe(), (False, False))

    def test_proof_is_current_state_not_ingest_audit_or_user_text(self):
        store = EventStore(self.root / 'events.sqlite')
        self.addCleanup(store.close)
        source = SourceKey('fixture', 'generation', 'message_0.db', self.table)
        stamps = ((1, 2, 3, 4, 5), None)
        binding = 'a' * 64
        store._save_empty_window_proofs([(source, 1700000000, 1700000100, stamps, True, binding)])
        proofs = store._empty_window_proofs('fixture', 'generation')
        self.assertEqual(proofs[source], (1700000000, 1700000100, stamps, True, binding))
        self.assertEqual(store._conn.execute('SELECT count(*) FROM window_ingest_runs').fetchone()[0], 0)
        store._save_empty_window_proofs([(source, 1700000030, 1700000130, stamps, True, binding)])
        self.assertEqual(len(store._empty_window_proofs('fixture', 'generation')), 1)
        store._conn.execute('UPDATE current_empty_window_coverage SET predicate_version=2')
        store._conn.commit()
        self.assertEqual(store._empty_window_proofs('fixture', 'generation'), {})
        store._conn.execute("UPDATE current_empty_window_coverage SET predicate_version=1,source_stamps='broken'")
        store._conn.commit()
        self.assertEqual(store._empty_window_proofs('fixture', 'generation'), {})

    def test_attempt_is_durable_across_store_reopen(self):
        path=self.root/'attempts.sqlite'
        first=EventStore(path)
        self.assertTrue(first._begin_window_baseline('fixture','generation'))
        self.assertFalse(first._window_baseline_initialized('fixture','generation'))
        first.close()
        second=EventStore(path)
        try:
            self.assertFalse(second._begin_window_baseline('fixture','generation'))
            self.assertFalse(second._window_baseline_initialized('fixture','generation'))
        finally:second.close()


class RuntimeEmptyCoverageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.message_root = root / 'message'
        self.message_root.mkdir()
        self.tables = ['Msg_' + format(index, '032x') for index in (0, 1)]
        for index, table in enumerate(self.tables):
            db = sqlite3.connect(self.message_root / f'message_{index}.db')
            db.execute(f'CREATE TABLE {table} (local_id INTEGER PRIMARY KEY,server_id INTEGER,local_type INTEGER NOT NULL,create_time INTEGER,real_sender_id INTEGER,message_content BLOB,WCDB_CT_message_content INTEGER)')
            db.execute(f'INSERT INTO {table} VALUES (1,101,1,?,7,?,0)', (1699999900 if index == 0 else 1700000050, b'fixture'))
            db.commit();db.close()
        self.store = EventStore(root / 'events.sqlite');self.addCleanup(self.store.close)
        self.agent = AgentStore(root / 'agent.sqlite');self.addCleanup(self.agent.close)
        def open_query_only(path):
            db = sqlite3.connect(path, check_same_thread=False)
            db.execute('PRAGMA query_only=ON')
            return db
        self.catalog = LiveCatalog.dynamic(self.message_root, open_query_only, store=self.store, account_id='fixture', source_generation='fixture')
        self.addCleanup(self.catalog.close)
        self.runtime = runtime_module.CodexFirstRuntime(self.catalog, self.store, self.agent, account_id='fixture', source_generation='fixture', capability_check=lambda _: runtime_module.SourceCapabilityEvidence(direction_proven=False, contact_mapping_proven=False))
        self.runtime._scan_workers = 2
        self.runtime._empty_window_enabled = True
    def window(self, start, end):
        return TimeWindow.from_utc(datetime.fromtimestamp(start, timezone.utc), datetime.fromtimestamp(end, timezone.utc))
    def test_rolling_empty_source_reused_without_read_or_ingest(self):
        original = runtime_module.read_message_window
        observed = []
        def read(connection, table, **kwargs):
            observed.append(table)
            return original(connection, table, **kwargs)
        with patch.object(runtime_module, 'read_message_window', read):
            first = self.runtime.scan(self.window(1700000000, 1700000100), display_limit=10)
            self.assertNotEqual(first.status, 'unavailable')
            first_count = self.store._conn.execute('SELECT count(*) FROM window_ingest_runs').fetchone()[0]
            observed.clear()
            second = self.runtime.scan(self.window(1700000030, 1700000130), display_limit=10)
        self.assertNotEqual(second.status, 'unavailable')
        self.assertEqual(observed, [self.tables[1]])
        self.assertEqual(first_count, 2)
        self.assertEqual(self.store._conn.execute('SELECT count(*) FROM window_ingest_runs').fetchone()[0], 3)
    def test_changed_source_reloads_low_id_and_keeps_result(self):
        first = self.runtime.scan(self.window(1700000000, 1700000100), display_limit=10)
        self.assertNotEqual(first.status, 'unavailable')
        with sqlite3.connect(self.message_root / 'message_0.db') as db:
            db.execute(f'INSERT INTO {self.tables[0]} VALUES (0,102,1,1700000050,7,?,0)', (b'changed',))
        with patch.object(runtime_module, '_empty_window_probe', side_effect=AssertionError('outside-window metadata must not be rescanned')):
            second = self.runtime.scan(self.window(1700000030, 1700000130), display_limit=10)
        self.assertNotEqual(second.status, 'unavailable')
        self.assertEqual(sum(group.total_messages for group in second.groups), 2)

    def test_fresh_connection_reuses_stable_empty_proof(self):
        self.runtime.scan(self.window(1700000000, 1700000100), display_limit=10)
        self.catalog.close()
        def connect(path):
            db = sqlite3.connect(path, check_same_thread=False)
            db.execute('PRAGMA query_only=ON')
            return db
        self.catalog = LiveCatalog.dynamic(self.message_root, connect, store=self.store, account_id='fixture', source_generation='fixture')
        self.addCleanup(self.catalog.close)
        runtime = runtime_module.CodexFirstRuntime(self.catalog, self.store, self.agent, account_id='fixture', source_generation='fixture', capability_check=lambda _: runtime_module.SourceCapabilityEvidence(direction_proven=False, contact_mapping_proven=False))
        runtime._scan_workers = 2;runtime._empty_window_enabled = True
        observed = [];original = runtime_module.read_message_window
        def read(connection, table, **kwargs):
            observed.append(table);return original(connection, table, **kwargs)
        with patch.object(runtime_module, 'read_message_window', read):
            result = runtime.scan(self.window(1700000030, 1700000130), display_limit=10)
        self.assertNotEqual(result.status, 'unavailable')
        self.assertEqual(observed, [self.tables[1]])

    def test_all_proofs_hit_without_source_unavailable_or_new_ingest(self):
        with sqlite3.connect(self.message_root / 'message_1.db') as db:
            db.execute(f'DELETE FROM {self.tables[1]}')
        self.runtime.scan(self.window(1700000000, 1700000100), display_limit=10)
        before = self.store._conn.execute('SELECT count(*) FROM window_ingest_runs').fetchone()[0]
        with patch.object(runtime_module, 'read_message_window', side_effect=AssertionError('unexpected message read')), patch.object(runtime_module, '_empty_window_probe', side_effect=AssertionError('unexpected metadata scan')):
            result = self.runtime.scan(self.window(1700000030, 1700000130), display_limit=10)
        self.assertNotEqual(result.error_code, 'source_unavailable')
        self.assertNotEqual(result.status, 'unavailable')
        self.assertEqual(sum(group.total_messages for group in result.groups), 0)
        self.assertEqual(self.store._conn.execute('SELECT count(*) FROM window_ingest_runs').fetchone()[0], before)

    def test_future_timestamp_disallows_rolling_shortcut(self):
        with sqlite3.connect(self.message_root / 'message_0.db') as db:
            db.execute(f'INSERT INTO {self.tables[0]} VALUES (2,102,1,1700000120,7,?,0)', (b'future',))
        first = self.runtime.scan(self.window(1700000000, 1700000100), display_limit=10)
        self.assertNotEqual(first.status, 'unavailable')
        second = self.runtime.scan(self.window(1700000030, 1700000130), display_limit=10)
        self.assertNotEqual(second.status, 'unavailable')
        self.assertEqual(sum(group.total_messages for group in second.groups), 2)

    def test_expanding_left_cannot_reuse_empty_window(self):
        self.runtime.scan(self.window(1700000000, 1700000100), display_limit=10)
        second = self.runtime.scan(self.window(1699999800, 1700000100), display_limit=10)
        self.assertNotEqual(second.status, 'unavailable')
        self.assertEqual(sum(group.total_messages for group in second.groups), 2)

    def test_change_after_reuse_check_fails_closed_and_next_query_recovers(self):
        self.runtime.scan(self.window(1700000000, 1700000100), display_limit=10)
        original = self.runtime._parallel_window_reads
        def read_then_modify(*args):
            values = original(*args)
            with sqlite3.connect(self.message_root / 'message_0.db') as db:
                db.execute(f'INSERT INTO {self.tables[0]} VALUES (0,102,1,1700000050,7,?,0)', (b'changed',))
            return values
        with patch.object(self.runtime, '_parallel_window_reads', read_then_modify):
            result = self.runtime.scan(self.window(1700000030, 1700000130), display_limit=10)
        self.assertEqual(result.error_code, 'source_changed')
        self.assertEqual(result.status, 'unavailable')
        recovered = self.runtime.scan(self.window(1700000030, 1700000130), display_limit=10)
        self.assertNotEqual(recovered.status, 'unavailable')
        self.assertEqual(sum(group.total_messages for group in recovered.groups), 2)

    def test_path_binding_mismatch_is_a_cache_miss(self):
        self.runtime.scan(self.window(1700000000, 1700000100), display_limit=10)
        self.store._conn.execute("UPDATE current_empty_window_coverage SET path_binding=?", ('0'*64,))
        self.store._conn.commit()
        observed=[];original=runtime_module.read_message_window
        def read(connection, table, **kwargs):
            observed.append(table);return original(connection, table, **kwargs)
        with patch.object(runtime_module, 'read_message_window', read):
            result=self.runtime.scan(self.window(1700000030, 1700000130), display_limit=10)
        self.assertNotEqual(result.status, 'unavailable')
        self.assertIn(self.tables[0], observed)

    def test_wal_change_invalidates_empty_proof(self):
        writer = sqlite3.connect(self.message_root / 'message_0.db')
        self.addCleanup(writer.close)
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute(f'UPDATE {self.tables[0]} SET server_id=101')
        writer.commit()
        self.runtime.scan(self.window(1700000000, 1700000100), display_limit=10)
        writer.execute(f'INSERT INTO {self.tables[0]} VALUES (0,102,1,1700000050,7,?,0)', (b'wal change',))
        writer.commit()
        result=self.runtime.scan(self.window(1700000030, 1700000130), display_limit=10)
        self.assertNotEqual(result.status, 'unavailable')
        self.assertEqual(sum(group.total_messages for group in result.groups), 2)

    def test_empty_proof_does_not_reuse_deleted_store_member(self):
        self.runtime.scan(self.window(1700000000, 1700000100), display_limit=10)
        with sqlite3.connect(self.message_root / 'message_1.db') as db:
            db.execute(f'DELETE FROM {self.tables[1]}')
        after_delete = self.runtime.scan(self.window(1700000000, 1700000100), display_limit=10)
        self.assertEqual(sum(g.total_messages for g in after_delete.groups), 0)
        with patch.object(runtime_module, 'read_message_window', side_effect=AssertionError('unexpected source read')):
            reused = self.runtime.scan(self.window(1700000000, 1700000100), display_limit=10)
        self.assertNotEqual(reused.status, 'unavailable')
        self.assertEqual(sum(g.total_messages for g in reused.groups), 0)

    def test_failed_max_is_not_empty_and_baseline_remains_incomplete(self):
        original=runtime_module._empty_window_probe
        def probe(connection, table, **kwargs):
            if table==self.tables[0]:
                raise RuntimeError('private failure text')
            return original(connection, table, **kwargs)
        with patch.object(runtime_module, '_empty_window_probe', probe):
            result=self.runtime.scan(self.window(1700000000, 1700000100), display_limit=10)
        self.assertIn('source_read_unavailable', result.coverage.gaps)
        self.assertFalse(self.store._window_baseline_initialized('fixture','fixture'))
        self.assertNotIn(self.tables[0], [s.table_name for s in self.store._empty_window_proofs('fixture','fixture')])
        self.assertNotIn('private failure text', str(self.runtime._query_failures))

    def test_failed_first_probe_is_not_repeated_on_ordinary_query(self):
        with patch.object(runtime_module, '_empty_window_probe', side_effect=RuntimeError('synthetic')):
            self.runtime.scan(self.window(1700000000,1700000100), display_limit=10)
        self.assertFalse(self.store._window_baseline_initialized('fixture','fixture'))
        with patch.object(runtime_module, '_empty_window_probe', side_effect=AssertionError('must not retry MAX')) as probe:
            result=self.runtime.scan(self.window(1700000000,1700000100), display_limit=10)
        probe.assert_not_called()
        self.assertNotEqual(result.status,'unavailable')
        self.assertFalse(self.store._window_baseline_initialized('fixture','fixture'))

    def test_cancelled_first_probe_is_not_repeated_on_ordinary_query(self):
        with patch.object(runtime_module, '_empty_window_probe', side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.runtime.scan(self.window(1700000000,1700000100), display_limit=10)
        self.assertFalse(self.store._window_baseline_initialized('fixture','fixture'))
        with patch.object(runtime_module, '_empty_window_probe', side_effect=AssertionError('must not retry MAX')) as probe:
            result=self.runtime.scan(self.window(1700000000,1700000100), display_limit=10)
        probe.assert_not_called()
        self.assertNotEqual(result.status,'unavailable')
        self.assertFalse(self.store._window_baseline_initialized('fixture','fixture'))

    def test_empty_max_proof_lost_before_worker_finishes_fails_closed(self):
        original=runtime_module._empty_window_probe
        def probe(connection,table,**kwargs):
            result=original(connection,table,**kwargs)
            if table==self.tables[0]:
                with sqlite3.connect(self.message_root/'message_0.db') as writer:
                    writer.execute(f'INSERT INTO {table} VALUES (0,102,1,1700000050,7,?,0)',(b'new',))
            return result
        with patch.object(runtime_module,'_empty_window_probe',probe):
            result=self.runtime.scan(self.window(1700000000,1700000100),display_limit=10)
        self.assertEqual(result.status,'unavailable')
        self.assertEqual(result.error_code,'source_changed')

    def test_empty_max_proof_lost_during_commit_fails_closed(self):
        original=self.store.commit_window_messages
        changed=[]
        def commit(source,*args,**kwargs):
            result=original(source,*args,**kwargs)
            if source.table_name==self.tables[0] and not changed:
                with sqlite3.connect(self.message_root/'message_0.db') as writer:
                    writer.execute(f'INSERT INTO {self.tables[0]} VALUES (0,102,1,1700000050,7,?,0)',(b'new',))
                changed.append(True)
            return result
        with patch.object(self.store,'commit_window_messages',commit):
            result=self.runtime.scan(self.window(1700000000,1700000100),display_limit=10)
        self.assertEqual(result.status,'unavailable')
        self.assertEqual(result.error_code,'source_changed')

    def test_old_completed_marker_migrates_without_retry(self):
        path=self.root_for_migration()
        store=EventStore(path);store.close()
        with sqlite3.connect(path) as db:
            db.execute('DROP TABLE current_window_baseline')
            db.execute('CREATE TABLE current_window_baseline (account_id TEXT NOT NULL,source_generation TEXT NOT NULL,predicate_version INTEGER NOT NULL,PRIMARY KEY(account_id,source_generation))')
            db.execute("INSERT INTO current_window_baseline VALUES ('old','generation',1)")
        migrated=EventStore(path)
        try:
            self.assertTrue(migrated._window_baseline_initialized('old','generation'))
            self.assertFalse(migrated._begin_window_baseline('old','generation'))
        finally:migrated.close()

    def root_for_migration(self):
        return self.message_root.parent/'old-events.sqlite'


if __name__ == '__main__':
    unittest.main()
