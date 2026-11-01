"""Bounded latest selection against synthetic encrypted source tables."""
import json
import io
import contextlib
import shutil
from unittest.mock import patch

from tests.test_contact_query import ContactQueryTests, TARGET, table
from tests import test_codex_first_voice_cli as fixture
import wechat_local.codex_first_runtime as runtime
from wechat_local.event_store import EventStore
import sqlite3


class LatestWindowTests(ContactQueryTests):
    def latest(self, *extra, contact_path=None, jsonl=False):
        f = self.f
        argv = ['--live-db-root', str(f.live_root), '--message-root', str(f.message_root),
                '--passphrase', str(f.passphrase), '--event-store', str(f.event_store_path),
                '--agent-store', str(f.agent_store_path), '--account-id', 'synthetic-account',
                '--source-generation', 'synthetic-generation', '--self-username', 'synthetic-self',
                '--timezone', 'UTC', '--json', '--encrypted-contact-database', str(contact_path or f.message_path),
                '--contact-query', 'Target Remark', '--latest', *extra]
        if jsonl:
            argv.remove('--json')
            argv.extend(['--voice-jsonl', '--voice-transcripts'])
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                code = runtime.main(argv)
            except SystemExit as exc:
                code = exc.code
        lines = out.getvalue().splitlines()
        self.latest_phases = [json.loads(line).get('phase') for line in lines] if jsonl else []
        return code, json.loads(lines[-1]) if lines else {}, err.getvalue()

    def test_latest_selects_both_directions_without_date(self):
        code, result, _ = self.latest('2')
        self.assertEqual(code, 0)
        self.assertEqual(result['total_count'], 2)
        self.assertEqual({r['citation_text'] for g in result['groups'] for r in g['event_refs']},
                         {'private incoming', 'private outgoing'})
        self.assertEqual(result['query_mode']['kind'], 'latest')
        self.assertEqual(result['requested'], {'start': None, 'end': None})
        with EventStore(self.f.event_store_path) as store:
            self.assertEqual(store._conn.execute('SELECT count(*) FROM current_window_baseline').fetchone()[0], 0)
            self.assertEqual(store._conn.execute('SELECT count(*) FROM current_empty_window_coverage').fetchone()[0], 0)
        with sqlite3.connect(self.f.agent_store_path) as conn:
            self.assertEqual(conn.execute('SELECT COALESCE(max(last_materialization_id),0) FROM projection_checkpoints').fetchone()[0], 0)

    def test_same_second_only_selected_ids_are_read(self):
        c = fixture._open_encrypted_writer(self.f.message_path)
        try:
            c.executemany('INSERT INTO "' + table(TARGET) + '" VALUES(?,?,?,?,?,?,?,?)',
                          [(i, 1000+i, 1, 1700000100, 9, ('body-%s' % i).encode(), 0, None)
                           for i in range(50, 60)])
            c.commit()
        finally:
            c.close()
        observed = []
        sql = []
        fetched = []
        original = runtime.read_message_window
        class Cursor:
            def __init__(self, cursor, body): self.cursor, self.body = cursor, body
            def fetchall(self):
                rows = self.cursor.fetchall()
                if self.body: fetched.extend(rows)
                return rows
            def __getattr__(self, name): return getattr(self.cursor, name)
        class Connection:
            def __init__(self, connection): self.connection = connection
            def execute(self, statement, *params):
                return Cursor(self.connection.execute(statement, *params),
                              'message_content' in statement and statement.lstrip().upper().startswith('SELECT'))
            def __getattr__(self, name): return getattr(self.connection, name)
        def wrapped(*args, **kwargs):
            args[0].set_trace_callback(sql.append)
            try:
                result = original(Connection(args[0]), *args[1:], **kwargs)
            finally:
                args[0].set_trace_callback(None)
            observed.extend(m.local_id for m in result.messages)
            return result
        with patch.object(runtime, 'read_message_window', side_effect=wrapped):
            code, result, _ = self.latest('2')
        self.assertEqual(code, 0)
        self.assertEqual(len(observed), 2)
        self.assertEqual(len(fetched), 2)
        self.assertEqual(result['total_count'], 2)
        body_sql = [s for s in sql if 'message_content' in s and s.lstrip().upper().startswith('SELECT')]
        self.assertTrue(body_sql)
        self.assertTrue(all('local_id IN (' in s for s in body_sql))

    def test_multiple_shards_are_merged_before_body_read(self):
        second = self.f.message_root / 'message_1.db'
        shutil.copyfile(self.f.message_path, second)
        c = fixture._open_encrypted_writer(second)
        try:
            c.execute('DELETE FROM "' + table(TARGET) + '"')
            c.executemany('INSERT INTO "' + table(TARGET) + '" VALUES(?,?,?,?,?,?,?,?)',
                          [(70, 707, 1, 1700000010, 9, b'shard incoming', 0, None),
                           (71, 708, 1, 1700000001, 11, b'shard outgoing', 0, None)])
            c.commit()
        finally:
            c.close()
        seen = []
        original = runtime.read_message_window
        def wrapped(*args, **kwargs):
            result = original(*args, **kwargs)
            seen.extend(m.raw_content for m in result.messages)
            return result
        with patch.object(runtime, 'read_message_window', side_effect=wrapped):
            code, result, _ = self.latest('2')
        self.assertEqual(code, 0)
        self.assertEqual(result['total_count'], 2)
        self.assertEqual(len(seen), 2)
        self.assertIn(b'shard incoming', seen)
        self.assertFalse(any(x in seen for x in (b'other group', b'unknown sender', b'outside window')))

    def test_empty_source_and_invalid_time_are_distinct(self):
        c = fixture._open_encrypted_writer(self.f.message_path)
        try:
            c.execute('DELETE FROM "' + table(TARGET) + '"')
            c.commit()
        finally:
            c.close()
        code, result, _ = self.latest()
        self.assertEqual(code, 0)
        self.assertEqual(result['total_count'], 0)
        c = fixture._open_encrypted_writer(self.f.message_path)
        try:
            c.execute('INSERT INTO "' + table(TARGET) + '" VALUES(?,?,?,?,?,?,?,?)',
                      (100, 1100, 1, None, 9, b'not ordered', 0, None))
            c.commit()
        finally:
            c.close()
        with patch.object(runtime, 'read_message_window', side_effect=AssertionError('no body on unknown order')):
            code, result, _ = self.latest()
        self.assertNotEqual(code, 0)
        self.assertNotIn('groups', result)

    def test_same_local_id_in_different_shards_is_not_deduplicated(self):
        second = self.f.message_root / 'message_1.db'
        shutil.copyfile(self.f.message_path, second)
        c = fixture._open_encrypted_writer(second)
        try:
            c.execute('DELETE FROM "' + table(TARGET) + '"')
            c.execute('INSERT INTO "' + table(TARGET) + '" VALUES(?,?,?,?,?,?,?,?)',
                      (21, 999, 1, 1700000001, 9, b'other shard incoming', 0, None))
            c.commit()
        finally:
            c.close()
        code, result, _ = self.latest('2')
        self.assertEqual(code, 0)
        refs = [r for group in result['groups'] for r in group['event_refs']]
        self.assertEqual({r['citation_text'] for r in refs}, {'private outgoing', 'other shard incoming'})
        self.assertEqual(result['query_mode']['same_second_order'], 'not_guaranteed')

    def test_unselected_shard_does_not_read_body_at_same_second(self):
        for number in (1, 2):
            path = self.f.message_root / ('message_%d.db' % number)
            shutil.copyfile(self.f.message_path, path)
            c = fixture._open_encrypted_writer(path)
            try:
                c.execute('DELETE FROM "' + table(TARGET) + '"')
                c.execute('INSERT INTO "' + table(TARGET) + '" VALUES(?,?,?,?,?,?,?,?)',
                          (21, 900 + number, 1, 1700000001, 9,
                           ('synthetic-shard-%d' % number).encode(), 0, None))
                c.commit()
            finally:
                c.close()
        sources = []
        fetched = []
        original = runtime.read_message_window
        def observed(connection, *args, **kwargs):
            sources.append(connection.execute('PRAGMA database_list').fetchone()[2])
            result = original(connection, *args, **kwargs)
            fetched.extend(result.messages)
            return result
        with patch.object(runtime, 'read_message_window', side_effect=observed):
            code, result, _ = self.latest('2')
        self.assertEqual(code, 0)
        self.assertEqual(result['total_count'], 2)
        self.assertEqual(len(fetched), 2)
        self.assertEqual({str((self.f.message_root / 'message_1.db').resolve()), str((self.f.message_root / 'message_2.db').resolve())}, set(sources))

    def test_source_changed_after_metadata_does_not_read_body(self):
        contact = self.f.voice_path
        c = fixture._open_encrypted_writer(contact)
        try:
            c.execute('CREATE TABLE contact(username TEXT, alias TEXT, remark TEXT, nick_name TEXT)')
            c.execute('INSERT INTO contact VALUES(?,?,?,?)', (TARGET, 'alias', 'Target Remark', 'name'))
            c.commit()
        finally:
            c.close()
        original = runtime.CodexFirstRuntime._latest_candidates
        def changed(instance, sources, limit):
            result = original(instance, sources, limit)
            c = fixture._open_encrypted_writer(self.f.message_path)
            try:
                c.execute('UPDATE "' + table(TARGET) + '" SET create_time=create_time+1 WHERE local_id=20')
                c.commit()
            finally:
                c.close()
            return result
        with patch.object(runtime.CodexFirstRuntime, '_latest_candidates', new=changed), \
             patch.object(runtime, 'read_message_window', side_effect=AssertionError('changed source must not read body')):
            code, result, _ = self.latest('2', contact_path=contact)
        self.assertTrue(code != 0 or result['status'] == 'unavailable')
        self.assertFalse(result.get('groups'))

    def test_source_change_after_render_rejects_output(self):
        contact = self.f.voice_path
        c = fixture._open_encrypted_writer(contact)
        try:
            c.execute('CREATE TABLE contact(username TEXT, alias TEXT, remark TEXT, nick_name TEXT)')
            c.execute('INSERT INTO contact VALUES(?,?,?,?)', (TARGET, 'alias', 'Target Remark', 'name'))
            c.commit()
        finally:
            c.close()
        original = runtime.CodexFirstService.render
        def changed(instance, *args, **kwargs):
            result = original(instance, *args, **kwargs)
            c = fixture._open_encrypted_writer(self.f.message_path)
            try:
                c.execute('UPDATE "' + table(TARGET) + '" SET create_time=create_time+1 WHERE local_id=20')
                c.commit()
            finally:
                c.close()
            return result
        with patch.object(runtime.CodexFirstService, 'render', new=changed):
            code, result, _ = self.latest('2', contact_path=contact)
        self.assertTrue(code != 0 or result['status'] == 'unavailable')
        self.assertFalse(result.get('groups'))

    def test_duplicate_local_id_is_rejected_before_body(self):
        c = fixture._open_encrypted_writer(self.f.message_path)
        try:
            name = '"' + table(TARGET) + '"'
            c.execute('DROP TABLE ' + name)
            c.execute('CREATE TABLE ' + name + ' (local_id INTEGER,server_id INTEGER,local_type INTEGER,create_time INTEGER,real_sender_id INTEGER,message_content BLOB,WCDB_CT_message_content INTEGER,packed_info_data BLOB)')
            c.executemany('INSERT INTO ' + name + ' VALUES(?,?,?,?,?,?,?,?)',
                          [(90, 900, 1, 1700000010, 9, b'first', 0, None),
                           (90, 901, 1, 1700000009, 9, b'second', 0, None)])
            c.commit()
        finally:
            c.close()
        statements = []
        original = runtime.read_message_window
        def traced(*args, **kwargs):
            args[0].set_trace_callback(statements.append)
            try: return original(*args, **kwargs)
            finally: args[0].set_trace_callback(None)
        with patch.object(runtime, 'read_message_window', side_effect=traced):
            code, result, _ = self.latest('1')
        self.assertTrue(code != 0 or result['status'] == 'unavailable')
        self.assertFalse(result.get('groups'))
        self.assertFalse(any('message_content' in s and s.lstrip().upper().startswith('SELECT') for s in statements))

    def test_name2id_change_after_metadata_does_not_read_body(self):
        contact = self.f.voice_path
        c = fixture._open_encrypted_writer(contact)
        try:
            c.execute('CREATE TABLE contact(username TEXT, alias TEXT, remark TEXT, nick_name TEXT)')
            c.execute('INSERT INTO contact VALUES(?,?,?,?)', (TARGET, 'alias', 'Target Remark', 'name'))
            c.commit()
        finally:
            c.close()
        original = runtime.CodexFirstRuntime._latest_candidates
        def changed(instance, sources, limit):
            result = original(instance, sources, limit)
            c = fixture._open_encrypted_writer(self.f.message_path)
            try:
                c.execute('DELETE FROM Name2Id WHERE user_name=?', (TARGET,))
                c.commit()
            finally:
                c.close()
            return result
        with patch.object(runtime.CodexFirstRuntime, '_latest_candidates', new=changed), \
             patch.object(runtime, 'read_message_window', side_effect=AssertionError('changed mapping must not read body')):
            code, result, _ = self.latest('2', contact_path=contact)
        self.assertTrue(code != 0 or result['status'] == 'unavailable')
        self.assertFalse(result.get('groups'))

    def test_latest_and_request_are_mutually_exclusive(self):
        code, _, _ = self.latest('--request', '从 2023-11-14 22:13 到 2023-11-14 22:15')
        self.assertNotEqual(code, 0)

    def test_latest_jsonl_has_one_terminal(self):
        code, result, _ = self.latest('2', jsonl=True)
        self.assertEqual(code, 0)
        self.assertEqual(self.latest_phases, ['messages', 'complete'])
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['query_mode']['kind'], 'latest')
