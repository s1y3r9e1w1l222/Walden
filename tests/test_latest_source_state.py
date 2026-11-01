"""Synthetic latest selection must bind the complete current shard set."""
import shutil
from unittest.mock import patch

from tests.test_latest_window import LatestWindowTests, TARGET, table
from tests import test_codex_first_voice_cli as fixture
import wechat_local.codex_first_runtime as runtime


class LatestSourceStateTests(LatestWindowTests):
    def test_empty_latest_cannot_hide_new_shard(self):
        c = fixture._open_encrypted_writer(self.f.message_path)
        try:
            c.execute('DELETE FROM "' + table(TARGET) + '"')
            c.commit()
        finally:
            c.close()
        original = runtime.CodexFirstRuntime._latest_candidates
        def added(instance, sources, limit):
            result = original(instance, sources, limit)
            shutil.copyfile(self.f.message_path, self.f.message_root / 'message_3.db')
            return result
        with patch.object(runtime.CodexFirstRuntime, '_latest_candidates', new=added), \
             patch.object(runtime, 'read_message_window', side_effect=AssertionError('no body after shard change')):
            code, result, _ = self.latest('2')
        self.assertTrue(code != 0 or result['status'] == 'unavailable')
        self.assertFalse(result.get('groups'))
        self.assertEqual(result.get('error_code'), 'source_changed')

    def test_unselected_shard_disappearing_rejects(self):
        path = self.f.message_root / 'message_1.db'
        shutil.copyfile(self.f.message_path, path)
        c = fixture._open_encrypted_writer(path)
        try:
            c.execute('DROP TABLE "' + table(TARGET) + '"')
            c.commit()
        finally:
            c.close()
        original = runtime.CodexFirstRuntime._latest_candidates
        def removed(instance, sources, limit):
            result = original(instance, sources, limit)
            path.unlink()
            return result
        with patch.object(runtime.CodexFirstRuntime, '_latest_candidates', new=removed), \
             patch.object(runtime, 'read_message_window', side_effect=AssertionError('no body after shard change')):
            code, result, _ = self.latest('2')
        self.assertTrue(code != 0 or result['status'] == 'unavailable')
        self.assertFalse(result.get('groups'))
        self.assertEqual(result.get('error_code'), 'source_changed')

    def test_shard_appearing_during_scope_discovery_rejects_before_body(self):
        original = runtime.CodexFirstRuntime._prepare_contact_scope
        def added(instance, window):
            shutil.copyfile(self.f.message_path, self.f.message_root / 'message_3.db')
            return original(instance, window)
        with patch.object(runtime.CodexFirstRuntime, '_prepare_contact_scope', new=added), \
             patch.object(runtime, 'read_message_window', side_effect=AssertionError('no body after shard change')):
            code, result, _ = self.latest('2')
        self.assertTrue(code != 0 or result['status'] == 'unavailable')
        self.assertFalse(result.get('groups'))
        self.assertEqual(result.get('error_code'), 'source_changed')

    def test_unselected_shard_changes_after_render_rejects_output(self):
        path = self.f.message_root / 'message_1.db'
        shutil.copyfile(self.f.message_path, path)
        c = fixture._open_encrypted_writer(path)
        try:
            c.execute('DROP TABLE "' + table(TARGET) + '"')
            c.commit()
        finally:
            c.close()
        original = runtime.CodexFirstService.render
        def changed(instance, *args, **kwargs):
            result = original(instance, *args, **kwargs)
            c = fixture._open_encrypted_writer(path)
            try:
                c.execute('CREATE TABLE "' + table(TARGET) + '" (local_id INTEGER PRIMARY KEY,server_id INTEGER,local_type INTEGER,create_time INTEGER,real_sender_id INTEGER,message_content BLOB,WCDB_CT_message_content INTEGER,packed_info_data BLOB)')
                c.execute('INSERT INTO "' + table(TARGET) + '" VALUES(?,?,?,?,?,?,?,?)',
                          (99, 999, 1, 1700001000, 9, b'new target table', 0, None))
                c.commit()
            finally:
                c.close()
            return result
        with patch.object(runtime.CodexFirstService, 'render', new=changed):
            code, result, _ = self.latest('2')
        self.assertTrue(code != 0 or result['status'] == 'unavailable')
        self.assertFalse(result.get('groups'))
        self.assertEqual(result.get('error_code'), 'source_changed')

    def test_existing_date_mode_does_not_enable_latest_guard(self):
        with patch.object(runtime.CodexFirstRuntime, '_ensure_latest_source_state',
                          side_effect=AssertionError('latest guard only')):
            code, result, _ = self.run_cli('--contact-query', 'Target Remark')
        self.assertEqual(code, 0)
        self.assertEqual(result['total_count'], 2)

    def test_new_shard_after_candidates_rejects_before_body(self):
        original = runtime.CodexFirstRuntime._latest_candidates
        def added(instance, sources, limit):
            result = original(instance, sources, limit)
            path = self.f.message_root / 'message_3.db'
            shutil.copyfile(self.f.message_path, path)
            c = fixture._open_encrypted_writer(path)
            try:
                c.execute('INSERT INTO "' + table(TARGET) + '" VALUES(?,?,?,?,?,?,?,?)',
                          (99, 999, 1, 1700001000, 9, b'new shard', 0, None))
                c.commit()
            finally:
                c.close()
            return result
        with patch.object(runtime.CodexFirstRuntime, '_latest_candidates', new=added), \
             patch.object(runtime, 'read_message_window', side_effect=AssertionError('no body after shard change')):
            code, result, _ = self.latest('2')
        self.assertTrue(code != 0 or result['status'] == 'unavailable')
        self.assertFalse(result.get('groups'))
        self.assertEqual(result.get('error_code'), 'source_changed')

    def test_other_shard_gains_target_table_after_candidates(self):
        path = self.f.message_root / 'message_1.db'
        shutil.copyfile(self.f.message_path, path)
        c = fixture._open_encrypted_writer(path)
        try:
            c.execute('DROP TABLE "' + table(TARGET) + '"')
            c.commit()
        finally:
            c.close()
        original = runtime.CodexFirstRuntime._latest_candidates
        def added(instance, sources, limit):
            result = original(instance, sources, limit)
            c = fixture._open_encrypted_writer(path)
            try:
                c.execute('CREATE TABLE "' + table(TARGET) + '" (local_id INTEGER PRIMARY KEY,server_id INTEGER,local_type INTEGER,create_time INTEGER,real_sender_id INTEGER,message_content BLOB,WCDB_CT_message_content INTEGER,packed_info_data BLOB)')
                c.execute('INSERT INTO "' + table(TARGET) + '" VALUES(?,?,?,?,?,?,?,?)',
                          (99, 999, 1, 1700001000, 9, b'new target table', 0, None))
                c.commit()
            finally:
                c.close()
            return result
        with patch.object(runtime.CodexFirstRuntime, '_latest_candidates', new=added), \
             patch.object(runtime, 'read_message_window', side_effect=AssertionError('no body after shard change')):
            code, result, _ = self.latest('2')
        self.assertTrue(code != 0 or result['status'] == 'unavailable')
        self.assertFalse(result.get('groups'))
        self.assertEqual(result.get('error_code'), 'source_changed')
