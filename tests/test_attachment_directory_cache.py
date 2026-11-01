"""One-query attachment directory snapshot, using synthetic local files."""
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from tests.test_attachment_resolver import SOURCE, message
from wechat_local.attachment_resolver import AttachmentResolver, ResourceBinding
from wechat_local.event_store import _content_fingerprint


class AttachmentDirectoryCacheTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name).resolve()
        (self.root/'sub').mkdir()
        (self.root/'sub/a.pdf').write_bytes(b'%PDF-1.7\na')
        (self.root/'sub/b.pdf').write_bytes(b'%PDF-1.7\nb')
        self.resolver=AttachmentResolver(self.root,account_id=SOURCE.account_id,source_generation=SOURCE.source_generation)
    def tearDown(self):self.tmp.cleanup()
    def resolve(self,name):
        return self.resolver.resolve(SOURCE,message(name,b'%PDF-1.7\n'+name[0].encode()))

    def test_two_attachments_one_complete_directory_walk(self):
        with patch('wechat_local.attachment_resolver.os.fwalk',wraps=os.fwalk) as walk:
            self.assertEqual(self.resolve('a.pdf')['status'],'resolved')
            self.assertEqual(self.resolve('b.pdf')['status'],'resolved')
        self.assertEqual(walk.call_count,1)

    def test_direct_resource_does_not_enumerate(self):
        msg=message('a.pdf',b'%PDF-1.7\na');binding=ResourceBinding(SOURCE,msg.local_id,_content_fingerprint(msg),'sub/a.pdf',True)
        with patch('wechat_local.attachment_resolver.os.fwalk',wraps=os.fwalk) as walk:
            self.assertEqual(self.resolver.resolve(SOURCE,msg,resource=binding)['status'],'resolved')
        self.assertEqual(walk.call_count,0)

    def test_directory_change_between_resolutions_cold_scans_and_detects_ambiguity(self):
        self.assertEqual(self.resolve('a.pdf')['status'],'resolved')
        (self.root/'other').mkdir();(self.root/'other/b.pdf').write_bytes(b'%PDF-1.7\nb')
        with patch('wechat_local.attachment_resolver.os.fwalk',wraps=os.fwalk) as walk:
            result=self.resolve('b.pdf')
        self.assertEqual(result['status'],'ambiguous');self.assertEqual(walk.call_count,1)

    def test_new_candidate_during_selection_fails_without_retry(self):
        self.assertEqual(self.resolve('a.pdf')['status'],'resolved')
        original=self.resolver._check_candidate_type
        def mutate(relative):
            original(relative)
            (self.root/'sub/new.pdf').write_bytes(b'%PDF-1.7\nnew')
        with patch.object(self.resolver,'_check_candidate_type',side_effect=mutate), \
             patch('wechat_local.attachment_resolver.os.fwalk',wraps=os.fwalk) as walk:
            result=self.resolve('b.pdf')
        self.assertEqual((result['status'],result['reason']),('scan_incomplete','directory_changed'))
        self.assertEqual(walk.call_count,0)

    def test_directory_change_after_hash_fails_without_retry(self):
        self.assertEqual(self.resolve('a.pdf')['status'],'resolved');original=self.resolver._verify
        def mutate(*args):
            result=original(*args);(self.root/'sub/new.pdf').write_bytes(b'%PDF-1.7\nnew');return result
        with patch.object(self.resolver,'_verify',side_effect=mutate), \
             patch('wechat_local.attachment_resolver.os.fwalk',wraps=os.fwalk) as walk:
            result=self.resolve('b.pdf')
        self.assertEqual((result['status'],result['reason']),('scan_incomplete','directory_changed'))
        self.assertEqual(walk.call_count,0)

    def test_changed_file_is_rehashed_on_next_resolution(self):
        self.assertEqual(self.resolve('a.pdf')['status'],'resolved')
        (self.root/'sub/b.pdf').write_bytes(b'%PDF-1.7\nx')
        with patch('wechat_local.attachment_resolver.os.fwalk',wraps=os.fwalk) as walk:
            result=self.resolve('b.pdf')
        self.assertEqual((result['status'],result['reason']),('verification_failed','hash_mismatch'))
        self.assertEqual(walk.call_count,0)

    def test_failed_first_walk_never_publishes_partial_index(self):
        original=os.fwalk
        def failed(*args,**kwargs):
            for entry in original(*args,**kwargs):
                yield entry
                raise OSError('synthetic enumeration failure')
        with patch('wechat_local.attachment_resolver.os.fwalk',side_effect=failed):
            self.assertNotEqual(self.resolve('a.pdf')['status'],'resolved')
        with patch('wechat_local.attachment_resolver.os.fwalk',wraps=original) as walk:
            self.assertEqual(self.resolve('b.pdf')['status'],'resolved')
        self.assertEqual(walk.call_count,1)

    def test_independent_resolver_and_new_query_enumerate_again(self):
        with patch('wechat_local.attachment_resolver.os.fwalk',wraps=os.fwalk) as walk:
            self.assertEqual(self.resolve('a.pdf')['status'],'resolved')
            self.resolver._begin_query()
            self.assertEqual(self.resolve('b.pdf')['status'],'resolved')
            other=AttachmentResolver(self.root,account_id=SOURCE.account_id,source_generation=SOURCE.source_generation)
            self.assertEqual(other.resolve(SOURCE,message('a.pdf',b'%PDF-1.7\na'))['status'],'resolved')
        self.assertEqual(walk.call_count,3)

    def test_unrelated_symlink_not_checked_as_candidate(self):
        (self.root/'sub/unrelated.pdf').symlink_to(self.root/'missing')
        self.assertEqual(self.resolve('a.pdf')['status'],'resolved')
        self.assertEqual(self.resolve('b.pdf')['status'],'resolved')

    def test_new_same_name_symlink_is_not_accepted(self):
        self.assertEqual(self.resolve('a.pdf')['status'],'resolved')
        (self.root/'other').mkdir();(self.root/'other/b.pdf').symlink_to(self.root/'sub/b.pdf')
        result=self.resolve('b.pdf')
        self.assertNotEqual(result['status'],'resolved')

    def test_cached_candidate_limit_is_preserved(self):
        for number in range(101):
            folder=self.root/('d%03d'%number);folder.mkdir();(folder/'many.pdf').write_bytes(b'%PDF-1.7\nm')
        self.assertEqual(self.resolve('a.pdf')['status'],'resolved')
        with patch('wechat_local.attachment_resolver.os.fwalk',wraps=os.fwalk) as walk:
            result=self.resolve('many.pdf')
        self.assertEqual((result['status'],result['reason']),('scan_incomplete','candidate_limit'))
        self.assertEqual(walk.call_count,0)

    def test_root_replacement_never_creates_a_new_baseline(self):
        self.assertEqual(self.resolve('a.pdf')['status'],'resolved')
        moved=self.root.with_name(self.root.name+'-old');self.root.rename(moved);self.root.mkdir()
        try:
            with patch('wechat_local.attachment_resolver.os.fwalk',wraps=os.fwalk) as walk:
                result=self.resolve('b.pdf')
            self.assertEqual((result['status'],result['reason']),('unavailable','root_changed'))
            self.assertEqual(walk.call_count,0)
        finally:
            self.root.rmdir();moved.rename(self.root)

    def test_replaced_directory_symlink_never_reads_outside_root(self):
        self.assertEqual(self.resolve('a.pdf')['status'],'resolved')
        moved=self.root.with_name(self.root.name+'-old-sub');(self.root/'sub').rename(moved)
        (self.root/'sub').symlink_to(moved,target_is_directory=True)
        try:
            result=self.resolve('b.pdf')
            self.assertNotEqual(result['status'],'resolved')
        finally:
            (self.root/'sub').unlink();moved.rename(self.root/'sub')

    def test_directory_disappears_between_resolutions_cold_scans(self):
        self.assertEqual(self.resolve('a.pdf')['status'],'resolved')
        moved=self.root.with_name(self.root.name+'-removed-sub');(self.root/'sub').rename(moved)
        try:
            with patch('wechat_local.attachment_resolver.os.fwalk',wraps=os.fwalk) as walk:
                result=self.resolve('b.pdf')
            self.assertEqual(result['status'],'not_found');self.assertEqual(walk.call_count,1)
        finally:
            moved.rename(self.root/'sub')

    def test_directory_depth_limit_does_not_publish_index(self):
        path=self.root
        for number in range(9):
            path=path/('depth%d'%number);path.mkdir()
        result=self.resolve('a.pdf')
        self.assertEqual((result['status'],result['reason']),('scan_incomplete','depth_limit'))
        self.assertIsNone(self.resolver._directory_index)
