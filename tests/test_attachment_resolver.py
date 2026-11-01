from __future__ import annotations

import hashlib
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import zstandard

from wechat_local.attachment_resolver import AttachmentResolver, ResourceBinding
from wechat_local.event_store import RawMessage, SourceKey, _content_fingerprint


SOURCE = SourceKey('fixture-account', 'fixture-generation', 'message_0.db', 'Msg_fixture')


def message(name='report.pdf', data=b'%PDF-1.7\nfixture', *, md5=True, size=None):
    digest = hashlib.md5(data).hexdigest() if md5 else ''
    xml = (f'<msg><appmsg><type>6</type><title>{name}</title><appattach>'
           f'<totallen>{len(data) if size is None else size}</totallen><md5>{digest}</md5>'
           '</appattach></appmsg></msg>')
    return RawMessage(1, 101, 49, 1700000000, 7, xml, 0)


class AttachmentResolverTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.resolver = AttachmentResolver(self.root, account_id=SOURCE.account_id,
                                           source_generation=SOURCE.source_generation)

    def tearDown(self):
        self.temp.cleanup()

    def put(self, relative, data):
        p = self.root / relative
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        return p

    def resolve(self, msg, binding=None):
        return self.resolver.resolve(SOURCE, msg, resource=binding)

    def binding(self, msg, path, downloaded=True):
        return ResourceBinding(SOURCE, msg.local_id, _content_fingerprint(msg), path, downloaded)

    def test_three_formats_return_verified_path_and_provenance(self):
        for extension, data in [('pdf', b'%PDF-1.7\nfixture'), ('xlsx', b'PK\x03\x04xlsx fixture'),
                                ('zip', b'PK\x05\x06zip fixture')]:
            with self.subTest(extension=extension):
                msg = message('report.' + extension, data)
                p = self.put('2023-11/report.' + extension, data)
                result = self.resolve(msg)
                self.assertEqual(result['status'], 'resolved')
                self.assertEqual(result['path'], str(p))
                self.assertEqual(result['sha256'], hashlib.sha256(data).hexdigest())
                self.assertEqual(result['binding']['raw_fingerprint'], _content_fingerprint(msg))
                self.assertEqual(result['binding']['server_id'], 101)

    def test_requested_word_spreadsheet_presentation_and_archive_formats(self):
        ole = b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1fixture'
        zipped = b'PK\x03\x04fixture'
        formats = [('doc', ole), ('docx', zipped), ('docm', zipped), ('wps', ole),
                   ('xls', ole), ('xlsm', zipped), ('xltm', zipped), ('numbers', zipped),
                   ('pptx', zipped), ('rar', b'Rar!\x1a\x07\x00fixture'),
                   ('7z', b'7z\xbc\xaf\x27\x1cfixture')]
        for extension, data in formats:
            with self.subTest(extension=extension):
                p = self.put('a/report.' + extension, data)
                result = self.resolve(message('report.' + extension, data))
                self.assertEqual(result['status'], 'resolved')
                self.assertEqual(result['path'], str(p))
                self.assertEqual(result['sha256'], hashlib.sha256(data).hexdigest())
        self.put('a/invalid.rar', zipped)
        self.assertEqual(self.resolve(message('invalid.rar', zipped))['reason'], 'format_mismatch')
        self.put('a/rar5.rar', b'Rar!\x1a\x07\x01\x00fixture')
        self.assertEqual(self.resolve(message('rar5.rar', b'Rar!\x1a\x07\x01\x00fixture'))['status'], 'resolved')

    def test_same_name_different_content_uses_hash_not_name_size_or_time(self):
        data = b'%PDF-1.7\nright'
        wrong = b'%PDF-1.7\nwrong'
        self.put('a/report.pdf', wrong)
        p = self.put('b/report.pdf', data)
        self.assertEqual(self.resolve(message(data=data))['path'], str(p))

    def test_same_content_at_two_paths_is_ambiguous(self):
        data = b'%PDF-1.7\nfixture'
        self.put('a/report.pdf', data)
        self.put('b/report.pdf', data)
        result = self.resolve(message())
        self.assertEqual(result['status'], 'ambiguous')
        self.assertNotIn('path', result)

    def test_name_and_size_without_hash_are_insufficient(self):
        self.put('a/report.pdf', b'%PDF-1.7\nfixture')
        self.assertEqual(self.resolve(message(md5=False))['status'], 'insufficient_evidence')

    def test_direct_appmsg_md5_has_explicit_provenance(self):
        msg = message()
        digest = hashlib.md5(b'%PDF-1.7\nfixture').hexdigest()
        msg = replace(msg, raw_content=msg.raw_content.replace(f'<md5>{digest}</md5>', '').replace('</appmsg>', f'<md5>{digest}</md5></appmsg>'))
        p = self.put('a/report.pdf', b'%PDF-1.7\nfixture')
        result = self.resolve(msg)
        self.assertEqual(result['status'], 'resolved')
        self.assertEqual(result['path'], str(p))
        self.assertEqual(result['md5_xml_path'], 'appmsg/md5')

    def test_conflicting_direct_and_attach_hashes_are_rejected(self):
        msg = message()
        msg = replace(msg, raw_content=msg.raw_content.replace('</appmsg>', '<md5>' + '0'*32 + '</md5></appmsg>'))
        self.put('a/report.pdf', b'%PDF-1.7\nfixture')
        self.assertEqual(self.resolve(msg)['status'], 'invalid_metadata')

    def test_nested_thumbnail_hash_never_identifies_file(self):
        msg = message(md5=False)
        digest = hashlib.md5(b'%PDF-1.7\nfixture').hexdigest()
        msg = replace(msg, raw_content=msg.raw_content.replace('</appmsg>', f'<thumbnail><md5>{digest}</md5></thumbnail></appmsg>'))
        self.put('a/report.pdf', b'%PDF-1.7\nfixture')
        self.assertEqual(self.resolve(msg)['status'], 'insufficient_evidence')

    def test_no_candidate_does_not_claim_never_downloaded(self):
        result = self.resolve(message())
        self.assertEqual(result['status'], 'not_found')
        self.assertEqual(result['download_state'], 'unknown')

    def test_verified_resource_missing_and_not_downloaded_are_distinct(self):
        msg = message()
        self.assertEqual(self.resolve(msg, self.binding(msg, 'a/report.pdf'))['status'], 'missing')
        self.assertEqual(self.resolve(msg, self.binding(msg, None, False))['status'], 'not_downloaded')

    def test_explicit_binding_has_priority_and_cannot_fallback_on_failure(self):
        msg = message()
        p = self.put('right/report.pdf', b'%PDF-1.7\nfixture')
        self.put('wrong/report.pdf', b'%PDF-1.7\ncorrupt')
        self.assertEqual(self.resolve(msg, self.binding(msg, 'right/report.pdf'))['path'], str(p))
        result = self.resolve(msg, self.binding(msg, 'wrong/report.pdf'))
        self.assertEqual(result['status'], 'verification_failed')
        self.assertNotIn('path', result)

    def test_resource_can_bind_renamed_file_without_message_hash(self):
        msg = message(md5=False)
        p = self.put('a/renamed.pdf', b'%PDF-1.7\nfixture')
        self.assertEqual(self.resolve(msg, self.binding(msg, 'a/renamed.pdf'))['path'], str(p))

    def test_hash_size_and_format_failures_are_explicit(self):
        self.put('a/report.pdf', b'%PDF-1.7\ncorrupt')
        self.assertEqual(self.resolve(message())['status'], 'verification_failed')
        self.assertEqual(self.resolve(message(size=1))['reason'], 'size_mismatch')
        self.put('a/report.pdf', b'wrong signature')
        self.assertEqual(self.resolve(message(data=b'wrong signature'))['reason'], 'format_mismatch')
        self.assertEqual(self.resolve(message('report.exe'))['status'], 'unsupported_format')

    def test_duplicate_message_is_idempotent_but_revision_is_revalidated(self):
        self.put('a/report.pdf', b'%PDF-1.7\nfixture')
        msg = message()
        self.assertEqual(self.resolve(msg), self.resolve(msg))
        changed = replace(msg, raw_content=msg.raw_content.replace('report.pdf', 'changed.pdf'))
        self.assertEqual(self.resolve(changed, self.binding(msg, 'a/report.pdf'))['status'], 'binding_mismatch')
        self.put('a/report.pdf', b'%PDF-1.7\ncorrupt')
        self.assertEqual(self.resolve(msg)['status'], 'verification_failed')

    def test_binding_cannot_cross_account_generation_or_local_id(self):
        msg = message()
        self.put('a/report.pdf', b'%PDF-1.7\nfixture')
        for binding in [replace(self.binding(msg, 'a/report.pdf'), source=replace(SOURCE, account_id='other')),
                        replace(self.binding(msg, 'a/report.pdf'), local_id=2)]:
            self.assertEqual(self.resolve(msg, binding)['status'], 'binding_mismatch')
        self.assertEqual(self.resolver.resolve(replace(SOURCE, source_generation='other'), msg)['status'], 'binding_mismatch')

    def test_paths_traversal_and_symlinks_never_produce_reference(self):
        msg = message()
        p = self.put('a/report.pdf', b'%PDF-1.7\nfixture')
        for value in ['../report.pdf', str(p), 'a/../a/report.pdf']:
            self.assertEqual(self.resolve(msg, self.binding(msg, value))['status'], 'unsafe_path')
        (self.root / 'link').symlink_to(self.root / 'a', target_is_directory=True)
        self.assertEqual(self.resolve(msg, self.binding(msg, 'link/report.pdf'))['status'], 'unsafe_path')
        (self.root / 'linked.pdf').symlink_to(p)
        linked = message('linked.pdf')
        self.assertNotEqual(self.resolve(linked)['status'], 'resolved')

    def test_metadata_validation_is_bounded_and_does_not_open_media(self):
        msg = message()
        for content in ['<!DOCTYPE msg [<!ENTITY x "y">]><msg/>', '<msg>',
                        msg.raw_content.replace('<md5>', '<md5>bad'),
                        msg.raw_content.replace('<totallen>', '<totallen>-'),
                        '<msg>' + 'x' * 1048576 + '</msg>']:
            self.assertEqual(self.resolve(replace(msg, raw_content=content))['status'], 'invalid_metadata')
        compressed = replace(msg, raw_content=zstandard.ZstdCompressor().compress(msg.raw_content.encode()), compression_type=4)
        self.assertEqual(self.resolve(compressed)['status'], 'not_found')
        self.assertEqual(self.resolve(replace(msg, local_type=1))['status'], 'not_file')
        self.assertEqual(self.resolve(replace(msg, raw_content=msg.raw_content.replace('<type>6', '<type>5')))['status'], 'not_file')

    def test_search_limits_fail_closed_instead_of_picking_first_match(self):
        self.put('a/report.pdf', b'%PDF-1.7\nfixture')
        self.put('b/report.pdf', b'%PDF-1.7\nfixture')
        resolver = AttachmentResolver(self.root, account_id=SOURCE.account_id,
                                      source_generation=SOURCE.source_generation, max_entries=1)
        self.assertEqual(resolver.resolve(SOURCE, message())['status'], 'scan_incomplete')

    def test_file_changed_during_hashing_is_not_resolved(self):
        p = self.put('a/report.pdf', b'%PDF-1.7\nfixture')
        msg = message()
        original = hashlib.md5
        class Digest:
            def __init__(self):
                self.value = original()
            def update(self, data):
                self.value.update(data)
                p.write_bytes(b'%PDF-1.7\nchanged')
            def hexdigest(self):
                return self.value.hexdigest()
        # Change the real file after a block is read, before the final stat.
        with patch('wechat_local.attachment_resolver.hashlib.md5', Digest):
            result = self.resolve(msg)
        self.assertEqual(result['status'], 'unavailable')
        self.assertEqual(result['reason'], 'file_changed')

    def test_oversized_file_stops_before_hashing(self):
        p = self.put('a/report.pdf', b'')
        with p.open('wb') as handle:
            handle.truncate(256 * 1024 * 1024 + 1)
        result = self.resolve(message(size=256 * 1024 * 1024 + 1))
        self.assertEqual(result['status'], 'unavailable')
        self.assertEqual(result['reason'], 'file_limit')

    def test_hundred_candidates_limit_cannot_return_partial_unique_match(self):
        for number in range(101):
            self.put(f'{number}/report.pdf', b'%PDF-1.7\nfixture')
        self.assertEqual(self.resolve(message())['status'], 'scan_incomplete')

    def test_candidate_created_during_enumeration_prevents_unique_resolution(self):
        import os
        self.put('a/report.pdf', b'%PDF-1.7\nfixture')
        original = os.fwalk
        def changed(*args, **kwargs):
            first = True
            for item in original(*args, **kwargs):
                if first:
                    self.put('b/report.pdf', b'%PDF-1.7\nfixture')
                    first = False
                yield item
        with patch('wechat_local.attachment_resolver.os.fwalk', changed):
            self.assertEqual(self.resolve(message())['status'], 'scan_incomplete')

    def test_verified_file_is_rechecked_after_other_candidates_are_hashed(self):
        first = self.put('a/report.pdf', b'%PDF-1.7\nfixture')
        self.put('b/report.pdf', b'%PDF-1.7\ninvalid')
        original = self.resolver._verify
        def changed(relative, *args):
            try:
                return original(relative, *args)
            finally:
                if relative == 'b/report.pdf':
                    first.write_bytes(b'%PDF-1.7\nchanged')
        with patch.object(self.resolver, '_verify', changed):
            self.assertNotEqual(self.resolve(message())['status'], 'resolved')

    def test_mismatching_candidate_is_also_rechecked_before_unique_result(self):
        first = self.put('a/report.pdf', b'%PDF-1.7\ninvalid')
        self.put('b/report.pdf', b'%PDF-1.7\nfixture')
        original = self.resolver._verify
        def changed(relative, *args):
            result = original(relative, *args)
            if relative == 'b/report.pdf':
                first.write_bytes(b'%PDF-1.7\nfixture')
            return result
        with patch.object(self.resolver, '_verify', changed):
            self.assertNotEqual(self.resolve(message())['status'], 'resolved')

    def test_hash_budget_counts_actual_handle_bytes_after_candidate_growth(self):
        self.put('a/report.pdf', b'')
        self.put('b/report.pdf', b'')
        msg = message()
        msg = replace(msg, raw_content=msg.raw_content.replace('<totallen>16</totallen>', ''))
        original = self.resolver._verify
        def grown(relative, *args):
            (self.root / relative).write_bytes(b'%PDF-1.7\ninvalid' if relative.startswith('a/') else b'%PDF-1.7\nfixture')
            return original(relative, *args)
        with patch('wechat_local.attachment_resolver.MAX_HASH_BYTES', 20), patch.object(self.resolver, '_verify', grown):
            result = self.resolve(msg)
        self.assertEqual(result['status'], 'scan_incomplete')
        self.assertEqual(result['reason'], 'hash_budget')


if __name__ == '__main__':
    unittest.main()
