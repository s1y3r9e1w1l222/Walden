"""Exercise the public command against the existing synthetic encrypted fixture."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest

import test_codex_first_voice_cli as fixtures


class AttachmentCliTests(unittest.TestCase):
    setUp = fixtures.CodexFirstVoiceCliTests.setUp
    tearDown = fixtures.CodexFirstVoiceCliTests.tearDown

    def test_public_command_resolves_all_supported_formats_without_changing_source(self):
        files = self.root.resolve() / 'attachments'
        files.mkdir()
        ole = b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1fixture'
        zipped = b'PK\x03\x04fixture'
        formats = [('pdf', b'%PDF-1.7\nfixture'), ('xlsx', zipped), ('zip', zipped),
                   ('doc', ole), ('docx', zipped), ('docm', zipped), ('wps', ole),
                   ('xls', ole), ('xlsm', zipped), ('xltm', zipped), ('numbers', zipped),
                   ('pptx', zipped), ('rar', b'Rar!\x1a\x07\x00fixture'),
                   ('7z', b'7z\xbc\xaf\x27\x1cfixture')]
        with fixtures._open_encrypted_writer(self.message_path) as source:
            source.execute(f'DELETE FROM {fixtures.TABLE}')
            for number, (extension, data) in enumerate(formats, 1):
                name = f'report.{extension}'
                (files / name).write_bytes(data)
                xml = ('<msg><appmsg><type>6</type>' + f'<title>{name}</title><appattach>'
                       f'<totallen>{len(data)}</totallen><md5>{hashlib.md5(data).hexdigest()}</md5>'
                       '</appattach></appmsg></msg>')
                if extension == 'pdf':
                    digest_tag = f'<md5>{hashlib.md5(data).hexdigest()}</md5>'
                    xml = xml.replace(digest_tag, '').replace('</appmsg>', digest_tag + '</appmsg>')
                source.execute(f'INSERT INTO {fixtures.TABLE} VALUES (?, ?, 49, ?, 9, ?, 0, NULL)',
                               (number, number + 100, 1700000000 + number, xml))
        before = self.message_path.read_bytes()
        environment = os.environ.copy()
        environment['PYTHONPATH'] = str(Path(__file__).parents[1] / 'src')
        args = [sys.executable, '-m', 'wechat_local.codex_first_runtime',
                '--live-db-root', str(self.live_root), '--message-root', str(self.message_root),
                '--passphrase', str(self.passphrase), '--event-store', str(self.event_store_path),
                '--agent-store', str(self.agent_store_path), '--account-id', 'synthetic-account',
                '--source-generation', 'synthetic-generation', '--file-attachments-root', str(files),
                '--request', '从 2023-11-14 22:13 到 2023-11-14 22:15', '--timezone', 'UTC', '--json']
        completed = subprocess.run(args, env=environment, capture_output=True, text=True, timeout=45)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        result = json.loads(completed.stdout)
        refs = [ref for group in result['groups'] for ref in group['event_refs']]
        self.assertEqual(len(refs), 14)
        for ref, (extension, data) in zip(refs, formats):
            self.assertEqual(ref['attachment']['status'], 'resolved')
            self.assertEqual(ref['attachment']['path'], str(files / f'report.{extension}'))
            self.assertEqual(ref['attachment']['sha256'], hashlib.sha256(data).hexdigest())
            self.assertEqual(ref['attachment']['md5_xml_path'],
                             'appmsg/md5' if extension == 'pdf' else 'appmsg/appattach/md5')
        self.assertEqual(self.message_path.read_bytes(), before)
        self.assertEqual(sorted(p.name for p in files.iterdir()),
                         sorted('report.' + extension for extension, _data in formats))


if __name__ == '__main__':
    unittest.main()
