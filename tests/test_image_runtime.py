import hashlib
import base64
import io
import os
import sqlite3
import struct
import tempfile
import unittest
from unittest.mock import patch
from datetime import timezone
from pathlib import Path

from wechat_local.image_runtime import (
    ImageMessageIdentity,
    ImageUnavailable,
    _bound_file,
    decode_image,
    enrich_bound_images,
)
import wechat_local.image_runtime as image_runtime


class ImageBindingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.conn = sqlite3.connect(":memory:")
        self.conn.executescript(
            """
            CREATE TABLE ChatName2Id (rowid INTEGER PRIMARY KEY, user_name TEXT NOT NULL);
            CREATE TABLE MessageResourceInfo (
                chat_id INTEGER, message_svr_id INTEGER, message_local_id INTEGER,
                message_create_time INTEGER, message_local_type INTEGER,
                message_id TEXT, packed_info BLOB
            );
            CREATE TABLE MessageResourceDetail (message_id TEXT, size INTEGER);
            """
        )
        self.conversation = "synthetic-chat@chatroom"
        self.token = "a" * 32
        self.when = 1792972800
        self.conn.execute("INSERT INTO ChatName2Id(rowid,user_name) VALUES (1,?)", (self.conversation,))
        self.conn.execute(
            "INSERT INTO MessageResourceInfo VALUES (1, 17, 8, ?, 3, 'resource-1', ?)",
            (self.when, f"<img>{self.token}</img>".encode()),
        )
        self.conn.execute("INSERT INTO MessageResourceDetail VALUES ('resource-1', 4)")
        month = __import__("datetime").datetime.fromtimestamp(self.when, timezone.utc).strftime("%Y-%m")
        self.attachments = self.root / hashlib.md5(self.conversation.encode()).hexdigest() / month / "Img"
        self.attachments.mkdir(parents=True)
        self.file = self.attachments / f"{self.token}_thumb.png"
        self.file.write_bytes(b"data")
        self.identity = ImageMessageIdentity(self.conversation, 17, 8, self.when, 3, timezone.utc)
        self.conn.execute("PRAGMA query_only=ON")

    def tearDown(self):
        self.conn.close()
        self.temp.cleanup()

    def test_multiple_verified_representations_of_one_message_are_preserved(self):
        self.conn.execute("PRAGMA query_only=OFF")
        self.conn.execute("INSERT INTO MessageResourceDetail VALUES ('resource-1', 5)")
        self.conn.execute("PRAGMA query_only=ON")
        alternate = self.attachments / f"{self.token}_original.png"
        alternate.write_bytes(b"image")
        bound = _bound_file(self.conn, self.identity, self.root)
        representations = bound.files
        self.assertEqual(
            [(size, path.name) for size, path in representations],
            [(4, self.file.name), (5, alternate.name)],
        )

    def test_duplicate_resource_record_fails_closed(self):
        self.conn.execute("PRAGMA query_only=OFF")
        self.conn.execute(
            "INSERT INTO MessageResourceInfo VALUES (1, 17, 8, ?, 3, 'resource-2', ?)",
            (self.when, f"<img>{self.token}</img>".encode()),
        )
        self.conn.execute("PRAGMA query_only=ON")
        with self.assertRaisesRegex(ImageUnavailable, "image_resource_ambiguous"):
            _bound_file(self.conn, self.identity, self.root)

    def test_two_files_with_same_resource_size_are_ambiguous(self):
        duplicate = self.attachments / f"{self.token}_duplicate.png"
        duplicate.write_bytes(b"data")
        with self.assertRaisesRegex(ImageUnavailable, "image_attachment_ambiguous"):
            _bound_file(self.conn, self.identity, self.root)

    def test_two_hardlinks_of_one_object_are_one_representation(self):
        duplicate=self.attachments/f'{self.token}_hardlink.png'
        os.link(self.file,duplicate)
        bound=_bound_file(self.conn,self.identity,self.root)
        self.assertEqual(len(bound.files),1)

    def test_different_inode_and_different_content_remains_ambiguous(self):
        duplicate=self.attachments/f'{self.token}_other.png'
        duplicate.write_bytes(b'else')
        with self.assertRaisesRegex(ImageUnavailable,'image_attachment_ambiguous'):
            _bound_file(self.conn,self.identity,self.root)

    def test_hardlink_plus_different_object_remains_ambiguous(self):
        os.link(self.file,self.attachments/f'{self.token}_hardlink.png')
        (self.attachments/f'{self.token}_other.png').write_bytes(b'data')
        with self.assertRaisesRegex(ImageUnavailable,'image_attachment_ambiguous'):
            _bound_file(self.conn,self.identity,self.root)

    def test_hardlink_replacement_before_open_fails_closed(self):
        duplicate=self.attachments/f'{self.token}_hardlink.png'
        os.link(self.file,duplicate)
        replacement=self.root/'replacement';replacement.write_bytes(b'evil')
        original_open=os.open
        def swap(path,*args,**kwargs):
            if kwargs.get('dir_fd') is not None and str(path).endswith('.png'):
                os.replace(replacement,self.attachments/str(path))
            return original_open(path,*args,**kwargs)
        with patch('wechat_local.image_runtime.os.open',side_effect=swap):
            with self.assertRaisesRegex(ImageUnavailable,'image_attachment_changed'):
                _bound_file(self.conn,self.identity,self.root)

    def test_hardlink_replaced_by_symlink_fails_closed(self):
        duplicate=self.attachments/f'{self.token}_hardlink.png'
        os.link(self.file,duplicate)
        original_open=os.open
        def swap(path,*args,**kwargs):
            if kwargs.get('dir_fd') is not None and str(path).endswith('.png'):
                target=self.attachments/str(path)
                target.unlink();target.symlink_to(self.root/'outside')
            return original_open(path,*args,**kwargs)
        with patch('wechat_local.image_runtime.os.open',side_effect=swap):
            with self.assertRaisesRegex(ImageUnavailable,'image_attachment_changed'):
                _bound_file(self.conn,self.identity,self.root)

    def test_hardlink_parent_swapped_to_external_symlink_fails_closed(self):
        os.link(self.file,self.attachments/f'{self.token}_hardlink.png')
        outside=self.root.parent / (self.root.name + '-outside')
        outside.mkdir()
        self.addCleanup(__import__('shutil').rmtree, outside, True)
        other=outside/self.file.name; other.write_bytes(b'evil')
        os.link(other,outside/f'{self.token}_hardlink.png')
        original=image_runtime._read_same_inode
        def swap(*args,**kwargs):
            self.attachments.rename(self.attachments.with_name('Img-original'))
            self.attachments.symlink_to(outside,target_is_directory=True)
            return original(*args,**kwargs)
        with patch.object(image_runtime,'_read_same_inode',side_effect=swap):
            with self.assertRaisesRegex(ImageUnavailable,'image_attachment_changed'):
                _bound_file(self.conn,self.identity,self.root)

    def test_hardlink_parent_replaced_with_regular_directory_fails_closed(self):
        os.link(self.file,self.attachments/f'{self.token}_hardlink.png')
        original=image_runtime._read_same_inode
        def swap(*args,**kwargs):
            self.attachments.rename(self.attachments.with_name('Img-original'))
            self.attachments.mkdir()
            other=self.attachments/self.file.name;other.write_bytes(b'evil')
            os.link(other,self.attachments/f'{self.token}_hardlink.png')
            return original(*args,**kwargs)
        with patch.object(image_runtime,'_read_same_inode',side_effect=swap):
            with self.assertRaisesRegex(ImageUnavailable,'image_attachment_changed'):
                _bound_file(self.conn,self.identity,self.root)

    def test_hardlink_parent_replaced_during_read_fails_closed(self):
        os.link(self.file,self.attachments/f'{self.token}_hardlink.png')
        original_read=os.read; changed=False
        def swap(fd,size):
            nonlocal changed
            value=original_read(fd,size)
            if not changed:
                changed=True
                self.attachments.rename(self.attachments.with_name('Img-original'))
                self.attachments.mkdir()
            return value
        with patch('wechat_local.image_runtime.os.read',side_effect=swap):
            with self.assertRaisesRegex(ImageUnavailable,'image_attachment_changed'):
                _bound_file(self.conn,self.identity,self.root)

    def test_hardlink_modified_during_read_fails_closed(self):
        os.link(self.file,self.attachments/f'{self.token}_hardlink.png')
        original_read=os.read; changed=False
        def mutate(fd,size):
            nonlocal changed
            value=original_read(fd,size)
            if not changed:
                changed=True;self.file.write_bytes(b'evil')
            return value
        with patch('wechat_local.image_runtime.os.read',side_effect=mutate):
            with self.assertRaisesRegex(ImageUnavailable,'image_attachment_changed'):
                _bound_file(self.conn,self.identity,self.root)

    @unittest.skipIf(image_runtime.Image is None, 'optional Pillow is unavailable')
    def test_hardlink_decode_consumes_verified_bytes_not_replaced_path(self):
        def png(color):
            stream=io.BytesIO();image_runtime.Image.new('RGB',(2,3),color).save(stream,format='PNG');return stream.getvalue()
        trusted=png((255,0,0));other=png((0,0,255))
        self.file.write_bytes(trusted)
        os.link(self.file,self.attachments/f'{self.token}_hardlink.png')
        self.conn.execute('PRAGMA query_only=OFF')
        self.conn.execute("UPDATE MessageResourceDetail SET size=?",(len(trusted),))
        self.conn.execute('PRAGMA query_only=ON')
        original=_bound_file
        def replace_after_bind(*args,**kwargs):
            bound=original(*args,**kwargs)
            path=bound.files[0][1];path.unlink();path.write_bytes(other)
            return bound
        with patch('wechat_local.image_runtime._bound_file',side_effect=replace_after_bind):
            result=enrich_bound_images(self.conn,self.identity,attachments_root=self.root,output_root=self.root/'decoded')
        self.assertEqual(result['status'],'decoded')
        with image_runtime.Image.open(result['representations'][0]['path']) as decoded:
            self.assertEqual(decoded.getpixel((0,0)),(255,0,0))

    @unittest.skipIf(image_runtime.Image is None, 'optional Pillow is unavailable')
    def test_single_candidate_decode_consumes_bound_bytes_after_parent_swap(self):
        def png(color):
            stream=io.BytesIO();image_runtime.Image.new('RGB',(2,3),color).save(stream,format='PNG');return stream.getvalue()
        trusted=png((255,0,0));other=png((0,0,255));self.file.write_bytes(trusted)
        self.conn.execute('PRAGMA query_only=OFF')
        self.conn.execute('UPDATE MessageResourceDetail SET size=?',(len(trusted),))
        self.conn.execute('PRAGMA query_only=ON')
        outside=self.root.parent/(self.root.name+'-outside');outside.mkdir()
        self.addCleanup(__import__('shutil').rmtree,outside,True)
        (outside/self.file.name).write_bytes(other)
        original=_bound_file
        def swap(*args,**kwargs):
            bound=original(*args,**kwargs)
            self.attachments.rename(self.attachments.with_name('Img-original'))
            self.attachments.symlink_to(outside,target_is_directory=True)
            return bound
        with patch('wechat_local.image_runtime._bound_file',side_effect=swap):
            result=enrich_bound_images(self.conn,self.identity,attachments_root=self.root,output_root=self.root/'decoded')
        self.assertEqual(result['status'],'decoded')
        with image_runtime.Image.open(result['representations'][0]['path']) as decoded:
            self.assertEqual(decoded.getpixel((0,0)),(255,0,0))

    def test_enrichment_reports_both_resource_representations_without_guessing(self):
        self.conn.execute("PRAGMA query_only=OFF")
        self.conn.execute("INSERT INTO MessageResourceDetail VALUES ('resource-1', 5)")
        self.conn.execute("PRAGMA query_only=ON")
        (self.attachments / f"{self.token}_variant.png").write_bytes(b"image")
        result = enrich_bound_images(
            self.conn,
            self.identity,
            attachments_root=self.root,
            output_root=self.root / "decoded",
        )
        self.assertEqual(result["expected_resource_sizes"], [4, 5])
        self.assertEqual(
            [item["resource_size_bytes"] for item in result["representations"]],
            [4, 5],
        )
        self.assertNotIn("original", str(result))

    def test_decoder_configuration_reaches_each_bound_representation(self):
        ffmpeg = self.root / "ffmpeg"
        ffmpeg.write_text("synthetic executable marker")
        with patch("wechat_local.image_runtime._decode_image_bytes", return_value={"status": "decoded"}) as decode:
            result = enrich_bound_images(
                self.conn, self.identity,
                attachments_root=self.root,
                output_root=self.root / "decoded",
                key_codes=(11, 12),
                account_scope="synthetic-scope",
                ffmpeg=ffmpeg,
            )
        self.assertEqual(result["status"], "decoded")
        decode.assert_called_once()
        self.assertEqual(decode.call_args.kwargs["key_codes"], (11, 12))
        self.assertEqual(decode.call_args.kwargs["account_scope"], "synthetic-scope")
        self.assertEqual(decode.call_args.kwargs["ffmpeg"], ffmpeg)

    @unittest.skipIf(image_runtime.Image is None or image_runtime.Cipher is None, "optional Pillow/cryptography extras are unavailable")
    def test_synthetic_png_v2_and_wxgf_decode_positive_paths(self):
        from cryptography.hazmat.primitives.padding import PKCS7

        image = image_runtime.Image.new("RGB", (2, 3), (17, 83, 149))
        stream = io.BytesIO()
        image.save(stream, format="PNG")
        plain = stream.getvalue()
        static_path = self.root / "synthetic.png"
        static_path.write_bytes(plain)
        static = decode_image(static_path, output_root=self.root / "png-output")
        self.assertEqual((static["status"], static["size"]), ("decoded", [2, 3]))

        code, scope = 23, "synthetic-scope"
        key = hashlib.md5((str(code) + scope).encode()).hexdigest()[:16].encode("ascii")
        padder = PKCS7(image_runtime.algorithms.AES.block_size).padder()
        padded = padder.update(plain) + padder.finalize()
        encryptor = image_runtime.Cipher(image_runtime.algorithms.AES(key), image_runtime.modes.ECB()).encryptor()
        encrypted = encryptor.update(padded) + encryptor.finalize()
        v2_path = self.root / "synthetic-v2.bin"
        v2_path.write_bytes(b"\x07\x08V2\x08\x07" + struct.pack("<II", len(plain), 0) + b"\0" + encrypted)
        v2 = decode_image(v2_path, output_root=self.root / "v2-output", key_codes=[code], account_scope=scope)
        self.assertEqual((v2["status"], v2["size"]), ("decoded", [2, 3]))

        ffmpeg = self.root / "ffmpeg"
        payload = base64.b64encode(plain).decode("ascii")
        ffmpeg.write_text(
            "#!/usr/bin/python3\nimport base64,sys\nsys.stdin.buffer.read()\n"
            f'sys.stdout.buffer.write(base64.b64decode("{payload}"))\n'
        )
        ffmpeg.chmod(0o700)
        wxgf_path = self.root / "synthetic.wxgf"
        wxgf_path.write_bytes(b"wxgf" + b"\0" * 9 + b"\x00\x00\x00\x01\x40" + b"\x00" * 16)
        hevc = decode_image(wxgf_path, output_root=self.root / "hevc-output", ffmpeg=ffmpeg)
        self.assertEqual((hevc["status"], hevc["size"], hevc["first_frame_only"]), ("decoded", [2, 3], True))

    def test_size_mismatch_and_symlink_do_not_resolve(self):
        self.conn.execute("PRAGMA query_only=OFF")
        self.conn.execute("UPDATE MessageResourceDetail SET size=5")
        self.conn.execute("PRAGMA query_only=ON")
        with self.assertRaisesRegex(ImageUnavailable, "image_attachment_missing"):
            _bound_file(self.conn, self.identity, self.root)

    def test_symlink_in_conversation_ancestor_cannot_escape_attachment_root(self):
        chat_dir = self.attachments.parents[1]
        outside = self.root.parent / (self.root.name + "-outside")
        external_folder = outside / chat_dir.name / self.attachments.parents[0].name / "Img"
        external_folder.mkdir(parents=True)
        (external_folder / f"{self.token}_outside.png").write_bytes(b"data")
        chat_dir.rename(chat_dir.with_name(chat_dir.name + "-saved"))
        chat_dir.symlink_to(outside / chat_dir.name, target_is_directory=True)
        try:
            with self.assertRaisesRegex(ImageUnavailable, "image_attachment_missing"):
                _bound_file(self.conn, self.identity, self.root)
        finally:
            chat_dir.unlink()
            chat_dir.with_name(chat_dir.name + "-saved").rename(chat_dir)
            import shutil
            shutil.rmtree(outside)
        self.conn.execute("PRAGMA query_only=OFF")
        self.conn.execute("UPDATE MessageResourceDetail SET size=4")
        self.conn.execute("PRAGMA query_only=ON")
        self.file.unlink()
        self.file.symlink_to(self.root / "missing-target")
        with self.assertRaisesRegex(ImageUnavailable, "image_attachment_missing"):
            _bound_file(self.conn, self.identity, self.root)

    def test_non_query_only_source_is_rejected(self):
        writable = sqlite3.connect(":memory:")
        with self.assertRaisesRegex(ImageUnavailable, "image_source_unavailable"):
            _bound_file(writable, self.identity, self.root)
        writable.close()

    def test_decoder_returns_controlled_unavailable_without_optional_libraries(self):
        result = decode_image(self.file, output_root=self.root / "out")
        self.assertIn(result["status"], {"decoded", "unavailable"})
        if result["status"] == "unavailable":
            self.assertIn(result["reason"], {"image_decoder_unavailable", "image_decode_unavailable"})


if __name__ == "__main__":
    unittest.main()
