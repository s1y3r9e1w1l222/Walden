from __future__ import annotations

import hashlib
import os
import subprocess
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from wechat_local.local_voice_runtime import (
    LocalVoiceRuntimeError,
    LocalWhisperTranscriber,
)


class LocalVoiceRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        (self.root / "python" / "pilk").mkdir(parents=True)
        (self.root / "whisper.cpp" / "build" / "bin").mkdir(parents=True)
        (self.root / "downloads").mkdir()
        (self.root / "python" / "pilk" / "__init__.py").write_text(
            "def decode(source, target, pcm_rate=24000):\n"
            "    assert pcm_rate == 24000\n"
            "    open(target, 'wb').write(b'\\x00\\x00' * 2400)\n",
            encoding="utf-8",
        )
        self.whisper = self.root / "whisper.cpp" / "build" / "bin" / "whisper-cli"
        self.whisper.write_text(
            "#!" + sys.executable + "\n"
            "import pathlib, sys\n"
            "if '--version' in sys.argv:\n"
            "    print('whisper.cpp version: 1.9.5-dev')\n"
            "    raise SystemExit(0)\n"
            "output = pathlib.Path(sys.argv[sys.argv.index('-of') + 1])\n"
            "output.with_suffix('.txt').write_text('synthetic local transcript\\n', encoding='utf-8')\n",
            encoding="utf-8",
        )
        os.chmod(self.whisper, 0o700)
        self.model = self.root / "downloads" / "ggml-small.bin"
        self.model.write_bytes(b"synthetic-model")
        self.decoder_origin = self.root / "decoder-origin.json"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def transcriber(self) -> LocalWhisperTranscriber:
        model_sha = hashlib.sha256(self.model.read_bytes()).hexdigest()
        with patch(
            "wechat_local.local_voice_runtime.EXPECTED_MODEL_SHA256", model_sha
        ):
            return LocalWhisperTranscriber(self.root, Path(sys.executable))

    def test_decodes_silk_and_runs_local_whisper_without_persisting_payload(self) -> None:
        model_sha = hashlib.sha256(self.model.read_bytes()).hexdigest()
        with patch(
            "wechat_local.local_voice_runtime.EXPECTED_MODEL_SHA256", model_sha
        ):
            result = self.transcriber().transcribe(
                b"\x02#!SILK_V3" + b"x" * 20
            )

        self.assertEqual(result.text, "synthetic local transcript")
        self.assertIsNone(result.confidence)
        self.assertEqual(result.recognizer_version, "whisper.cpp-1.9.5-dev")
        self.assertEqual(result.model_sha256, model_sha)
        self.assertEqual(
            sorted(path.name for path in self.root.iterdir()),
            ["downloads", "python", "whisper.cpp"],
        )

    def test_version_once_for_three_transcriptions_and_new_object_rechecks(self):
        original=subprocess.run;calls=[];sha=hashlib.sha256(self.model.read_bytes()).hexdigest()
        def run(args,*a,**kw):
            calls.append('version' if '--version' in args else ('recognizer' if '-m' in args else 'decoder'))
            return original(args,*a,**kw)
        with patch('wechat_local.local_voice_runtime.EXPECTED_MODEL_SHA256',sha),patch('wechat_local.local_voice_runtime.subprocess.run',new=run):
            transcriber=self.transcriber()
            results=[transcriber.transcribe(b'\x02#!SILK_V3'+b'x'*20) for _ in range(3)]
            self.assertEqual([r.recognizer_version for r in results],['whisper.cpp-1.9.5-dev']*3)
            self.assertEqual([r.text for r in results],['synthetic local transcript']*3)
            self.assertEqual(calls.count('version'),1);self.assertEqual(calls.count('decoder'),3);self.assertEqual(calls.count('recognizer'),3)
            self.transcriber().transcribe(b'\x02#!SILK_V3'+b'x'*20)
            self.assertEqual(calls.count('version'),2)

    def test_replaced_or_modified_recognizer_rechecks_version(self):
        original=subprocess.run;calls=[];sha=hashlib.sha256(self.model.read_bytes()).hexdigest()
        def run(args,*a,**kw):
            if '--version' in args:calls.append('version')
            return original(args,*a,**kw)
        with patch('wechat_local.local_voice_runtime.EXPECTED_MODEL_SHA256',sha),patch('wechat_local.local_voice_runtime.subprocess.run',new=run):
            tr=self.transcriber();payload=b'\x02#!SILK_V3'+b'x'*20
            self.assertEqual(tr.transcribe(payload).recognizer_version,'whisper.cpp-1.9.5-dev')
            replacement=self.whisper.with_name('replacement');replacement.write_text(self.whisper.read_text().replace('1.9.5-dev','1.9.6-dev'));replacement.chmod(0o700);os.replace(replacement,self.whisper)
            self.assertEqual(tr.transcribe(payload).recognizer_version,'whisper.cpp-1.9.6-dev')
            self.whisper.write_text(self.whisper.read_text().replace('1.9.6-dev','1.9.7-dev'))
            self.assertEqual(tr.transcribe(payload).recognizer_version,'whisper.cpp-1.9.7-dev')
            self.assertEqual(tr.transcribe(payload).recognizer_version,'whisper.cpp-1.9.7-dev')
            self.assertEqual(len(calls),3)

    def test_version_unknown_cached_for_unchanged_binary_and_refreshed_on_change(self):
        original=subprocess.run;sha=hashlib.sha256(self.model.read_bytes()).hexdigest()
        for failure in ('exception','unrecognized'):
            with self.subTest(failure=failure):
                calls=[]
                def run(args,*a,**kw):
                    if '--version' in args:
                        calls.append('version')
                        if len(calls)==1:
                            if failure=='exception':raise subprocess.CalledProcessError(1,args)
                            return subprocess.CompletedProcess(args,0,stdout=b'unrecognized version')
                    return original(args,*a,**kw)
                with patch('wechat_local.local_voice_runtime.EXPECTED_MODEL_SHA256',sha),patch('wechat_local.local_voice_runtime.subprocess.run',new=run):
                    tr=self.transcriber();payload=b'\x02#!SILK_V3'+b'x'*20
                    results=[tr.transcribe(payload) for _ in range(3)]
                    self.assertEqual([r.recognizer_version for r in results],['whisper.cpp-version-unknown']*3)
                    self.assertEqual(len(calls),1)
                    replacement=self.whisper.with_name('replacement');replacement.write_text(self.whisper.read_text());replacement.chmod(0o700);os.replace(replacement,self.whisper)
                    self.assertEqual(tr.transcribe(payload).recognizer_version,'whisper.cpp-1.9.5-dev')
                    self.assertEqual(len(calls),2)

    def test_recognizer_changed_during_version_check_does_not_cache_old_value(self):
        original=subprocess.run;calls=[];sha=hashlib.sha256(self.model.read_bytes()).hexdigest()
        def run(args,*a,**kw):
            result=original(args,*a,**kw)
            if '--version' in args:
                calls.append('version')
                if len(calls)==1:
                    replacement=self.whisper.with_name('replacement');replacement.write_text(self.whisper.read_text().replace('1.9.5-dev','1.9.6-dev'));replacement.chmod(0o700);os.replace(replacement,self.whisper)
            return result
        with patch('wechat_local.local_voice_runtime.EXPECTED_MODEL_SHA256',sha),patch('wechat_local.local_voice_runtime.subprocess.run',new=run):
            tr=self.transcriber();payload=b'\x02#!SILK_V3'+b'x'*20
            self.assertEqual(tr.transcribe(payload).recognizer_version,'whisper.cpp-version-unknown')
            self.assertEqual(tr.transcribe(payload).recognizer_version,'whisper.cpp-1.9.6-dev')
            self.assertEqual(len(calls),2)

    def test_cached_version_does_not_skip_existing_per_item_path_checks(self):
        sha=hashlib.sha256(self.model.read_bytes()).hexdigest()
        with patch('wechat_local.local_voice_runtime.EXPECTED_MODEL_SHA256',sha):
            tr=self.transcriber();payload=b'\x02#!SILK_V3'+b'x'*20
            tr.transcribe(payload);self.model.unlink()
            with self.assertRaisesRegex(LocalVoiceRuntimeError,'model_unavailable'):tr.transcribe(payload)

    def test_decoder_failure_remains_per_item_after_version_cache(self):
        original=subprocess.run;calls=[];sha=hashlib.sha256(self.model.read_bytes()).hexdigest()
        def run(args,*a,**kw):
            kind='version' if '--version' in args else ('recognizer' if '-m' in args else 'decoder');calls.append(kind)
            if kind=='decoder' and calls.count('decoder')==2:raise subprocess.CalledProcessError(1,args)
            return original(args,*a,**kw)
        with patch('wechat_local.local_voice_runtime.EXPECTED_MODEL_SHA256',sha),patch('wechat_local.local_voice_runtime.subprocess.run',new=run):
            tr=self.transcriber();payload=b'\x02#!SILK_V3'+b'x'*20
            self.assertEqual(tr.transcribe(payload).text,'synthetic local transcript')
            with self.assertRaisesRegex(LocalVoiceRuntimeError,'transcription_unavailable'):tr.transcribe(payload)
            self.assertEqual(tr.transcribe(payload).text,'synthetic local transcript')
        self.assertEqual((calls.count('version'),calls.count('decoder'),calls.count('recognizer')),(1,3,2))
        self.assertFalse(any(self.root.glob('voice-run-*')))

    def test_decoder_child_keeps_selected_venv_and_excludes_user_and_base_sites(self) -> None:
        marker = repr(str(self.decoder_origin))
        (self.root / "python" / "pilk" / "__init__.py").write_text(
            "import json, site, sys\n"
            "def decode(source, target, pcm_rate=24000):\n"
            f"    open({marker}, 'w').write(json.dumps({{'prefix': sys.prefix, 'base_prefix': sys.base_prefix, 'user_site_enabled': site.ENABLE_USER_SITE, 'paths': sys.path}}))\n"
            "    assert pcm_rate == 24000\n"
            "    open(target, 'wb').write(b'\\x00\\x00' * 2400)\n",
            encoding="utf-8",
        )
        model_sha = hashlib.sha256(self.model.read_bytes()).hexdigest()
        with patch(
            "wechat_local.local_voice_runtime.EXPECTED_MODEL_SHA256", model_sha
        ):
            result = self.transcriber().transcribe(b"\x02#!SILK_V3" + b"x" * 20)

        self.assertEqual(result.text, "synthetic local transcript")
        origin = json.loads(self.decoder_origin.read_text(encoding="utf-8"))
        self.assertEqual(origin["prefix"], sys.prefix)
        self.assertNotEqual(origin["prefix"], origin["base_prefix"])
        self.assertFalse(origin["user_site_enabled"])
        self.assertFalse(
            any(
                str(Path(path)).startswith(str(Path(origin["base_prefix"])))
                and "site-packages" in path
                for path in origin["paths"]
            )
        )

    def test_rejects_wrong_model_hash_before_reading_audio(self) -> None:
        with self.assertRaisesRegex(LocalVoiceRuntimeError, "model_unavailable"):
            LocalWhisperTranscriber(self.root, Path(sys.executable)).transcribe(
                b"\x02#!SILK_V3" + b"x" * 20
            )

    def test_rejects_non_silk_payload(self) -> None:
        with self.assertRaisesRegex(LocalVoiceRuntimeError, "audio_format_unsupported"):
            self.transcriber().transcribe(b"not-silk")


if __name__ == "__main__":
    unittest.main()
