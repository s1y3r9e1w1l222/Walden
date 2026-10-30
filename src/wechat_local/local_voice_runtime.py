"""Narrow, local-only SILK to whisper.cpp transcription adapter."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
import tempfile
import wave
from pathlib import Path

from .voice_transcript import LocalTranscription


EXPECTED_MODEL_SHA256 = (
    "1be3a9b2063867b937e64e2ec7483364a79917e157fa98c5d94b5c1fffea987b"
)
MAX_VOICE_BYTES = 16 * 1024 * 1024
MAX_TRANSCRIPT_BYTES = 1024 * 1024
TRANSCRIPTION_TIMEOUT_SECONDS = 300


class LocalVoiceRuntimeError(RuntimeError):
    """Controlled local-runtime failure with no filesystem details."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _contained_file(root: Path, candidate: Path, code: str) -> Path:
    if candidate.is_symlink() or not candidate.is_file():
        raise LocalVoiceRuntimeError(code)
    resolved = candidate.resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise LocalVoiceRuntimeError(code) from exc
    return resolved


class LocalWhisperTranscriber:
    """Run the approved private pilk + whisper.cpp runtime synchronously."""

    def __init__(self, runtime_root: Path, python_executable: Path) -> None:
        self.runtime_root_input = Path(runtime_root)
        self.python_executable_input = Path(python_executable)
        self._model_verified = False
        self._recognizer_version_identity: tuple | None = None

    def _prepare(self) -> None:
        root_input = self.runtime_root_input
        if root_input.is_symlink() or not root_input.is_dir():
            raise LocalVoiceRuntimeError("runtime_unavailable")
        self.runtime_root = root_input.resolve()
        python_path = self.python_executable_input
        if python_path.resolve().is_relative_to(self.runtime_root):
            self.python_executable = _contained_file(
                self.runtime_root, python_path, "decoder_unavailable"
            )
        else:
            self.python_executable = self._external_python(python_path)
        self.pilk_root = self.runtime_root / "python"
        if self.pilk_root.is_symlink() or not (self.pilk_root / "pilk").is_dir():
            raise LocalVoiceRuntimeError("decoder_unavailable")
        self.whisper = _contained_file(
            self.runtime_root,
            self.runtime_root / "whisper.cpp" / "build" / "bin" / "whisper-cli",
            "whisper_unavailable",
        )
        self.model = _contained_file(
            self.runtime_root,
            self.runtime_root / "downloads" / "ggml-small.bin",
            "model_unavailable",
        )
        def recognizer_identity() -> tuple | None:
            try:
                info = self.whisper.stat()
                return (str(self.whisper), info.st_dev, info.st_ino, info.st_mode,
                        info.st_size, info.st_mtime_ns, info.st_ctime_ns)
            except OSError:
                return None

        before = recognizer_identity()
        if before is not None and before == self._recognizer_version_identity:
            return
        self._recognizer_version_identity = None
        whisper_environment = {
            "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
            "DYLD_LIBRARY_PATH": str(self.whisper.parent),
        }
        try:
            version = subprocess.run(
                [str(self.whisper), "--version"],
                env=whisper_environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                check=True,
                timeout=10,
            ).stdout.decode("utf-8", errors="replace")
        except (OSError, subprocess.SubprocessError):
            self.recognizer_version = "whisper.cpp-version-unknown"
        else:
            match = re.search(r"whisper\.cpp version:\s*([A-Za-z0-9.+_-]+)", version)
            self.recognizer_version = (
                f"whisper.cpp-{match.group(1)}" if match else "whisper.cpp-version-unknown"
            )
        after = recognizer_identity()
        if before is not None and before == after:
            self._recognizer_version_identity = before
        else:
            self.recognizer_version = "whisper.cpp-version-unknown"

    @staticmethod
    def _external_python(path: Path) -> Path:
        # Keep the caller-selected venv entrypoint: resolving its symlink can
        # bypass pyvenv.cfg and start the base interpreter instead.
        selected = Path(os.path.abspath(path))
        try:
            target = selected.resolve(strict=True)
        except OSError as exc:
            raise LocalVoiceRuntimeError("decoder_unavailable") from exc
        if not target.is_file() or not os.access(selected, os.X_OK):
            raise LocalVoiceRuntimeError("decoder_unavailable")
        return selected

    def transcribe(self, payload: bytes) -> LocalTranscription:
        if not isinstance(payload, bytes) or not payload or len(payload) > MAX_VOICE_BYTES:
            raise LocalVoiceRuntimeError("audio_payload_unavailable")
        if not (
            payload.startswith(b"#!SILK_V3")
            or (len(payload) > 1 and payload[1:].startswith(b"#!SILK_V3"))
        ):
            raise LocalVoiceRuntimeError("audio_format_unsupported")
        self._prepare()
        if not self._model_verified:
            if _sha256_file(self.model) != EXPECTED_MODEL_SHA256:
                raise LocalVoiceRuntimeError("model_unavailable")
            self._model_verified = True

        run_dir = Path(tempfile.mkdtemp(prefix="voice-run-", dir=self.runtime_root))
        os.chmod(run_dir, 0o700)
        try:
            silk_path = run_dir / "source.silk"
            pcm_path = run_dir / "source.pcm"
            wav_path = run_dir / "source.wav"
            output_base = run_dir / "transcript"
            transcript_path = run_dir / "transcript.txt"
            silk_path.write_bytes(payload)
            os.chmod(silk_path, 0o600)
            decoder_code = (
                "import sys; sys.path.insert(0, sys.argv[1]); "
                "import pilk; pilk.decode(sys.argv[2], sys.argv[3], pcm_rate=24000)"
            )
            environment = {
                "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
            }
            subprocess.run(
                [
                    str(self.python_executable),
                    "-I",
                    "-c",
                    decoder_code,
                    str(self.pilk_root),
                    str(silk_path),
                    str(pcm_path),
                ],
                env=environment,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
                timeout=TRANSCRIPTION_TIMEOUT_SECONDS,
            )
            pcm = pcm_path.read_bytes()
            if not pcm or len(pcm) % 2:
                raise LocalVoiceRuntimeError("decode_unavailable")
            with wave.open(str(wav_path), "wb") as output:
                output.setparams((1, 2, 24000, 0, "NONE", "NONE"))
                output.writeframes(pcm)
            for path in (pcm_path, wav_path):
                os.chmod(path, 0o600)
            whisper_environment = {
                "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
                "DYLD_LIBRARY_PATH": str(self.whisper.parent),
            }
            subprocess.run(
                [
                    str(self.whisper),
                    "-m",
                    str(self.model),
                    "-f",
                    str(wav_path),
                    "-l",
                    "auto",
                    "-otxt",
                    "-of",
                    str(output_base),
                    "-np",
                    "-nt",
                    "-ng",
                ],
                env=whisper_environment,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
                timeout=TRANSCRIPTION_TIMEOUT_SECONDS,
            )
            if (
                transcript_path.is_symlink()
                or not transcript_path.is_file()
                or transcript_path.stat().st_size > MAX_TRANSCRIPT_BYTES
            ):
                raise LocalVoiceRuntimeError("transcription_unavailable")
            text = transcript_path.read_text(encoding="utf-8").strip()
            if not text:
                raise LocalVoiceRuntimeError("transcription_unavailable")
            return LocalTranscription(
                text,
                confidence=None,
                recognizer_version=self.recognizer_version,
                model_sha256=EXPECTED_MODEL_SHA256,
            )
        except LocalVoiceRuntimeError:
            raise
        except (OSError, subprocess.SubprocessError, UnicodeError, wave.Error) as exc:
            raise LocalVoiceRuntimeError("transcription_unavailable") from exc
        finally:
            shutil.rmtree(run_dir, ignore_errors=True)
