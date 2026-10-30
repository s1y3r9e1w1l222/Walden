# Optional local voice runtime

This installs only third-party local ASR dependencies. The package wheel does not include `pilk`, whisper.cpp binaries/libraries, or the model. It never obtains a WeChat key, opens a WeChat database, starts a client, or sends a message.

## Requirements and compatibility

Build the runtime on the machine that will run it, with the dependencies and pinned versions described below. Compatibility across OS and CPU combinations is not guaranteed. The packaged credential script is x86_64-specific; this guide does not provide or promise a way to obtain a debuggable WeChat copy.

The whisper.cpp release archive had SHA-256 `ff1a9053feb509ff9d7729703355541ae9690073a6b1c40eb692c962e0dc1720`. Its build from an archive (without `.git`) reports `whisper.cpp version: 1.9.5-dev`; the query result reads this version from the actual binary. The downloaded model came from [ggerganov/whisper.cpp on Hugging Face](https://huggingface.co/ggerganov/whisper.cpp), whose model card declares MIT; `ggml-small.bin` is pinned by SHA-256 `1be3a9b2063867b937e64e2ec7483364a79917e157fa98c5d94b5c1fffea987b` (466 MiB). The download URL is `https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-small.bin`; content hash verification pins the exact file regardless of the mutable branch name.

`pilk` 0.2.4 is a separate GPL-3.0 third-party package from [PyPI](https://pypi.org/project/pilk/0.2.4/) / [foyoux/pilk](https://github.com/foyoux/pilk). Its source archive SHA-256 is `d4a1bcf93dc6ef5e95e0cfd728ed4ef4d49f9c0476d70816fecbe456cc762e7f`; the sdist carries its `LICENSE` and declares `typing-extensions` as a runtime dependency. `typing-extensions` 4.16.0 is fetched from its exact PyPI wheel URL and pinned by SHA-256 `481caa481374e813c1b176ada14e97f1f67a4539ce9cfeb3f350d78d6370c2e8`; its wheel metadata declares PSF-2.0. The install helper builds pilk locally and does not redistribute its source or compiled extension in this wheel. The wheel's own notices do not change the separate licenses of downloaded tools or model.

## Build the local runtime

Requirements: an existing isolated Python virtual environment with pip, network access to PyPI, GitHub and Hugging Face, Xcode Command Line Tools (`clang`, `make`), `curl`, `tar`, and `shasum`. The helper writes only to a new runtime root and refuses to overwrite one. It installs the pinned CMake, setuptools and wheel build tools into the supplied venv, builds pilk from its hash-checked sdist, builds whisper.cpp locally, then downloads and hash-checks the model.

Set `APP_HOME` to the same application-data directory used by the installed source profile. For a setup-created profile whose `event_store` is `<app-home>/events.sqlite`, its parent is `<app-home>` and the documented runtime is `<app-home>/voice-runtime`. The default follows `XDG_DATA_HOME`; for a custom profile, set `WECHAT_LOCAL_APP_HOME` to that profile's application-data directory and `WECHAT_LOCAL_PYTHON` to the exact absolute `runtime_python` recorded in `current-source.json`. These variables only select the documented commands; they do not change the user's profile or Python environment. If the paths in the profile do not match, stop and correct the local path references before installing. Do not replace an existing helper or runtime.

```sh
DATA_HOME="${XDG_DATA_HOME:-$HOME/.local/share}"
APP_HOME="${WECHAT_LOCAL_APP_HOME:-$DATA_HOME/wechat-local}"
APP_PYTHON="${WECHAT_LOCAL_PYTHON:-$APP_HOME/venv/bin/python}"
VOICE_SETUP="$APP_HOME/install_local_voice_runtime.sh"
export APP_HOME APP_PYTHON
test -x "$APP_PYTHON" || { echo "Configured runtime_python is unavailable" >&2; exit 1; }
mkdir -p "$APP_HOME"
test ! -e "$VOICE_SETUP" || { echo "Refusing to overwrite helper" >&2; exit 1; }
"$APP_PYTHON" - <<'PY'
from importlib.resources import files
import os
from pathlib import Path
p=Path(os.environ["APP_HOME"])/"install_local_voice_runtime.sh"
p.write_bytes(files("wechat_local").joinpath("resources/tools/install_local_voice_runtime.sh").read_bytes())
p.chmod(0o700)
print(p.resolve())
PY
"$VOICE_SETUP" "$APP_PYTHON" "$APP_HOME/voice-runtime"
```

The helper outputs the runtime location and whisper.cpp binary version. It uses this layout, which matches `LocalWhisperTranscriber`:

```text
voice-runtime/
  python/pilk/                 # locally built pilk extension
  whisper.cpp/build/bin/       # whisper-cli and its dylibs
  downloads/ggml-small.bin     # hash-pinned model
```

`typing-extensions` is installed beside `pilk` under `python/`. The product invokes the configured Python in isolated mode, explicitly adds that runtime's `python/` directory to the child import path, and disables user-site and environment-based Python path injection. CMake and the wheel builder are build tools in the supplied venv. The model and voice data remain local; temporary decoded audio is created with restrictive permissions and removed after transcription.

To request local voice enrichment, use the configured absolute `window_cli` entry with `--source-config <current-source.json>`, `--voice-transcripts`, `--voice-jsonl`, `--voice-runtime-root "$APP_HOME/voice-runtime"`, and `--voice-python "$APP_PYTHON"`. The profile supplies its discovered voice databases; do not combine `--source-config` with explicit `--voice-database` arguments. Resolve `window_cli` from the same `current-source.json` profile; do not assume shell activation or a default HOME path in another terminal. The query validates the configuration and message shards in the same invocation; `--check-config` is an optional separate diagnostic. Profile voice databases must be under the configured live data root and are opened read-only with the same verified passphrase file. Any missing or wrong-key shard reports `voice_source_unavailable`; it must not be silently dropped before ASR.

Local voice transcription depends on the configured runtime, supported audio and the available model. Recognition accuracy, complete private-chat coverage and compatibility in every environment are not guaranteed.

## License/source notes

- Candidate source license and included adapted SQLCipher/WCDB notice remain in `THIRD_PARTY_NOTICES.md` and `licenses/`.
- `pilk` is a separately obtained GPL-3.0 dependency; see its source distribution's `LICENSE` and PyPI metadata. Its sdist includes that license; the downloaded source and binary are not part of this wheel.
- whisper.cpp is MIT-licensed upstream; the compiled executable/libraries are separately built from the versioned source archive and are not part of this wheel.
- The `ggml-small.bin` model card declares MIT. It is downloaded separately and is not part of this wheel.
- CMake is a build-time PyPI dependency and is not part of the candidate wheel.
