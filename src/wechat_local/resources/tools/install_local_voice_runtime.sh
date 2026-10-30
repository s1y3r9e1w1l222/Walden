#!/bin/sh
set -eu

if [ "$#" -ne 2 ]; then
  echo "usage: install_local_voice_runtime.sh /absolute/venv/bin/python /absolute/empty/runtime-root" >&2
  exit 2
fi

PYTHON=$1
RUNTIME_ROOT=$2
case "$PYTHON" in /*) ;; *) echo "Python path must be absolute" >&2; exit 2 ;; esac
case "$RUNTIME_ROOT" in /*) ;; *) echo "Runtime root must be absolute" >&2; exit 2 ;; esac
test -x "$PYTHON" || { echo "Python executable is unavailable" >&2; exit 1; }
test ! -e "$RUNTIME_ROOT" || { echo "Runtime root exists; refusing to overwrite" >&2; exit 1; }
command -v curl >/dev/null
command -v shasum >/dev/null
command -v tar >/dev/null
command -v xcrun >/dev/null
xcrun --find clang >/dev/null

PILK_SHA256=d4a1bcf93dc6ef5e95e0cfd728ed4ef4d49f9c0476d70816fecbe456cc762e7f
TYPING_EXTENSIONS_SHA256=481caa481374e813c1b176ada14e97f1f67a4539ce9cfeb3f350d78d6370c2e8
WHISPER_SHA256=ff1a9053feb509ff9d7729703355541ae9690073a6b1c40eb692c962e0dc1720
MODEL_SHA256=1be3a9b2063867b937e64e2ec7483364a79917e157fa98c5d94b5c1fffea987b
WORK=$(mktemp -d "${TMPDIR:-/tmp}/wechat-voice-install.XXXXXX")
trap 'rm -rf "$WORK"' EXIT HUP INT TERM

mkdir -p "$(dirname "$RUNTIME_ROOT")"
"$PYTHON" -m pip install 'cmake==3.31.6' 'setuptools==58.0.4' 'wheel==0.45.1'
mkdir -p "$RUNTIME_ROOT/python" "$RUNTIME_ROOT/downloads"
curl --fail --location --silent --show-error \
  'https://files.pythonhosted.org/packages/49/d3/b8441a820a491ddfc024b0b0cf0393375b75ea13866d9c66727e54c2fc80/typing_extensions-4.16.0-py3-none-any.whl' \
  --output "$WORK/typing_extensions-4.16.0-py3-none-any.whl"
printf '%s  %s\n' "$TYPING_EXTENSIONS_SHA256" "$WORK/typing_extensions-4.16.0-py3-none-any.whl" | shasum -a 256 -c -
"$PYTHON" -m pip install --no-deps --target "$RUNTIME_ROOT/python" "$WORK/typing_extensions-4.16.0-py3-none-any.whl"

curl --fail --location --silent --show-error \
  'https://files.pythonhosted.org/packages/66/bb/938dd697b6bc2d851ffec4ffe82ed20078e58bd0ef049e84f4d038d6f991/pilk-0.2.4.tar.gz' \
  --output "$WORK/pilk-0.2.4.tar.gz"
printf '%s  %s\n' "$PILK_SHA256" "$WORK/pilk-0.2.4.tar.gz" | shasum -a 256 -c -
"$PYTHON" -m pip install --no-deps --no-build-isolation --target "$RUNTIME_ROOT/python" "$WORK/pilk-0.2.4.tar.gz"

curl --fail --location --silent --show-error \
  'https://github.com/ggml-org/whisper.cpp/archive/refs/tags/v1.9.5.tar.gz' \
  --output "$WORK/whisper.cpp-v1.9.5.tar.gz"
printf '%s  %s\n' "$WHISPER_SHA256" "$WORK/whisper.cpp-v1.9.5.tar.gz" | shasum -a 256 -c -
mkdir -p "$RUNTIME_ROOT/whisper.cpp"
tar -xzf "$WORK/whisper.cpp-v1.9.5.tar.gz" --strip-components=1 -C "$RUNTIME_ROOT/whisper.cpp"
"$(dirname "$PYTHON")/cmake" --version >/dev/null
"$PYTHON" -c 'import sysconfig; print(sysconfig.get_platform())'
"$(dirname "$PYTHON")/cmake" -S "$RUNTIME_ROOT/whisper.cpp" -B "$RUNTIME_ROOT/whisper.cpp/build" \
  -DCMAKE_BUILD_TYPE=Release -DWHISPER_BUILD_TESTS=OFF -DWHISPER_BUILD_EXAMPLES=ON \
  -DGGML_METAL=OFF -DGGML_OPENMP=OFF
"$(dirname "$PYTHON")/cmake" --build "$RUNTIME_ROOT/whisper.cpp/build" --config Release --parallel

curl --fail --location --silent --show-error \
  'https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-small.bin' \
  --output "$RUNTIME_ROOT/downloads/ggml-small.bin"
printf '%s  %s\n' "$MODEL_SHA256" "$RUNTIME_ROOT/downloads/ggml-small.bin" | shasum -a 256 -c -

"$PYTHON" - "$RUNTIME_ROOT" <<'PY'
import importlib.metadata as md
import sys
from pathlib import Path

root = Path(sys.argv[1])
sys.path.insert(0, str(root / "python"))
import pilk

assert md.version("typing-extensions") == "4.16.0"
assert pilk.__version__ == "0.2.4"
assert (root / "whisper.cpp/build/bin/whisper-cli").is_file()
assert (root / "downloads/ggml-small.bin").is_file()
print("Voice runtime files and Python modules are present.")
PY
"$RUNTIME_ROOT/whisper.cpp/build/bin/whisper-cli" --version
echo "Local voice runtime installed at: $RUNTIME_ROOT"
