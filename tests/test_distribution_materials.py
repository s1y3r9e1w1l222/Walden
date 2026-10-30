from importlib.resources import files
from pathlib import Path
import unittest


class DistributionMaterialsTests(unittest.TestCase):
    def test_wheel_resource_is_the_existing_capture_script(self) -> None:
        installed_resource = files("wechat_local").joinpath(
            "resources/tools/render_lldb_capture.py"
        )
        source_script = Path(__file__).parents[1] / "tools" / "render_lldb_capture.py"

        self.assertTrue(installed_resource.is_file())
        self.assertEqual(installed_resource.read_bytes(), source_script.read_bytes())

    def test_voice_runtime_materials_are_shipped_as_exact_resources(self) -> None:
        root = Path(__file__).parents[1]
        for relative in (
            "docs/VOICE_RUNTIME.md",
            "tools/install_local_voice_runtime.sh",
        ):
            installed = files("wechat_local").joinpath(f"resources/{relative}")
            source = root / "src/wechat_local/resources" / relative
            self.assertTrue(installed.is_file(), relative)
            self.assertEqual(installed.read_bytes(), source.read_bytes(), relative)
        helper = root / "tools/install_local_voice_runtime.sh"
        self.assertEqual(
            helper.read_bytes(),
            (root / "src/wechat_local/resources/tools/install_local_voice_runtime.sh").read_bytes(),
        )

    def test_voice_runtime_uses_one_xdg_or_explicit_profile_root(self) -> None:
        root = Path(__file__).parents[1]
        guide = (root / "src/wechat_local/resources/docs/VOICE_RUNTIME.md").read_text(
            encoding="utf-8"
        )
        self.assertIn('APP_HOME="${WECHAT_LOCAL_APP_HOME:-$DATA_HOME/wechat-local}"', guide)
        self.assertIn('APP_PYTHON="${WECHAT_LOCAL_PYTHON:-$APP_HOME/venv/bin/python}"', guide)
        self.assertIn('Path(os.environ["APP_HOME"])', guide)
        self.assertIn('"$APP_HOME/voice-runtime"', guide)
        self.assertNotIn('$HOME/.local/share/wechat-local/', guide)
        self.assertNotIn("PYTHONPATH", guide)


if __name__ == "__main__":
    unittest.main()
