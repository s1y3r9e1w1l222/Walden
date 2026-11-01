"""The 0.4 distribution excludes optional relationship-context interfaces."""
from pathlib import Path
import inspect
import unittest

ROOT = Path(__file__).resolve().parents[1]


class ReleaseSplitTests(unittest.TestCase):
    def test_context_modules_and_console_are_absent(self):
        for name in ('reply_context_local.py', 'reply_context_options.py'):
            self.assertFalse((ROOT / 'src/wechat_local' / name).exists())
        self.assertEqual(list((ROOT / 'tests').glob('test_reply_context*.py')), [])
        self.assertNotIn('wechat-local-reply-context', (ROOT / 'pyproject.toml').read_text())

    def test_dual_skill_has_no_context_contract(self):
        a = (ROOT / 'skills/wechat-local-query/SKILL.md').read_bytes()
        b = (ROOT / 'src/wechat_local/resources/skills/wechat-local-query/SKILL.md').read_bytes()
        self.assertEqual(a, b)
        self.assertNotIn(b'## Optional private reply context', a)
        self.assertNotIn(b'wechat-local-reply-context', a)

    def test_normal_reply_keeps_shared_source_verifier(self):
        from wechat_local import reply_source_binding, reply_workflow
        self.assertIs(reply_workflow.verify_reply_sources, reply_source_binding.verify_reply_sources)
        self.assertEqual(tuple(inspect.signature(reply_source_binding.verify_reply_sources).parameters),
                         ('config_path', 'event_ids', 'target_id', 'target_label'))

    def test_verification_closes_all_owned_connections_after_close_failure(self):
        from wechat_local.reply_source_binding import _ReplySourceSession
        closed = []
        class Connection:
            def __init__(self, name, fail=False): self.name, self.fail = name, fail
            def close(self):
                closed.append(self.name)
                if self.fail: raise ValueError('synthetic close failure')
        session = _ReplySourceSession()
        session.connections = {'one': (Connection('one', True), None, None),
                               'two': (Connection('two'), None, None)}
        session.db = Connection('store')
        with self.assertRaisesRegex(ValueError, 'synthetic close failure'):
            session.close()
        self.assertEqual(closed, ['one', 'two', 'store'])
