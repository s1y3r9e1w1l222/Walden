import json
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from wechat_local.reply_workflow import (
    confirm_reply,
    prepare_reply,
    reply_cli_main,
    send_confirmed_once,
)


class ReplyWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.state_dir = Path(self.temp.name) / "state"
        self.binding_patch=patch('wechat_local.reply_workflow.verify_reply_sources', return_value='synthetic-binding')
        self.binding_patch.start()
        self.pending = prepare_reply(
            target_id="synthetic-contact-id",
            target_label="Synthetic contact",
            body="合成回复正文",
            source_config='/synthetic/config', source_event_id='synthetic-event',
        )

    def tearDown(self):
        self.binding_patch.stop()
        self.temp.cleanup()

    def test_confirmation_must_match_exact_target_and_body(self):
        self.assertIsNone(
            confirm_reply(self.pending, target_id=self.pending.target_id,
                          target_label=self.pending.target_label, body=self.pending.body,
                          confirmed=False)
        )
        self.assertIsNone(
            confirm_reply(self.pending, target_id="another-contact",
                          target_label=self.pending.target_label, body=self.pending.body,
                          confirmed=True)
        )

    def test_unconfirmed_draft_never_calls_midscene(self):
        calls = []
        result = send_confirmed_once(
            None,
            state_dir=self.state_dir,
            runner=lambda args, **kwargs: calls.append(args),
            command_prefix=("midscene-substitute",),
        )
        self.assertEqual(result["status"], "awaiting_confirmation")
        self.assertEqual(calls, [])
        self.assertIsNone(
            confirm_reply(self.pending, target_id=self.pending.target_id,
                          target_label=self.pending.target_label, body="changed body",
                          confirmed=True)
        )

    def test_confirmed_reply_runs_connect_once_act_once_then_assert(self):
        approved = confirm_reply(
            self.pending,
            target_id=self.pending.target_id,
            target_label=self.pending.target_label,
            body=self.pending.body,
            confirmed=True,
        )
        calls = []

        def runner(args, **kwargs):
            calls.append(args)
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        result = send_confirmed_once(
            approved, state_dir=self.state_dir, runner=runner,
            command_prefix=("midscene-substitute",),
        )
        self.assertEqual(result["status"], "sent_verified")
        self.assertEqual([call[1] for call in calls], ["connect", "act", "assert"])
        act_prompt = calls[1][calls[1].index("--prompt") + 1]
        self.assertIn(json.dumps(self.pending.target_label, ensure_ascii=False), act_prompt)
        self.assertIn(json.dumps(self.pending.body, ensure_ascii=False), act_prompt)

    def test_unknown_send_outcome_is_persisted_and_never_retried(self):
        approved = confirm_reply(
            self.pending,
            target_id=self.pending.target_id,
            target_label=self.pending.target_label,
            body=self.pending.body,
            confirmed=True,
        )
        calls = []

        def runner(args, **kwargs):
            calls.append(args)
            return SimpleNamespace(returncode=0 if args[1] == "connect" else 1,
                                   stdout="", stderr="")

        first = send_confirmed_once(approved, state_dir=self.state_dir,
                                    runner=runner, command_prefix=("midscene-substitute",))
        second = send_confirmed_once(approved, state_dir=self.state_dir,
                                     runner=runner, command_prefix=("midscene-substitute",))
        self.assertEqual(first["status"], "outcome_unknown")
        self.assertEqual(second["status"], "outcome_unknown")
        self.assertEqual([call[1] for call in calls], ["connect", "act"])

    def test_target_or_body_change_requires_new_confirmation(self):
        changed = prepare_reply("synthetic-contact-id", "Synthetic contact", "新正文",source_config='/synthetic/config',source_event_id='synthetic-event')
        self.assertNotEqual(changed.operation_id, self.pending.operation_id)
        self.assertNotEqual(changed.body, self.pending.body)

    def test_missing_source_and_old_pending_cannot_send(self):
        from wechat_local.reply_workflow import PendingReply
        from wechat_local.reply_source_binding import ReplySourceError
        with self.assertRaisesRegex(ReplySourceError,'source_binding_required'):
            prepare_reply('target','label','body')
        old=PendingReply('0'*32,'target','label','body')
        with self.assertRaisesRegex(ReplySourceError,'source_binding_required'):
            confirm_reply(old,target_id='target',target_label='label',body='body',confirmed=True)

    def test_binding_changes_after_connect_stops_before_action(self):
        approved=confirm_reply(self.pending,target_id=self.pending.target_id,target_label=self.pending.target_label,
                               body=self.pending.body,confirmed=True)
        calls=[]
        def runner(args, **kwargs):
            calls.append(args)
            return SimpleNamespace(returncode=0,stdout='',stderr='')
        with patch('wechat_local.reply_workflow.verify_reply_sources',side_effect=['synthetic-binding','changed']):
            result=send_confirmed_once(approved,state_dir=self.state_dir,runner=runner,command_prefix=('synthetic',))
        self.assertEqual(result,{'status':'not_started','reason':'source_binding_unavailable'})
        self.assertEqual([call[1] for call in calls],['connect'])

    def test_old_pending_file_requires_reprepare_and_is_preserved(self):
        self.state_dir.mkdir(mode=0o700)
        old_id='a'*32
        path=self.state_dir/(old_id+'.pending.json')
        from wechat_local.reply_workflow import _pending_path
        path=_pending_path(self.state_dir,old_id)
        old={'operation_id':old_id,'confirmation_digest':'old'}
        path.write_text(json.dumps(old))
        out=io.StringIO()
        with redirect_stdout(out):
            code=reply_cli_main(['confirm','--state-dir',str(self.state_dir),'--operation-id',old_id,
                '--target-id','target','--target-label','label','--body','body','--confirmed','--json'])
        self.assertEqual(code,2)
        self.assertEqual(json.loads(out.getvalue())['reason'],'source_binding_required')
        self.assertEqual(json.loads(path.read_text()),old)

    def test_reply_cli_requires_exact_confirmation_and_uses_midscene_prefix(self):
        calls = []

        class Result:
            returncode = 0
            stdout = ""
            stderr = ""

        def runner(args, **kwargs):
            calls.append(args)
            return Result()

        output = io.StringIO()
        with redirect_stdout(output):
            prepared = reply_cli_main([
                "prepare", "--state-dir", str(self.state_dir),
                "--source-config", "/synthetic/config", "--source-event-id", "synthetic-event",
                "--target-id", "synthetic-target", "--target-label", "Synthetic target",
                "--body", "synthetic body", "--json",
            ], runner=runner)
        self.assertEqual(prepared, 0)
        self.assertEqual(calls, [])
        pending = json.loads(output.getvalue())
        self.assertEqual(pending["status"], "awaiting_confirmation")

        output = io.StringIO()
        with redirect_stdout(output):
            mismatch = reply_cli_main([
                "confirm", "--state-dir", str(self.state_dir),
                "--operation-id", pending["operation_id"],
                "--target-id", "synthetic-target", "--target-label", "Synthetic target",
                "--body", "changed body", "--confirmed", "--json",
            ], runner=runner)
        self.assertEqual(mismatch, 2)
        self.assertEqual(json.loads(output.getvalue())["reason"], "confirmation_binding_invalid")
        self.assertEqual(calls, [])

        output = io.StringIO()
        with redirect_stdout(output):
            confirmed = reply_cli_main([
                "confirm", "--state-dir", str(self.state_dir),
                "--operation-id", pending["operation_id"],
                "--target-id", "synthetic-target", "--target-label", "Synthetic target",
                "--body", "synthetic body", "--confirmed", "--json",
            ], runner=runner)
        self.assertEqual(confirmed, 0)
        self.assertEqual([call[3] for call in calls], ["connect", "act", "assert"])
        self.assertIn('"synthetic body"', calls[1][calls[1].index("--prompt") + 1])


if __name__ == "__main__":
    unittest.main()
