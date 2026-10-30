"""Compose synthetic package components; this does not exercise the real CLI."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from wechat_local.reply_workflow import confirm_reply, prepare_reply, send_confirmed_once

import test_codex_first_runtime as runtime_fixtures
import test_codex_first_voice_runtime as voice_fixtures
import test_image_runtime as image_fixtures
from wechat_local.codex_first import TimeWindow


class CandidateAcceptanceFlowTests(unittest.TestCase):
    def test_synthetic_config_query_media_and_reply_components_compose(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            query = runtime_fixtures.CodexFirstRuntimeTests()
            query.setUp()
            image = image_fixtures.ImageBindingTests()
            image.setUp()
            voice = voice_fixtures.CodexFirstVoiceRuntimeTests()
            voice.setUp()
            try:
                # Give one synthetic source message an image local type, then query
                # only the fixture's fixed UTC interval through the real runtime.
                with sqlite3.connect(query.source_path) as source:
                    source.execute(
                        f"UPDATE {query.source.table_name} SET local_type=3 WHERE local_id=2"
                    )
                result = query._runtime().scan(
                    TimeWindow.from_utc(runtime_fixtures.START, runtime_fixtures.END), display_limit=50
                )
                group = result.groups[0]
                bound_message = next(item for item in group.messages if item.event_ref["content_kind"] == "image")
                self.assertEqual((group.display_name, bound_message.direction), ("Fixture Contact", "inbound"))
                self.assertEqual(bound_message.event_ref["conversation_id"], group.conversation_id)

                # Bind both resource representations to that exact synthetic
                # message identity; neither size is guessed to be the original.
                image.conn.execute("PRAGMA query_only=OFF")
                image.conn.execute(
                    "UPDATE ChatName2Id SET user_name=? WHERE rowid=1",
                    (group.conversation_id,),
                )
                image.conn.execute(
                    "UPDATE MessageResourceInfo SET message_svr_id=102, message_local_id=2, "
                    "message_create_time=1700000000 WHERE message_id='resource-1'"
                )
                image.conn.execute("INSERT INTO MessageResourceDetail VALUES ('resource-1', 5)")
                image.conn.execute("PRAGMA query_only=ON")
                import hashlib

                old_attachments = image.attachments
                month = datetime.fromtimestamp(1700000000, timezone.utc).strftime("%Y-%m")
                new_parent = image.root / hashlib.md5(group.conversation_id.encode()).hexdigest() / month / "Img"
                new_parent.mkdir(parents=True)
                for item in old_attachments.iterdir():
                    item.rename(new_parent / item.name)
                old_attachments.rmdir()
                image.attachments = new_parent
                (new_parent / f"{image.token}_alternate.png").write_bytes(b"other")
                identity = image_fixtures.ImageMessageIdentity(
                    group.conversation_id, 102, 2, 1700000000, 3, image.identity.display_timezone
                )
                bound = image_fixtures._bound_file(image.conn, identity, image.root)
                self.assertEqual([size for size, _path in bound.files], [4, 5])

                # Voice handling uses a separate real runtime synthetic fixture;
                # the unmodified base text and linked transcript remain distinct.
                voice_result = voice.runtime().scan(
                    TimeWindow.from_utc(voice_fixtures.START, voice_fixtures.END), display_limit=50
                )
                voice_message = next(
                    item for item in voice_result.groups[0].messages
                    if item.event_ref.get("voice", {}).get("status") == "native_transcript"
                )
                self.assertEqual(voice_message.event_ref["voice"]["transcript"]["text"], "native result")
                self.assertEqual(voice_message.text, "audio")

                pending = prepare_reply(group.conversation_id, group.display_name, "合成回复")
                confirmed = confirm_reply(
                    pending,
                    target_id=group.conversation_id,
                    target_label=group.display_name,
                    body="合成回复",
                    confirmed=True,
                )
                calls = []

                class Result:
                    returncode = 0
                    stdout = ""
                    stderr = ""

                def substitute(args, **_kwargs):
                    calls.append(args)
                    return Result()

                sent = send_confirmed_once(
                    confirmed,
                    state_dir=root / "reply-state",
                    runner=substitute,
                    command_prefix=("midscene-substitute",),
                )
                self.assertEqual(sent["status"], "sent_verified")
                self.assertEqual([call[1] for call in calls], ["connect", "act", "assert"])
            finally:
                voice.tearDown()
                image.tearDown()
                query.tearDown()


if __name__ == "__main__":
    unittest.main()
