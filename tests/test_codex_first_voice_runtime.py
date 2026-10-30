from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime, timezone
from pathlib import Path

from wechat_local.agent_store import AgentStore
from wechat_local.codex_first import QueryRunAnchor, TimeWindow
from wechat_local.codex_first_runtime import (
    CodexFirstRuntime,
    SourceCapabilityEvidence,
)
from wechat_local.event_store import EventStore, RawMessage, SourceKey, WindowIngestRun
from wechat_local.live_catalog import LiveCatalog
from wechat_local.live_identity import LiveIdentityAdapter
from wechat_local.voice_transcript import (
    LocalTranscription,
    VoiceTranscriptEnricher,
)


GROUP = "synthetic-runtime-group@chatroom"
TABLE = "Msg_" + hashlib.md5(GROUP.encode("utf-8")).hexdigest()
START = datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc)
END = datetime(2023, 11, 14, 22, 15, 0, tzinfo=timezone.utc)


def _field(number: int, value: bytes) -> bytes:
    return bytes([(number << 3) | 2, len(value)]) + value


def _native(value: str) -> bytes:
    return _field(5, _field(2, value.encode("utf-8")))


def _query_only(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA query_only=ON")
    return connection


class _RecordingTranscriber:
    def __init__(self) -> None:
        self.calls: list[bytes] = []

    def transcribe(self, payload: bytes) -> LocalTranscription:
        self.calls.append(payload)
        return LocalTranscription("synthetic local transcript")


class CodexFirstVoiceRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.message_root = self.root / "live" / "message"
        self.message_root.mkdir(parents=True)
        self.message_path = self.message_root / "message_0.db"
        with sqlite3.connect(self.message_path) as source:
            source.execute("CREATE TABLE Name2Id (user_name TEXT)")
            source.execute(
                "INSERT INTO Name2Id(rowid, user_name) VALUES (7, ?)", (GROUP,)
            )
            source.execute(
                f"""
                CREATE TABLE {TABLE} (
                    local_id INTEGER PRIMARY KEY,
                    server_id INTEGER,
                    local_type INTEGER NOT NULL,
                    create_time INTEGER,
                    real_sender_id INTEGER,
                    message_content BLOB,
                    WCDB_CT_message_content INTEGER,
                    packed_info_data BLOB
                )
                """
            )
            source.executemany(
                f"INSERT INTO {TABLE} VALUES (?, ?, ?, ?, 9, ?, 0, ?)",
                (
                    (1, 101, 34, 1_700_000_000, None, _field(1, b"abcde")),
                    (2, 102, 1, 1_700_000_010, "text remains readable", None),
                    (3, 103, 34, 1_700_000_020, None, _native("native result")),
                ),
            )
        self.voice_path = self.root / "voice.db"
        self.silk = b"\x02#!SILK_V3" + b"x" * 32
        with sqlite3.connect(self.voice_path) as voice:
            voice.execute("CREATE TABLE Name2Id (user_name TEXT)")
            voice.execute(
                "INSERT INTO Name2Id(rowid, user_name) VALUES (7, ?)", (GROUP,)
            )
            voice.execute(
                """
                CREATE TABLE VoiceInfo (
                    chat_name_id INTEGER, create_time INTEGER,
                    local_id INTEGER, svr_id INTEGER,
                    voice_data BLOB, data_index BLOB
                )
                """
            )
            voice.execute(
                "INSERT INTO VoiceInfo VALUES (7, 1700000000, 1, 101, ?, NULL)",
                (self.silk,),
            )
        self.event_store = EventStore(self.root / "events.db")
        self.agent_store = AgentStore(self.root / "agent.db")
        self.catalog = LiveCatalog.dynamic(
            self.message_root,
            _query_only,
            store=self.event_store,
            account_id="synthetic-account",
            source_generation="synthetic-generation",
        )
        self.voice = _query_only(self.voice_path)

    def tearDown(self) -> None:
        self.voice.close()
        self.catalog.close()
        self.agent_store.close()
        self.event_store.close()
        self.temporary.cleanup()

    @staticmethod
    def capability(_catalog: LiveCatalog) -> SourceCapabilityEvidence:
        return SourceCapabilityEvidence(
            direction_proven=True,
            contact_mapping_proven=True,
            inbound_message=lambda _source, _message: True,
            inbound_event_ref=lambda _event_ref: True,
            display_name=lambda _conversation_id: "Synthetic Group",
        )

    def runtime(self, transcriber=None) -> CodexFirstRuntime:
        return CodexFirstRuntime(
            self.catalog,
            self.event_store,
            self.agent_store,
            account_id="synthetic-account",
            source_generation="synthetic-generation",
            capability_check=self.capability,
            voice_enricher=VoiceTranscriptEnricher(
                (self.voice,), transcriber=transcriber
            ),
        )

    def test_voice_uses_one_verified_batch_without_per_message_materialization_reads(self):
        transcriber=_RecordingTranscriber()
        with patch.object(self.event_store,'standard_event_materializations_for_messages',wraps=self.event_store.standard_event_materializations_for_messages) as lookup:
            result=self.runtime(transcriber).scan(TimeWindow.from_utc(START,END),display_limit=50)
        self.assertEqual(lookup.call_count,1)
        self.assertEqual(sum('voice' in m.event_ref for g in result.groups for m in g.messages),2)
        self.assertEqual(transcriber.calls,[self.silk])

    def test_external_revision_change_after_batch_discards_voice_context(self):
        original=self.event_store.standard_event_materializations_for_messages;changed=[]
        def after_batch(source,messages,**kwargs):
            resolved=original(source,messages,**kwargs)
            if len(messages)>1 and not changed:
                changed.append(True)
                with EventStore(self.event_store.path) as other:
                    other.commit_window_messages(source,[RawMessage(1,101,34,1700000000,9,'changed synthetic data',0)],WindowIngestRun(START,END,{'source_min_time':1700000000,'source_max_time':1700000000,'examined_count':1}))
            return resolved
        with patch.object(self.event_store,'standard_event_materializations_for_messages',side_effect=after_batch):
            result=self.runtime(_RecordingTranscriber()).scan(TimeWindow.from_utc(START,END),display_limit=50)
        self.assertEqual(result.total_messages,3)
        self.assertFalse(any('voice' in m.event_ref for g in result.groups for m in g.messages))
        self.assertIn('voice_association_unavailable',result.coverage.gaps)

    def test_same_connection_revision_change_in_base_callback_discards_voice_context(self):
        source=SourceKey('synthetic-account','synthetic-generation','message_0.db',TABLE)
        def callback(_result):
            self.event_store.commit_window_messages(source,[RawMessage(1,101,34,1700000000,9,'changed synthetic data',0)],WindowIngestRun(START,END,{'source_min_time':1700000000,'source_max_time':1700000000,'examined_count':1}))
        result=self.runtime(_RecordingTranscriber()).scan(TimeWindow.from_utc(START,END),display_limit=50,on_base_messages=callback)
        self.assertEqual(result.total_messages,3)
        self.assertFalse(any('voice' in m.event_ref for g in result.groups for m in g.messages))
        self.assertIn('voice_association_unavailable',result.coverage.gaps)

    def test_batch_collector_rejects_missing_fingerprint_and_duplicate_ids(self):
        from wechat_local.standard_event_repository import ProjectionProfile
        from wechat_local.event_store import EventStoreError
        self.runtime(_RecordingTranscriber()).scan(TimeWindow.from_utc(START,END),display_limit=50)
        source=SourceKey('synthetic-account','synthetic-generation','message_0.db',TABLE)
        invalid=RawMessage(1,101,34,1700000000,9,'different content',0)
        valid=RawMessage(3,103,34,1700000020,9,None,0)
        profile=ProjectionProfile();matched={}
        returned=self.event_store.standard_event_materializations_for_messages(source,[invalid,valid],contract_version=profile.contract_version,normalizer_name=profile.normalizer_name,normalizer_version=profile.normalizer_version,_by_local_id=matched)
        self.assertEqual(set(matched),{3});self.assertEqual(len(returned),1)
        self.assertEqual(matched[3].event_id,next(iter(returned)))
        other=SourceKey('other-synthetic-account','synthetic-generation','message_0.db',TABLE)
        missing={}
        self.assertEqual(self.event_store.standard_event_materializations_for_messages(other,[valid],contract_version=profile.contract_version,normalizer_name=profile.normalizer_name,normalizer_version=profile.normalizer_version,_by_local_id=missing),{})
        self.assertEqual(missing,{})
        missing={}
        self.assertEqual(self.event_store.standard_event_materializations_for_messages(source,[valid],contract_version=profile.contract_version,normalizer_name=profile.normalizer_name,normalizer_version='other-version',_by_local_id=missing),{})
        self.assertEqual(missing,{})
        with self.assertRaises(EventStoreError):
            self.event_store.standard_event_materializations_for_messages(source,[valid,valid],contract_version=profile.contract_version,normalizer_name=profile.normalizer_name,normalizer_version=profile.normalizer_version,_by_local_id={})

    def test_visible_voice_is_enriched_and_original_text_is_preserved(self) -> None:
        transcriber = _RecordingTranscriber()
        result = self.runtime(transcriber).scan(
            TimeWindow.from_utc(START, END), display_limit=50
        )

        messages = result.groups[0].messages
        local_voice = next(message for message in messages if message.event_ref.get("voice", {}).get("status") == "local_transcript")
        native_voice = next(message for message in messages if message.event_ref.get("voice", {}).get("status") == "native_transcript")
        text = next(message for message in messages if message.text == "text remains readable")
        self.assertTrue(local_voice.text)
        self.assertEqual(local_voice.event_ref["voice"]["transcript"]["text"], "synthetic local transcript")
        self.assertEqual(native_voice.event_ref["voice"]["transcript"]["text"], "native result")
        self.assertEqual(text.event_ref.get("voice"), None)
        self.assertEqual(transcriber.calls, [self.silk])

    def test_voice_outside_current_page_is_not_processed_or_read_from_voice_source(self) -> None:
        transcriber = _RecordingTranscriber()
        statements: list[str] = []
        self.catalog.shards[0].connection.set_trace_callback(statements.append)
        voice_statements: list[str] = []
        self.voice.set_trace_callback(voice_statements.append)
        with patch('wechat_local.voice_transcript._native_transcript') as parse_native:
            result = self.runtime(transcriber).scan(
                TimeWindow.from_utc(START, END), display_limit=1, display_offset=1
            )

        self.assertEqual(result.groups[0].messages[0].text, "text remains readable")
        self.assertEqual(transcriber.calls, [])
        parse_native.assert_not_called()
        self.assertFalse(any("from voiceinfo" in sql.lower() for sql in voice_statements))
        self.assertFalse(any("packed_info_data" in sql.lower() and "where local_id=" in sql.lower() for sql in statements))

    def test_anchor_excluded_voice_is_not_processed_or_read_from_voice_source(self) -> None:
        baseline = CodexFirstRuntime(
            self.catalog,
            self.event_store,
            self.agent_store,
            account_id="synthetic-account",
            source_generation="synthetic-generation",
            capability_check=self.capability,
        ).scan(TimeWindow.from_utc(START, END), display_limit=50)
        text_id = next(
            message.event_id
            for message in baseline.groups[0].messages
            if message.text == "text remains readable"
        )
        transcriber = _RecordingTranscriber()
        statements: list[str] = []
        self.catalog.shards[0].connection.set_trace_callback(statements.append)

        voice_statements: list[str] = []
        self.voice.set_trace_callback(voice_statements.append)
        with patch('wechat_local.voice_transcript._native_transcript') as parse_native:
            result = self.runtime(transcriber).scan(
                TimeWindow.from_utc(START, END),
                display_limit=50,
                query_anchor=QueryRunAnchor((text_id,)),
            )

        self.assertEqual([message.event_id for message in result.groups[0].messages], [text_id])
        self.assertEqual(transcriber.calls, [])
        parse_native.assert_not_called()
        self.assertFalse(any("from voiceinfo" in sql.lower() for sql in voice_statements))
        self.assertFalse(any("packed_info_data" in sql.lower() and "where local_id=" in sql.lower() for sql in statements))

    def test_base_messages_callback_runs_before_local_transcription(self) -> None:
        order: list[str] = []

        class OrderedTranscriber(_RecordingTranscriber):
            def transcribe(self, payload: bytes) -> LocalTranscription:
                order.append("transcribe")
                return super().transcribe(payload)

        def base(result) -> None:
            order.append("messages")
            self.assertTrue(
                all(
                    "voice" not in message.event_ref
                    for group in result.groups
                    for message in group.messages
                )
            )

        def voice(_event_id, _result) -> None:
            order.append("voice")

        self.runtime(OrderedTranscriber()).scan(
            TimeWindow.from_utc(START, END),
            display_limit=50,
            on_base_messages=base,
            on_voice_result=voice,
        )

        self.assertEqual(order[0], "messages")
        self.assertEqual(order.count("voice"), 2)
        self.assertGreater(order.index("transcribe"), order.index("messages"))

    def test_unknown_conversation_preserves_text_and_withholds_voice_reads(self) -> None:
        with sqlite3.connect(self.message_path) as source:
            source.execute("DELETE FROM Name2Id WHERE rowid=7")
        identity = LiveIdentityAdapter(self.catalog, self_username="synthetic-self")
        transcriber = _RecordingTranscriber()
        voice_queries: list[str] = []
        self.voice.set_trace_callback(voice_queries.append)
        runtime = CodexFirstRuntime(
            self.catalog,
            self.event_store,
            self.agent_store,
            account_id="synthetic-account",
            source_generation="synthetic-generation",
            capability_check=lambda _catalog: identity.evidence(),
            voice_enricher=VoiceTranscriptEnricher((self.voice,), transcriber=transcriber),
        )
        result = runtime.scan(TimeWindow.from_utc(START, END), display_limit=50)
        text = next(message for message in result.groups[0].messages if message.text == "text remains readable")
        self.assertEqual(text.event_ref["direction"], "unknown")
        self.assertEqual(len(result.groups[0].messages), 3)
        voice_messages = [message for message in result.groups[0].messages if message.content_kind == "audio"]
        self.assertTrue(voice_messages)
        self.assertTrue(all(message.event_ref["voice"]["status"] == "enrichment_unavailable" for message in voice_messages))
        self.assertEqual(transcriber.calls, [])
        self.assertFalse(any("VoiceInfo" in sql for sql in voice_queries))


if __name__ == "__main__":
    unittest.main()
