from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import unittest
from pathlib import Path

from wechat_local.voice_transcript import (
    LocalTranscription,
    VoiceMessageBinding,
    VoiceTranscriptEnricher,
)


GROUP = "synthetic-group@chatroom"
TABLE = "Msg_" + hashlib.md5(GROUP.encode("utf-8")).hexdigest()
PRIVATE = "synthetic-private-contact"
PRIVATE_TABLE = "Msg_" + hashlib.md5(PRIVATE.encode("utf-8")).hexdigest()


def _varint(value: int) -> bytes:
    encoded = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        encoded.append(byte | (0x80 if value else 0))
        if not value:
            return bytes(encoded)


def _field(number: int, value: bytes) -> bytes:
    return _varint((number << 3) | 2) + _varint(len(value)) + value


def native_transcript(value: str) -> bytes:
    return _field(5, _field(2, value.encode("utf-8")))


class VoiceTranscriptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.message_path = self.root / "message.db"
        with sqlite3.connect(self.message_path) as connection:
            connection.execute("CREATE TABLE Name2Id (user_name TEXT)")
            connection.execute(
                "INSERT INTO Name2Id(rowid, user_name) VALUES (7, ?)", (GROUP,)
            )
            connection.execute(
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
            connection.executemany(
                f"INSERT INTO {TABLE} VALUES (?, ?, 34, ?, 9, NULL, 0, ?)",
                (
                    (1, 101, 1_700_000_000, native_transcript("synthetic native transcript")),
                    (2, 102, 1_700_000_001, _field(1, b"abcde")),
                    (3, 103, 1_700_000_002, b"\x2a\x05\x12"),
                    (
                        4,
                        104,
                        1_700_000_003,
                        native_transcript("first") + native_transcript("second"),
                    ),
                    (
                        5,
                        105,
                        1_700_000_004,
                        native_transcript("same") + native_transcript("same"),
                    ),
                ),
            )
            connection.execute(
                f"INSERT INTO {TABLE} VALUES (6, 106, 34, 1700000005, NULL, NULL, 0, ?)",
                (native_transcript("unknown sender transcript"),),
            )
        self.message = sqlite3.connect(self.message_path)
        self.message.execute("PRAGMA query_only = ON")
        self.voice_connections: list[sqlite3.Connection] = []

    def tearDown(self) -> None:
        for connection in self.voice_connections:
            connection.close()
        self.message.close()
        self.temporary.cleanup()

    def voice_database(
        self,
        *,
        local_id: int,
        server_id: int,
        create_time: int,
        voice_data: bytes | None,
    ) -> sqlite3.Connection:
        path = self.root / f"voice-{len(self.voice_connections)}.db"
        with sqlite3.connect(path) as connection:
            connection.execute("CREATE TABLE Name2Id (user_name TEXT)")
            connection.execute(
                "INSERT INTO Name2Id(rowid, user_name) VALUES (7, ?)", (GROUP,)
            )
            connection.execute(
                """
                CREATE TABLE VoiceInfo (
                    chat_name_id INTEGER,
                    create_time INTEGER,
                    local_id INTEGER,
                    svr_id INTEGER,
                    voice_data BLOB,
                    data_index BLOB
                )
                """
            )
            connection.execute(
                "INSERT INTO VoiceInfo VALUES (7, ?, ?, ?, ?, ?)",
                (create_time, local_id, server_id, voice_data, b"opaque-token"),
            )
        result = sqlite3.connect(path)
        result.execute("PRAGMA query_only = ON")
        self.voice_connections.append(result)
        return result

    @staticmethod
    def binding(local_id: int) -> VoiceMessageBinding:
        return VoiceMessageBinding(
            account_id="synthetic-account",
            source_generation="synthetic-generation",
            shard_name="message_7.db",
            table_name=TABLE,
            local_id=local_id,
            server_id=100 + local_id,
            local_type=34,
            create_time=1_699_999_999 + local_id,
            sender_id=9,
            message_id=f"evt-{local_id}",
            conversation_id="conv-synthetic",
            occurred_at=f"2023-11-14T22:13:2{local_id}+00:00",
            source_revision_id=200 + local_id,
            revision_number=1,
            direction="unknown",
        )

    def test_window_packed_binding_matches_original_and_rejects_wrong_identity(self):
        from wechat_local.live_tailer import read_message_window
        result = read_message_window(self.message, TABLE, start_unix_seconds=1700000000,
                                     end_unix_seconds=1700000000, batch_size=10, collect_voice_packed=True)
        captured = result.voice_packed[1]
        original = VoiceTranscriptEnricher().enrich(self.message, self.binding(1))
        sql = []
        self.message.set_trace_callback(sql.append)
        same = VoiceTranscriptEnricher().enrich(self.message, self.binding(1), _window_packed=captured)
        self.assertEqual(original, same)
        self.assertFalse(any('packed_info_data' in statement.lower() and statement.lstrip().lower().startswith('select') for statement in sql))
        wrong = list(captured); wrong[1] = 999
        self.assertEqual(VoiceTranscriptEnricher().enrich(self.message, self.binding(1), _window_packed=tuple(wrong))['status'], 'source_identity_unavailable')

    def test_window_packed_missing_server_column_matches_unsupported_binding(self):
        from dataclasses import replace
        from wechat_local.live_tailer import read_message_window
        connection = sqlite3.connect(':memory:')
        connection.execute('CREATE TABLE Name2Id(user_name)')
        connection.execute('INSERT INTO Name2Id VALUES(?)', (GROUP,))
        connection.execute(f'CREATE TABLE {TABLE}(local_id INTEGER PRIMARY KEY, local_type, create_time, real_sender_id, message_content, WCDB_CT_message_content, packed_info_data)')
        connection.execute(f'INSERT INTO {TABLE} VALUES(1,34,1700000000,9,NULL,0,?)', (native_transcript('synthetic should stay unsupported'),))
        connection.execute('PRAGMA query_only=ON')
        try:
            binding = replace(self.binding(1), server_id=None)
            read = read_message_window(connection, TABLE, start_unix_seconds=1700000000,
                                       end_unix_seconds=1700000000, batch_size=10, collect_voice_packed=True)
            original = VoiceTranscriptEnricher().enrich(connection, binding)
            carried = VoiceTranscriptEnricher().enrich(connection, binding, _window_packed=read.voice_packed[1])
            self.assertEqual(original['transcript']['status'], 'unsupported')
            self.assertEqual(carried, original)
            mismatched = replace(binding, server_id=123, sender_id=777)
            self.assertEqual(VoiceTranscriptEnricher().enrich(connection, mismatched, _window_packed=read.voice_packed[1]),
                             VoiceTranscriptEnricher().enrich(connection, mismatched))
        finally:
            connection.close()

    def test_window_packed_oversized_is_not_carried_and_remains_malformed(self):
        from wechat_local.live_tailer import read_message_window
        from wechat_local.voice_transcript import MAX_PACKED_INFO_BYTES
        with sqlite3.connect(self.message_path) as writer:
            writer.execute(f'UPDATE {TABLE} SET packed_info_data=? WHERE local_id=1', (b'x' * (MAX_PACKED_INFO_BYTES + 1),))
        read = read_message_window(self.message, TABLE, start_unix_seconds=1700000000,
                                   end_unix_seconds=1700000000, batch_size=10, collect_voice_packed=True)
        captured = read.voice_packed[1]
        self.assertIsNone(captured[5])
        result = VoiceTranscriptEnricher().enrich(self.message, self.binding(1), _window_packed=captured)
        self.assertEqual(result['transcript']['status'], 'malformed')

    def test_native_transcript_is_bound_to_exact_message_and_packed_hash(self) -> None:
        result = VoiceTranscriptEnricher().enrich(
            self.message, self.binding(1)
        )

        self.assertEqual(result["status"], "native_transcript")
        self.assertEqual(result["label"], "语音转写")
        self.assertEqual(result["transcript"]["source"], "wechat_native_transcript")
        self.assertEqual(result["transcript"]["text"], "synthetic native transcript")
        self.assertIsNone(result["transcript"]["confidence"])
        self.assertEqual(
            result["binding"]["packed_info_sha256"],
            hashlib.sha256(native_transcript("synthetic native transcript")).hexdigest(),
        )
        self.assertEqual(result["binding"]["source_revision_id"], 201)
        self.assertEqual(result["binding"]["direction"], "unknown")

    def test_missing_native_transcript_with_silk_payload_reports_asr_unavailable(self) -> None:
        voice = self.voice_database(
            local_id=2,
            server_id=102,
            create_time=1_700_000_001,
            voice_data=b"\x02#!SILK_V3" + b"x" * 32,
        )
        result = VoiceTranscriptEnricher((voice,)).enrich(
            self.message, self.binding(2)
        )

        self.assertEqual(result["status"], "asr_unavailable")
        self.assertEqual(result["transcript"]["status"], "missing")
        self.assertEqual(result["audio"]["association_status"], "unique")
        self.assertEqual(result["audio"]["payload_status"], "available")
        self.assertEqual(result["audio"]["format"], "silk")
        self.assertEqual(result["audio"]["byte_count"], 42)
        self.assertNotIn("data_index", result["audio"])

    def test_private_voice_uses_exact_name2id_mapping_and_message_binding(self) -> None:
        writer = sqlite3.connect(self.message_path)
        writer.execute("INSERT INTO Name2Id(rowid, user_name) VALUES (17, ?)", (PRIVATE,))
        writer.execute(
            f"CREATE TABLE {PRIVATE_TABLE} (local_id INTEGER PRIMARY KEY, server_id INTEGER, "
            "local_type INTEGER NOT NULL, create_time INTEGER, real_sender_id INTEGER, "
            "message_content BLOB, WCDB_CT_message_content INTEGER, packed_info_data BLOB)"
        )
        writer.execute(
            f"INSERT INTO {PRIVATE_TABLE} VALUES (1, 501, 34, 1700000100, 9, NULL, 0, ?)",
            (b"",),
        )
        writer.commit()
        writer.close()

        voice_path = self.root / "private-voice.db"
        with sqlite3.connect(voice_path) as writer:
            writer.execute("CREATE TABLE Name2Id (user_name TEXT)")
            writer.execute("INSERT INTO Name2Id(rowid, user_name) VALUES (17, ?)", (PRIVATE,))
            writer.execute(
                "CREATE TABLE VoiceInfo (chat_name_id INTEGER, create_time INTEGER, "
                "local_id INTEGER, svr_id INTEGER, voice_data BLOB, data_index BLOB)"
            )
            writer.execute(
                "INSERT INTO VoiceInfo VALUES (17, 1700000100, 1, 501, ?, ?)",
                (b"\\x02#!SILK_V3" + b"x" * 32, b"opaque-token"),
            )
        voice = sqlite3.connect(voice_path)
        voice.execute("PRAGMA query_only=ON")
        self.voice_connections.append(voice)
        private_message = sqlite3.connect(self.message_path)
        private_message.execute("PRAGMA query_only=ON")
        self.voice_connections.append(private_message)
        binding = VoiceMessageBinding(
            account_id="synthetic-account", source_generation="synthetic-generation",
            shard_name="message_7.db", table_name=PRIVATE_TABLE, local_id=1,
            server_id=501, local_type=34, create_time=1700000100, sender_id=9,
            message_id="evt-private-voice", conversation_id="conv-private",
            occurred_at="2023-11-14T22:15:00+00:00", source_revision_id=501,
            revision_number=1, direction="unknown",
        )

        result = VoiceTranscriptEnricher((voice,)).enrich(private_message, binding)

        self.assertEqual(result["audio"]["association_status"], "unique")
        self.assertEqual(result["audio"]["payload_status"], "available")

    def test_empty_voice_payload_is_explicit_and_data_index_is_not_followed(self) -> None:
        voice = self.voice_database(
            local_id=2,
            server_id=102,
            create_time=1_700_000_001,
            voice_data=b"",
        )
        statements: list[str] = []
        voice.set_trace_callback(statements.append)

        result = VoiceTranscriptEnricher((voice,)).enrich(
            self.message, self.binding(2)
        )

        self.assertEqual(result["status"], "audio_payload_empty")
        self.assertEqual(result["audio"]["payload_status"], "empty")
        self.assertTrue(all("data_index" not in sql.lower() for sql in statements))

    def test_configured_local_transcriber_receives_only_unique_silk_payload(self) -> None:
        payload = b"\x02#!SILK_V3" + b"x" * 32
        voice = self.voice_database(
            local_id=2,
            server_id=102,
            create_time=1_700_000_001,
            voice_data=payload,
        )

        class LocalTranscriber:
            def __init__(self) -> None:
                self.payloads: list[bytes] = []

            def transcribe(self, value: bytes) -> LocalTranscription:
                self.payloads.append(value)
                return LocalTranscription("synthetic local transcript")

        transcriber = LocalTranscriber()
        result = VoiceTranscriptEnricher(
            (voice,), transcriber=transcriber
        ).enrich(self.message, self.binding(2))

        self.assertEqual(result["status"], "local_transcript")
        self.assertEqual(result["transcript"]["source"], "local_whisper_cpp")
        self.assertEqual(result["transcript"]["text"], "synthetic local transcript")
        self.assertEqual(result["audio"]["decode_status"], "transcribed")
        self.assertEqual(
            result["audio"]["audio_payload_sha256"], hashlib.sha256(payload).hexdigest()
        )
        self.assertEqual(transcriber.payloads, [payload])

    def test_unknown_conversation_scope_reads_neither_packed_nor_voice(self) -> None:
        unknown_table = "Msg_00000000000000000000000000000000"
        writer = sqlite3.connect(self.message_path)
        writer.execute(
            f"""
            CREATE TABLE {unknown_table} (
                local_id INTEGER PRIMARY KEY, server_id INTEGER,
                local_type INTEGER, create_time INTEGER, real_sender_id INTEGER,
                message_content BLOB, WCDB_CT_message_content INTEGER,
                packed_info_data BLOB
            )
            """
        )
        writer.execute(
            f"INSERT INTO {unknown_table} VALUES (1, 101, 34, 1700000000, 9, NULL, 0, ?)",
            (native_transcript("must not be read"),),
        )
        writer.commit()
        writer.close()
        voice = self.voice_database(
            local_id=1,
            server_id=101,
            create_time=1_700_000_000,
            voice_data=b"\x02#!SILK_V3payload",
        )
        message_statements: list[str] = []
        voice_statements: list[str] = []
        self.message.set_trace_callback(message_statements.append)
        voice.set_trace_callback(voice_statements.append)
        binding = VoiceMessageBinding(
            **{**self.binding(1).__dict__, "table_name": unknown_table}
        )

        result = VoiceTranscriptEnricher((voice,)).enrich(self.message, binding)

        self.assertEqual(result["status"], "scope_unavailable")
        self.assertFalse(any("packed_info_data" in sql for sql in message_statements))
        self.assertFalse(any("voiceinfo" in sql.lower() for sql in voice_statements))

    def test_source_identity_mismatch_fails_closed(self) -> None:
        binding = VoiceMessageBinding(
            **{**self.binding(1).__dict__, "server_id": 999}
        )
        result = VoiceTranscriptEnricher().enrich(self.message, binding)
        self.assertEqual(result["status"], "source_identity_unavailable")
        self.assertIsNone(result["binding"]["packed_info_sha256"])

    def test_malformed_and_ambiguous_native_transcripts_are_not_guessed(self) -> None:
        malformed = VoiceTranscriptEnricher().enrich(
            self.message, self.binding(3)
        )
        ambiguous = VoiceTranscriptEnricher().enrich(
            self.message, self.binding(4)
        )

        self.assertEqual(malformed["transcript"]["status"], "malformed")
        self.assertIsNone(malformed["transcript"]["text"])
        self.assertEqual(ambiguous["transcript"]["status"], "ambiguous")
        self.assertIsNone(ambiguous["transcript"]["text"])

    def test_identical_native_candidates_are_deduplicated(self) -> None:
        result = VoiceTranscriptEnricher().enrich(
            self.message, self.binding(5)
        )
        self.assertEqual(result["status"], "native_transcript")
        self.assertEqual(result["transcript"]["text"], "same")

    def test_unknown_sender_and_confidence_are_not_invented(self) -> None:
        binding = VoiceMessageBinding(
            **{**self.binding(6).__dict__, "sender_id": None}
        )
        result = VoiceTranscriptEnricher().enrich(self.message, binding)
        self.assertEqual(result["status"], "native_transcript")
        self.assertIsNone(result["binding"]["sender_id"])
        self.assertEqual(result["binding"]["direction"], "unknown")
        self.assertIsNone(result["transcript"]["confidence"])

    def test_unsupported_audio_and_local_failure_are_explicit(self) -> None:
        unsupported = self.voice_database(
            local_id=2,
            server_id=102,
            create_time=1_700_000_001,
            voice_data=b"not-silk",
        )
        result = VoiceTranscriptEnricher((unsupported,)).enrich(
            self.message, self.binding(2)
        )
        self.assertEqual(result["status"], "audio_format_unsupported")

        unsupported.close()
        silk = self.voice_database(
            local_id=2,
            server_id=102,
            create_time=1_700_000_001,
            voice_data=b"\x02#!SILK_V3" + b"x" * 32,
        )

        class FailingTranscriber:
            def transcribe(self, _payload: bytes) -> LocalTranscription:
                raise RuntimeError("synthetic local failure")

        failed = VoiceTranscriptEnricher(
            (silk,), transcriber=FailingTranscriber()
        ).enrich(self.message, self.binding(2))
        self.assertEqual(failed["status"], "local_transcription_unavailable")
        self.assertEqual(failed["audio"]["decode_status"], "unavailable")
        self.assertIsNotNone(failed["audio"]["audio_payload_sha256"])

    def test_cross_database_duplicate_voice_association_is_ambiguous(self) -> None:
        first = self.voice_database(
            local_id=2,
            server_id=102,
            create_time=1_700_000_001,
            voice_data=b"\x02#!SILK_V3first",
        )
        second = self.voice_database(
            local_id=99,
            server_id=102,
            create_time=1_700_000_099,
            voice_data=b"\x02#!SILK_V3second",
        )
        result = VoiceTranscriptEnricher((first, second)).enrich(
            self.message, self.binding(2)
        )

        self.assertEqual(result["status"], "audio_source_ambiguous")
        self.assertEqual(result["audio"]["association_status"], "ambiguous")
        self.assertEqual(result["audio"]["match_count"], 2)

    def test_unreadable_explicit_shard_prevents_unique_match_and_asr(self) -> None:
        good = self.voice_database(
            local_id=2,
            server_id=102,
            create_time=1_700_000_001,
            voice_data=b"\x02#!SILK_V3" + b"x" * 32,
        )
        unreadable = self.voice_database(
            local_id=99,
            server_id=999,
            create_time=1_700_000_099,
            voice_data=b"not-silk",
        )
        unreadable.close()

        class RecordingTranscriber:
            def __init__(self) -> None:
                self.calls = 0

            def transcribe(self, _payload: bytes) -> LocalTranscription:
                self.calls += 1
                return LocalTranscription("synthetic local transcript")

        transcriber = RecordingTranscriber()
        result = VoiceTranscriptEnricher(
            (good, unreadable), transcriber=transcriber
        ).enrich(self.message, self.binding(2))

        self.assertEqual(result["status"], "voice_source_unavailable")
        self.assertFalse(result["audio"]["source_complete"])
        self.assertEqual(transcriber.calls, 0)
        good.close()


if __name__ == "__main__":
    unittest.main()
