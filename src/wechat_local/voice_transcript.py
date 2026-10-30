"""Read-only WeChat native and explicitly configured local voice enrichment.

The only supported recognition source is the transcript already stored by
WeChat in ``Msg_*.packed_info_data``. Optional local ASR is invoked only after
one exact ``VoiceInfo`` association is proven across all supplied shards. This
module never follows ``VoiceInfo.data_index``. A SILK header is format evidence
only; it is not reported as a successful decode unless the configured local
transcriber returns text.
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Protocol

from .common import quote_identifier


MAX_PACKED_INFO_BYTES = 1_048_576
NATIVE_TRANSCRIPT_SOURCE = "wechat_native_transcript"
LOCAL_TRANSCRIPT_SOURCE = "local_whisper_cpp"


class VoiceTranscriptError(RuntimeError):
    """Raised when a source cannot be queried without weakening its boundary."""


@dataclass(frozen=True)
class LocalTranscription:
    text: str
    confidence: float | None = None
    recognizer_version: str | None = None
    model_sha256: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.text, str) or not self.text.strip():
            raise ValueError("local transcription text must be non-empty")
        if self.text != self.text.strip():
            raise ValueError("local transcription text must be trimmed")
        if self.confidence is not None and not 0.0 <= self.confidence <= 1.0:
            raise ValueError("local transcription confidence must be in [0, 1]")
        for name in ("recognizer_version", "model_sha256"):
            value = getattr(self, name)
            if value is not None and (
                not isinstance(value, str) or not value or value.strip() != value
            ):
                raise ValueError(f"{name} must be non-empty trimmed text")


class LocalVoiceTranscriber(Protocol):
    def transcribe(self, payload: bytes) -> LocalTranscription: ...


@dataclass(frozen=True)
class VoiceMessageBinding:
    account_id: str
    source_generation: str
    shard_name: str
    table_name: str
    local_id: int
    server_id: int | None
    local_type: int
    create_time: int
    sender_id: int | None
    message_id: str
    conversation_id: str
    occurred_at: str
    source_revision_id: int
    revision_number: int
    direction: str

    def __post_init__(self) -> None:
        for name in (
            "account_id",
            "source_generation",
            "shard_name",
            "table_name",
            "message_id",
            "conversation_id",
            "occurred_at",
            "direction",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value or value.strip() != value:
                raise ValueError(f"{name} must be non-empty trimmed text")
        if self.local_id < 0:
            raise ValueError("local_id must not be negative")
        if (int(self.local_type) & 0xFFFFFFFF) != 34:
            raise ValueError("voice binding requires local_type 34")
        if self.source_revision_id <= 0 or self.revision_number <= 0:
            raise ValueError("revision identity must be positive")
        if self.direction not in {
            "inbound",
            "outbound",
            "self",
            "other",
            "unknown",
        }:
            raise ValueError("direction is unsupported")


def _require_query_only(connection: Any) -> None:
    row = connection.execute("PRAGMA query_only").fetchone()
    if not row or int(row[0]) != 1:
        raise VoiceTranscriptError("Voice source connection must be query-only")


def _message_table(username: Any) -> str:
    if not isinstance(username, str):
        return ""
    return "Msg_" + hashlib.md5(username.encode("utf-8")).hexdigest()


def _confirmed_conversation_username(
    connection: Any, table_name: str
) -> str | None:
    """Return one exact Name2Id identity without returning a contact listing."""

    _require_query_only(connection)
    try:
        connection.create_function(
            "wa_message_table", 1, _message_table, deterministic=True
        )
    except TypeError:  # pragma: no cover - old sqlite wrapper fallback
        connection.create_function("wa_message_table", 1, _message_table)
    try:
        rows = connection.execute(
            """
            SELECT user_name FROM Name2Id
            WHERE typeof(user_name)='text'
              AND wa_message_table(user_name)=?
            LIMIT 2
            """,
            (table_name,),
        ).fetchall()
    except Exception:
        return None
    if len(rows) != 1 or not isinstance(rows[0][0], str):
        return None
    return str(rows[0][0])


def _columns(connection: Any, table_name: str) -> set[str]:
    return {
        str(row[1])
        for row in connection.execute(
            f"PRAGMA table_info({quote_identifier(table_name)})"
        ).fetchall()
    }


def _packed_info_for_binding(
    connection: Any, binding: VoiceMessageBinding
) -> tuple[str, bytes | None]:
    required = {
        "local_id",
        "server_id",
        "local_type",
        "create_time",
        "real_sender_id",
        "packed_info_data",
    }
    if not required.issubset(_columns(connection, binding.table_name)):
        return "unsupported", None
    row = connection.execute(
        f"""
        SELECT server_id, local_type, create_time, real_sender_id,
               packed_info_data
        FROM {quote_identifier(binding.table_name)}
        WHERE local_id=?
        """,
        (binding.local_id,),
    ).fetchone()
    if row is None:
        return "identity_unavailable", None
    observed = (
        int(row[0]) if row[0] is not None else None,
        int(row[1]),
        int(row[2] or 0),
        int(row[3]) if row[3] is not None else None,
    )
    expected = (
        binding.server_id,
        int(binding.local_type),
        binding.create_time,
        binding.sender_id,
    )
    if observed != expected:
        return "identity_unavailable", None
    packed = row[4]
    if packed is None:
        return "missing", None
    if isinstance(packed, memoryview):
        packed = bytes(packed)
    if not isinstance(packed, bytes) or len(packed) > MAX_PACKED_INFO_BYTES:
        return "malformed", None
    return "available", packed


def _read_varint(payload: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while offset < len(payload) and shift <= 63:
        byte = payload[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, offset
        shift += 7
    raise ValueError("invalid protobuf varint")


def _protobuf_fields(payload: bytes) -> list[tuple[int, int, Any]]:
    fields: list[tuple[int, int, Any]] = []
    offset = 0
    while offset < len(payload):
        key, offset = _read_varint(payload, offset)
        field_number = key >> 3
        wire_type = key & 0x07
        if field_number < 1:
            raise ValueError("invalid protobuf field")
        if wire_type == 0:
            value, offset = _read_varint(payload, offset)
        elif wire_type == 1:
            if offset + 8 > len(payload):
                raise ValueError("truncated protobuf fixed64")
            value = payload[offset : offset + 8]
            offset += 8
        elif wire_type == 2:
            length, offset = _read_varint(payload, offset)
            if length > len(payload) - offset:
                raise ValueError("truncated protobuf bytes")
            value = payload[offset : offset + length]
            offset += length
        elif wire_type == 5:
            if offset + 4 > len(payload):
                raise ValueError("truncated protobuf fixed32")
            value = payload[offset : offset + 4]
            offset += 4
        else:
            raise ValueError("unsupported protobuf wire type")
        fields.append((field_number, wire_type, value))
    return fields


def _native_transcript(packed: bytes | None) -> tuple[str, str | None]:
    if packed is None or packed == b"":
        return "missing", None
    try:
        candidates: list[str] = []
        for field_number, wire_type, value in _protobuf_fields(packed):
            if field_number != 5 or wire_type != 2:
                continue
            for nested_number, nested_wire, nested_value in _protobuf_fields(value):
                if nested_number != 2 or nested_wire != 2:
                    continue
                decoded = nested_value.decode("utf-8", errors="strict").strip()
                if decoded:
                    candidates.append(decoded)
    except (UnicodeDecodeError, ValueError, TypeError):
        return "malformed", None
    unique = list(dict.fromkeys(candidates))
    if not unique:
        return "missing", None
    if len(unique) != 1:
        return "ambiguous", None
    return "available", unique[0]


def _silk_format(header: bytes) -> str:
    if header.startswith(b"#!SILK_V3") or (
        len(header) > 1 and header[1:].startswith(b"#!SILK_V3")
    ):
        return "silk"
    return "unknown"


def _voice_audio(
    connections: Iterable[Any],
    conversation_username: str,
    binding: VoiceMessageBinding,
    *,
    transcriber: LocalVoiceTranscriber | None,
    source_unavailable: bool = False,
) -> tuple[dict[str, Any], LocalTranscription | None]:
    # Uniqueness is established by conversation + server id across every
    # explicitly supplied shard first. local_id/create_time are checked only
    # after that, so reused server ids cannot be hidden by extra predicates.
    matches: list[tuple[Any, int, int, int, int | None, bytes]] = []
    source_epochs: list[tuple[Any, tuple[int, int]]] = []
    if binding.server_id is None:
        return _audio_result("unavailable", 0), None
    if source_unavailable:
        return _audio_result("unavailable", 0, source_complete=False), None
    source_complete = True
    for connection in connections:
        try:
            _require_query_only(connection)
            if not {
                "chat_name_id",
                "create_time",
                "local_id",
                "svr_id",
                "voice_data",
            }.issubset(_columns(connection, "VoiceInfo")):
                source_complete = False
                continue
            if "user_name" not in _columns(connection, "Name2Id"):
                source_complete = False
                continue
            epoch = (int(connection.execute('PRAGMA data_version').fetchone()[0]),
                     int(connection.total_changes))
            source_epochs.append((connection, epoch))
            rows = connection.execute(
                """
                SELECT voice.rowid, voice.local_id, voice.create_time,
                       length(voice.voice_data),
                       substr(voice.voice_data, 1, 16)
                FROM VoiceInfo AS voice
                JOIN Name2Id AS names ON names.rowid=voice.chat_name_id
                WHERE names.user_name=? AND voice.svr_id=?
                LIMIT 2
                """,
                (conversation_username, binding.server_id),
            ).fetchall()
        except Exception:
            # A missing/wrong-key/unreadable explicitly supplied voice shard is
            # a controlled unavailable source. Do not establish uniqueness from
            # an incomplete set of explicitly supplied shards.
            source_complete = False
            continue
        for rowid, local_id, create_time, length, header in rows:
            if isinstance(header, memoryview):
                header = bytes(header)
            matches.append(
                (
                    connection,
                    int(rowid),
                    int(local_id),
                    int(create_time or 0),
                    int(length) if length is not None else None,
                    header if isinstance(header, bytes) else b"",
                )
            )
    if not source_complete:
        return _audio_result(
            "unavailable", len(matches), source_complete=False
        ), None
    if not matches:
        return _audio_result("unavailable", 0), None
    if len(matches) != 1:
        return _audio_result("ambiguous", len(matches)), None
    connection, rowid, local_id, create_time, length, header = matches[0]
    if local_id != binding.local_id or create_time != binding.create_time:
        return _audio_result("identity_mismatch", 1), None
    if not length:
        return _audio_result(
            "unique", 1, payload_status="empty", byte_count=int(length or 0)
        ), None
    audio = _audio_result(
        "unique",
        1,
        payload_status="available",
        format_name=_silk_format(header),
        byte_count=length,
    )
    if audio["format"] != "silk" or transcriber is None:
        return audio, None
    try:
        row = connection.execute(
            """SELECT voice_data FROM VoiceInfo AS voice
               JOIN Name2Id AS names ON names.rowid=voice.chat_name_id
               WHERE voice.rowid=? AND names.user_name=? AND voice.svr_id=?
                 AND voice.local_id=? AND voice.create_time=?""",
            (rowid, conversation_username, binding.server_id, binding.local_id, binding.create_time),
        ).fetchone()
        payload = row[0] if row is not None else None
        if isinstance(payload, memoryview):
            payload = bytes(payload)
        if (
            not isinstance(payload, bytes)
            or len(payload) != length
            or payload[:16] != header
            or _silk_format(payload[:16]) != "silk"
            or any(before != (int(source.execute('PRAGMA data_version').fetchone()[0]),
                              int(source.total_changes)) for source, before in source_epochs)
        ):
            return {**audio, "decode_status": "source_changed"}, None
        audio = {
            **audio,
            "audio_payload_sha256": hashlib.sha256(payload).hexdigest(),
        }
        transcription = transcriber.transcribe(payload)
        if not isinstance(transcription, LocalTranscription):
            raise TypeError("local transcriber returned an invalid result")
    except Exception:
        return {**audio, "decode_status": "unavailable"}, None
    return {**audio, "decode_status": "transcribed"}, transcription


def _audio_result(
    association_status: str,
    match_count: int,
    *,
    payload_status: str = "unknown",
    format_name: str | None = None,
    byte_count: int | None = None,
    audio_payload_sha256: str | None = None,
    source_complete: bool = True,
) -> dict[str, Any]:
    return {
        "association_status": association_status,
        "match_count": match_count,
        "payload_status": payload_status,
        "format": format_name,
        "byte_count": byte_count,
        "audio_payload_sha256": audio_payload_sha256,
        "decode_status": "not_attempted",
        "source_complete": source_complete,
    }


class VoiceTranscriptEnricher:
    """Derive one bounded voice result with optional local-only ASR."""

    def __init__(
        self,
        voice_connections: Iterable[Any] = (),
        *,
        transcriber: LocalVoiceTranscriber | None = None,
        source_unavailable: bool = False,
    ) -> None:
        self.voice_connections = tuple(voice_connections)
        self.transcriber = transcriber
        self.source_unavailable = source_unavailable

    def enrich(
        self, message_connection: Any, binding: VoiceMessageBinding,
        *, _window_packed: tuple[Any, ...] | None = None,
    ) -> dict[str, Any]:
        if not isinstance(binding, VoiceMessageBinding):
            raise TypeError("binding must be a VoiceMessageBinding")
        binding_payload = asdict(binding)
        binding_payload["packed_info_sha256"] = None
        result: dict[str, Any] = {
            "kind": "voice",
            "label": "语音转写",
            "status": "scope_unavailable",
            "binding": binding_payload,
            "transcript": {
                "status": "not_read",
                "source": NATIVE_TRANSCRIPT_SOURCE,
                "text": None,
                "confidence": None,
                "recognizer_version": None,
                "model_sha256": None,
            },
            "audio": _audio_result("not_read", 0),
        }

        conversation_username = _confirmed_conversation_username(
            message_connection, binding.table_name
        )
        if conversation_username is None:
            return result

        if _window_packed is None:
            packed_status, packed = _packed_info_for_binding(message_connection, binding)
        else:
            available, server, local_type, created, sender, packed, packed_kind, packed_size = _window_packed
            observed = (int(server) if server is not None else None, int(local_type), int(created or 0), int(sender) if sender is not None else None)
            expected = (binding.server_id, int(binding.local_type), binding.create_time, binding.sender_id)
            if not available:
                packed_status, packed = "unsupported", None
            elif observed != expected:
                packed_status, packed = "identity_unavailable", None
            elif packed_kind == 'null':
                packed_status = "missing"
            elif packed_kind != 'blob' or packed_size is None or packed_size > MAX_PACKED_INFO_BYTES:
                packed_status, packed = "malformed", None
            else:
                if isinstance(packed, memoryview):
                    packed = bytes(packed)
                if not isinstance(packed, bytes) or len(packed) > MAX_PACKED_INFO_BYTES:
                    packed_status, packed = "malformed", None
                else:
                    packed_status = "available"
        if packed_status == "identity_unavailable":
            result["status"] = "source_identity_unavailable"
            return result
        if packed is not None:
            result["binding"]["packed_info_sha256"] = hashlib.sha256(
                packed
            ).hexdigest()
        transcript_status, transcript = _native_transcript(packed)
        if packed_status == "unsupported":
            transcript_status = "unsupported"
        elif packed_status == "malformed":
            transcript_status = "malformed"
        result["transcript"]["status"] = transcript_status
        if transcript_status == "available":
            result["status"] = "native_transcript"
            result["transcript"]["text"] = transcript
            result["audio"] = _audio_result("not_needed", 0)
            return result

        audio, local_transcript = _voice_audio(
            self.voice_connections,
            conversation_username,
            binding,
            transcriber=self.transcriber,
            source_unavailable=self.source_unavailable,
        )
        result["audio"] = audio
        if local_transcript is not None:
            result["status"] = "local_transcript"
            result["transcript"] = {
                "status": "available",
                "source": LOCAL_TRANSCRIPT_SOURCE,
                "text": local_transcript.text,
                "confidence": local_transcript.confidence,
                "recognizer_version": local_transcript.recognizer_version,
                "model_sha256": local_transcript.model_sha256,
            }
            return result
        if not audio["source_complete"]:
            result["status"] = "voice_source_unavailable"
        elif audio["association_status"] == "ambiguous":
            result["status"] = "audio_source_ambiguous"
        elif audio["association_status"] != "unique":
            result["status"] = "audio_source_unavailable"
        elif audio["payload_status"] == "empty":
            result["status"] = "audio_payload_empty"
        elif audio["decode_status"] in {"unavailable", "source_changed"}:
            result["status"] = "local_transcription_unavailable"
            result["transcript"]["source"] = LOCAL_TRANSCRIPT_SOURCE
        elif audio["format"] == "silk":
            result["status"] = "asr_unavailable"
        else:
            result["status"] = "audio_format_unsupported"
        return result
