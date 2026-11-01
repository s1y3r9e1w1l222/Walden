"""Pure WeChat raw-revision to Standard Event v1 normalization.

This module deliberately has no EventStore, SQLite, live-database, Session,
filesystem, or network dependency.  Its complete input is the immutable raw
event/revision plus explicitly pinned contract and normalizer versions.
"""

from __future__ import annotations

import hashlib
import json
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

import zstandard as zstd


STANDARD_EVENT_SPEC_VERSION = "wechat-local.standard-event/1"
WECHAT_NORMALIZER_NAME = "wechat-macos-v4"
WECHAT_PROVIDER = "wechat"
MAX_XML_CHARACTERS = 2_000_000
SOURCE_TIMESTAMP_LOWER_BOUND = 946_684_800  # 2000-01-01T00:00:00Z, inclusive
SOURCE_TIMESTAMP_UPPER_BOUND = 4_102_444_800  # 2100-01-01T00:00:00Z, exclusive

BASE_TYPE_KINDS = {
    1: "text",
    3: "image",
    34: "audio",
    42: "contact",
    43: "video",
    47: "sticker",
    48: "location",
    50: "call",
    10_000: "system",
    10_002: "recall",
}
APP_SUBTYPE_KINDS = {
    5: "link",
    6: "file",
    19: "chat_bundle",
    33: "mini_app",
    36: "mini_app",
    57: "quote",
    2_000: "payment",
}
QUALITY_CODE_ORDER = (
    "conversation_unresolved",
    "actor_unresolved",
    "unknown_message_type",
    "unknown_app_subtype",
    "unsupported_compression",
    "decompression_failed",
    "invalid_utf8_replaced",
    "xml_too_large",
    "xml_parse_failed",
    "timestamp_missing",
    "timestamp_out_of_range",
)
DEGRADED_QUALITY_CODES = {
    "unknown_message_type",
    "unknown_app_subtype",
    "unsupported_compression",
    "decompression_failed",
    "invalid_utf8_replaced",
    "xml_too_large",
    "xml_parse_failed",
}
RAW_CONTENT_KINDS = {"null", "text", "blob"}

_ZSTD_DECOMPRESSOR = zstd.ZstdDecompressor()
_XML_ROOT_PATTERN = re.compile(r"<(?:msg|sysmsg|voipmsg|appmsg)(?:\s|>)")
_XML_TAG_PATTERN = re.compile(r"<(?:\?xml|[A-Za-z_][A-Za-z0-9_.:-]*)(?:\s|/?>)")


def _validated_text(value: str, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be text")
    if not value or value.strip() != value:
        raise ValueError(f"{field_name} must be a non-empty trimmed string")
    if any(ord(character) < 32 for character in value):
        raise ValueError(f"{field_name} must not contain control characters")
    return value


def _validated_observed_at(value: str) -> str:
    _validated_text(value, "observed_at")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("observed_at must be a canonical UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("observed_at must be a canonical UTC timestamp")
    canonical = parsed.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if value != canonical:
        raise ValueError("observed_at must be a canonical UTC timestamp")
    return value


@dataclass(frozen=True)
class RawEventInput:
    """Durable raw event fields; physical fields never enter the core payload."""

    event_uid: str
    account_id: str
    source_generation: str
    shard_name: str
    table_name: str
    local_id: int

    def __post_init__(self) -> None:
        for field_name in (
            "event_uid",
            "account_id",
            "source_generation",
            "shard_name",
            "table_name",
        ):
            _validated_text(getattr(self, field_name), field_name)
        if not isinstance(self.local_id, int) or isinstance(self.local_id, bool):
            raise TypeError("local_id must be an integer")
        if self.local_id < 0:
            raise ValueError("local_id must not be negative")


@dataclass(frozen=True)
class RawRevisionInput:
    """Exact immutable raw revision fields needed by Standard Event v1."""

    content_fingerprint: str
    revision_number: int
    server_id: str | None
    local_type: int
    create_time: int | None
    real_sender_id: int | None
    raw_content: str | bytes | None
    raw_content_kind: str
    compression_type: int | None
    observed_at: str

    def __post_init__(self) -> None:
        _validated_text(self.content_fingerprint, "content_fingerprint")
        if (
            not isinstance(self.revision_number, int)
            or isinstance(self.revision_number, bool)
            or self.revision_number <= 0
        ):
            raise ValueError("revision_number must be a positive integer")
        if not isinstance(self.local_type, int) or isinstance(self.local_type, bool):
            raise TypeError("local_type must be an integer")
        if self.server_id is not None and not isinstance(self.server_id, str):
            raise TypeError("server_id must be text or null")
        if self.create_time is not None and (
            not isinstance(self.create_time, int)
            or isinstance(self.create_time, bool)
        ):
            raise TypeError("create_time must be an integer or null")
        if self.real_sender_id is not None and (
            not isinstance(self.real_sender_id, int)
            or isinstance(self.real_sender_id, bool)
        ):
            raise TypeError("real_sender_id must be an integer or null")
        if self.raw_content_kind not in RAW_CONTENT_KINDS:
            raise ValueError("raw_content_kind must be null, text, or blob")
        if self.raw_content_kind == "null" and self.raw_content is not None:
            raise ValueError("null raw content kind requires null content")
        if self.raw_content_kind == "text" and not isinstance(
            self.raw_content, str
        ):
            raise ValueError("text raw content kind requires text content")
        if self.raw_content_kind == "blob" and not isinstance(
            self.raw_content, bytes
        ):
            raise ValueError("blob raw content kind requires bytes content")
        if self.compression_type is not None and (
            not isinstance(self.compression_type, int)
            or isinstance(self.compression_type, bool)
        ):
            raise TypeError("compression_type must be an integer or null")
        _validated_observed_at(self.observed_at)


@dataclass(frozen=True)
class NormalizerConfig:
    """Explicitly pinned contract and implementation versions."""

    contract_version: str
    normalizer_name: str
    normalizer_version: str

    def __post_init__(self) -> None:
        _validated_text(self.contract_version, "contract_version")
        _validated_text(self.normalizer_name, "normalizer_name")
        _validated_text(self.normalizer_version, "normalizer_version")
        if self.contract_version != STANDARD_EVENT_SPEC_VERSION:
            raise ValueError("Unsupported Standard Event contract version")
        if self.normalizer_name != WECHAT_NORMALIZER_NAME:
            raise ValueError("Unsupported WeChat normalizer name")


@dataclass(frozen=True)
class StandardEventResult:
    """Immutable pure output ready for I1 materialization persistence."""

    event_id: str
    subject_id: str
    stream_id: str
    conversation_id: str
    contract_version: str
    normalizer_name: str
    normalizer_version: str
    canonical_payload_json: str
    payload_sha256: str
    quality_status: str


@dataclass(frozen=True)
class _DecodedContent:
    text: str | None
    quality_codes: tuple[str, ...]


@dataclass(frozen=True)
class _XmlParseResult:
    root: ET.Element | None
    quality_code: str | None


def _typed_bytes(value: Any) -> bytes:
    if value is None:
        kind = b"N"
        payload = b""
    elif isinstance(value, bytes):
        kind = b"B"
        payload = value
    elif isinstance(value, str):
        kind = b"S"
        payload = value.encode("utf-8", errors="surrogatepass")
    elif isinstance(value, int) and not isinstance(value, bool):
        kind = b"I"
        payload = str(value).encode("ascii")
    else:
        raise TypeError("Standard ID inputs must be null, bytes, text, or integers")
    return kind + len(payload).to_bytes(8, "big") + payload


def _standard_id(prefix: str, domain: str, *values: Any) -> str:
    digest = hashlib.sha256()
    for value in (domain, *values):
        digest.update(_typed_bytes(value))
    return prefix + digest.hexdigest()


def _subject_id(event: RawEventInput) -> str:
    return _standard_id("msg_", "standard-subject-v1", event.event_uid)


def _event_id(event: RawEventInput, revision: RawRevisionInput) -> str:
    return _standard_id(
        "evt_",
        "standard-event-v1",
        event.event_uid,
        revision.content_fingerprint,
    )


def _stream_id(event: RawEventInput) -> str:
    return _standard_id(
        "stream_",
        "standard-stream-v1",
        event.account_id,
        event.source_generation,
    )


def _conversation_id(event: RawEventInput) -> str:
    return _standard_id(
        "conv_",
        "standard-conversation-v1",
        event.account_id,
        event.source_generation,
        event.table_name,
    )


def _decode_utf8(value: bytes) -> _DecodedContent:
    try:
        return _DecodedContent(value.decode("utf-8"), ())
    except UnicodeDecodeError:
        return _DecodedContent(
            value.decode("utf-8", errors="replace"),
            ("invalid_utf8_replaced",),
        )


def _decode_raw_content(revision: RawRevisionInput) -> _DecodedContent:
    if revision.raw_content_kind == "null":
        return _DecodedContent(None, ())
    if revision.raw_content_kind == "text":
        assert isinstance(revision.raw_content, str)
        return _DecodedContent(revision.raw_content.replace("\x00", ""), ())

    assert isinstance(revision.raw_content, bytes)
    if revision.compression_type not in (None, 0, 4):
        return _DecodedContent(None, ("unsupported_compression",))
    raw_bytes = revision.raw_content
    if revision.compression_type == 4:
        try:
            raw_bytes = _ZSTD_DECOMPRESSOR.decompress(raw_bytes)
        except zstd.ZstdError:
            return _DecodedContent(None, ("decompression_failed",))
    decoded = _decode_utf8(raw_bytes)
    return _DecodedContent(
        decoded.text.replace("\x00", "") if decoded.text is not None else None,
        decoded.quality_codes,
    )


def _parse_xml(value: str) -> _XmlParseResult:
    if len(value) > MAX_XML_CHARACTERS:
        return _XmlParseResult(None, "xml_too_large")
    if "<!DOCTYPE" in value.upper() or "<!ENTITY" in value.upper():
        return _XmlParseResult(None, "xml_parse_failed")
    try:
        return _XmlParseResult(ET.fromstring(value), None)
    except (ET.ParseError, ValueError):
        match = _XML_ROOT_PATTERN.search(value)
        if match is None:
            return _XmlParseResult(None, "xml_parse_failed")
        try:
            return _XmlParseResult(ET.fromstring(value[match.start() :]), None)
        except (ET.ParseError, ValueError):
            return _XmlParseResult(None, "xml_parse_failed")


def _find_element(root: ET.Element | None, tag: str) -> ET.Element | None:
    if root is None:
        return None
    if root.tag == tag:
        return root
    return root.find(f".//{tag}")


def _clean_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.replace("\x00", "").strip()
    return cleaned or None


def _first_xml_text(root: ET.Element | None, paths: tuple[str, ...]) -> str | None:
    if root is None:
        return None
    for path in paths:
        node = root.find(path)
        if node is None:
            continue
        cleaned = _clean_optional_text("".join(node.itertext()))
        if cleaned is not None:
            return cleaned
    return None


def _system_profile_template(root: ET.Element | None) -> str | None:
    """Resolve only the known profile template; never render link targets or raw XML."""
    if root is None or root.tag != "sysmsg" or root.get("type") != "sysmsgtemplate":
        return None
    containers = root.findall("./sysmsgtemplate/content_template")
    if len(containers) != 1 or containers[0].get("type") != "tmpl_type_profile":
        return None
    container = containers[0]
    plain = container.findall("./plain")
    if len(plain) > 1 or (plain and len(plain[0])):
        return None
    if plain and _clean_optional_text(plain[0].text):
        return _clean_optional_text(plain[0].text)
    templates = container.findall("./template")
    lists = container.findall("./link_list")
    if len(templates) != 1 or len(templates[0]) or len(lists) != 1:
        return None
    template = _clean_optional_text(templates[0].text)
    if not template or len(template) > 8192:
        return None
    names = re.findall(r"\$([A-Za-z_][A-Za-z0-9_]{0,63})\$", template)
    if not names or len(names) > 32 or "$" in re.sub(r"\$([A-Za-z_][A-Za-z0-9_]{0,63})\$", "", template):
        return None
    links = lists[0].findall("./link")
    if len(links) > 32:
        return None
    replacements = {}
    for name in set(names):
        found = [link for link in links if link.get("name") == name]
        if len(found) != 1 or found[0].get("type") != "link_profile":
            return None
        members = found[0].findall("./memberlist/member")
        if len(members) != 1:
            return None
        usernames = members[0].findall("./username")
        nicknames = members[0].findall("./nickname")
        if len(usernames) != 1 or len(nicknames) != 1 or len(usernames[0]) or len(nicknames[0]):
            return None
        username = _clean_optional_text(usernames[0].text)
        nickname = _clean_optional_text(nicknames[0].text)
        if not username or not nickname or len(nickname) > 512:
            return None
        replacements[name] = nickname
    result = re.sub(r"\$([A-Za-z_][A-Za-z0-9_]{0,63})\$", lambda match: replacements[match.group(1)], template)
    return result


def _optional_attribute(node: ET.Element | None, *names: str) -> str | None:
    if node is None:
        return None
    for name in names:
        value = _clean_optional_text(node.get(name))
        if value is not None:
            return value
    return None


def _parse_duration_seconds(value: str | None) -> int | None:
    if value is None or re.fullmatch(r"[0-9]+", value) is None:
        return None
    duration = int(value)
    return duration if duration >= 0 else None


def _append_if_present(
    attributes: dict[str, Any], field_name: str, value: Any | None
) -> None:
    if value is not None and value != "":
        attributes[field_name] = value


def _app_content(
    decoded: str | None, quality_codes: set[str]
) -> tuple[str, dict[str, Any]]:
    root = None
    if decoded is not None and decoded != "":
        parsed = _parse_xml(decoded)
        root = parsed.root
        if parsed.quality_code is not None:
            quality_codes.add(parsed.quality_code)
    app = _find_element(root, "appmsg")
    subtype_text = _first_xml_text(app, ("type",))
    try:
        subtype = int(subtype_text) if subtype_text is not None else None
    except ValueError:
        subtype = None
    kind = APP_SUBTYPE_KINDS.get(subtype, "card")
    if subtype not in APP_SUBTYPE_KINDS:
        quality_codes.add("unknown_app_subtype")

    attributes: dict[str, Any] = {}
    title = _first_xml_text(app, ("title",))
    description = _first_xml_text(app, ("des", "description"))
    if kind in {"link", "file", "chat_bundle", "mini_app", "card"}:
        _append_if_present(attributes, "title", title)
        _append_if_present(attributes, "description", description)
    elif kind == "quote":
        _append_if_present(attributes, "title", title)
        _append_if_present(attributes, "description", description)
        _append_if_present(
            attributes,
            "referenced_actor_label",
            _first_xml_text(app, (".//refermsg/displayname",)),
        )
        _append_if_present(
            attributes,
            "referenced_text",
            _first_xml_text(app, (".//refermsg/content",)),
        )
    elif kind == "payment":
        _append_if_present(attributes, "title", title)
        _append_if_present(
            attributes,
            "amount_label",
            _first_xml_text(
                app,
                (".//wcpayinfo/feedesc", ".//wcpayinfo/fee_desc"),
            ),
        )
        _append_if_present(
            attributes,
            "memo",
            _first_xml_text(app, (".//wcpayinfo/pay_memo",)),
        )
    return kind, attributes


def _structured_content(
    base_type: int,
    decoded: str | None,
    quality_codes: set[str],
) -> tuple[str, str | None, dict[str, Any]]:
    if base_type == 49:
        kind, attributes = _app_content(decoded, quality_codes)
        return kind, None, attributes

    kind = BASE_TYPE_KINDS.get(base_type)
    if kind is None:
        quality_codes.add("unknown_message_type")
        return "unknown", None, {}
    if kind == "text":
        return kind, decoded, {}
    if kind == "video":
        attributes: dict[str, Any] = {}
        if decoded:
            parsed = _parse_xml(decoded)
            if parsed.quality_code is not None:
                quality_codes.add(parsed.quality_code)
            duration = _parse_duration_seconds(
                _optional_attribute(_find_element(parsed.root, "videomsg"), "playlength")
            )
            _append_if_present(attributes, "duration_seconds", duration)
        return kind, None, attributes
    if kind == "location":
        attributes = {}
        if decoded:
            parsed = _parse_xml(decoded)
            if parsed.quality_code is not None:
                quality_codes.add(parsed.quality_code)
            label = _optional_attribute(
                _find_element(parsed.root, "location"), "label", "poiname"
            )
            _append_if_present(attributes, "label", label)
        return kind, None, attributes
    if kind == "system":
        if decoded is None:
            return kind, None, {}
        if _XML_TAG_PATTERN.search(decoded) is None:
            return kind, decoded, {}
        parsed = _parse_xml(decoded)
        if parsed.quality_code is not None:
            quality_codes.add(parsed.quality_code)
            return kind, None, {}
        text = _first_xml_text(parsed.root, (".//content", "content"))
        return kind, text if text is not None else _system_profile_template(parsed.root), {}
    return kind, None, {}


def _occurred_at(
    create_time: int | None, quality_codes: set[str]
) -> tuple[int, bool]:
    if create_time is None or create_time == 0:
        quality_codes.add("timestamp_missing")
        return 0, False
    if (
        create_time < SOURCE_TIMESTAMP_LOWER_BOUND
        or create_time >= SOURCE_TIMESTAMP_UPPER_BOUND
    ):
        quality_codes.add("timestamp_out_of_range")
        return create_time, False
    return create_time, True


def canonical_json(value: Mapping[str, Any]) -> str:
    """Serialize a Standard Event structure deterministically as UTF-8 JSON."""

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def canonical_payload_sha256(canonical_payload_json: str) -> str:
    """Return the SHA-256 of exact canonical UTF-8 payload bytes."""

    if not isinstance(canonical_payload_json, str):
        raise TypeError("canonical_payload_json must be text")
    return hashlib.sha256(canonical_payload_json.encode("utf-8")).hexdigest()


def normalize_event(
    event: RawEventInput,
    revision: RawRevisionInput,
    config: NormalizerConfig,
) -> StandardEventResult:
    """Purely normalize one durable raw event and exact immutable revision."""

    if not isinstance(event, RawEventInput):
        raise TypeError("event must be RawEventInput")
    if not isinstance(revision, RawRevisionInput):
        raise TypeError("revision must be RawRevisionInput")
    if not isinstance(config, NormalizerConfig):
        raise TypeError("config must be NormalizerConfig")

    subject_id = _subject_id(event)
    event_id = _event_id(event, revision)
    stream_id = _stream_id(event)
    conversation_id = _conversation_id(event)
    decoded = _decode_raw_content(revision)
    quality_codes = {
        "conversation_unresolved",
        "actor_unresolved",
        *decoded.quality_codes,
    }
    base_type = revision.local_type & 0xFFFFFFFF
    kind, text, attributes = _structured_content(
        base_type, decoded.text, quality_codes
    )
    occurred_at_unix_seconds, occurred_at_valid = _occurred_at(
        revision.create_time, quality_codes
    )
    ordered_quality_codes = [
        code for code in QUALITY_CODE_ORDER if code in quality_codes
    ]
    quality_status = (
        "degraded"
        if any(code in DEGRADED_QUALITY_CODES for code in quality_codes)
        else "normalized"
    )
    actor_role = "system" if kind == "system" else "unknown"
    event_type = (
        "message.observed"
        if revision.revision_number == 1
        else "message.revision_observed"
    )

    payload = {
        "spec_version": config.contract_version,
        "id": event_id,
        "type": event_type,
        "subject_id": subject_id,
        "sequence": revision.revision_number,
        "normalization": {
            "normalizer": config.normalizer_name,
            "normalizer_version": config.normalizer_version,
        },
        "source": {
            "provider": WECHAT_PROVIDER,
            "adapter": WECHAT_NORMALIZER_NAME,
            "stream_id": stream_id,
        },
        "conversation": {"id": conversation_id, "kind": "unknown"},
        "actor": {
            "id": None,
            "role": actor_role,
            "resolution": "unresolved",
        },
        "time": {
            "occurred_at_unix_seconds": occurred_at_unix_seconds,
            "occurred_at_valid": occurred_at_valid,
            "observed_at": revision.observed_at,
        },
        "content": {"kind": kind, "text": text, "attributes": attributes},
        "quality": {"status": quality_status, "codes": ordered_quality_codes},
        "provenance": {
            "raw_event_uid": event.event_uid,
            "raw_revision_number": revision.revision_number,
            "raw_revision_fingerprint": revision.content_fingerprint,
        },
    }
    serialized = canonical_json(payload)
    return StandardEventResult(
        event_id=event_id,
        subject_id=subject_id,
        stream_id=stream_id,
        conversation_id=conversation_id,
        contract_version=config.contract_version,
        normalizer_name=config.normalizer_name,
        normalizer_version=config.normalizer_version,
        canonical_payload_json=serialized,
        payload_sha256=canonical_payload_sha256(serialized),
        quality_status=quality_status,
    )
