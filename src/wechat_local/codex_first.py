"""Deterministic, no-send Codex-first result contract.

This module is deliberately pure: it accepts already projected EventRefs and
never opens a source database, calls a provider, or infers identity/direction.
"""

from __future__ import annotations

import re
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from datetime import timezone as dt_timezone
from typing import Any, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo


class CodexFirstError(ValueError):
    """A controlled, user-safe Codex-first request/result error."""


@dataclass(frozen=True)
class TimeWindow:
    start: datetime
    end: datetime
    requested_text: str

    def __post_init__(self) -> None:
        for value, name in ((self.start, "start"), (self.end, "end")):
            if value.tzinfo is None or value.utcoffset() is None:
                raise CodexFirstError(f"{name} must be timezone-aware")
            if value.utcoffset() != timedelta(0):
                raise CodexFirstError(f"{name} must be UTC")
        if self.end < self.start:
            raise CodexFirstError("time range end must not be before start")
        if not isinstance(self.requested_text, str):
            raise CodexFirstError("requested text must be text")

    @classmethod
    def from_utc(cls, start: datetime, end: datetime, requested_text: str = "") -> "TimeWindow":
        def utc(value: datetime) -> datetime:
            if value.tzinfo is None or value.utcoffset() is None:
                raise CodexFirstError("time bounds must be timezone-aware")
            return value.astimezone(timezone.utc)
        return cls(utc(start), utc(end), requested_text)


def _local_now(now: datetime, tz: ZoneInfo) -> datetime:
    if now.tzinfo is None or now.utcoffset() is None:
        return now.replace(tzinfo=tz)
    return now.astimezone(tz)


def _localize(value: datetime, tz: ZoneInfo) -> datetime:
    candidates = [value.replace(tzinfo=tz, fold=fold).astimezone(timezone.utc) for fold in (0, 1)]
    round_trips = [candidate.astimezone(tz).replace(tzinfo=None) for candidate in candidates]
    if any(round_trip != value for round_trip in round_trips):
        raise CodexFirstError("time range contains a nonexistent local time")
    if candidates[0] != candidates[1]:
        raise CodexFirstError("time range contains an ambiguous local time")
    return candidates[0]


def _parse_wall(text: str, tz: ZoneInfo, *, default_date: datetime) -> datetime:
    text = text.strip()
    formats = (
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y/%m/%d %H:%M:%S.%f",
        "%m月%d日 %H:%M",
    )
    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt.startswith("%m月"):
                parsed = parsed.replace(year=default_date.year)
            return _localize(parsed, tz)
        except ValueError:
            continue
    raise CodexFirstError("unsupported or ambiguous time range")


def parse_time_window(request: str, *, now: datetime, timezone: ZoneInfo) -> TimeWindow:
    """Parse the explicitly supported Chinese time forms into UTC bounds."""
    if not isinstance(request, str):
        raise CodexFirstError("request must be text")
    local_now = _local_now(now, timezone)
    text = request.strip()
    if not text:
        raise CodexFirstError("request must not be empty")

    prefix = r"(?:(?:请|麻烦)\s*)?(?:帮我\s*)?(?:(?:看看|看下|查一下|查下|查询)\s*)?"
    suffix = r"\s*(?:，|,)?\s*(?:谁找过我|谁找我|谁联系过我|谁联系我)?\s*[?？。]?"
    recent = re.fullmatch(prefix + r"最近\s*(-?\d+)\s*(分钟|小时|天|周)" + suffix, text)
    if recent:
        quantity = int(recent.group(1))
        if quantity <= 0:
            raise CodexFirstError("time quantity must be a positive integer")
        unit = recent.group(2)
        span = {
            "分钟": timedelta(minutes=quantity),
            "小时": timedelta(hours=quantity),
            "天": timedelta(days=quantity),
            "周": timedelta(weeks=quantity),
        }[unit]
        end = local_now.astimezone(dt_timezone.utc)
        return TimeWindow.from_utc(end - span, end, text)

    if re.fullmatch(prefix + r"今天上午" + suffix, text):
        day = local_now.date()
        start = datetime.combine(day, time(0), timezone)
        end = datetime.combine(day, time(12), timezone)
        return TimeWindow.from_utc(start, end, text)

    if re.fullmatch(prefix + r"今天" + suffix, text):
        start = datetime.combine(local_now.date(), time(0), timezone)
        return TimeWindow.from_utc(start, local_now, text)

    if re.fullmatch(prefix + r"昨天" + suffix, text):
        end = datetime.combine(local_now.date(), time(0), timezone)
        return TimeWindow.from_utc(end - timedelta(days=1), end, text)

    if re.fullmatch(prefix + r"(?:本周|这周)" + suffix, text):
        start_day = local_now.date() - timedelta(days=local_now.weekday())
        start = datetime.combine(start_day, time(0), timezone)
        return TimeWindow.from_utc(start, local_now, text)

    # Absolute local wall-clock range. Date separators and optional spaces are
    # intentionally narrow to avoid silently guessing an ambiguous request.
    absolute = re.fullmatch(
        prefix
        +
        r"从\s*(\d{4}[-/]\d{1,2}[-/]\d{1,2}\s+\d{1,2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?)\s*到\s*"
        r"(\d{4}[-/]\d{1,2}[-/]\d{1,2}\s+\d{1,2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?)"
        + suffix,
        text,
    )
    if absolute:
        start = _parse_wall(absolute.group(1), timezone, default_date=local_now)
        end = _parse_wall(absolute.group(2), timezone, default_date=local_now)
        return TimeWindow.from_utc(start, end, text)

    if re.fullmatch(prefix + r"从昨晚\s*十点到现在" + suffix, text):
        yesterday = (local_now - timedelta(days=1)).date()
        start = datetime.combine(yesterday, time(22), timezone)
        return TimeWindow.from_utc(start, local_now, text)

    default_request = (
        r"(?:(?:请|麻烦)\s*)?(?:帮我\s*)?"
        r"(?:(?:看看|看下|查一下|查下|查询)\s*)?"
        r"(?:谁找过我|谁找我|谁联系过我|谁联系我)\s*[?？。]?"
    )
    if re.fullmatch(default_request, text):
        end = local_now.astimezone(dt_timezone.utc)
        return TimeWindow.from_utc(end - timedelta(hours=3), end, text)
    raise CodexFirstError("unsupported or ambiguous time range")


@dataclass(frozen=True)
class ScanCoverage:
    requested_start: Optional[datetime] = None
    requested_end: Optional[datetime] = None
    actual_start: Optional[datetime] = None
    actual_end: Optional[datetime] = None
    gaps: tuple[str, ...] = ()
    source_capability: str = "available"

    @property
    def complete(self) -> bool:
        return not self.gaps and self.source_capability == "available"

    @property
    def status(self) -> str:
        if self.source_capability != "available":
            return "unavailable"
        if self.gaps:
            return "partial"
        return "covered_empty"


@dataclass(frozen=True)
class GroupedMessage:
    event_id: str
    conversation_id: str
    occurred_at: datetime
    text: str
    event_ref: Mapping[str, Any]
    quality_status: str = "normalized"
    content_kind: str = "text"
    event_type: str = "text"
    direction: str = "unknown"


@dataclass(frozen=True)
class MessageGroup:
    conversation_id: str
    messages: tuple[GroupedMessage, ...]
    total_messages: int
    has_more: bool
    display_name: Optional[str] = None
    continuation_offset: Optional[int] = None
    conversation_kind: str = "unknown"


@dataclass(frozen=True)
class QueryRunAnchor:
    """Finite, content-free identity boundary for one query run."""

    event_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.event_ids or any(not isinstance(value, str) or not value for value in self.event_ids):
            raise CodexFirstError("query anchor must contain stable EventRef identities")
        if len(set(self.event_ids)) != len(self.event_ids):
            raise CodexFirstError("query anchor contains duplicate EventRef identities")


@dataclass(frozen=True)
class ScanResult:
    groups: tuple[MessageGroup, ...]
    coverage: ScanCoverage
    total_messages: int
    error_code: Optional[str] = None
    status: str = "ok"
    query_anchor: Optional[QueryRunAnchor] = None

    @property
    def source_capability_unavailable(self) -> bool:
        return self.error_code == "source_capability_unavailable"

    @property
    def requested_range(self) -> tuple[Optional[datetime], Optional[datetime]]:
        """Return the exact request bounds carried by the coverage record."""

        return (self.coverage.requested_start, self.coverage.requested_end)


def _occurred(value: Any) -> datetime:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        try:
            result = datetime.fromisoformat(value)
        except ValueError as exc:
            raise CodexFirstError("EventRef occurred_at is invalid") from exc
    else:
        raise CodexFirstError("EventRef occurred_at is invalid")
    if result.tzinfo is None or result.utcoffset() is None:
        raise CodexFirstError("EventRef occurred_at must be timezone-aware")
    return result


class CodexFirstService:
    """Group safe EventRefs into a bounded entry-neutral query result."""

    def __init__(self, display_name_resolver: Any = None, *, direction_capability: bool = False) -> None:
        self.display_name_resolver = display_name_resolver
        self.direction_capability = direction_capability

    def _name(self, conversation_id: str) -> Optional[str]:
        resolver = self.display_name_resolver
        if resolver is None:
            return None
        if callable(resolver):
            value = resolver(conversation_id)
        elif isinstance(resolver, Mapping):
            value = resolver.get(conversation_id)
        else:
            value = None
        return value if isinstance(value, str) and value else None

    @staticmethod
    def _is_unresolved_direction(ref: Mapping[str, Any]) -> bool:
        value = ref.get("direction", ref.get("inbound"))
        return value in (None, "unknown", "unresolved") if "direction" in ref or "inbound" in ref else False

    @staticmethod
    def _direction(ref: Mapping[str, Any]) -> str:
        value = ref.get("direction", ref.get("inbound"))
        if value in ("inbound", "outbound", "self", "other"):
            return str(value)
        return "unknown"

    def render(self, event_refs: Sequence[Mapping[str, Any]], *, coverage: ScanCoverage, display_limit: int = 50, display_offset: int = 0, query_anchor: Optional[QueryRunAnchor] = None) -> ScanResult:
        if display_limit < 1 or display_offset < 0:
            raise CodexFirstError("display limit and offset must be valid")
        refs = list(event_refs)
        if query_anchor is not None:
            if not isinstance(query_anchor, QueryRunAnchor):
                raise CodexFirstError("query_anchor must be a QueryRunAnchor")
            allowed = set(query_anchor.event_ids)
            refs = [ref for ref in refs if str(ref.get("event_id", "")) in allowed]
        groups: "OrderedDict[str, list[GroupedMessage]]" = OrderedDict()
        identities = {str(ref.get("event_id")) for ref in refs if ref.get("event_id")}
        if len(identities) != len(refs):
            raise CodexFirstError("duplicate or missing EventRef identity")
        for ref in sorted(refs, key=lambda item: (_occurred(item.get("occurred_at")), str(item.get("event_id", "")))):
            event_id = str(ref.get("event_id", ""))
            conversation_id = str(ref.get("conversation_id", ""))
            if not event_id or not conversation_id:
                raise CodexFirstError("EventRef is missing stable identity")
            groups.setdefault(conversation_id, []).append(
                GroupedMessage(
                    event_id=event_id,
                    conversation_id=conversation_id,
                    occurred_at=_occurred(ref.get("occurred_at")),
                    text=str(ref.get("citation_text", "")),
                    event_ref=ref,
                    quality_status=str(ref.get("quality_status", "unknown")),
                    content_kind=str(ref.get("content_kind", "unknown")),
                    event_type=str(ref.get("event_type", "unknown")),
                    direction=self._direction(ref),
                )
            )
        result_groups = []
        ordered_groups = sorted(
            groups.items(),
            key=lambda item: (
                -max(message.occurred_at.timestamp() for message in item[1]),
                item[0],
            ),
        )
        for alias_number, (conversation_id, messages) in enumerate(ordered_groups, start=1):
            visible = tuple(messages[display_offset : display_offset + display_limit])
            display_name = self._name(conversation_id) or f"会话 {alias_number}"
            next_offset = display_offset + len(visible)
            continuation_offset = next_offset if next_offset < len(messages) else None
            result_groups.append(MessageGroup(conversation_id, visible, len(messages), continuation_offset is not None, display_name, continuation_offset))
        anchor = query_anchor or (QueryRunAnchor(tuple(sorted(identities))) if identities else None)
        return ScanResult(tuple(result_groups), coverage, len(refs), query_anchor=anchor)
