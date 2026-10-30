import unittest
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from wechat_local.codex_first import (
    CodexFirstError,
    CodexFirstService,
    QueryRunAnchor,
    ScanCoverage,
    parse_time_window,
)


class CodexFirstTests(unittest.TestCase):
    NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
    SHANGHAI = ZoneInfo("Asia/Shanghai")

    def test_package_import_does_not_eagerly_load_runtime_dependencies(self):
        environment = dict(os.environ)
        environment["PYTHONPATH"] = "src"
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys, wechat_local; "
                "print(int('wechat_local.codex_first_runtime' in sys.modules))",
            ],
            cwd=os.getcwd(),
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )

        self.assertEqual(completed.stdout.strip(), "0")

    def test_omitted_range_defaults_to_three_local_hours(self):
        result = parse_time_window("看看谁找过我", now=self.NOW, timezone=self.SHANGHAI)
        self.assertEqual(result.start, self.NOW - timedelta(hours=3))
        self.assertEqual(result.end, self.NOW)

    def test_recent_any_positive_integer_hours_is_supported(self):
        self.assertEqual(
            parse_time_window("最近7小时", now=self.NOW, timezone=self.SHANGHAI).start,
            self.NOW - timedelta(hours=7),
        )

    def test_recent_minutes_days_and_weeks_are_supported(self):
        cases = (
            ("最近45分钟", timedelta(minutes=45)),
            ("最近2天", timedelta(days=2)),
            ("最近3周", timedelta(weeks=3)),
        )
        for request, span in cases:
            with self.subTest(request=request):
                result = parse_time_window(request, now=self.NOW, timezone=self.SHANGHAI)
                self.assertEqual((result.start, result.end), (self.NOW - span, self.NOW))

    def test_yesterday_today_and_this_week_use_local_calendar_boundaries(self):
        today = parse_time_window("今天", now=self.NOW, timezone=self.SHANGHAI)
        yesterday = parse_time_window("昨天", now=self.NOW, timezone=self.SHANGHAI)
        this_week = parse_time_window("本周", now=self.NOW, timezone=self.SHANGHAI)
        self.assertEqual(today.start.astimezone(self.SHANGHAI).hour, 0)
        self.assertEqual(today.end, self.NOW)
        self.assertEqual(yesterday.end.astimezone(self.SHANGHAI).hour, 0)
        self.assertEqual(yesterday.end - yesterday.start, timedelta(days=1))
        self.assertEqual(this_week.start.astimezone(self.SHANGHAI).weekday(), 0)
        self.assertEqual(this_week.start.astimezone(self.SHANGHAI).hour, 0)

    def test_invalid_hour_is_rejected(self):
        for request in ("最近0小时", "最近-1小时", "最近几小时"):
            with self.assertRaises(CodexFirstError):
                parse_time_window(request, now=self.NOW, timezone=self.SHANGHAI)

    def test_multiple_negated_or_corrected_ranges_fail_closed(self):
        ambiguous = (
            "不要查最近24小时，只查最近1小时谁找过我",
            "最近2小时，不对，最近5小时",
            "从2026-09-28 09:00到2026-09-28 10:00，最近1小时",
            "不要查最近1小时",
        )
        for request in ambiguous:
            with self.subTest(request=request), self.assertRaisesRegex(
                CodexFirstError, "ambiguous"
            ):
                parse_time_window(request, now=self.NOW, timezone=self.SHANGHAI)

    def test_supported_range_mixed_with_unsupported_time_phrase_fails_closed(self):
        ambiguous = (
            "最近1小时，昨天也一起查",
            "今天上午，以及昨晚九点以后",
        )
        for request in ambiguous:
            with self.subTest(request=request), self.assertRaisesRegex(
                CodexFirstError, "ambiguous"
            ):
                parse_time_window(request, now=self.NOW, timezone=self.SHANGHAI)

    def test_exact_range_and_morning_range(self):
        exact = parse_time_window(
            "从2026-09-28 09:00到2026-09-28 10:30",
            now=self.NOW,
            timezone=self.SHANGHAI,
        )
        self.assertEqual(exact.end - exact.start, timedelta(hours=1, minutes=30))
        self.assertEqual(exact.start.isoformat(), "2026-09-28T01:00:00+00:00")
        self.assertEqual(exact.end.isoformat(), "2026-09-28T02:30:00+00:00")
        morning = parse_time_window("今天上午", now=self.NOW, timezone=self.SHANGHAI)
        self.assertEqual(morning.start.astimezone(self.SHANGHAI).hour, 0)
        self.assertEqual(morning.end.astimezone(self.SHANGHAI).hour, 12)

    def test_fixed_confirmation_range_round_trips_microseconds(self):
        fixed = parse_time_window(
            "从2026-09-28 20:00:00.123456 到 2026-09-29 20:00:00.654321",
            now=self.NOW,
            timezone=self.SHANGHAI,
        )
        self.assertEqual(fixed.start.microsecond, 123456)
        self.assertEqual(fixed.end.microsecond, 654321)

    def test_end_before_start_is_rejected(self):
        with self.assertRaisesRegex(CodexFirstError, "before"):
            parse_time_window(
                "从2026-09-28 10:30到2026-09-28 09:00",
                now=self.NOW,
                timezone=self.SHANGHAI,
            )

    def test_coverage_gaps_and_degraded_placeholder_are_preserved(self):
        ref = {"event_id": "evt", "conversation_id": "conv", "occurred_at": "2026-09-28T10:00:00+00:00", "citation_text": "unknown message type", "content_kind": "unknown", "quality_status": "degraded"}
        coverage = ScanCoverage(gaps=("source unavailable",))
        result = CodexFirstService(direction_capability=True).render([ref], coverage=coverage)
        self.assertEqual(result.coverage.gaps, ("source unavailable",))
        self.assertEqual(result.groups[0].messages[0].text, "unknown message type")

    def test_coverage_exposes_explicit_tri_state(self):
        empty = CodexFirstService().render([], coverage=ScanCoverage())
        partial = CodexFirstService().render([], coverage=ScanCoverage(gaps=("source_read_unavailable",)))
        unavailable = CodexFirstService().render(
            [], coverage=ScanCoverage(source_capability="unavailable")
        )
        self.assertEqual(empty.coverage.status, "covered_empty")
        self.assertEqual(partial.coverage.status, "partial")
        self.assertEqual(unavailable.coverage.status, "unavailable")

    def test_scan_result_exposes_the_requested_range_from_coverage(self):
        start = datetime(2026, 9, 28, 9, 0, tzinfo=timezone.utc)
        end = datetime(2026, 9, 28, 10, 0, tzinfo=timezone.utc)
        result = CodexFirstService(direction_capability=True).render(
            [],
            coverage=ScanCoverage(requested_start=start, requested_end=end),
        )

        self.assertEqual(result.requested_range, (start, end))

    def test_grouping_limit_is_bounded(self):
        events = [
            {"event_id": "evt_2", "conversation_id": "conv_a", "occurred_at": "2026-09-28T10:02:00+00:00", "citation_text": "two", "content_kind": "text", "event_type": "text", "quality_status": "normalized"},
            {"event_id": "evt_1", "conversation_id": "conv_a", "occurred_at": "2026-09-28T10:01:00+00:00", "citation_text": "one", "content_kind": "text", "event_type": "text", "quality_status": "normalized"},
            {"event_id": "evt_3", "conversation_id": "conv_a", "occurred_at": "2026-09-28T10:03:00+00:00", "citation_text": "three", "content_kind": "text", "event_type": "text", "quality_status": "degraded"},
        ]
        service = CodexFirstService(direction_capability=True)
        rendered = service.render(events, coverage=ScanCoverage(), display_limit=2)
        self.assertEqual(rendered.groups[0].total_messages, 3)
        self.assertEqual([m.event_id for m in rendered.groups[0].messages], ["evt_1", "evt_2"])
        self.assertTrue(rendered.groups[0].has_more)

    def test_groups_order_by_latest_activity_descending_with_deterministic_ties(self):
        events = [
            {"event_id": "a", "conversation_id": "older", "occurred_at": "2026-09-28T10:00:00+00:00"},
            {"event_id": "b", "conversation_id": "newer", "occurred_at": "2026-09-28T10:02:00+00:00"},
            {"event_id": "c", "conversation_id": "tied", "occurred_at": "2026-09-28T10:02:00+00:00"},
        ]
        result = CodexFirstService().render(events, coverage=ScanCoverage())
        self.assertEqual([group.conversation_id for group in result.groups], ["newer", "tied", "older"])

    def test_display_offset_has_no_overlap_and_exposes_continuation_offset(self):
        events = [
            {"event_id": f"evt-{index}", "conversation_id": "conv", "occurred_at": f"2026-09-28T10:0{index}:00+00:00"}
            for index in range(4)
        ]
        service = CodexFirstService()
        first = service.render(events, coverage=ScanCoverage(), display_limit=2, display_offset=0)
        second = service.render(events, coverage=ScanCoverage(), display_limit=2, display_offset=2)
        self.assertEqual([message.event_id for message in first.groups[0].messages], ["evt-0", "evt-1"])
        self.assertEqual([message.event_id for message in second.groups[0].messages], ["evt-2", "evt-3"])
        self.assertEqual(first.groups[0].continuation_offset, 2)
        self.assertIsNone(second.groups[0].continuation_offset)

    def test_query_run_anchor_keeps_later_pages_stable_when_new_messages_arrive(self):
        initial = [
            {"event_id": "evt-1", "conversation_id": "conv", "occurred_at": "2026-09-28T10:01:00+00:00"},
            {"event_id": "evt-2", "conversation_id": "conv", "occurred_at": "2026-09-28T10:02:00+00:00"},
            {"event_id": "evt-3", "conversation_id": "conv", "occurred_at": "2026-09-28T10:03:00+00:00"},
        ]
        service = CodexFirstService()
        first = service.render(initial, coverage=ScanCoverage(), display_limit=2)
        self.assertIsInstance(first.query_anchor, QueryRunAnchor)

        later = service.render(
            initial + [{"event_id": "evt-new", "conversation_id": "conv", "occurred_at": "2026-09-28T10:04:00+00:00"}],
            coverage=ScanCoverage(),
            display_limit=2,
            display_offset=2,
            query_anchor=first.query_anchor,
        )

        self.assertEqual([message.event_id for message in later.groups[0].messages], ["evt-3"])

    def test_unresolved_direction_is_retained_as_safe_degradation(self):
        ref = {"event_id": "evt", "conversation_id": "conv", "occurred_at": "2026-09-28T10:00:00+00:00", "direction": "unresolved"}
        result = CodexFirstService().render([ref], coverage=ScanCoverage())
        self.assertIsNone(result.error_code)
        self.assertEqual(result.total_messages, 1)
        self.assertEqual(result.groups[0].messages[0].direction, "unknown")
        self.assertEqual(result.groups[0].display_name, "会话 1")

    def test_missing_direction_evidence_is_visible_by_default(self):
        ref = {"event_id": "evt", "conversation_id": "conv", "occurred_at": "2026-09-28T10:00:00+00:00"}
        result = CodexFirstService().render([ref], coverage=ScanCoverage())
        self.assertEqual(result.total_messages, 1)
        self.assertEqual(result.groups[0].messages[0].direction, "unknown")

    def test_dst_gap_and_fold_are_rejected(self):
        eastern = ZoneInfo("America/New_York")
        for request in (
            "从2026-03-08 02:00到2026-03-08 03:00",
            "从2026-11-01 01:00到2026-11-01 02:00",
        ):
            with self.assertRaises(CodexFirstError):
                parse_time_window(request, now=self.NOW, timezone=eastern)


if __name__ == "__main__":
    unittest.main()
