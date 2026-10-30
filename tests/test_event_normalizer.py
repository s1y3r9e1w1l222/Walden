from __future__ import annotations

import hashlib
import json
import math
import unittest
from dataclasses import fields, replace

import zstandard as zstd

from wechat_local.event_normalizer import (
    MAX_XML_CHARACTERS,
    NormalizerConfig,
    RawEventInput,
    RawRevisionInput,
    canonical_json,
    normalize_event,
)


OBSERVED_AT = "2026-08-29T01:02:03.000004+00:00"


class EventNormalizerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.event = RawEventInput(
            event_uid="raw-event-uid-synthetic",
            account_id="synthetic-account",
            source_generation="synthetic-generation-1",
            shard_name="message_0.db",
            table_name="Msg_00000000000000000000000000000000",
            local_id=7,
        )
        self.config = NormalizerConfig(
            contract_version="wechat-local.standard-event/1",
            normalizer_name="wechat-macos-v4",
            normalizer_version="1",
        )

    def _revision(
        self,
        *,
        fingerprint: str = "raw-fingerprint-1",
        revision_number: int = 1,
        server_id: str | None = "1000",
        local_type: int = 1,
        create_time: int | None = 1_700_000_000,
        real_sender_id: int | None = 42,
        raw_content: str | bytes | None = "synthetic text",
        raw_content_kind: str = "text",
        compression_type: int | None = 0,
        observed_at: str = OBSERVED_AT,
    ) -> RawRevisionInput:
        return RawRevisionInput(
            content_fingerprint=fingerprint,
            revision_number=revision_number,
            server_id=server_id,
            local_type=local_type,
            create_time=create_time,
            real_sender_id=real_sender_id,
            raw_content=raw_content,
            raw_content_kind=raw_content_kind,
            compression_type=compression_type,
            observed_at=observed_at,
        )

    def _normalize(self, revision: RawRevisionInput, event=None, config=None):
        return normalize_event(
            event or self.event,
            revision,
            config or self.config,
        )

    @staticmethod
    def _payload(result):
        return json.loads(result.canonical_payload_json)

    @staticmethod
    def _app_xml(subtype: int | str | None, extra: str = "") -> str:
        subtype_xml = "" if subtype is None else f"<type>{subtype}</type>"
        return (
            "<msg><appmsg>"
            f"{subtype_xml}<title>Synthetic title</title>"
            "<des>Synthetic description</des>"
            f"{extra}</appmsg></msg>"
        )

    def test_same_explicit_input_is_byte_deterministic(self) -> None:
        revision = self._revision(raw_content="确定性\n文本")
        first = self._normalize(revision)
        second = self._normalize(revision)

        self.assertEqual(first, second)
        self.assertEqual(first.canonical_payload_json, second.canonical_payload_json)
        self.assertEqual(first.payload_sha256, second.payload_sha256)
        self.assertEqual(
            first.payload_sha256,
            hashlib.sha256(first.canonical_payload_json.encode("utf-8")).hexdigest(),
        )
        self.assertIn("确定性", first.canonical_payload_json)
        self.assertNotIn("\\u786e", first.canonical_payload_json)

    def test_normalizer_version_changes_projection_but_not_logical_ids(self) -> None:
        revision = self._revision()
        v1 = self._normalize(revision)
        v2 = self._normalize(
            revision,
            config=replace(self.config, normalizer_version="2"),
        )

        self.assertEqual(v1.event_id, v2.event_id)
        self.assertEqual(v1.subject_id, v2.subject_id)
        self.assertEqual(v1.stream_id, v2.stream_id)
        self.assertEqual(v1.conversation_id, v2.conversation_id)
        self.assertNotEqual(v1.canonical_payload_json, v2.canonical_payload_json)
        self.assertNotEqual(v1.payload_sha256, v2.payload_sha256)
        self.assertEqual(
            self._payload(v2)["normalization"]["normalizer_version"], "2"
        )

    def test_contract_and_normalizer_are_explicit_and_pinned(self) -> None:
        with self.assertRaisesRegex(ValueError, "contract version"):
            NormalizerConfig(
                contract_version="unsupported/2",
                normalizer_name="wechat-macos-v4",
                normalizer_version="1",
            )
        with self.assertRaisesRegex(ValueError, "normalizer name"):
            NormalizerConfig(
                contract_version="wechat-local.standard-event/1",
                normalizer_name="other-adapter",
                normalizer_version="1",
            )

    def test_standard_event_v1_envelope_has_exact_approved_sections(self) -> None:
        payload = self._payload(self._normalize(self._revision()))

        self.assertEqual(
            set(payload),
            {
                "spec_version",
                "id",
                "type",
                "subject_id",
                "sequence",
                "normalization",
                "source",
                "conversation",
                "actor",
                "time",
                "content",
                "quality",
                "provenance",
            },
        )
        self.assertEqual(
            payload["normalization"],
            {"normalizer": "wechat-macos-v4", "normalizer_version": "1"},
        )
        self.assertEqual(set(payload["source"]), {"provider", "adapter", "stream_id"})
        self.assertEqual(payload["source"]["provider"], "wechat")
        self.assertEqual(payload["source"]["adapter"], "wechat-macos-v4")
        self.assertEqual(payload["conversation"]["kind"], "unknown")
        self.assertNotIn("context_version", payload["normalization"])
        self.assertNotIn("context_fingerprint", payload["normalization"])

    def test_canonical_json_is_sorted_compact_and_rejects_nonfinite_numbers(self) -> None:
        serialized = canonical_json({"z": "中文", "a": [2, 1]})
        self.assertEqual(serialized, '{"a":[2,1],"z":"中文"}')
        for nonfinite in (math.nan, math.inf, -math.inf):
            with self.subTest(nonfinite=nonfinite):
                with self.assertRaises(ValueError):
                    canonical_json({"value": nonfinite})

    def test_revisions_share_subject_but_have_distinct_event_ids(self) -> None:
        first = self._normalize(self._revision())
        second = self._normalize(
            self._revision(fingerprint="raw-fingerprint-2", revision_number=2)
        )

        self.assertEqual(first.subject_id, second.subject_id)
        self.assertNotEqual(first.event_id, second.event_id)

    def test_event_id_ignores_server_sender_and_has_no_revision_row_id_input(self) -> None:
        baseline = self._normalize(self._revision())
        changed_physical_metadata = self._normalize(
            self._revision(server_id="999999", real_sender_id=777)
        )

        self.assertEqual(baseline.event_id, changed_physical_metadata.event_id)
        self.assertEqual(
            baseline.canonical_payload_json,
            changed_physical_metadata.canonical_payload_json,
        )
        self.assertNotIn("source_revision_id", {field.name for field in fields(RawRevisionInput)})

    def test_conversation_ignores_shard_but_changes_with_generation(self) -> None:
        revision = self._revision()
        baseline = self._normalize(revision)
        other_shard = self._normalize(
            revision,
            event=replace(self.event, shard_name="message_99.db"),
        )
        other_generation = self._normalize(
            revision,
            event=replace(self.event, source_generation="synthetic-generation-2"),
        )

        self.assertEqual(baseline.conversation_id, other_shard.conversation_id)
        self.assertEqual(baseline.stream_id, other_shard.stream_id)
        self.assertNotEqual(
            baseline.conversation_id, other_generation.conversation_id
        )
        self.assertNotEqual(baseline.stream_id, other_generation.stream_id)

    def test_standard_id_domains_and_prefixes_are_distinct(self) -> None:
        result = self._normalize(self._revision())
        identifiers = (
            result.subject_id,
            result.event_id,
            result.stream_id,
            result.conversation_id,
        )

        self.assertEqual(len(set(identifiers)), 4)
        self.assertTrue(result.subject_id.startswith("msg_"))
        self.assertTrue(result.event_id.startswith("evt_"))
        self.assertTrue(result.stream_id.startswith("stream_"))
        self.assertTrue(result.conversation_id.startswith("conv_"))

    def test_length_prefixing_prevents_ambiguous_identity_concatenation(self) -> None:
        revision = self._revision()
        first = self._normalize(
            revision,
            event=replace(
                self.event,
                account_id="ab",
                source_generation="c",
            ),
        )
        second = self._normalize(
            revision,
            event=replace(
                self.event,
                account_id="a",
                source_generation="bc",
            ),
        )

        self.assertNotEqual(first.stream_id, second.stream_id)
        self.assertNotEqual(first.conversation_id, second.conversation_id)

    def test_revision_lifecycle_is_conservative_including_recall(self) -> None:
        observed = self._payload(self._normalize(self._revision()))
        revised = self._payload(
            self._normalize(self._revision(revision_number=2))
        )
        recall = self._payload(
            self._normalize(
                self._revision(
                    revision_number=2,
                    local_type=10_002,
                    raw_content="<sysmsg><revokemsg/></sysmsg>",
                )
            )
        )

        self.assertEqual(observed["type"], "message.observed")
        self.assertEqual(revised["type"], "message.revision_observed")
        self.assertEqual(recall["type"], "message.revision_observed")
        self.assertEqual(recall["content"]["kind"], "recall")
        self.assertNotEqual(recall["type"], "message.recalled")

    def test_all_approved_base_types_map_to_contract_kinds(self) -> None:
        cases = {
            1: ("text", "synthetic", "text"),
            3: ("image", None, "null"),
            34: ("audio", None, "null"),
            42: ("contact", None, "null"),
            43: ("video", None, "null"),
            47: ("sticker", None, "null"),
            48: ("location", None, "null"),
            49: ("link", self._app_xml(5), "text"),
            50: ("call", None, "null"),
            10_000: ("system", "synthetic system", "text"),
            10_002: ("recall", "synthetic recall", "text"),
        }
        for base_type, (expected, raw_content, raw_kind) in cases.items():
            with self.subTest(base_type=base_type):
                payload = self._payload(
                    self._normalize(
                        self._revision(
                            local_type=base_type,
                            raw_content=raw_content,
                            raw_content_kind=raw_kind,
                        )
                    )
                )
                self.assertEqual(payload["content"]["kind"], expected)

    def test_low_32_bit_masking_handles_signed_and_high_bit_types(self) -> None:
        high_bit = self._payload(
            self._normalize(self._revision(local_type=(1 << 40) | 34))
        )
        signed = self._payload(
            self._normalize(self._revision(local_type=-4_294_967_295))
        )

        self.assertEqual(high_bit["content"]["kind"], "audio")
        self.assertEqual(signed["content"]["kind"], "text")

    def test_unknown_base_type_is_degraded_without_invented_text(self) -> None:
        payload = self._payload(
            self._normalize(self._revision(local_type=777, raw_content="private raw"))
        )

        self.assertEqual(payload["content"], {
            "kind": "unknown", "text": None, "attributes": {}
        })
        self.assertEqual(payload["quality"]["status"], "degraded")
        self.assertIn("unknown_message_type", payload["quality"]["codes"])
        self.assertNotIn("private raw", json.dumps(payload, ensure_ascii=False))

    def test_all_approved_app_subtypes_map(self) -> None:
        cases = {
            5: "link",
            6: "file",
            19: "chat_bundle",
            33: "mini_app",
            36: "mini_app",
            57: "quote",
            2_000: "payment",
        }
        for subtype, expected in cases.items():
            with self.subTest(subtype=subtype):
                payload = self._payload(
                    self._normalize(
                        self._revision(local_type=49, raw_content=self._app_xml(subtype))
                    )
                )
                self.assertEqual(payload["content"]["kind"], expected)
                self.assertNotIn(
                    "unknown_app_subtype", payload["quality"]["codes"]
                )

    def test_unknown_or_absent_app_subtype_is_degraded_card(self) -> None:
        for subtype in (999, None, "not-an-integer"):
            with self.subTest(subtype=subtype):
                payload = self._payload(
                    self._normalize(
                        self._revision(local_type=49, raw_content=self._app_xml(subtype))
                    )
                )
                self.assertEqual(payload["content"]["kind"], "card")
                self.assertEqual(payload["quality"]["status"], "degraded")
                self.assertIn(
                    "unknown_app_subtype", payload["quality"]["codes"]
                )

    def test_text_preserves_whitespace_and_removes_only_nul_artifacts(self) -> None:
        raw = "  first line\nsecond\t line\x00  "
        payload = self._payload(
            self._normalize(self._revision(raw_content=raw))
        )
        self.assertEqual(
            payload["content"]["text"], "  first line\nsecond\t line  "
        )

    def test_text_group_prefix_remains_content_not_actor_identity(self) -> None:
        prefixed = "synthetic_sender:\nmessage body"
        payload = self._payload(
            self._normalize(self._revision(raw_content=prefixed))
        )

        self.assertEqual(payload["content"]["text"], prefixed)
        self.assertEqual(
            payload["actor"],
            {"id": None, "role": "unknown", "resolution": "unresolved"},
        )

    def test_null_media_content_is_not_a_decode_error(self) -> None:
        payload = self._payload(
            self._normalize(
                self._revision(
                    local_type=3,
                    raw_content=None,
                    raw_content_kind="null",
                    compression_type=None,
                )
            )
        )
        self.assertIsNone(payload["content"]["text"])
        self.assertEqual(payload["quality"]["status"], "normalized")

    def test_plain_blob_utf8_and_zstd_decode_deterministically(self) -> None:
        plain = "plain blob 文本".encode("utf-8")
        compressed = zstd.ZstdCompressor().compress("zstd 文本".encode("utf-8"))
        plain_payload = self._payload(
            self._normalize(
                self._revision(raw_content=plain, raw_content_kind="blob")
            )
        )
        zstd_payload = self._payload(
            self._normalize(
                self._revision(
                    raw_content=compressed,
                    raw_content_kind="blob",
                    compression_type=4,
                )
            )
        )

        self.assertEqual(plain_payload["content"]["text"], "plain blob 文本")
        self.assertEqual(zstd_payload["content"]["text"], "zstd 文本")

    def test_invalid_utf8_uses_replacement_and_degrades(self) -> None:
        payload = self._payload(
            self._normalize(
                self._revision(raw_content=b"valid\xfftail", raw_content_kind="blob")
            )
        )

        self.assertEqual(payload["content"]["text"], "valid�tail")
        self.assertEqual(payload["quality"]["status"], "degraded")
        self.assertIn("invalid_utf8_replaced", payload["quality"]["codes"])

    def test_unsupported_compression_and_corrupt_zstd_do_not_invent_text(self) -> None:
        unsupported = self._payload(
            self._normalize(
                self._revision(
                    raw_content=b"private bytes",
                    raw_content_kind="blob",
                    compression_type=9,
                )
            )
        )
        corrupt = self._payload(
            self._normalize(
                self._revision(
                    raw_content=b"not-zstandard-data",
                    raw_content_kind="blob",
                    compression_type=4,
                )
            )
        )

        self.assertIsNone(unsupported["content"]["text"])
        self.assertIn("unsupported_compression", unsupported["quality"]["codes"])
        self.assertIsNone(corrupt["content"]["text"])
        self.assertIn("decompression_failed", corrupt["quality"]["codes"])
        self.assertNotIn("not-zstandard-data", json.dumps(corrupt))

    def test_valid_video_and_location_xml_emit_only_allowlisted_attributes(self) -> None:
        video_xml = (
            '<msg><videomsg playlength="12" private="do-not-copy"/></msg>'
        )
        location_xml = (
            '<msg><location label="Synthetic place" x="1" y="2"/></msg>'
        )
        video = self._payload(
            self._normalize(self._revision(local_type=43, raw_content=video_xml))
        )
        location = self._payload(
            self._normalize(self._revision(local_type=48, raw_content=location_xml))
        )

        self.assertEqual(
            video["content"]["attributes"], {"duration_seconds": 12}
        )
        self.assertEqual(
            location["content"]["attributes"], {"label": "Synthetic place"}
        )

    def test_prefixed_xml_is_parsed_without_actor_resolution(self) -> None:
        prefixed = "synthetic_sender:\n" + self._app_xml(5)
        payload = self._payload(
            self._normalize(self._revision(local_type=49, raw_content=prefixed))
        )

        self.assertEqual(payload["content"]["kind"], "link")
        self.assertEqual(payload["actor"], {
            "id": None, "role": "unknown", "resolution": "unresolved"
        })
        self.assertNotIn("synthetic_sender", json.dumps(payload))

    def test_malformed_oversized_and_entity_xml_degrade_without_raising(self) -> None:
        malformed = self._payload(
            self._normalize(
                self._revision(local_type=49, raw_content="<msg><appmsg>")
            )
        )
        oversized = self._payload(
            self._normalize(
                self._revision(
                    local_type=49,
                    raw_content="<msg>" + ("x" * MAX_XML_CHARACTERS),
                )
            )
        )
        entity_xml = '<!DOCTYPE msg [<!ENTITY x SYSTEM "file:///private">]><msg>&x;</msg>'
        entity = self._payload(
            self._normalize(self._revision(local_type=49, raw_content=entity_xml))
        )

        self.assertIn("xml_parse_failed", malformed["quality"]["codes"])
        self.assertIn("xml_too_large", oversized["quality"]["codes"])
        self.assertIn("xml_parse_failed", entity["quality"]["codes"])
        self.assertEqual(malformed["quality"]["status"], "degraded")

    def test_missing_and_unknown_xml_fields_are_not_emitted(self) -> None:
        xml = (
            "<msg><appmsg><type>5</type><title></title>"
            "<private_field>do-not-leak</private_field></appmsg></msg>"
        )
        payload = self._payload(
            self._normalize(self._revision(local_type=49, raw_content=xml))
        )

        self.assertEqual(payload["content"]["attributes"], {})
        self.assertNotIn("private_field", payload["content"]["attributes"])
        self.assertNotIn("do-not-leak", json.dumps(payload))

    def test_quote_attributes_match_exact_allowlist(self) -> None:
        extra = (
            "<refermsg><displayname>Synthetic actor</displayname>"
            "<content>Referenced text</content><private>hidden</private></refermsg>"
        )
        payload = self._payload(
            self._normalize(
                self._revision(
                    local_type=49,
                    raw_content=self._app_xml(57, extra),
                )
            )
        )

        self.assertEqual(
            payload["content"]["attributes"],
            {
                "title": "Synthetic title",
                "description": "Synthetic description",
                "referenced_actor_label": "Synthetic actor",
                "referenced_text": "Referenced text",
            },
        )

    def test_payment_keeps_amount_as_text_and_uses_exact_allowlist(self) -> None:
        extra = (
            "<wcpayinfo><feedesc>USD 12.30</feedesc>"
            "<pay_memo>Synthetic memo</pay_memo>"
            "<currency>USD</currency></wcpayinfo>"
        )
        payload = self._payload(
            self._normalize(
                self._revision(
                    local_type=49,
                    raw_content=self._app_xml(2_000, extra),
                )
            )
        )

        self.assertEqual(
            payload["content"]["attributes"],
            {
                "title": "Synthetic title",
                "amount_label": "USD 12.30",
                "memo": "Synthetic memo",
            },
        )
        self.assertIsInstance(
            payload["content"]["attributes"]["amount_label"], str
        )

    def test_system_xml_extracts_safe_text_and_system_actor(self) -> None:
        xml = "<sysmsg><content>Synthetic notice</content><private>x</private></sysmsg>"
        payload = self._payload(
            self._normalize(self._revision(local_type=10_000, raw_content=xml))
        )

        self.assertEqual(payload["content"]["text"], "Synthetic notice")
        self.assertEqual(payload["actor"]["role"], "system")
        self.assertNotIn("<sysmsg>", json.dumps(payload))

    def test_unknown_system_xml_is_not_copied_as_raw_text(self) -> None:
        xml = "<private_provider_root><secret>do-not-copy</secret></private_provider_root>"
        payload = self._payload(
            self._normalize(self._revision(local_type=10_000, raw_content=xml))
        )

        self.assertIsNone(payload["content"]["text"])
        self.assertNotIn("private_provider_root", json.dumps(payload))
        self.assertNotIn("do-not-copy", json.dumps(payload))

    def test_timestamp_validity_uses_fixed_range_and_preserves_observation(self) -> None:
        below_lower = self._payload(
            self._normalize(self._revision(create_time=946_684_799))
        )
        lower_result = self._normalize(
            self._revision(create_time=946_684_800)
        )
        lower_replay = self._normalize(
            self._revision(create_time=946_684_800)
        )
        lower = self._payload(lower_result)
        below_upper = self._payload(
            self._normalize(self._revision(create_time=4_102_444_799))
        )
        upper = self._payload(
            self._normalize(self._revision(create_time=4_102_444_800))
        )
        missing_zero = self._payload(self._normalize(self._revision(create_time=0)))
        missing_null = self._payload(
            self._normalize(self._revision(create_time=None))
        )

        self.assertFalse(below_lower["time"]["occurred_at_valid"])
        self.assertIn("timestamp_out_of_range", below_lower["quality"]["codes"])
        self.assertTrue(lower["time"]["occurred_at_valid"])
        self.assertTrue(below_upper["time"]["occurred_at_valid"])
        self.assertFalse(upper["time"]["occurred_at_valid"])
        self.assertIn("timestamp_out_of_range", upper["quality"]["codes"])
        self.assertEqual(lower["time"]["observed_at"], OBSERVED_AT)
        self.assertEqual(
            lower_result.canonical_payload_json,
            lower_replay.canonical_payload_json,
        )
        self.assertEqual(lower_result.payload_sha256, lower_replay.payload_sha256)
        for payload in (missing_zero, missing_null):
            self.assertFalse(payload["time"]["occurred_at_valid"])
            self.assertEqual(payload["time"]["occurred_at_unix_seconds"], 0)
            self.assertIn("timestamp_missing", payload["quality"]["codes"])

    def test_unresolved_identity_and_invalid_timestamp_do_not_alone_degrade(self) -> None:
        payload = self._payload(
            self._normalize(self._revision(create_time=0))
        )

        self.assertEqual(payload["quality"]["status"], "normalized")
        self.assertEqual(
            payload["quality"]["codes"],
            ["conversation_unresolved", "actor_unresolved", "timestamp_missing"],
        )

    def test_quality_codes_have_stable_contract_order(self) -> None:
        payload = self._payload(
            self._normalize(
                self._revision(
                    local_type=777,
                    create_time=0,
                    raw_content=b"private",
                    raw_content_kind="blob",
                    compression_type=9,
                )
            )
        )
        self.assertEqual(
            payload["quality"]["codes"],
            [
                "conversation_unresolved",
                "actor_unresolved",
                "unknown_message_type",
                "unsupported_compression",
                "timestamp_missing",
            ],
        )

    def test_core_payload_does_not_leak_physical_or_unknown_provider_fields(self) -> None:
        event = RawEventInput(
            event_uid="allowed-opaque-event-uid",
            account_id="private-account-sentinel",
            source_generation="private-generation-sentinel",
            shard_name="private-shard-sentinel.db",
            table_name="Msg_private-table-sentinel",
            local_id=987_654_321,
        )
        raw_xml = (
            "<msg><appmsg><type>5</type><title>Allowed title</title>"
            "<private>private-xml-sentinel</private></appmsg></msg>"
        )
        revision = self._revision(
            fingerprint="allowed-opaque-fingerprint",
            server_id="private-server-sentinel",
            real_sender_id=13_579,
            local_type=49,
            raw_content=raw_xml,
        )
        result = self._normalize(revision, event=event)
        payload = self._payload(result)
        serialized = result.canonical_payload_json

        self.assertEqual(
            payload["provenance"]["raw_event_uid"], "allowed-opaque-event-uid"
        )
        self.assertEqual(
            payload["provenance"]["raw_revision_fingerprint"],
            "allowed-opaque-fingerprint",
        )
        for forbidden in (
            "private-account-sentinel",
            "private-generation-sentinel",
            "private-shard-sentinel.db",
            "Msg_private-table-sentinel",
            "987654321",
            "private-server-sentinel",
            "13579",
            "private-xml-sentinel",
            raw_xml,
            "/private/source/database.db",
            "passphrase-sentinel",
            "exception-sentinel",
        ):
            self.assertNotIn(forbidden, serialized)
        self.assertEqual(
            set(payload["content"]["attributes"]), {"title"}
        )

    def test_input_validation_is_pure_and_requires_immutable_observation_time(self) -> None:
        with self.assertRaisesRegex(ValueError, "canonical UTC"):
            self._revision(observed_at="2026-08-29T09:02:03.000004+08:00")
        with self.assertRaisesRegex(ValueError, "text raw content"):
            self._revision(raw_content=b"text", raw_content_kind="text")


if __name__ == "__main__":
    unittest.main()
