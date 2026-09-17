"""Regression checks for Python's runtime contract and immutable evidence."""

import unittest
from dataclasses import replace
from uuid import uuid4

from bqip.capture.format import Frame, Limits
from bqip.capture.model import RawCaptureEvent
from bqip.contracts.values import (
    CaptureId,
    ContractError,
    DecimalValue,
    PayloadKind,
    ProcessingContext,
    ReplayMode,
)
from bqip.identity import CaptureSequencer, raw_hash
from bqip.metadata import InstrumentMetadataVersion


class ContractValidationTests(unittest.TestCase):
    def test_metadata_requires_exact_decimal_objects(self):
        version = InstrumentMetadataVersion("test", "v1", 0, 0, "test", "A", "B")
        for field in ("contract_multiplier", "tick_size", "lot_size"):
            for invalid in (0.1, 1, "0.1", {"coefficient": "1", "scale": 1}):
                with self.subTest(field=field, invalid=invalid):
                    with self.assertRaises(ContractError):
                        replace(version, **{field: invalid})
            self.assertEqual(getattr(replace(version, **{field: DecimalValue("1", 1)}), field),
                             DecimalValue("1", 1))

    def test_contract_strings_cannot_hide_nontext_or_invalid_unicode(self):
        version = InstrumentMetadataVersion("test", "v1", 0, 0, "test", "A", "B")
        for field in ("instrument_id", "metadata_version", "product_type", "margin_asset",
                      "settlement_asset", "quantity_unit", "supersedes_version"):
            for invalid in (1, b"test", "\ud800"):
                with self.subTest(field=field, invalid=repr(invalid)):
                    with self.assertRaises(ContractError):
                        replace(version, **{field: invalid})
        context = ProcessingContext(0, "commit", 1, uuid4(), ReplayMode.LIVE)
        for invalid in (1, b"commit", "\ud800"):
            with self.assertRaises(ContractError):
                replace(context, code_commit=invalid)

    def test_raw_rejects_invalid_optional_context_and_mutable_evidence(self):
        event = RawCaptureEvent(CaptureId(uuid4(), 0), uuid4(), uuid4(), "test", "test",
                                "test", "test", 0, 0, PayloadKind.BINARY, 0, raw_hash(b""))
        for field, invalid in (("processing_context", 1), ("venue_event_id", bytearray(b"id")),
                               ("raw_content_hash", bytearray(event.raw_content_hash)),
                               ("source", 1), ("channel", "\ud800")):
            with self.subTest(field=field), self.assertRaises(ContractError):
                replace(event, **{field: invalid})
        for metadata, payload in ((bytearray(b"m"), b"p"), (b"m", bytearray(b"p"))):
            with self.assertRaises(ContractError):
                Frame(metadata, payload)

    def test_limits_require_integers(self):
        for invalid in (True, 1.5, -1, 1 << 32):
            for field in ("metadata_bytes", "payload_bytes"):
                with self.subTest(field=field, invalid=invalid), self.assertRaises(ValueError):
                    Limits(**{field: invalid})

    def test_collector_identity_cannot_change_mid_sequence(self):
        sequencer = CaptureSequencer()
        first = sequencer.next_id()
        with self.assertRaises(AttributeError):
            sequencer.collector_instance_id = uuid4()
        second = sequencer.next_id()
        self.assertEqual(first.collector_instance_id, second.collector_instance_id)
        self.assertEqual(second.capture_seq, first.capture_seq + 1)
        with self.assertRaises(ContractError):
            CaptureSequencer("not a UUID")


if __name__ == "__main__":
    unittest.main()
