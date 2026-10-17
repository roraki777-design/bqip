"""External wire vectors, calculated from IDL tags rather than either binding."""
import json
import unittest
from pathlib import Path

from bqip.v1.capture_pb2 import RawCaptureEvent


class CollectorBindingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = json.loads(
            (Path(__file__).parents[1] / "fixtures/collector/wire.json").read_text()
        )

    def test_typed_contexts(self) -> None:
        wire = bytes.fromhex(self.fixture["typed_contexts_hex"])
        event = RawCaptureEvent.FromString(wire)
        self.assertTrue(event.HasField("http_source"))
        self.assertTrue(event.HasField("stream_source"))
        self.assertEqual(event.SerializeToString(deterministic=True), wire)

    def test_content_encoding_is_opaque_bytes(self) -> None:
        wire = bytes.fromhex(self.fixture["content_encoding_hex"])
        event = RawCaptureEvent.FromString(wire)
        self.assertEqual(
            event.http_content_encoding,
            bytes.fromhex(self.fixture["content_encoding_bytes_hex"]),
        )
        self.assertEqual(event.SerializeToString(deterministic=True), wire)
