import hashlib
import unittest

from tests.brief002_support import FIXTURES, SESSION, fixture, parse, payload, validate

from bqip.capture.format import Frame, read_frame
from bqip.contracts.protobuf import metadata_from_proto
from bqip.contracts.values import ContractError
from bqip.v1 import capture_pb2 as raw_pb
from bqip.v1 import common_pb2 as c
from bqip.v1 import market_pb2 as m
from bqip.v1 import metadata_pb2 as md
from bqip.v1 import operational_pb2 as op
from bqip.v1 import source_pb2 as s
from bqip.venues.book import BookSync
from bqip.venues.profile import DEPTH_ENDPOINT, DEPTH_STREAM
from bqip.venues.replay import replay_depth

TYPES = {"trade": m.TradeEvent, "depth": m.BinanceDepthUpdate,
         "snapshot": m.BinanceDepthSnapshot, "exchange": md.InstrumentMetadataVersion}


class BinanceWire(unittest.TestCase):
    def test_external_wire_bytes_and_hashes(self):
        for v in fixture("wire.json"):
            with self.subTest(v["kind"]):
                expected = bytes.fromhex(v["wire_hex"])
                actual = parse(v["kind"], (FIXTURES / v["raw_file"]).read_bytes())[0]
                self.assertEqual(actual.SerializeToString(deterministic=True), expected)
                self.assertEqual(hashlib.sha256(expected).hexdigest(), v["sha256"])
                decoded = TYPES[v["kind"]].FromString(expected)
                validate(v["kind"], decoded)
                self.assertEqual(decoded.SerializeToString(deterministic=True), expected)

    def test_shared_enum_stability(self):
        families = {"EffectiveTimeBasis": (md, "EFFECTIVE_TIME_BASIS"),
                    "MetadataResolutionBasis": (md, "METADATA_RESOLUTION_BASIS"),
                    "TradeGranularity": (m, "TRADE_GRANULARITY"), "AggressorSide": (m, "AGGRESSOR_SIDE"),
                    "LiquidationObservation": (m, "LIQUIDATION_OBSERVATION"),
                    "SourceTransport": (s, "SOURCE_TRANSPORT"), "CauseCode_additions": (op, "CAUSE_CODE")}
        for family, vectors in fixture("enums.json").items():
            module, prefix = families[family]
            for token, number in vectors.items():
                self.assertEqual(getattr(module, prefix+"_"+token), number)

    def test_every_market_scalar_and_source_required_field(self):
        for kind in ("trade", "depth", "snapshot"):
            event = parse(kind, payload(kind))[0]
            for descriptor, _ in event.ListFields():
                if descriptor.is_repeated:
                    continue
                candidate = TYPES[kind].FromString(event.SerializeToString())
                candidate.ClearField(descriptor.name)
                with self.subTest(kind=kind, missing=descriptor.name):
                    with self.assertRaises(ContractError):
                        validate(kind, candidate)

    def test_source_lineage_roundtrip_is_immutable(self):
        message = parse("exchange", payload("exchange"))[0]
        original = message.SerializeToString(deterministic=True)
        domain = metadata_from_proto(md.InstrumentMetadataVersion.FromString(original))
        self.assertEqual(domain.source.to_proto(), message.source)
        self.assertFalse(message.HasField("effective_from_ns"))
        message.source.lineage.raw_content_hash = b"x"*32
        self.assertNotEqual(domain.source.raw_content_hash, message.source.lineage.raw_content_hash)

    def test_new_metadata_unspecified_and_absent_basis_rejected(self):
        for basis in (None, 0, 999):
            message = parse("exchange", payload("exchange"))[0]
            if basis is None:
                message.ClearField("effective_time_basis")
            else:
                message.effective_time_basis = basis
            with self.assertRaises(ValueError):
                metadata_from_proto(message)

    def test_trade_wire_cannot_change_granularity_or_rpi(self):
        event = parse("trade", payload("trade"))[0]
        event.granularity = m.TRADE_GRANULARITY_INDIVIDUAL
        with self.assertRaises(ContractError):
            validate("trade", event)
        event.granularity = m.TRADE_GRANULARITY_AGGREGATED
        event.quantity_ex_rpi.coefficient = "99"
        with self.assertRaises(ContractError):
            validate("trade", event)

    def test_unknown_wire_fields_do_not_conflict(self):
        event = parse("depth", payload("depth"))[0]
        annotated = m.BinanceDepthUpdate.FromString(event.SerializeToString()+bytes.fromhex("a00607"))
        book = BookSync(SESSION)
        book.begin()
        book.request_snapshot()
        book.ingest(event)
        self.assertEqual(book.ingest(annotated), "DUPLICATE")

    def test_bqrc_replay_preserves_raw_and_original_session(self):
        import io
        body = payload("depth")
        raw = raw_pb.RawCaptureEvent(
            capture_id=c.CaptureId(collector_instance_id=bytes(range(16)), capture_seq=7),
            connection_id=bytes(range(16)), session_id=SESSION, transport="WEBSOCKET", source="BINANCE",
            endpoint=DEPTH_ENDPOINT, channel=DEPTH_STREAM, receive_timestamp_ns=5000000,
            monotonic_ns_since_collector_start=100, payload_kind=raw_pb.PAYLOAD_KIND_TEXT,
            payload_length=len(body), raw_content_hash=hashlib.sha256(body).digest())
        frame = Frame(raw.SerializeToString(deterministic=True), body)
        reread = read_frame(io.BytesIO(frame.encode()), 0)
        self.assertEqual(reread.payload, body)
        decoded = raw_pb.RawCaptureEvent.FromString(reread.metadata)
        event, _ = replay_depth(decoded, reread.payload)
        self.assertEqual(event.source.original_session_id, SESSION)
        self.assertEqual(event.source.lineage.capture_id, raw.capture_id)
        expected = next(v for v in fixture("wire.json") if v["kind"] == "depth")
        self.assertEqual(event.SerializeToString(deterministic=True).hex(), expected["wire_hex"])
        with self.assertRaises(ContractError):
            replay_depth(decoded, body+b" ")
