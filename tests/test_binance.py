import hashlib
import json
import unittest
from dataclasses import FrozenInstanceError

from bqip.contracts.protobuf import metadata_from_proto
from bqip.contracts.values import ContractError
from bqip.identity import logical_hash, preimage
from bqip.manifests import canonical_manifest, parse_manifest
from bqip.metadata import MetadataCatalog, ResolverMode
from bqip.v1 import metadata_pb2 as md
from bqip.venues import binance as b
from bqip.venues.book import BookSync, BookSyncState
from bqip.venues.profile import PROFILE_PATH, profile_hash
from tests.brief002_support import (
    FIXTURES,
    SESSION,
    changed,
    fixture,
    parse,
    payload,
    project,
    sides,
    source,
    validate,
)


def running():
    book = BookSync(SESSION)
    book.begin()
    book.request_snapshot()
    return book


class BinanceGolden(unittest.TestCase):
    def test_all_external_parser_vectors(self):
        for vector in fixture("parsing.json"):
            with self.subTest(vector["name"]):
                raw = (FIXTURES / vector["raw_file"]).read_bytes()
                self.assertEqual(hashlib.sha256(raw).hexdigest(), vector["raw_sha256"])
                if "error" in vector:
                    with self.assertRaisesRegex(ContractError, "^"+vector["error"]+"$"):
                        parse(vector["kind"], raw)
                else:
                    event, _ = parse(vector["kind"], raw)
                    validate(vector["kind"], event)
                    self.assertEqual(project(event, vector["expected"]), vector["expected"])

    def test_profile_external_jcs_and_hash(self):
        v = fixture("profile.json")
        parsed = parse_manifest(PROFILE_PATH.read_text())
        self.assertEqual(parsed, v["input"])
        self.assertEqual(canonical_manifest(parsed).hex(), v["canonical_hex"])
        self.assertEqual(profile_hash().hex(), v["sha256"])

    def test_temporal_vectors_in_both_orders(self):
        for v in fixture("metadata.json"):
            for records in (v["versions"], list(reversed(v["versions"]))):
                with self.subTest(v["name"], records=records):
                    def resolve():
                        catalog = MetadataCatalog(tuple(metadata_from_proto(md.InstrumentMetadataVersion(**item)) for item in records))
                        return catalog.resolve_with_basis("TEST", v["event_time_ns"], ResolverMode(v["mode"]), v["as_of_ns"])
                    if "error" in v:
                        with self.assertRaisesRegex(ContractError, "^"+v["error"]+"$"):
                            resolve()
                    else:
                        result = resolve()
                        self.assertEqual(result.version.metadata_version, v["expected"])
                        self.assertEqual(result.resolution_basis, v["basis"])
                        self.assertEqual(result.quality, v["quality"])

    def test_identity_external_preimages_and_hashes(self):
        for v in fixture("identity.json"):
            parts = [(k, bytes.fromhex(value)) for k, value in v["components"]]
            self.assertEqual(preimage(parts).hex(), v["preimage_hex"])
            self.assertEqual(logical_hash(parts).hex(), v["sha256"])
            components = dict(parts)
            actual = b.trade_id(int(components["a"])) if components["event_class"] == b"AGG_TRADE" else b.depth_id(components["session_id"], int(components["U"]), int(components["u"]))
            self.assertEqual(actual.hex(), v["sha256"])

    def test_exact_bridge_boundaries(self):
        for v in fixture("boundaries.json"):
            with self.subTest(v["name"]):
                book = running()
                book.ingest(changed("depth", {"U": v["U"], "u": v["u"]}))
                book.snapshot(changed("snapshot", {"lastUpdateId": v["S"]}))
                self.assertEqual(book.state.value, v["state"])

    def test_external_sync_sequence(self):
        v = fixture("sync.json")
        book = running()
        events = [parse("depth", json.dumps(e).encode())[0] for e in v["events"]]
        book.ingest(events[0])
        book.ingest(events[1])
        book.snapshot(parse("snapshot", json.dumps(v["snapshot"]).encode())[0])
        bids, asks = book.trusted_book()
        self.assertEqual({"bids": sides(bids), "asks": sides(asks)}, v["after_bridge"])
        book.ingest(events[2])
        bids, asks = book.trusted_book()
        self.assertEqual({"bids": sides(bids), "asks": sides(asks)}, v["after_next"])
        with self.assertRaisesRegex(ContractError, v["error"]):
            book.ingest(events[3])
        self.assertEqual([state.value for state in book.transitions], v["transitions"])
        with self.assertRaisesRegex(ContractError, "BOOK_NOT_HEALTHY"):
            book.trusted_book()
        with self.assertRaisesRegex(ContractError, "INVALID_SYNC_STATE"):
            book.ingest(events[2])

    def test_duplicate_is_content_based_and_ignores_capture(self):
        book = running()
        event = parse("depth", payload("depth"))[0]
        self.assertEqual(book.ingest(event), "BUFFERED")
        event.source.lineage.capture_id.capture_seq += 1
        event.source.lineage.raw_content_hash = b"x" * 32
        event.source.lineage.receive_timestamp_ns += 1
        self.assertEqual(book.ingest(event), "DUPLICATE")

    def test_pu_change_conflicts_without_changing_identity(self):
        book = running()
        first = parse("depth", payload("depth"))[0]
        other = changed("depth", {"pu": 1})
        self.assertEqual(first.logical_event_id, other.logical_event_id)
        book.ingest(first)
        with self.assertRaisesRegex(ContractError, "VENUE_IDENTITY_CONFLICT"):
            book.ingest(other)
        self.assertEqual(book.state, BookSyncState.RESYNC_REQUIRED)

    def test_levels_change_conflicts(self):
        book = running()
        first = parse("depth", payload("depth"))[0]
        other = changed("depth", {"b": [["100", "3"]]})
        self.assertEqual(first.logical_event_id, other.logical_event_id)
        book.ingest(first)
        with self.assertRaisesRegex(ContractError, "VENUE_IDENTITY_CONFLICT"):
            book.ingest(other)

    def test_session_scope_and_wrong_replay_session(self):
        first = parse("depth", payload("depth"))[0]
        other = parse("depth", payload("depth"), bytes(range(32, 48)))[0]
        self.assertNotEqual(first.logical_event_id, other.logical_event_id)
        with self.assertRaisesRegex(ContractError, "ORIGINAL_SESSION_MISMATCH"):
            running().ingest(other)
        a = parse("trade", payload("trade"))[0]
        other_trade = parse("trade", payload("trade"), bytes(range(32, 48)))[0]
        self.assertEqual(a.logical_event_id, other_trade.logical_event_id)

    def test_buffer_and_trusted_view_are_owned(self):
        book = running()
        event = parse("depth", payload("depth"))[0]
        book.ingest(event)
        event.bids[0].quantity.coefficient = "999"
        book.snapshot(parse("snapshot", payload("snapshot"))[0])
        bids, _ = book.trusted_book()
        self.assertEqual(bids[0].quantity.coefficient, "2")
        bids[0].quantity.coefficient = "999"
        self.assertEqual(book.trusted_book()[0][0].quantity.coefficient, "2")

    def test_unknown_fields_observable_and_migration_optional(self):
        for kind in ("trade", "depth"):
            value = json.loads(payload(kind))
            value["newMetric"] = 1.5
            event, drift = parse(kind, json.dumps(value).encode())
            self.assertIn("newMetric", drift)
            del value["st"]
            optional, _ = parse(kind, json.dumps(value).encode())
            self.assertEqual(event.logical_event_id, optional.logical_event_id)
            _, drift = parse(kind, payload(kind)[:-1] + b',"future":-0}')
            self.assertIn("future", drift)

    def test_each_required_native_field_missing_fails(self):
        fields = {"trade": ["e", "E", "s", "a", "p", "q", "nq", "f", "l", "T", "m"],
                  "depth": ["e", "E", "T", "s", "U", "u", "pu", "b", "a"],
                  "snapshot": ["lastUpdateId", "E", "T", "bids", "asks"]}
        for kind, required in fields.items():
            for key in required:
                with self.subTest(kind=kind, missing=key):
                    value = json.loads(payload(kind))
                    del value[key]
                    with self.assertRaises(ContractError):
                        parse(kind, json.dumps(value).encode())

    def test_duplicate_json_keys_and_nonfinite_rejected(self):
        for raw in (payload("trade")[:-1]+b',"a":8}', payload("trade")[:-1]+b',"new":NaN}', b'{"x":"\\ud800"}'):
            with self.assertRaises(ContractError):
                parse("trade", raw)
        value = json.loads(payload("trade"))
        value["a"] = "NEGATIVE_ZERO"
        raw = json.dumps(value).encode().replace(b'"NEGATIVE_ZERO"', b'-0')
        with self.assertRaisesRegex(ContractError, "PARSE_FAILURE"):
            parse("trade", raw)

    def test_http_source_failure_and_hash_failure(self):
        for kind in ("exchange", "snapshot"):
            raw = payload(kind)
            context = source(kind, raw)
            context.http_status = 500
            parser = b.parse_snapshot if kind == "snapshot" else lambda data, src: b.parse_exchange_info(data, src, "v1")
            with self.assertRaisesRegex(ContractError, "HTTP_STATUS_FAILURE"):
                parser(raw, context)
            context.http_status = 200
            context.lineage.raw_content_hash = b"x"*32
            with self.assertRaisesRegex(ContractError, "RAW_HASH_MISMATCH"):
                parser(raw, context)

    def test_metadata_immutable_append_and_labels_required(self):
        observed = metadata_from_proto(parse("exchange", payload("exchange"))[0])
        catalog = MetadataCatalog((observed,))
        with self.assertRaisesRegex(ContractError, "LABELED_METADATA_RESOLUTION_REQUIRED"):
            catalog.resolve(observed.instrument_id, 1, ResolverMode.AS_LIVED, observed.known_from_ns)
        with self.assertRaises(FrozenInstanceError):
            observed.effective_from_ns = 1
        copy_of_source = observed.source.to_proto()
        copy_of_source.lineage.receive_timestamp_ns = 999
        self.assertEqual(observed.source.receive_timestamp_ns, 5000000)

    def test_absolute_replacement_and_zero_are_idempotent(self):
        book = running()
        book.ingest(parse("depth", payload("depth"))[0])
        book.snapshot(parse("snapshot", payload("snapshot"))[0])
        for last in range(103, 130):
            book.ingest(changed("depth", {"U": last, "u": last, "pu": last-1, "b": [["100", "7"], ["50", "0"]], "a": [["101", "0"]]}))
            bids, asks = book.trusted_book()
            self.assertEqual(sides(bids), [["100:0", "7:0"], ["99:0", "2:0"]])
            self.assertEqual(asks, [])

    def test_timestamp_limits(self):
        limit = ((1 << 63)-1)//1_000_000
        self.assertEqual(b.ms_to_ns(limit), limit*1_000_000)
        for invalid in (limit+1, -1, True, 1.0):
            with self.assertRaises(ContractError):
                b.ms_to_ns(invalid)

    def test_no_child_trades_even_large_member_range(self):
        event = changed("trade", {"f": 0, "l": (1 << 64)-1})
        self.assertEqual(event.granularity, 2)
        self.assertEqual(event.quantity.coefficient, "6")

    def test_snapshot_cannot_skip_first_nonstale_event(self):
        book = running()
        book.ingest(changed("depth", {"U": 101, "u": 105}))
        book.ingest(changed("depth", {"U": 98, "u": 102}))
        book.snapshot(parse("snapshot", payload("snapshot"))[0])
        self.assertEqual(book.state, BookSyncState.BRIDGING)
        with self.assertRaises(ContractError):
            book.trusted_book()

    def test_semantically_equal_decimal_spelling_deduplicates(self):
        book = running()
        first = parse("depth", payload("depth"))[0]
        book.ingest(first)
        self.assertEqual(book.ingest(changed("depth", {"b": [["100.00", "2.00"]], "ps": "BTCUSDT"})), "DUPLICATE")

    def test_illegal_state_transitions(self):
        book = BookSync(SESSION)
        event = parse("depth", payload("depth"))[0]
        with self.assertRaises(ContractError):
            book.ingest(event)
        book.begin()
        with self.assertRaises(ContractError):
            book.begin()
        with self.assertRaises(ContractError):
            book.snapshot(parse("snapshot", payload("snapshot"))[0])
