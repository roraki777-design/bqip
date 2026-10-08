import hashlib
import json
from pathlib import Path

from bqip.v1 import common_pb2 as c
from bqip.v1 import market_pb2 as m
from bqip.v1 import source_pb2 as s
from bqip.venues import binance as b
from bqip.venues import profile as p

FIXTURES = Path(__file__).parent / "fixtures/binance-usdm"
SESSION = bytes(range(16, 32))


def fixture(name):
    return json.loads((FIXTURES / name).read_text())


def payload(kind):
    name = {"trade": "trade-sell", "depth": "depth-ordinary", "snapshot": "snapshot-valid", "exchange": "exchange-valid"}[kind]
    return (FIXTURES / "raw" / (name + ".json")).read_bytes()


def source(kind, raw, session=SESSION):
    lineage = s.SourceLineage(venue_profile_id=p.PROFILE_ID, venue_profile_hash=p.profile_hash(),
                             capture_id=c.CaptureId(collector_instance_id=bytes(range(16)), capture_seq=7),
                             raw_content_hash=hashlib.sha256(raw).digest(), receive_timestamp_ns=5000000)
    if kind in ("trade", "depth"):
        return s.StreamSourceContext(lineage=lineage, transport=s.SOURCE_TRANSPORT_WEBSOCKET,
                                     endpoint=p.TRADE_ENDPOINT if kind == "trade" else p.DEPTH_ENDPOINT,
                                     channel=p.TRADE_STREAM if kind == "trade" else p.DEPTH_STREAM,
                                     original_session_id=session)
    return s.HttpSourceContext(lineage=lineage, transport=s.SOURCE_TRANSPORT_HTTP_REST, method="GET",
                               endpoint="/fapi/v1/depth" if kind == "snapshot" else "/fapi/v1/exchangeInfo",
                               query_parameters=[s.QueryParameter(name=k, value=v) for k, v in
                                                 (("limit", "1000"), ("symbol", "BTCUSDT"))] if kind == "snapshot" else [], http_status=200)


def parse(kind, raw, session=SESSION):
    context = source(kind, raw, session)
    return {"trade": b.parse_trade, "depth": b.parse_depth, "snapshot": b.parse_snapshot,
            "exchange": lambda raw, src: b.parse_exchange_info(raw, src, "observation-1")}[kind](raw, context)


def changed(kind, changes, session=SESSION):
    doc = json.loads(payload(kind))
    doc.update(changes)
    return parse(kind, json.dumps(doc, separators=(",", ":")).encode(), session)[0]


def sides(levels):
    return [[f"{v.price.coefficient}:{v.price.scale}", f"{v.quantity.coefficient}:{v.quantity.scale}"] for v in levels]


def project(message, expected):
    result = {}
    enums = {"aggressor": {1: "BUY", 2: "SELL"}, "granularity": {2: "AGGREGATED"},
             "liquidation": {1: "NOT_OBSERVED_FROM_THIS_SOURCE"}, "effective_time_basis": {3: "UNKNOWN"}}
    for key in expected:
        value = getattr(message, key)
        if key in ("bids", "asks"):
            result[key] = sides(value)
        elif key == "effective_from_ns":
            result[key] = value if message.HasField(key) else None
        elif key in enums:
            result[key] = enums[key][value]
        elif isinstance(value, c.DecimalValue):
            result[key] = {"coefficient": value.coefficient, "scale": value.scale}
        else:
            result[key] = value
    return result


def validate(kind, message):
    from bqip.contracts.protobuf import metadata_from_proto
    from bqip.venues.validation import validate_depth, validate_snapshot, validate_trade
    return {"trade": validate_trade, "depth": validate_depth, "snapshot": validate_snapshot,
            "exchange": metadata_from_proto}[kind](message)


WIRE_TYPES = {"trade": m.TradeEvent, "depth": m.BinanceDepthUpdate, "snapshot": m.BinanceDepthSnapshot}
