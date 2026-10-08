"""Replay entry point has no parameter for a new session or receive timestamp."""
from bqip.contracts.protobuf import raw_from_proto
from bqip.contracts.values import ContractError
from bqip.v1 import capture_pb2, market_pb2, source_pb2
from bqip.venues.binance import parse_depth
from bqip.venues.profile import PROFILE_ID, profile_hash


def replay_depth(raw: capture_pb2.RawCaptureEvent, payload: bytes
                 ) -> tuple[market_pb2.BinanceDepthUpdate, tuple[str, ...]]:
    raw_from_proto(raw, payload)
    if raw.source != "BINANCE" or raw.transport != "WEBSOCKET":
        raise ContractError("PROFILE_MISMATCH")
    source = source_pb2.StreamSourceContext(
        lineage=source_pb2.SourceLineage(venue_profile_id=PROFILE_ID, venue_profile_hash=profile_hash(),
            capture_id=raw.capture_id, raw_content_hash=raw.raw_content_hash,
            receive_timestamp_ns=raw.receive_timestamp_ns),
        transport=source_pb2.SOURCE_TRANSPORT_WEBSOCKET, endpoint=raw.endpoint,
        channel=raw.channel, original_session_id=raw.session_id)
    return parse_depth(payload, source)
