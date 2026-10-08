"""The pinned profile is a repository artifact; this module performs no I/O on the network."""

from pathlib import Path

from bqip.manifests import manifest_hash, parse_manifest

PROFILE_PATH = Path(__file__).resolve().parents[4] / "config/venues/binance/usdm/btcusdt-perpetual-v1.json"
PROFILE_ID = "BINANCE-USD_M_FUTURES-BTCUSDT-PERPETUAL-v1"
INSTRUMENT_ID = "BINANCE:USD_M_FUTURES:BTCUSDT"
DEPTH_STREAM = "btcusdt@depth@100ms"
TRADE_STREAM = "btcusdt@aggTrade"
DEPTH_ENDPOINT = "wss://fstream.binance.com/public/ws/" + DEPTH_STREAM
TRADE_ENDPOINT = "wss://fstream.binance.com/market/ws/" + TRADE_STREAM


def profile_hash() -> bytes:
    return manifest_hash(parse_manifest(PROFILE_PATH.read_text()))
