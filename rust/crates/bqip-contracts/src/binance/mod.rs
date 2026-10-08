//! Brief 002 offline contracts. No network client or collector runtime.
pub mod book;
mod json;
mod parser;
pub mod source;
pub mod validation;
pub use parser::*;

use crate::{identity, manifests, ContractError, Result};

pub const PROFILE_ID: &str = "BINANCE-USD_M_FUTURES-BTCUSDT-PERPETUAL-v1";
pub const INSTRUMENT_ID: &str = "BINANCE:USD_M_FUTURES:BTCUSDT";
pub const DEPTH_STREAM: &str = "btcusdt@depth@100ms";
pub const TRADE_STREAM: &str = "btcusdt@aggTrade";
pub const DEPTH_ENDPOINT: &str = "wss://fstream.binance.com/public/ws/btcusdt@depth@100ms";
pub const TRADE_ENDPOINT: &str = "wss://fstream.binance.com/market/ws/btcusdt@aggTrade";
pub const PROFILE_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../config/venues/binance/usdm/btcusdt-perpetual-v1.json"
));

pub fn profile_hash() -> Result<[u8; 32]> {
    manifests::manifest_hash(&manifests::parse_manifest(PROFILE_JSON)?)
}

pub fn identity_components<'a>(
    class: &'a [u8],
    tail: &[(&'a str, &'a [u8])],
) -> Vec<(&'a str, &'a [u8])> {
    let mut parts = vec![
        ("venue", b"BINANCE".as_slice()),
        ("product_family", b"USD_M_FUTURES"),
        ("symbol", b"BTCUSDT"),
        ("event_class", class),
    ];
    parts.extend_from_slice(tail);
    parts
}

pub fn trade_id(id: u64) -> Result<[u8; 32]> {
    identity::logical_hash(&identity_components(
        b"AGG_TRADE",
        &[("a", id.to_string().as_bytes())],
    ))
}

pub fn depth_id(session: &[u8], first: u64, last: u64) -> Result<[u8; 32]> {
    if session.len() != 16 {
        return Err(ContractError("INVALID_ORIGINAL_SESSION"));
    }
    identity::logical_hash(&identity_components(
        b"DEPTH_UPDATE",
        &[
            ("session_id", session),
            ("U", first.to_string().as_bytes()),
            ("u", last.to_string().as_bytes()),
        ],
    ))
}

pub fn ms_to_ns(ms: u64) -> Result<i64> {
    i64::try_from(ms)
        .ok()
        .and_then(|n| n.checked_mul(1_000_000))
        .ok_or(ContractError("TIMESTAMP_OVERFLOW"))
}

/// Replay never accepts a replacement session, capture ID or receive timestamp.
pub fn replay_depth(
    raw: &crate::proto::RawCaptureEvent,
    payload: &[u8],
) -> Result<(crate::proto::BinanceDepthUpdate, Vec<String>)> {
    use crate::proto::{SourceLineage, SourceTransport, StreamSourceContext};
    raw.validate(payload)?;
    if raw.transport.as_deref() != Some("WEBSOCKET") || raw.source.as_deref() != Some("BINANCE") {
        return Err(ContractError("PROFILE_MISMATCH"));
    }
    parse_depth(
        payload,
        &StreamSourceContext {
            lineage: Some(SourceLineage {
                venue_profile_id: Some(PROFILE_ID.into()),
                venue_profile_hash: Some(profile_hash()?.to_vec()),
                capture_id: raw.capture_id.clone(),
                raw_content_hash: raw.raw_content_hash.clone(),
                receive_timestamp_ns: raw.receive_timestamp_ns,
            }),
            transport: Some(SourceTransport::Websocket as i32),
            endpoint: raw.endpoint.clone(),
            channel: raw.channel.clone(),
            original_session_id: raw.session_id.clone(),
        },
    )
}
