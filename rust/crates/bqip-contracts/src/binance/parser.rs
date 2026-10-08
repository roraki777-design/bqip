use super::*;
use crate::{proto::*, ContractError, Result};
use serde_json::Value;
use std::{cmp::Ordering, collections::BTreeMap};

fn text(v: &Value) -> Result<&str> {
    v.as_str()
        .filter(|s| !s.is_empty())
        .ok_or(ContractError("PARSE_FAILURE"))
}
fn uint(v: &Value) -> Result<u64> {
    v.as_u64().ok_or(ContractError("PARSE_FAILURE"))
}
fn array(v: &Value) -> Result<&Vec<Value>> {
    v.as_array().ok_or(ContractError("PARSE_FAILURE"))
}
pub fn decimal(v: &Value, positive: bool) -> Result<DecimalValue> {
    let d = DecimalValue::parse(text(v).map_err(|_| ContractError("INVALID_VENUE_DECIMAL"))?)
        .map_err(|_| ContractError("INVALID_VENUE_DECIMAL"))?;
    validate_decimal(&d, positive)?;
    Ok(d)
}
pub fn validate_decimal(d: &DecimalValue, positive: bool) -> Result<()> {
    d.validate()?;
    let coefficient = d.coefficient.as_deref().unwrap_or_default();
    if coefficient.starts_with('-') || (positive && coefficient == "0") {
        return Err(ContractError("INVALID_VENUE_DECIMAL"));
    }
    Ok(())
}
pub fn compare(a: &DecimalValue, b: &DecimalValue) -> Ordering {
    let (a_text, b_text) = (
        a.coefficient.as_deref().unwrap_or("0"),
        b.coefficient.as_deref().unwrap_or("0"),
    );
    if a_text == "0" || b_text == "0" {
        return (a_text != "0").cmp(&(b_text != "0"));
    }
    let a_exp = a_text.len() as i64 - i64::from(a.scale.unwrap_or(0));
    let b_exp = b_text.len() as i64 - i64::from(b.scale.unwrap_or(0));
    a_exp.cmp(&b_exp).then_with(|| {
        let len = a_text.len().max(b_text.len());
        a_text
            .bytes()
            .chain(std::iter::repeat(b'0'))
            .take(len)
            .cmp(b_text.bytes().chain(std::iter::repeat(b'0')).take(len))
    })
}
fn levels(v: &Value) -> Result<Vec<PriceLevel>> {
    array(v)?
        .iter()
        .map(|row| {
            let row = array(row)?;
            if row.len() != 2 {
                return Err(ContractError("INVALID_LEVEL"));
            }
            Ok(PriceLevel {
                price: Some(decimal(&row[0], true)?),
                quantity: Some(decimal(&row[1], false)?),
            })
        })
        .collect()
}
fn migration(v: &Value, key: &str) -> Result<()> {
    if v[key] != "BTCUSDT"
        || (v.get("st").is_some() && uint(&v["st"])? != 1)
        || (v.get("ps").is_some() && v["ps"] != "BTCUSDT")
    {
        return Err(ContractError("PROFILE_MISMATCH"));
    }
    Ok(())
}
fn drift(v: &Value, known: &[&str]) -> Vec<String> {
    v.as_object()
        .into_iter()
        .flat_map(|o| o.keys())
        .filter(|key| !known.contains(&key.as_str()))
        .cloned()
        .collect()
}

pub fn parse_trade(
    payload: &[u8],
    source: &StreamSourceContext,
) -> Result<(TradeEvent, Vec<String>)> {
    source::stream(source, TRADE_ENDPOINT, TRADE_STREAM, Some(payload))?;
    let v = json::document(payload)?;
    migration(&v, "s")?;
    if v["e"] != "aggTrade" {
        return Err(ContractError("PARSE_FAILURE"));
    }
    let maker = v["m"].as_bool().ok_or(ContractError("PARSE_FAILURE"))?;
    let (a, f, l) = (uint(&v["a"])?, uint(&v["f"])?, uint(&v["l"])?);
    if f > l {
        return Err(ContractError("INVALID_TRADE_RANGE"));
    }
    let (e, t) = (uint(&v["E"])?, uint(&v["T"])?);
    let (q, nq) = (decimal(&v["q"], true)?, decimal(&v["nq"], false)?);
    if compare(&nq, &q).is_gt() {
        return Err(ContractError("INVALID_RPI_QUANTITY"));
    }
    Ok((
        TradeEvent {
            instrument_id: Some(INSTRUMENT_ID.into()),
            venue_event_id: Some(a.to_string()),
            logical_event_id: Some(trade_id(a)?.to_vec()),
            price: Some(decimal(&v["p"], true)?),
            quantity: Some(q),
            quantity_ex_rpi: Some(nq),
            event_timestamp_ns: Some(ms_to_ns(e)?),
            trade_timestamp_ns: Some(ms_to_ns(t)?),
            native_event_time_ms: Some(e),
            native_trade_time_ms: Some(t),
            aggressor: Some(if maker {
                AggressorSide::Sell
            } else {
                AggressorSide::Buy
            } as i32),
            granularity: Some(TradeGranularity::Aggregated as i32),
            aggregate_id: Some(a),
            first_venue_trade_id: Some(f),
            last_venue_trade_id: Some(l),
            buyer_is_maker: Some(maker),
            liquidation: Some(LiquidationObservation::NotObservedFromThisSource as i32),
            source: Some(source.clone()),
        },
        drift(
            &v,
            &[
                "e", "E", "s", "a", "p", "q", "nq", "f", "l", "T", "m", "st", "ps",
            ],
        ),
    ))
}

pub fn parse_depth(
    payload: &[u8],
    source: &StreamSourceContext,
) -> Result<(BinanceDepthUpdate, Vec<String>)> {
    source::stream(source, DEPTH_ENDPOINT, DEPTH_STREAM, Some(payload))?;
    let v = json::document(payload)?;
    migration(&v, "s")?;
    if v["e"] != "depthUpdate" {
        return Err(ContractError("PARSE_FAILURE"));
    }
    let (first, final_id, previous) = (uint(&v["U"])?, uint(&v["u"])?, uint(&v["pu"])?);
    if first > final_id {
        return Err(ContractError("INVALID_DEPTH_RANGE"));
    }
    let (e, t) = (uint(&v["E"])?, uint(&v["T"])?);
    Ok((
        BinanceDepthUpdate {
            first_update_id: Some(first),
            final_update_id: Some(final_id),
            previous_final_update_id: Some(previous),
            native_event_time_ms: Some(e),
            native_transaction_time_ms: Some(t),
            event_timestamp_ns: Some(ms_to_ns(e)?),
            transaction_timestamp_ns: Some(ms_to_ns(t)?),
            bids: levels(&v["b"])?,
            asks: levels(&v["a"])?,
            logical_event_id: Some(
                depth_id(
                    source.original_session_id.as_deref().unwrap_or_default(),
                    first,
                    final_id,
                )?
                .to_vec(),
            ),
            source: Some(source.clone()),
        },
        drift(
            &v,
            &["e", "E", "T", "s", "U", "u", "pu", "b", "a", "ps", "st"],
        ),
    ))
}

pub fn parse_snapshot(
    payload: &[u8],
    source: &HttpSourceContext,
) -> Result<(BinanceDepthSnapshot, Vec<String>)> {
    source::http(source, Some(payload))?;
    if source.endpoint.as_deref() != Some("/fapi/v1/depth") {
        return Err(ContractError("PROFILE_MISMATCH"));
    }
    let v = json::document(payload)?;
    let (e, t) = (uint(&v["E"])?, uint(&v["T"])?);
    Ok((
        BinanceDepthSnapshot {
            last_update_id: Some(uint(&v["lastUpdateId"])?),
            native_event_time_ms: Some(e),
            native_transaction_time_ms: Some(t),
            event_timestamp_ns: Some(ms_to_ns(e)?),
            transaction_timestamp_ns: Some(ms_to_ns(t)?),
            bids: levels(&v["bids"])?,
            asks: levels(&v["asks"])?,
            source: Some(source.clone()),
        },
        drift(&v, &["lastUpdateId", "E", "T", "bids", "asks"]),
    ))
}

pub fn parse_exchange_info(
    payload: &[u8],
    source: &HttpSourceContext,
    version: &str,
) -> Result<(InstrumentMetadataVersion, Vec<String>)> {
    source::http(source, Some(payload))?;
    if source.endpoint.as_deref() != Some("/fapi/v1/exchangeInfo") || version.is_empty() {
        return Err(ContractError("PROFILE_MISMATCH"));
    }
    let root = json::document(payload)?;
    let symbols = array(&root["symbols"])?;
    if symbols.iter().any(|v| !v.is_object()) {
        return Err(ContractError("PARSE_FAILURE"));
    }
    let found: Vec<_> = symbols
        .iter()
        .filter(|v| v["symbol"] == "BTCUSDT")
        .collect();
    if found.len() != 1 {
        return Err(ContractError("METADATA_INCOMPLETE"));
    }
    let v = found[0];
    migration(v, "symbol")?;
    for (key, expected) in [
        ("pair", "BTCUSDT"),
        ("contractType", "PERPETUAL"),
        ("baseAsset", "BTC"),
        ("quoteAsset", "USDT"),
        ("marginAsset", "USDT"),
    ] {
        if v[key] != expected {
            return Err(ContractError("PROFILE_MISMATCH"));
        }
    }
    let mut filters = BTreeMap::new();
    for filter in array(&v["filters"])? {
        let name = text(&filter["filterType"])?;
        if filters.insert(name, filter).is_some() {
            return Err(ContractError("METADATA_INCOMPLETE"));
        }
    }
    let price = filters
        .get("PRICE_FILTER")
        .ok_or(ContractError("METADATA_INCOMPLETE"))?;
    let lot = filters
        .get("LOT_SIZE")
        .ok_or(ContractError("METADATA_INCOMPLETE"))?;
    if price.get("tickSize").is_none()
        || ["stepSize", "minQty", "maxQty"]
            .iter()
            .any(|k| lot.get(k).is_none())
    {
        return Err(ContractError("METADATA_INCOMPLETE"));
    }
    let (minimum, maximum) = (
        decimal(&lot["minQty"], false)?,
        decimal(&lot["maxQty"], true)?,
    );
    if compare(&minimum, &maximum).is_gt() {
        return Err(ContractError("METADATA_INCOMPLETE"));
    }
    let result = InstrumentMetadataVersion {
        instrument_id: Some(INSTRUMENT_ID.into()),
        metadata_version: Some(version.into()),
        known_from_ns: source.lineage.as_ref().and_then(|v| v.receive_timestamp_ns),
        effective_time_basis: Some(EffectiveTimeBasis::Unknown as i32),
        product_type: Some("LINEAR".into()),
        margin_asset: Some("USDT".into()),
        settlement_asset: Some("USDT".into()),
        quantity_unit: Some("BTC".into()),
        tick_size: Some(decimal(&price["tickSize"], true)?),
        lot_size: Some(decimal(&lot["stepSize"], true)?),
        minimum_quantity: Some(minimum),
        maximum_quantity: Some(maximum),
        venue: Some("BINANCE".into()),
        product_family: Some("USD_M_FUTURES".into()),
        symbol: Some("BTCUSDT".into()),
        pair: Some("BTCUSDT".into()),
        contract_type: Some("PERPETUAL".into()),
        base_asset: Some("BTC".into()),
        quote_asset: Some("USDT".into()),
        venue_status: Some(text(&v["status"])?.into()),
        source: Some(source.clone()),
        ..Default::default()
    };
    result.validate_metadata()?;
    let mut unknown: Vec<_> = drift(
        v,
        &[
            "symbol",
            "pair",
            "contractType",
            "baseAsset",
            "quoteAsset",
            "marginAsset",
            "status",
            "pricePrecision",
            "quantityPrecision",
            "onboardDate",
            "filters",
            "st",
            "ps",
        ],
    )
    .into_iter()
    .map(|k| format!("symbols.BTCUSDT.{k}"))
    .collect();
    unknown.extend(
        drift(
            &root,
            &[
                "symbols",
                "serverTime",
                "timezone",
                "rateLimits",
                "exchangeFilters",
                "assets",
            ],
        )
        .into_iter()
        .map(|k| format!("root.{k}")),
    );
    for (name, filter) in filters {
        if !["PRICE_FILTER", "LOT_SIZE"].contains(&name) {
            unknown.push(format!("filter.{name}"));
        } else {
            let known: &[&str] = if name == "PRICE_FILTER" {
                &["filterType", "tickSize", "minPrice", "maxPrice"]
            } else {
                &["filterType", "stepSize", "minQty", "maxQty"]
            };
            unknown.extend(
                drift(filter, known)
                    .into_iter()
                    .map(|k| format!("filter.{name}.{k}")),
            );
        }
    }
    unknown.sort();
    Ok((result, unknown))
}
