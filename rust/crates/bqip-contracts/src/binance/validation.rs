use super::*;
use crate::{proto::*, ContractError, Result};

fn need<T>(v: Option<T>) -> Result<T> {
    v.ok_or(ContractError("MISSING_MARKET_FIELD"))
}
pub fn level(v: &PriceLevel) -> Result<()> {
    validate_decimal(need(v.price.as_ref())?, true)?;
    validate_decimal(need(v.quantity.as_ref())?, false)
}
pub fn trade(v: &TradeEvent) -> Result<()> {
    source::stream(need(v.source.as_ref())?, TRADE_ENDPOINT, TRADE_STREAM, None)?;
    validate_decimal(need(v.price.as_ref())?, true)?;
    validate_decimal(need(v.quantity.as_ref())?, true)?;
    validate_decimal(need(v.quantity_ex_rpi.as_ref())?, false)?;
    if compare(
        need(v.quantity_ex_rpi.as_ref())?,
        need(v.quantity.as_ref())?,
    )
    .is_gt()
    {
        return Err(ContractError("INVALID_RPI_QUANTITY"));
    }
    if need(v.first_venue_trade_id)? > need(v.last_venue_trade_id)? {
        return Err(ContractError("INVALID_TRADE_RANGE"));
    }
    let id = need(v.aggregate_id)?;
    if need(v.instrument_id.as_deref())? != INSTRUMENT_ID
        || need(v.venue_event_id.as_deref())? != id.to_string()
        || need(v.logical_event_id.as_deref())? != trade_id(id)?
    {
        return Err(ContractError("INVALID_MARKET_IDENTITY"));
    }
    if need(v.event_timestamp_ns)? != ms_to_ns(need(v.native_event_time_ms)?)?
        || need(v.trade_timestamp_ns)? != ms_to_ns(need(v.native_trade_time_ms)?)?
    {
        return Err(ContractError("TIMESTAMP_MISMATCH"));
    }
    let maker = need(v.buyer_is_maker)?;
    if need(v.granularity)? != TradeGranularity::Aggregated as i32
        || need(v.aggressor)?
            != (if maker {
                AggressorSide::Sell
            } else {
                AggressorSide::Buy
            }) as i32
        || need(v.liquidation)? != LiquidationObservation::NotObservedFromThisSource as i32
    {
        return Err(ContractError("INVALID_TRADE_SEMANTICS"));
    }
    Ok(())
}
pub fn depth(v: &BinanceDepthUpdate) -> Result<()> {
    let source = need(v.source.as_ref())?;
    source::stream(source, DEPTH_ENDPOINT, DEPTH_STREAM, None)?;
    let (first, final_id) = (need(v.first_update_id)?, need(v.final_update_id)?);
    need(v.previous_final_update_id)?;
    if first > final_id {
        return Err(ContractError("INVALID_DEPTH_RANGE"));
    }
    if need(v.logical_event_id.as_deref())?
        != depth_id(
            need(source.original_session_id.as_deref())?,
            first,
            final_id,
        )?
    {
        return Err(ContractError("INVALID_MARKET_IDENTITY"));
    }
    if need(v.event_timestamp_ns)? != ms_to_ns(need(v.native_event_time_ms)?)?
        || need(v.transaction_timestamp_ns)? != ms_to_ns(need(v.native_transaction_time_ms)?)?
    {
        return Err(ContractError("TIMESTAMP_MISMATCH"));
    }
    for value in v.bids.iter().chain(&v.asks) {
        level(value)?;
    }
    Ok(())
}
pub fn snapshot(v: &BinanceDepthSnapshot) -> Result<()> {
    let source = need(v.source.as_ref())?;
    source::http(source, None)?;
    if source.endpoint.as_deref() != Some("/fapi/v1/depth") {
        return Err(ContractError("PROFILE_MISMATCH"));
    }
    need(v.last_update_id)?;
    if need(v.event_timestamp_ns)? != ms_to_ns(need(v.native_event_time_ms)?)?
        || need(v.transaction_timestamp_ns)? != ms_to_ns(need(v.native_transaction_time_ms)?)?
    {
        return Err(ContractError("TIMESTAMP_MISMATCH"));
    }
    for value in v.bids.iter().chain(&v.asks) {
        level(value)?;
    }
    Ok(())
}
