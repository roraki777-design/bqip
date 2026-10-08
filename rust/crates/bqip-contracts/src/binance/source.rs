use super::{profile_hash, PROFILE_ID};
use crate::{identity::raw_hash, proto::*, ContractError, Result};

pub fn lineage(v: &SourceLineage, payload: Option<&[u8]>) -> Result<()> {
    v.capture_id
        .as_ref()
        .ok_or(ContractError("MISSING_SOURCE_FIELD"))?
        .validate()?;
    v.receive_timestamp_ns
        .ok_or(ContractError("MISSING_SOURCE_FIELD"))?;
    if v.venue_profile_id.as_deref() != Some(PROFILE_ID)
        || v.venue_profile_hash.as_deref() != Some(&profile_hash()?)
    {
        return Err(ContractError("PROFILE_MISMATCH"));
    }
    if v.raw_content_hash.as_ref().is_none_or(|h| h.len() != 32) {
        return Err(ContractError("INVALID_SOURCE_LINEAGE"));
    }
    if payload.is_some_and(|p| v.raw_content_hash.as_deref() != Some(&raw_hash(p))) {
        return Err(ContractError("RAW_HASH_MISMATCH"));
    }
    Ok(())
}

pub fn http(v: &HttpSourceContext, payload: Option<&[u8]>) -> Result<()> {
    lineage(
        v.lineage
            .as_ref()
            .ok_or(ContractError("MISSING_SOURCE_FIELD"))?,
        payload,
    )?;
    if v.transport != Some(SourceTransport::HttpRest as i32) || v.method.as_deref() != Some("GET") {
        return Err(ContractError("PROFILE_MISMATCH"));
    }
    if v.http_status != Some(200) {
        return Err(ContractError("HTTP_STATUS_FAILURE"));
    }
    let mut previous = "";
    let mut query = Vec::new();
    for parameter in &v.query_parameters {
        let key = parameter
            .name
            .as_deref()
            .ok_or(ContractError("MISSING_SOURCE_FIELD"))?;
        let value = parameter
            .value
            .as_deref()
            .ok_or(ContractError("MISSING_SOURCE_FIELD"))?;
        if !key.is_ascii() || key <= previous {
            return Err(ContractError("NON_CANONICAL_QUERY"));
        }
        previous = key;
        query.push((key, value));
    }
    let expected = match v.endpoint.as_deref() {
        Some("/fapi/v1/exchangeInfo") => vec![],
        Some("/fapi/v1/depth") => vec![("limit", "1000"), ("symbol", "BTCUSDT")],
        _ => return Err(ContractError("PROFILE_MISMATCH")),
    };
    if query != expected {
        return Err(ContractError("PROFILE_MISMATCH"));
    }
    Ok(())
}

pub fn stream(
    v: &StreamSourceContext,
    endpoint: &str,
    channel: &str,
    payload: Option<&[u8]>,
) -> Result<()> {
    lineage(
        v.lineage
            .as_ref()
            .ok_or(ContractError("MISSING_SOURCE_FIELD"))?,
        payload,
    )?;
    if v.transport != Some(SourceTransport::Websocket as i32)
        || v.endpoint.as_deref() != Some(endpoint)
        || v.channel.as_deref() != Some(channel)
    {
        return Err(ContractError("PROFILE_MISMATCH"));
    }
    if v.original_session_id
        .as_ref()
        .is_none_or(|id| id.len() != 16)
    {
        return Err(ContractError("INVALID_ORIGINAL_SESSION"));
    }
    Ok(())
}
