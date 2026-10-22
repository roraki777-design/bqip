//! Streaming restricted-JCS transcript. Only fixed object framing is written here;
//! all variable values use the accepted canonicalizer.
use crate::{
    hash,
    normalize::{Outcome, OutcomeValue},
    plan::digest_without,
    Error, Result,
};
use bqip_contracts::{hex, manifests};
use prost::Message;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::io::Write;

pub fn entry(o: &Outcome) -> Value {
    let (version, basis, digest, rejection) = match &o.value {
        OutcomeValue::Market(e) => (
            Some(e.metadata_version.clone()),
            Some(e.metadata_resolution_basis.as_str_name()),
            Some(e.payload.digest()),
            None,
        ),
        OutcomeValue::Metadata(m) => (
            m.metadata_version.clone(),
            None,
            Some(hash(&m.encode_to_vec())),
            None,
        ),
        OutcomeValue::Rejected(e) => (None, None, None, Some(e.code())),
    };
    json!({
        "input_segment_ordinal":o.lineage.input_segment_ordinal,"segment_id":o.lineage.source_segment_id.to_string(),
        "record_index":o.lineage.source_record_index.to_string(),
        "capture_collector_uuid":o.lineage.capture_id.collector_instance_id.as_deref().and_then(|b|uuid::Uuid::from_slice(b).ok()).map(|u|u.to_string()),
        "capture_seq":o.lineage.capture_id.capture_seq.map(|n|n.to_string()),
        "raw_content_hash":hex(&o.lineage.raw_content_hash),"source_class":o.source_class.name(),"lane":o.lineage.lane,
        "original_receive_timestamp_ns":o.lineage.original_receive_timestamp_ns.to_string(),
        "original_monotonic_ns":o.lineage.original_monotonic_ns.to_string(),
        "raw_loss_status":o.lineage.raw_loss_status, "outcome":o.name(),"event_kind":o.parsed_kind,"venue_event_id":o.venue_event_id,
        "logical_event_id":o.logical_event_id.as_deref().map(hex),"metadata_version":version,
        "metadata_resolution_basis":basis,"quality_codes":o.quality_codes,"schema_drift_paths":o.schema_drift_paths,
        "normalized_payload_digest":digest,"rejection_code":rejection
    })
}

pub struct Transcript<W: Write> {
    out: W,
    hash: Sha256,
    count: u64,
}
impl<W: Write> Transcript<W> {
    pub fn new(out: W) -> Result<Self> {
        let mut s = Self {
            out,
            hash: Sha256::new(),
            count: 0,
        };
        s.core(b"{\"entries\":[")?;
        Ok(s)
    }
    fn core(&mut self, bytes: &[u8]) -> Result<()> {
        self.out
            .write_all(bytes)
            .map_err(|_| Error::OutputInvalid)?;
        self.hash.update(bytes);
        Ok(())
    }
    pub fn push(&mut self, o: &Outcome) -> Result<()> {
        if self.count > 0 {
            self.core(b",")?;
        }
        self.core(&manifests::canonical_bytes(&entry(o))?)?;
        self.count = self.count.checked_add(1).ok_or(Error::CapacityExceeded)?;
        Ok(())
    }
    pub fn finish(mut self, header: &Value) -> Result<String> {
        self.core(b"],\"header\":")?;
        self.core(&manifests::canonical_bytes(header)?)?;
        self.core(b",\"schema\":\"BQIP-REPLAY-TRANSCRIPT-V1\"")?;
        self.hash.update(b"}");
        let digest = hex(&self.hash.finalize());
        self.out
            .write_all(format!(",\"transcript_hash\":\"{digest}\"}}").as_bytes())
            .and_then(|()| self.out.flush())
            .map_err(|_| Error::OutputInvalid)?;
        Ok(digest)
    }
}

/// Bounded caller-supplied document verification; not an unbounded file reader.
pub fn verify(text: &str) -> Result<Value> {
    let v = manifests::parse_manifest(text).map_err(|_| Error::TranscriptInvalid)?;
    if v["schema"] != "BQIP-REPLAY-TRANSCRIPT-V1"
        || v["transcript_hash"].as_str() != Some(&digest_without(&v, "transcript_hash")?)
    {
        return Err(Error::TranscriptInvalid);
    }
    let entries = v["entries"].as_array().ok_or(Error::TranscriptInvalid)?;
    if v["header"]["input_record_count"].as_str() != Some(&entries.len().to_string()) {
        return Err(Error::TranscriptInvalid);
    }
    Ok(v)
}
