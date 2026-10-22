//! #005 deterministic offline research replay. No book construction or network runtime.
pub mod config;
pub mod error;
pub mod input;
pub mod metadata;
pub mod normalize;
pub mod plan;
pub mod transcript;
use bqip_contracts::{hex, proto::*};
use config::{Config, BUILD_COMMIT, LOCK_BYTES};
pub use error::{Error, Result};
use metadata::Catalog;
use normalize::{Outcome, OutcomeValue, RecordPosition};
use plan::ReplayPlan;
use prost::Message;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, io::Write, path::Path, time::Instant};

pub fn hash(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

pub struct Replay<'a> {
    pub source_root: &'a Path,
    pub plan: &'a ReplayPlan,
    pub config: &'a Config,
    /// None discovers all accepted exchangeInfo observations in a first pass.
    /// Some pins the supplied immutable catalog exclusively; never implicitly merges it.
    pub catalog: Option<&'a Catalog>,
}
#[derive(Debug)]
pub struct Summary {
    pub input_record_count: u64,
    pub normalized_count: u64,
    pub metadata_observation_count: u64,
    pub rejected_count: u64,
    pub parser_successes: BTreeMap<String, u64>,
    pub rejection_codes: BTreeMap<String, u64>,
    pub raw_bytes: u64,
    pub transcript_hash: String,
    pub metadata_catalog_digest: String,
    pub transcript_time_ns: u128,
    pub elapsed_ns: u128,
}
impl Summary {
    pub fn report(&self, plan: &ReplayPlan, context: &ProcessingContext) -> Value {
        json!({"schema":"BQIP-REPLAY-RUN-V1","run_id":context.run_id.as_deref().map(hex),
            "processing_timestamp_ns":context.processing_timestamp_ns.map(|n|n.to_string()),"normalizer_commit":BUILD_COMMIT,
            "normalizer_config_hash":plan.normalizer_config_hash,"plan_hash":plan.plan_hash,"transcript_hash":self.transcript_hash,
            "metadata_catalog_digest":self.metadata_catalog_digest,"replay_mode":plan.replay_mode,
            "input_record_count":self.input_record_count.to_string(),"normalized_count":self.normalized_count.to_string(),
            "metadata_observation_count":self.metadata_observation_count.to_string(),"rejected_count":self.rejected_count.to_string(),
            "parser_successes":self.parser_successes,"rejection_codes":self.rejection_codes,"raw_bytes":self.raw_bytes.to_string(),
            "transcript_time_ns":self.transcript_time_ns.to_string(),"elapsed_ns":self.elapsed_ns.to_string(),
            "cross_lane_order":"OPERATIONAL_TRAVERSAL_ONLY","completion":"SUCCESS"})
    }
}
impl Replay<'_> {
    pub fn discover_catalog(&self) -> Result<Catalog> {
        let mut versions = Vec::new();
        let mut bytes = 0usize;
        for i in 0..self.plan.ordered_segments.len() {
            let s = self.plan.load_segment(self.source_root, i, self.config)?;
            for frame in s.records() {
                let frame = frame?;
                let raw = frame
                    .decode_event()
                    .map_err(|_| Error::SegmentSourceInvalid)?;
                if let Ok((v, _)) = normalize::metadata_observation(&raw, &frame.payload) {
                    bytes = bytes
                        .checked_add(v.encoded_len())
                        .ok_or(Error::CapacityExceeded)?;
                    if versions.len() >= self.config.max_metadata_versions
                        || bytes > self.config.max_metadata_bytes
                    {
                        return Err(Error::CapacityExceeded);
                    }
                    versions.push(v);
                }
            }
        }
        Catalog::new(versions, self.config)
    }

    /// Callback outputs are tentative until Ok. No completed transcript on any failure.
    /// Incremental frames from one bounded validated immutable byte snapshot at a time.
    pub fn execute<W: Write>(
        &self,
        processing: &ProcessingContext,
        output: W,
        mut observe: impl FnMut(&Outcome) -> Result<()>,
    ) -> Result<Summary> {
        let start = Instant::now();
        self.plan.verify_inputs(self.source_root, self.config)?;
        processing.validate()?;
        if processing.code_commit.as_deref() != Some(BUILD_COMMIT) {
            return Err(Error::NormalizerCommitMismatch);
        }
        if processing.replay_mode != Some(ReplayMode::VersionPinnedResearch as i32) {
            return Err(Error::ForensicProvenanceInsufficient);
        }
        if processing.schema_version != Some(self.config.schema_version) {
            return Err(Error::UnsupportedSchemaVersion);
        }
        let discovered;
        let catalog = if let Some(c) = self.catalog {
            c
        } else {
            discovered = self.discover_catalog()?;
            &discovered
        };
        let mut transcript = transcript::Transcript::new(output)?;
        let mut summary = Summary {
            input_record_count: 0,
            normalized_count: 0,
            metadata_observation_count: 0,
            rejected_count: 0,
            parser_successes: BTreeMap::new(),
            rejection_codes: BTreeMap::new(),
            raw_bytes: 0,
            transcript_hash: String::new(),
            metadata_catalog_digest: catalog.digest().into(),
            transcript_time_ns: 0,
            elapsed_ns: 0,
        };
        for i in 0..self.plan.ordered_segments.len() {
            let s = self.plan.load_segment(self.source_root, i, self.config)?;
            let mut index = 0u64;
            for frame in s.records() {
                let frame = frame?;
                let raw = frame
                    .decode_event()
                    .map_err(|_| Error::SegmentSourceInvalid)?;
                let position = RecordPosition {
                    segment_id: s.id,
                    segment_ordinal: i,
                    record_index: index,
                    lane: s.lane,
                    loss_recorded: s.loss_recorded(),
                };
                let outcome = normalize::normalize_record(
                    &raw,
                    &frame.payload,
                    &position,
                    catalog,
                    processing,
                )?;
                summary.input_record_count += 1;
                summary.raw_bytes += frame.payload.len() as u64;
                if let Some(kind) = outcome.parsed_kind {
                    *summary.parser_successes.entry(kind.into()).or_default() += 1;
                }
                match &outcome.value {
                    OutcomeValue::Market(_) => summary.normalized_count += 1,
                    OutcomeValue::Metadata(_) => summary.metadata_observation_count += 1,
                    OutcomeValue::Rejected(e) => {
                        summary.rejected_count += 1;
                        *summary.rejection_codes.entry(e.code().into()).or_default() += 1;
                    }
                }
                observe(&outcome)?;
                let timing = Instant::now();
                transcript.push(&outcome)?;
                summary.transcript_time_ns += timing.elapsed().as_nanos();
                index += 1;
            }
            if index != s.record_count {
                return Err(Error::ReplayInputChanged);
            }
        }
        if summary.input_record_count != self.plan.total_records()?
            || summary.input_record_count
                != summary.normalized_count
                    + summary.metadata_observation_count
                    + summary.rejected_count
        {
            return Err(Error::ReplayInputChanged);
        }
        // Detect replacement/mutation after planning, metadata discovery or any callback.
        // Successful output describes the exact pinned bytes; RAW remains read-only.
        self.plan.verify_inputs(self.source_root, self.config)?;
        let header = json!({"plan_hash":self.plan.plan_hash,"replay_mode":self.plan.replay_mode,
            "normalizer_commit":BUILD_COMMIT,"normalizer_config_hash":self.plan.normalizer_config_hash,
            "cargo_lock_sha256":hash(LOCK_BYTES),"venue_profile_id":self.plan.venue_profile_id,
            "venue_profile_hash":self.plan.venue_profile_hash,"metadata_catalog_digest":catalog.digest(),
            "schema_version":self.config.schema_version,"input_record_count":summary.input_record_count.to_string(),
            "ordered_segments":self.plan.ordered_segments});
        let timing = Instant::now();
        summary.transcript_hash = transcript.finish(&header)?;
        summary.transcript_time_ns += timing.elapsed().as_nanos();
        self.plan.verify_inputs(self.source_root, self.config)?;
        summary.elapsed_ns = start.elapsed().as_nanos();
        Ok(summary)
    }
}
