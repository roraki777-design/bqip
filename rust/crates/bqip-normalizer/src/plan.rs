use crate::{
    config::{full_hash, Config, BUILD_COMMIT},
    hash,
    input::{relative_path, Segment},
    Error, Result,
};
use bqip_contracts::{binance, hex, manifests};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashSet, path::Path};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Mode {
    Live,
    VersionPinnedResearch,
    AsLivedForensic,
}
impl Mode {
    pub fn guard(self) -> Result<()> {
        match self {
            Self::VersionPinnedResearch => Ok(()),
            Self::Live => Err(Error::ReplayPlanInvalid),
            // No approved deployment/artifact provenance contract exists for #005.
            Self::AsLivedForensic => Err(Error::ForensicProvenanceInsufficient),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentPin {
    pub relative_path: String,
    pub segment_id: String,
    pub capture_sha256: String,
    pub seal_sha256: String,
    pub manifest_sha256: String,
    /// Decimal strings avoid JCS's 53-bit integer boundary.
    pub record_count: String,
}
impl SegmentPin {
    pub fn from_segment(relative_path: String, s: &Segment) -> Self {
        Self {
            relative_path,
            segment_id: s.id.to_string(),
            capture_sha256: s.hashes[0].clone(),
            seal_sha256: s.hashes[1].clone(),
            manifest_sha256: s.hashes[2].clone(),
            record_count: s.record_count.to_string(),
        }
    }
    pub fn matches(&self, s: &Segment) -> bool {
        *self == Self::from_segment(self.relative_path.clone(), s)
    }
    pub fn count(&self) -> Result<u64> {
        let n = self
            .record_count
            .parse::<u64>()
            .map_err(|_| Error::ReplayPlanInvalid)?;
        if n == 0 || n.to_string() != self.record_count {
            return Err(Error::ReplayPlanInvalid);
        }
        Ok(n)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayPlan {
    pub schema: String,
    pub replay_mode: Mode,
    pub venue_profile_id: String,
    pub venue_profile_hash: String,
    pub normalizer_commit: String,
    pub normalizer_config_hash: String,
    pub ordered_segments: Vec<SegmentPin>,
    pub plan_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comments: Option<String>,
}
pub fn digest_without(v: &Value, field: &str) -> Result<String> {
    // Reuse JCS, with only the operational contract's root exclusions.
    let mut core = v.as_object().ok_or(Error::ReplayPlanInvalid)?.clone();
    core.remove(field);
    core.remove("comments");
    Ok(hash(&manifests::canonical_bytes(&Value::Object(core))?))
}
impl ReplayPlan {
    pub fn create(root: &Path, paths: &[String], config: &Config, mode: Mode) -> Result<Self> {
        config.validate()?;
        mode.guard()?;
        if !full_hash(BUILD_COMMIT, 40) {
            return Err(Error::NormalizerCommitMismatch);
        }
        if paths.is_empty() || paths.len() > config.max_segments {
            return Err(Error::ReplayPlanInvalid);
        }
        let mut plan = Self {
            schema: "BQIP-REPLAY-PLAN-V1".into(),
            replay_mode: mode,
            venue_profile_id: binance::PROFILE_ID.into(),
            venue_profile_hash: hex(&binance::profile_hash()?),
            normalizer_commit: BUILD_COMMIT.into(),
            normalizer_config_hash: config.digest()?,
            ordered_segments: Vec::new(),
            plan_hash: String::new(),
            comments: None,
        };
        for path in paths {
            let segment = Segment::load(root, path, config)?;
            plan.ordered_segments
                .push(SegmentPin::from_segment(path.clone(), &segment));
        }
        plan.plan_hash = plan.digest()?;
        plan.validate(config)?;
        Ok(plan)
    }
    pub fn parse(text: &str) -> Result<Self> {
        let v = manifests::parse_manifest(text).map_err(|_| Error::ReplayPlanInvalid)?;
        serde_json::from_value(v).map_err(|_| Error::ReplayPlanInvalid)
    }
    pub fn value(&self) -> Result<Value> {
        serde_json::to_value(self).map_err(|_| Error::ReplayPlanInvalid)
    }
    pub fn digest(&self) -> Result<String> {
        digest_without(&self.value()?, "plan_hash")
    }
    pub fn total_records(&self) -> Result<u64> {
        self.ordered_segments.iter().try_fold(0u64, |n, p| {
            n.checked_add(p.count()?).ok_or(Error::CapacityExceeded)
        })
    }
    pub fn validate(&self, config: &Config) -> Result<()> {
        config.validate()?;
        if self.schema != "BQIP-REPLAY-PLAN-V1" || self.digest()? != self.plan_hash {
            return Err(Error::ReplayPlanInvalid);
        }
        self.replay_mode.guard()?;
        if self.venue_profile_id != binance::PROFILE_ID
            || self.venue_profile_hash != hex(&binance::profile_hash()?)
        {
            return Err(Error::ReplayProfileMismatch);
        }
        if !full_hash(BUILD_COMMIT, 40) || self.normalizer_commit != BUILD_COMMIT {
            return Err(Error::NormalizerCommitMismatch);
        }
        if self.normalizer_config_hash != config.digest()? {
            return Err(Error::NormalizerConfigMismatch);
        }
        if self.ordered_segments.is_empty() || self.ordered_segments.len() > config.max_segments {
            return Err(Error::ReplayPlanInvalid);
        }
        let mut ids = HashSet::new();
        for p in &self.ordered_segments {
            relative_path(&p.relative_path)?;
            let id = Uuid::parse_str(&p.segment_id).map_err(|_| Error::ReplayPlanInvalid)?;
            if id.to_string() != p.segment_id
                || ![&p.capture_sha256, &p.seal_sha256, &p.manifest_sha256]
                    .iter()
                    .all(|s| full_hash(s, 64))
            {
                return Err(Error::ReplayPlanInvalid);
            }
            p.count()?;
            if !ids.insert(id) {
                return Err(Error::DuplicateReplaySegment);
            }
        }
        self.total_records()?;
        Ok(())
    }
    pub fn load_segment(&self, root: &Path, ordinal: usize, config: &Config) -> Result<Segment> {
        let p = self
            .ordered_segments
            .get(ordinal)
            .ok_or(Error::ReplayPlanInvalid)?;
        let s = Segment::load_pinned(
            root,
            &p.relative_path,
            config,
            Some(&[
                p.capture_sha256.clone(),
                p.seal_sha256.clone(),
                p.manifest_sha256.clone(),
            ]),
        )?;
        if !p.matches(&s) {
            return Err(Error::ReplayInputChanged);
        }
        Ok(s)
    }
    pub fn verify_inputs(&self, root: &Path, config: &Config) -> Result<()> {
        self.validate(config)?;
        for i in 0..self.ordered_segments.len() {
            self.load_segment(root, i, config)?;
        }
        Ok(())
    }
}
