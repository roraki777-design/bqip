use crate::identity::raw_hash;
use crate::proto::{
    CaptureId, CauseCode, ManifestEnvelope, OperationalEvent, OperationalKind, PayloadKind,
    ProcessingContext, QualityEvent, RawCaptureEvent, RawLossStatus, RealtimeContinuity,
    ReplayMode,
};
use crate::{ContractError, Result};

fn uuid(value: &Option<Vec<u8>>) -> Result<()> {
    if value.as_ref().is_none_or(|v| v.len() != 16) {
        return Err(ContractError("INVALID_OR_MISSING_UUID"));
    }
    Ok(())
}

fn nonempty(value: &Option<String>) -> Result<()> {
    if value.as_ref().is_none_or(|v| v.is_empty()) {
        return Err(ContractError("MISSING_REQUIRED_STRING"));
    }
    Ok(())
}

impl CaptureId {
    pub fn validate(&self) -> Result<()> {
        uuid(&self.collector_instance_id)?;
        self.capture_seq
            .ok_or(ContractError("MISSING_CAPTURE_SEQUENCE"))?;
        Ok(())
    }
}

impl ProcessingContext {
    pub fn validate(&self) -> Result<()> {
        self.processing_timestamp_ns
            .ok_or(ContractError("MISSING_PROCESSING_TIME"))?;
        nonempty(&self.code_commit)?;
        if self.schema_version.is_none_or(|v| v == 0) {
            return Err(ContractError("INVALID_SCHEMA_VERSION"));
        }
        uuid(&self.run_id)?;
        let mode = ReplayMode::try_from(
            self.replay_mode
                .ok_or(ContractError("MISSING_REPLAY_MODE"))?,
        )
        .map_err(|_| ContractError("INVALID_REPLAY_MODE"))?;
        if mode == ReplayMode::Unknown {
            return Err(ContractError("INVALID_REPLAY_MODE"));
        }
        Ok(())
    }
}

impl RawCaptureEvent {
    pub fn validate(&self, payload: &[u8]) -> Result<()> {
        self.capture_id
            .as_ref()
            .ok_or(ContractError("MISSING_CAPTURE_ID"))?
            .validate()?;
        uuid(&self.connection_id)?;
        uuid(&self.session_id)?;
        for field in [&self.transport, &self.source, &self.endpoint, &self.channel] {
            nonempty(field)?;
        }
        self.receive_timestamp_ns
            .ok_or(ContractError("MISSING_RECEIVE_TIME"))?;
        self.monotonic_ns_since_collector_start
            .ok_or(ContractError("MISSING_MONOTONIC_TIME"))?;
        let kind = PayloadKind::try_from(
            self.payload_kind
                .ok_or(ContractError("MISSING_PAYLOAD_KIND"))?,
        )
        .map_err(|_| ContractError("INVALID_PAYLOAD_KIND"))?;
        if kind == PayloadKind::Unknown {
            return Err(ContractError("INVALID_PAYLOAD_KIND"));
        }
        if self.payload_length.map(|n| n as usize) != Some(payload.len()) {
            return Err(ContractError("PAYLOAD_LENGTH_MISMATCH"));
        }
        if self.raw_content_hash.as_deref() != Some(raw_hash(payload).as_slice()) {
            return Err(ContractError("RAW_HASH_MISMATCH"));
        }
        if let Some(context) = &self.processing_context {
            context.validate()?;
        }
        Ok(())
    }

    pub fn semantic_equal(&self, other: &Self) -> bool {
        let mut left = self.clone();
        let mut right = other.clone();
        left.processing_context = None;
        right.processing_context = None;
        left == right
    }
}

impl QualityEvent {
    pub fn validate(&self) -> Result<()> {
        CauseCode::try_from(self.cause.ok_or(ContractError("MISSING_CAUSE"))?)
            .map_err(|_| ContractError("INVALID_CAUSE"))?;
        RawLossStatus::try_from(
            self.raw_loss_status
                .ok_or(ContractError("MISSING_RAW_LOSS_STATUS"))?,
        )
        .map_err(|_| ContractError("INVALID_RAW_LOSS_STATUS"))?;
        RealtimeContinuity::try_from(
            self.realtime_continuity
                .ok_or(ContractError("MISSING_CONTINUITY"))?,
        )
        .map_err(|_| ContractError("INVALID_REALTIME_CONTINUITY"))?;
        self.timestamp_ns
            .ok_or(ContractError("MISSING_QUALITY_TIME"))?;
        self.context
            .as_ref()
            .ok_or(ContractError("MISSING_CONTEXT"))?
            .validate()?;
        if let Some(capture) = &self.capture_id {
            capture.validate()?;
        }
        if self.session_id.is_some() {
            uuid(&self.session_id)?;
        }
        Ok(())
    }
}

impl OperationalEvent {
    pub fn validate(&self) -> Result<()> {
        let kind =
            OperationalKind::try_from(self.kind.ok_or(ContractError("MISSING_OPERATIONAL_KIND"))?)
                .map_err(|_| ContractError("INVALID_OPERATIONAL_KIND"))?;
        if kind == OperationalKind::Unknown {
            return Err(ContractError("INVALID_OPERATIONAL_KIND"));
        }
        self.timestamp_ns
            .ok_or(ContractError("MISSING_OPERATIONAL_TIME"))?;
        self.context
            .as_ref()
            .ok_or(ContractError("MISSING_CONTEXT"))?
            .validate()?;
        if let Some(capture) = &self.capture_id {
            capture.validate()?;
        }
        if self.session_id.is_some() {
            uuid(&self.session_id)?;
        }
        if let Some(quality) = &self.quality {
            quality.validate()?;
        }
        Ok(())
    }
}

impl ManifestEnvelope {
    pub fn validate(&self) -> Result<()> {
        if self.schema_version.is_none_or(|version| version == 0) {
            return Err(ContractError("INVALID_SCHEMA_VERSION"));
        }
        let bytes = self
            .canonical_body_json
            .as_deref()
            .ok_or(ContractError("MISSING_MANIFEST_BODY"))?;
        let text = std::str::from_utf8(bytes).map_err(|_| ContractError("INVALID_UNICODE"))?;
        let body = crate::manifests::parse_manifest(text)?;
        if crate::manifests::canonical_manifest(&body)?.as_slice() != bytes {
            return Err(ContractError("NON_CANONICAL_MANIFEST_BODY"));
        }
        if self.manifest_hash.as_deref() != Some(raw_hash(bytes).as_slice()) {
            return Err(ContractError("MANIFEST_HASH_MISMATCH"));
        }
        Ok(())
    }
}
