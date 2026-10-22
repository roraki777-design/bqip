//! Stable operational errors. No RAW body, host path or runtime ID in codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    ReplayPlanInvalid,
    ReplayInputChanged,
    DuplicateReplaySegment,
    SegmentSourceInvalid,
    ReplayProfileMismatch,
    NormalizerCommitMismatch,
    NormalizerConfigMismatch,
    UnsupportedSchemaVersion,
    UnsupportedRawSource,
    UnsupportedContentEncoding,
    ParseFailure,
    ProfileMismatch,
    MetadataMissing,
    MetadataAmbiguous,
    MetadataEffectiveTimeUnknown,
    MetadataEventTimeMissing,
    ForensicProvenanceInsufficient,
    CatalogInvalid,
    CapacityExceeded,
    OutputInvalid,
    TranscriptInvalid,
    Contract(&'static str),
}
impl Error {
    pub fn code(self) -> &'static str {
        match self {
            Self::ReplayPlanInvalid => "REPLAY_PLAN_INVALID",
            Self::ReplayInputChanged => "REPLAY_INPUT_CHANGED",
            Self::DuplicateReplaySegment => "DUPLICATE_REPLAY_SEGMENT",
            Self::SegmentSourceInvalid => "SEGMENT_SOURCE_INVALID",
            Self::ReplayProfileMismatch => "REPLAY_PROFILE_MISMATCH",
            Self::NormalizerCommitMismatch => "NORMALIZER_COMMIT_MISMATCH",
            Self::NormalizerConfigMismatch => "NORMALIZER_CONFIG_MISMATCH",
            Self::UnsupportedSchemaVersion => "UNSUPPORTED_SCHEMA_VERSION",
            Self::UnsupportedRawSource => "UNSUPPORTED_RAW_SOURCE",
            Self::UnsupportedContentEncoding => "UNSUPPORTED_CONTENT_ENCODING",
            Self::ParseFailure => "PARSE_FAILURE",
            Self::ProfileMismatch => "PROFILE_MISMATCH",
            Self::MetadataMissing => "METADATA_MISSING",
            Self::MetadataAmbiguous => "METADATA_AMBIGUOUS",
            Self::MetadataEffectiveTimeUnknown => "METADATA_EFFECTIVE_TIME_UNKNOWN",
            Self::MetadataEventTimeMissing => "METADATA_EVENT_TIME_MISSING",
            Self::ForensicProvenanceInsufficient => "FORENSIC_PROVENANCE_INSUFFICIENT",
            Self::CatalogInvalid => "METADATA_CATALOG_INVALID",
            Self::CapacityExceeded => "REPLAY_CAPACITY_EXCEEDED",
            Self::OutputInvalid => "REPLAY_OUTPUT_INVALID",
            Self::TranscriptInvalid => "REPLAY_TRANSCRIPT_INVALID",
            Self::Contract(s) => s,
        }
    }
}
impl From<bqip_contracts::ContractError> for Error {
    fn from(e: bqip_contracts::ContractError) -> Self {
        match e.0 {
            "PARSE_FAILURE" => Self::ParseFailure,
            "PROFILE_MISMATCH" => Self::ProfileMismatch,
            "METADATA_MISSING" => Self::MetadataMissing,
            "METADATA_AMBIGUOUS" => Self::MetadataAmbiguous,
            "METADATA_EFFECTIVE_TIME_UNKNOWN" => Self::MetadataEffectiveTimeUnknown,
            code => Self::Contract(code),
        }
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;
