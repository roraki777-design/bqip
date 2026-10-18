#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Kind {
    Configuration,
    StateIo,
    ScanCapacity,
    LocalSourceInvalid,
    ArchiveAuthFailure,
    ArchiveCapabilityUnsupported,
    ArchiveObjectTooLarge,
    UploadRetryable,
    UploadFailed,
    RemoteObjectConflict,
    RemoteHashMismatch,
    RemoteLengthMismatch,
    RemoteManifestMismatch,
    RemoteArchiveDivergence,
}
/// Only static/allowlisted diagnostics cross the provider boundary.
#[derive(Clone, Debug, serde::Serialize)]
pub struct Error {
    pub kind: Kind,
    pub detail: &'static str,
    pub provider_status: Option<u16>,
    pub provider_code: Option<&'static str>,
}
impl Error {
    pub const fn new(kind: Kind, detail: &'static str) -> Self {
        Self {
            kind,
            detail,
            provider_status: None,
            provider_code: None,
        }
    }
    pub fn retryable(&self) -> bool {
        self.kind == Kind::UploadRetryable
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.detail)
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;
