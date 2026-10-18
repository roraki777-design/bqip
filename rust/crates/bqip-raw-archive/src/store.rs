use crate::Result;
use std::future::Future;
#[derive(Clone, Debug, Default)]
pub struct RemoteMetadata {
    pub etag: Option<String>,
    pub version: Option<String>,
}
pub struct RemoteObject {
    pub bytes: Vec<u8>,
    pub metadata: RemoteMetadata,
}
#[derive(Debug, PartialEq, Eq)]
pub enum Created {
    New,
    AlreadyExists,
}
/// Only conditional create and bounded full GET. No delete, overwrite or listing.
pub trait ArchiveStore: Send + Sync + 'static {
    fn create_if_absent(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> impl Future<Output = Result<Created>> + Send;
    fn get_exact(
        &self,
        key: &str,
        max_bytes: u64,
    ) -> impl Future<Output = Result<Option<RemoteObject>>> + Send;
}
