use crate::{
    config::Config,
    store::{ArchiveStore, Created, RemoteMetadata, RemoteObject},
    Error, Kind, Result,
};
use futures_util::StreamExt;
use object_store::{
    aws::{AmazonS3, AmazonS3Builder, S3ConditionalPut},
    path::Path,
    ClientOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, RetryConfig,
};
use std::time::Duration;
pub struct Credentials {
    access: String,
    secret: String,
    token: Option<String>,
}
impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Credentials([REDACTED])")
    }
}
impl Credentials {
    pub fn from_env() -> Result<Self> {
        let read = |name| {
            std::env::var(name)
                .ok()
                .filter(|s| !s.is_empty())
                .ok_or(Error::new(
                    Kind::ArchiveAuthFailure,
                    "explicit archive credentials required",
                ))
        };
        let token = match std::env::var("BQIP_ARCHIVE_SESSION_TOKEN") {
            Ok(s) if !s.is_empty() => Some(s),
            Err(std::env::VarError::NotPresent) => None,
            _ => {
                return Err(Error::new(
                    Kind::ArchiveAuthFailure,
                    "invalid explicit session token",
                ))
            }
        };
        Ok(Self {
            access: read("BQIP_ARCHIVE_ACCESS_KEY_ID")?,
            secret: read("BQIP_ARCHIVE_SECRET_ACCESS_KEY")?,
            token,
        })
    }
}
pub struct S3Store {
    inner: AmazonS3,
}
fn builder(c: &Config, credentials: Credentials) -> AmazonS3Builder {
    // Static explicit credentials exclude the SDK default-chain / IMDS fallback.
    let mut b = AmazonS3Builder::new()
        .with_access_key_id(credentials.access)
        .with_secret_access_key(credentials.secret)
        .with_endpoint(&c.endpoint)
        .with_region(&c.region)
        .with_bucket_name(&c.bucket)
        .with_virtual_hosted_style_request(false)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .with_retry(RetryConfig {
            max_retries: 0,
            ..Default::default()
        })
        .with_client_options(
            ClientOptions::new()
                .with_allow_http(false)
                .with_allow_invalid_certificates(false)
                .with_timeout(c.timeout())
                .with_connect_timeout(Duration::from_millis(c.connect_timeout_ms)),
        );
    if let Some(t) = credentials.token {
        b = b.with_token(t);
    }
    b
}
impl S3Store {
    pub fn new(c: &Config, credentials: Credentials) -> Result<Self> {
        let c = c.clone().validate()?;
        Ok(Self {
            inner: builder(&c, credentials).build().map_err(provider_error)?,
        })
    }
}
// RetryError status is not public in object_store. Extract allowlisted tokens
// only; never retain/log the original diagnostic string, URL, headers or body.
fn provider_error(e: object_store::Error) -> Error {
    let diagnostic = e.to_string();
    let status = (400u16..=599).find(|n| {
        diagnostic.contains(&format!("{n} ")) || diagnostic.contains(&format!("status: {n}"))
    });
    let code = [
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "ExpiredToken",
        "InvalidToken",
        "AccessDenied",
        "NotImplemented",
        "InvalidRequest",
        "SlowDown",
        "InternalError",
        "ConditionalRequestConflict",
    ]
    .into_iter()
    .find(|c| diagnostic.contains(&format!("<Code>{c}</Code>")));
    let kind = match e {
        object_store::Error::PermissionDenied { .. }
        | object_store::Error::Unauthenticated { .. } => Kind::ArchiveAuthFailure,
        object_store::Error::NotSupported { .. } | object_store::Error::NotImplemented { .. } => {
            Kind::ArchiveCapabilityUnsupported
        }
        object_store::Error::InvalidPath { .. }
        | object_store::Error::UnknownConfigurationKey { .. } => Kind::Configuration,
        _ => match (status, code) {
            (Some(401 | 403), _)
            | (
                _,
                Some(
                    "InvalidAccessKeyId"
                    | "SignatureDoesNotMatch"
                    | "ExpiredToken"
                    | "InvalidToken"
                    | "AccessDenied",
                ),
            ) => Kind::ArchiveAuthFailure,
            (Some(501), _) | (_, Some("NotImplemented" | "InvalidRequest")) => {
                Kind::ArchiveCapabilityUnsupported
            }
            (Some(408 | 409 | 425 | 429 | 500..=599), _) => Kind::UploadRetryable,
            (Some(400..=499), _) => Kind::UploadFailed,
            _ => Kind::UploadRetryable,
        },
    };
    Error {
        kind,
        detail: "S3 operation failed",
        provider_status: status,
        provider_code: code,
    }
}
impl ArchiveStore for S3Store {
    async fn create_if_absent(&self, key: &str, bytes: &[u8]) -> Result<Created> {
        let path =
            Path::parse(key).map_err(|_| Error::new(Kind::Configuration, "invalid remote key"))?;
        match self
            .inner
            .put_opts(
                &path,
                bytes.to_vec().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Ok(Created::New),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => Ok(Created::AlreadyExists),
            Err(e) => Err(provider_error(e)),
        }
    }
    async fn get_exact(&self, key: &str, max_bytes: u64) -> Result<Option<RemoteObject>> {
        let path =
            Path::parse(key).map_err(|_| Error::new(Kind::Configuration, "invalid remote key"))?;
        let r = match self.inner.get(&path).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(provider_error(e)),
        };
        if r.meta.size > max_bytes {
            return Err(Error::new(
                Kind::RemoteLengthMismatch,
                "remote object exceeds expected length",
            ));
        }
        let metadata = RemoteMetadata {
            etag: r.meta.e_tag.clone(),
            version: r.meta.version.clone(),
        };
        let mut bytes = Vec::with_capacity(r.meta.size as usize);
        let mut stream = r.into_stream();
        while let Some(part) = stream.next().await {
            let part = part.map_err(provider_error)?;
            if bytes.len() as u64 + part.len() as u64 > max_bytes {
                return Err(Error::new(
                    Kind::RemoteLengthMismatch,
                    "remote stream exceeds expected length",
                ));
            }
            bytes.extend_from_slice(&part);
        }
        Ok(Some(RemoteObject { bytes, metadata }))
    }
}
#[cfg(test)]
mod tests;
