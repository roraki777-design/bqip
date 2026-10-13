use std::fmt;

#[derive(Debug, Clone, Copy, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Kind {
    Configuration,
    Connect,
    Handshake,
    HttpRequest,
    HttpStatus,
    RawWrite,
    DurabilityFence,
    SealManifest,
    Parse,
    ProfileMismatch,
    VenueGap,
    IdentityConflict,
    ResyncExhausted,
    Shutdown,
}
#[derive(Debug, Clone)]
pub struct Error {
    pub kind: Kind,
    pub detail: String,
}
impl Error {
    pub fn new(kind: Kind, detail: impl fmt::Display) -> Self {
        Self {
            kind,
            detail: detail.to_string(),
        }
    }
}
/// Keep the complete transport cause chain: reqwest's top-level Display hides
/// DNS/TLS/proxy failures needed to distinguish an external block from a code fault.
pub fn network_error(kind: Kind, error: &reqwest::Error) -> Error {
    let mut detail = error.to_string();
    let mut cause = std::error::Error::source(error);
    while let Some(source) = cause {
        detail.push_str(": ");
        detail.push_str(&source.to_string());
        cause = source.source();
    }
    Error::new(kind, detail)
}

/// Only positively identified DNS/TCP failures use Connect. TLS failures,
/// upgrade failures and ambiguous negotiation failures remain Handshake.
pub fn websocket_error(error: reqwest::Error) -> Error {
    let mut kind = Kind::Handshake;
    let mut cause = std::error::Error::source(&error);
    while let Some(source) = cause {
        if let Some(io) = source.downcast_ref::<std::io::Error>() {
            use std::io::ErrorKind;
            if matches!(
                io.kind(),
                ErrorKind::ConnectionRefused
                    | ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::NotConnected
                    | ErrorKind::AddrNotAvailable
                    | ErrorKind::AddrInUse
                    | ErrorKind::NetworkDown
                    | ErrorKind::NetworkUnreachable
                    | ErrorKind::HostUnreachable
                    | ErrorKind::PermissionDenied
            ) {
                kind = Kind::Connect;
            }
        }
        // hyper-util's pinned DNS resolver has no public error type in reqwest.
        if source.to_string().starts_with("dns error") {
            kind = Kind::Connect;
        }
        cause = source.source();
    }
    network_error(kind, &error)
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.detail)
    }
}
impl std::error::Error for Error {}
impl From<bqip_contracts::ContractError> for Error {
    fn from(e: bqip_contracts::ContractError) -> Self {
        Self::new(
            match e.0 {
                "PROFILE_MISMATCH" => Kind::ProfileMismatch,
                "VENUE_GAP" => Kind::VenueGap,
                "VENUE_IDENTITY_CONFLICT" => Kind::IdentityConflict,
                _ => Kind::Parse,
            },
            e,
        )
    }
}
pub type Result<T> = std::result::Result<T, Error>;
