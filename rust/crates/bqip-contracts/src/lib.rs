//! AC-001 contract kernel. Normative types are generated from proto/bqip/v1.

pub mod decimal;
pub mod identity;
pub mod manifests;
pub mod metadata;
pub mod validation;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/bqip.v1.rs"));
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractError(pub &'static str);

impl std::fmt::Display for ContractError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for ContractError {}

pub type Result<T> = std::result::Result<T, ContractError>;

pub fn hex(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(ALPHABET[(byte >> 4) as usize] as char);
        output.push(ALPHABET[(byte & 15) as usize] as char);
    }
    output
}
