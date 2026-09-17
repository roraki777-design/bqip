use crate::{proto::CaptureId, ContractError, Result};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub fn raw_hash(payload: &[u8]) -> [u8; 32] {
    Sha256::digest(payload).into()
}

pub fn preimage(components: &[(&str, &[u8])]) -> Result<Vec<u8>> {
    let mut result = b"BQIP-ID-V1".to_vec();
    for (label, value) in components {
        for bytes in [label.as_bytes(), *value] {
            let length = u32::try_from(bytes.len())
                .map_err(|_| ContractError("IDENTITY_COMPONENT_TOO_LONG"))?;
            result.extend_from_slice(&length.to_be_bytes());
            result.extend_from_slice(bytes);
        }
    }
    Ok(result)
}

pub fn logical_hash(components: &[(&str, &[u8])]) -> Result<[u8; 32]> {
    Ok(raw_hash(&preimage(components)?))
}

pub fn enum_bytes(token: &str) -> Result<Vec<u8>> {
    if token.is_empty()
        || !token.as_bytes()[0].is_ascii_uppercase()
        || !token
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err(ContractError("INVALID_ENUM_TOKEN"));
    }
    Ok(token.as_bytes().to_vec())
}

/// Single owner per collector process. Reconnect does not replace this object.
pub struct CaptureSequencer {
    collector_instance_id: Uuid,
    next_seq: Option<u64>,
}

impl Default for CaptureSequencer {
    fn default() -> Self {
        Self::new(Uuid::new_v4())
    }
}

impl CaptureSequencer {
    pub fn new(collector_instance_id: Uuid) -> Self {
        Self {
            collector_instance_id,
            next_seq: Some(0),
        }
    }

    pub fn collector_instance_id(&self) -> Uuid {
        self.collector_instance_id
    }

    pub fn next_id(&mut self) -> Result<CaptureId> {
        let current = self
            .next_seq
            .ok_or(ContractError("CAPTURE_SEQUENCE_EXHAUSTED"))?;
        self.next_seq = current.checked_add(1);
        Ok(CaptureId {
            collector_instance_id: Some(self.collector_instance_id.as_bytes().to_vec()),
            capture_seq: Some(current),
        })
    }

    pub fn new_connection_id() -> Uuid {
        Uuid::new_v4()
    }
    pub fn new_session_id() -> Uuid {
        Uuid::new_v4()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collector_identity_is_stable_and_sequence_never_wraps() {
        let mut sequencer = CaptureSequencer::default();
        let collector = sequencer.collector_instance_id();
        let first = sequencer.next_id().unwrap();
        let second = sequencer.next_id().unwrap();
        assert_eq!(first.collector_instance_id, second.collector_instance_id);
        assert_eq!(
            first.collector_instance_id.as_deref(),
            Some(collector.as_bytes().as_slice())
        );
        assert_eq!(first.capture_seq, Some(0));
        assert_eq!(second.capture_seq, Some(1));
        sequencer.next_seq = Some(u64::MAX);
        assert_eq!(sequencer.next_id().unwrap().capture_seq, Some(u64::MAX));
        assert_eq!(
            sequencer.next_id().unwrap_err().0,
            "CAPTURE_SEQUENCE_EXHAUSTED"
        );
    }
}
