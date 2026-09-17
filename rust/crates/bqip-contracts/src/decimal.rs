use crate::{proto::DecimalValue, ContractError, Result};

impl DecimalValue {
    pub fn normalize(coefficient: &str, mut scale: u32) -> Result<Self> {
        let negative = coefficient.starts_with('-');
        let digits = coefficient.strip_prefix('-').unwrap_or(coefficient);
        if digits.is_empty() || !digits.bytes().all(|value| value.is_ascii_digit()) {
            return Err(ContractError("INVALID_COEFFICIENT"));
        }
        let mut digits = digits.trim_start_matches('0');
        if digits.is_empty() {
            return Ok(Self {
                coefficient: Some("0".into()),
                scale: Some(0),
            });
        }
        while scale > 0 && digits.ends_with('0') {
            digits = &digits[..digits.len() - 1];
            scale -= 1;
        }
        let coefficient = if negative {
            format!("-{digits}")
        } else {
            digits.into()
        };
        Ok(Self {
            coefficient: Some(coefficient),
            scale: Some(scale),
        })
    }

    pub fn parse(text: &str) -> Result<Self> {
        if let Some((whole, fraction)) = text.split_once('.') {
            let unsigned = whole.strip_prefix('-').unwrap_or(whole);
            if unsigned.is_empty()
                || fraction.is_empty()
                || !unsigned.bytes().all(|b| b.is_ascii_digit())
                || !fraction.bytes().all(|b| b.is_ascii_digit())
            {
                return Err(ContractError("INVALID_DECIMAL_TEXT"));
            }
            let scale =
                u32::try_from(fraction.len()).map_err(|_| ContractError("INTEGER_OUT_OF_RANGE"))?;
            Self::normalize(&format!("{whole}{fraction}"), scale)
        } else {
            Self::normalize(text, 0)
        }
    }

    pub fn validate(&self) -> Result<()> {
        let coefficient = self
            .coefficient
            .as_deref()
            .ok_or(ContractError("MISSING_COEFFICIENT"))?;
        let scale = self.scale.ok_or(ContractError("MISSING_SCALE"))?;
        if Self::normalize(coefficient, scale)? != *self {
            return Err(ContractError("NON_CANONICAL_DECIMAL"));
        }
        Ok(())
    }

    pub fn identity_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        Ok(format!(
            "{}:{}",
            self.coefficient.as_deref().unwrap_or_default(),
            self.scale.unwrap_or_default()
        )
        .into_bytes())
    }
}
