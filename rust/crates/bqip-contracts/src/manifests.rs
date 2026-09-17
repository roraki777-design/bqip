use crate::{identity::raw_hash, ContractError, Result};
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};

const MAX_SAFE: i64 = 9_007_199_254_740_991;

// Strict deserialization preserves duplicate-key errors before Value can erase them.
struct StrictValue(Value);
impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct StrictVisitor;
        impl<'de> Visitor<'de> for StrictVisitor {
            type Value = StrictValue;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("integer-only I-JSON without duplicate keys")
            }
            fn visit_unit<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Null))
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Bool(v)))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<Self::Value, E> {
                if !(-MAX_SAFE..=MAX_SAFE).contains(&v) {
                    return Err(E::custom("unsafe integer"));
                }
                Ok(StrictValue(Value::from(v)))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<Self::Value, E> {
                if v > MAX_SAFE as u64 {
                    return Err(E::custom("unsafe integer"));
                }
                Ok(StrictValue(Value::from(v)))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::String(v.into())))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(StrictValue(value)) = sequence.next_element()? {
                    values.push(value);
                }
                Ok(StrictValue(Value::Array(values)))
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Map::new();
                while let Some((key, StrictValue(value))) =
                    map.next_entry::<String, StrictValue>()?
                {
                    if values.insert(key, value).is_some() {
                        return Err(de::Error::custom("duplicate JSON key"));
                    }
                }
                Ok(StrictValue(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(StrictVisitor)
    }
}

// serde_json otherwise classifies the integer token -0 as a float. Validate the
// numeric token grammar before parsing and normalize that single integer token.
fn integer_tokens(text: &str) -> Result<String> {
    let input = text.as_bytes();
    let mut output = Vec::with_capacity(input.len());
    let (mut index, mut quoted, mut escaped) = (0, false, false);
    while index < input.len() {
        let byte = input[index];
        if quoted {
            output.push(byte);
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
            index += 1;
        } else if byte == b'"' {
            quoted = true;
            output.push(byte);
            index += 1;
        } else if byte == b'-' || byte.is_ascii_digit() {
            let start = index;
            while index < input.len()
                && (input[index].is_ascii_digit()
                    || matches!(input[index], b'-' | b'+' | b'.' | b'e' | b'E'))
            {
                index += 1;
            }
            let token = &input[start..index];
            let digits = token.strip_prefix(b"-").unwrap_or(token);
            if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
                return Err(ContractError("NON_INTEGER_JSON_NUMBER"));
            }
            output.extend_from_slice(if token == b"-0" { b"0" } else { token });
        } else {
            output.push(byte);
            index += 1;
        }
    }
    String::from_utf8(output).map_err(|_| ContractError("INVALID_UNICODE"))
}

pub fn parse_manifest(text: &str) -> Result<Value> {
    let normalized = integer_tokens(text)?;
    let StrictValue(value) =
        serde_json::from_str(&normalized).map_err(|_| ContractError("INVALID_MANIFEST_JSON"))?;
    if !value.is_object() {
        return Err(ContractError("MANIFEST_OBJECT_REQUIRED"));
    }
    Ok(value)
}

fn write(value: &Value, output: &mut Vec<u8>) -> Result<()> {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(v) => output.extend_from_slice(if *v { b"true" } else { b"false" }),
        Value::Number(v) => {
            let integer = v
                .as_i64()
                .filter(|n| (-MAX_SAFE..=MAX_SAFE).contains(n))
                .ok_or(ContractError("UNSAFE_OR_NON_INTEGER_JSON_NUMBER"))?;
            output.extend_from_slice(integer.to_string().as_bytes());
        }
        Value::String(v) => output.extend_from_slice(
            serde_json::to_string(v)
                .map_err(|_| ContractError("INVALID_UNICODE"))?
                .as_bytes(),
        ),
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write(value, output)?;
            }
            output.push(b']');
        }
        Value::Object(values) => {
            let mut entries: Vec<_> = values.iter().collect();
            entries.sort_by(|(a, _), (b, _)| a.encode_utf16().cmp(b.encode_utf16()));
            output.push(b'{');
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write(&Value::String(key.clone()), output)?;
                output.push(b':');
                write(value, output)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}

pub fn canonical_bytes(value: &Value) -> Result<Vec<u8>> {
    let mut result = Vec::new();
    write(value, &mut result)?;
    Ok(result)
}

pub fn canonical_manifest(value: &Value) -> Result<Vec<u8>> {
    canonical_bytes(value)?;
    let mut body = value
        .as_object()
        .ok_or(ContractError("MANIFEST_OBJECT_REQUIRED"))?
        .clone();
    body.remove("manifest_hash");
    body.remove("comments");
    canonical_bytes(&Value::Object(body))
}

pub fn manifest_hash(value: &Value) -> Result<[u8; 32]> {
    Ok(raw_hash(&canonical_manifest(value)?))
}
