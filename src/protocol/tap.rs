//! Shared TAP envelope decode (`{"p":"tap","op":...}`).

use serde_json::{Map, Value};

use crate::error::{Error, Result};

pub const PROTOCOL_TAP: &str = "tap";

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum TapOp {
    DmtDeploy,
    DmtMint,
    TokenTransfer,
    TokenSend,
    BlockTransferables,
    UnblockTransferables,
}

impl TapOp {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "dmt-deploy" => Some(Self::DmtDeploy),
            "dmt-mint" => Some(Self::DmtMint),
            "token-transfer" => Some(Self::TokenTransfer),
            "token-send" => Some(Self::TokenSend),
            "block-transferables" => Some(Self::BlockTransferables),
            "unblock-transferables" => Some(Self::UnblockTransferables),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::DmtDeploy => "dmt-deploy",
            Self::DmtMint => "dmt-mint",
            Self::TokenTransfer => "token-transfer",
            Self::TokenSend => "token-send",
            Self::BlockTransferables => "block-transferables",
            Self::UnblockTransferables => "unblock-transferables",
        }
    }
}

/// Decode a JSON payload into `(op, body)`. Returns a protocol error
/// for anything that isn't a TAP-protocol object with a known op.
pub fn decode_envelope(payload: &[u8]) -> Result<(TapOp, Map<String, Value>)> {
    let raw: Value =
        serde_json::from_slice(payload).map_err(|e| Error::Protocol(format!("json: {e}")))?;
    let mut object = match raw {
        Value::Object(m) => m,
        _ => return Err(Error::Protocol("tap payload must be JSON object".into())),
    };
    let protocol = take_string(&mut object, "p")?;
    if protocol != PROTOCOL_TAP {
        return Err(Error::Protocol(format!("not TAP protocol: {protocol:?}")));
    }
    let op_str = take_string(&mut object, "op")?;
    let op = TapOp::parse(&op_str)
        .ok_or_else(|| Error::Protocol(format!("unsupported op: {op_str}")))?;
    Ok((op, object))
}

pub fn take_string(object: &mut Map<String, Value>, field: &'static str) -> Result<String> {
    match object.remove(field) {
        None => Err(Error::Protocol(format!("missing field: {field}"))),
        Some(Value::String(v)) => Ok(v),
        Some(_) => Err(Error::Protocol(format!("field {field} must be string"))),
    }
}

pub fn take_optional_string(
    object: &mut Map<String, Value>,
    field: &'static str,
) -> Result<Option<String>> {
    match object.remove(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(v)) => Ok(Some(v)),
        Some(_) => Err(Error::Protocol(format!("field {field} must be string"))),
    }
}

/// Take a field that the spec defines as a numeric string but some
/// inscribers emit as a raw JSON number. Accept either; reject anything
/// else (booleans, objects, arrays, null).
pub fn take_numeric_string(object: &mut Map<String, Value>, field: &'static str) -> Result<String> {
    match object.remove(field) {
        None => Err(Error::Protocol(format!("missing field: {field}"))),
        Some(Value::String(v)) => Ok(v),
        Some(Value::Number(n)) => Ok(n.to_string()),
        Some(_) => Err(Error::Protocol(format!(
            "field {field} must be string or number"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deploy_envelope() {
        let p = br#"{"p":"tap","op":"dmt-deploy","tick":"nat","elem":"x","dt":"n"}"#;
        let (op, body) = decode_envelope(p).unwrap();
        assert_eq!(op, TapOp::DmtDeploy);
        assert_eq!(body.get("tick").and_then(Value::as_str), Some("nat"));
    }

    #[test]
    fn rejects_non_tap() {
        assert!(decode_envelope(br#"{"p":"brc-20","op":"deploy"}"#).is_err());
    }

    #[test]
    fn rejects_unsupported() {
        // `token-send` is now supported; pick an op that really isn't.
        assert!(decode_envelope(br#"{"p":"tap","op":"token-trade"}"#).is_err());
    }
}
