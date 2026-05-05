//! Shared TAP envelope decode (`{"p":"tap","op":...}`).

use serde_json::{Map, Value};

use crate::error::{Error, Result};

pub const PROTOCOL_TAP: &str = "tap";

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum TapOp {
    TokenDeploy,
    TokenMint,
    DmtDeploy,
    DmtMint,
    TokenTransfer,
    TokenSend,
    TokenTrade,
    TokenAuth,
    PrivilegeAuth,
    BlockTransferables,
    UnblockTransferables,
}

impl TapOp {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "token-deploy" => Some(Self::TokenDeploy),
            "token-mint" => Some(Self::TokenMint),
            "dmt-deploy" => Some(Self::DmtDeploy),
            "dmt-mint" => Some(Self::DmtMint),
            "token-transfer" => Some(Self::TokenTransfer),
            "token-send" => Some(Self::TokenSend),
            "token-trade" => Some(Self::TokenTrade),
            "token-auth" => Some(Self::TokenAuth),
            "privilege-auth" => Some(Self::PrivilegeAuth),
            "block-transferables" => Some(Self::BlockTransferables),
            "unblock-transferables" => Some(Self::UnblockTransferables),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::TokenDeploy => "token-deploy",
            Self::TokenMint => "token-mint",
            Self::DmtDeploy => "dmt-deploy",
            Self::DmtMint => "dmt-mint",
            Self::TokenTransfer => "token-transfer",
            Self::TokenSend => "token-send",
            Self::TokenTrade => "token-trade",
            Self::TokenAuth => "token-auth",
            Self::PrivilegeAuth => "privilege-auth",
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
    // `p` and `op` are case-insensitive per ord-tap — both fields are
    // lowercased before comparison. A payload with `"P":"TAP"` or
    // `"op":"DMT-Mint"` is admitted by ord-tap; v2 previously required
    // exact case and would silently drop such inscriptions.
    let protocol = take_string(&mut object, "p")?.to_ascii_lowercase();
    if protocol != PROTOCOL_TAP {
        return Err(Error::Protocol(format!("not TAP protocol: {protocol:?}")));
    }
    let op_str = take_string(&mut object, "op")?.to_ascii_lowercase();
    let op = TapOp::parse(&op_str)
        .ok_or_else(|| Error::Protocol(format!("unsupported op: {op_str}")))?;
    Ok((op, object))
}

/// Match ord-tap's ValueStringifyActivation parser: raw JSON numbers
/// assigned to `amt`, `lim`, or `max` are parsed as strings while all
/// other JSON values keep normal serde semantics.
pub fn value_stringify_source(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] != b'"' {
            out.push(bytes[i] as char);
            i += 1;
            continue;
        }
        let key_start = i;
        i += 1;
        let mut escaped = false;
        while i < bytes.len() {
            let b = bytes[i];
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                i += 1;
                break;
            }
            i += 1;
        }
        let key_end = i;
        out.push_str(&s[key_start..key_end]);
        let mut j = i;
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        if j >= bytes.len() || bytes[j] != b':' {
            i = key_end;
            continue;
        }
        let key = serde_json::from_str::<String>(&s[key_start..key_end]).unwrap_or_default();
        if key != "max" && key != "lim" && key != "amt" {
            out.push_str(&s[i..=j]);
            i = j + 1;
            continue;
        }
        out.push_str(&s[i..=j]);
        j += 1;
        let value_prefix_start = j;
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        out.push_str(&s[value_prefix_start..j]);
        if j >= bytes.len() {
            i = j;
            continue;
        }
        if let Some(num_end) = scan_json_number_end(bytes, j) {
            out.push('"');
            out.push_str(&s[j..num_end]);
            out.push('"');
            i = num_end;
        } else {
            i = j;
        }
    }
    out
}

fn scan_json_number_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut i = start;
    if i < bytes.len() && bytes[i] == b'-' {
        i += 1;
    }
    if i >= bytes.len() {
        return None;
    }
    if bytes[i] == b'0' {
        i += 1;
    } else if (b'1'..=b'9').contains(&bytes[i]) {
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
    } else {
        return None;
    }
    if i < bytes.len() && bytes[i] == b'.' {
        let frac_start = i + 1;
        i = frac_start;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == frac_start {
            return Some(frac_start - 1);
        }
    }
    if i < bytes.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
        let exp_marker = i;
        i += 1;
        if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
            i += 1;
        }
        let exp_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == exp_start {
            return Some(exp_marker);
        }
    }
    Some(i)
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
        assert!(decode_envelope(br#"{"p":"tap","op":"token-unknown"}"#).is_err());
    }

    #[test]
    fn accepts_token_trade() {
        let (op, _) = decode_envelope(br#"{"p":"tap","op":"token-trade"}"#).unwrap();
        assert_eq!(op, TapOp::TokenTrade);
    }

    #[test]
    fn accepts_token_send() {
        let (op, _) = decode_envelope(br#"{"p":"tap","op":"token-send"}"#).unwrap();
        assert_eq!(op, TapOp::TokenSend);
    }

    #[test]
    fn accepts_token_auth() {
        let (op, _) = decode_envelope(br#"{"p":"tap","op":"token-auth"}"#).unwrap();
        assert_eq!(op, TapOp::TokenAuth);
    }

    #[test]
    fn value_stringify_preserves_writer_sources_for_amount_fields() {
        let src = r#"{"amt":1,"items":[{"amt":74.0100},{"lim":2.500}],"nested":{"max":21000000.0100},"lim":1000.90,"max":21000000.0100,"other":42,"\u0061mt":0.0100}"#;
        let parsed: Value = serde_json::from_str(&value_stringify_source(src)).unwrap();
        assert_eq!(parsed.get("amt").and_then(Value::as_str), Some("0.0100"));
        assert_eq!(parsed.get("lim").and_then(Value::as_str), Some("1000.90"));
        assert_eq!(
            parsed.get("max").and_then(Value::as_str),
            Some("21000000.0100")
        );
        assert_eq!(parsed.get("other").and_then(Value::as_i64), Some(42));
        assert_eq!(
            parsed
                .get("items")
                .and_then(Value::as_array)
                .and_then(|items| items[0].get("amt"))
                .and_then(Value::as_str),
            Some("74.0100")
        );
    }
}
