//! `token-send` payload parsing.
//!
//! Payload shape (ord-tap `/tmp/ot_send.rs`):
//! ```json
//! { "p":"tap","op":"token-send",
//!   "items":[ { "tick":"<tick>","amt":"<num>","address":"<bc1…>","dta":"<?>" }, … ] }
//! ```
//!
//! Each item carries an independent `(tick, amt, address)` triple and an
//! optional opaque `dta` field (≤512 bytes). Items are validated at parse
//! time per ord-tap's creation-time rules:
//!  - `tick` must normalize via [`normalize_ticker`]
//!  - `address` must normalize via [`normalize_address`]
//!  - `amt` must parse via [`parse_amount`]
//!  - `dta` (if present) must be ≤512 bytes
//!
//! We keep items as raw normalized strings (not filtered to a specific
//! deployed ticker) because the per-item canonical-ticker check against
//! the indexed asset happens later in the scan pipeline.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::protocol::address::{normalize_address, NormalizedAddress};
use crate::protocol::amount::parse_amount;
use crate::protocol::tap::{decode_envelope, TapOp};
use crate::protocol::ticker::{normalize_ticker, NormalizedTicker};

/// Maximum allowed size of the optional `dta` field, in bytes.
/// Mirrors ord-tap's explicit guard at `/tmp/ot_send.rs:45`.
pub const DTA_MAX_BYTES: usize = 512;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TokenSendItem {
    pub ticker: NormalizedTicker,
    pub recipient: NormalizedAddress,
    pub amount: u128,
    /// Opaque metadata — preserved byte-identical for audit trails; not
    /// semantically interpreted by the indexer.
    pub dta: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TokenSendPayload {
    pub items: Vec<TokenSendItem>,
}

pub fn parse_send(payload: &[u8]) -> Result<TokenSendPayload> {
    let (op, mut body) = decode_envelope(payload)?;
    if op != TapOp::TokenSend {
        return Err(Error::Protocol(format!(
            "expected token-send, got {}",
            op.as_str()
        )));
    }
    let items_val = body
        .remove("items")
        .ok_or_else(|| Error::Protocol("missing field: items".into()))?;
    let items_arr = match items_val {
        serde_json::Value::Array(a) => a,
        _ => return Err(Error::Protocol("items must be array".into())),
    };
    if items_arr.is_empty() {
        return Err(Error::Protocol("token-send items empty".into()));
    }
    let mut items = Vec::with_capacity(items_arr.len());
    for raw in items_arr {
        items.push(parse_item(raw)?);
    }
    Ok(TokenSendPayload { items })
}

fn parse_item(raw: serde_json::Value) -> Result<TokenSendItem> {
    let obj = match raw {
        serde_json::Value::Object(m) => m,
        _ => return Err(Error::Protocol("token-send item must be object".into())),
    };
    let tick_str = obj
        .get("tick")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Protocol("item.tick missing".into()))?;
    let ticker = normalize_ticker(tick_str)?;
    let addr_str = obj
        .get("address")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Protocol("item.address missing".into()))?;
    let recipient = normalize_address(addr_str)
        .ok_or_else(|| Error::Protocol("item.address invalid".into()))?;
    // `amt` is allowed as string OR number (ord-tap ValueStringifyActivation
    // rejects numeric forms at/after block 885,588; we defer that gate to
    // the caller that has the inscribed_height).
    let amt_str = match obj.get("amt") {
        None => return Err(Error::Protocol("item.amt missing".into())),
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Number(n)) => n.to_string(),
        Some(_) => return Err(Error::Protocol("item.amt must be string or number".into())),
    };
    let amount = parse_amount(&amt_str)?;
    let dta = match obj.get("dta") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(s)) => {
            if s.len() > DTA_MAX_BYTES {
                return Err(Error::Protocol("item.dta exceeds 512 bytes".into()));
            }
            Some(s.clone())
        }
        Some(_) => return Err(Error::Protocol("item.dta must be string".into())),
    };
    Ok(TokenSendItem {
        ticker,
        recipient,
        amount,
        dta,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_item() {
        let p = br#"{"p":"tap","op":"token-send","items":[{"tick":"dmt-nat","amt":"1000","address":"bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4"}]}"#;
        let out = parse_send(p).unwrap();
        assert_eq!(out.items.len(), 1);
        assert_eq!(out.items[0].ticker.as_str(), "dmt-nat");
        assert_eq!(out.items[0].amount, 1000);
    }

    #[test]
    fn multi_item() {
        let p = br#"{"p":"tap","op":"token-send","items":[
            {"tick":"dmt-nat","amt":"100","address":"bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4"},
            {"tick":"dmt-nat","amt":"200","address":"bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4","dta":"hello"}
        ]}"#;
        let out = parse_send(p).unwrap();
        assert_eq!(out.items.len(), 2);
        assert_eq!(out.items[0].amount, 100);
        assert_eq!(out.items[1].amount, 200);
        assert_eq!(out.items[1].dta.as_deref(), Some("hello"));
    }

    #[test]
    fn empty_items_rejected() {
        let p = br#"{"p":"tap","op":"token-send","items":[]}"#;
        assert!(parse_send(p).is_err());
    }

    #[test]
    fn bad_address_rejected() {
        let p = br#"{"p":"tap","op":"token-send","items":[{"tick":"dmt-nat","amt":"1","address":"  "}]}"#;
        assert!(parse_send(p).is_err());
    }

    #[test]
    fn negative_amount_rejected() {
        let p = br#"{"p":"tap","op":"token-send","items":[{"tick":"dmt-nat","amt":"-5","address":"bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4"}]}"#;
        assert!(parse_send(p).is_err());
    }

    #[test]
    fn dta_too_long() {
        let big = "x".repeat(513);
        let p = format!(
            r#"{{"p":"tap","op":"token-send","items":[{{"tick":"dmt-nat","amt":"1","address":"bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4","dta":"{}"}}]}}"#,
            big
        );
        assert!(parse_send(p.as_bytes()).is_err());
    }

    #[test]
    fn missing_items_rejected() {
        let p = br#"{"p":"tap","op":"token-send"}"#;
        assert!(parse_send(p).is_err());
    }
}
