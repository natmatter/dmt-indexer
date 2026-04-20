//! `token-send` payload parsing.
//!
//! Per TAP spec (§ token-send): a single inscription declares many
//! recipient credits at once. The inscribe is atomic at the *syntactic*
//! level (every item must be well-formed) but executes per-item at
//! *semantic* level (insufficient balance on one item does NOT fail
//! the others). Execution is deferred until the inscription is tapped
//! back to its creator ("self-tap") — see `sync/scan.rs::execute_send`.
//!
//! Canonical ticker rule: both `dmt-<name>` and `<name>` ticker forms
//! map to the same deployment; we canonicalize per-item downstream.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::protocol::address::{normalize_address, NormalizedAddress};
use crate::protocol::amount::parse_amount;
use crate::protocol::tap::{decode_envelope, TapOp};
use crate::protocol::ticker::{normalize_ticker, NormalizedTicker};
use serde_json::Value;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TokenSendItem {
    pub ticker: NormalizedTicker,
    pub address: NormalizedAddress,
    pub amount: u128,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TokenSendPayload {
    pub items: Vec<TokenSendItem>,
}

impl TokenSendPayload {
    /// Return the first item's ticker. token-send allows multiple
    /// tickers per inscription, but the upstream pipeline filters by a
    /// single ticker at envelope scope — we expose the first to keep
    /// the existing routing surface unchanged, and the per-item ticker
    /// is used at settle time.
    pub fn first_ticker(&self) -> Option<&NormalizedTicker> {
        self.items.first().map(|i| &i.ticker)
    }
}

pub fn parse_send(payload: &[u8]) -> Result<TokenSendPayload> {
    let (op, mut body) = decode_envelope(payload)?;
    if op != TapOp::TokenSend {
        return Err(Error::Protocol(format!(
            "expected token-send, got {}",
            op.as_str()
        )));
    }
    let items_raw = body
        .remove("items")
        .ok_or_else(|| Error::Protocol("token-send: missing items".into()))?;
    let items_arr = match items_raw {
        Value::Array(a) => a,
        _ => return Err(Error::Protocol("token-send: items must be array".into())),
    };
    if items_arr.is_empty() {
        return Err(Error::Protocol("token-send: items array is empty".into()));
    }
    let mut items = Vec::with_capacity(items_arr.len());
    for raw in items_arr {
        let mut obj = match raw {
            Value::Object(m) => m,
            _ => return Err(Error::Protocol("token-send: item must be object".into())),
        };
        let tick = match obj.remove("tick") {
            Some(Value::String(s)) => s,
            _ => {
                return Err(Error::Protocol(
                    "token-send item: tick must be string".into(),
                ))
            }
        };
        let ticker = normalize_ticker(&tick)?;
        let addr_raw = match obj.remove("address") {
            Some(Value::String(s)) => s,
            _ => {
                return Err(Error::Protocol(
                    "token-send item: address must be string".into(),
                ))
            }
        };
        let address = normalize_address(&addr_raw).ok_or_else(|| {
            Error::Protocol(format!("token-send item: invalid address: {addr_raw}"))
        })?;
        let amt_str = match obj.remove("amt") {
            Some(Value::String(s)) => s,
            Some(Value::Number(n)) => n.to_string(),
            _ => {
                return Err(Error::Protocol(
                    "token-send item: amt must be numeric string".into(),
                ))
            }
        };
        let amount = parse_amount(&amt_str)?;
        items.push(TokenSendItem {
            ticker,
            address,
            amount,
        });
    }
    Ok(TokenSendPayload { items })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_item() {
        // normalize_ticker lowercases but does NOT strip the
        // `dmt-` prefix — that's done by canonical_ticker at the
        // scan-layer. Here we just check the parsed form.
        let p = br#"{"p":"tap","op":"token-send","items":[{"tick":"DMT-NAT","address":"bc1qxz9nmfg3czfpm6ml025xfsuwx7sa8nlslpwa4f","amt":"1000"}]}"#;
        let s = parse_send(p).unwrap();
        assert_eq!(s.items.len(), 1);
        assert_eq!(s.items[0].ticker.as_str(), "dmt-nat");
        assert_eq!(s.items[0].amount, 1000);
    }

    #[test]
    fn multi_item() {
        let p = br#"{"p":"tap","op":"token-send","items":[
          {"tick":"nat","address":"bc1qxz9nmfg3czfpm6ml025xfsuwx7sa8nlslpwa4f","amt":"100"},
          {"tick":"dmt-nat","address":"bc1qxz9nmfg3czfpm6ml025xfsuwx7sa8nlslpwa4f","amt":"200"}
        ]}"#;
        let s = parse_send(p).unwrap();
        assert_eq!(s.items.len(), 2);
        assert_eq!(s.items[0].amount, 100);
        assert_eq!(s.items[1].amount, 200);
    }

    #[test]
    fn rejects_empty_items() {
        let p = br#"{"p":"tap","op":"token-send","items":[]}"#;
        assert!(parse_send(p).is_err());
    }

    #[test]
    fn rejects_missing_items() {
        let p = br#"{"p":"tap","op":"token-send"}"#;
        assert!(parse_send(p).is_err());
    }

    #[test]
    fn rejects_bad_address() {
        let p = br#"{"p":"tap","op":"token-send","items":[{"tick":"nat","address":"","amt":"1"}]}"#;
        assert!(parse_send(p).is_err());
    }

    #[test]
    fn rejects_negative_amt() {
        let p = br#"{"p":"tap","op":"token-send","items":[{"tick":"nat","address":"bc1qxz9nmfg3czfpm6ml025xfsuwx7sa8nlslpwa4f","amt":"-1"}]}"#;
        assert!(parse_send(p).is_err());
    }
}
