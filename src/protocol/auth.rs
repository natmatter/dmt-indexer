//! `token-auth` payload parsing.
//!
//! Three payload forms share the `token-auth` op, distinguished by which
//! of `auth` / `cancel` / `redeem` is present (ord-tap
//! `/tmp/ot_auth.rs`):
//!
//! - **Create**: `{ "p":"tap","op":"token-auth","auth":[<tick>,…],"sig":{…},"hash":"…","salt":"…" }`
//! - **Cancel**: `{ "p":"tap","op":"token-auth","cancel":"<auth_inscription_id>" }`
//! - **Redeem**: `{ "p":"tap","op":"token-auth","redeem":{…},"sig":{…},"hash":"…","salt":"…" }`
//!
//! The parser preserves the original subtree as a `serde_json::Value`
//! (with `preserve_order` feature enabled on `serde_json`) so callers
//! can later re-serialize exactly the bytes the signer signed. This is
//! load-bearing for ECDSA verification — see `src/crypto/ecdsa_recover.rs`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};
use crate::protocol::address::normalize_address;
use crate::protocol::amount::parse_amount;
use crate::protocol::tap::{decode_envelope, TapOp};
use crate::protocol::ticker::normalize_ticker;

/// Maximum allowed size of the optional `dta` field, in bytes.
pub const DTA_MAX_BYTES: usize = 512;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TokenAuthSig {
    pub v: String,
    pub r: String,
    pub s: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TokenAuthCreate {
    /// Whitelisted tickers. An empty array means "no whitelist"
    /// (post-916,233 whitelist check is skipped in that case).
    pub auth_tickers: Vec<String>,
    pub sig: TokenAuthSig,
    pub hash: String,
    pub salt: String,
    /// Raw `auth` array value, byte-identical to how it was parsed (for
    /// signature verification).
    pub auth_array_raw: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TokenAuthCancel {
    pub cancel_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TokenAuthRedeemItem {
    pub ticker: String,
    pub address: String,
    pub amount: u128,
    pub dta: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TokenAuthRedeem {
    pub auth_id: String,
    pub items: Vec<TokenAuthRedeemItem>,
    pub sig: TokenAuthSig,
    pub hash: String,
    pub salt: String,
    /// Raw `redeem` subtree value, byte-identical for sig verification.
    pub redeem_subtree_raw: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TokenAuthForm {
    Create(TokenAuthCreate),
    Cancel(TokenAuthCancel),
    Redeem(TokenAuthRedeem),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TokenAuthPayload {
    pub form: TokenAuthForm,
}

impl TokenAuthPayload {
    pub fn is_redeem(&self) -> bool {
        matches!(self.form, TokenAuthForm::Redeem(_))
    }

    /// Representative ticker for envelope matching. For redeem we
    /// return the first item's tick; for create we return an empty
    /// whitelist sentinel ("") so the envelope filter can decide.
    pub fn representative_ticker(&self) -> &str {
        match &self.form {
            TokenAuthForm::Create(c) => c
                .auth_tickers
                .first()
                .map(String::as_str)
                .unwrap_or("dmt-nat"),
            TokenAuthForm::Cancel(_) => "dmt-nat",
            TokenAuthForm::Redeem(r) => r
                .items
                .first()
                .map(|i| i.ticker.as_str())
                .unwrap_or("dmt-nat"),
        }
    }
}

pub fn parse_auth(payload: &[u8]) -> Result<TokenAuthPayload> {
    // We need the full raw JSON value (preserve_order) so we can keep
    // byte-exact subtrees for signature verification. Parse twice: once
    // via decode_envelope for validation of p/op + case-folding, then
    // re-parse the raw payload as a Value.
    let (op, _body) = decode_envelope(payload)?;
    if op != TapOp::TokenAuth {
        return Err(Error::Protocol(format!(
            "expected token-auth, got {}",
            op.as_str()
        )));
    }
    let root: Value = serde_json::from_slice(payload)
        .map_err(|e| Error::Protocol(format!("token-auth json: {e}")))?;
    let obj = root
        .as_object()
        .ok_or_else(|| Error::Protocol("token-auth payload must be object".into()))?;

    // Dispatch on form. Cancel > Redeem > Create priority (ord-tap
    // index_token_auth_created checks `cancel` first, then `redeem`,
    // then falls through to create).
    if let Some(cancel_val) = obj.get("cancel") {
        let cancel_id = cancel_val
            .as_str()
            .ok_or_else(|| Error::Protocol("cancel must be string".into()))?
            .to_string();
        return Ok(TokenAuthPayload {
            form: TokenAuthForm::Cancel(TokenAuthCancel { cancel_id }),
        });
    }

    if let Some(redeem_val) = obj.get("redeem") {
        return parse_redeem(obj, redeem_val).map(|form| TokenAuthPayload {
            form: TokenAuthForm::Redeem(form),
        });
    }

    // Create form.
    parse_create(obj).map(|form| TokenAuthPayload {
        form: TokenAuthForm::Create(form),
    })
}

fn parse_sig(v: &Value) -> Result<TokenAuthSig> {
    let obj = v
        .as_object()
        .ok_or_else(|| Error::Protocol("sig must be object".into()))?;
    fn s(obj: &serde_json::Map<String, Value>, field: &str) -> Result<String> {
        match obj.get(field) {
            Some(Value::String(s)) => Ok(s.clone()),
            Some(Value::Number(n)) => Ok(n.to_string()),
            _ => Err(Error::Protocol(format!(
                "sig.{field} missing or wrong type"
            ))),
        }
    }
    Ok(TokenAuthSig {
        v: s(obj, "v")?,
        r: s(obj, "r")?,
        s: s(obj, "s")?,
    })
}

fn parse_create(obj: &serde_json::Map<String, Value>) -> Result<TokenAuthCreate> {
    let sig_val = obj
        .get("sig")
        .ok_or_else(|| Error::Protocol("create: missing sig".into()))?;
    let sig = parse_sig(sig_val)?;
    let hash = obj
        .get("hash")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Protocol("create: missing hash".into()))?
        .to_string();
    let salt = obj
        .get("salt")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Protocol("create: missing salt".into()))?
        .to_string();
    let auth_val = obj
        .get("auth")
        .ok_or_else(|| Error::Protocol("create: missing auth array".into()))?;
    let auth_arr = auth_val
        .as_array()
        .ok_or_else(|| Error::Protocol("create: auth must be array".into()))?;
    let mut auth_tickers = Vec::with_capacity(auth_arr.len());
    for tv in auth_arr {
        let t = tv
            .as_str()
            .ok_or_else(|| Error::Protocol("create: auth[] must be string".into()))?;
        // Validate ticker shape (lowercases + rejects whitespace) without
        // deciding indexing — that's the scan.rs pipeline's job.
        let _ = normalize_ticker(t)?;
        auth_tickers.push(t.to_string());
    }
    Ok(TokenAuthCreate {
        auth_tickers,
        sig,
        hash,
        salt,
        auth_array_raw: auth_val.clone(),
    })
}

fn parse_redeem(
    obj: &serde_json::Map<String, Value>,
    redeem_val: &Value,
) -> Result<TokenAuthRedeem> {
    let sig_val = obj
        .get("sig")
        .ok_or_else(|| Error::Protocol("redeem: missing sig".into()))?;
    let sig = parse_sig(sig_val)?;
    let hash = obj
        .get("hash")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Protocol("redeem: missing hash".into()))?
        .to_string();
    let salt = obj
        .get("salt")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Protocol("redeem: missing salt".into()))?
        .to_string();
    let redeem_obj = redeem_val
        .as_object()
        .ok_or_else(|| Error::Protocol("redeem: must be object".into()))?;
    let auth_id = redeem_obj
        .get("auth")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Protocol("redeem: missing auth".into()))?
        .to_string();
    // ord-tap `/tmp/ot_auth.rs:45`: data must be present (can be empty string).
    if redeem_obj.get("data").is_none() {
        return Err(Error::Protocol("redeem: missing data".into()));
    }
    let items_arr = redeem_obj
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| Error::Protocol("redeem: missing items".into()))?;
    if items_arr.is_empty() {
        return Err(Error::Protocol("redeem: items empty".into()));
    }
    let mut items = Vec::with_capacity(items_arr.len());
    for raw in items_arr {
        items.push(parse_redeem_item(raw)?);
    }
    Ok(TokenAuthRedeem {
        auth_id,
        items,
        sig,
        hash,
        salt,
        redeem_subtree_raw: redeem_val.clone(),
    })
}

fn parse_redeem_item(v: &Value) -> Result<TokenAuthRedeemItem> {
    let obj = v
        .as_object()
        .ok_or_else(|| Error::Protocol("redeem item must be object".into()))?;
    let tick_str = obj
        .get("tick")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Protocol("redeem item.tick missing".into()))?;
    let _ = normalize_ticker(tick_str)?;
    let addr_str = obj
        .get("address")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Protocol("redeem item.address missing".into()))?;
    let norm_addr = normalize_address(addr_str)
        .ok_or_else(|| Error::Protocol("redeem item.address invalid".into()))?;
    let amt_str = match obj.get("amt") {
        None => return Err(Error::Protocol("redeem item.amt missing".into())),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        Some(_) => return Err(Error::Protocol("redeem item.amt bad type".into())),
    };
    let amount = parse_amount(&amt_str)?;
    let dta = match obj.get("dta") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => {
            if s.len() > DTA_MAX_BYTES {
                return Err(Error::Protocol("redeem item.dta too long".into()));
            }
            Some(s.clone())
        }
        Some(_) => return Err(Error::Protocol("redeem item.dta bad type".into())),
    };
    Ok(TokenAuthRedeemItem {
        ticker: tick_str.to_ascii_lowercase(),
        address: norm_addr.into_inner(),
        amount,
        dta,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_create() {
        let p = br#"{"p":"tap","op":"token-auth","auth":["dmt-nat"],"sig":{"v":"1","r":"1","s":"2"},"hash":"aa","salt":"sss"}"#;
        let out = parse_auth(p).unwrap();
        match out.form {
            TokenAuthForm::Create(c) => {
                assert_eq!(c.auth_tickers, vec!["dmt-nat".to_string()]);
                assert_eq!(c.hash, "aa");
                assert_eq!(c.salt, "sss");
            }
            _ => panic!("expected create"),
        }
    }

    #[test]
    fn parses_cancel() {
        let p = br#"{"p":"tap","op":"token-auth","cancel":"abc123i0"}"#;
        let out = parse_auth(p).unwrap();
        match out.form {
            TokenAuthForm::Cancel(c) => assert_eq!(c.cancel_id, "abc123i0"),
            _ => panic!("expected cancel"),
        }
    }

    #[test]
    fn parses_redeem() {
        let p = br#"{
            "p":"tap","op":"token-auth",
            "redeem":{"auth":"abc123i0","data":"","items":[
                {"tick":"dmt-nat","amt":"100","address":"bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4"}
            ]},
            "sig":{"v":"1","r":"1","s":"2"},"hash":"aa","salt":"sss"
        }"#;
        let out = parse_auth(p).unwrap();
        match out.form {
            TokenAuthForm::Redeem(r) => {
                assert_eq!(r.auth_id, "abc123i0");
                assert_eq!(r.items.len(), 1);
                assert_eq!(r.items[0].amount, 100);
            }
            _ => panic!("expected redeem"),
        }
    }

    #[test]
    fn redeem_requires_data_field() {
        let p = br#"{"p":"tap","op":"token-auth","redeem":{"auth":"a","items":[{"tick":"dmt-nat","amt":"1","address":"bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4"}]},"sig":{"v":"1","r":"1","s":"2"},"hash":"","salt":""}"#;
        assert!(parse_auth(p).is_err());
    }

    #[test]
    fn wrong_op_rejected() {
        let p = br#"{"p":"tap","op":"token-transfer","tick":"nat","amt":"1"}"#;
        assert!(parse_auth(p).is_err());
    }

    /// Regression: preserve_order must keep field order exactly as the
    /// bytes were parsed. This test constructs a Value from a known JSON
    /// string and asserts that serde_json::to_string returns the same
    /// byte sequence. If preserve_order is disabled, BTreeMap-based
    /// ordering would alphabetize and break ECDSA sig verification.
    #[test]
    fn serde_preserve_order_round_trip() {
        let input = r#"{"zzz":1,"aaa":2,"mmm":3}"#;
        let v: Value = serde_json::from_str(input).unwrap();
        let out = serde_json::to_string(&v).unwrap();
        assert_eq!(out, input);
    }
}
