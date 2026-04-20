//! `token-auth` payload shell.
//!
//! TAP spec §token-auth defines two modes. **Create-auth**: tap a
//! signed `{auth: [tickers]}` inscription to register an authority
//! that can sign future redeems. **Redeem**: inscribe a signed item
//! batch referencing a prior auth id; on settle, each item moves
//! balance like a `token-send` from the auth-registered address.
//!
//! Full execute-path support requires secp256k1 ECDSA verification and
//! the exact SHA-256 message construction ord-tap uses. That's heavy
//! lift; for now we parse far enough to classify and track, and defer
//! balance effects. Consequence: wallets funded via `token-auth` redeem
//! will still be under-credited in our view. NAT-side auth volume is
//! small but nonzero.
//!
//! This parser is intentionally lenient — ord-tap's behavior is "drop
//! on any field error", but for diagnostic visibility we admit any
//! well-formed `{p:"tap", op:"token-auth"}` JSON and let downstream
//! emit an `inscribe_admitted` event that marks execution as deferred.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};
use crate::protocol::tap::{decode_envelope, TapOp};
use crate::protocol::ticker::{normalize_ticker, NormalizedTicker};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TokenAuthPayload {
    /// Best-effort representative ticker. If the payload is a redeem,
    /// this is the first item's ticker. If it's a create-auth, it's
    /// the first ticker in the auth list. If neither is present
    /// (malformed but valid JSON), we default to the indexed ticker
    /// so upstream filtering doesn't drop silently.
    pub ticker: NormalizedTicker,
    /// Original JSON body for future execute-path use.
    pub raw: Value,
}

pub fn parse_auth(payload: &[u8]) -> Result<TokenAuthPayload> {
    let (op, body) = decode_envelope(payload)?;
    if op != TapOp::TokenAuth {
        return Err(Error::Protocol(format!(
            "expected token-auth, got {}",
            op.as_str()
        )));
    }
    let raw = Value::Object(body.clone());

    // Try: redeem.items[0].tick → auth[0] → "nat"
    let tick_str = raw
        .get("redeem")
        .and_then(|r| r.get("items"))
        .and_then(|a| a.as_array())
        .and_then(|a| a.first())
        .and_then(|it| it.get("tick"))
        .and_then(|v| v.as_str())
        .or_else(|| {
            raw.get("auth")
                .and_then(|a| a.as_array())
                .and_then(|a| a.first())
                .and_then(|v| v.as_str())
        })
        .unwrap_or("nat");
    let ticker = normalize_ticker(tick_str)?;
    Ok(TokenAuthPayload { ticker, raw })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_create_auth() {
        let p = br#"{"p":"tap","op":"token-auth","auth":["nat"],"sig":{},"hash":"x","salt":"y"}"#;
        let t = parse_auth(p).unwrap();
        assert_eq!(t.ticker.as_str(), "nat");
    }

    #[test]
    fn parses_redeem() {
        let p = br#"{"p":"tap","op":"token-auth","sig":{},"hash":"x","salt":"y","redeem":{"auth":"id","data":"d","items":[{"tick":"dmt-nat","address":"bc1q","amt":"1"}]}}"#;
        let t = parse_auth(p).unwrap();
        assert_eq!(t.ticker.as_str(), "dmt-nat");
    }

    #[test]
    fn parses_minimal_defaults_to_nat() {
        let p = br#"{"p":"tap","op":"token-auth"}"#;
        let t = parse_auth(p).unwrap();
        assert_eq!(t.ticker.as_str(), "nat");
    }
}
