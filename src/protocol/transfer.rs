//! `token-transfer` payload parsing.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::protocol::amount::parse_amount;
use crate::protocol::tap::{decode_envelope, take_numeric_string, take_string, TapOp};
use crate::protocol::ticker::{normalize_ticker, NormalizedTicker};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TokenTransferPayload {
    pub ticker: NormalizedTicker,
    pub amount: u128,
}

pub fn parse_transfer(payload: &[u8]) -> Result<TokenTransferPayload> {
    let (op, mut body) = decode_envelope(payload)?;
    if op != TapOp::TokenTransfer {
        return Err(Error::Protocol(format!(
            "expected token-transfer, got {}",
            op.as_str()
        )));
    }
    let tick = take_string(&mut body, "tick")?;
    let ticker = normalize_ticker(&tick)?;
    let amt = take_numeric_string(&mut body, "amt")?;
    let amount = parse_amount(&amt)?;
    Ok(TokenTransferPayload { ticker, amount })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_transfer() {
        let p = br#"{"p":"tap","op":"token-transfer","tick":"nat","amt":"1000"}"#;
        let t = parse_transfer(p).unwrap();
        assert_eq!(t.ticker.as_str(), "nat");
        assert_eq!(t.amount, 1000);
    }

    #[test]
    fn rejects_negative() {
        let p = br#"{"p":"tap","op":"token-transfer","tick":"nat","amt":"-5"}"#;
        assert!(parse_transfer(p).is_err());
    }
}
