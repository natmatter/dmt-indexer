//! `dmt-mint` payload parsing.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::protocol::tap::{
    decode_envelope, take_numeric_string, take_optional_string, take_string, TapOp,
};
use crate::protocol::ticker::{normalize_ticker, NormalizedTicker};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MintPayload {
    pub ticker: NormalizedTicker,
    pub deployment_inscription_id: Option<String>,
    pub block_number: u64,
}

pub fn parse_mint(payload: &[u8]) -> Result<MintPayload> {
    let (op, mut body) = decode_envelope(payload)?;
    if op != TapOp::DmtMint {
        return Err(Error::Protocol(format!(
            "expected dmt-mint, got {}",
            op.as_str()
        )));
    }
    let tick = take_string(&mut body, "tick")?;
    let ticker = normalize_ticker(&tick)?;
    let dep = take_optional_string(&mut body, "dep")?;
    let blk = take_numeric_string(&mut body, "blk")?;
    let block_number = parse_block_number(&blk)?;
    Ok(MintPayload {
        ticker,
        deployment_inscription_id: dep,
        block_number,
    })
}

fn parse_block_number(raw: &str) -> Result<u64> {
    let t = raw.trim();
    if t.is_empty() || !t.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Error::Protocol(format!("blk not integer: {raw:?}")));
    }
    t.parse::<u64>()
        .map_err(|_| Error::Protocol(format!("blk out of range: {raw}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_canonical_mint() {
        let p = br#"{"p":"tap","op":"dmt-mint","dep":"abcd0000i0","tick":"NAT","blk":"885588"}"#;
        let m = parse_mint(p).unwrap();
        assert_eq!(m.ticker.as_str(), "nat");
        assert_eq!(m.block_number, 885_588);
        assert_eq!(m.deployment_inscription_id.as_deref(), Some("abcd0000i0"));
    }

    #[test]
    fn accepts_missing_dep() {
        let p = br#"{"p":"tap","op":"dmt-mint","tick":"nat","blk":"1000000"}"#;
        let m = parse_mint(p).unwrap();
        assert!(m.deployment_inscription_id.is_none());
    }

    #[test]
    fn rejects_non_integer_blk() {
        assert!(parse_mint(br#"{"p":"tap","op":"dmt-mint","tick":"nat","blk":"1.5"}"#).is_err());
    }
}
