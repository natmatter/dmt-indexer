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

/// Match ord-tap's `js_parse_int_repr`: take the leading run of ASCII
/// digits as the integer, ignore anything after. Empty or leading
/// non-digit fails. The post-885,588 strict round-trip check doesn't
/// apply to NAT — all NAT mints are pre-activation — so we don't
/// re-emit the original string representation at this layer.
fn parse_block_number(raw: &str) -> Result<u64> {
    // ord-tap is NOT whitespace-tolerant here — mirror that.
    let digit_prefix: String = raw.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digit_prefix.is_empty() {
        return Err(Error::Protocol(format!("blk not integer: {raw:?}")));
    }
    digit_prefix
        .parse::<u64>()
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

    // ord-tap semantics: `blk:"1.5"` parses to `1` via JS parseInt.
    // NAT mints are always pre-DmtParseintActivation (885,588 = also
    // the NAT rewards activation), so the strict-roundtrip check does
    // not apply. Assert the parse admits with the truncated value.
    #[test]
    fn blk_parses_with_jsparseint_semantics() {
        let m = parse_mint(br#"{"p":"tap","op":"dmt-mint","tick":"nat","blk":"1.5"}"#).unwrap();
        assert_eq!(m.block_number, 1);
    }

    #[test]
    fn rejects_non_digit_leading_blk() {
        assert!(parse_mint(br#"{"p":"tap","op":"dmt-mint","tick":"nat","blk":"abc"}"#).is_err());
    }
}
