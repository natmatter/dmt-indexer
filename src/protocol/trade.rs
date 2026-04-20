//! `token-trade` payload shell.
//!
//! TAP spec §token-trade defines a two-party atomic swap: a seller
//! inscribes an `offer` (`side: 0`) with what they give and what they
//! accept in return; a buyer inscribes an `accept` (`side: 1`) naming
//! the offer's trade id; when the accept inscription lands on the
//! offer's UTXO, balances swap atomically (optionally with a fee).
//!
//! This module parses far enough to classify the inscription and pull
//! out the primary ticker so upstream ticker-filtering keeps working.
//! The atomic swap execute-path is **not** implemented here — see
//! `sync/scan.rs::record_trade` which stores the accumulator state but
//! defers balance effects. The parse is non-fatal: syntactically valid
//! but semantically off-spec trades (unknown tickers, bad side values)
//! are admitted as accumulator entries so the move-time machinery can
//! emit a diagnostic event rather than dropping silently.
//!
//! Canonical ticker: lowercased, strip `dmt-` prefix if present.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};
use crate::protocol::tap::{decode_envelope, TapOp};
use crate::protocol::ticker::{normalize_ticker, NormalizedTicker};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TokenTradePayload {
    /// 0 = offer (seller), 1 = accept (buyer). Preserved as-received
    /// so execute-path logic can distinguish the two halves.
    pub side: u8,
    /// Primary ticker for the offer/accept — this is what upstream
    /// ticker-filters see.
    pub ticker: NormalizedTicker,
    /// Original JSON body, untouched, so the execute-path can recover
    /// every field (amt, accept array, trade id, valid height, fee_rcv)
    /// without re-parsing.
    pub raw: Value,
}

pub fn parse_trade(payload: &[u8]) -> Result<TokenTradePayload> {
    let (op, body) = decode_envelope(payload)?;
    if op != TapOp::TokenTrade {
        return Err(Error::Protocol(format!(
            "expected token-trade, got {}",
            op.as_str()
        )));
    }
    let raw = Value::Object(body.clone());
    let side = match raw.get("side") {
        Some(Value::String(s)) => s
            .trim()
            .parse::<u8>()
            .map_err(|_| Error::Protocol(format!("token-trade: bad side: {s}")))?,
        Some(Value::Number(n)) => n
            .as_u64()
            .and_then(|x| u8::try_from(x).ok())
            .ok_or_else(|| Error::Protocol("token-trade: side out of range".into()))?,
        _ => return Err(Error::Protocol("token-trade: missing side".into())),
    };
    if side > 1 {
        return Err(Error::Protocol(format!("token-trade: bad side {side}")));
    }
    let tick = raw
        .get("tick")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Protocol("token-trade: missing tick".into()))?;
    let ticker = normalize_ticker(tick)?;
    Ok(TokenTradePayload { side, ticker, raw })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_offer() {
        let p = br#"{"p":"tap","op":"token-trade","side":0,"tick":"nat","amt":"100","accept":[{"tick":"nat","amt":"50"}],"valid":"900000"}"#;
        let t = parse_trade(p).unwrap();
        assert_eq!(t.side, 0);
        assert_eq!(t.ticker.as_str(), "nat");
    }

    #[test]
    fn parses_accept() {
        let p =
            br#"{"p":"tap","op":"token-trade","side":"1","tick":"nat","trade":"abc","amt":"100"}"#;
        let t = parse_trade(p).unwrap();
        assert_eq!(t.side, 1);
    }

    #[test]
    fn rejects_bad_side() {
        let p = br#"{"p":"tap","op":"token-trade","side":5,"tick":"nat"}"#;
        assert!(parse_trade(p).is_err());
    }
}
