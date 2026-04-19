//! `block-transferables` / `unblock-transferables` payload parsing.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::protocol::tap::{decode_envelope, take_string, TapOp};
use crate::protocol::ticker::{normalize_ticker, NormalizedTicker};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlOp {
    Block,
    Unblock,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ControlPayload {
    pub op: ControlOp,
    pub ticker: NormalizedTicker,
}

pub fn parse_control(payload: &[u8]) -> Result<ControlPayload> {
    let (op, mut body) = decode_envelope(payload)?;
    let control_op = match op {
        TapOp::BlockTransferables => ControlOp::Block,
        TapOp::UnblockTransferables => ControlOp::Unblock,
        other => {
            return Err(Error::Protocol(format!(
                "expected control op, got {}",
                other.as_str()
            )))
        }
    };
    let tick = take_string(&mut body, "tick")?;
    let ticker = normalize_ticker(&tick)?;
    Ok(ControlPayload {
        op: control_op,
        ticker,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_op() {
        let p = br#"{"p":"tap","op":"block-transferables","tick":"nat"}"#;
        let c = parse_control(p).unwrap();
        assert_eq!(c.op, ControlOp::Block);
    }

    #[test]
    fn unblock_op() {
        let p = br#"{"p":"tap","op":"unblock-transferables","tick":"NAT"}"#;
        let c = parse_control(p).unwrap();
        assert_eq!(c.op, ControlOp::Unblock);
        assert_eq!(c.ticker.as_str(), "nat");
    }
}
