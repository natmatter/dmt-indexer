//! `block-transferables` / `unblock-transferables` payload parsing.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::protocol::tap::{decode_envelope, TapOp};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlOp {
    Block,
    Unblock,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ControlPayload {
    pub op: ControlOp,
}

pub fn parse_control(payload: &[u8]) -> Result<ControlPayload> {
    let (op, _body) = decode_envelope(payload)?;
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
    Ok(ControlPayload { op: control_op })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_op() {
        let p = br#"{"p":"tap","op":"block-transferables"}"#;
        let c = parse_control(p).unwrap();
        assert_eq!(c.op, ControlOp::Block);
    }

    #[test]
    fn unblock_op() {
        let p = br#"{"p":"tap","op":"unblock-transferables"}"#;
        let c = parse_control(p).unwrap();
        assert_eq!(c.op, ControlOp::Unblock);
    }
}
