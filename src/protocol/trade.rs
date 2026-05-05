//! TAP `token-trade` payload parsing.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};
use crate::protocol::address::{normalize_address, NormalizedAddress};
use crate::protocol::tap::{decode_envelope, TapOp};
use crate::protocol::ticker::normalize_ticker;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TokenTradeAcceptItem {
    pub ticker: String,
    pub amount_raw: String,
    #[serde(default)]
    pub amount_was_number: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TokenTradePayload {
    Offer {
        ticker: String,
        amount_raw: String,
        #[serde(default)]
        amount_was_number: bool,
        valid_until: i64,
        accept: Vec<TokenTradeAcceptItem>,
    },
    Cancel {
        trade_id: String,
    },
    Fill {
        ticker: String,
        amount_raw: String,
        #[serde(default)]
        amount_was_number: bool,
        trade_id: String,
        fee_receiver: Option<NormalizedAddress>,
    },
}

impl TokenTradePayload {
    pub fn ticker(&self) -> &str {
        match self {
            Self::Offer { ticker, .. } | Self::Fill { ticker, .. } => ticker,
            Self::Cancel { .. } => "",
        }
    }
}

pub fn parse_trade(payload: &[u8]) -> Result<TokenTradePayload> {
    let (op, _body) = decode_envelope(payload)?;
    if op != TapOp::TokenTrade {
        return Err(Error::Protocol(format!(
            "expected token-trade, got {}",
            op.as_str()
        )));
    }
    let root: Value =
        serde_json::from_slice(payload).map_err(|e| Error::Protocol(format!("trade json: {e}")))?;
    let obj = root
        .as_object()
        .ok_or_else(|| Error::Protocol("token-trade payload must be object".into()))?;
    let side = value_string(
        obj.get("side")
            .ok_or_else(|| Error::Protocol("missing side".into()))?,
    );
    match side.as_deref() {
        Some("0") => {
            if let Some(trade) = obj.get("trade").and_then(Value::as_str) {
                return Ok(TokenTradePayload::Cancel {
                    trade_id: trade.trim().to_string(),
                });
            }
            let ticker = parse_ticker(obj, "tick")?;
            let amount_val = obj
                .get("amt")
                .ok_or_else(|| Error::Protocol("trade offer missing amt".into()))?;
            let amount_was_number = matches!(amount_val, Value::Number(_));
            let amount_raw = value_number_string(amount_val)?;
            let accept_val = obj
                .get("accept")
                .and_then(Value::as_array)
                .ok_or_else(|| Error::Protocol("trade offer missing accept".into()))?;
            if accept_val.is_empty() {
                return Err(Error::Protocol("trade offer accept empty".into()));
            }
            let mut accept = Vec::with_capacity(accept_val.len());
            for item in accept_val {
                let item_obj = item
                    .as_object()
                    .ok_or_else(|| Error::Protocol("trade accept item must be object".into()))?;
                let ticker = parse_ticker(item_obj, "tick")?;
                let amt = item_obj
                    .get("amt")
                    .ok_or_else(|| Error::Protocol("trade accept item missing amt".into()))?;
                accept.push(TokenTradeAcceptItem {
                    ticker,
                    amount_raw: value_number_string(amt)?,
                    amount_was_number: matches!(amt, Value::Number(_)),
                });
            }
            let valid_until = parse_valid(obj.get("valid"))?;
            Ok(TokenTradePayload::Offer {
                ticker,
                amount_raw,
                amount_was_number,
                valid_until,
                accept,
            })
        }
        Some("1") => {
            let ticker = parse_ticker(obj, "tick")?;
            let trade_id = obj
                .get("trade")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::Protocol("trade fill missing trade".into()))?
                .trim()
                .to_string();
            let amount_val = obj
                .get("amt")
                .ok_or_else(|| Error::Protocol("trade fill missing amt".into()))?;
            let amount_was_number = matches!(amount_val, Value::Number(_));
            let fee_receiver = match obj.get("fee_rcv") {
                None | Some(Value::Null) => None,
                Some(Value::String(s)) => Some(
                    normalize_address(s)
                        .ok_or_else(|| Error::Protocol("trade fee_rcv invalid".into()))?,
                ),
                Some(_) => return Err(Error::Protocol("trade fee_rcv must be string".into())),
            };
            Ok(TokenTradePayload::Fill {
                ticker,
                amount_raw: value_number_string(amount_val)?,
                amount_was_number,
                trade_id,
                fee_receiver,
            })
        }
        _ => Err(Error::Protocol("trade side must be 0 or 1".into())),
    }
}

fn parse_ticker(obj: &serde_json::Map<String, Value>, field: &str) -> Result<String> {
    let raw = obj
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Protocol(format!("missing {field}")))?;
    let ticker = normalize_ticker(raw)?;
    Ok(ticker.as_str().to_string())
}

fn parse_valid(v: Option<&Value>) -> Result<i64> {
    let Some(v) = v else {
        return Err(Error::Protocol("trade offer missing valid".into()));
    };
    let s = value_string(v).ok_or_else(|| Error::Protocol("trade valid invalid".into()))?;
    s.trim()
        .parse::<i64>()
        .map_err(|_| Error::Protocol("trade valid invalid".into()))
}

fn value_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn value_number_string(v: &Value) -> Result<String> {
    value_string(v).ok_or_else(|| Error::Protocol("field must be string or number".into()))
}
