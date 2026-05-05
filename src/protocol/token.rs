//! Standard TAP `token-deploy` / `token-mint` payload parsing.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};
use crate::protocol::amount::resolve_number_string;
use crate::protocol::tap::{decode_envelope, take_string, TapOp};
use crate::protocol::ticker::{normalize_ticker, NormalizedTicker};

pub const DTA_MAX_BYTES: usize = 512;
pub const TAP_TOKEN_MAX_DECIMALS: u32 = 18;
pub const MAX_DEC_U64_STR: &str = "18446744073709551615";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TokenDeployPayload {
    pub ticker: NormalizedTicker,
    pub max_supply: u128,
    pub mint_limit: u128,
    pub decimals: u32,
    pub prv: Option<String>,
    pub dta: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TokenMintPayload {
    pub ticker: NormalizedTicker,
    pub amount_raw: String,
    #[serde(default)]
    pub amount_was_number: bool,
    pub dta: Option<String>,
    pub prv: Option<Value>,
}

pub fn parse_token_deploy(payload: &[u8]) -> Result<TokenDeployPayload> {
    let (op, mut body) = decode_envelope(payload)?;
    if op != TapOp::TokenDeploy {
        return Err(Error::Protocol(format!(
            "expected token-deploy, got {}",
            op.as_str()
        )));
    }
    let tick = take_string(&mut body, "tick")?;
    let ticker = normalize_ticker(&tick)?;
    let tick_lower = ticker.as_str();
    if tick_lower.starts_with('-') || tick_lower.starts_with("dmt-") {
        return Err(Error::Protocol("token-deploy ticker reserved".into()));
    }

    let decimals = parse_decimals(body.get("dec"))?;
    let max_raw = value_number_string(
        body.get("max")
            .ok_or_else(|| Error::Protocol("missing field: max".into()))?,
    )?;
    let max_supply = parse_scaled_u128(&max_raw, decimals)?;
    if max_supply == 0 {
        return Err(Error::Protocol("token-deploy max is zero".into()));
    }
    let cap = parse_scaled_u128(MAX_DEC_U64_STR, decimals)?;
    if max_supply > cap {
        return Err(Error::Protocol("token-deploy max above cap".into()));
    }

    let mint_limit = match body.get("lim") {
        None | Some(Value::Null) => 0,
        Some(v) => {
            let raw = value_number_string(v)?;
            let lim = parse_scaled_u128(&raw, decimals)?;
            if lim == 0 || lim > cap {
                return Err(Error::Protocol("token-deploy lim invalid".into()));
            }
            lim
        }
    };
    let prv = match body.get("prv") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => return Err(Error::Protocol("token-deploy prv must be string".into())),
    };
    let dta = match body.get("dta") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => {
            if s.len() > DTA_MAX_BYTES {
                return Err(Error::Protocol("token-deploy dta too long".into()));
            }
            Some(s.clone())
        }
        Some(_) => return Err(Error::Protocol("token-deploy dta must be string".into())),
    };

    Ok(TokenDeployPayload {
        ticker,
        max_supply,
        mint_limit,
        decimals,
        prv,
        dta,
    })
}

pub fn parse_token_mint(payload: &[u8]) -> Result<TokenMintPayload> {
    let (op, mut body) = decode_envelope(payload)?;
    if op != TapOp::TokenMint {
        return Err(Error::Protocol(format!(
            "expected token-mint, got {}",
            op.as_str()
        )));
    }
    let tick = take_string(&mut body, "tick")?;
    let ticker = normalize_ticker(&tick)?;
    let tick_lower = ticker.as_str();
    if tick_lower.starts_with('-') || tick_lower.starts_with("dmt-") {
        return Err(Error::Protocol("token-mint ticker reserved".into()));
    }
    let amt_val = body
        .get("amt")
        .ok_or_else(|| Error::Protocol("missing field: amt".into()))?;
    let amount_was_number = matches!(amt_val, Value::Number(_));
    let amount_raw = value_number_string(amt_val)?;
    let dta = match body.get("dta") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => {
            if s.len() > DTA_MAX_BYTES {
                return Err(Error::Protocol("token-mint dta too long".into()));
            }
            Some(s.clone())
        }
        Some(_) => return Err(Error::Protocol("token-mint dta must be string".into())),
    };
    Ok(TokenMintPayload {
        ticker,
        amount_raw,
        amount_was_number,
        dta,
        prv: body.get("prv").cloned(),
    })
}

pub fn parse_scaled_u128(raw: &str, decimals: u32) -> Result<u128> {
    let s = resolve_number_string(raw, decimals)
        .ok_or_else(|| Error::Protocol(format!("invalid number: {raw:?}")))?;
    s.parse::<u128>()
        .map_err(|_| Error::Protocol(format!("number overflow: {raw}")))
}

fn parse_decimals(v: Option<&Value>) -> Result<u32> {
    let Some(v) = v else {
        return Ok(TAP_TOKEN_MAX_DECIMALS);
    };
    let dec_str = value_number_string(v)?;
    let Ok(parsed) = dec_str.parse::<i64>() else {
        return Ok(TAP_TOKEN_MAX_DECIMALS);
    };
    if !(0..TAP_TOKEN_MAX_DECIMALS as i64).contains(&parsed) {
        return Ok(TAP_TOKEN_MAX_DECIMALS);
    }
    if resolve_number_string(&dec_str, 0).is_none() {
        return Err(Error::Protocol("token-deploy dec invalid".into()));
    }
    Ok(parsed as u32)
}

fn value_number_string(v: &Value) -> Result<String> {
    match v {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        _ => Err(Error::Protocol("field must be string or number".into())),
    }
}
