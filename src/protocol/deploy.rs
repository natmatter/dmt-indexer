//! `dmt-deploy` payload parsing.
//!
//! Per the DMT docs, required fields are `p`, `op`, `elem`, `tick`.
//! `dt` is optional (the NAT deploy includes it; other DMT deploys may
//! omit it — default treats element bytes as a raw string).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};
use crate::protocol::tap::{decode_envelope, take_optional_string, take_string, TapOp};
use crate::protocol::ticker::{normalize_ticker, NormalizedTicker};

const ALLOWED_DT: [&str; 5] = ["h", "n", "x", "s", "b"];
const ALLOWED_DIM: [&str; 4] = ["h", "v", "d", "a"];

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeployPayload {
    pub ticker: NormalizedTicker,
    pub element_inscription_id: String,
    pub dt: Option<String>,
    pub dim: Option<String>,
    pub prj: Option<String>,
    pub id: Option<String>,
    /// `prv` (privilege authority) — we reject any deploy that carries
    /// this because we cannot validate the authority. See
    /// IMPLEMENTATION_PLAN.
    pub prv: Option<String>,
}

pub fn parse_deploy(payload: &[u8]) -> Result<DeployPayload> {
    let (op, mut body) = decode_envelope(payload)?;
    if op != TapOp::DmtDeploy {
        return Err(Error::Protocol(format!(
            "expected dmt-deploy, got {}",
            op.as_str()
        )));
    }
    let tick = take_string(&mut body, "tick")?;
    let ticker = normalize_ticker(&tick)?;
    let elem = take_string(&mut body, "elem")?;

    let dt = take_optional_string(&mut body, "dt")?;
    if let Some(ref v) = dt {
        if !ALLOWED_DT.contains(&v.as_str()) {
            return Err(Error::Protocol(format!("unknown dt: {v}")));
        }
    }
    let dim = take_optional_string(&mut body, "dim")?;
    if let Some(ref v) = dim {
        if !ALLOWED_DIM.contains(&v.as_str()) {
            return Err(Error::Protocol(format!("unknown dim: {v}")));
        }
    }
    let prj = take_optional_string(&mut body, "prj")?;
    let id = take_optional_string(&mut body, "id")?;
    let prv = match body.remove("prv") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s),
        Some(_) => return Err(Error::Protocol("prv must be string".into())),
    };
    Ok(DeployPayload {
        ticker,
        element_inscription_id: elem,
        dt,
        dim,
        prj,
        id,
        prv,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nat_shape() {
        let p = br#"{"p":"tap","op":"dmt-deploy","tick":"nat","elem":"63b5i0","dt":"n"}"#;
        let d = parse_deploy(p).unwrap();
        assert_eq!(d.ticker.as_str(), "nat");
        assert_eq!(d.dt.as_deref(), Some("n"));
    }

    #[test]
    fn accepts_missing_dt() {
        let p = br#"{"p":"tap","op":"dmt-deploy","tick":"nat","elem":"x"}"#;
        let d = parse_deploy(p).unwrap();
        assert!(d.dt.is_none());
    }

    #[test]
    fn rejects_unknown_dt() {
        let p = br#"{"p":"tap","op":"dmt-deploy","tick":"nat","elem":"x","dt":"zz"}"#;
        assert!(parse_deploy(p).is_err());
    }

    #[test]
    fn rejects_unknown_dim() {
        let p = br#"{"p":"tap","op":"dmt-deploy","tick":"nat","elem":"x","dim":"z"}"#;
        assert!(parse_deploy(p).is_err());
    }

    #[test]
    fn captures_prv() {
        let p = br#"{"p":"tap","op":"dmt-deploy","tick":"nat","elem":"x","prv":"abc"}"#;
        let d = parse_deploy(p).unwrap();
        assert_eq!(d.prv.as_deref(), Some("abc"));
    }
}
