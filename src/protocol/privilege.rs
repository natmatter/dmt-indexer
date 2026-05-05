//! `privilege-auth` payload parsing.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};
use crate::protocol::auth::TokenAuthSig;
use crate::protocol::tap::{decode_envelope, TapOp};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrivilegeAuthCreate {
    pub auth_json: Value,
    pub sig: TokenAuthSig,
    pub hash: String,
    pub salt: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrivilegeAuthCancel {
    pub cancel_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PrivilegeAuthForm {
    Create(PrivilegeAuthCreate),
    Cancel(PrivilegeAuthCancel),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrivilegeAuthPayload {
    pub form: PrivilegeAuthForm,
}

pub fn parse_privilege_auth(payload: &[u8]) -> Result<PrivilegeAuthPayload> {
    let (op, _) = decode_envelope(payload)?;
    if op != TapOp::PrivilegeAuth {
        return Err(Error::Protocol(format!(
            "expected privilege-auth, got {}",
            op.as_str()
        )));
    }
    let root: Value = serde_json::from_slice(payload)
        .map_err(|e| Error::Protocol(format!("privilege-auth json: {e}")))?;
    let obj = root
        .as_object()
        .ok_or_else(|| Error::Protocol("privilege-auth payload must be object".into()))?;

    if let Some(cancel_val) = obj.get("cancel") {
        let cancel_id = cancel_val
            .as_str()
            .ok_or_else(|| Error::Protocol("privilege-auth cancel must be string".into()))?
            .to_string();
        return Ok(PrivilegeAuthPayload {
            form: PrivilegeAuthForm::Cancel(PrivilegeAuthCancel { cancel_id }),
        });
    }

    let auth_json = obj
        .get("auth")
        .filter(|v| v.is_object())
        .cloned()
        .ok_or_else(|| Error::Protocol("privilege-auth missing auth object".into()))?;
    if auth_json
        .get("name")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty() && s.chars().count() <= 512)
        .is_none()
    {
        return Err(Error::Protocol("privilege-auth missing auth.name".into()));
    }
    let sig_val = obj
        .get("sig")
        .ok_or_else(|| Error::Protocol("privilege-auth missing sig".into()))?;
    let sig = parse_sig(sig_val)?;
    let hash = obj
        .get("hash")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Protocol("privilege-auth missing hash".into()))?
        .to_string();
    let salt = obj
        .get("salt")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Protocol("privilege-auth missing salt".into()))?
        .to_string();

    Ok(PrivilegeAuthPayload {
        form: PrivilegeAuthForm::Create(PrivilegeAuthCreate {
            auth_json,
            sig,
            hash,
            salt,
        }),
    })
}

fn parse_sig(v: &Value) -> Result<TokenAuthSig> {
    let obj = v
        .as_object()
        .ok_or_else(|| Error::Protocol("sig must be object".into()))?;
    fn s(obj: &serde_json::Map<String, Value>, field: &str) -> Result<String> {
        match obj.get(field) {
            Some(Value::String(s)) => Ok(s.clone()),
            Some(Value::Number(n)) => Ok(n.to_string()),
            _ => Err(Error::Protocol(format!(
                "sig.{field} missing or wrong type"
            ))),
        }
    }
    Ok(TokenAuthSig {
        v: s(obj, "v")?,
        r: s(obj, "r")?,
        s: s(obj, "s")?,
    })
}
