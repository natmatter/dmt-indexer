//! Inscription envelope → TAP payload routing.
//!
//! Takes an [`inscription::Envelope`] and either returns a fully-parsed
//! TAP operation or `None` (not TAP / not DMT-relevant). This is where
//! we apply brotli decoding if `content_encoding == "br"`, and where
//! we cheap-filter on ticker to avoid running op-specific parsers on
//! every non-DMT TAP inscription.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::inscription::envelope::Envelope;
use crate::protocol::auth::{parse_auth, TokenAuthPayload};
use crate::protocol::control::{parse_control, ControlPayload};
use crate::protocol::deploy::{parse_deploy, DeployPayload};
use crate::protocol::mint::{parse_mint, MintPayload};
use crate::protocol::privilege::{parse_privilege_auth, PrivilegeAuthPayload};
use crate::protocol::send::{parse_send, TokenSendPayload};
use crate::protocol::tap::{decode_envelope, value_stringify_source, TapOp};
use crate::protocol::token::{
    parse_token_deploy, parse_token_mint, TokenDeployPayload, TokenMintPayload,
};
use crate::protocol::trade::{parse_trade, TokenTradePayload};
use crate::protocol::transfer::{parse_transfer, TokenTransferPayload};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TapEnvelope {
    TokenDeploy(TokenDeployPayload),
    TokenMint(TokenMintPayload),
    Deploy(DeployPayload),
    Mint(MintPayload),
    Transfer(TokenTransferPayload),
    Trade(TokenTradePayload),
    Control(ControlPayload),
    Send(TokenSendPayload),
    Auth(TokenAuthPayload),
    PrivilegeAuth(PrivilegeAuthPayload),
}

impl TapEnvelope {
    /// Representative ticker for the envelope. For multi-item ops this
    /// returns the first item's ticker; used only by the coarse
    /// `envelope_matches_indexed` filter in scan.rs.
    pub fn ticker(&self) -> &str {
        match self {
            Self::TokenDeploy(d) => d.ticker.as_str(),
            Self::TokenMint(m) => m.ticker.as_str(),
            Self::Deploy(d) => d.ticker.as_str(),
            Self::Mint(m) => m.ticker.as_str(),
            Self::Transfer(t) => t.ticker.as_str(),
            Self::Trade(t) => t.ticker(),
            Self::Control(_) => "dmt-nat",
            Self::Send(s) => s
                .items
                .first()
                .map(|i| i.ticker.as_str())
                .unwrap_or("dmt-nat"),
            Self::Auth(a) => a.representative_ticker(),
            Self::PrivilegeAuth(_) => "nat",
        }
    }

    pub fn op(&self) -> TapOp {
        match self {
            Self::TokenDeploy(_) => TapOp::TokenDeploy,
            Self::TokenMint(_) => TapOp::TokenMint,
            Self::Deploy(_) => TapOp::DmtDeploy,
            Self::Mint(_) => TapOp::DmtMint,
            Self::Transfer(_) => TapOp::TokenTransfer,
            Self::Trade(_) => TapOp::TokenTrade,
            Self::Send(_) => TapOp::TokenSend,
            Self::Auth(_) => TapOp::TokenAuth,
            Self::PrivilegeAuth(_) => TapOp::PrivilegeAuth,
            Self::Control(c) => match c.op {
                crate::protocol::control::ControlOp::Block => TapOp::BlockTransferables,
                crate::protocol::control::ControlOp::Unblock => TapOp::UnblockTransferables,
            },
        }
    }
}

/// Attempt to decode an ord envelope as a TAP payload. Returns `Ok(None)`
/// for non-TAP content (not an error — most inscriptions are not TAP).
pub fn decode_tap_payload(env: &Envelope) -> Result<Option<TapEnvelope>> {
    decode_tap_payload_with_value_stringify(env, false)
}

pub fn decode_tap_payload_at_height(env: &Envelope, height: u64) -> Result<Option<TapEnvelope>> {
    decode_tap_payload_with_value_stringify(env, height >= 885_588)
}

fn decode_tap_payload_with_value_stringify(
    env: &Envelope,
    stringify_values: bool,
) -> Result<Option<TapEnvelope>> {
    if !tap_compatible_content_type(env) {
        return Ok(None);
    }
    let mut body = maybe_decode_content(env);
    if stringify_values {
        if let Ok(s) = std::str::from_utf8(&body) {
            body = value_stringify_source(s).into_bytes();
        }
    }
    // Cheap prefix sniff: TAP payloads are JSON objects. Anything else
    // can be skipped without invoking serde_json.
    let first = body.iter().copied().find(|b| !b.is_ascii_whitespace());
    if first != Some(b'{') {
        return Ok(None);
    }
    // Peek at the op so we route directly to the right parser.
    let (op, _) = match decode_envelope(&body) {
        Ok(v) => v,
        // Non-TAP or malformed JSON. Not our problem.
        Err(_) => return Ok(None),
    };
    match op {
        TapOp::TokenDeploy => Ok(Some(TapEnvelope::TokenDeploy(parse_token_deploy(&body)?))),
        TapOp::TokenMint => Ok(Some(TapEnvelope::TokenMint(parse_token_mint(&body)?))),
        TapOp::DmtDeploy => Ok(Some(TapEnvelope::Deploy(parse_deploy(&body)?))),
        TapOp::DmtMint => Ok(Some(TapEnvelope::Mint(parse_mint(&body)?))),
        TapOp::TokenTransfer => Ok(Some(TapEnvelope::Transfer(parse_transfer(&body)?))),
        TapOp::TokenSend => Ok(Some(TapEnvelope::Send(parse_send(&body)?))),
        TapOp::TokenTrade => Ok(Some(TapEnvelope::Trade(parse_trade(&body)?))),
        TapOp::TokenAuth => Ok(Some(TapEnvelope::Auth(parse_auth(&body)?))),
        TapOp::PrivilegeAuth => Ok(Some(TapEnvelope::PrivilegeAuth(parse_privilege_auth(
            &body,
        )?))),
        TapOp::BlockTransferables | TapOp::UnblockTransferables => {
            Ok(Some(TapEnvelope::Control(parse_control(&body)?)))
        }
    }
}

pub fn tap_compatible_content_type(env: &Envelope) -> bool {
    match env.content_type.as_deref() {
        None | Some("") => true,
        Some(ct) => ct.starts_with("text/plain") || ct.starts_with("application/json"),
    }
}

fn maybe_decode_content(env: &Envelope) -> Vec<u8> {
    match env.content_encoding.as_deref() {
        Some("br") => decompress_brotli(&env.content).unwrap_or_else(|| env.content.clone()),
        _ => env.content.clone(),
    }
}

fn decompress_brotli(bytes: &[u8]) -> Option<Vec<u8>> {
    use std::io::Read;
    let mut out = Vec::with_capacity(bytes.len() * 4);
    let mut reader = brotli::Decompressor::new(bytes, 4096);
    match reader.read_to_end(&mut out) {
        Ok(_) => Some(out),
        Err(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn brotli_roundtrip() {
        use std::io::Write;
        let src = br#"{"p":"tap","op":"token-transfer","tick":"nat","amt":"1000"}"#;
        let mut encoded = Vec::new();
        let mut enc = brotli::CompressorWriter::new(&mut encoded, 4096, 5, 22);
        enc.write_all(src).unwrap();
        drop(enc);
        let back = decompress_brotli(&encoded).unwrap();
        assert_eq!(back, src);
    }

    #[test]
    fn missing_content_type_can_still_be_tap() {
        let env = Envelope {
            kind: crate::inscription::envelope::EnvelopeKind::Inscription { index: 0 },
            content_type: None,
            content_encoding: None,
            delegate: None,
            content: br#"{"p":"tap","op":"token-transfer","tick":"dmt-nat","amt":"800000000000"}"#
                .to_vec(),
            input_index: 0,
            envelope_offset: 0,
            pointer: None,
            duplicate_field: false,
            incomplete_field: false,
            unrecognized_even_field: false,
            pushnum: false,
            stutter: false,
        };
        let decoded = decode_tap_payload_at_height(&env, 946_020)
            .unwrap()
            .unwrap();
        assert_eq!(decoded.ticker(), "dmt-nat");
    }

    #[test]
    fn incompatible_content_type_is_not_tap() {
        let env = Envelope {
            kind: crate::inscription::envelope::EnvelopeKind::Inscription { index: 0 },
            content_type: Some("image/png".to_string()),
            content_encoding: None,
            delegate: None,
            content: br#"{"p":"tap","op":"token-transfer","tick":"dmt-nat","amt":"1"}"#.to_vec(),
            input_index: 0,
            envelope_offset: 0,
            pointer: None,
            duplicate_field: false,
            incomplete_field: false,
            unrecognized_even_field: false,
            pushnum: false,
            stutter: false,
        };
        assert!(decode_tap_payload_at_height(&env, 946_020)
            .unwrap()
            .is_none());
    }

    #[test]
    fn not_brotli_returns_none() {
        assert!(decompress_brotli(b"not brotli bytes").is_none());
    }
}

/// Reject fields we know cannot belong to a TAP-relevant inscription.
/// (Currently a no-op; reserved for forward compatibility.)
pub fn reject_non_tap(env: &Envelope) -> Result<()> {
    if let Some(ct) = env.content_type.as_deref() {
        if !ct.starts_with("text/plain") && !ct.starts_with("application/json") && !ct.is_empty() {
            return Err(Error::Protocol(format!(
                "content-type not TAP-compatible: {ct}"
            )));
        }
    }
    Ok(())
}
