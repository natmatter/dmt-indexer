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
use crate::protocol::control::{parse_control, ControlPayload};
use crate::protocol::deploy::{parse_deploy, DeployPayload};
use crate::protocol::mint::{parse_mint, MintPayload};
use crate::protocol::send::{parse_send, TokenSendPayload};
use crate::protocol::tap::{decode_envelope, TapOp};
use crate::protocol::transfer::{parse_transfer, TokenTransferPayload};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TapEnvelope {
    Deploy(DeployPayload),
    Mint(MintPayload),
    Transfer(TokenTransferPayload),
    Send(TokenSendPayload),
    Control(ControlPayload),
}

impl TapEnvelope {
    pub fn ticker(&self) -> &str {
        match self {
            Self::Deploy(d) => d.ticker.as_str(),
            Self::Mint(m) => m.ticker.as_str(),
            Self::Transfer(t) => t.ticker.as_str(),
            // token-send has per-item tickers; upstream filters by the
            // first one (must be non-empty; parser guarantees it).
            Self::Send(s) => s.first_ticker().map(|t| t.as_str()).unwrap_or(""),
            Self::Control(c) => c.ticker.as_str(),
        }
    }

    pub fn op(&self) -> TapOp {
        match self {
            Self::Deploy(_) => TapOp::DmtDeploy,
            Self::Mint(_) => TapOp::DmtMint,
            Self::Transfer(_) => TapOp::TokenTransfer,
            Self::Send(_) => TapOp::TokenSend,
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
    let body = maybe_decode_content(env);
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
        TapOp::DmtDeploy => Ok(Some(TapEnvelope::Deploy(parse_deploy(&body)?))),
        TapOp::DmtMint => Ok(Some(TapEnvelope::Mint(parse_mint(&body)?))),
        TapOp::TokenTransfer => Ok(Some(TapEnvelope::Transfer(parse_transfer(&body)?))),
        TapOp::TokenSend => Ok(Some(TapEnvelope::Send(parse_send(&body)?))),
        TapOp::BlockTransferables | TapOp::UnblockTransferables => {
            Ok(Some(TapEnvelope::Control(parse_control(&body)?)))
        }
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
