//! Ord-style inscription envelope parser.
//!
//! The envelope format (current ord ≥ 0.14) embeds an inscription in a
//! taproot witness script as:
//!
//! ```text
//!   OP_FALSE OP_IF
//!     <"ord">
//!     OP_1     <content_type>
//!     OP_11    <metadata>       // optional
//!     OP_5     <metaprotocol>   // optional
//!     OP_7     <content_encoding> // optional
//!     OP_9     <delegate>       // optional
//!     OP_0     <content chunk>*
//!   OP_ENDIF
//! ```
//!
//! A single reveal tx can contain multiple envelopes (ord calls these
//! "inscriptions at index 0/1/2..."). We parse the script, return each
//! envelope's full byte content concatenated, and identify the TAP
//! protocol ID `"ord"` + content-type `"text/plain"` (or missing) +
//! content whose JSON starts with `{"p":"tap"` in `protocol::envelope`.
//!
//! We handle the envelope tags that affect carrier routing or payload
//! decoding:
//! - content-type (tag 1), content-encoding (tag 9): needed to decode
//!   the body (brotli-encoded JSON for live $NAT).
//! - pointer (tag 2): reroutes the inscription to a non-default sat
//!   offset within the reveal tx. Per ord-tap
//!   `inscriptions/inscription.rs:223` the value is a 0..8 byte
//!   little-endian u64; values with non-zero bytes past index 8 are
//!   rejected. Used by
//!   `index/updater/inscription_updater.rs:520` as:
//!   `payload.pointer().filter(|p| *p < total_output_value).unwrap_or(offset)`.
//!   Rarely used by TAP payloads but still applied when present.
//! - parent/child (`OP_3`, `OP_5`) — not needed for ledger correctness.
//!
//! For brotli-decoding of `content_encoding == "br"` payloads see
//! `decode_content`. The live `$NAT` deploy uses brotli-encoded JSON.

use bitcoin::opcodes::all as op;
use bitcoin::opcodes::{OP_FALSE, OP_TRUE};
use bitcoin::script::{Instruction, Script};
use bitcoin::Transaction;
use serde::{Deserialize, Serialize};

pub const PROTOCOL_ID: &[u8] = b"ord";

/// Jubilee activation height (ord's switch from "cursed" to "blessed"
/// for all inscriptions). Block < JUBILEE: apply the pre-Jubilee
/// strictness rules; block >= JUBILEE: all well-formed envelopes are
/// blessed.
pub const JUBILEE_HEIGHT: u64 = 824_544;

/// Ord envelope tags we care about. NAT uses content-type,
/// content-encoding, and (rarely) pointer. Parent/metadata/delegate
/// etc. are irrelevant to TAP ledger routing.
const TAG_CONTENT_TYPE: u8 = 1;
const TAG_POINTER: u8 = 2;
const TAG_CONTENT_ENCODING: u8 = 9;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum EnvelopeKind {
    /// Inscription at index N within the reveal tx (0 == first).
    Inscription { index: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub kind: EnvelopeKind,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub content: Vec<u8>,
    /// Input index on the reveal tx that carried this envelope.
    pub input_index: u32,
    /// OP_2 pointer value, decoded as little-endian u64. None when the
    /// envelope has no pointer tag or the value is malformed (non-zero
    /// bytes past index 8). Used by the scan loop to route the
    /// inscription to a non-default sat offset per ord's
    /// `Inscription::pointer()`.
    #[serde(default)]
    pub pointer: Option<u64>,
}

impl Envelope {
    /// Decode the content body (de-brotli if `content_encoding == "br"`).
    /// Falls back to raw bytes on decode failure — callers downstream
    /// still try to parse both forms.
    pub fn decoded_content(&self) -> Vec<u8> {
        match self.content_encoding.as_deref() {
            Some("br") => decompress_brotli(&self.content).unwrap_or_else(|| self.content.clone()),
            _ => self.content.clone(),
        }
    }
}

/// Maximum decompressed content size we'll honor. TAP JSON payloads are
/// typically < 1 KB. This cap defends against brotli-bomb inscriptions
/// that compress tiny bytes into gigabytes — we stop reading at this
/// limit and let downstream parsers reject the truncated bytes.
const MAX_DECOMPRESSED_SIZE: u64 = 2 * 1024 * 1024; // 2 MB

fn decompress_brotli(bytes: &[u8]) -> Option<Vec<u8>> {
    use std::io::Read;
    let mut out = Vec::with_capacity(bytes.len().saturating_mul(4).min(64 * 1024));
    let decoder = brotli::Decompressor::new(bytes, 4096);
    match decoder.take(MAX_DECOMPRESSED_SIZE).read_to_end(&mut out) {
        Ok(_) => Some(out),
        Err(_) => None,
    }
}

/// Scan every witness script in the tx for ord envelopes at `block_height`.
///
/// Pre-Jubilee (`block_height < JUBILEE_HEIGHT`) ord's cursed rules
/// demote any envelope that is not the single envelope on input 0.
/// Per TAP/DMT conformance, cursed inscriptions carrying a `tick`
/// internally belong to a dash-prefixed namespace (`-nat` not `nat`),
/// which we do NOT index. We filter cursed candidates out at parse
/// time so they never enter the downstream ledger.
///
/// Post-Jubilee (`>= JUBILEE_HEIGHT`) every well-formed envelope is
/// blessed — multiple inscriptions per tx and on non-zero inputs are
/// all accepted.
pub fn parse_envelopes(tx: &Transaction) -> Vec<Envelope> {
    parse_envelopes_at_height(tx, u64::MAX)
}

pub fn parse_envelopes_at_height(tx: &Transaction, block_height: u64) -> Vec<Envelope> {
    let mut out = Vec::new();
    let mut global_index: u32 = 0;
    for (input_index, txin) in tx.input.iter().enumerate() {
        if let Some(tapscript) = tapscript_from_witness(&txin.witness) {
            // Cheap pre-filter: the ord envelope opening sequence
            // begins with `0x00 0x63` (OP_FALSE OP_IF) followed shortly
            // by the 4-byte push of protocol id "ord" (`0x03 "ord"`).
            // Scanning for the 5-byte signature takes ~nanoseconds via
            // memchr and rules out ≥99% of real-world taproot
            // script-path spends that are not inscriptions. The script
            // parser is hundreds of ns to microseconds per call, so
            // this saves a lot on mint-era blocks with many non-ord
            // taproot inputs.
            if !script_has_envelope_signature(tapscript) {
                continue;
            }
            for env in parse_script(tapscript, input_index as u32, &mut global_index) {
                out.push(env);
            }
        }
    }
    if block_height < JUBILEE_HEIGHT {
        // Pre-Jubilee cursed rules (simplified to the two that
        // actually matter for TAP payloads):
        //   1. An inscription outside input 0 is cursed.
        //   2. A second+ inscription in the same tx is cursed.
        // Keep only the first envelope on input 0, if any.
        out.retain(|e| e.input_index == 0);
        out.truncate(1);
    }
    out
}

/// Extract the tapscript (second-to-last element of the witness when
/// the last element is the control block) from a taproot script-path
/// spend witness.
fn tapscript_from_witness(witness: &bitcoin::Witness) -> Option<&[u8]> {
    let len = witness.len();
    if len < 2 {
        return None;
    }
    // Annex-prefixed witness: last element starts with 0x50 and is the
    // annex. Skip it if present.
    let last = witness.nth(len - 1)?;
    let (script_idx, annex) = if last.first() == Some(&0x50) {
        if len < 3 {
            return None;
        }
        (len - 3, Some(last))
    } else {
        (len - 2, None)
    };
    let _ = annex;
    witness.nth(script_idx)
}

fn parse_script(script_bytes: &[u8], input_index: u32, global_index: &mut u32) -> Vec<Envelope> {
    let script = Script::from_bytes(script_bytes);
    let mut out = Vec::new();
    let mut iter = script.instructions().peekable();
    while iter.peek().is_some() {
        // Look for OP_FALSE OP_IF "ord"
        match iter.next() {
            Some(Ok(Instruction::Op(o))) if o == OP_FALSE.into() => {}
            Some(Ok(Instruction::Op(o))) if is_push_0(o) => {}
            Some(Ok(Instruction::PushBytes(b))) if b.as_bytes().is_empty() => {}
            _ => continue,
        }
        match iter.next() {
            Some(Ok(Instruction::Op(o))) if o == op::OP_IF.into() => {}
            _ => continue,
        }
        match iter.next() {
            Some(Ok(Instruction::PushBytes(b))) if b.as_bytes() == PROTOCOL_ID => {}
            _ => continue,
        }

        // Now read tag/value pairs until we hit OP_0 (body start) or
        // OP_ENDIF (no body).
        let mut content_type: Option<Vec<u8>> = None;
        let mut content_encoding: Option<Vec<u8>> = None;
        let mut pointer_bytes: Option<Vec<u8>> = None;
        let mut content: Vec<u8> = Vec::new();
        let mut in_body = false;
        let mut closed = false;
        while let Some(inst) = iter.next() {
            let Ok(inst) = inst else {
                break;
            };
            match inst {
                Instruction::Op(o) if o == op::OP_ENDIF.into() => {
                    closed = true;
                    break;
                }
                Instruction::Op(o) if is_push_0(o) => {
                    in_body = true;
                }
                Instruction::PushBytes(b) if b.as_bytes().is_empty() => {
                    in_body = true;
                }
                Instruction::PushBytes(b) if !in_body => {
                    // Tag: a single-byte push; value: following push.
                    let tag_bytes = b.as_bytes();
                    let tag = if tag_bytes.len() == 1 {
                        tag_bytes[0]
                    } else {
                        continue;
                    };
                    let Some(Ok(Instruction::PushBytes(val))) = iter.next() else {
                        continue;
                    };
                    match tag {
                        TAG_CONTENT_TYPE => content_type = Some(val.as_bytes().to_vec()),
                        TAG_CONTENT_ENCODING => content_encoding = Some(val.as_bytes().to_vec()),
                        TAG_POINTER => pointer_bytes = Some(val.as_bytes().to_vec()),
                        _ => {}
                    }
                }
                Instruction::PushBytes(b) if in_body => {
                    content.extend_from_slice(b.as_bytes());
                }
                Instruction::Op(o) if in_body && pushnum_to_int(o).is_some() => {
                    // OP_1..OP_16 inside body are treated as byte
                    // values per ord. Not used by TAP; skip.
                }
                _ => {}
            }
        }
        if !closed {
            continue;
        }
        let pointer = pointer_bytes.as_deref().and_then(decode_pointer_u64);
        let env = Envelope {
            kind: EnvelopeKind::Inscription {
                index: *global_index,
            },
            content_type: content_type.and_then(|b| String::from_utf8(b).ok()),
            content_encoding: content_encoding.and_then(|b| String::from_utf8(b).ok()),
            content,
            input_index,
            pointer,
        };
        *global_index += 1;
        out.push(env);
    }
    out
}

/// Fast pre-filter: return true if the script *could* contain an
/// ord envelope. Looks for the 6-byte opening sequence
/// `0x00 0x63 0x03 b'o' b'r' b'd'` (OP_FALSE OP_IF PUSH3 "ord").
///
/// This is a pure byte-scan with no script parsing. False positives
/// (scripts containing the sequence as non-envelope data) are fine —
/// the real parser will reject them cheaply. False negatives would
/// silently drop valid inscriptions, so the signature MUST match
/// every valid ord envelope produced by ord ≥ 0.14.
fn script_has_envelope_signature(bytes: &[u8]) -> bool {
    const SIGNATURE: &[u8] = &[0x00, 0x63, 0x03, b'o', b'r', b'd'];
    // `memchr::memmem` would be slightly faster but adds a dep; the
    // stdlib windows iterator is adequate here because the signature
    // is 6 bytes.
    bytes.windows(SIGNATURE.len()).any(|w| w == SIGNATURE)
}

/// Decode a pointer payload per ord-tap
/// `inscriptions/inscription.rs:223`. Up to 8 bytes, little-endian.
/// Any non-zero byte past index 8 invalidates the pointer (returns
/// None) — matches ord's strict validity check.
fn decode_pointer_u64(value: &[u8]) -> Option<u64> {
    if value.iter().skip(8).copied().any(|b| b != 0) {
        return None;
    }
    let mut buf = [0u8; 8];
    for (i, b) in value.iter().take(8).enumerate() {
        buf[i] = *b;
    }
    Some(u64::from_le_bytes(buf))
}

fn is_push_0(o: bitcoin::opcodes::Opcode) -> bool {
    o == OP_FALSE.into() || o == bitcoin::opcodes::OP_0.into()
}

fn pushnum_to_int(o: bitcoin::opcodes::Opcode) -> Option<u8> {
    let v = o.to_u8();
    if (op::OP_PUSHNUM_1.to_u8()..=op::OP_PUSHNUM_16.to_u8()).contains(&v) {
        Some(v - op::OP_PUSHNUM_1.to_u8() + 1)
    } else if o == OP_TRUE.into() {
        Some(1)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hashes::Hash;

    fn hexstr(s: &str) -> Vec<u8> {
        hex::decode(s).unwrap()
    }

    #[test]
    fn tapscript_extraction_ignores_single_element_witness() {
        let mut w = bitcoin::Witness::new();
        w.push(hexstr("00"));
        assert!(tapscript_from_witness(&w).is_none());
    }

    #[test]
    fn empty_tx_no_envelopes() {
        let tx = bitcoin::Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![],
            output: vec![],
        };
        assert_eq!(parse_envelopes(&tx).len(), 0);
    }

    #[test]
    fn pushnum_mapping() {
        assert_eq!(pushnum_to_int(op::OP_PUSHNUM_1.into()), Some(1));
        assert_eq!(pushnum_to_int(op::OP_PUSHNUM_16.into()), Some(16));
    }

    #[test]
    fn fast_path_detects_ord_signature() {
        let script: &[u8] = &[0x00, 0x63, 0x03, b'o', b'r', b'd', 0x68];
        assert!(script_has_envelope_signature(script));
    }

    #[test]
    fn fast_path_rejects_random_script() {
        let script: &[u8] = &[0x51, 0x02, 0xff, 0xaa, 0x63, 0x01, 0x75];
        assert!(!script_has_envelope_signature(script));
    }

    #[test]
    fn fast_path_accepts_signature_with_surrounding_bytes() {
        let mut script = vec![0x51, 0x52, 0x53];
        script.extend_from_slice(&[0x00, 0x63, 0x03, b'o', b'r', b'd']);
        script.extend_from_slice(&[0x01, 0x01, 0x68]);
        assert!(script_has_envelope_signature(&script));
    }

    #[test]
    fn pointer_decode_empty_is_zero() {
        assert_eq!(decode_pointer_u64(&[]), Some(0));
    }

    #[test]
    fn pointer_decode_little_endian_u64() {
        // 0x0102 LE == 258
        assert_eq!(decode_pointer_u64(&[0x02, 0x01]), Some(258));
        // Max 8-byte LE
        assert_eq!(decode_pointer_u64(&[0xff; 8]), Some(u64::MAX));
    }

    #[test]
    fn pointer_decode_rejects_non_zero_past_byte_8() {
        let mut v = vec![0u8; 10];
        v[9] = 1; // non-zero at index 9
        assert_eq!(decode_pointer_u64(&v), None);
    }

    #[test]
    fn pointer_decode_allows_trailing_zero_padding() {
        // ord accepts values longer than 8 bytes when the extra bytes
        // are all zero — the prefix must be the u64 LE value.
        let v = vec![0x05, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(decode_pointer_u64(&v), Some(5));
    }

    #[test]
    fn hash_type_sanity() {
        // sanity: bitcoin crate imports compile
        let _h: bitcoin::hashes::sha256::Hash = bitcoin::hashes::sha256::Hash::hash(b"hi");
    }
}
