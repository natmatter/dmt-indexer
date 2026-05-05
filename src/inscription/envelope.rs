//! Ord-style inscription envelope parser.
//!
//! The envelope format (current ord ≥ 0.14) embeds an inscription in a
//! taproot witness script as:
//!
//! ```text
//!   OP_FALSE OP_IF
//!     <"ord">
//!     OP_1     <content_type>
//!     OP_5     <metadata>       // optional
//!     OP_7     <metaprotocol>   // optional
//!     OP_9     <content_encoding> // optional
//!     OP_11    <delegate>       // optional
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

use std::collections::BTreeMap;

use bitcoin::hashes::Hash as _;
use bitcoin::opcodes::all as op;
use bitcoin::opcodes::{OP_FALSE, OP_TRUE};
use bitcoin::script::{Instruction, Script};
use bitcoin::{Transaction, Txid};
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
const TAG_PARENT: u8 = 3;
const TAG_METADATA: u8 = 5;
const TAG_METAPROTOCOL: u8 = 7;
const TAG_CONTENT_ENCODING: u8 = 9;
const TAG_DELEGATE: u8 = 11;
const TAG_RUNE: u8 = 13;
const TAG_PROPERTIES: u8 = 17;

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
    /// Ord delegate inscription id, decoded from tag 11 when present.
    /// A delegated inscription keeps its own inscription id/owner, but
    /// ord-tap resolves the TAP payload from this delegate content.
    #[serde(default)]
    pub delegate: Option<String>,
    pub content: Vec<u8>,
    /// Input index on the reveal tx that carried this envelope.
    pub input_index: u32,
    /// Envelope offset within the input tapscript. Pre-Jubilee ord curses
    /// inscriptions whose offset is not zero.
    #[serde(default)]
    pub envelope_offset: u32,
    /// OP_2 pointer value, decoded as little-endian u64. None when the
    /// envelope has no pointer tag or the value is malformed (non-zero
    /// bytes past index 8). Used by the scan loop to route the
    /// inscription to a non-default sat offset per ord's
    /// `Inscription::pointer()`.
    #[serde(default)]
    pub pointer: Option<u64>,
    /// Ord curse metadata. These fields are retained so pre-Jubilee TAP/DMT
    /// payloads match ord-tap's blessed/cursed split.
    #[serde(default)]
    pub duplicate_field: bool,
    #[serde(default)]
    pub incomplete_field: bool,
    #[serde(default)]
    pub unrecognized_even_field: bool,
    #[serde(default)]
    pub pushnum: bool,
    #[serde(default)]
    pub stutter: bool,
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

    /// Pre-Jubilee curse tests that are knowable from the envelope itself.
    /// Reinscription is chain-state dependent and is checked by the scan loop
    /// against the sat tracker.
    pub fn is_pre_jubilee_cursed_without_reinscription(&self) -> bool {
        self.unrecognized_even_field
            || self.duplicate_field
            || self.incomplete_field
            || self.input_index != 0
            || self.envelope_offset != 0
            || self.pointer.is_some()
            || self.pushnum
            || self.stutter
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
            // Cheap pre-filter: ord envelopes begin with OP_FALSE OP_IF,
            // then a script-level push of protocol id "ord". Do not
            // require the push to use minimal `PUSH3` encoding here:
            // ord-tap relies on script instruction parsing and accepts
            // valid non-minimal pushes such as `OP_PUSHDATA1 03 "ord"`.
            // This pre-filter must be broad enough to avoid false
            // negatives; parse_script remains the authoritative check.
            if !script_has_envelope_signature(tapscript) {
                continue;
            }
            for env in parse_script(tapscript, input_index as u32, &mut global_index) {
                out.push(env);
            }
        }
    }
    if block_height < JUBILEE_HEIGHT {
        out.retain(|e| !e.is_pre_jubilee_cursed_without_reinscription());
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
    let mut stuttered = false;
    while iter.peek().is_some() {
        // Look for OP_FALSE OP_IF "ord"
        let Some(Ok(inst)) = iter.next() else {
            continue;
        };
        if !is_empty_push_instruction(&inst) {
            continue;
        }
        let (next_stuttered, env) = parse_envelope_from_instructions(
            &mut iter,
            input_index,
            out.len() as u32,
            stuttered,
            global_index,
        );
        if let Some(env) = env {
            out.push(env);
            stuttered = false;
        } else {
            stuttered = next_stuttered;
        }
    }
    out
}

fn parse_envelope_from_instructions<'a, I>(
    iter: &mut std::iter::Peekable<I>,
    input_index: u32,
    envelope_offset: u32,
    stutter: bool,
    global_index: &mut u32,
) -> (bool, Option<Envelope>)
where
    I: Iterator<Item = Result<Instruction<'a>, bitcoin::script::Error>>,
{
    if !accept_next_op(iter, op::OP_IF.into()) {
        return (peek_is_empty_push(iter), None);
    }
    if !accept_protocol_id(iter) {
        return (peek_is_empty_push(iter), None);
    }

    let mut payload: Vec<Vec<u8>> = Vec::new();
    let mut pushnum = false;
    loop {
        let Some(inst) = iter.next() else {
            return (false, None);
        };
        let Ok(inst) = inst else {
            return (false, None);
        };
        match inst {
            Instruction::Op(o) if o == op::OP_ENDIF.into() => {
                let env = envelope_from_payload(
                    input_index,
                    envelope_offset,
                    stutter,
                    pushnum,
                    payload,
                    global_index,
                );
                return (false, Some(env));
            }
            Instruction::Op(o) if is_push_0(o) => payload.push(Vec::new()),
            Instruction::Op(o) if o == op::OP_PUSHNUM_NEG1.into() => {
                pushnum = true;
                payload.push(vec![0x81]);
            }
            Instruction::Op(o) if pushnum_to_int(o).is_some() => {
                pushnum = true;
                payload.push(vec![pushnum_to_int(o).unwrap()]);
            }
            Instruction::PushBytes(b) => payload.push(b.as_bytes().to_vec()),
            _ => return (false, None),
        }
    }
}

fn envelope_from_payload(
    input_index: u32,
    envelope_offset: u32,
    stutter: bool,
    pushnum: bool,
    payload: Vec<Vec<u8>>,
    global_index: &mut u32,
) -> Envelope {
    let body = payload
        .iter()
        .enumerate()
        .position(|(i, push)| i % 2 == 0 && push.is_empty());
    let mut fields: BTreeMap<Vec<u8>, Vec<Vec<u8>>> = BTreeMap::new();
    let mut incomplete_field = false;
    for item in payload[..body.unwrap_or(payload.len())].chunks(2) {
        match item {
            [key, value] => fields.entry(key.clone()).or_default().push(value.clone()),
            _ => incomplete_field = true,
        }
    }
    let duplicate_field = fields.values().any(|values| values.len() > 1);

    let content_encoding = take_field(&mut fields, TAG_CONTENT_ENCODING, false);
    let content_type = take_field(&mut fields, TAG_CONTENT_TYPE, false);
    let delegate_bytes = take_field(&mut fields, TAG_DELEGATE, false);
    let pointer_bytes = take_field(&mut fields, TAG_POINTER, false);
    let _metadata = take_field(&mut fields, TAG_METADATA, true);
    let _metaprotocol = take_field(&mut fields, TAG_METAPROTOCOL, false);
    let _parents = take_array_field(&mut fields, TAG_PARENT);
    let _properties = take_field(&mut fields, TAG_PROPERTIES, true);
    let _rune = take_field(&mut fields, TAG_RUNE, false);

    let unrecognized_even_field = fields
        .keys()
        .any(|tag| tag.first().map(|lsb| lsb % 2 == 0).unwrap_or_default());
    let content = body
        .map(|i| payload[i + 1..].iter().flatten().copied().collect())
        .unwrap_or_default();
    let pointer = pointer_bytes.as_deref().and_then(decode_pointer_u64);
    let env = Envelope {
        kind: EnvelopeKind::Inscription {
            index: *global_index,
        },
        content_type: content_type.and_then(|b| String::from_utf8(b).ok()),
        content_encoding: content_encoding.and_then(|b| String::from_utf8(b).ok()),
        delegate: delegate_bytes
            .as_deref()
            .and_then(decode_inscription_id_value),
        content,
        input_index,
        envelope_offset,
        pointer,
        duplicate_field,
        incomplete_field,
        unrecognized_even_field,
        pushnum,
        stutter,
    };
    *global_index += 1;
    env
}

fn take_field(
    fields: &mut BTreeMap<Vec<u8>, Vec<Vec<u8>>>,
    tag: u8,
    chunked: bool,
) -> Option<Vec<u8>> {
    let key = vec![tag];
    if chunked {
        fields
            .remove(&key)
            .map(|values| values.into_iter().flatten().collect())
    } else {
        let values = fields.get_mut(&key)?;
        if values.is_empty() {
            None
        } else {
            let value = values.remove(0);
            if values.is_empty() {
                fields.remove(&key);
            }
            Some(value)
        }
    }
}

fn take_array_field(fields: &mut BTreeMap<Vec<u8>, Vec<Vec<u8>>>, tag: u8) -> Vec<Vec<u8>> {
    fields.remove(&vec![tag]).unwrap_or_default()
}

fn decode_inscription_id_value(value: &[u8]) -> Option<String> {
    if value.len() < 32 || value.len() > 36 {
        return None;
    }
    let (txid_bytes, index_bytes) = value.split_at(32);
    if let Some(last) = index_bytes.last() {
        if index_bytes.len() != 4 && *last == 0 {
            return None;
        }
    }
    let txid = Txid::from_slice(txid_bytes).ok()?;
    let index = [
        index_bytes.first().copied().unwrap_or_default(),
        index_bytes.get(1).copied().unwrap_or_default(),
        index_bytes.get(2).copied().unwrap_or_default(),
        index_bytes.get(3).copied().unwrap_or_default(),
    ];
    Some(format!("{}i{}", txid, u32::from_le_bytes(index)))
}

/// Fast pre-filter: return true if the script *could* contain an
/// ord envelope. Looks for the script envelope opener
/// `0x00 0x63` (OP_FALSE OP_IF).
///
/// This is a pure byte-scan with no script parsing. False positives
/// (scripts containing the sequence as non-envelope data) are fine —
/// the real parser will reject them cheaply. False negatives would
/// silently drop valid inscriptions, so the signature MUST match
/// every script form ord-tap's instruction parser can accept.
fn script_has_envelope_signature(bytes: &[u8]) -> bool {
    const SIGNATURE: &[u8] = &[0x00, 0x63];
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

fn is_empty_push_instruction(inst: &Instruction<'_>) -> bool {
    match inst {
        Instruction::Op(o) => is_push_0(*o),
        Instruction::PushBytes(b) => b.as_bytes().is_empty(),
    }
}

fn accept_next_op<'a, I>(
    iter: &mut std::iter::Peekable<I>,
    opcode: bitcoin::opcodes::Opcode,
) -> bool
where
    I: Iterator<Item = Result<Instruction<'a>, bitcoin::script::Error>>,
{
    if matches!(iter.peek(), Some(Ok(Instruction::Op(o))) if *o == opcode) {
        let _ = iter.next();
        true
    } else {
        false
    }
}

fn accept_protocol_id<'a, I>(iter: &mut std::iter::Peekable<I>) -> bool
where
    I: Iterator<Item = Result<Instruction<'a>, bitcoin::script::Error>>,
{
    if matches!(iter.peek(), Some(Ok(Instruction::PushBytes(b))) if b.as_bytes() == PROTOCOL_ID) {
        let _ = iter.next();
        true
    } else {
        false
    }
}

fn peek_is_empty_push<'a, I>(iter: &mut std::iter::Peekable<I>) -> bool
where
    I: Iterator<Item = Result<Instruction<'a>, bitcoin::script::Error>>,
{
    matches!(iter.peek(), Some(Ok(inst)) if is_empty_push_instruction(inst))
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

    fn script_with_payload(payload_script: &[u8]) -> Vec<u8> {
        let mut script = vec![0x00, 0x63, 0x03, b'o', b'r', b'd'];
        script.extend_from_slice(payload_script);
        script.push(0x68);
        script
    }

    fn parse_one_script(script: &[u8]) -> Envelope {
        let mut global_index = 0;
        let envs = parse_script(script, 0, &mut global_index);
        assert_eq!(envs.len(), 1);
        envs.into_iter().next().unwrap()
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
    fn fast_path_accepts_non_minimal_protocol_push() {
        // ord-tap accepts this as PushBytes("ord"). The fast path must
        // not reject it before the instruction parser sees it.
        let script: &[u8] = &[0x00, 0x63, 0x4c, 0x03, b'o', b'r', b'd', 0x68];
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
    fn parse_envelopes_accepts_non_minimal_protocol_push() {
        let script = vec![
            0x00, 0x63, 0x4c, 0x03, b'o', b'r', b'd', 0x00, 0x01, b'x', 0x68,
        ];
        let mut witness = bitcoin::Witness::new();
        witness.push([0x01]);
        witness.push(script);
        witness.push([0xc0]);
        let tx = bitcoin::Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness,
            }],
            output: vec![],
        };
        let envs = parse_envelopes(&tx);
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].content.as_slice(), b"x");
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
    fn parser_preserves_pre_jubilee_duplicate_field_curse() {
        // tag 1/value "a", tag 1/value "b", body "x"
        let script = script_with_payload(&[
            0x01, 0x01, 0x01, b'a', 0x01, 0x01, 0x01, b'b', 0x00, 0x01, b'x',
        ]);
        let env = parse_one_script(&script);
        assert!(env.duplicate_field);
        assert!(env.is_pre_jubilee_cursed_without_reinscription());
    }

    #[test]
    fn parser_preserves_pre_jubilee_unrecognized_even_field_curse() {
        // tag 4 is not an ord-recognized tag and has an even low byte.
        let script = script_with_payload(&[0x01, 0x04, 0x01, b'a', 0x00, 0x01, b'x']);
        let env = parse_one_script(&script);
        assert!(env.unrecognized_even_field);
        assert!(env.is_pre_jubilee_cursed_without_reinscription());
    }

    #[test]
    fn parser_preserves_pre_jubilee_incomplete_field_curse() {
        let script = script_with_payload(&[0x01, 0x01]);
        let env = parse_one_script(&script);
        assert!(env.incomplete_field);
        assert!(env.is_pre_jubilee_cursed_without_reinscription());
    }

    #[test]
    fn parser_preserves_pre_jubilee_pointer_curse() {
        // Pointer tag with empty value decodes as pointer 0.
        let script = script_with_payload(&[0x01, 0x02, 0x00, 0x00, 0x01, b'x']);
        let env = parse_one_script(&script);
        assert_eq!(env.pointer, Some(0));
        assert!(env.is_pre_jubilee_cursed_without_reinscription());
    }

    #[test]
    fn parser_preserves_pre_jubilee_pushnum_curse() {
        let script = script_with_payload(&[op::OP_PUSHNUM_1.to_u8(), 0x01, b'a', 0x00]);
        let env = parse_one_script(&script);
        assert!(env.pushnum);
        assert!(env.is_pre_jubilee_cursed_without_reinscription());
    }

    #[test]
    fn parser_preserves_pre_jubilee_stutter_curse() {
        let mut script = vec![0x00];
        script.extend_from_slice(&script_with_payload(&[0x00, 0x01, b'x']));
        let env = parse_one_script(&script);
        assert!(env.stutter);
        assert!(env.is_pre_jubilee_cursed_without_reinscription());
    }

    #[test]
    fn hash_type_sanity() {
        // sanity: bitcoin crate imports compile
        let _h: bitcoin::hashes::sha256::Hash = bitcoin::hashes::sha256::Hash::hash(b"hi");
    }
}
