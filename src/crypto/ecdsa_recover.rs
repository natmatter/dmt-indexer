//! ECDSA-with-public-key-recovery signature verification for
//! `token-auth` create and redeem forms.
//!
//! Mirrors ord-tap's `verify_sig_obj_against_msg_with_hash` at
//! `/tmp/ot_iu.rs:1450-1481`.
//!
//! Inputs:
//!  - `sig_v` / `sig_r` / `sig_s` — payload's `sig` subobject components.
//!    `v` is the recovery id (accepts raw 0/1 or ethereum-style 27/28;
//!    ord-tap tries raw then falls back to v-27).
//!  - `sig_r` / `sig_s` — hex-encoded (`0x…` optionally) or decimal big-
//!    integer strings, left-padded to 32 bytes big-endian.
//!  - `recovery_hash_hex` — 32-byte hex, the *declared* hash the signer
//!    claims they signed. Must equal `msg_hash` (else the verifier
//!    recovered a pubkey from the wrong preimage).
//!  - `msg_hash` — the locally-recomputed `sha256(serde_json(subtree) || salt)`.
//!    The caller builds this from the exact bytes the signer signed.
//!
//! Returns `Some((compact_sig_hex, pubkey_hex))` on success. `pubkey_hex`
//! is the UNCOMPRESSED serialization (65 bytes → 130 hex chars) — this
//! matches ord-tap (`/tmp/ot_iu.rs:1475-1476`) so pubkey comparison
//! between create and redeem stays byte-identical.

use num_bigint_stub::BigUintLite;
use secp256k1::ecdsa::{RecoverableSignature, RecoveryId, Signature as SecpSignature};
use secp256k1::{Message, Secp256k1};
use sha2::{Digest, Sha256};

/// Lightweight in-house big-integer parser: accepts an ASCII decimal
/// string and returns its big-endian byte representation, suitable for
/// 32-byte-pad comparisons. Avoids pulling a `num-bigint` dep just for
/// the one call site. Returns `None` on non-digit input.
mod num_bigint_stub {
    pub struct BigUintLite(pub Vec<u8>);

    impl BigUintLite {
        pub fn parse_decimal(s: &str) -> Option<Self> {
            let s = s.trim();
            if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            // Decimal → base-256: repeated divide-by-256 from leading.
            // For 32-byte numbers this is O(n²) but with n≤78 it's trivial.
            let mut digits: Vec<u8> = s.bytes().map(|b| b - b'0').collect();
            let mut out: Vec<u8> = Vec::new();
            while !digits.is_empty() {
                let mut carry: u32 = 0;
                let mut new_digits: Vec<u8> = Vec::with_capacity(digits.len());
                for &d in &digits {
                    let cur = carry * 10 + d as u32;
                    let q = cur / 256;
                    carry = cur % 256;
                    if !new_digits.is_empty() || q != 0 {
                        new_digits.push(q as u8);
                    }
                }
                out.push(carry as u8);
                digits = new_digits;
            }
            if out.is_empty() {
                out.push(0);
            }
            out.reverse();
            Some(BigUintLite(out))
        }
    }
}

fn parse_sig_component_to_32(raw: &str) -> Option<[u8; 32]> {
    let s = raw.trim();
    if let Some(rest) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        let mut bytes = hex::decode(rest).ok()?;
        if bytes.len() > 32 {
            return None;
        }
        if bytes.len() < 32 {
            let mut v = vec![0u8; 32 - bytes.len()];
            v.extend(bytes);
            bytes = v;
        }
        let mut out = [0u8; 32];
        out.copy_from_slice(&bytes);
        return Some(out);
    }
    let big = BigUintLite::parse_decimal(s)?;
    let mut bytes = big.0;
    if bytes.len() > 32 {
        return None;
    }
    if bytes.len() < 32 {
        let mut v = vec![0u8; 32 - bytes.len()];
        v.extend(bytes);
        bytes = v;
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Some(out)
}

/// Compute `sha256(serde_json(subtree) || salt)`. `subtree_json_bytes`
/// MUST be the exact output of `serde_json::to_vec(&value)` with
/// `preserve_order` on — any reorder breaks verification.
pub fn compute_msg_hash(subtree_json_bytes: &[u8], salt: &str) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(subtree_json_bytes);
    h.update(salt.as_bytes());
    let out = h.finalize();
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&out);
    arr
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum VerifyFailure {
    BadR,
    BadS,
    BadV,
    DeclaredHashMismatch,
    RecoverFailed,
    VerifyFailed,
}

/// Verify the signature recovered from `(r, s, v)` over `recovery_hash`
/// produced the given `pubkey`; then verify that same `(r, s)` over
/// `msg_hash` under that `pubkey`. The declared `recovery_hash` MUST
/// equal the locally-recomputed `msg_hash` — else the signer signed a
/// different preimage than the one we computed.
///
/// Returns `Ok((compact_sig_hex, pubkey_hex_uncompressed))` on success.
pub fn verify_ecdsa_recover(
    sig_v: &str,
    sig_r: &str,
    sig_s: &str,
    declared_hash_hex: &str,
    msg_hash: &[u8; 32],
) -> std::result::Result<(String, String), VerifyFailure> {
    let v_i: i32 = sig_v
        .trim()
        .parse::<i32>()
        .map_err(|_| VerifyFailure::BadV)?;
    let r_bytes = parse_sig_component_to_32(sig_r).ok_or(VerifyFailure::BadR)?;
    let s_bytes = parse_sig_component_to_32(sig_s).ok_or(VerifyFailure::BadS)?;

    // Normalize recovery id: accept raw 0/1 or eth-style 27/28.
    let recid = RecoveryId::from_i32(v_i)
        .or_else(|_| RecoveryId::from_i32(v_i - 27))
        .map_err(|_| VerifyFailure::BadV)?;

    // Parse the declared recovery hash.
    let declared_clean = declared_hash_hex.trim_start_matches("0x");
    let declared = hex::decode(declared_clean).map_err(|_| VerifyFailure::DeclaredHashMismatch)?;
    if declared.len() != 32 {
        return Err(VerifyFailure::DeclaredHashMismatch);
    }
    let mut declared_arr = [0u8; 32];
    declared_arr.copy_from_slice(&declared);
    if declared_arr != *msg_hash {
        return Err(VerifyFailure::DeclaredHashMismatch);
    }

    let mut sig_bytes = [0u8; 64];
    sig_bytes[..32].copy_from_slice(&r_bytes);
    sig_bytes[32..].copy_from_slice(&s_bytes);

    let secp = Secp256k1::new();
    let rsig = RecoverableSignature::from_compact(&sig_bytes, recid)
        .map_err(|_| VerifyFailure::RecoverFailed)?;
    let rec_msg =
        Message::from_digest_slice(&declared_arr).map_err(|_| VerifyFailure::RecoverFailed)?;
    let pubkey = secp
        .recover_ecdsa(&rec_msg, &rsig)
        .map_err(|_| VerifyFailure::RecoverFailed)?;

    let nsig = SecpSignature::from_compact(&sig_bytes).map_err(|_| VerifyFailure::VerifyFailed)?;
    let verify_msg =
        Message::from_digest_slice(msg_hash).map_err(|_| VerifyFailure::VerifyFailed)?;
    secp.verify_ecdsa(&verify_msg, &nsig, &pubkey)
        .map_err(|_| VerifyFailure::VerifyFailed)?;

    let compact_hex = hex::encode(sig_bytes);
    let pubkey_hex = hex::encode(pubkey.serialize_uncompressed());
    Ok((compact_hex, pubkey_hex))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    /// Raw on-chain redeem payload:
    /// `3c8f5e041b7b9ef8495d81d8374a42fa50a48a6d58814161cf743b7f2d8c01cai0`.
    /// Also covers the key property that `serde_json::to_string` with
    /// `preserve_order` produces the exact byte sequence we need.
    const REDEEM_JSON: &str = r#"{"p":"tap","op":"token-auth","redeem":{"items":[{"tick":"dmt-nat","amt":"10000000000","address":"bc1p2ydvl5tk2f4s2ag5ev0hh32akp0wh9m7vpzkud4mwmkr6slw5k7q7h5fhf"}],"auth":"db649630594091ec41073d615434dfc2d6c2ddfaae02e64d098ec4641429a5edi0","data":""},"sig":{"v":"1","r":"67727946467451808419717134227733026252135239491401711204566112435356193721554","s":"14230602951198490598305641885339349318631820657202163377062850709749802665983"},"hash":"ea887eee0a0771c7d8d7666db407ef72b5a921cc7e03377eca025717d23dd1b5","salt":"0.3889456278692911"}"#;

    #[test]
    fn live_redeem_verifies() {
        let v: Value = serde_json::from_str(REDEEM_JSON).unwrap();
        let redeem = v.get("redeem").unwrap();
        let salt = v.get("salt").unwrap().as_str().unwrap();
        let redeem_bytes = serde_json::to_vec(redeem).unwrap();
        let msg_hash = compute_msg_hash(&redeem_bytes, salt);
        let sig = v.get("sig").unwrap();
        let declared = v.get("hash").unwrap().as_str().unwrap();
        let result = verify_ecdsa_recover(
            sig.get("v").unwrap().as_str().unwrap(),
            sig.get("r").unwrap().as_str().unwrap(),
            sig.get("s").unwrap().as_str().unwrap(),
            declared,
            &msg_hash,
        );
        assert!(result.is_ok(), "verify failed: {:?}", result.err());
        let (compact, pub_hex) = result.unwrap();
        assert_eq!(compact.len(), 128);
        assert_eq!(pub_hex.len(), 130); // uncompressed
    }

    /// Live authority-create payload referenced by the redeem above.
    /// Verifies create-form signatures over the `auth` array.
    const AUTH_CREATE_JSON: &str = r#"{"p":"tap","op":"token-auth","auth":[],"sig":{"v":"1","r":"51114599000453674123197239793836480107526848033552783873067622024011080413763","s":"50685103394885756609589123136185143207568002823354595196258972188535791695356"},"hash":"7cba3150c0292b63378c64182eec132ac3354039eda4db6cf9b4b81aecfc36e7","salt":"0.9042142468466474"}"#;

    #[test]
    fn live_create_verifies() {
        let v: Value = serde_json::from_str(AUTH_CREATE_JSON).unwrap();
        let auth = v.get("auth").unwrap();
        let salt = v.get("salt").unwrap().as_str().unwrap();
        let bytes = serde_json::to_vec(auth).unwrap();
        let msg_hash = compute_msg_hash(&bytes, salt);
        let sig = v.get("sig").unwrap();
        let declared = v.get("hash").unwrap().as_str().unwrap();
        let r = verify_ecdsa_recover(
            sig.get("v").unwrap().as_str().unwrap(),
            sig.get("r").unwrap().as_str().unwrap(),
            sig.get("s").unwrap().as_str().unwrap(),
            declared,
            &msg_hash,
        );
        assert!(r.is_ok(), "create verify failed: {:?}", r.err());
    }

    #[test]
    fn tamper_hash_detected() {
        let v: Value = serde_json::from_str(REDEEM_JSON).unwrap();
        let redeem = v.get("redeem").unwrap();
        let salt = v.get("salt").unwrap().as_str().unwrap();
        let redeem_bytes = serde_json::to_vec(redeem).unwrap();
        let msg_hash = compute_msg_hash(&redeem_bytes, salt);
        let sig = v.get("sig").unwrap();
        // Corrupt declared hash.
        let bad = "0000000000000000000000000000000000000000000000000000000000000000";
        let r = verify_ecdsa_recover(
            sig.get("v").unwrap().as_str().unwrap(),
            sig.get("r").unwrap().as_str().unwrap(),
            sig.get("s").unwrap().as_str().unwrap(),
            bad,
            &msg_hash,
        );
        assert!(matches!(r, Err(VerifyFailure::DeclaredHashMismatch)));
    }

    #[test]
    fn tamper_msg_detected() {
        let v: Value = serde_json::from_str(REDEEM_JSON).unwrap();
        let sig = v.get("sig").unwrap();
        let declared = v.get("hash").unwrap().as_str().unwrap();
        let mut bad = [0u8; 32];
        bad[0] = 0xff;
        let r = verify_ecdsa_recover(
            sig.get("v").unwrap().as_str().unwrap(),
            sig.get("r").unwrap().as_str().unwrap(),
            sig.get("s").unwrap().as_str().unwrap(),
            declared,
            &bad,
        );
        // declared != bad, so fails at the declared-hash check.
        assert!(matches!(r, Err(VerifyFailure::DeclaredHashMismatch)));
    }

    #[test]
    fn decimal_r_parses() {
        // 32-byte decimal component parsing (matches the live vectors).
        let got = parse_sig_component_to_32(
            "51114599000453674123197239793836480107526848033552783873067622024011080413763",
        )
        .unwrap();
        assert_eq!(got.len(), 32);
    }

    #[test]
    fn hex_r_parses() {
        let got = parse_sig_component_to_32(
            "0x0102030405060708010203040506070801020304050607080102030405060708",
        )
        .unwrap();
        assert_eq!(got[0], 0x01);
    }
}
