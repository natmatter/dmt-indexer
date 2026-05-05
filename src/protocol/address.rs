//! Address normalization + script → address decoding.

use bitcoin::{Address, Network, ScriptBuf};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct NormalizedAddress(String);

impl NormalizedAddress {
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for NormalizedAddress {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NormalizedAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

pub fn normalize_address(raw: &str) -> Option<NormalizedAddress> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let canonical = if is_bech32_prefix(trimmed) {
        trimmed.to_ascii_lowercase()
    } else {
        trimmed.to_string()
    };
    Some(NormalizedAddress(canonical))
}

/// ord-tap compatible address gate for payload-recipient fields.
///
/// Before TAP_TESTNET_FIX_ACTIVATION_HEIGHT, ord-tap accepts any Bitcoin
/// network if the address shape is one of the supported script types.
/// At/after the activation it requires mainnet on mainnet indexing.
pub fn is_valid_tap_address_at_height(raw: &str, height: u64) -> bool {
    const TAP_TESTNET_FIX_ACTIVATION_HEIGHT: u64 = 916_233;
    let Ok(parsed) = raw.parse::<Address<bitcoin::address::NetworkUnchecked>>() else {
        return false;
    };
    let main_ok = parsed.clone().require_network(Network::Bitcoin).is_ok();
    let any_net_ok = main_ok
        || parsed.clone().require_network(Network::Testnet).is_ok()
        || parsed.clone().require_network(Network::Signet).is_ok()
        || parsed.clone().require_network(Network::Regtest).is_ok();
    let net_ok = if height < TAP_TESTNET_FIX_ACTIVATION_HEIGHT {
        any_net_ok
    } else {
        main_ok
    };
    if !net_ok {
        return false;
    }
    let spk = parsed.assume_checked_ref().script_pubkey();
    let b = spk.as_bytes();
    let is_p2wpkh = b.len() == 22 && b[0] == 0x00 && b[1] == 0x14;
    let is_p2wsh = b.len() == 34 && b[0] == 0x00 && b[1] == 0x20;
    let is_p2tr = b.len() == 34 && b[0] == 0x51 && b[1] == 0x20;
    let is_p2pkh = b.len() == 25
        && b[0] == 0x76
        && b[1] == 0xa9
        && b[2] == 0x14
        && b[23] == 0x88
        && b[24] == 0xac;
    let is_p2sh = b.len() == 23 && b[0] == 0xa9 && b[1] == 0x14 && b[22] == 0x87;
    is_p2wpkh || is_p2wsh || is_p2tr || is_p2pkh || is_p2sh
}

fn is_bech32_prefix(address: &str) -> bool {
    let lowered = address.to_ascii_lowercase();
    lowered.starts_with("bc1") || lowered.starts_with("tb1") || lowered.starts_with("bcrt1")
}

/// Decode a mainnet script to canonical address string if possible.
/// Returns `None` for OP_RETURN / unparseable scripts.
pub fn address_from_script(script: &bitcoin::Script) -> Option<String> {
    let buf: ScriptBuf = script.to_owned().into();
    Address::from_script(&buf, Network::Bitcoin)
        .ok()
        .map(|a| a.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lowercases_mainnet_bech32() {
        let a = normalize_address("BC1QW508D6QEJXTDG4Y5R3ZARVARY0C5XW7KV8F3T4").unwrap();
        assert_eq!(a.as_str(), "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4");
    }

    #[test]
    fn leaves_base58_untouched() {
        let a = normalize_address("  1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2  ").unwrap();
        assert_eq!(a.as_str(), "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2");
    }

    #[test]
    fn empty_returns_none() {
        assert!(normalize_address("   ").is_none());
    }
}
