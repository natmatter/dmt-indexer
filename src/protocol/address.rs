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
