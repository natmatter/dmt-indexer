//! Bits-to-amount decoding.
//!
//! NAT's element decodes `nBits` (the Bitcoin header's compact
//! difficulty target) as its numeric u32 value. A prior version of
//! this indexer treated the bits field as a hex string and parsed it
//! as decimal, returning 0 when it contained any a-f character. That
//! "scarcity" rule is not in the DMT spec and caused ~90% of valid
//! mints to be silently dropped.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BitsMode {
    RawHex,
}

impl BitsMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::RawHex => "raw_hex",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "raw_hex" | "text_over_hex" => Some(Self::RawHex),
            _ => None,
        }
    }
}

pub fn decode_bits(bits: u32, mode: BitsMode) -> Result<u128> {
    match mode {
        BitsMode::RawHex => Ok(u128::from(bits)),
    }
}

/// Wrapper that maps any unknown mode string to a protocol error. Used
/// in deploy parsing.
pub fn parse_mode(value: &str) -> Result<BitsMode> {
    BitsMode::parse(value).ok_or_else(|| Error::Protocol(format!("unknown bits mode: {value}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_hex_is_u32_value() {
        assert_eq!(
            decode_bits(0x17034220, BitsMode::RawHex).unwrap(),
            0x17034220
        );
    }

    #[test]
    fn letters_in_bits_are_not_scarcity() {
        // Previously this returned 0 under the removed "text_over_hex"
        // scarcity rule. The correct behavior is the u32 numeric value.
        assert_eq!(
            decode_bits(0x1801d854, BitsMode::RawHex).unwrap(),
            0x1801d854
        );
    }

    #[test]
    fn legacy_mode_string_still_parses_as_raw_hex() {
        assert_eq!(BitsMode::parse("text_over_hex"), Some(BitsMode::RawHex));
    }
}
