//! DMT element field registry.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ElementField {
    BlockHeight,
    Nonce,
    Bits,
}

pub const KNOWN_ELEMENTS: &[ElementField] = &[
    ElementField::BlockHeight,
    ElementField::Nonce,
    ElementField::Bits,
];

impl ElementField {
    pub fn from_token(token: &str) -> Result<Self> {
        match token {
            "4" => Ok(Self::BlockHeight),
            "10" => Ok(Self::Nonce),
            "11" => Ok(Self::Bits),
            other => Err(Error::Protocol(format!("unknown element field: {other}"))),
        }
    }

    pub fn token(&self) -> &'static str {
        match self {
            Self::BlockHeight => "4",
            Self::Nonce => "10",
            Self::Bits => "11",
        }
    }
}

/// Parse an element inscription body (`name.field.element`).
///
/// NAT uses `dmt.11.element`. We accept any three-segment body whose
/// last segment is `element` and whose second-to-last is a known
/// DMT field token (4/10/11). Pattern-carrying elements are possible
/// per DMT spec but dmt-indexer v0.1.0 is NAT-only and NAT's element
/// has no pattern, so we don't index pattern-based tokens here.
pub fn parse_element_body(body: &str) -> Result<ElementField> {
    let trimmed = body.trim();
    let segments: Vec<&str> = trimmed.split('.').collect();
    if segments.len() < 3 {
        return Err(Error::Protocol(format!(
            "malformed element body (too short): {trimmed}"
        )));
    }
    if segments[segments.len() - 1] != "element" {
        return Err(Error::Protocol(format!(
            "malformed element body (no `.element` suffix): {trimmed}"
        )));
    }
    ElementField::from_token(segments[segments.len() - 2])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nat_element() {
        assert_eq!(
            parse_element_body("dmt.11.element").unwrap(),
            ElementField::Bits
        );
    }

    #[test]
    fn rejects_unknown_field() {
        assert!(parse_element_body("dmt.7.element").is_err());
    }

    #[test]
    fn rejects_no_suffix() {
        assert!(parse_element_body("dmt.11").is_err());
    }

    #[test]
    fn rejects_too_short() {
        assert!(parse_element_body("element").is_err());
    }

    #[test]
    fn round_trip_tokens() {
        for f in KNOWN_ELEMENTS {
            assert_eq!(ElementField::from_token(f.token()).unwrap(), *f);
        }
    }
}
