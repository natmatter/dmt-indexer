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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DmtElementPayload {
    pub name: String,
    pub pattern: Option<String>,
    pub field: ElementField,
    pub field_token: String,
}

/// Parse a raw DMT element inscription (`name.field.element` or
/// `name.pattern.field.element`). ord-tap indexes these as plain text
/// inscriptions, not TAP JSON envelopes.
pub fn parse_dmt_element(body: &str) -> Result<Option<DmtElementPayload>> {
    let trimmed = body.trim();
    let lower = trimmed.to_lowercase();
    if !lower.ends_with(".element") {
        return Ok(None);
    }
    let parts: Vec<&str> = trimmed.split('.').collect();
    if parts.len() < 3 {
        return Ok(None);
    }
    let element_tag = parts[parts.len() - 1];
    if element_tag != "element" {
        return Ok(None);
    }
    let name = parts[0].to_lowercase();
    if name.is_empty()
        || name.chars().any(|c| {
            matches!(
                c,
                '/' | '.'
                    | '['
                    | ']'
                    | '{'
                    | '}'
                    | ':'
                    | ';'
                    | '"'
                    | '\''
                    | ' '
                    | '\t'
                    | '\n'
                    | '\r'
            )
        })
    {
        return Ok(None);
    }
    let field_token = parts[parts.len() - 2].to_string();
    let parsed_field = field_token
        .parse::<i64>()
        .map_err(|_| Error::Protocol(format!("invalid element field: {field_token}")))?;
    if parsed_field < 0 {
        return Ok(None);
    }
    let field = ElementField::from_token(&parsed_field.to_string())?;
    let pattern = if parts.len() > 3 {
        let p = parts[1..parts.len() - 2].join(".");
        if p.is_empty() {
            None
        } else {
            // Rust regex is close enough for the RE2 acceptance check
            // ord-tap performs here, and the same engine is used later
            // for counting pattern matches.
            regex::Regex::new(&p).map_err(|e| Error::Protocol(format!("invalid pattern: {e}")))?;
            Some(p)
        }
    } else {
        None
    };
    Ok(Some(DmtElementPayload {
        name,
        pattern,
        field,
        field_token,
    }))
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
