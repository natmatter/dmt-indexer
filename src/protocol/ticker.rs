//! Ticker normalization and length validation.
//!
//! Per TAP, tickers are case-insensitive. Length rules depend on
//! inscription height:
//!
//! - Before block `861,576`: length ∈ {3} ∪ [5, 32]. Four-character
//!   tickers are reserved.
//! - At or after block `861,576`: length ∈ [1, 32].

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

pub const TICKER_UNLOCK_HEIGHT: u64 = 861_576;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct NormalizedTicker(String);

impl NormalizedTicker {
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for NormalizedTicker {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NormalizedTicker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Case-fold to lowercase and reject obviously-malformed tickers. The
/// height-conditional length rule is applied separately by
/// [`ticker_is_valid_at_height`].
pub fn normalize_ticker(raw: &str) -> Result<NormalizedTicker> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(Error::Protocol("ticker empty".into()));
    }
    let char_count = trimmed.chars().count();
    if char_count > 32 {
        return Err(Error::Protocol(format!(
            "ticker too long ({char_count} chars, max 32)"
        )));
    }
    if trimmed
        .chars()
        .any(|c| c.is_control() || c.is_whitespace() || c == '"')
    {
        return Err(Error::Protocol(
            "ticker has whitespace / control / quote chars".into(),
        ));
    }
    Ok(NormalizedTicker(trimmed.to_lowercase()))
}

/// Height-conditional length rule. Returns `true` if the ticker is
/// valid for a deploy inscribed at `inscribed_height`.
pub fn ticker_is_valid_at_height(ticker: &NormalizedTicker, inscribed_height: u64) -> bool {
    let len = ticker.as_str().chars().count();
    if len == 0 || len > 32 {
        return false;
    }
    if inscribed_height >= TICKER_UNLOCK_HEIGHT {
        true
    } else {
        len == 3 || (5..=32).contains(&len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lowercases() {
        assert_eq!(normalize_ticker("NAT").unwrap().as_str(), "nat");
    }

    #[test]
    fn rejects_four_char_before_unlock() {
        let t = normalize_ticker("abcd").unwrap();
        assert!(!ticker_is_valid_at_height(&t, TICKER_UNLOCK_HEIGHT - 1));
    }

    #[test]
    fn accepts_four_char_after_unlock() {
        let t = normalize_ticker("abcd").unwrap();
        assert!(ticker_is_valid_at_height(&t, TICKER_UNLOCK_HEIGHT));
    }

    #[test]
    fn accepts_single_char_after_unlock() {
        let t = normalize_ticker("x").unwrap();
        assert!(ticker_is_valid_at_height(&t, TICKER_UNLOCK_HEIGHT));
        assert!(!ticker_is_valid_at_height(&t, TICKER_UNLOCK_HEIGHT - 1));
    }

    #[test]
    fn rejects_empty() {
        assert!(normalize_ticker("").is_err());
    }

    #[test]
    fn rejects_whitespace_inside() {
        assert!(normalize_ticker("na t").is_err());
    }

    #[test]
    fn rejects_too_long() {
        assert!(normalize_ticker(&"a".repeat(33)).is_err());
    }
}
