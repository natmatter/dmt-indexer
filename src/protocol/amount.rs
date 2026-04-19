//! Integer amount parsing for TAP operations.
//!
//! DMT deployments in v0.1.0 use integer-only amounts (`dt: "n"` or
//! missing `dt`). We parse into `u128`; nothing in TAP overflows this.

use crate::error::{Error, Result};

pub fn parse_amount(raw: &str) -> Result<u128> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || !trimmed.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Error::Protocol(format!("amount not integer: {raw:?}")));
    }
    trimmed
        .parse::<u128>()
        .map_err(|_| Error::Protocol(format!("amount overflow: {raw}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_integer() {
        assert_eq!(parse_amount("1000").unwrap(), 1000);
    }

    #[test]
    fn leading_zeros_ok() {
        assert_eq!(parse_amount("0000123").unwrap(), 123);
    }

    #[test]
    fn rejects_negative() {
        assert!(parse_amount("-1").is_err());
    }

    #[test]
    fn rejects_decimal() {
        assert!(parse_amount("1.5").is_err());
    }

    #[test]
    fn rejects_internal_whitespace() {
        assert!(parse_amount("1 000").is_err());
    }

    #[test]
    fn u128_max_ok() {
        assert_eq!(parse_amount(&u128::MAX.to_string()).unwrap(), u128::MAX);
    }

    #[test]
    fn overflow_rejected() {
        assert!(parse_amount(&format!("{}0", u128::MAX)).is_err());
    }
}
