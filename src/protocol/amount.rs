//! Numeric-string parsing for TAP operations.
//!
//! Mirrors ord-tap's `resolve_number_string` (see
//! `/tmp/ot_mod.rs:134-160`). The on-chain payload carries `amt` as a
//! string; the effective integer amount is derived by scaling to the
//! deployment's decimal count.
//!
//! Rules (NAT uses `decimals = 0`):
//!   - `is_valid_number`: digits and at most one `.` — no sign, no
//!     exponent, no whitespace (not even leading/trailing).
//!   - Fractional part is right-padded with zeros to `decimals`
//!     places and then TRUNCATED at `decimals`. Digits past the
//!     decimal place are discarded.
//!   - Integer "0" prefix is dropped before concat; after concat,
//!     leading zeros are stripped (so `"00100"` → `"100"`,
//!     `"0.0"` → `"0"`).
//!
//! For NAT (dec=0) this means `"123"` → `"123"`, `"0123"` → `"123"`,
//! `"123.999"` → `"123"` (truncated), `"0.5"` → `"0"`, `"1e9"` →
//! rejected. v2's earlier `parse_amount` rejected any `.` and
//! admitted whitespace-padded strings — both diverged from ord-tap.

use crate::error::{Error, Result};

pub fn parse_amount(raw: &str) -> Result<u128> {
    let resolved = resolve_number_string(raw, 0)
        .ok_or_else(|| Error::Protocol(format!("amount not a valid number: {raw:?}")))?;
    resolved
        .parse::<u128>()
        .map_err(|_| Error::Protocol(format!("amount overflow: {raw}")))
}

/// Strict number-string validator: only ASCII digits and at most one
/// `.`. Empty string is considered valid (matches ord-tap's helper,
/// though callers below reject the empty output).
fn is_valid_number(s: &str) -> bool {
    let mut seen_dot = false;
    for c in s.chars() {
        if c.is_ascii_digit() {
            continue;
        }
        if c == '.' && !seen_dot {
            seen_dot = true;
            continue;
        }
        return false;
    }
    true
}

/// Direct port of ord-tap's `resolve_number_string`. Returns `None` for
/// malformed input; otherwise the canonical digit-string representation
/// of the integer amount scaled to `decimals`.
pub fn resolve_number_string(num: &str, decimals: u32) -> Option<String> {
    if !is_valid_number(num) {
        return None;
    }
    let mut parts = num.split('.');
    let int_part = parts.next().unwrap_or("");
    let mut frac_part = parts.next().unwrap_or("").to_string();
    if parts.next().is_some() {
        return None;
    }
    if frac_part.len() < decimals as usize {
        frac_part.extend(std::iter::repeat('0').take(decimals as usize - frac_part.len()));
    }
    let frac_trunc: String = frac_part.chars().take(decimals as usize).collect();
    let mut number = String::new();
    if int_part != "0" {
        number.push_str(int_part);
    }
    number.push_str(&frac_trunc);
    let is_zero = number.is_empty() || number.chars().all(|c| c == '0');
    if is_zero {
        return Some("0".to_string());
    }
    // Strip leading zeros.
    match number.find(|c: char| c != '0') {
        Some(0) => Some(number),
        Some(i) => Some(number[i..].to_string()),
        None => Some("0".to_string()),
    }
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

    // Ord-tap parity: `"1.5"` with dec=0 truncates to `"1"`, doesn't reject.
    #[test]
    fn decimal_truncated_at_dec_zero() {
        assert_eq!(parse_amount("1.5").unwrap(), 1);
    }

    #[test]
    fn decimal_zero_point_five_is_zero() {
        assert_eq!(parse_amount("0.5").unwrap(), 0);
    }

    #[test]
    fn rejects_exponent() {
        assert!(parse_amount("1e9").is_err());
    }

    #[test]
    fn rejects_internal_whitespace() {
        assert!(parse_amount("1 000").is_err());
    }

    #[test]
    fn rejects_leading_whitespace() {
        assert!(parse_amount(" 1000").is_err());
    }

    #[test]
    fn u128_max_ok() {
        assert_eq!(parse_amount(&u128::MAX.to_string()).unwrap(), u128::MAX);
    }

    #[test]
    fn overflow_rejected() {
        assert!(parse_amount(&format!("{}0", u128::MAX)).is_err());
    }

    // Pure ord-tap parity probes on the helper.
    #[test]
    fn resolve_dec0_truncates() {
        assert_eq!(resolve_number_string("123.456", 0).as_deref(), Some("123"));
    }

    #[test]
    fn resolve_dec2_pads() {
        assert_eq!(resolve_number_string("1", 2).as_deref(), Some("100"));
    }

    #[test]
    fn resolve_dec2_truncates_and_strips() {
        assert_eq!(resolve_number_string("0.12345", 2).as_deref(), Some("12"));
    }

    #[test]
    fn resolve_all_zero_canonicalizes() {
        assert_eq!(resolve_number_string("0.000", 0).as_deref(), Some("0"));
        assert_eq!(resolve_number_string("000", 0).as_deref(), Some("0"));
    }

    #[test]
    fn resolve_rejects_two_dots() {
        assert!(resolve_number_string("1.2.3", 0).is_none());
    }
}
