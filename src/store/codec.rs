//! JSON codec helpers for redb values.
//!
//! redb stores raw bytes; we encode each value as JSON. JSON is a bit
//! heavier than bincode but makes the on-disk format forward-compatible
//! with schema evolution (we can add new optional fields) and makes
//! debugging trivial (`redb::inspect` returns human-readable bytes).
//!
//! For hot paths (wallet_state, activity_recent), we key on `&str`
//! tuples serialized as length-prefixed bytes so range scans stay
//! efficient; values are JSON blobs.

use serde::{de::DeserializeOwned, Serialize};

use crate::error::{Error, Result};

pub fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(v).map_err(|e| Error::Store(format!("encode: {e}")))
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|e| Error::Store(format!("decode: {e}")))
}

/// Inverted balance: for a descending leaderboard index we key on
/// `(ticker, inv_balance_bytes, address)`. We use `u128::MAX - balance`
/// so lexicographic big-endian ordering yields balances DESC.
pub fn inverted_balance(balance: u128) -> [u8; 16] {
    let inv = u128::MAX - balance;
    inv.to_be_bytes()
}
