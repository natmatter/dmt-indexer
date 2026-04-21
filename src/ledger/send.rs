//! Per-item `token-send` execution.
//!
//! Called from the scan-loop move handler when a registered
//! `InscriptionKind::TokenSend` carrier is spent. Enforces the
//! self-tap rule (creator == new_owner) at the call site, then
//! iterates items in payload order and invokes the shared
//! [`exec_internal_send_one`] primitive per item.
//!
//! Mirrors ord-tap's `index_token_send_executed` at
//! `/tmp/ot_send.rs:75-115`.

use chrono::{DateTime, Utc};

use crate::error::Result;
use crate::ledger::primitives::{exec_internal_send_one, SendOutcome};

/// Outcome of one token-send item's execution.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SendItemOutcome {
    Sent,
    SelfSend,
    Skipped,
}

/// Execute one token-send item against the WALLET_STATE table. Wrapper
/// over the shared primitive so the call site only needs a semantic
/// enum to drive event emission.
pub fn execute_send_item(
    wtx: &redb::WriteTransaction,
    ticker: &str,
    from: &str,
    to: &str,
    amount: u128,
    occurred_at: DateTime<Utc>,
) -> Result<SendItemOutcome> {
    match exec_internal_send_one(wtx, ticker, from, to, amount, occurred_at)? {
        SendOutcome::Sent => Ok(SendItemOutcome::Sent),
        SendOutcome::SelfSend => Ok(SendItemOutcome::SelfSend),
        SendOutcome::InsufficientAvailable { .. } => Ok(SendItemOutcome::Skipped),
    }
}
