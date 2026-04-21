//! Per-item `token-auth` redeem execution.
//!
//! The balance-moving path for token-auth redeems. Mirrors ord-tap's
//! redeem branch of `index_token_auth_created`
//! (`/tmp/ot_auth.rs:42-102`): after authority lookup + signature
//! verification + replay/cancel/whitelist guards, iterate the redeem
//! items and execute each via [`exec_internal_send_one`].

use chrono::{DateTime, Utc};

use crate::error::Result;
use crate::ledger::primitives::{exec_internal_send_one, SendOutcome};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RedeemItemOutcome {
    Credited,
    SelfSend,
    Skipped,
}

pub fn execute_redeem_item(
    wtx: &redb::WriteTransaction,
    ticker: &str,
    from_authority: &str,
    to: &str,
    amount: u128,
    occurred_at: DateTime<Utc>,
) -> Result<RedeemItemOutcome> {
    match exec_internal_send_one(wtx, ticker, from_authority, to, amount, occurred_at)? {
        SendOutcome::Sent => Ok(RedeemItemOutcome::Credited),
        SendOutcome::SelfSend => Ok(RedeemItemOutcome::SelfSend),
        SendOutcome::InsufficientAvailable { .. } => Ok(RedeemItemOutcome::Skipped),
    }
}
