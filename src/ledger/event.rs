//! Ledger event type — the single row we write to the `events` table.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventFamily {
    Deploy,
    Mint,
    Transfer,
    Control,
    Coinbase,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    DmtDeployRegistered,
    DmtMintCredit,
    DmtMintDuplicateRejected,
    DmtMintIgnoredPostActivation,
    TokenTransferInscribeAdmitted,
    TokenTransferSkippedSemantic,
    TokenTransferDebit,
    TokenTransferCredit,
    TokenTransferBurned,
    /// `token-send` inscription observed and stored as pending.
    TokenSendInscribeAdmitted,
    /// Parsing/validation OK but the inscribe was dropped (e.g. unknown
    /// ticker, miner-reward shield). Emitted once at inscribe time.
    TokenSendInscribeRejected,
    /// One item in a tapped `token-send` moved balance: sender lost
    /// `delta_total` of the ticker, recipient gained it. Emitted
    /// per-item (one inscription → one event per successful item).
    TokenSendDebit,
    TokenSendCredit,
    /// One item in a tapped `token-send` failed its availability check
    /// at tap time. Other items in the same accumulator may still have
    /// succeeded — each item is independent per TAP spec.
    TokenSendSkipped,
    /// `token-trade` inscription observed. We parse side/tick and
    /// persist the accumulator for diagnostic/audit visibility; the
    /// full atomic-swap execute-path is not yet implemented, so no
    /// balance change follows. Emitted once per inscribe.
    TokenTradeInscribeAdmitted,
    /// `token-trade` move seen but execute-path is unimplemented;
    /// would have debited/credited per spec. Emitted once per move.
    TokenTradeNotExecuted,
    /// `token-auth` inscription observed (create-auth OR redeem).
    /// Full execute-path requires secp256k1 signature verification;
    /// deferred. Emitted once per inscribe.
    TokenAuthInscribeAdmitted,
    /// `token-auth` move seen but execute-path is unimplemented.
    TokenAuthNotExecuted,
    BlockTransferablesInscribed,
    BlockTransferablesTapped,
    UnblockTransferablesInscribed,
    UnblockTransferablesTapped,
    CoinbaseRewardCredit,
    CoinbaseRewardBurned,
    CoinbaseRewardLocked,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::DmtDeployRegistered => "dmt_deploy_registered",
            Self::DmtMintCredit => "dmt_mint_credit",
            Self::DmtMintDuplicateRejected => "dmt_mint_duplicate_rejected",
            Self::DmtMintIgnoredPostActivation => "dmt_mint_ignored_post_activation",
            Self::TokenTransferInscribeAdmitted => "token_transfer_inscribe_admitted",
            Self::TokenTransferSkippedSemantic => "token_transfer_skipped_semantic",
            Self::TokenTransferDebit => "token_transfer_debit",
            Self::TokenTransferCredit => "token_transfer_credit",
            Self::TokenTransferBurned => "token_transfer_burned",
            Self::TokenSendInscribeAdmitted => "token_send_inscribe_admitted",
            Self::TokenSendInscribeRejected => "token_send_inscribe_rejected",
            Self::TokenSendDebit => "token_send_debit",
            Self::TokenSendCredit => "token_send_credit",
            Self::TokenSendSkipped => "token_send_skipped",
            Self::TokenTradeInscribeAdmitted => "token_trade_inscribe_admitted",
            Self::TokenTradeNotExecuted => "token_trade_not_executed",
            Self::TokenAuthInscribeAdmitted => "token_auth_inscribe_admitted",
            Self::TokenAuthNotExecuted => "token_auth_not_executed",
            Self::BlockTransferablesInscribed => "block_transferables_inscribed",
            Self::BlockTransferablesTapped => "block_transferables_tapped",
            Self::UnblockTransferablesInscribed => "unblock_transferables_inscribed",
            Self::UnblockTransferablesTapped => "unblock_transferables_tapped",
            Self::CoinbaseRewardCredit => "coinbase_reward_credit",
            Self::CoinbaseRewardBurned => "coinbase_reward_burned",
            Self::CoinbaseRewardLocked => "coinbase_reward_locked",
        }
    }
}

/// Signed-integer balance deltas per event. `i128` is enough headroom
/// for any realistic sum across all DMT tokens.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct EventDelta {
    pub delta_total: i128,
    pub delta_available: i128,
    pub delta_transferable: i128,
    pub delta_burned: i128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerEvent {
    pub ticker: String,
    pub event_id: u64,
    pub event_key: String,
    pub family: EventFamily,
    pub event_type: EventType,
    pub block_height: u64,
    pub block_hash: String,
    pub tx_index: Option<u32>,
    pub txid: Option<String>,
    pub inscription_id: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub address: Option<String>,
    pub counterparty_address: Option<String>,
    pub delta: EventDelta,
    pub source_kind: String,
    pub metadata: serde_json::Value,
}
