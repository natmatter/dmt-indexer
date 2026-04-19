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
