//! All redb table definitions.
//!
//! Key conventions:
//! - tickers are always lowercase-normalized strings
//! - addresses are bech32-lowercased or base58-untouched
//! - u64 sort keys use big-endian bytes so lexicographic order matches
//!   numeric order (or inverted for DESC via u64::MAX - x)

use chrono::{DateTime, Utc};
#[allow(unused_imports)]
use redb::ReadableTable;
use redb::{TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// Cursor / global metadata. Keyed by string label ("cursor_protocol_scan").
pub const META: TableDefinition<'static, &str, &[u8]> = TableDefinition::new("meta");

/// Deployments: key = ticker, value = JSON Deployment.
pub const DEPLOYMENTS: TableDefinition<'static, &str, &[u8]> = TableDefinition::new("deployments");

/// Events: key = (ticker, event_id big-endian u64), value = JSON LedgerEvent.
pub const EVENTS: TableDefinition<'static, (&str, u64), &[u8]> = TableDefinition::new("events");

/// Wallet state: key = (ticker, address), value = JSON WalletState.
pub const WALLET_STATE: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("wallet_state");

/// Secondary index for leaderboard: key = (ticker, inv_balance_be, address).
pub const BALANCES_BY_VALUE: TableDefinition<'static, (&str, &[u8], &str), u64> =
    TableDefinition::new("balances_by_value");

/// Mint claims: key = (ticker, block_number), value = JSON MintClaim.
pub const MINT_CLAIMS: TableDefinition<'static, (&str, u64), &[u8]> =
    TableDefinition::new("mint_claims");

/// Valid token-transfer inscribes: key = inscription_id, value = JSON ValidTransfer.
pub const VALID_TRANSFERS: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("valid_transfers");

/// Secondary index for `/wallets/:addr/transferables`: per-sender set
/// of UNSETTLED transfer inscription ids. Inserts happen on admit,
/// removes on settle / burn. Keyed `(ticker, sender, inscription_id)`
/// so a single narrow range scan returns a wallet's listable inventory
/// without scanning the full `valid_transfers` table.
pub const TRANSFERABLES_BY_SENDER: TableDefinition<'static, (&str, &str, &str), u8> =
    TableDefinition::new("transferables_by_sender");

/// Pending control inscriptions (pre-tap): key = inscription_id, value = JSON PendingControl.
pub const PENDING_CONTROLS: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("pending_controls");

/// Addresses that have ever received a DMT coinbase reward credit at or
/// after the miner-reward-shield activation height. Permanent marker —
/// set once on first post-activation credit and never removed, even if
/// the owner later inscribes `unblock-transferables`. Used by the
/// transfer-execution shield (height >= 942,002) to void outstanding
/// transfers from miners who slipped past the create-time bltr check
/// by unblocking after inscribing. Mirrors ord-tap's `dmtrwd/<addr>`.
/// Value byte is unused; presence of the key is the signal.
pub const DMT_REWARD_ADDRESSES: TableDefinition<'static, &str, u8> =
    TableDefinition::new("dmt_reward_addresses");

/// Cumulative mint + coinbase issuance per ticker, encoded as the
/// `u128` total in LE bytes. Incremented on every admitted mint and
/// coinbase credit; the mint resolver clamps each candidate's amount
/// so `cumulative + amount <= max_supply` per the deploy's `max`
/// field. Mirrors ord-tap's `dc/<tick>` "tokens_left" counter.
pub const MINT_TOTALS: TableDefinition<'static, &str, &[u8]> = TableDefinition::new("mint_totals");

/// Carrier map: which outpoint currently carries which inscription.
/// Keyed by `{txid}:{vout}`, value = JSON InscriptionOwner.
pub const INSCRIPTION_OWNERS: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("inscription_owners");

/// Daily per-deployment rollups: key = (ticker, day_bucket_be_u32), value = JSON DailyStats.
pub const DAILY_STATS: TableDefinition<'static, (&str, u32), &[u8]> =
    TableDefinition::new("daily_stats");

/// Per-(ticker, day) set of addresses touched. Lets us maintain
/// `DailyStats.active_addresses` without iterating events.
pub const DAILY_ACTIVE_ADDRESSES: TableDefinition<'static, (&str, u32, &str), u8> =
    TableDefinition::new("daily_active_addresses");

/// Recent activity feed: key = (ticker, inv_occurred_at_be_u64, event_id), value = JSON RecentActivity.
pub const ACTIVITY_RECENT: TableDefinition<'static, (&str, u64, u64), &[u8]> =
    TableDefinition::new("activity_recent");

/// Per-wallet activity feed.
pub const WALLET_ACTIVITY: TableDefinition<'static, (&str, &str, u64, u64), &[u8]> =
    TableDefinition::new("wallet_activity");

/// Inscription lookup table — used by `/inscriptions/:id`.
/// Key = inscription_id, value = JSON InscriptionIndex entry.
pub const INSCRIPTIONS: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("inscriptions");

/// Sync stats — updated per block for diagnostics / /metrics.
pub const STATS: TableDefinition<'static, &str, &[u8]> = TableDefinition::new("stats");

pub fn init_all(tx: &WriteTransaction) -> Result<()> {
    let _ = tx.open_table(META)?;
    let _ = tx.open_table(DEPLOYMENTS)?;
    let _ = tx.open_table(EVENTS)?;
    let _ = tx.open_table(WALLET_STATE)?;
    let _ = tx.open_table(BALANCES_BY_VALUE)?;
    let _ = tx.open_table(MINT_CLAIMS)?;
    let _ = tx.open_table(VALID_TRANSFERS)?;
    let _ = tx.open_table(PENDING_CONTROLS)?;
    let _ = tx.open_table(INSCRIPTION_OWNERS)?;
    let _ = tx.open_table(DAILY_STATS)?;
    let _ = tx.open_table(ACTIVITY_RECENT)?;
    let _ = tx.open_table(WALLET_ACTIVITY)?;
    let _ = tx.open_table(INSCRIPTIONS)?;
    let _ = tx.open_table(STATS)?;
    let _ = tx.open_table(DAILY_ACTIVE_ADDRESSES)?;
    let _ = tx.open_table(TRANSFERABLES_BY_SENDER)?;
    let _ = tx.open_table(DMT_REWARD_ADDRESSES)?;
    let _ = tx.open_table(MINT_TOTALS)?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Cursor {
    pub height: u64,
    pub block_hash: String,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WalletState {
    pub total: i128,
    pub available: i128,
    pub transferable: i128,
    pub burned: i128,
    pub transferables_blocked: bool,
    pub first_activity: Option<DateTime<Utc>>,
    pub last_activity: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MintClaim {
    pub winning_inscription_id: String,
    pub winning_inscription_number: i64,
    pub inscribed_height: u64,
    pub amount: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidTransfer {
    pub ticker: String,
    pub sender: String,
    pub amount: u128,
    pub inscribed_height: u64,
    pub consumed_height: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingControl {
    pub ticker: String,
    pub address: String,
    pub op: String,
    pub inscribed_height: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InscriptionOwner {
    pub inscription_id: String,
    pub ticker: String,
    pub kind: String,
    pub current_outpoint: String,
    /// Sat-offset of the inscription within its current outpoint
    /// (ord-compatible FIFO tracking). Defaults to 0 for pre-upgrade
    /// rows via serde default.
    #[serde(default)]
    pub offset_in_outpoint: u64,
    /// Total value in sats of the current outpoint. Needed to compute
    /// FIFO landing when this outpoint is spent.
    #[serde(default)]
    pub outpoint_value_sats: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DailyStats {
    pub transfer_count: u64,
    pub volume: u128,
    pub minted: u128,
    pub burned: u128,
    pub active_addresses: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InscriptionIndex {
    pub ticker: String,
    pub kind: String,
    pub original_amount: Option<u128>,
    pub inscribed_height: u64,
    pub current_owner_address: Option<String>,
    pub consumed_height: Option<u64>,
}

pub fn cursor_get(tx: &redb::ReadTransaction) -> Result<Option<Cursor>> {
    let table = tx.open_table(META)?;
    let v = table.get("cursor_protocol_scan")?;
    match v {
        Some(raw) => {
            let c: Cursor = crate::store::codec::decode(raw.value())?;
            Ok(Some(c))
        }
        None => Ok(None),
    }
}

pub fn cursor_set(tx: &WriteTransaction, cursor: &Cursor) -> Result<()> {
    let mut table = tx.open_table(META)?;
    let v = crate::store::codec::encode(cursor)?;
    table.insert("cursor_protocol_scan", v.as_slice())?;
    Ok(())
}

/// u64 big-endian for use as a sort key.
pub fn be_u64(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

/// Invert u64 so lexicographic order becomes descending by original.
pub fn inv_u64(v: u64) -> u64 {
    u64::MAX - v
}

/// Protect against accidental write to a read-only cursor.
pub fn assert_writable(_tx: &WriteTransaction) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_wallet_state_codec() {
        let s = WalletState {
            total: 100,
            available: 50,
            transferable: 50,
            ..Default::default()
        };
        let bytes = crate::store::codec::encode(&s).unwrap();
        let back: WalletState = crate::store::codec::decode(&bytes).unwrap();
        assert_eq!(back.total, 100);
    }

    #[test]
    fn be_u64_is_monotonic() {
        assert!(be_u64(1) < be_u64(2));
        assert!(be_u64(100) < be_u64(1_000_000));
    }

    #[test]
    fn inv_u64_flips_order() {
        assert!(inv_u64(100) > inv_u64(200));
    }

    fn _use_error() -> Result<()> {
        Err(crate::error::Error::Store("x".into()))
    }
}
