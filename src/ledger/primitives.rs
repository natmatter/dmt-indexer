//! Reusable balance-movement primitive shared across the transfer,
//! token-send, and token-auth-redeem code paths.
//!
//! Mirrors ord-tap's `exec_internal_send_one` (`/tmp/ot_mod.rs:290-358`):
//!
//! 1. Check `from.available >= amount` (ord-tap computes this as
//!    `from_balance - from_trf` ≥ `amount`; our `WalletState.available`
//!    already stores the same quantity directly).
//! 2. If `from == to`, return a `Self` outcome — no state mutation.
//! 3. Else debit sender (available -a, total -a) and credit recipient
//!    (available +a, total +a).
//!
//! Called by:
//!  - `scan::settle_transfer` (token-transfer consummation at tap): the
//!    existing logic holds its own `VALID_TRANSFERS` row so it passes
//!    `from_transferable = amount, from_available = 0` and uses the
//!    tight-form `exec_debit_credit` below. The available/transferable
//!    split is specific to token-transfer so it stays inline.
//!  - `ledger::send::execute_send_item` (token-send consummation).
//!  - `ledger::auth::execute_redeem_item` (token-auth redeem).

use redb::ReadableTable;

use crate::error::Result;
use crate::store::codec::{decode, encode};
use crate::store::tables::{WalletState, BALANCES_BY_VALUE, WALLET_STATE};

/// Outcome of one send primitive invocation.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SendOutcome {
    /// Balance movement applied: `-amount` from sender, `+amount` to
    /// recipient. Counts both total and available deltas.
    Sent,
    /// Self-send (`from == to`). No state mutation per ord-tap's parity
    /// rule at `/tmp/ot_mod.rs:321-335`. Callers that need an audit
    /// event should emit paired debit+credit rows with zero deltas.
    SelfSend,
    /// Sender's available balance was below `amount`. No state mutation;
    /// per-item caller emits a Skipped event. Mirrors ord-tap's `fail=true`
    /// branch at `/tmp/ot_mod.rs:317-318`.
    InsufficientAvailable { snapshot_available: i128 },
}

/// Apply the send primitive to wallet_state + balances_by_value.
///
/// NOTE: This primitive operates at the balance-movement layer only.
/// Event emission, carrier bookkeeping, and deployment lookups are the
/// caller's responsibility (the three call-sites have materially
/// different metadata needs).
pub fn exec_internal_send_one(
    wtx: &redb::WriteTransaction,
    ticker: &str,
    from: &str,
    to: &str,
    amount: u128,
    occurred_at: chrono::DateTime<chrono::Utc>,
) -> Result<SendOutcome> {
    let amt_i128 = i128::try_from(amount).unwrap_or(i128::MAX);
    // Read sender available BEFORE self-send check so that self-send on
    // an insufficient-available sender still reports the right snapshot.
    let from_available = {
        let table = wtx.open_table(WALLET_STATE)?;
        let raw = table.get((ticker, from))?;
        raw.and_then(|v| decode::<WalletState>(v.value()).ok())
            .map(|s| s.available)
            .unwrap_or(0)
    };
    if from_available < amt_i128 {
        return Ok(SendOutcome::InsufficientAvailable {
            snapshot_available: from_available,
        });
    }
    if from == to {
        return Ok(SendOutcome::SelfSend);
    }
    // Debit sender: total -a, available -a.
    apply_wallet_delta(wtx, ticker, from, -amt_i128, -amt_i128, 0, 0, occurred_at)?;
    // Credit recipient: total +a, available +a.
    apply_wallet_delta(wtx, ticker, to, amt_i128, amt_i128, 0, 0, occurred_at)?;
    Ok(SendOutcome::Sent)
}

/// Lower-level delta application, copied from `scan.rs::apply_wallet_delta`.
/// We keep the scan.rs copy until refactor lands; for now both point at
/// the same WALLET_STATE table with identical semantics.
#[allow(clippy::too_many_arguments)]
pub fn apply_wallet_delta(
    wtx: &redb::WriteTransaction,
    ticker: &str,
    address: &str,
    delta_total: i128,
    delta_available: i128,
    delta_transferable: i128,
    delta_burned: i128,
    occurred_at: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let prev_total: i128 = {
        let table = wtx.open_table(WALLET_STATE)?;
        let raw = table.get((ticker, address))?;
        raw.and_then(|v| decode::<WalletState>(v.value()).ok())
            .map(|s| s.total)
            .unwrap_or(0)
    };
    let mut state: WalletState = {
        let table = wtx.open_table(WALLET_STATE)?;
        let raw = table.get((ticker, address))?;
        raw.and_then(|v| decode(v.value()).ok()).unwrap_or_default()
    };
    state.total += delta_total;
    state.available += delta_available;
    state.transferable += delta_transferable;
    state.burned += delta_burned;
    if state.first_activity.is_none() {
        state.first_activity = Some(occurred_at);
    }
    state.last_activity = Some(occurred_at);
    let new_total = state.total;
    {
        let mut table = wtx.open_table(WALLET_STATE)?;
        table.insert((ticker, address), encode(&state)?.as_slice())?;
    }
    update_balances_index(wtx, ticker, address, prev_total, new_total)?;
    Ok(())
}

fn update_balances_index(
    wtx: &redb::WriteTransaction,
    ticker: &str,
    address: &str,
    old_total: i128,
    new_total: i128,
) -> Result<()> {
    let mut table = wtx.open_table(BALANCES_BY_VALUE)?;
    if old_total > 0 {
        let old_bytes = crate::store::codec::inverted_balance(old_total as u128);
        table.remove((ticker, old_bytes.as_slice(), address))?;
    }
    if new_total > 0 {
        let new_bytes = crate::store::codec::inverted_balance(new_total as u128);
        table.insert((ticker, new_bytes.as_slice(), address), new_total as u64)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::tables::{init_all, WalletState};
    use redb::Database;
    use tempfile::NamedTempFile;

    fn mk_store() -> Database {
        let f = NamedTempFile::new().unwrap();
        // Keep the temp-file path alive in the test via leak. This is
        // fine — the OS cleans it up when the process exits.
        let path = f.path().to_path_buf();
        std::mem::forget(f);
        let db = Database::create(&path).unwrap();
        {
            let tx = db.begin_write().unwrap();
            init_all(&tx).unwrap();
            tx.commit().unwrap();
        }
        db
    }

    fn seed_balance(db: &Database, ticker: &str, addr: &str, total: i128, available: i128) {
        let tx = db.begin_write().unwrap();
        {
            let mut table = tx.open_table(WALLET_STATE).unwrap();
            let s = WalletState {
                total,
                available,
                ..Default::default()
            };
            let bytes = encode(&s).unwrap();
            table.insert((ticker, addr), bytes.as_slice()).unwrap();
        }
        tx.commit().unwrap();
    }

    fn read_balance(db: &Database, ticker: &str, addr: &str) -> (i128, i128) {
        let tx = db.begin_read().unwrap();
        let table = tx.open_table(WALLET_STATE).unwrap();
        let v = table.get((ticker, addr)).unwrap();
        let s: WalletState = v
            .and_then(|raw| decode(raw.value()).ok())
            .unwrap_or_default();
        (s.total, s.available)
    }

    #[test]
    fn self_send_is_no_op() {
        let db = mk_store();
        seed_balance(&db, "nat", "addr", 100, 100);
        let tx = db.begin_write().unwrap();
        let outcome =
            exec_internal_send_one(&tx, "nat", "addr", "addr", 50, chrono::Utc::now()).unwrap();
        tx.commit().unwrap();
        assert!(matches!(outcome, SendOutcome::SelfSend));
        let (total, avail) = read_balance(&db, "nat", "addr");
        assert_eq!(total, 100);
        assert_eq!(avail, 100);
    }

    #[test]
    fn insufficient_available_noop() {
        let db = mk_store();
        seed_balance(&db, "nat", "alice", 10, 10);
        let tx = db.begin_write().unwrap();
        let outcome =
            exec_internal_send_one(&tx, "nat", "alice", "bob", 50, chrono::Utc::now()).unwrap();
        tx.commit().unwrap();
        match outcome {
            SendOutcome::InsufficientAvailable {
                snapshot_available, ..
            } => assert_eq!(snapshot_available, 10),
            _ => panic!("expected insufficient"),
        }
        let (at, aa) = read_balance(&db, "nat", "alice");
        assert_eq!(at, 10);
        assert_eq!(aa, 10);
        let (bt, ba) = read_balance(&db, "nat", "bob");
        assert_eq!(bt, 0);
        assert_eq!(ba, 0);
    }

    #[test]
    fn success_moves_balances() {
        let db = mk_store();
        seed_balance(&db, "nat", "alice", 100, 100);
        let tx = db.begin_write().unwrap();
        let outcome =
            exec_internal_send_one(&tx, "nat", "alice", "bob", 30, chrono::Utc::now()).unwrap();
        tx.commit().unwrap();
        assert!(matches!(outcome, SendOutcome::Sent));
        let (at, aa) = read_balance(&db, "nat", "alice");
        assert_eq!(at, 70);
        assert_eq!(aa, 70);
        let (bt, ba) = read_balance(&db, "nat", "bob");
        assert_eq!(bt, 30);
        assert_eq!(ba, 30);
    }
}
