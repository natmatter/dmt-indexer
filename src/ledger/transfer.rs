//! Transfer logic: balance-validated inscribe + validity-gated settle.
//!
//! ord-tap validates `token-transfer` creation against the wallet's
//! total balance minus already-open transferables. Open transferables
//! are revalidated again when the inscription is executed.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::protocol::address::NormalizedAddress;

/// The outcome of balance-validating a token-transfer inscribe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InscribeResolution {
    /// Balance covered the amount. Emit a debit event + write a
    /// validity row for later settle.
    Admitted {
        inscription_id: String,
        ticker: String,
        address: NormalizedAddress,
        amount: u128,
        inscribed_height: u64,
        tx_index: u32,
    },
    /// Insufficient total balance OR sender's `transferables_blocked`
    /// flag is set (the latter is ord-tap's miner-reward-shield at
    /// create time). Emit a semantic-skip event; do NOT write a
    /// validity row. The `reason` string distinguishes the two cases.
    Skipped {
        inscription_id: String,
        ticker: String,
        address: NormalizedAddress,
        attempted_amount: u128,
        snapshot_balance: i128,
        inscribed_height: u64,
        tx_index: u32,
        reason: &'static str,
    },
}

#[derive(Debug, Clone)]
pub struct TransferInscribeCandidate {
    pub inscription_id: String,
    pub inscription_number: i64,
    pub inscribed_block_height: u64,
    pub ticker: String,
    pub address: NormalizedAddress,
    pub amount: u128,
    pub tx_index: u32,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TransferBalanceSnapshot {
    pub total: i128,
    pub transferable: i128,
}

/// Resolver: sorts candidates by `inscription_number ASC` and validates
/// each candidate against the sender's remaining transferable capacity.
///
/// `blocked_senders` is the set of `(ticker, address)` pairs whose
/// `transferables_blocked` flag is set in wallet state — when true,
/// Tapalytics' create-time miner-reward-shield silently rejects the
/// inscribe regardless of available balance. We emit a Skipped with
/// reason = "transferables_blocked" so the event stays visible for
/// diagnostics.
pub fn resolve_transfer_inscribes(
    candidates: Vec<TransferInscribeCandidate>,
    balance_snapshot: &HashMap<(String, String), TransferBalanceSnapshot>,
    blocked_senders: &HashSet<(String, String)>,
) -> Vec<InscribeResolution> {
    let mut cands = candidates;
    cands.sort_by_key(|c| c.inscription_number);
    let mut live = balance_snapshot.clone();
    let mut out = Vec::with_capacity(cands.len());
    for c in cands {
        let key = (c.ticker.clone(), c.address.as_str().to_string());
        let state = live.entry(key.clone()).or_default();
        if blocked_senders.contains(&key) {
            out.push(InscribeResolution::Skipped {
                inscription_id: c.inscription_id,
                ticker: c.ticker,
                address: c.address,
                attempted_amount: c.amount,
                snapshot_balance: state.total,
                inscribed_height: c.inscribed_block_height,
                tx_index: c.tx_index,
                reason: "transferables_blocked",
            });
            continue;
        }
        let wanted = i128::try_from(c.amount).unwrap_or(i128::MAX);
        if state.transferable.saturating_add(wanted) > state.total {
            out.push(InscribeResolution::Skipped {
                inscription_id: c.inscription_id,
                ticker: c.ticker,
                address: c.address,
                attempted_amount: c.amount,
                snapshot_balance: state.total,
                inscribed_height: c.inscribed_block_height,
                tx_index: c.tx_index,
                reason: "insufficient_transferable_capacity",
            });
        } else {
            state.transferable = state.transferable.saturating_add(wanted);
            out.push(InscribeResolution::Admitted {
                inscription_id: c.inscription_id,
                ticker: c.ticker,
                address: c.address,
                amount: c.amount,
                inscribed_height: c.inscribed_block_height,
                tx_index: c.tx_index,
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::address::normalize_address;

    #[test]
    fn overcommitted_open_transferables_are_rejected_at_creation() {
        let addr = normalize_address("bc1qalice").unwrap();
        let mut snap = HashMap::new();
        snap.insert(
            ("nat".into(), addr.as_str().to_string()),
            TransferBalanceSnapshot {
                total: 150,
                transferable: 0,
            },
        );
        let c1 = TransferInscribeCandidate {
            inscription_id: "a".into(),
            inscription_number: 1,
            inscribed_block_height: 900_000,
            ticker: "nat".into(),
            address: addr.clone(),
            amount: 100,
            tx_index: 0,
        };
        let c2 = TransferInscribeCandidate {
            inscription_id: "b".into(),
            inscription_number: 2,
            inscribed_block_height: 900_000,
            ticker: "nat".into(),
            address: addr,
            amount: 100,
            tx_index: 1,
        };
        let out = resolve_transfer_inscribes(vec![c1, c2], &snap, &HashSet::new());
        assert!(matches!(out[0], InscribeResolution::Admitted { .. }));
        assert!(matches!(
            out[1],
            InscribeResolution::Skipped {
                reason: "insufficient_transferable_capacity",
                ..
            }
        ));
    }

    #[test]
    fn blocked_sender_is_skipped_regardless_of_balance() {
        let addr = normalize_address("bc1qalice").unwrap();
        let mut snap = HashMap::new();
        snap.insert(
            ("nat".into(), addr.as_str().to_string()),
            TransferBalanceSnapshot {
                total: 1_000_000,
                transferable: 0,
            },
        );
        let mut blocked = HashSet::new();
        blocked.insert(("nat".into(), addr.as_str().to_string()));
        let c = TransferInscribeCandidate {
            inscription_id: "a".into(),
            inscription_number: 1,
            inscribed_block_height: 950_000,
            ticker: "nat".into(),
            address: addr,
            amount: 100,
            tx_index: 0,
        };
        let out = resolve_transfer_inscribes(vec![c], &snap, &blocked);
        match &out[0] {
            InscribeResolution::Skipped { reason, .. } => {
                assert_eq!(*reason, "transferables_blocked")
            }
            _ => panic!("expected skipped"),
        }
    }

    #[test]
    fn sort_respected() {
        let addr = normalize_address("bc1qalice").unwrap();
        let mut snap = HashMap::new();
        snap.insert(
            ("nat".into(), addr.as_str().to_string()),
            TransferBalanceSnapshot {
                total: 100,
                transferable: 0,
            },
        );
        // c2 arrives first with higher inscription_number; c1 should
        // win after sort.
        let c1 = TransferInscribeCandidate {
            inscription_id: "a".into(),
            inscription_number: 5,
            inscribed_block_height: 900_000,
            ticker: "nat".into(),
            address: addr.clone(),
            amount: 100,
            tx_index: 5,
        };
        let c2 = TransferInscribeCandidate {
            inscription_id: "b".into(),
            inscription_number: 3,
            inscribed_block_height: 900_000,
            ticker: "nat".into(),
            address: addr,
            amount: 100,
            tx_index: 3,
        };
        let out = resolve_transfer_inscribes(vec![c1, c2], &snap, &HashSet::new());
        if let InscribeResolution::Admitted { inscription_id, .. } = &out[0] {
            assert_eq!(inscription_id, "b");
        } else {
            panic!("expected first result admitted");
        }
    }
}
