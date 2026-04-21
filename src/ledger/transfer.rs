//! Transfer logic: balance-validated inscribe + validity-gated settle.
//!
//! We use a serial resolver for inscribes (see IMPLEMENTATION_PLAN
//! "Lessons from nat-backend") so that multiple inscribes in the same
//! block from the same address correctly see each other's balance
//! consumption.

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
    /// Insufficient available balance OR sender's `transferables_blocked`
    /// flag is set (the latter is ord-tap's miner-reward-shield at
    /// create time). Emit a semantic-skip event; do NOT write a
    /// validity row. The `reason` string distinguishes the two cases.
    Skipped {
        inscription_id: String,
        ticker: String,
        address: NormalizedAddress,
        attempted_amount: u128,
        snapshot_available: i128,
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

/// Serial resolver: sorts candidates by `inscription_number ASC`, keeps
/// a mutable running `available` per `(ticker, address)`, returns one
/// resolution per candidate in canonical order.
///
/// `blocked_senders` is the set of `(ticker, address)` pairs whose
/// `transferables_blocked` flag is set in wallet state — when true,
/// ord-tap's create-time miner-reward-shield silently rejects the
/// inscribe regardless of available balance. We emit a Skipped with
/// reason = "transferables_blocked" so the event stays visible for
/// diagnostics.
pub fn resolve_transfer_inscribes(
    candidates: Vec<TransferInscribeCandidate>,
    available_snapshot: &HashMap<(String, String), i128>,
    blocked_senders: &HashSet<(String, String)>,
) -> Vec<InscribeResolution> {
    let mut cands = candidates;
    cands.sort_by_key(|c| c.inscription_number);
    let mut running: HashMap<(String, String), i128> = HashMap::new();
    let mut out = Vec::with_capacity(cands.len());
    for c in cands {
        let key = (c.ticker.clone(), c.address.as_str().to_string());
        if blocked_senders.contains(&key) {
            out.push(InscribeResolution::Skipped {
                inscription_id: c.inscription_id,
                ticker: c.ticker,
                address: c.address,
                attempted_amount: c.amount,
                snapshot_available: *running
                    .entry(key.clone())
                    .or_insert_with(|| available_snapshot.get(&key).copied().unwrap_or(0)),
                inscribed_height: c.inscribed_block_height,
                tx_index: c.tx_index,
                reason: "transferables_blocked",
            });
            continue;
        }
        let available = *running
            .entry(key.clone())
            .or_insert_with(|| available_snapshot.get(&key).copied().unwrap_or(0));
        let wanted = i128::try_from(c.amount).unwrap_or(i128::MAX);
        if available < wanted {
            out.push(InscribeResolution::Skipped {
                inscription_id: c.inscription_id,
                ticker: c.ticker,
                address: c.address,
                attempted_amount: c.amount,
                snapshot_available: available,
                inscribed_height: c.inscribed_block_height,
                tx_index: c.tx_index,
                reason: "insufficient_available",
            });
        } else {
            *running.get_mut(&key).unwrap() = available - wanted;
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
    fn within_block_running_balance() {
        let addr = normalize_address("bc1qalice").unwrap();
        let mut snap = HashMap::new();
        snap.insert(("nat".into(), addr.as_str().to_string()), 150);
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
        assert!(matches!(out[1], InscribeResolution::Skipped { .. }));
    }

    #[test]
    fn blocked_sender_is_skipped_regardless_of_balance() {
        let addr = normalize_address("bc1qalice").unwrap();
        let mut snap = HashMap::new();
        snap.insert(("nat".into(), addr.as_str().to_string()), 1_000_000);
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
        snap.insert(("nat".into(), addr.as_str().to_string()), 100);
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
