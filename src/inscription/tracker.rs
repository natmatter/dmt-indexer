//! Sat-level inscription carrier tracker (ord-compatible FIFO model).
//!
//! Each tracked inscription sits on a specific satoshi inside its
//! carrier outpoint. When a transaction spends a carrier outpoint, we:
//!
//! 1. Compute the inscription's absolute sat offset from the start of
//!    the tx's input sat range (= sum of prior input values + its
//!    offset within the spent outpoint).
//! 2. Walk outputs in order, summing their values as a cumulative sat
//!    range. The inscription lands on the first output whose range
//!    contains the absolute offset.
//! 3. If the offset exceeds the sum of all output values, the
//!    inscription falls into fees (effectively sent to the coinbase
//!    miner); we treat this as a burn for balance accounting.
//! 4. If the landing output is OP_RETURN or otherwise un-addressable,
//!    treat as burn.
//!
//! This matches ord's simple-rare-sat-tracking rule for inscription
//! movement and gives us full correctness for the edge cases the
//! "input 0 → first non-OP_RETURN output" simplification missed.

use std::collections::HashMap;

use bitcoin::{OutPoint, Transaction};
use serde::{Deserialize, Serialize};

use crate::protocol::address::address_from_script;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct TrackedInscription {
    pub inscription_id: String,
    pub ticker: String,
    pub kind: InscriptionKind,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
pub enum InscriptionKind {
    TokenTransfer,
    Control,
    /// `dmt-mint` inscriptions. Tracked only so we can keep
    /// `INSCRIPTIONS.current_owner_address` fresh as the UNAT moves;
    /// mint balance credits are permanent to the original inscriber and
    /// do not shift when the inscription is sent.
    Mint,
}

/// Full-fidelity carrier record: the inscription + where in its
/// outpoint's sat range it lives + the outpoint's total value (needed
/// for the FIFO computation when this outpoint is spent).
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct TrackedCarrier {
    pub inscription: TrackedInscription,
    pub offset_in_outpoint: u64,
    pub outpoint_value_sats: u64,
}

#[derive(Debug, Clone)]
pub enum TrackerMove {
    /// Inscription moved to a real output. `to_offset` is the sat
    /// offset within the new outpoint.
    Moved {
        inscription: TrackedInscription,
        from: OutPoint,
        to: OutPoint,
        to_offset: u64,
        to_outpoint_value_sats: u64,
        new_owner_address: Option<String>,
    },
    /// Landed on OP_RETURN or un-addressable script, or fell into
    /// fees. The inscription is considered burned.
    Burned {
        inscription: TrackedInscription,
        from: OutPoint,
        reason: BurnReason,
    },
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum BurnReason {
    OpReturn,
    Unaddressable,
    IntoFees,
}

#[derive(Debug, Default)]
pub struct InscriptionTracker {
    by_outpoint: HashMap<OutPoint, TrackedCarrier>,
}

impl InscriptionTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, outpoint: OutPoint, carrier: TrackedCarrier) {
        self.by_outpoint.insert(outpoint, carrier);
    }

    pub fn remove(&mut self, outpoint: &OutPoint) -> Option<TrackedCarrier> {
        self.by_outpoint.remove(outpoint)
    }

    pub fn get(&self, outpoint: &OutPoint) -> Option<&TrackedCarrier> {
        self.by_outpoint.get(outpoint)
    }

    /// Snapshot for persistence.
    pub fn snapshot(&self) -> Vec<(OutPoint, TrackedCarrier)> {
        self.by_outpoint
            .iter()
            .map(|(op, c)| (*op, c.clone()))
            .collect()
    }

    /// Process one transaction. `input_values` must map EVERY input
    /// outpoint of `tx` to its value-in-sats (caller fetches via RPC
    /// for non-tracked inputs).
    ///
    /// Returns the list of moves (or burns) for any inscription this
    /// tx carried.
    pub fn apply_tx(
        &mut self,
        tx: &Transaction,
        input_values: &HashMap<OutPoint, u64>,
    ) -> Vec<TrackerMove> {
        let mut moves = Vec::new();
        // Collect tracked inscriptions that this tx spends, keyed by
        // absolute sat offset from the start of the tx's combined
        // input sat range.
        let mut cumulative_input: u64 = 0;
        let mut pending: Vec<(OutPoint, TrackedCarrier, u64)> = Vec::new();
        for txin in &tx.input {
            let outpoint = txin.previous_output;
            let value = *input_values.get(&outpoint).unwrap_or(&0);
            if let Some(carrier) = self.by_outpoint.remove(&outpoint) {
                let abs_offset = cumulative_input.saturating_add(carrier.offset_in_outpoint);
                pending.push((outpoint, carrier, abs_offset));
            }
            cumulative_input = cumulative_input.saturating_add(value);
        }
        if pending.is_empty() {
            return moves;
        }
        let txid = tx.compute_txid();

        // For each tracked inscription spent, find its landing output.
        for (from, carrier, abs_offset) in pending {
            let inscription = carrier.inscription;
            let mut cumulative_output: u64 = 0;
            let mut landed = false;
            for (vout, out) in tx.output.iter().enumerate() {
                let value = out.value.to_sat();
                if abs_offset < cumulative_output.saturating_add(value) {
                    let to_offset = abs_offset - cumulative_output;
                    let to = OutPoint {
                        txid,
                        vout: vout as u32,
                    };
                    if out.script_pubkey.is_op_return() {
                        moves.push(TrackerMove::Burned {
                            inscription: inscription.clone(),
                            from,
                            reason: BurnReason::OpReturn,
                        });
                    } else if let Some(addr) = address_from_script(&out.script_pubkey) {
                        self.by_outpoint.insert(
                            to,
                            TrackedCarrier {
                                inscription: inscription.clone(),
                                offset_in_outpoint: to_offset,
                                outpoint_value_sats: value,
                            },
                        );
                        moves.push(TrackerMove::Moved {
                            inscription: inscription.clone(),
                            from,
                            to,
                            to_offset,
                            to_outpoint_value_sats: value,
                            new_owner_address: Some(addr),
                        });
                    } else {
                        moves.push(TrackerMove::Burned {
                            inscription: inscription.clone(),
                            from,
                            reason: BurnReason::Unaddressable,
                        });
                    }
                    landed = true;
                    break;
                }
                cumulative_output = cumulative_output.saturating_add(value);
            }
            if !landed {
                moves.push(TrackerMove::Burned {
                    inscription: inscription.clone(),
                    from,
                    reason: BurnReason::IntoFees,
                });
            }
        }
        moves
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{
        absolute::LockTime, hashes::Hash, transaction::Version, Amount, ScriptBuf, Sequence, TxIn,
        TxOut,
    };

    fn op(txid_byte: u8, vout: u32) -> OutPoint {
        OutPoint {
            txid: bitcoin::Txid::from_raw_hash(bitcoin::hashes::sha256d::Hash::from_byte_array(
                [txid_byte; 32],
            )),
            vout,
        }
    }

    fn spend_tx(ins: Vec<TxIn>, outs: Vec<TxOut>) -> Transaction {
        Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: ins,
            output: outs,
        }
    }

    fn txin(prev: OutPoint) -> TxIn {
        TxIn {
            previous_output: prev,
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: bitcoin::Witness::new(),
        }
    }

    fn p2wpkh(sats: u64) -> TxOut {
        let mut s = Vec::with_capacity(22);
        s.push(0x00);
        s.push(0x14);
        s.extend_from_slice(&[0x11u8; 20]);
        TxOut {
            value: Amount::from_sat(sats),
            script_pubkey: ScriptBuf::from(s),
        }
    }

    fn opreturn() -> TxOut {
        TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from(vec![0x6a]),
        }
    }

    fn carrier(id: &str, value: u64) -> TrackedCarrier {
        TrackedCarrier {
            inscription: TrackedInscription {
                inscription_id: id.into(),
                ticker: "nat".into(),
                kind: InscriptionKind::TokenTransfer,
            },
            offset_in_outpoint: 0,
            outpoint_value_sats: value,
        }
    }

    #[test]
    fn single_input_single_output_moves_at_offset_zero() {
        let prev = op(1, 0);
        let mut t = InscriptionTracker::new();
        t.insert(prev, carrier("a", 546));
        let tx = spend_tx(vec![txin(prev)], vec![p2wpkh(546)]);
        let mut iv = HashMap::new();
        iv.insert(prev, 546);
        let moves = t.apply_tx(&tx, &iv);
        assert_eq!(moves.len(), 1);
        match &moves[0] {
            TrackerMove::Moved { to_offset, .. } => assert_eq!(*to_offset, 0),
            _ => panic!("expected moved"),
        }
    }

    #[test]
    fn fifo_lands_on_second_output_when_first_is_small() {
        let prev = op(1, 0);
        let mut t = InscriptionTracker::new();
        // Inscription at offset 600 within a 1000-sat outpoint.
        let mut c = carrier("a", 1000);
        c.offset_in_outpoint = 600;
        t.insert(prev, c);
        // Outputs: first takes 400 sats, second takes 600. Sat 600
        // lands in the second output at offset 200.
        let tx = spend_tx(vec![txin(prev)], vec![p2wpkh(400), p2wpkh(600)]);
        let mut iv = HashMap::new();
        iv.insert(prev, 1000);
        let moves = t.apply_tx(&tx, &iv);
        match &moves[0] {
            TrackerMove::Moved { to, to_offset, .. } => {
                assert_eq!(to.vout, 1);
                assert_eq!(*to_offset, 200);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn landing_on_op_return_burns() {
        let prev = op(1, 0);
        let mut t = InscriptionTracker::new();
        t.insert(prev, carrier("a", 546));
        // Single output is OP_RETURN at value=0. Offset 0 lands on it
        // only if we also have a value > 0 output — actually offset 0
        // < cumulative_output (0) + 0 is false; the FIFO walk moves
        // past a 0-value output. Use a tx where OP_RETURN has
        // positive sats to force the landing.
        let tx = spend_tx(
            vec![txin(prev)],
            vec![TxOut {
                value: Amount::from_sat(546),
                script_pubkey: ScriptBuf::from(vec![0x6a, 0x01, 0xff]),
            }],
        );
        let mut iv = HashMap::new();
        iv.insert(prev, 546);
        let moves = t.apply_tx(&tx, &iv);
        match &moves[0] {
            TrackerMove::Burned { reason, .. } => assert_eq!(*reason, BurnReason::OpReturn),
            _ => panic!(),
        }
    }

    #[test]
    fn offset_past_outputs_falls_into_fees() {
        let prev = op(1, 0);
        let mut t = InscriptionTracker::new();
        let mut c = carrier("a", 1000);
        c.offset_in_outpoint = 900;
        t.insert(prev, c);
        // One output of 500 sats; fee is 500; offset 900 is into the
        // fees, i.e. coinbase/miner — treated as burn.
        let tx = spend_tx(vec![txin(prev)], vec![p2wpkh(500)]);
        let mut iv = HashMap::new();
        iv.insert(prev, 1000);
        let moves = t.apply_tx(&tx, &iv);
        match &moves[0] {
            TrackerMove::Burned { reason, .. } => assert_eq!(*reason, BurnReason::IntoFees),
            _ => panic!(),
        }
    }

    #[test]
    fn multi_input_respects_input_order() {
        let a = op(1, 0);
        let b = op(2, 0);
        let mut t = InscriptionTracker::new();
        // Inscription on input B (prev_value=1000, offset 0).
        t.insert(b, carrier("b", 1000));
        // Input A is 500 sats (not tracked). So in the tx sat space:
        //   A sat range [0, 500), B sat range [500, 1500).
        // Inscription absolute offset = 500 + 0 = 500.
        // Output layout: 700, 800. Offset 500 lands in output 0 at
        // position 500.
        let tx = spend_tx(vec![txin(a), txin(b)], vec![p2wpkh(700), p2wpkh(800)]);
        let mut iv = HashMap::new();
        iv.insert(a, 500);
        iv.insert(b, 1000);
        let moves = t.apply_tx(&tx, &iv);
        match &moves[0] {
            TrackerMove::Moved { to, to_offset, .. } => {
                assert_eq!(to.vout, 0);
                assert_eq!(*to_offset, 500);
            }
            _ => panic!(),
        }
    }
}
