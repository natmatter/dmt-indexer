//! Control-inscription (block/unblock-transferables) two-phase logic.
//!
//! Phase 1 — Inscribe: record the intent; caller writes a
//! `pending_controls` row keyed by inscription_id with
//! `(ticker, address, op, inscribed_height)`.
//!
//! Phase 2 — Tap: the inscription is sent back to the same address in
//! a later transaction. The sync layer sees the self-send through
//! `inscription::tracker`, looks up the pending row, and flips the
//! wallet's `transferables_blocked` flag.
//!
//! For `block-transferables`, tap sets `transferables_blocked = true`.
//! For `unblock-transferables`, tap sets `transferables_blocked = false`.
//! Either way, the pending row is deleted after tap.

use crate::protocol::address::NormalizedAddress;
use crate::protocol::control::ControlOp;

#[derive(Debug, Clone)]
pub struct PendingControl {
    pub inscription_id: String,
    pub ticker: String,
    pub address: NormalizedAddress,
    pub op: ControlOp,
    pub inscribed_height: u64,
}

/// Compute the effect of tapping a control inscription. Caller applies
/// this to wallet state + emits the matching tapped event.
pub fn apply_tap(pending: &PendingControl, tapped_address: &str) -> Option<TapEffect> {
    if pending.address.as_str() != tapped_address {
        // Not a self-send to the inscriber → no effect; just delete
        // the pending row in caller.
        return Some(TapEffect {
            consumed_inscription_id: pending.inscription_id.clone(),
            set_transferables_blocked: None,
        });
    }
    let new_flag = match pending.op {
        ControlOp::Block => true,
        ControlOp::Unblock => false,
    };
    Some(TapEffect {
        consumed_inscription_id: pending.inscription_id.clone(),
        set_transferables_blocked: Some(new_flag),
    })
}

#[derive(Debug, Clone)]
pub struct TapEffect {
    pub consumed_inscription_id: String,
    pub set_transferables_blocked: Option<bool>,
}
