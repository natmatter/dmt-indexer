//! Coinbase-reward distribution for NAT (post-885,588).
//!
//! Deterministic cumulative-share math mirroring nat-backend
//! (`crates/nat-core/src/coinbase.rs`). Each output's share is the
//! delta between its cumulative share and the previous one; the final
//! output absorbs rounding residual, so `sum(shares) == reward` exactly.
//!
//! OP_RETURN / un-addressable outputs still receive a proportional
//! share — it is recorded as a burn event rather than a wallet credit.
//!
//! At block ≥ `miner_transfer_activation` new recipients get
//! `transferables_blocked = true` on their **first** coinbase credit
//! only; we do NOT clobber a wallet's existing flag on subsequent
//! credits.

use bitcoin::Transaction;
use serde::{Deserialize, Serialize};

use crate::ledger::deploy::Deployment;
use crate::protocol::address::address_from_script;
use crate::protocol::bits::decode_bits;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoinbaseShare {
    /// Output vout this share comes from.
    pub vout: u32,
    pub address: Option<String>,
    pub share_amount: u128,
    pub value_sats: u64,
    pub is_burn: bool,
    /// True if this share should set the recipient's
    /// `transferables_blocked` flag (per the miner-lock activation
    /// height). Burns are never locked.
    pub should_lock_on_first_credit: bool,
}

pub fn distribute(
    deployment: &Deployment,
    coinbase: &Transaction,
    block_height: u64,
    bits: u32,
) -> Vec<CoinbaseShare> {
    let Some(activation) = deployment.coinbase_activation else {
        return Vec::new();
    };
    if block_height < activation {
        return Vec::new();
    }
    let reward = decode_bits(bits, deployment.bits_mode).unwrap_or(0);
    if reward == 0 || coinbase.output.is_empty() {
        return Vec::new();
    }
    let total_value: u128 = coinbase
        .output
        .iter()
        .map(|o| u128::from(o.value.to_sat()))
        .sum();
    if total_value == 0 {
        return Vec::new();
    }

    let miner_locked = deployment
        .miner_transfer_activation
        .map(|h| block_height >= h)
        .unwrap_or(false);

    let mut cumulative_value: u128 = 0;
    let mut cumulative_share: u128 = 0;
    let mut out = Vec::with_capacity(coinbase.output.len());
    for (vout, output) in coinbase.output.iter().enumerate() {
        cumulative_value += u128::from(output.value.to_sat());
        let next_cumulative_share = reward
            .checked_mul(cumulative_value)
            .map(|product| product / total_value)
            .unwrap_or_else(|| {
                let share = (u128::from(output.value.to_sat()) * reward) / total_value;
                cumulative_share + share
            });
        let share = next_cumulative_share.saturating_sub(cumulative_share);
        cumulative_share = next_cumulative_share;
        if share == 0 {
            continue;
        }

        let address = address_from_script(&output.script_pubkey);
        let is_burn = output.script_pubkey.is_op_return() || address.is_none();
        out.push(CoinbaseShare {
            vout: vout as u32,
            address,
            share_amount: share,
            value_sats: output.value.to_sat(),
            is_burn,
            should_lock_on_first_credit: miner_locked && !is_burn,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::deploy::{
        Deployment, NAT_COINBASE_ACTIVATION, NAT_MINER_TRANSFER_ACTIVATION,
    };
    use crate::protocol::bits::{decode_bits, BitsMode};
    use crate::protocol::element::ElementField;
    use crate::protocol::ticker::normalize_ticker;
    use bitcoin::{absolute::LockTime, transaction::Version, Amount, ScriptBuf, TxIn, TxOut};

    fn nat_deployment() -> Deployment {
        Deployment {
            ticker: normalize_ticker("nat").unwrap(),
            deploy_inscription_id: "".into(),
            element_inscription_id: "".into(),
            element_field: ElementField::Bits,
            dt: None,
            dim: None,
            bits_mode: BitsMode::RawHex,
            activation_height: 817_709,
            coinbase_activation: Some(NAT_COINBASE_ACTIVATION),
            miner_transfer_activation: Some(NAT_MINER_TRANSFER_ACTIVATION),
            registered_at: chrono::Utc::now(),
        }
    }

    fn tx(outs: Vec<TxOut>) -> Transaction {
        Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn::default()],
            output: outs,
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

    fn op_return(sats: u64) -> TxOut {
        TxOut {
            value: Amount::from_sat(sats),
            script_pubkey: ScriptBuf::from(vec![0x6a, 0x01, 0xff]),
        }
    }

    #[test]
    fn empty_below_activation() {
        let d = nat_deployment();
        let t = tx(vec![]);
        assert!(distribute(&d, &t, NAT_COINBASE_ACTIVATION - 1, 0x17034220).is_empty());
    }

    #[test]
    fn zero_bits_is_empty() {
        let d = nat_deployment();
        let t = tx(vec![p2wpkh(1000)]);
        assert!(distribute(&d, &t, NAT_COINBASE_ACTIVATION, 0).is_empty());
    }

    #[test]
    fn sum_equals_reward_exactly() {
        let d = nat_deployment();
        let t = tx(vec![p2wpkh(333), p2wpkh(667), op_return(1)]);
        let shares = distribute(&d, &t, NAT_COINBASE_ACTIVATION, 0x17034220);
        let reward = decode_bits(0x17034220, BitsMode::RawHex).unwrap();
        let sum: u128 = shares.iter().map(|s| s.share_amount).sum();
        assert_eq!(sum, reward);
    }

    #[test]
    fn all_op_return_burns_everything() {
        let d = nat_deployment();
        let t = tx(vec![op_return(100), op_return(200)]);
        let shares = distribute(&d, &t, NAT_COINBASE_ACTIVATION, 0x17034220);
        assert!(!shares.is_empty());
        assert!(shares.iter().all(|s| s.is_burn));
        assert!(shares.iter().all(|s| !s.should_lock_on_first_credit));
    }

    #[test]
    fn miner_lock_past_activation() {
        let d = nat_deployment();
        let t = tx(vec![p2wpkh(1000)]);
        let shares = distribute(&d, &t, NAT_MINER_TRANSFER_ACTIVATION, 0x17034220);
        assert!(shares.iter().any(|s| s.should_lock_on_first_credit));
    }

    #[test]
    fn miner_unlock_before_activation() {
        let d = nat_deployment();
        let t = tx(vec![p2wpkh(1000)]);
        let shares = distribute(&d, &t, NAT_MINER_TRANSFER_ACTIVATION - 1, 0x17034220);
        assert!(shares.iter().all(|s| !s.should_lock_on_first_credit));
    }
}
