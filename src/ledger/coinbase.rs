//! Coinbase-reward distribution for NAT (post-885,588).
//!
//! Deterministic per-output floor math mirroring ord-tap. OP_RETURN /
//! un-addressable coinbase outputs are excluded from both the denominator
//! and distribution.
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
    #[allow(dead_code)]
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
    let eligible: Vec<(u32, String, u64)> = coinbase
        .output
        .iter()
        .enumerate()
        .filter_map(|(vout, output)| {
            if output.script_pubkey.is_op_return() {
                return None;
            }
            let address = address_from_script(&output.script_pubkey)?;
            Some((vout as u32, address, output.value.to_sat()))
        })
        .collect();
    let total_value: u128 = eligible
        .iter()
        .map(|(_, _, value)| u128::from(*value))
        .sum();
    if total_value == 0 {
        return Vec::new();
    }

    let miner_locked = deployment
        .miner_transfer_activation
        .map(|h| block_height >= h)
        .unwrap_or(false);

    let mut out = Vec::with_capacity(eligible.len());
    for (vout, address, value_sats) in eligible {
        let share = reward
            .checked_mul(u128::from(value_sats))
            .map(|product| product / total_value)
            .unwrap_or_else(|| (u128::from(value_sats) * reward) / total_value);
        if share == 0 {
            continue;
        }
        out.push(CoinbaseShare {
            vout,
            address: Some(address),
            share_amount: share,
            value_sats,
            is_burn: false,
            should_lock_on_first_credit: miner_locked,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::deploy::{
        Deployment, NAT_COINBASE_ACTIVATION, NAT_MAX_SUPPLY, NAT_MINER_TRANSFER_ACTIVATION,
    };
    use crate::protocol::bits::{decode_bits, BitsMode};
    use crate::protocol::element::ElementField;
    use crate::protocol::ticker::normalize_ticker;
    use bitcoin::{absolute::LockTime, transaction::Version, Amount, ScriptBuf, TxIn, TxOut};

    fn nat_deployment() -> Deployment {
        Deployment {
            ticker: normalize_ticker("nat").unwrap(),
            deploy_inscription_id: "".into(),
            dmt: true,
            element_inscription_id: "".into(),
            element_field: ElementField::Bits,
            dt: None,
            dim: None,
            bits_mode: BitsMode::RawHex,
            decimals: 0,
            mint_limit: 0,
            privilege_auth: None,
            activation_height: 817_709,
            coinbase_activation: Some(NAT_COINBASE_ACTIVATION),
            miner_transfer_activation: Some(NAT_MINER_TRANSFER_ACTIVATION),
            max_supply: NAT_MAX_SUPPLY,
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
        let t = tx(vec![p2wpkh(333), p2wpkh(667)]);
        let shares = distribute(&d, &t, NAT_COINBASE_ACTIVATION, 0x17034220);
        let reward = decode_bits(0x17034220, BitsMode::RawHex).unwrap();
        let sum: u128 = shares.iter().map(|s| s.share_amount).sum();
        assert!(sum <= reward);
    }

    #[test]
    fn op_return_outputs_are_excluded() {
        let d = nat_deployment();
        let t = tx(vec![op_return(100), p2wpkh(200)]);
        let shares = distribute(&d, &t, NAT_COINBASE_ACTIVATION, 0x17034220);
        let reward = decode_bits(0x17034220, BitsMode::RawHex).unwrap();
        assert_eq!(shares.len(), 1);
        assert_eq!(shares[0].share_amount, reward);
        assert!(!shares[0].is_burn);
    }

    #[test]
    fn all_op_return_outputs_produce_no_rewards() {
        let d = nat_deployment();
        let t = tx(vec![op_return(100), op_return(200)]);
        assert!(distribute(&d, &t, NAT_COINBASE_ACTIVATION, 0x17034220).is_empty());
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
