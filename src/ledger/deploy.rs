//! Deployment state.
//!
//! dmt-indexer v0.1.0 is NAT-only. The hardcoded seed in
//! `sync::scan::Syncer::seed_hardcoded_deployments` registers the one
//! deployment we index; we don't auto-register additional deploys
//! off-chain. The `register` function is retained only to make it
//! straightforward for a future fork to re-enable auto-discovery if
//! scope is expanded.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::protocol::bits::BitsMode;
use crate::protocol::deploy::DeployPayload;
use crate::protocol::element::ElementField;
use crate::protocol::ticker::{ticker_is_valid_at_height, NormalizedTicker};

/// Per-deployment registration. Persisted in redb keyed by ticker.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Deployment {
    pub ticker: NormalizedTicker,
    pub deploy_inscription_id: String,
    pub element_inscription_id: String,
    pub element_field: ElementField,
    pub dt: Option<String>,
    pub dim: Option<String>,
    pub bits_mode: BitsMode,
    /// Block at which the deploy inscription was mined.
    pub activation_height: u64,
    /// Block at which coinbase-reward distribution starts (NAT: 885,588).
    pub coinbase_activation: Option<u64>,
    /// Block at which new coinbase recipients start with
    /// transferability blocked (NAT: 941,848).
    pub miner_transfer_activation: Option<u64>,
    pub registered_at: DateTime<Utc>,
}

pub const NAT_COINBASE_ACTIVATION: u64 = 885_588;
pub const NAT_MINER_TRANSFER_ACTIVATION: u64 = 941_848;
/// Second phase of the miner-reward shield. After this height, a
/// token-transfer tap from a DMT-reward address is voided regardless
/// of the sender's current `transferables_blocked` flag's origin —
/// the permanent `dmt_reward_addresses` marker is the authoritative
/// signal. The transferable is released back to available; sender
/// keeps the balance, recipient gets nothing. Mirrors ord-tap's
/// `MinerRewardTransferExecutionShieldActivation` (942,002).
pub const NAT_MINER_TRANSFER_EXECUTION_SHIELD: u64 = 942_002;

/// Build a [`Deployment`] from a parsed on-chain payload. Preserved
/// for future scope expansion; the v0.1.0 sync pipeline short-circuits
/// non-NAT deploys before this runs, so under normal operation the
/// hardcoded seed is the only path that creates Deployment rows.
pub fn register(
    payload: DeployPayload,
    deploy_inscription_id: String,
    inscribed_height: u64,
    element_field: ElementField,
) -> Result<Deployment> {
    if payload.prv.is_some() {
        return Err(Error::Protocol(
            "deploy carries prv (privilege-auth) which we do not validate".into(),
        ));
    }
    if !ticker_is_valid_at_height(&payload.ticker, inscribed_height) {
        return Err(Error::Protocol(format!(
            "ticker {} invalid at height {inscribed_height}",
            payload.ticker
        )));
    }
    // All bits-field deployments decode bits as its u32 numeric value.
    // Earlier versions of this indexer used a "text over hex" mode that
    // treated any a–f hex digit in bits as a scarcity rule producing
    // zero supply — that rule is not in the DMT spec and caused ~90%
    // of mint credits to be silently dropped.
    let _ = element_field;
    let bits_mode = BitsMode::RawHex;
    let (coinbase_activation, miner_transfer_activation) = if payload.ticker.as_str() == "nat" {
        (
            Some(NAT_COINBASE_ACTIVATION),
            Some(NAT_MINER_TRANSFER_ACTIVATION),
        )
    } else {
        (None, None)
    };
    Ok(Deployment {
        ticker: payload.ticker,
        deploy_inscription_id,
        element_inscription_id: payload.element_inscription_id,
        element_field,
        dt: payload.dt,
        dim: payload.dim,
        bits_mode,
        activation_height: inscribed_height,
        coinbase_activation,
        miner_transfer_activation,
        registered_at: Utc::now(),
    })
}
