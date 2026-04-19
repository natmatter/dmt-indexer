//! Mint logic for NAT.
//!
//! First-is-first per `(ticker, blk)`. Within a block, ties broken by
//! `inscription_number` ascending (we use `(tx_index, env_pos)` as a
//! deterministic synthetic rank). After the deployment's
//! `coinbase_activation` height, `dmt-mint` inscriptions for the
//! ticker are ignored — supply comes from coinbase distribution.

use serde::{Deserialize, Serialize};

use crate::ledger::deploy::Deployment;
use crate::protocol::address::NormalizedAddress;
use crate::protocol::bits::decode_bits;
use crate::protocol::mint::MintPayload;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MintCandidate {
    pub inscription_id: String,
    pub inscription_number: i64,
    pub inscribed_block_height: u64,
    pub address: NormalizedAddress,
    pub payload: MintPayload,
    /// Bits of the block referenced by `payload.block_number`. NAT
    /// uses `element_field = Bits`, so this is the only field-value
    /// input the resolver needs.
    pub referenced_bits: u32,
    pub tx_index: u32,
}

#[derive(Debug, Clone)]
pub struct MintResolution {
    pub admitted: Vec<AdmittedMint>,
    pub rejected: Vec<RejectedMint>,
    pub post_activation_ignored: Vec<PostActivationMint>,
}

#[derive(Debug, Clone)]
pub struct AdmittedMint {
    pub candidate: MintCandidate,
    pub amount: u128,
}

#[derive(Debug, Clone)]
pub struct RejectedMint {
    pub candidate: MintCandidate,
    pub reason: &'static str,
}

#[derive(Debug, Clone)]
pub struct PostActivationMint {
    pub candidate: MintCandidate,
}

/// Resolve a block's mint candidates for one deployment. Rules match
/// nat-backend's `derive_mint` (`workers/nat-sync/src/derive.rs`):
///
/// 1. Post-activation skip (`block_height >= coinbase_activation`).
/// 2. `dep` enforcement: if present, must equal the deployment's
///    deploy_inscription_id; always required (NAT is `dep_optional = never`).
/// 3. `blk` sanity: `1..=block_height` (reject `0` / future-dated).
/// 4. First-is-first per `(ticker, payload.block_number)` with
///    deterministic tie-break on `inscription_number ASC`.
/// 5. Bits-decoded amount via `deployment.bits_mode`. A zero amount
///    (scarcity) silently consumes the slot without emitting a credit.
pub fn resolve_mints(
    deployment: &Deployment,
    block_height: u64,
    already_claimed_blocks: &std::collections::HashSet<u64>,
    mut candidates: Vec<MintCandidate>,
) -> MintResolution {
    candidates.sort_by_key(|c| (c.payload.block_number, c.inscription_number));
    let mut admitted = Vec::new();
    let mut rejected = Vec::new();
    let mut post_activation_ignored = Vec::new();
    let mut claimed_now: std::collections::HashSet<u64> = std::collections::HashSet::new();

    let post_activation = deployment
        .coinbase_activation
        .map(|h| block_height >= h)
        .unwrap_or(false);

    for c in candidates {
        if post_activation {
            post_activation_ignored.push(PostActivationMint { candidate: c });
            continue;
        }
        // `dep` enforcement. NAT always requires `dep` to be present
        // and match the deployment.
        match c.payload.deployment_inscription_id.as_deref() {
            Some(dep) if dep != deployment.deploy_inscription_id => {
                rejected.push(RejectedMint {
                    candidate: c,
                    reason: "dep_mismatch",
                });
                continue;
            }
            None => {
                rejected.push(RejectedMint {
                    candidate: c,
                    reason: "dep_required",
                });
                continue;
            }
            _ => {}
        }
        // `blk` range sanity (nat-backend derive.rs:343).
        if c.payload.block_number == 0 || c.payload.block_number > block_height {
            rejected.push(RejectedMint {
                candidate: c,
                reason: "blk_out_of_range",
            });
            continue;
        }
        let blk = c.payload.block_number;
        if already_claimed_blocks.contains(&blk) || claimed_now.contains(&blk) {
            rejected.push(RejectedMint {
                candidate: c,
                reason: "block_already_claimed",
            });
            continue;
        }
        let Ok(amount) = decode_bits(c.referenced_bits, deployment.bits_mode) else {
            rejected.push(RejectedMint {
                candidate: c,
                reason: "bits_decode_failed",
            });
            continue;
        };
        if amount == 0 {
            claimed_now.insert(blk);
            rejected.push(RejectedMint {
                candidate: c,
                reason: "zero_amount_scarcity",
            });
            continue;
        }
        claimed_now.insert(blk);
        admitted.push(AdmittedMint {
            candidate: c,
            amount,
        });
    }

    MintResolution {
        admitted,
        rejected,
        post_activation_ignored,
    }
}
