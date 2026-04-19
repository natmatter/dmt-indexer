//! Lightweight views over a Bitcoin Core block, fed by our RPC client.
//!
//! We don't use `bitcoincore-rpc` directly because we want a single HTTP
//! client (`reqwest`) shared between the RPC transport and the API
//! layer, and we want control over retry + backoff semantics.
//!
//! The views here are deliberately narrow: we only materialize the
//! fields the indexer actually consumes (bits/height/coinbase script /
//! output scripts / witness data for inscription parsing).

use bitcoin::Transaction;
use serde::{Deserialize, Serialize};

/// Decoded block body as returned by `getblock <hash> 0` (hex) plus the
/// header metadata we pull from `getblock <hash> 1` (summary JSON).
#[derive(Debug, Clone)]
pub struct BlockView {
    pub height: u64,
    pub hash: bitcoin::BlockHash,
    pub prev_hash: bitcoin::BlockHash,
    pub bits: u32,
    /// Nonce from the block header — used by deploys with
    /// `element_field=Nonce`.
    pub nonce: u32,
    pub timestamp: u32,
    /// Full transactions in block order. Index 0 is the coinbase.
    pub txs: Vec<TxView>,
}

#[derive(Debug, Clone)]
pub struct TxView {
    pub txid: bitcoin::Txid,
    pub is_coinbase: bool,
    pub tx: Transaction,
}

/// A per-output script + value, used for coinbase distribution and
/// inscription-carrier tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxOutView {
    pub value_sats: u64,
    pub script_pubkey_hex: String,
    /// Canonical address if we can decode the script for mainnet.
    pub address: Option<String>,
}

impl TxView {
    pub fn outputs(&self) -> impl Iterator<Item = &bitcoin::TxOut> {
        self.tx.output.iter()
    }

    pub fn inputs(&self) -> impl Iterator<Item = &bitcoin::TxIn> {
        self.tx.input.iter()
    }
}
