//! Main sync loop.
//!
//! Polls Bitcoin Core, fetches blocks from `cursor + 1` through
//! `tip - finality_depth`, and for each block applies the full TAP
//! protocol in a single redb write transaction. On panic we roll back
//! and the cursor stays put — forward progress is monotonic.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bitcoin::OutPoint;
use chrono::{DateTime, TimeZone, Utc};
use redb::ReadableTable;
use tokio::sync::broadcast;
use tokio::sync::Notify;
use tokio::time::sleep;
use tracing::{debug, info};

use crate::btc::{BlockView, RpcClient};
use crate::config::Config;
use crate::error::Result;
use crate::inscription::tracker::{
    InscriptionKind, InscriptionTracker, TrackedCarrier, TrackedInscription, TrackerMove,
};
use crate::ledger::coinbase::distribute;
use crate::ledger::deploy::Deployment;
use crate::ledger::event::{EventDelta, EventFamily, EventType, LedgerEvent};
use crate::ledger::mint::{resolve_mints, MintCandidate};
use crate::ledger::transfer::{
    resolve_transfer_inscribes, InscribeResolution, TransferInscribeCandidate,
};
use crate::protocol::address::{address_from_script, normalize_address};
use crate::protocol::control::ControlOp;
use crate::protocol::envelope::{decode_tap_payload, TapEnvelope};
use crate::store::codec::{decode, encode};
use crate::store::tables::{
    self, cursor_get, cursor_set, Cursor, DailyStats, InscriptionIndex, InscriptionOwner,
    MintClaim, PendingControl as StoredPendingControl, ValidTransfer, WalletState, ACTIVITY_RECENT,
    DAILY_STATS, DEPLOYMENTS, EVENTS, INSCRIPTIONS, INSCRIPTION_OWNERS, MINT_CLAIMS,
    PENDING_CONTROLS, STATS, TRANSFERABLES_BY_SENDER, VALID_TRANSFERS, WALLET_ACTIVITY,
    WALLET_STATE,
};
use crate::store::Store;
use crate::sync::reorg::{detect_reorg, rewind_cursor};

pub type EventBus = Arc<broadcast::Sender<LedgerEvent>>;

#[derive(Clone)]
pub struct SyncHandle {
    pub store: Store,
    pub bus: EventBus,
    pub shutdown: Arc<Notify>,
}

pub struct Syncer {
    cfg: Config,
    store: Store,
    rpc: RpcClient,
    bus: EventBus,
    shutdown: Arc<Notify>,
    next_event_id: u64,
    /// Bounded cache of block height → bits, avoiding redundant RPC
    /// fetches when many mints reference the same prior block.
    bits_cache: HashMap<u64, u32>,
    /// In-memory tracker state carried across blocks within a commit
    /// batch. Reloaded from redb on every commit boundary (where
    /// persisted state and in-memory state are in sync).
    tracker: Option<InscriptionTracker>,
    /// Snapshot of the carrier-map keys at the start of the commit
    /// batch. At commit time, we diff against this set to know which
    /// outpoints to remove from the persisted INSCRIPTION_OWNERS.
    tracker_baseline_keys: std::collections::HashSet<String>,
    /// Count of blocks applied in the current (uncommitted) batch.
    pending_blocks: u64,
    /// Events queued for broadcast after commit.
    pending_broadcasts: Vec<LedgerEvent>,
}

#[allow(dead_code)]
struct TransferInscribe {
    inscription_id: String,
    ticker: String,
    address: String,
    amount: u128,
}

struct ControlInscribe {
    inscription_id: String,
    ticker: String,
    address: String,
    op: ControlOp,
}

impl Syncer {
    pub fn new(cfg: Config, store: Store, rpc: RpcClient, bus: EventBus) -> Self {
        Self {
            cfg,
            store,
            rpc,
            bus,
            shutdown: Arc::new(Notify::new()),
            next_event_id: 0,
            bits_cache: HashMap::new(),
            tracker: None,
            tracker_baseline_keys: std::collections::HashSet::new(),
            pending_blocks: 0,
            pending_broadcasts: Vec::new(),
        }
    }

    pub fn handle(&self) -> SyncHandle {
        SyncHandle {
            store: self.store.clone(),
            bus: self.bus.clone(),
            shutdown: self.shutdown.clone(),
        }
    }

    pub async fn run(mut self) -> Result<()> {
        self.rpc.preflight().await?;
        self.seed_cursor_if_empty()?;
        self.seed_next_event_id()?;
        self.migrate_transferables_by_sender()?;

        loop {
            if let Some(rewind) =
                detect_reorg(&self.store, &self.rpc, self.cfg.sync.finality_depth).await?
            {
                rewind_cursor(&self.store, rewind)?;
            }
            let advanced = self.tick().await?;
            if !advanced {
                tokio::select! {
                    _ = self.shutdown.notified() => return Ok(()),
                    _ = sleep(Duration::from_millis(self.cfg.sync.poll_idle_ms)) => {},
                }
            }
        }
    }

    fn seed_cursor_if_empty(&self) -> Result<()> {
        let tx = self.store.read()?;
        let have_cursor = cursor_get(&tx)?.is_some();
        drop(tx);
        if have_cursor {
            self.seed_hardcoded_deployments()?;
            return Ok(());
        }
        let tx = self.store.write()?;
        cursor_set(
            &tx,
            &Cursor {
                height: self.cfg.sync.start_height.saturating_sub(1),
                block_hash: String::new(),
                updated_at: Some(Utc::now()),
            },
        )?;
        tx.commit()?;
        info!(start = self.cfg.sync.start_height, "seeded cursor");
        self.seed_hardcoded_deployments()?;
        Ok(())
    }

    /// Seed known mainnet DMT deployments so users don't need to
    /// sync from the deploy block to pick them up. Idempotent — skips
    /// any ticker already registered.
    fn seed_hardcoded_deployments(&self) -> Result<()> {
        use crate::ledger::deploy::{
            Deployment, NAT_COINBASE_ACTIVATION, NAT_MINER_TRANSFER_ACTIVATION,
        };
        use crate::protocol::bits::BitsMode;
        use crate::protocol::element::ElementField;
        use crate::protocol::ticker::normalize_ticker;

        let known = vec![Deployment {
            ticker: normalize_ticker("nat").unwrap(),
            deploy_inscription_id:
                "4d967af36dcacd7e6199c39bda855d7b1b37268f4c8031fed5403a99ac57fe67i0".to_string(),
            element_inscription_id:
                "63b5bd2e28c043c4812981718e65d202ab8f68c0f6a1834d9ebea49d8fac7e62i0".to_string(),
            element_field: ElementField::Bits,
            dt: Some("n".into()),
            dim: None,
            bits_mode: BitsMode::RawHex,
            activation_height: 817_709,
            coinbase_activation: Some(NAT_COINBASE_ACTIVATION),
            miner_transfer_activation: Some(NAT_MINER_TRANSFER_ACTIVATION),
            registered_at: Utc::now(),
        }];
        let wtx = self.store.write()?;
        {
            let mut table = wtx.open_table(DEPLOYMENTS)?;
            for d in known {
                if table.get(d.ticker.as_str())?.is_none() {
                    info!(ticker = d.ticker.as_str(), "seeded hardcoded deployment");
                    table.insert(d.ticker.as_str(), encode(&d)?.as_slice())?;
                }
            }
        }
        wtx.commit()?;
        Ok(())
    }

    fn seed_next_event_id(&mut self) -> Result<()> {
        let tx = self.store.read()?;
        let table = tx.open_table(EVENTS)?;
        let mut max_id: u64 = 0;
        for row in table.iter()? {
            let (k, _) = row?;
            let (_t, id) = k.value();
            if id > max_id {
                max_id = id;
            }
        }
        self.next_event_id = max_id + 1;
        Ok(())
    }

    /// Back-fill `TRANSFERABLES_BY_SENDER` from every unconsumed row in
    /// `VALID_TRANSFERS`. Idempotent via a `meta` flag — runs once per
    /// db on first boot after the index was added.
    fn migrate_transferables_by_sender(&self) -> Result<()> {
        const FLAG: &str = "migration_transferables_by_sender_v1";
        let rtx = self.store.read()?;
        let meta = rtx.open_table(tables::META)?;
        let already = meta.get(FLAG)?.is_some();
        drop(meta);
        drop(rtx);
        if already {
            return Ok(());
        }
        let wtx = self.store.write()?;
        let mut inserted = 0u64;
        {
            let mut idx = wtx.open_table(TRANSFERABLES_BY_SENDER)?;
            let vt = wtx.open_table(VALID_TRANSFERS)?;
            for row in vt.iter()? {
                let Ok((k, v)) = row else { continue };
                let Ok(vtrow) = decode::<ValidTransfer>(v.value()) else {
                    continue;
                };
                if vtrow.consumed_height.is_none() {
                    idx.insert(
                        (vtrow.ticker.as_str(), vtrow.sender.as_str(), k.value()),
                        1u8,
                    )?;
                    inserted += 1;
                }
            }
        }
        {
            let done: &[u8] = b"done";
            let mut meta = wtx.open_table(tables::META)?;
            meta.insert(FLAG, done)?;
        }
        wtx.commit()?;
        info!(inserted, "backfilled transferables_by_sender index");
        Ok(())
    }

    async fn tick(&mut self) -> Result<bool> {
        use futures_util::stream::{self, StreamExt};

        let cursor = {
            let rtx = self.store.read()?;
            cursor_get(&rtx)?.unwrap_or_default()
        };
        let (tip_height, _tip_hash) = self.rpc.tip().await?;
        let target = tip_height.saturating_sub(self.cfg.sync.finality_depth);
        if cursor.height >= target {
            // If there's an uncommitted batch (e.g. when we catch tip
            // mid-batch), flush now so /status reflects progress.
            self.commit_batch_if_pending()?;
            return Ok(false);
        }
        let first = cursor.height + 1;
        let last = std::cmp::min(first + self.cfg.sync.max_blocks_per_tick - 1, target);

        // Parallel prefetch: issue up to `parallelism` concurrent
        // (get_block_hash + get_block) pairs, preserve ordering via
        // `buffered`.
        let concurrency = self.cfg.sync.parallelism.max(1);
        let rpc = self.rpc.clone();
        let mut stream = stream::iter(first..=last)
            .map(move |h| {
                let rpc = rpc.clone();
                async move {
                    let hash = rpc.get_block_hash(h).await?;
                    let block = rpc.get_block(hash, h).await?;
                    Ok::<_, crate::error::Error>(block)
                }
            })
            .buffered(concurrency);

        // Open one write tx for up to `commit_interval` blocks, flushing
        // intermittently to amortize the fsync cost (ord's pattern).
        let commit_interval = self.cfg.sync.commit_interval.max(1);
        let mut wtx_opt: Option<redb::WriteTransaction> = Some(self.open_batch_wtx()?);

        while let Some(block_res) = stream.next().await {
            let block = block_res?;
            {
                let wtx = wtx_opt.as_ref().unwrap();
                self.apply_block(&block, wtx).await?;
            }
            self.pending_blocks += 1;

            if self.pending_blocks >= commit_interval {
                let wtx = wtx_opt.take().unwrap();
                self.finalize_and_commit_batch(wtx, block.height, block.hash.to_string())?;
                // Start a new batch for continuing blocks.
                wtx_opt = Some(self.open_batch_wtx()?);
            }
        }
        // Commit the tail of the range if any blocks applied.
        if let Some(wtx) = wtx_opt {
            if self.pending_blocks > 0 {
                // Use the LAST applied height/hash from cursor tracking in
                // the wtx (we already updated cursor inside apply_block).
                self.finalize_and_commit_batch(wtx, last, String::new())?;
            } else {
                // No blocks applied; abort the empty tx.
                drop(wtx);
            }
        }
        info!(
            first,
            last,
            tip = tip_height,
            concurrency,
            "applied block range"
        );
        Ok(true)
    }

    fn open_batch_wtx(&mut self) -> Result<redb::WriteTransaction> {
        let wtx = self.store.write()?;
        // Initialize the in-memory tracker from the freshly-opened
        // wtx. The tracker sees any writes committed before this
        // batch but nothing uncommitted (there's nothing uncommitted
        // — we just opened the tx).
        if self.tracker.is_none() {
            let (tracker, keys) = self.load_tracker_from(&wtx)?;
            self.tracker = Some(tracker);
            self.tracker_baseline_keys = keys;
        }
        Ok(wtx)
    }

    fn commit_batch_if_pending(&mut self) -> Result<()> {
        // Called when the loop is idle (caught up to tip). Nothing
        // to do — there are no uncommitted blocks because every
        // tick that processed blocks already committed its tail.
        Ok(())
    }

    fn finalize_and_commit_batch(
        &mut self,
        wtx: redb::WriteTransaction,
        last_height: u64,
        last_hash: String,
    ) -> Result<()> {
        // Persist carrier map diffs for the batch. We kept the tracker
        // in memory across all blocks in this batch; now flush once.
        if let Some(tracker) = self.tracker.as_ref() {
            let baseline = self.tracker_baseline_keys.clone();
            self.save_carriers(&wtx, tracker, &baseline)?;
        }
        // Advance cursor to the last applied height. apply_block sets
        // the cursor per-block, but only the committed write counts —
        // so the final cursor_set here is the one that matters.
        if !last_hash.is_empty() {
            cursor_set(
                &wtx,
                &Cursor {
                    height: last_height,
                    block_hash: last_hash,
                    updated_at: Some(Utc::now()),
                },
            )?;
        }
        wtx.commit()?;

        // After commit, reload the tracker baseline so the next batch
        // starts with the right diff reference.
        {
            let rtx = self.store.read()?;
            let mut keys = std::collections::HashSet::new();
            if let Ok(t) = rtx.open_table(INSCRIPTION_OWNERS) {
                for r in t.iter()? {
                    if let Ok((k, _)) = r {
                        keys.insert(k.value().to_string());
                    }
                }
            }
            self.tracker_baseline_keys = keys;
        }
        self.pending_blocks = 0;

        // Now broadcast all pending events. Subscribers only see events
        // that are durably committed.
        let events = std::mem::take(&mut self.pending_broadcasts);
        for ev in events {
            let _ = self.bus.send(ev);
        }
        Ok(())
    }

    async fn apply_block(&mut self, block: &BlockView, wtx: &redb::WriteTransaction) -> Result<()> {
        // Guard the tracker so an error inside apply_block_inner still
        // restores `self.tracker` — otherwise a subsequent apply would
        // panic on the `.take().expect(...)` below.
        let mut tracker_guard = self
            .tracker
            .take()
            .expect("tracker initialized at batch start");
        let result = self.apply_block_inner(block, wtx, &mut tracker_guard).await;
        self.tracker = Some(tracker_guard);
        result
    }

    async fn apply_block_inner(
        &mut self,
        block: &BlockView,
        wtx: &redb::WriteTransaction,
        tracker: &mut InscriptionTracker,
    ) -> Result<()> {
        debug!(height = block.height, "applying block");
        let occurred_at = Utc
            .timestamp_opt(block.timestamp as i64, 0)
            .single()
            .unwrap_or_else(Utc::now);

        // Load active deployments (by ticker) — reads from the current
        // write tx, so new deploys registered earlier in the same
        // batch are visible.
        let deployments = self.load_deployments_from(wtx)?;
        // Build intra-block UTXO cache: for any tx in this block we
        // already know its outputs without hitting RPC. Covers the
        // common case where a later tx in the same block spends an
        // earlier tx's output.
        let block_utxos: HashMap<bitcoin::Txid, Vec<u64>> = block
            .txs
            .iter()
            .map(|t| {
                (
                    t.txid,
                    t.tx.output.iter().map(|o| o.value.to_sat()).collect(),
                )
            })
            .collect();

        // -------- Parse phase (parallel per-tx) --------
        // The expensive per-tx work (envelope byte-scan + script parse
        // + TAP decode + brotli decompress) is independent across txs,
        // so we fan it out over rayon's thread pool. The serial work
        // that follows (ledger-state updates, tracker moves) sees each
        // tx in canonical block order.
        use rayon::prelude::*;
        struct ParsedEnvelope {
            /// Ord's per-tx envelope global_index (used in inscription_id).
            global_index: u32,
            /// Position within the tx's envelope list (for within-block rank).
            env_pos: u32,
            payload: TapEnvelope,
        }
        struct PreParsedTx {
            tx_index: u32,
            txid: bitcoin::Txid,
            envelopes: Vec<ParsedEnvelope>,
        }
        let block_height = block.height;
        let pre_parsed: Vec<PreParsedTx> = block
            .txs
            .par_iter()
            .enumerate()
            .map(|(tx_index, txv)| {
                let envelopes =
                    crate::inscription::envelope::parse_envelopes_at_height(&txv.tx, block_height);
                let mut with_payloads = Vec::new();
                for (env_pos, env) in envelopes.iter().enumerate() {
                    if let Ok(Some(payload)) = decode_tap_payload(env) {
                        let global_index = match env.kind {
                            crate::inscription::envelope::EnvelopeKind::Inscription { index } => {
                                index
                            }
                        };
                        with_payloads.push(ParsedEnvelope {
                            global_index,
                            env_pos: env_pos as u32,
                            payload,
                        });
                    }
                }
                PreParsedTx {
                    tx_index: tx_index as u32,
                    txid: txv.txid,
                    envelopes: with_payloads,
                }
            })
            .collect();

        let mut mint_cands: HashMap<String, Vec<MintCandidate>> = HashMap::new();
        let mut transfer_inscribes_unresolved: Vec<TransferInscribeCandidate> = Vec::new();
        let mut transfer_inscribe_meta: HashMap<String, TransferInscribe> = HashMap::new();
        let mut control_inscribes: Vec<ControlInscribe> = Vec::new();
        let mut fresh_carriers: Vec<(OutPoint, TrackedCarrier, Option<String>)> = Vec::new();
        let mut moves: Vec<(u32, TrackerMove)> = Vec::new();
        let mut inscription_rows: Vec<(String, InscriptionIndex)> = Vec::new();

        for pre in &pre_parsed {
            let tx_index = pre.tx_index;
            let txv = &block.txs[tx_index as usize];
            // Snapshot fresh_carriers length before this tx so we can
            // register just this tx's new inscriptions into the tracker
            // after its own spend-check pass. Required for same-block
            // inscribe+move: tx A reveals an inscription, tx B later in
            // the same block spends that reveal UTXO. Without this, B's
            // spend check never matches because the carrier wasn't in
            // the tracker yet.
            let carriers_before = fresh_carriers.len();
            for pe in &pre.envelopes {
                // Per ord's default landing rule: envelope K's inscription
                // sits on the first sat of output K. TAP credits the mint
                // / carries the transfer to the ADDRESS owning that sat —
                // which is output K's address, not always output 0. For
                // batch reveals with distinct recipient outputs this is
                // material; previously we always used vout 0 and misattributed
                // envelopes past the first.
                let landing_vout = pe.env_pos as usize;
                let to_addr = txv.tx.output.get(landing_vout).and_then(|o| {
                    if o.script_pubkey.is_op_return() {
                        None
                    } else {
                        address_from_script(&o.script_pubkey)
                    }
                });
                // Deterministic within-block ordering: encodes
                // (tx_index, env_pos) so downstream sorts see a
                // stable, canonical order without needing chain-wide
                // inscription_number.
                let within_block_rank = i64::from(tx_index) * 1_000_000 + pe.env_pos as i64;
                let insc_id = format!("{}i{}", pre.txid, pe.global_index);
                // Take payload by clone to keep pre_parsed immutable
                // for now; could be optimized later.
                let payload = pe.payload.clone();
                // dmt-indexer v0.1.0 indexes only $NAT. Non-NAT
                // DMT/TAP inscriptions (even well-formed ones) are
                // skipped here without being registered, so the rest
                // of the pipeline never wastes work on them. The
                // ticker-filter is the single source of truth for our
                // scope; every code path below assumes ticker == "nat".
                if !is_indexed_ticker(payload.ticker()) {
                    continue;
                }
                // Per DMT docs, `token-transfer` inscriptions carry the
                // long-form ticker `dmt-<name>` while `dmt-deploy` and
                // `dmt-mint` carry the short form `<name>`. Strip the
                // prefix so the rest of the pipeline sees one canonical
                // ticker string regardless of which op produced it.
                let tick_canon = canonical_ticker(payload.ticker());
                match payload {
                    TapEnvelope::Deploy(_dp) => {
                        // NAT is seeded at startup. A fresh on-chain
                        // `nat` deploy would hit this arm, find the
                        // ticker already registered, and drop. Record
                        // an inscription row for diagnostic visibility
                        // of the attempt.
                        inscription_rows.push((
                            insc_id,
                            InscriptionIndex {
                                ticker: "nat".to_string(),
                                kind: "dmt-deploy".to_string(),
                                original_amount: None,
                                inscribed_height: block.height,
                                current_owner_address: to_addr.clone(),
                                consumed_height: None,
                            },
                        ));
                    }
                    TapEnvelope::Mint(mp) => {
                        let Some(dep) = deployments.get(tick_canon.as_str()) else {
                            continue;
                        };
                        // Short-circuit bogus `blk` values so we don't
                        // RPC a non-existent height (which would fail
                        // with code=-8 and kill the tick). Downstream
                        // rejects them as blk_out_of_range.
                        let referenced_bits =
                            if mp.block_number == 0 || mp.block_number > block.height {
                                0
                            } else if mp.block_number == block.height {
                                block.bits
                            } else if let Some(&b) = self.bits_cache.get(&mp.block_number) {
                                b
                            } else {
                                let b = fetch_block_bits_retry(&self.rpc, mp.block_number).await?;
                                if self.bits_cache.len() > 4096 {
                                    self.bits_cache.clear();
                                }
                                self.bits_cache.insert(mp.block_number, b);
                                b
                            };
                        let Some(addr) = to_addr.as_deref().and_then(normalize_address) else {
                            continue;
                        };
                        mint_cands
                            .entry(dep.ticker.as_str().to_string())
                            .or_default()
                            .push(MintCandidate {
                                inscription_id: insc_id.clone(),
                                inscription_number: within_block_rank,
                                inscribed_block_height: block.height,
                                address: addr,
                                payload: mp.clone(),
                                referenced_bits,
                                tx_index,
                            });
                        // Track the mint inscription as a carrier so its
                        // `INSCRIPTIONS.current_owner_address` updates
                        // when the UNAT moves. Balance is not affected —
                        // mint credits stay with the original inscriber.
                        let reveal_value = txv
                            .tx
                            .output
                            .get(landing_vout)
                            .map(|o| o.value.to_sat())
                            .unwrap_or(0);
                        fresh_carriers.push((
                            OutPoint {
                                txid: txv.txid,
                                vout: landing_vout as u32,
                            },
                            TrackedCarrier {
                                inscription: TrackedInscription {
                                    inscription_id: insc_id.clone(),
                                    ticker: tick_canon.clone(),
                                    kind: InscriptionKind::Mint,
                                },
                                offset_in_outpoint: 0,
                                outpoint_value_sats: reveal_value,
                            },
                            to_addr.clone(),
                        ));
                        inscription_rows.push((
                            insc_id,
                            InscriptionIndex {
                                ticker: tick_canon.clone(),
                                kind: "dmt-mint".to_string(),
                                original_amount: None,
                                inscribed_height: block.height,
                                current_owner_address: to_addr.clone(),
                                consumed_height: None,
                            },
                        ));
                    }
                    TapEnvelope::Transfer(tp) => {
                        let Some(_) = deployments.get(tick_canon.as_str()) else {
                            continue;
                        };
                        let Some(addr) = to_addr.as_deref().and_then(normalize_address) else {
                            continue;
                        };
                        transfer_inscribes_unresolved.push(TransferInscribeCandidate {
                            inscription_id: insc_id.clone(),
                            inscription_number: within_block_rank,
                            inscribed_block_height: block.height,
                            ticker: tick_canon.clone(),
                            address: addr.clone(),
                            amount: tp.amount,
                            tx_index,
                        });
                        transfer_inscribe_meta.insert(
                            insc_id.clone(),
                            TransferInscribe {
                                inscription_id: insc_id.clone(),
                                ticker: tick_canon.clone(),
                                address: addr.as_str().to_string(),
                                amount: tp.amount,
                            },
                        );
                        let reveal_value = txv
                            .tx
                            .output
                            .get(landing_vout)
                            .map(|o| o.value.to_sat())
                            .unwrap_or(0);
                        fresh_carriers.push((
                            OutPoint {
                                txid: txv.txid,
                                vout: landing_vout as u32,
                            },
                            TrackedCarrier {
                                inscription: TrackedInscription {
                                    inscription_id: insc_id.clone(),
                                    ticker: tick_canon.clone(),
                                    kind: InscriptionKind::TokenTransfer,
                                },
                                offset_in_outpoint: 0,
                                outpoint_value_sats: reveal_value,
                            },
                            to_addr.clone(),
                        ));
                        inscription_rows.push((
                            insc_id,
                            InscriptionIndex {
                                ticker: tick_canon.clone(),
                                kind: "token-transfer".to_string(),
                                original_amount: Some(tp.amount),
                                inscribed_height: block.height,
                                current_owner_address: to_addr.clone(),
                                consumed_height: None,
                            },
                        ));
                    }
                    TapEnvelope::Control(cp) => {
                        let Some(_) = deployments.get(tick_canon.as_str()) else {
                            continue;
                        };
                        let Some(addr) = to_addr.as_deref().and_then(normalize_address) else {
                            continue;
                        };
                        control_inscribes.push(ControlInscribe {
                            inscription_id: insc_id.clone(),
                            ticker: tick_canon.clone(),
                            address: addr.as_str().to_string(),
                            op: cp.op,
                        });
                        let reveal_value = txv
                            .tx
                            .output
                            .get(landing_vout)
                            .map(|o| o.value.to_sat())
                            .unwrap_or(0);
                        fresh_carriers.push((
                            OutPoint {
                                txid: txv.txid,
                                vout: landing_vout as u32,
                            },
                            TrackedCarrier {
                                inscription: TrackedInscription {
                                    inscription_id: insc_id.clone(),
                                    ticker: tick_canon.clone(),
                                    kind: InscriptionKind::Control,
                                },
                                offset_in_outpoint: 0,
                                outpoint_value_sats: reveal_value,
                            },
                            to_addr.clone(),
                        ));
                        inscription_rows.push((
                            insc_id,
                            InscriptionIndex {
                                ticker: tick_canon.clone(),
                                kind: "control".to_string(),
                                original_amount: None,
                                inscribed_height: block.height,
                                current_owner_address: to_addr.clone(),
                                consumed_height: None,
                            },
                        ));
                    }
                }
            }
            // Carrier movement for this tx. Runs BEFORE registering
            // this tx's own fresh carriers — a tx cannot spend its own
            // outputs, so nothing this tx just inscribed can be moved
            // by this tx. Skip the RPC round-trip when no input matches
            // a tracked outpoint.
            let spends_tracked = txv
                .tx
                .input
                .iter()
                .any(|i| tracker.get(&i.previous_output).is_some());
            if spends_tracked {
                let input_values = self
                    .load_input_values(&txv.tx, &tracker, &block_utxos)
                    .await?;
                let tx_moves = tracker.apply_tx(&txv.tx, &input_values);
                for m in tx_moves {
                    moves.push((tx_index, m));
                }
            }

            // Register THIS tx's new carriers so subsequent txs in the
            // same block can detect them as move targets. Without this
            // step, same-block inscribe+move patterns silently lose
            // the move event, leaving the inscription stuck on the
            // inscriber in our tracker while the ledger shows no debit
            // and the recipient no credit.
            for (outpoint, carrier, _addr) in &fresh_carriers[carriers_before..] {
                tracker.insert(*outpoint, carrier.clone());
            }
        }

        // Resolve transfer inscribes serially
        let avail_snapshot = self.snapshot_available_from(wtx, &transfer_inscribes_unresolved)?;
        let inscribe_resolutions =
            resolve_transfer_inscribes(transfer_inscribes_unresolved, &avail_snapshot);

        // Resolve mints per ticker
        let mut mint_resolutions = Vec::new();
        for (ticker, cands) in mint_cands {
            let Some(dep) = deployments.get(&ticker) else {
                continue;
            };
            let claimed = self.load_claimed_blocks_from(wtx, &ticker)?;
            mint_resolutions.push((ticker, resolve_mints(dep, block.height, &claimed, cands)));
        }

        // Coinbase distribution
        let mut coinbase_shares_per_ticker = Vec::new();
        for (ticker, dep) in &deployments {
            if let Some(activation) = dep.coinbase_activation {
                if block.height >= activation {
                    let shares = distribute(dep, &block.txs[0].tx, block.height, block.bits);
                    if !shares.is_empty() {
                        coinbase_shares_per_ticker.push((ticker.clone(), shares));
                    }
                }
            }
        }

        // -------- Write phase (reuses caller's wtx across blocks) --------
        let mut events: Vec<LedgerEvent> = Vec::new();
        let block_hash = block.hash.to_string();

        // (Deploys are not auto-registered — v0.1.0 indexes only the
        // NAT deployment, which is seeded at startup.)

        // 2. Transfer inscribes
        for r in inscribe_resolutions {
            match r {
                InscribeResolution::Admitted {
                    inscription_id,
                    ticker,
                    address,
                    amount,
                    inscribed_height,
                    tx_index,
                } => {
                    {
                        let mut table = wtx.open_table(VALID_TRANSFERS)?;
                        let v = ValidTransfer {
                            ticker: ticker.clone(),
                            sender: address.as_str().to_string(),
                            amount,
                            inscribed_height,
                            consumed_height: None,
                        };
                        table.insert(inscription_id.as_str(), encode(&v)?.as_slice())?;
                    }
                    // Maintain `transferables_by_sender` so
                    // `/wallets/:addr/transferables` can range-scan
                    // instead of full-table scan.
                    {
                        let mut idx = wtx.open_table(TRANSFERABLES_BY_SENDER)?;
                        idx.insert(
                            (ticker.as_str(), address.as_str(), inscription_id.as_str()),
                            1u8,
                        )?;
                    }
                    let a = i128::try_from(amount).unwrap_or(i128::MAX);
                    apply_wallet_delta(&wtx, &ticker, address.as_str(), 0, -a, a, 0, occurred_at)?;
                    events.push(self.new_event(
                        &ticker,
                        EventFamily::Transfer,
                        EventType::TokenTransferInscribeAdmitted,
                        block,
                        occurred_at,
                        Some(tx_index),
                        Some(inscription_id),
                        Some(address.as_str().to_string()),
                        None,
                        EventDelta {
                            delta_available: -a,
                            delta_transferable: a,
                            ..Default::default()
                        },
                        serde_json::json!({ "amount": amount }),
                        &block_hash,
                    ));
                }
                InscribeResolution::Skipped {
                    inscription_id,
                    ticker,
                    address,
                    attempted_amount,
                    snapshot_available,
                    inscribed_height: _,
                    tx_index,
                } => {
                    events.push(self.new_event(
                        &ticker,
                        EventFamily::Transfer,
                        EventType::TokenTransferSkippedSemantic,
                        block,
                        occurred_at,
                        Some(tx_index),
                        Some(inscription_id),
                        Some(address.as_str().to_string()),
                        None,
                        EventDelta::default(),
                        serde_json::json!({
                            "amount": attempted_amount,
                            "available": snapshot_available,
                            "reason": "insufficient_available",
                        }),
                        &block_hash,
                    ));
                }
            }
        }

        // 3. Control inscribes
        for c in control_inscribes {
            {
                let mut table = wtx.open_table(PENDING_CONTROLS)?;
                let v = StoredPendingControl {
                    ticker: c.ticker.clone(),
                    address: c.address.clone(),
                    op: match c.op {
                        ControlOp::Block => "block".to_string(),
                        ControlOp::Unblock => "unblock".to_string(),
                    },
                    inscribed_height: block.height,
                };
                table.insert(c.inscription_id.as_str(), encode(&v)?.as_slice())?;
            }
            let et = match c.op {
                ControlOp::Block => EventType::BlockTransferablesInscribed,
                ControlOp::Unblock => EventType::UnblockTransferablesInscribed,
            };
            events.push(self.new_event(
                &c.ticker,
                EventFamily::Control,
                et,
                block,
                occurred_at,
                None,
                Some(c.inscription_id),
                Some(c.address),
                None,
                EventDelta::default(),
                serde_json::json!({}),
                &block_hash,
            ));
        }

        // 4. Mints
        for (ticker, res) in mint_resolutions {
            for admitted in res.admitted {
                let amount = admitted.amount;
                let addr = admitted.candidate.address.as_str().to_string();
                let blk = admitted.candidate.payload.block_number;
                {
                    let mut table = wtx.open_table(MINT_CLAIMS)?;
                    let v = MintClaim {
                        winning_inscription_id: admitted.candidate.inscription_id.clone(),
                        winning_inscription_number: admitted.candidate.inscription_number,
                        inscribed_height: admitted.candidate.inscribed_block_height,
                        amount,
                    };
                    table.insert((ticker.as_str(), blk), encode(&v)?.as_slice())?;
                }
                let a = i128::try_from(amount).unwrap_or(i128::MAX);
                apply_wallet_delta(&wtx, &ticker, &addr, a, a, 0, 0, occurred_at)?;
                events.push(self.new_event(
                    &ticker,
                    EventFamily::Mint,
                    EventType::DmtMintCredit,
                    block,
                    occurred_at,
                    Some(admitted.candidate.tx_index),
                    Some(admitted.candidate.inscription_id),
                    Some(addr),
                    None,
                    EventDelta {
                        delta_total: a,
                        delta_available: a,
                        ..Default::default()
                    },
                    serde_json::json!({ "blk": blk, "amount": amount }),
                    &block_hash,
                ));
            }
            for rejected in res.rejected {
                if rejected.reason == "zero_amount_scarcity" {
                    continue;
                }
                events.push(self.new_event(
                    &ticker,
                    EventFamily::Mint,
                    EventType::DmtMintDuplicateRejected,
                    block,
                    occurred_at,
                    Some(rejected.candidate.tx_index),
                    Some(rejected.candidate.inscription_id),
                    Some(rejected.candidate.address.as_str().to_string()),
                    None,
                    EventDelta::default(),
                    serde_json::json!({ "reason": rejected.reason }),
                    &block_hash,
                ));
            }
            for skipped in res.post_activation_ignored {
                events.push(self.new_event(
                    &ticker,
                    EventFamily::Mint,
                    EventType::DmtMintIgnoredPostActivation,
                    block,
                    occurred_at,
                    Some(skipped.candidate.tx_index),
                    Some(skipped.candidate.inscription_id),
                    Some(skipped.candidate.address.as_str().to_string()),
                    None,
                    EventDelta::default(),
                    serde_json::json!({}),
                    &block_hash,
                ));
            }
        }

        // 5. Coinbase distribution
        for (ticker, shares) in coinbase_shares_per_ticker {
            for share in shares {
                if share.share_amount == 0 {
                    continue;
                }
                let a = i128::try_from(share.share_amount).unwrap_or(i128::MAX);
                if share.is_burn {
                    events.push(self.new_event(
                        &ticker,
                        EventFamily::Coinbase,
                        EventType::CoinbaseRewardBurned,
                        block,
                        occurred_at,
                        Some(0),
                        None,
                        None,
                        None,
                        EventDelta {
                            delta_burned: a,
                            ..Default::default()
                        },
                        serde_json::json!({
                            "value_sats": share.value_sats,
                            "amount": share.share_amount,
                        }),
                        &block_hash,
                    ));
                    continue;
                }
                if let Some(addr) = share.address.clone() {
                    let is_first_credit = wallet_is_new(&wtx, &ticker, &addr)?;
                    apply_wallet_delta(&wtx, &ticker, &addr, a, a, 0, 0, occurred_at)?;
                    // Only set the lock flag on the wallet's FIRST
                    // post-activation coinbase credit. This prevents a
                    // later miner credit from re-locking a wallet the
                    // owner has already run `unblock-transferables` on.
                    if share.should_lock_on_first_credit && is_first_credit {
                        set_wallet_locked(&wtx, &ticker, &addr)?;
                    }
                    events.push(self.new_event(
                        &ticker,
                        EventFamily::Coinbase,
                        if share.should_lock_on_first_credit && is_first_credit {
                            EventType::CoinbaseRewardLocked
                        } else {
                            EventType::CoinbaseRewardCredit
                        },
                        block,
                        occurred_at,
                        Some(0),
                        None,
                        Some(addr),
                        None,
                        EventDelta {
                            delta_total: a,
                            delta_available: a,
                            ..Default::default()
                        },
                        serde_json::json!({
                            "value_sats": share.value_sats,
                            "amount": share.share_amount,
                        }),
                        &block_hash,
                    ));
                }
            }
        }

        // 6. Apply moves (settle transfers + tap controls + burns)
        for (_, mv) in moves {
            // Update INSCRIPTIONS owner + consumed_height.
            if let Some((insc_id, new_owner, consumed)) = describe_move(&mv, block.height) {
                let mut table = wtx.open_table(INSCRIPTIONS)?;
                let existing: Option<InscriptionIndex> = {
                    let raw = table.get(insc_id.as_str())?;
                    raw.and_then(|v| decode::<InscriptionIndex>(v.value()).ok())
                };
                if let Some(mut ix) = existing {
                    ix.current_owner_address = new_owner;
                    if consumed {
                        ix.consumed_height = Some(block.height);
                    }
                    table.insert(insc_id.as_str(), encode(&ix)?.as_slice())?;
                }
            }
            match mv {
                TrackerMove::Moved {
                    inscription,
                    from: _,
                    to: _,
                    to_offset: _,
                    to_outpoint_value_sats: _,
                    new_owner_address,
                } => {
                    match inscription.kind {
                        InscriptionKind::TokenTransfer => {
                            self.settle_transfer(
                                &wtx,
                                &inscription.ticker,
                                &inscription.inscription_id,
                                new_owner_address.as_deref(),
                                block,
                                occurred_at,
                                &block_hash,
                                &mut events,
                            )?;
                        }
                        InscriptionKind::Control => {
                            self.tap_control(
                                &wtx,
                                &inscription.ticker,
                                &inscription.inscription_id,
                                new_owner_address.as_deref(),
                                block,
                                occurred_at,
                                &block_hash,
                                &mut events,
                            )?;
                        }
                        InscriptionKind::Mint => {
                            // No ledger effect — the INSCRIPTIONS owner
                            // column was already refreshed above.
                        }
                    }
                }
                TrackerMove::Burned { inscription, .. } => {
                    if matches!(inscription.kind, InscriptionKind::TokenTransfer) {
                        self.burn_transfer(
                            &wtx,
                            &inscription.ticker,
                            &inscription.inscription_id,
                            block,
                            occurred_at,
                            &block_hash,
                            &mut events,
                        )?;
                    }
                    // Mint / control burns: owner column cleared above,
                    // no ledger event needed.
                }
            }
        }

        // 7. Persist inscription index, carrier map, events, rollups
        {
            let mut table = wtx.open_table(INSCRIPTIONS)?;
            for (id, ix) in inscription_rows {
                table.insert(id.as_str(), encode(&ix)?.as_slice())?;
            }
        }
        // Tracker writes are deferred to commit time; caller's guard
        // will restore `self.tracker`.

        {
            let mut table = wtx.open_table(EVENTS)?;
            for ev in &events {
                table.insert((ev.ticker.as_str(), ev.event_id), encode(ev)?.as_slice())?;
            }
        }
        {
            let mut table = wtx.open_table(ACTIVITY_RECENT)?;
            let mut tickers_touched: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            for ev in &events {
                let inv = tables::inv_u64(ev.occurred_at.timestamp().max(0) as u64);
                table.insert(
                    (ev.ticker.as_str(), inv, ev.event_id),
                    encode(ev)?.as_slice(),
                )?;
                tickers_touched.insert(ev.ticker.clone());
            }
            // Row-count based prune (per lesson in IMPLEMENTATION_PLAN):
            // keep newest 10_000 entries per ticker. Keys in
            // ACTIVITY_RECENT are (ticker, inverted_ts_be, event_id)
            // so a forward iterator on the ticker's range gives
            // newest-first; anything past index 10_000 gets deleted.
            const MAX_PER_TICKER: usize = 10_000;
            for ticker in tickers_touched {
                let lo = (ticker.as_str(), 0u64, 0u64);
                let hi = (ticker.as_str(), u64::MAX, u64::MAX);
                let to_delete: Vec<(String, u64, u64)> = table
                    .range(lo..=hi)?
                    .filter_map(|r| r.ok())
                    .enumerate()
                    .filter(|(i, _)| *i >= MAX_PER_TICKER)
                    .map(|(_, (k, _))| {
                        let (t, inv, id) = k.value();
                        (t.to_string(), inv, id)
                    })
                    .collect();
                for (t, inv, id) in to_delete {
                    table.remove((t.as_str(), inv, id))?;
                }
            }
        }
        {
            let mut table = wtx.open_table(WALLET_ACTIVITY)?;
            for ev in &events {
                let inv = tables::inv_u64(ev.occurred_at.timestamp().max(0) as u64);
                // Index under both address and counterparty so both
                // sides of a transfer see the event in their per-wallet
                // feed.
                for addr in [&ev.address, &ev.counterparty_address].iter().copied() {
                    if let Some(a) = addr {
                        table.insert(
                            (ev.ticker.as_str(), a.as_str(), inv, ev.event_id),
                            encode(ev)?.as_slice(),
                        )?;
                    }
                }
            }
        }
        {
            let mut stats = wtx.open_table(DAILY_STATS)?;
            let mut actives = wtx.open_table(tables::DAILY_ACTIVE_ADDRESSES)?;
            for ev in &events {
                let day = (ev.occurred_at.timestamp() / 86400) as u32;
                let key = (ev.ticker.as_str(), day);
                let existing: DailyStats = stats
                    .get(key)?
                    .and_then(|v| decode::<DailyStats>(v.value()).ok())
                    .unwrap_or_default();
                let mut upd = existing;
                match ev.event_type {
                    EventType::DmtMintCredit
                    | EventType::CoinbaseRewardCredit
                    | EventType::CoinbaseRewardLocked => {
                        upd.minted = upd
                            .minted
                            .saturating_add(u128::try_from(ev.delta.delta_total).unwrap_or(0));
                    }
                    EventType::TokenTransferCredit => {
                        upd.transfer_count += 1;
                        upd.volume = upd
                            .volume
                            .saturating_add(u128::try_from(ev.delta.delta_total).unwrap_or(0));
                    }
                    EventType::TokenTransferBurned => {
                        upd.burned = upd
                            .burned
                            .saturating_add(u128::try_from(ev.delta.delta_burned).unwrap_or(0));
                    }
                    _ => {}
                }
                // Count distinct addresses touched per (ticker, day). We
                // insert each non-None address into DAILY_ACTIVE_ADDRESSES
                // and bump the counter only on first insert.
                for maybe_addr in [&ev.address, &ev.counterparty_address].iter().copied() {
                    if let Some(addr) = maybe_addr {
                        let ak = (ev.ticker.as_str(), day, addr.as_str());
                        if actives.get(ak)?.is_none() {
                            actives.insert(ak, 1u8)?;
                            upd.active_addresses = upd.active_addresses.saturating_add(1);
                        }
                    }
                }
                stats.insert(key, encode(&upd)?.as_slice())?;
            }
        }
        {
            let mut table = wtx.open_table(STATS)?;
            let summary = serde_json::json!({
                "tip_height": block.height,
                "tip_hash": block.hash.to_string(),
                "updated_at": Utc::now().to_rfc3339(),
            });
            table.insert("sync_summary", encode(&summary)?.as_slice())?;
        }

        cursor_set(
            &wtx,
            &Cursor {
                height: block.height,
                block_hash: block_hash.clone(),
                updated_at: Some(Utc::now()),
            },
        )?;

        // No commit here — the sync loop batches across
        // `commit_interval` blocks. Broadcasts are also deferred until
        // after commit so subscribers only ever see durable state.
        self.pending_broadcasts.extend(events);

        Ok(())
    }

    fn load_deployments_from(
        &self,
        wtx: &redb::WriteTransaction,
    ) -> Result<HashMap<String, Deployment>> {
        let table = wtx.open_table(DEPLOYMENTS)?;
        let mut out = HashMap::new();
        for row in table.iter()? {
            let (k, v) = row?;
            let dep: Deployment = decode(v.value())?;
            out.insert(k.value().to_string(), dep);
        }
        Ok(out)
    }

    /// Resolve every input of a tx to its value-in-sats.
    ///
    /// Lookup order (cheapest first):
    ///   1. Tracker cache — if we're tracking the carrier, we already
    ///      know its outpoint value.
    ///   2. Intra-block cache — if the prev tx is in the SAME block,
    ///      we have its outputs in memory; no RPC needed.
    ///   3. Batched concurrent RPC — for genuinely external prev txs,
    ///      fetch them all in parallel via `try_join_all`. Previously
    ///      this loop was serial and could hit ~50 RPCs per block in
    ///      transfer-heavy windows, serializing ~1s of latency per
    ///      block.
    async fn load_input_values(
        &mut self,
        tx: &bitcoin::Transaction,
        tracker: &InscriptionTracker,
        block_utxos: &HashMap<bitcoin::Txid, Vec<u64>>,
    ) -> Result<HashMap<OutPoint, u64>> {
        let mut out = HashMap::with_capacity(tx.input.len());
        let mut missing: Vec<OutPoint> = Vec::new();

        for txin in &tx.input {
            let op = txin.previous_output;
            if let Some(c) = tracker.get(&op) {
                out.insert(op, c.outpoint_value_sats);
                continue;
            }
            if let Some(vals) = block_utxos.get(&op.txid) {
                let v = vals.get(op.vout as usize).copied().unwrap_or(0);
                out.insert(op, v);
                continue;
            }
            missing.push(op);
        }

        if !missing.is_empty() {
            // Deduplicate by txid so we only fetch each prev tx once.
            let mut unique_txids: Vec<bitcoin::Txid> = missing.iter().map(|op| op.txid).collect();
            unique_txids.sort();
            unique_txids.dedup();
            // ONE batched JSON-RPC round-trip for all prev txs at once.
            let txs = self.rpc.get_raw_transactions_batch(&unique_txids).await?;
            let fetched: HashMap<bitcoin::Txid, Vec<u64>> = unique_txids
                .into_iter()
                .zip(txs.into_iter())
                .filter_map(|(txid, maybe_tx)| {
                    maybe_tx.map(|tx| (txid, tx.output.iter().map(|o| o.value.to_sat()).collect()))
                })
                .collect();
            for op in missing {
                let v = fetched
                    .get(&op.txid)
                    .and_then(|vec| vec.get(op.vout as usize).copied())
                    .unwrap_or(0);
                out.insert(op, v);
            }
        }
        Ok(out)
    }

    fn load_tracker_from(
        &self,
        wtx: &redb::WriteTransaction,
    ) -> Result<(InscriptionTracker, std::collections::HashSet<String>)> {
        let table = wtx.open_table(INSCRIPTION_OWNERS)?;
        let mut t = InscriptionTracker::new();
        let mut keys = std::collections::HashSet::new();
        for row in table.iter()? {
            let (k, v) = row?;
            let owner: InscriptionOwner = decode(v.value())?;
            keys.insert(k.value().to_string());
            let op: OutPoint = match parse_outpoint(&owner.current_outpoint) {
                Some(op) => op,
                None => continue,
            };
            t.insert(
                op,
                TrackedCarrier {
                    inscription: TrackedInscription {
                        inscription_id: owner.inscription_id.clone(),
                        ticker: owner.ticker.clone(),
                        kind: match owner.kind.as_str() {
                            "token-transfer" => InscriptionKind::TokenTransfer,
                            "control" => InscriptionKind::Control,
                            "dmt-mint" => InscriptionKind::Mint,
                            _ => InscriptionKind::Control,
                        },
                    },
                    offset_in_outpoint: owner.offset_in_outpoint,
                    outpoint_value_sats: owner.outpoint_value_sats,
                },
            );
        }
        Ok((t, keys))
    }

    fn load_claimed_blocks_from(
        &self,
        wtx: &redb::WriteTransaction,
        ticker: &str,
    ) -> Result<HashSet<u64>> {
        let table = wtx.open_table(MINT_CLAIMS)?;
        let mut out = HashSet::new();
        let lo = (ticker, 0u64);
        let hi = (ticker, u64::MAX);
        for row in table.range(lo..=hi)? {
            let (k, _) = row?;
            let (_, blk) = k.value();
            out.insert(blk);
        }
        Ok(out)
    }

    fn snapshot_available_from(
        &self,
        wtx: &redb::WriteTransaction,
        candidates: &[TransferInscribeCandidate],
    ) -> Result<HashMap<(String, String), i128>> {
        let table = wtx.open_table(WALLET_STATE)?;
        let mut out = HashMap::new();
        for c in candidates {
            let key = (c.ticker.clone(), c.address.as_str().to_string());
            if out.contains_key(&key) {
                continue;
            }
            let state: Option<WalletState> = table
                .get((c.ticker.as_str(), c.address.as_str()))?
                .and_then(|v| decode(v.value()).ok());
            let avail = state.map(|s| s.available).unwrap_or(0);
            out.insert(key, avail);
        }
        Ok(out)
    }

    fn save_carriers(
        &self,
        wtx: &redb::WriteTransaction,
        tracker: &InscriptionTracker,
        previous_keys: &std::collections::HashSet<String>,
    ) -> Result<()> {
        // Delta write: snapshot → diff vs `previous_keys`. Removed keys
        // are deleted; added/updated keys are written. At steady state
        // most blocks produce O(1) carrier changes.
        let mut table = wtx.open_table(INSCRIPTION_OWNERS)?;
        let current: Vec<(OutPoint, TrackedCarrier)> = tracker_iter(tracker);
        let mut current_keys: std::collections::HashSet<String> =
            std::collections::HashSet::with_capacity(current.len());
        for (op, carrier) in current {
            let key = format!("{}:{}", op.txid, op.vout);
            current_keys.insert(key.clone());
            let v = InscriptionOwner {
                inscription_id: carrier.inscription.inscription_id.clone(),
                ticker: carrier.inscription.ticker.clone(),
                kind: match carrier.inscription.kind {
                    InscriptionKind::TokenTransfer => "token-transfer".to_string(),
                    InscriptionKind::Control => "control".to_string(),
                    InscriptionKind::Mint => "dmt-mint".to_string(),
                },
                current_outpoint: key.clone(),
                offset_in_outpoint: carrier.offset_in_outpoint,
                outpoint_value_sats: carrier.outpoint_value_sats,
            };
            table.insert(key.as_str(), encode(&v)?.as_slice())?;
        }
        for removed in previous_keys.difference(&current_keys) {
            table.remove(removed.as_str())?;
        }
        Ok(())
    }

    fn settle_transfer(
        &mut self,
        wtx: &redb::WriteTransaction,
        ticker: &str,
        inscription_id: &str,
        new_owner: Option<&str>,
        block: &BlockView,
        occurred_at: DateTime<Utc>,
        block_hash: &str,
        events: &mut Vec<LedgerEvent>,
    ) -> Result<()> {
        // Look up validity row
        let (sender, amount) = {
            let mut table = wtx.open_table(VALID_TRANSFERS)?;
            let existing = table
                .get(inscription_id)?
                .and_then(|v| decode::<ValidTransfer>(v.value()).ok());
            let Some(mut v) = existing else {
                // Not a valid transfer; emit skip
                events.push(self.new_event(
                    ticker,
                    EventFamily::Transfer,
                    EventType::TokenTransferSkippedSemantic,
                    block,
                    occurred_at,
                    None,
                    Some(inscription_id.to_string()),
                    new_owner.map(str::to_string),
                    None,
                    EventDelta::default(),
                    serde_json::json!({ "reason": "no_valid_transfer_row" }),
                    block_hash,
                ));
                return Ok(());
            };
            if v.consumed_height.is_some() {
                events.push(self.new_event(
                    ticker,
                    EventFamily::Transfer,
                    EventType::TokenTransferSkippedSemantic,
                    block,
                    occurred_at,
                    None,
                    Some(inscription_id.to_string()),
                    new_owner.map(str::to_string),
                    None,
                    EventDelta::default(),
                    serde_json::json!({ "reason": "already_consumed" }),
                    block_hash,
                ));
                return Ok(());
            }
            v.consumed_height = Some(block.height);
            let sender = v.sender.clone();
            let amount = v.amount;
            table.insert(inscription_id, encode(&v)?.as_slice())?;
            (sender, amount)
        };
        // Remove from the sender's open-transferables index; this
        // inscription has settled.
        {
            let mut idx = wtx.open_table(TRANSFERABLES_BY_SENDER)?;
            idx.remove((ticker, sender.as_str(), inscription_id))?;
        }
        let a = i128::try_from(amount).unwrap_or(i128::MAX);
        // Debit sender transferable
        apply_wallet_delta(wtx, ticker, &sender, -a, 0, -a, 0, occurred_at)?;
        events.push(self.new_event(
            ticker,
            EventFamily::Transfer,
            EventType::TokenTransferDebit,
            block,
            occurred_at,
            None,
            Some(inscription_id.to_string()),
            Some(sender.clone()),
            new_owner.map(str::to_string),
            EventDelta {
                delta_total: -a,
                delta_transferable: -a,
                ..Default::default()
            },
            serde_json::json!({ "amount": amount }),
            block_hash,
        ));
        // Credit recipient
        if let Some(owner) = new_owner {
            apply_wallet_delta(wtx, ticker, owner, a, a, 0, 0, occurred_at)?;
            events.push(self.new_event(
                ticker,
                EventFamily::Transfer,
                EventType::TokenTransferCredit,
                block,
                occurred_at,
                None,
                Some(inscription_id.to_string()),
                Some(owner.to_string()),
                Some(sender),
                EventDelta {
                    delta_total: a,
                    delta_available: a,
                    ..Default::default()
                },
                serde_json::json!({ "amount": amount }),
                block_hash,
            ));
        }
        Ok(())
    }

    fn burn_transfer(
        &mut self,
        wtx: &redb::WriteTransaction,
        ticker: &str,
        inscription_id: &str,
        block: &BlockView,
        occurred_at: DateTime<Utc>,
        block_hash: &str,
        events: &mut Vec<LedgerEvent>,
    ) -> Result<()> {
        let (sender, amount) = {
            let mut table = wtx.open_table(VALID_TRANSFERS)?;
            let existing = table
                .get(inscription_id)?
                .and_then(|v| decode::<ValidTransfer>(v.value()).ok());
            let Some(mut v) = existing else {
                return Ok(());
            };
            if v.consumed_height.is_some() {
                return Ok(());
            }
            v.consumed_height = Some(block.height);
            let sender = v.sender.clone();
            let amount = v.amount;
            table.insert(inscription_id, encode(&v)?.as_slice())?;
            (sender, amount)
        };
        // Remove from the sender's open-transferables index.
        {
            let mut idx = wtx.open_table(TRANSFERABLES_BY_SENDER)?;
            idx.remove((ticker, sender.as_str(), inscription_id))?;
        }
        let a = i128::try_from(amount).unwrap_or(i128::MAX);
        apply_wallet_delta(wtx, ticker, &sender, -a, 0, -a, a, occurred_at)?;
        events.push(self.new_event(
            ticker,
            EventFamily::Transfer,
            EventType::TokenTransferBurned,
            block,
            occurred_at,
            None,
            Some(inscription_id.to_string()),
            Some(sender),
            None,
            EventDelta {
                delta_total: -a,
                delta_transferable: -a,
                delta_burned: a,
                ..Default::default()
            },
            serde_json::json!({ "amount": amount }),
            block_hash,
        ));
        Ok(())
    }

    fn tap_control(
        &mut self,
        wtx: &redb::WriteTransaction,
        ticker: &str,
        inscription_id: &str,
        new_owner: Option<&str>,
        block: &BlockView,
        occurred_at: DateTime<Utc>,
        block_hash: &str,
        events: &mut Vec<LedgerEvent>,
    ) -> Result<()> {
        let pending: Option<StoredPendingControl> = {
            let table = wtx.open_table(PENDING_CONTROLS)?;
            let raw = table.get(inscription_id)?;
            raw.and_then(|v| decode::<StoredPendingControl>(v.value()).ok())
        };
        let Some(p) = pending else {
            return Ok(());
        };
        // Self-send check: tap only if new_owner == inscriber's address.
        let self_send = matches!(new_owner, Some(o) if o == p.address);
        if self_send {
            let block_flag = p.op == "block";
            set_wallet_locked_flag(wtx, ticker, &p.address, block_flag)?;
            let et = if block_flag {
                EventType::BlockTransferablesTapped
            } else {
                EventType::UnblockTransferablesTapped
            };
            events.push(self.new_event(
                ticker,
                EventFamily::Control,
                et,
                block,
                occurred_at,
                None,
                Some(inscription_id.to_string()),
                Some(p.address.clone()),
                None,
                EventDelta::default(),
                serde_json::json!({ "op": p.op }),
                block_hash,
            ));
        }
        // Consume either way
        let mut table = wtx.open_table(PENDING_CONTROLS)?;
        table.remove(inscription_id)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn new_event(
        &mut self,
        ticker: &str,
        family: EventFamily,
        event_type: EventType,
        block: &BlockView,
        occurred_at: DateTime<Utc>,
        tx_index: Option<u32>,
        inscription_id: Option<String>,
        address: Option<String>,
        counterparty: Option<String>,
        delta: EventDelta,
        metadata: serde_json::Value,
        block_hash: &str,
    ) -> LedgerEvent {
        let id = self.next_event_id;
        self.next_event_id += 1;
        // Event key uniquely identifies this event across re-runs.
        // For inscription-tied events the inscription_id is sufficient;
        // for coinbase shares there's no inscription so we include
        // tx_index + address to disambiguate multiple recipients per
        // block.
        let event_key = format!(
            "{}:{}:{}:{}:{}:{}",
            ticker,
            event_type.as_str(),
            block.height,
            inscription_id.as_deref().unwrap_or(""),
            tx_index.map(|i| i.to_string()).unwrap_or_default(),
            address.as_deref().unwrap_or("")
        );
        LedgerEvent {
            ticker: ticker.to_string(),
            event_id: id,
            event_key,
            family,
            event_type,
            block_height: block.height,
            block_hash: block_hash.to_string(),
            tx_index,
            txid: tx_index.and_then(|i| block.txs.get(i as usize).map(|t| t.txid.to_string())),
            inscription_id,
            occurred_at,
            address,
            counterparty_address: counterparty,
            delta,
            source_kind: "witness_scan".to_string(),
            metadata,
        }
    }
}

/// The single allowlisted ticker for this build. v0.1.0 ships NAT only.
/// Returns true if the on-chain TAP payload's ticker should be
/// processed by the indexer.
fn is_indexed_ticker(ticker: &str) -> bool {
    canonical_ticker(ticker) == "nat"
}

/// Fold a raw on-chain ticker string to its canonical form.
///
/// Per the DMT docs, `token-transfer` inscriptions carry `dmt-<name>`
/// while `dmt-deploy` / `dmt-mint` carry `<name>`. Strip the prefix so
/// both forms map to the same deployment ticker. Lowercased since TAP
/// matches case-insensitively.
fn canonical_ticker(raw: &str) -> String {
    let lower = raw.trim().to_ascii_lowercase();
    lower
        .strip_prefix("dmt-")
        .map(str::to_string)
        .unwrap_or(lower)
}

async fn fetch_block_bits(rpc: &RpcClient, height: u64) -> Result<u32> {
    rpc.get_block_bits(height).await
}

async fn fetch_block_bits_retry(rpc: &RpcClient, height: u64) -> Result<u32> {
    let mut delay_ms = 200u64;
    let mut last_err: Option<crate::error::Error> = None;
    for _ in 0..3 {
        match fetch_block_bits(rpc, height).await {
            Ok(b) => return Ok(b),
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                delay_ms *= 2;
            }
        }
    }
    Err(last_err
        .unwrap_or_else(|| crate::error::Error::Rpc(format!("bits fetch failed at {height}"))))
}

fn describe_move(mv: &TrackerMove, _height: u64) -> Option<(String, Option<String>, bool)> {
    match mv {
        TrackerMove::Moved {
            inscription,
            new_owner_address,
            ..
        } => Some((
            inscription.inscription_id.clone(),
            new_owner_address.clone(),
            matches!(inscription.kind, InscriptionKind::TokenTransfer),
        )),
        TrackerMove::Burned { inscription, .. } => Some((
            inscription.inscription_id.clone(),
            None,
            matches!(inscription.kind, InscriptionKind::TokenTransfer),
        )),
    }
}

fn parse_outpoint(s: &str) -> Option<OutPoint> {
    let (txid, vout) = s.split_once(':')?;
    let txid: bitcoin::Txid = txid.parse().ok()?;
    let vout: u32 = vout.parse().ok()?;
    Some(OutPoint { txid, vout })
}

fn tracker_iter(t: &InscriptionTracker) -> Vec<(OutPoint, TrackedCarrier)> {
    t.snapshot()
}

#[allow(clippy::too_many_arguments)]
fn apply_wallet_delta(
    wtx: &redb::WriteTransaction,
    ticker: &str,
    address: &str,
    delta_total: i128,
    delta_available: i128,
    delta_transferable: i128,
    delta_burned: i128,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    let prev_total: i128 = {
        let table = wtx.open_table(WALLET_STATE)?;
        let raw = table.get((ticker, address))?;
        raw.and_then(|v| decode::<WalletState>(v.value()).ok())
            .map(|s| s.total)
            .unwrap_or(0)
    };
    let mut table = wtx.open_table(WALLET_STATE)?;
    let key = (ticker, address);
    let mut state: WalletState = table
        .get(key)?
        .and_then(|v| decode(v.value()).ok())
        .unwrap_or_default();
    state.total += delta_total;
    state.available += delta_available;
    state.transferable += delta_transferable;
    state.burned += delta_burned;
    if state.first_activity.is_none() {
        state.first_activity = Some(occurred_at);
    }
    state.last_activity = Some(occurred_at);
    let new_total = state.total;
    table.insert(key, encode(&state)?.as_slice())?;
    drop(table);
    // Maintain secondary index `balances_by_value` so /holders can
    // range-scan descending by balance without re-sorting in memory.
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
    let mut table = wtx.open_table(crate::store::tables::BALANCES_BY_VALUE)?;
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

fn set_wallet_locked(wtx: &redb::WriteTransaction, ticker: &str, address: &str) -> Result<()> {
    set_wallet_locked_flag(wtx, ticker, address, true)
}

fn wallet_is_new(wtx: &redb::WriteTransaction, ticker: &str, address: &str) -> Result<bool> {
    let table = wtx.open_table(WALLET_STATE)?;
    let raw = table.get((ticker, address))?;
    Ok(raw.is_none())
}

fn set_wallet_locked_flag(
    wtx: &redb::WriteTransaction,
    ticker: &str,
    address: &str,
    flag: bool,
) -> Result<()> {
    let mut table = wtx.open_table(WALLET_STATE)?;
    let key = (ticker, address);
    let mut state: WalletState = table
        .get(key)?
        .and_then(|v| decode(v.value()).ok())
        .unwrap_or_default();
    state.transferables_blocked = flag;
    table.insert(key, encode(&state)?.as_slice())?;
    Ok(())
}
