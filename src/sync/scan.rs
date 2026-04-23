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
use redb::{ReadableMultimapTable, ReadableTable};
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
use crate::protocol::auth::TokenAuthForm;
use crate::protocol::control::ControlOp;
use crate::protocol::envelope::{decode_tap_payload, TapEnvelope};
use crate::store::codec::{decode, encode};
use crate::store::tables::{
    self, cursor_get, cursor_set, Cursor, DailyStats, InscriptionIndex, InscriptionOwner,
    MintClaim, PendingControl as StoredPendingControl, ValidTransfer, WalletState, ACTIVITY_RECENT,
    DAILY_STATS, DEPLOYMENTS, DMT_REWARD_ADDRESSES, EVENTS, INSCRIPTIONS, INSCRIPTION_OWNERS,
    MINT_CLAIMS, MINT_TOTALS, PENDING_CONTROLS, STATS, TRANSFERABLES_BY_SENDER, VALID_TRANSFERS,
    WALLET_ACTIVITY, WALLET_STATE,
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

struct PendingSendReveal {
    inscription_id: String,
    creator: String,
    tx_index: u32,
    items: Vec<crate::store::tables::PendingSendItem>,
}

struct PendingAuthReveal {
    inscription_id: String,
    creator: String,
    tx_index: u32,
    form: crate::store::tables::PendingAuthForm,
}

/// Redeems execute at reveal time (unlike create/cancel which settle at
/// move). Carries the fully-parsed payload plus tx_index + inscriber
/// owner address.
struct AuthRedeemReveal {
    inscription_id: String,
    tx_index: u32,
    inscriber_addr: String,
    payload: crate::protocol::auth::TokenAuthRedeem,
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
            max_supply: crate::ledger::deploy::NAT_MAX_SUPPLY,
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
            if let Ok(t) = rtx.open_multimap_table(INSCRIPTION_OWNERS) {
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
            /// OP_2 pointer value if the envelope carried one. None ≡
            /// default sat-flow (inscription lands at sat 0 of the tx's
            /// combined output range → vout 0 offset 0).
            pointer: Option<u64>,
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
                            pointer: env.pointer,
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
        let mut send_reveals: Vec<PendingSendReveal> = Vec::new();
        let mut auth_reveals: Vec<PendingAuthReveal> = Vec::new();
        let mut auth_redeem_reveals: Vec<AuthRedeemReveal> = Vec::new();
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
                // Ord-tap's default sat-flow: each fresh envelope on
                // input 0 starts at sat 0 of the tx's output range
                // (= `total_input_value` at the input-0 boundary =
                // cumulative input sum before we enter the input loop,
                // which is 0). A pointer tag reroutes the inscription
                // to `pointer` if `pointer < total_output_value`.
                // Ref: ord-tap `index/updater/inscription_updater.rs:458,520-524`.
                //
                // Resolve that absolute sat offset into `(vout,
                // offset_in_outpoint)` by walking outputs once.
                // Without a pointer this is always (0, 0) — same as
                // the prior default. With a pointer ≥ total_output it
                // also falls back to (0, 0). Otherwise it routes to
                // whichever output contains the pointed-at sat.
                let (landing_vout, landing_offset) = resolve_landing(&txv.tx, pe.pointer);
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
                // of the pipeline never wastes work on them.
                //
                // Form depends on op per ord-tap. dmt-deploy registers
                // the ticker under its `dmt-`-prefixed effective form
                // (`dmt-nat`) and dmt-mint does the same prefix prepend
                // at lookup. Every other op (token-transfer/send/trade
                // /auth/block/unblock) looks up the deploy literally
                // under the ticker supplied in the payload — so the
                // payload MUST carry the long form `dmt-nat`, not the
                // short `nat`. Accepting bare `nat` for transfer-style
                // ops over-admits inscriptions ord-tap rejects (see
                // f88d1954…5a i0 at block 824,343, which reserved 1,000
                // units of transferable and cascaded a −11.86B short
                // through bc1plzjl35 → bc1pjhq8xseesnjv).
                if !envelope_matches_indexed(&payload) {
                    continue;
                }
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
                                offset_in_outpoint: landing_offset,
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
                                offset_in_outpoint: landing_offset,
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
                    TapEnvelope::Send(sp) => {
                        // Miner-reward shield at reveal time. ord-tap
                        // `/tmp/ot_send.rs:26-27`. Shield is also
                        // re-checked at move time.
                        let Some(addr) = to_addr.as_deref().and_then(normalize_address) else {
                            continue;
                        };
                        if is_dmt_reward_address(wtx, addr.as_str())? {
                            continue;
                        }
                        // Filter items to the indexed ticker. Per-item
                        // other-ticker drops are silent (parity with the
                        // coarse envelope filter).
                        let mut items: Vec<crate::store::tables::PendingSendItem> = Vec::new();
                        for it in sp.items.iter() {
                            if canonical_ticker(it.ticker.as_str()) != "nat" {
                                continue;
                            }
                            items.push(crate::store::tables::PendingSendItem {
                                ticker: canonical_ticker(it.ticker.as_str()),
                                recipient: it.recipient.as_str().to_string(),
                                amount: it.amount,
                                dta: it.dta.clone(),
                            });
                        }
                        if items.is_empty() {
                            continue;
                        }
                        send_reveals.push(PendingSendReveal {
                            inscription_id: insc_id.clone(),
                            creator: addr.as_str().to_string(),
                            tx_index,
                            items,
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
                                    ticker: "nat".to_string(),
                                    kind: InscriptionKind::TokenSend,
                                },
                                offset_in_outpoint: landing_offset,
                                outpoint_value_sats: reveal_value,
                            },
                            to_addr.clone(),
                        ));
                        inscription_rows.push((
                            insc_id,
                            InscriptionIndex {
                                ticker: "nat".to_string(),
                                kind: "token-send".to_string(),
                                original_amount: None,
                                inscribed_height: block.height,
                                current_owner_address: to_addr.clone(),
                                consumed_height: None,
                            },
                        ));
                    }
                    TapEnvelope::Auth(ap) => {
                        let Some(addr) = to_addr.as_deref().and_then(normalize_address) else {
                            continue;
                        };
                        let creator = addr.as_str().to_string();
                        let reveal_value = txv
                            .tx
                            .output
                            .get(landing_vout)
                            .map(|o| o.value.to_sat())
                            .unwrap_or(0);
                        match &ap.form {
                            TokenAuthForm::Redeem(r) => {
                                // Redeems execute AT REVEAL. Queue with
                                // the full payload for the write phase.
                                auth_redeem_reveals.push(AuthRedeemReveal {
                                    inscription_id: insc_id.clone(),
                                    tx_index,
                                    inscriber_addr: creator.clone(),
                                    payload: r.clone(),
                                });
                                // Track the carrier so INSCRIPTIONS row
                                // stays fresh on later transfers.
                                fresh_carriers.push((
                                    OutPoint {
                                        txid: txv.txid,
                                        vout: landing_vout as u32,
                                    },
                                    TrackedCarrier {
                                        inscription: TrackedInscription {
                                            inscription_id: insc_id.clone(),
                                            ticker: "nat".to_string(),
                                            kind: InscriptionKind::TokenAuth,
                                        },
                                        offset_in_outpoint: landing_offset,
                                        outpoint_value_sats: reveal_value,
                                    },
                                    to_addr.clone(),
                                ));
                                inscription_rows.push((
                                    insc_id,
                                    InscriptionIndex {
                                        ticker: "nat".to_string(),
                                        kind: "token-auth".to_string(),
                                        original_amount: None,
                                        inscribed_height: block.height,
                                        current_owner_address: to_addr.clone(),
                                        consumed_height: None,
                                    },
                                ));
                            }
                            TokenAuthForm::Create(c) => {
                                // Serialize the auth array byte-exact
                                // for later re-hash at move time.
                                let auth_array_json = match serde_json::to_string(&c.auth_array_raw)
                                {
                                    Ok(s) => s,
                                    Err(_) => continue,
                                };
                                let form = crate::store::tables::PendingAuthForm::Create {
                                    auth_tickers: c.auth_tickers.clone(),
                                    sig_v: c.sig.v.clone(),
                                    sig_r: c.sig.r.clone(),
                                    sig_s: c.sig.s.clone(),
                                    hash_hex: c.hash.clone(),
                                    salt: c.salt.clone(),
                                    auth_array_json,
                                };
                                auth_reveals.push(PendingAuthReveal {
                                    inscription_id: insc_id.clone(),
                                    creator: creator.clone(),
                                    tx_index,
                                    form,
                                });
                                fresh_carriers.push((
                                    OutPoint {
                                        txid: txv.txid,
                                        vout: landing_vout as u32,
                                    },
                                    TrackedCarrier {
                                        inscription: TrackedInscription {
                                            inscription_id: insc_id.clone(),
                                            ticker: "nat".to_string(),
                                            kind: InscriptionKind::TokenAuth,
                                        },
                                        offset_in_outpoint: landing_offset,
                                        outpoint_value_sats: reveal_value,
                                    },
                                    to_addr.clone(),
                                ));
                                inscription_rows.push((
                                    insc_id,
                                    InscriptionIndex {
                                        ticker: "nat".to_string(),
                                        kind: "token-auth".to_string(),
                                        original_amount: None,
                                        inscribed_height: block.height,
                                        current_owner_address: to_addr.clone(),
                                        consumed_height: None,
                                    },
                                ));
                            }
                            TokenAuthForm::Cancel(c) => {
                                let form = crate::store::tables::PendingAuthForm::Cancel {
                                    cancel_id: c.cancel_id.clone(),
                                };
                                auth_reveals.push(PendingAuthReveal {
                                    inscription_id: insc_id.clone(),
                                    creator: creator.clone(),
                                    tx_index,
                                    form,
                                });
                                fresh_carriers.push((
                                    OutPoint {
                                        txid: txv.txid,
                                        vout: landing_vout as u32,
                                    },
                                    TrackedCarrier {
                                        inscription: TrackedInscription {
                                            inscription_id: insc_id.clone(),
                                            ticker: "nat".to_string(),
                                            kind: InscriptionKind::TokenAuth,
                                        },
                                        offset_in_outpoint: landing_offset,
                                        outpoint_value_sats: reveal_value,
                                    },
                                    to_addr.clone(),
                                ));
                                inscription_rows.push((
                                    insc_id,
                                    InscriptionIndex {
                                        ticker: "nat".to_string(),
                                        kind: "token-auth".to_string(),
                                        original_amount: None,
                                        inscribed_height: block.height,
                                        current_owner_address: to_addr.clone(),
                                        consumed_height: None,
                                    },
                                ));
                            }
                        }
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
                                offset_in_outpoint: landing_offset,
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
                .any(|i| tracker.has(&i.previous_output));
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

        // Resolve transfer inscribes serially. The blocked-senders set
        // enforces ord-tap's create-time miner-reward-shield (any
        // wallet whose `transferables_blocked` flag is set cannot
        // inscribe new transferables). The flag is auto-set on the
        // first post-941,848 coinbase credit and cleared only by an
        // explicit `unblock-transferables` control op.
        //
        // Inscribe resolution itself is deferred to the write phase
        // (after mints / coinbase / settlements / auth-redeems have
        // applied). Reading `avail` from a pre-block snapshot causes a
        // false-negative when the sender was credited earlier in the
        // same block (e.g. via an auth-redeem or a transfer settlement
        // at a lower tx_index). ord-tap processes each envelope in
        // tx-index order and reads the live `b - t`, so same-block
        // credits do count.
        //
        // Resolve mints per ticker, threading the cumulative-issued
        // total so the resolver can clamp against `max_supply` and
        // match ord-tap's `dc/<tick>` tokens-left bookkeeping.
        let mut mint_resolutions = Vec::new();
        for (ticker, cands) in mint_cands {
            let Some(dep) = deployments.get(&ticker) else {
                continue;
            };
            let claimed = self.load_claimed_blocks_from(wtx, &ticker)?;
            let cumulative = read_mint_total(wtx, &ticker)?;
            mint_resolutions.push((
                ticker,
                resolve_mints(dep, block.height, &claimed, cands, cumulative),
            ));
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

        // (Transfer-inscribe resolution moved below the settlement /
        //  mint / coinbase / auth-redeem phases so it reads the LIVE
        //  post-those-events wallet state, matching ord-tap's
        //  tx-index-ordered traversal. Same-block credits via a
        //  transfer-settle, token-send-execute, auth-redeem, mint, or
        //  coinbase at a lower tx_index now count toward the
        //  inscribing wallet's available balance.)

        // 2b. Token-send reveals: register pending_sends rows + emit
        //     TokenSendInscribeAdmitted. Balance movement happens later
        //     at move time.
        for sr in send_reveals {
            {
                let mut table = wtx.open_table(tables::PENDING_SENDS)?;
                let row = tables::PendingSend {
                    creator: sr.creator.clone(),
                    inscribed_height: block.height,
                    items: sr.items.clone(),
                    consumed_height: None,
                };
                table.insert(sr.inscription_id.as_str(), encode(&row)?.as_slice())?;
            }
            let total_amount: u128 = sr.items.iter().map(|i| i.amount).sum();
            events.push(self.new_event(
                "nat",
                EventFamily::Send,
                EventType::TokenSendInscribeAdmitted,
                block,
                occurred_at,
                Some(sr.tx_index),
                Some(sr.inscription_id),
                Some(sr.creator),
                None,
                EventDelta::default(),
                serde_json::json!({
                    "item_count": sr.items.len(),
                    "total_amount": total_amount.to_string(),
                }),
                &block_hash,
            ));
        }

        // 2c. Token-auth create/cancel reveals: store accumulator + emit
        //     lifecycle event. Signature verification is deferred to
        //     move time for create; cancel fires at move as well.
        for ar in auth_reveals {
            {
                let mut table = wtx.open_table(tables::PENDING_AUTHS)?;
                let row = tables::PendingAuth {
                    creator: ar.creator.clone(),
                    inscribed_height: block.height,
                    form: ar.form.clone(),
                    consumed_height: None,
                };
                table.insert(ar.inscription_id.as_str(), encode(&row)?.as_slice())?;
            }
            let et = match &ar.form {
                tables::PendingAuthForm::Create { .. } => EventType::TokenAuthCreateInscribed,
                tables::PendingAuthForm::Cancel { .. } => EventType::TokenAuthCancelInscribed,
            };
            events.push(self.new_event(
                "nat",
                EventFamily::Auth,
                et,
                block,
                occurred_at,
                Some(ar.tx_index),
                Some(ar.inscription_id),
                Some(ar.creator),
                None,
                EventDelta::default(),
                serde_json::json!({}),
                &block_hash,
            ));
        }

        // 2d. Token-auth redeem reveals: execute balance moves at
        //     reveal time (ord-tap `/tmp/ot_auth.rs:42-102`).
        for rr in auth_redeem_reveals {
            self.process_auth_redeem(wtx, rr, block, occurred_at, &block_hash, &mut events)?;
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
                bump_mint_total(&wtx, &ticker, amount)?;
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
                    serde_json::json!({ "blk": blk, "amount": amount.to_string() }),
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
                            "amount": share.share_amount.to_string(),
                        }),
                        &block_hash,
                    ));
                    continue;
                }
                if let Some(addr) = share.address.clone() {
                    // Read the current bltr flag BEFORE applying the
                    // delta so we know whether to auto-set it. ord-tap:
                    // bltr is auto-set on the first post-activation
                    // reward credit if it wasn't already set; an
                    // explicit `unblock-transferables` clears it, and
                    // subsequent credits do NOT re-block.
                    let was_blocked = wallet_is_blocked(&wtx, &ticker, &addr)?;
                    apply_wallet_delta(&wtx, &ticker, &addr, a, a, 0, 0, occurred_at)?;
                    // Coinbase issuance counts toward the supply cap —
                    // ord-tap's `dc/<tick>` counter decrements on both
                    // `dmt-mint` admits and DMT-reward coinbase credits.
                    bump_mint_total(&wtx, &ticker, share.share_amount)?;
                    if share.should_lock_on_first_credit {
                        // dmtrwd marker is PERMANENT — idempotent put.
                        // Needed for the transfer-execution shield
                        // (height >= 942,002), which invalidates
                        // outstanding transferables from any past
                        // miner even after they unblock themselves.
                        dmt_reward_mark(&wtx, &addr)?;
                        if !was_blocked {
                            set_wallet_locked(&wtx, &ticker, &addr)?;
                        }
                    }
                    let did_lock_now = share.should_lock_on_first_credit && !was_blocked;
                    events.push(self.new_event(
                        &ticker,
                        EventFamily::Coinbase,
                        if did_lock_now {
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
                            "amount": share.share_amount.to_string(),
                        }),
                        &block_hash,
                    ));
                }
            }
        }

        // 5b. Transfer inscribes — admitted between step-5 credits and
        //     step-6 settlements, using a two-pass settle to handle
        //     BOTH same-block credit-then-inscribe AND same-block
        //     inscribe-then-spend ordering at once. Ord-tap processes
        //     envelopes and moves in tx-index order; a simple "all
        //     settles before all admits" breaks same-block
        //     inscribe+spend (settle can't find the just-written
        //     valid_transfer), and a simple "all admits before all
        //     settles" breaks credit-then-inscribe (admit can't see
        //     the yet-to-land credit). Two passes get both:
        //
        //       pass 1 (BEFORE admit): settle only the moves whose
        //           VALID_TRANSFERS already exists at block entry —
        //           i.e. the inscribe happened in a PRIOR block.
        //           Credits from these settlements land in
        //           WALLET_STATE before admission runs.
        //       step 5b: admit new inscribes against LIVE balance;
        //           write VALID_TRANSFERS for successes.
        //       pass 2 (AFTER admit): settle the moves whose
        //           VALID_TRANSFERS was just written — i.e.
        //           same-block inscribe+spend.
        //
        //     Non-transfer moves (Control / Send / Auth / Mint / Burn)
        //     all go in pass 1 — they don't gate on VALID_TRANSFERS.
        let (pass1_moves, pass2_moves) = {
            let vtable = wtx.open_table(VALID_TRANSFERS)?;
            let mut p1: Vec<(u32, TrackerMove)> = Vec::with_capacity(moves.len());
            let mut p2: Vec<(u32, TrackerMove)> = Vec::new();
            for (tx_index, mv) in moves.drain(..) {
                let defer = matches!(
                    &mv,
                    TrackerMove::Moved { inscription, .. }
                        if matches!(inscription.kind, InscriptionKind::TokenTransfer)
                            && vtable.get(inscription.inscription_id.as_str())?.is_none()
                );
                if defer {
                    p2.push((tx_index, mv));
                } else {
                    p1.push((tx_index, mv));
                }
            }
            (p1, p2)
        };
        // Execute pass 1 (settles with pre-existing VALID_TRANSFERS + all non-transfer moves)
        self.apply_moves(wtx, pass1_moves, block, occurred_at, &block_hash, &mut events)?;
        let avail_snapshot = self.snapshot_available_from(wtx, &transfer_inscribes_unresolved)?;
        let blocked_senders =
            self.snapshot_blocked_senders_from(wtx, &transfer_inscribes_unresolved)?;
        let inscribe_resolutions = resolve_transfer_inscribes(
            transfer_inscribes_unresolved,
            &avail_snapshot,
            &blocked_senders,
        );
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
                        serde_json::json!({ "amount": amount.to_string() }),
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
                    reason,
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
                            "amount": attempted_amount.to_string(),
                            "available": snapshot_available.to_string(),
                            "reason": reason,
                        }),
                        &block_hash,
                    ));
                }
            }
        }

        // 6. Apply pass-2 moves (same-block inscribe+spend whose
        //    VALID_TRANSFERS was just written in step 5b). Pass-1
        //    moves were already applied above before admission.
        self.apply_moves(wtx, pass2_moves, block, occurred_at, &block_hash, &mut events)?;

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
            if tracker.has(&op) {
                out.insert(op, tracker.outpoint_value(&op));
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
        let table = wtx.open_multimap_table(INSCRIPTION_OWNERS)?;
        let mut t = InscriptionTracker::new();
        let mut keys = std::collections::HashSet::new();
        for row in table.iter()? {
            let (k, values) = row?;
            let key_str = k.value().to_string();
            keys.insert(key_str.clone());
            let op: OutPoint = match parse_outpoint(&key_str) {
                Some(op) => op,
                None => continue,
            };
            // Multimap iter yields (key, MultimapValue) pairs; one
            // inscription per value. Preserve insertion order when
            // loading so reveal order is retained in-memory.
            for v in values {
                let raw = v?;
                let owner: InscriptionOwner = decode(raw.value())?;
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
                                "token-send" => InscriptionKind::TokenSend,
                                "token-auth" => InscriptionKind::TokenAuth,
                                _ => InscriptionKind::Control,
                            },
                        },
                        offset_in_outpoint: owner.offset_in_outpoint,
                        outpoint_value_sats: owner.outpoint_value_sats,
                    },
                );
            }
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

    /// Snapshot of `transferables_blocked` for each candidate's
    /// `(ticker, address)`. Used by `resolve_transfer_inscribes` to
    /// enforce ord-tap's create-time miner-reward-shield at height
    /// >= 941,848 — a blocked sender cannot inscribe new transferables,
    /// regardless of available balance.
    fn snapshot_blocked_senders_from(
        &self,
        wtx: &redb::WriteTransaction,
        candidates: &[TransferInscribeCandidate],
    ) -> Result<std::collections::HashSet<(String, String)>> {
        let table = wtx.open_table(WALLET_STATE)?;
        let mut out = std::collections::HashSet::new();
        for c in candidates {
            let key = (c.ticker.clone(), c.address.as_str().to_string());
            if out.contains(&key) {
                continue;
            }
            let state: Option<WalletState> = table
                .get((c.ticker.as_str(), c.address.as_str()))?
                .and_then(|v| decode(v.value()).ok());
            if state.map(|s| s.transferables_blocked).unwrap_or(false) {
                out.insert(key);
            }
        }
        Ok(out)
    }

    fn save_carriers(
        &self,
        wtx: &redb::WriteTransaction,
        tracker: &InscriptionTracker,
        previous_keys: &std::collections::HashSet<String>,
    ) -> Result<()> {
        // Multimap can't "replace" a key's value-set in-place, so for
        // every key that still has carriers we `remove_all` then
        // re-insert the full current set. Keys present before and
        // absent now get a plain `remove_all`. Re-insertion is cheap
        // (typically 1–3 values per outpoint) and matches how ord-tap
        // rewrites the full UtxoEntry on every touch.
        let mut table = wtx.open_multimap_table(INSCRIPTION_OWNERS)?;

        // Group current state by outpoint-key.
        let mut by_key: std::collections::HashMap<String, Vec<TrackedCarrier>> =
            std::collections::HashMap::new();
        for (op, carrier) in tracker_iter(tracker) {
            let key = format!("{}:{}", op.txid, op.vout);
            by_key.entry(key).or_default().push(carrier);
        }
        let current_keys: std::collections::HashSet<String> = by_key.keys().cloned().collect();

        // Drop removed outpoints entirely.
        for removed in previous_keys.difference(&current_keys) {
            table.remove_all(removed.as_str())?;
        }

        // For each current key: clear, then re-insert full set.
        for (key, carriers) in by_key {
            table.remove_all(key.as_str())?;
            for carrier in carriers {
                let v = InscriptionOwner {
                    inscription_id: carrier.inscription.inscription_id.clone(),
                    ticker: carrier.inscription.ticker.clone(),
                    kind: match carrier.inscription.kind {
                        InscriptionKind::TokenTransfer => "token-transfer".to_string(),
                        InscriptionKind::Control => "control".to_string(),
                        InscriptionKind::Mint => "dmt-mint".to_string(),
                        InscriptionKind::TokenSend => "token-send".to_string(),
                        InscriptionKind::TokenAuth => "token-auth".to_string(),
                    },
                    current_outpoint: key.clone(),
                    offset_in_outpoint: carrier.offset_in_outpoint,
                    outpoint_value_sats: carrier.outpoint_value_sats,
                };
                table.insert(key.as_str(), encode(&v)?.as_slice())?;
            }
        }
        Ok(())
    }

    /// Apply a batch of tracker moves. Extracted from the old step-6
    /// inline loop so it can be called twice (pass 1 before
    /// transfer-inscribe admission, pass 2 after). Each move updates
    /// INSCRIPTIONS owner + consumed_height, then dispatches to the
    /// kind-specific settle handler.
    #[allow(clippy::too_many_arguments)]
    fn apply_moves(
        &mut self,
        wtx: &redb::WriteTransaction,
        moves: Vec<(u32, TrackerMove)>,
        block: &BlockView,
        occurred_at: DateTime<Utc>,
        block_hash: &str,
        events: &mut Vec<LedgerEvent>,
    ) -> Result<()> {
        for (_, mv) in moves {
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
                    new_owner_address,
                    ..
                } => match inscription.kind {
                    InscriptionKind::TokenTransfer => {
                        self.settle_transfer(
                            wtx,
                            &inscription.ticker,
                            &inscription.inscription_id,
                            new_owner_address.as_deref(),
                            block,
                            occurred_at,
                            block_hash,
                            events,
                        )?;
                    }
                    InscriptionKind::Control => {
                        self.tap_control(
                            wtx,
                            &inscription.ticker,
                            &inscription.inscription_id,
                            new_owner_address.as_deref(),
                            block,
                            occurred_at,
                            block_hash,
                            events,
                        )?;
                    }
                    InscriptionKind::Mint => {}
                    InscriptionKind::TokenSend => {
                        self.settle_token_send(
                            wtx,
                            &inscription.inscription_id,
                            new_owner_address.as_deref(),
                            block,
                            occurred_at,
                            block_hash,
                            events,
                        )?;
                    }
                    InscriptionKind::TokenAuth => {
                        self.settle_token_auth(
                            wtx,
                            &inscription.inscription_id,
                            new_owner_address.as_deref(),
                            block,
                            occurred_at,
                            block_hash,
                            events,
                        )?;
                    }
                },
                TrackerMove::Burned { inscription, .. } => {
                    if matches!(inscription.kind, InscriptionKind::TokenTransfer) {
                        self.burn_transfer(
                            wtx,
                            &inscription.ticker,
                            &inscription.inscription_id,
                            block,
                            occurred_at,
                            block_hash,
                            events,
                        )?;
                    }
                }
            }
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

        // Miner-reward transfer-execution shield (height >= 942,002):
        // if the sender is a DMT-reward address, the tap is voided.
        // Sender keeps the balance (no total debit), the transferable
        // is released back to available, recipient gets nothing.
        // Mirrors ord-tap's `tap_blocks_dmt_reward_transfer_execution`.
        if block.height >= crate::ledger::deploy::NAT_MINER_TRANSFER_EXECUTION_SHIELD
            && is_dmt_reward_address(wtx, &sender)?
        {
            apply_wallet_delta(wtx, ticker, &sender, 0, a, -a, 0, occurred_at)?;
            events.push(self.new_event(
                ticker,
                EventFamily::Transfer,
                EventType::TokenTransferShieldVoided,
                block,
                occurred_at,
                None,
                Some(inscription_id.to_string()),
                Some(sender),
                new_owner.map(str::to_string),
                EventDelta {
                    delta_available: a,
                    delta_transferable: -a,
                    ..Default::default()
                },
                serde_json::json!({ "amount": amount.to_string(), "reason": "dmt_reward_execution_shield" }),
                block_hash,
            ));
            return Ok(());
        }

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
            serde_json::json!({ "amount": amount.to_string() }),
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
                serde_json::json!({ "amount": amount.to_string() }),
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
            serde_json::json!({ "amount": amount.to_string() }),
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

    /// Settle a `token-send` inscription at move time.
    ///
    /// Enforces the self-tap rule (creator must be the transfer's
    /// new_owner — ord-tap `/tmp/ot_send.rs:87`), re-checks the miner
    /// reward shield, then iterates items in payload order invoking
    /// [`crate::ledger::send::execute_send_item`] per item.
    #[allow(clippy::too_many_arguments)]
    fn settle_token_send(
        &mut self,
        wtx: &redb::WriteTransaction,
        inscription_id: &str,
        new_owner: Option<&str>,
        block: &BlockView,
        occurred_at: DateTime<Utc>,
        block_hash: &str,
        events: &mut Vec<LedgerEvent>,
    ) -> Result<()> {
        let pending: Option<tables::PendingSend> = {
            let table = wtx.open_table(tables::PENDING_SENDS)?;
            let raw = table.get(inscription_id)?;
            raw.and_then(|v| decode::<tables::PendingSend>(v.value()).ok())
        };
        let Some(mut ps) = pending else {
            return Ok(());
        };
        if ps.consumed_height.is_some() {
            return Ok(());
        }
        // Self-tap rule: creator == new_owner (ord-tap line 87).
        let self_tap = matches!(new_owner, Some(o) if o == ps.creator);
        if !self_tap {
            // Not a self-tap — consume without emitting any item events
            // so the pending row doesn't get re-processed on a later
            // transfer. Parity with ord-tap, which early-returns.
            ps.consumed_height = Some(block.height);
            let mut table = wtx.open_table(tables::PENDING_SENDS)?;
            table.insert(inscription_id, encode(&ps)?.as_slice())?;
            return Ok(());
        }
        // Miner-reward shield at move time too — ord-tap /tmp/ot_send.rs:90-91.
        if is_dmt_reward_address(wtx, &ps.creator)? {
            ps.consumed_height = Some(block.height);
            let mut table = wtx.open_table(tables::PENDING_SENDS)?;
            table.insert(inscription_id, encode(&ps)?.as_slice())?;
            return Ok(());
        }
        use crate::ledger::send::{execute_send_item, SendItemOutcome};
        for item in &ps.items {
            let outcome = execute_send_item(
                wtx,
                &item.ticker,
                &ps.creator,
                &item.recipient,
                item.amount,
                occurred_at,
            )?;
            let a = i128::try_from(item.amount).unwrap_or(i128::MAX);
            match outcome {
                SendItemOutcome::Sent => {
                    events.push(self.new_event(
                        &item.ticker,
                        EventFamily::Send,
                        EventType::TokenSendDebit,
                        block,
                        occurred_at,
                        None,
                        Some(inscription_id.to_string()),
                        Some(ps.creator.clone()),
                        Some(item.recipient.clone()),
                        EventDelta {
                            delta_total: -a,
                            delta_available: -a,
                            ..Default::default()
                        },
                        serde_json::json!({ "amount": item.amount.to_string() }),
                        block_hash,
                    ));
                    events.push(self.new_event(
                        &item.ticker,
                        EventFamily::Send,
                        EventType::TokenSendCredit,
                        block,
                        occurred_at,
                        None,
                        Some(inscription_id.to_string()),
                        Some(item.recipient.clone()),
                        Some(ps.creator.clone()),
                        EventDelta {
                            delta_total: a,
                            delta_available: a,
                            ..Default::default()
                        },
                        serde_json::json!({ "amount": item.amount.to_string() }),
                        block_hash,
                    ));
                }
                SendItemOutcome::SelfSend => {
                    // Emit paired 0-delta events for audit trail; no
                    // wallet_state movement.
                    events.push(self.new_event(
                        &item.ticker,
                        EventFamily::Send,
                        EventType::TokenSendDebit,
                        block,
                        occurred_at,
                        None,
                        Some(inscription_id.to_string()),
                        Some(ps.creator.clone()),
                        Some(item.recipient.clone()),
                        EventDelta::default(),
                        serde_json::json!({
                            "amount": item.amount.to_string(),
                            "self_send": true,
                        }),
                        block_hash,
                    ));
                    events.push(self.new_event(
                        &item.ticker,
                        EventFamily::Send,
                        EventType::TokenSendCredit,
                        block,
                        occurred_at,
                        None,
                        Some(inscription_id.to_string()),
                        Some(item.recipient.clone()),
                        Some(ps.creator.clone()),
                        EventDelta::default(),
                        serde_json::json!({
                            "amount": item.amount.to_string(),
                            "self_send": true,
                        }),
                        block_hash,
                    ));
                }
                SendItemOutcome::Skipped => {
                    events.push(self.new_event(
                        &item.ticker,
                        EventFamily::Send,
                        EventType::TokenSendSkipped,
                        block,
                        occurred_at,
                        None,
                        Some(inscription_id.to_string()),
                        Some(ps.creator.clone()),
                        Some(item.recipient.clone()),
                        EventDelta::default(),
                        serde_json::json!({
                            "amount": item.amount.to_string(),
                            "reason": "insufficient_available",
                        }),
                        block_hash,
                    ));
                }
            }
        }
        ps.consumed_height = Some(block.height);
        {
            let mut table = wtx.open_table(tables::PENDING_SENDS)?;
            table.insert(inscription_id, encode(&ps)?.as_slice())?;
        }
        Ok(())
    }

    /// Settle a `token-auth` inscription (create or cancel form) at
    /// move time. Redeem form settles at reveal; see
    /// [`Self::process_auth_redeem`].
    #[allow(clippy::too_many_arguments)]
    fn settle_token_auth(
        &mut self,
        wtx: &redb::WriteTransaction,
        inscription_id: &str,
        new_owner: Option<&str>,
        block: &BlockView,
        occurred_at: DateTime<Utc>,
        block_hash: &str,
        events: &mut Vec<LedgerEvent>,
    ) -> Result<()> {
        let pending: Option<tables::PendingAuth> = {
            let table = wtx.open_table(tables::PENDING_AUTHS)?;
            let raw = table.get(inscription_id)?;
            raw.and_then(|v| decode::<tables::PendingAuth>(v.value()).ok())
        };
        let Some(mut pa) = pending else {
            return Ok(());
        };
        if pa.consumed_height.is_some() {
            return Ok(());
        }
        // Self-tap rule: acc.addr == new_owner (ord-tap auth line 129).
        let self_tap = matches!(new_owner, Some(o) if o == pa.creator);
        if !self_tap {
            pa.consumed_height = Some(block.height);
            let mut table = wtx.open_table(tables::PENDING_AUTHS)?;
            table.insert(inscription_id, encode(&pa)?.as_slice())?;
            return Ok(());
        }
        match &pa.form {
            tables::PendingAuthForm::Cancel { cancel_id } => {
                // Record cancel marker (keyed by the referenced authority id).
                {
                    let mut t = wtx.open_table(tables::TOKEN_AUTH_CANCELS)?;
                    t.insert(cancel_id.as_str(), 1u8)?;
                }
                events.push(self.new_event(
                    "nat",
                    EventFamily::Auth,
                    EventType::TokenAuthCancelTapped,
                    block,
                    occurred_at,
                    None,
                    Some(inscription_id.to_string()),
                    Some(pa.creator.clone()),
                    None,
                    EventDelta::default(),
                    serde_json::json!({ "cancel_id": cancel_id }),
                    block_hash,
                ));
            }
            tables::PendingAuthForm::Create {
                auth_tickers,
                sig_v,
                sig_r,
                sig_s,
                hash_hex,
                salt,
                auth_array_json,
            } => {
                let msg_hash = crate::crypto::ecdsa_recover::compute_msg_hash(
                    auth_array_json.as_bytes(),
                    salt,
                );
                let verify_result = crate::crypto::ecdsa_recover::verify_ecdsa_recover(
                    sig_v, sig_r, sig_s, hash_hex, &msg_hash,
                );
                match verify_result {
                    Ok((compact_sig_hex, pubkey_hex)) => {
                        // Replay guard: if this sig was already used,
                        // reject even the create.
                        let already_used = {
                            let t = wtx.open_table(tables::TOKEN_AUTH_SIG_REPLAY)?;
                            let got = t.get(compact_sig_hex.as_str())?;
                            got.is_some()
                        };
                        if already_used {
                            events.push(self.new_event(
                                "nat",
                                EventFamily::Auth,
                                EventType::TokenAuthCreateRejected,
                                block,
                                occurred_at,
                                None,
                                Some(inscription_id.to_string()),
                                Some(pa.creator.clone()),
                                None,
                                EventDelta::default(),
                                serde_json::json!({ "reason": "sig_replayed" }),
                                block_hash,
                            ));
                        } else {
                            {
                                let mut t = wtx.open_table(tables::TOKEN_AUTH_RECORDS)?;
                                let rec = tables::TokenAuthRecord {
                                    inscription_id: inscription_id.to_string(),
                                    authority_addr: pa.creator.clone(),
                                    whitelisted_tickers: auth_tickers.clone(),
                                    authority_pubkey_hex: pubkey_hex.clone(),
                                    created_height: block.height,
                                };
                                t.insert(inscription_id, encode(&rec)?.as_slice())?;
                            }
                            {
                                let mut t = wtx.open_table(tables::TOKEN_AUTH_SIG_REPLAY)?;
                                t.insert(compact_sig_hex.as_str(), 1u8)?;
                            }
                            events.push(self.new_event(
                                "nat",
                                EventFamily::Auth,
                                EventType::TokenAuthCreateRegistered,
                                block,
                                occurred_at,
                                None,
                                Some(inscription_id.to_string()),
                                Some(pa.creator.clone()),
                                None,
                                EventDelta::default(),
                                serde_json::json!({
                                    "whitelisted_tickers": auth_tickers,
                                    "pubkey": pubkey_hex,
                                }),
                                block_hash,
                            ));
                        }
                    }
                    Err(fail) => {
                        events.push(self.new_event(
                            "nat",
                            EventFamily::Auth,
                            EventType::TokenAuthCreateRejected,
                            block,
                            occurred_at,
                            None,
                            Some(inscription_id.to_string()),
                            Some(pa.creator.clone()),
                            None,
                            EventDelta::default(),
                            serde_json::json!({ "reason": format!("{:?}", fail) }),
                            block_hash,
                        ));
                    }
                }
            }
        }
        pa.consumed_height = Some(block.height);
        {
            let mut table = wtx.open_table(tables::PENDING_AUTHS)?;
            table.insert(inscription_id, encode(&pa)?.as_slice())?;
        }
        Ok(())
    }

    /// Execute a `token-auth` redeem (balance-moving path) at reveal
    /// time. Mirrors ord-tap `/tmp/ot_auth.rs:42-102`.
    #[allow(clippy::too_many_arguments)]
    fn process_auth_redeem(
        &mut self,
        wtx: &redb::WriteTransaction,
        rr: AuthRedeemReveal,
        block: &BlockView,
        occurred_at: DateTime<Utc>,
        block_hash: &str,
        events: &mut Vec<LedgerEvent>,
    ) -> Result<()> {
        let p = &rr.payload;
        // 1. Verify ECDSA over sha256(serde_json(redeem_subtree) || salt).
        let redeem_json = match serde_json::to_vec(&p.redeem_subtree_raw) {
            Ok(v) => v,
            Err(_) => {
                events.push(self.new_event(
                    "nat",
                    EventFamily::Auth,
                    EventType::TokenAuthRedeemRejected,
                    block,
                    occurred_at,
                    Some(rr.tx_index),
                    Some(rr.inscription_id.clone()),
                    Some(rr.inscriber_addr.clone()),
                    None,
                    EventDelta::default(),
                    serde_json::json!({ "reason": "redeem_subtree_encode_failed" }),
                    block_hash,
                ));
                return Ok(());
            }
        };
        let msg_hash = crate::crypto::ecdsa_recover::compute_msg_hash(&redeem_json, &p.salt);
        let verify = crate::crypto::ecdsa_recover::verify_ecdsa_recover(
            &p.sig.v, &p.sig.r, &p.sig.s, &p.hash, &msg_hash,
        );
        let (compact_sig, redeemer_pubkey_hex) = match verify {
            Ok(ok) => ok,
            Err(fail) => {
                events.push(self.new_event(
                    "nat",
                    EventFamily::Auth,
                    EventType::TokenAuthRedeemRejected,
                    block,
                    occurred_at,
                    Some(rr.tx_index),
                    Some(rr.inscription_id.clone()),
                    Some(rr.inscriber_addr.clone()),
                    None,
                    EventDelta::default(),
                    serde_json::json!({ "reason": format!("{:?}", fail) }),
                    block_hash,
                ));
                return Ok(());
            }
        };
        // 2. Authority lookup.
        let rec: Option<tables::TokenAuthRecord> = {
            let t = wtx.open_table(tables::TOKEN_AUTH_RECORDS)?;
            let raw = t.get(p.auth_id.as_str())?;
            raw.and_then(|v| decode::<tables::TokenAuthRecord>(v.value()).ok())
        };
        let Some(rec) = rec else {
            events.push(self.new_event(
                "nat",
                EventFamily::Auth,
                EventType::TokenAuthRedeemRejected,
                block,
                occurred_at,
                Some(rr.tx_index),
                Some(rr.inscription_id.clone()),
                Some(rr.inscriber_addr.clone()),
                None,
                EventDelta::default(),
                serde_json::json!({ "reason": "authority_missing", "auth_id": p.auth_id }),
                block_hash,
            ));
            return Ok(());
        };
        // 3. Pubkey equality.
        if rec.authority_pubkey_hex.to_lowercase() != redeemer_pubkey_hex.to_lowercase() {
            events.push(self.new_event(
                "nat",
                EventFamily::Auth,
                EventType::TokenAuthRedeemRejected,
                block,
                occurred_at,
                Some(rr.tx_index),
                Some(rr.inscription_id.clone()),
                Some(rr.inscriber_addr.clone()),
                None,
                EventDelta::default(),
                serde_json::json!({ "reason": "pubkey_mismatch" }),
                block_hash,
            ));
            return Ok(());
        }
        // 4. Replay guard.
        let replayed = {
            let t = wtx.open_table(tables::TOKEN_AUTH_SIG_REPLAY)?;
            let got = t.get(compact_sig.as_str())?;
            got.is_some()
        };
        if replayed {
            events.push(self.new_event(
                "nat",
                EventFamily::Auth,
                EventType::TokenAuthRedeemRejected,
                block,
                occurred_at,
                Some(rr.tx_index),
                Some(rr.inscription_id.clone()),
                Some(rr.inscriber_addr.clone()),
                None,
                EventDelta::default(),
                serde_json::json!({ "reason": "sig_replayed" }),
                block_hash,
            ));
            return Ok(());
        }
        // 5. Whitelist at height >= 916,233.
        const TAP_AUTH_ITEM_LENGTH_ACTIVATION_HEIGHT: u64 = 916_233;
        if block.height >= TAP_AUTH_ITEM_LENGTH_ACTIVATION_HEIGHT
            && !rec.whitelisted_tickers.is_empty()
        {
            for it in &p.items {
                if !rec.whitelisted_tickers.iter().any(|t| t == &it.ticker) {
                    events.push(self.new_event(
                        "nat",
                        EventFamily::Auth,
                        EventType::TokenAuthRedeemRejected,
                        block,
                        occurred_at,
                        Some(rr.tx_index),
                        Some(rr.inscription_id.clone()),
                        Some(rr.inscriber_addr.clone()),
                        None,
                        EventDelta::default(),
                        serde_json::json!({
                            "reason": "whitelist_violation",
                            "ticker": it.ticker,
                        }),
                        block_hash,
                    ));
                    return Ok(());
                }
            }
        }
        // 6. Cancel guard.
        let cancelled = {
            let t = wtx.open_table(tables::TOKEN_AUTH_CANCELS)?;
            let got = t.get(rec.inscription_id.as_str())?;
            got.is_some()
        };
        if cancelled {
            events.push(self.new_event(
                "nat",
                EventFamily::Auth,
                EventType::TokenAuthRedeemRejected,
                block,
                occurred_at,
                Some(rr.tx_index),
                Some(rr.inscription_id.clone()),
                Some(rr.inscriber_addr.clone()),
                None,
                EventDelta::default(),
                serde_json::json!({ "reason": "authority_cancelled" }),
                block_hash,
            ));
            return Ok(());
        }
        // 7. Per-item execution.
        use crate::ledger::auth::{execute_redeem_item, RedeemItemOutcome};
        for item in &p.items {
            let canon = canonical_ticker(&item.ticker);
            // We only index NAT. Non-NAT items in a mixed-ticker redeem
            // are silently skipped; if this turns out to matter for the
            // audit we'll revisit.
            if canon != "nat" {
                continue;
            }
            let a = i128::try_from(item.amount).unwrap_or(i128::MAX);
            let outcome = execute_redeem_item(
                wtx,
                &canon,
                &rec.authority_addr,
                &item.address,
                item.amount,
                occurred_at,
            )?;
            match outcome {
                RedeemItemOutcome::Credited => {
                    events.push(self.new_event(
                        &canon,
                        EventFamily::Auth,
                        EventType::TokenAuthRedeemDebit,
                        block,
                        occurred_at,
                        Some(rr.tx_index),
                        Some(rr.inscription_id.clone()),
                        Some(rec.authority_addr.clone()),
                        Some(item.address.clone()),
                        EventDelta {
                            delta_total: -a,
                            delta_available: -a,
                            ..Default::default()
                        },
                        serde_json::json!({
                            "amount": item.amount.to_string(),
                            "auth_id": rec.inscription_id,
                        }),
                        block_hash,
                    ));
                    events.push(self.new_event(
                        &canon,
                        EventFamily::Auth,
                        EventType::TokenAuthRedeemCredit,
                        block,
                        occurred_at,
                        Some(rr.tx_index),
                        Some(rr.inscription_id.clone()),
                        Some(item.address.clone()),
                        Some(rec.authority_addr.clone()),
                        EventDelta {
                            delta_total: a,
                            delta_available: a,
                            ..Default::default()
                        },
                        serde_json::json!({
                            "amount": item.amount.to_string(),
                            "auth_id": rec.inscription_id,
                        }),
                        block_hash,
                    ));
                }
                RedeemItemOutcome::SelfSend => {
                    // Paired audit events, 0 deltas.
                    events.push(self.new_event(
                        &canon,
                        EventFamily::Auth,
                        EventType::TokenAuthRedeemDebit,
                        block,
                        occurred_at,
                        Some(rr.tx_index),
                        Some(rr.inscription_id.clone()),
                        Some(rec.authority_addr.clone()),
                        Some(item.address.clone()),
                        EventDelta::default(),
                        serde_json::json!({
                            "amount": item.amount.to_string(),
                            "self_send": true,
                            "auth_id": rec.inscription_id,
                        }),
                        block_hash,
                    ));
                    events.push(self.new_event(
                        &canon,
                        EventFamily::Auth,
                        EventType::TokenAuthRedeemCredit,
                        block,
                        occurred_at,
                        Some(rr.tx_index),
                        Some(rr.inscription_id.clone()),
                        Some(item.address.clone()),
                        Some(rec.authority_addr.clone()),
                        EventDelta::default(),
                        serde_json::json!({
                            "amount": item.amount.to_string(),
                            "self_send": true,
                            "auth_id": rec.inscription_id,
                        }),
                        block_hash,
                    ));
                }
                RedeemItemOutcome::Skipped => {
                    events.push(self.new_event(
                        &canon,
                        EventFamily::Auth,
                        EventType::TokenAuthRedeemSkipped,
                        block,
                        occurred_at,
                        Some(rr.tx_index),
                        Some(rr.inscription_id.clone()),
                        Some(rec.authority_addr.clone()),
                        Some(item.address.clone()),
                        EventDelta::default(),
                        serde_json::json!({
                            "amount": item.amount.to_string(),
                            "reason": "insufficient_available",
                            "auth_id": rec.inscription_id,
                        }),
                        block_hash,
                    ));
                }
            }
        }
        // 8. Write replay-guard marker on successful completion.
        {
            let mut t = wtx.open_table(tables::TOKEN_AUTH_SIG_REPLAY)?;
            t.insert(compact_sig.as_str(), 1u8)?;
        }
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

/// Accept only TAP payloads whose ticker matches the indexed asset in
/// the form ord-tap expects for that op.
///
/// ord-tap's dmt-deploy stores the deploy under `dmt-<short>`, and
/// dmt-mint prepends the same prefix on lookup, so both accept the
/// short form `nat`. Every other op does a literal `d/<tick>` lookup,
/// so it only accepts the long form `dmt-nat` (case-insensitive).
/// Mirroring this is necessary for balance parity — see scan.rs
/// comment at the admission site.
fn envelope_matches_indexed(env: &TapEnvelope) -> bool {
    let t = env.ticker().to_ascii_lowercase();
    match env {
        TapEnvelope::Deploy(_) | TapEnvelope::Mint(_) => t == "nat",
        // token-send / token-auth carry multiple items, each with its own
        // tick. We admit the envelope if ANY item references dmt-nat —
        // unrelated-ticker items inside the same inscription are filtered
        // at the per-item stage.
        TapEnvelope::Send(s) => s
            .items
            .iter()
            .any(|it| canonical_ticker(it.ticker.as_str()) == "nat"),
        TapEnvelope::Auth(a) => match &a.form {
            // Cancel form carries no ticker at all — admit. The cancel
            // either references a NAT authority (useful) or doesn't
            // (harmless; our scan won't find the auth record).
            crate::protocol::auth::TokenAuthForm::Cancel(_) => true,
            // Create form: admit if the whitelist is empty OR mentions
            // dmt-nat. Empty is ord-tap's permissive default.
            crate::protocol::auth::TokenAuthForm::Create(c) => {
                c.auth_tickers.is_empty()
                    || c.auth_tickers.iter().any(|t| canonical_ticker(t) == "nat")
            }
            // Redeem form: admit if any item references dmt-nat.
            crate::protocol::auth::TokenAuthForm::Redeem(r) => r
                .items
                .iter()
                .any(|it| canonical_ticker(it.ticker.as_str()) == "nat"),
        },
        _ => t == "dmt-nat",
    }
}

/// Fold a raw on-chain ticker string to its canonical form.
///
/// Per the DMT docs, `token-transfer` inscriptions carry `dmt-<name>`
/// while `dmt-deploy` / `dmt-mint` carry `<name>`. Strip the prefix so
/// both forms map to the same deployment ticker. Lowercased since TAP
/// matches case-insensitively.
fn canonical_ticker(raw: &str) -> String {
    // Do NOT trim. ord-tap matches the raw tick string literally, so
    // " dmt-nat" (with leading space) must NOT fold onto "nat". See
    // protocol/ticker.rs for the canonical parser that rejects whitespace
    // outright; this function handles callers that already hold a raw
    // or normalized string and just need case + prefix collapsing.
    let lower = raw.to_ascii_lowercase();
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
    let consumed = |k: InscriptionKind| {
        matches!(
            k,
            InscriptionKind::TokenTransfer
                | InscriptionKind::TokenSend
                | InscriptionKind::TokenAuth
        )
    };
    match mv {
        TrackerMove::Moved {
            inscription,
            new_owner_address,
            ..
        } => Some((
            inscription.inscription_id.clone(),
            new_owner_address.clone(),
            consumed(inscription.kind),
        )),
        TrackerMove::Burned { inscription, .. } => Some((
            inscription.inscription_id.clone(),
            None,
            consumed(inscription.kind),
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

/// Ord-style landing resolution: given a fresh envelope's optional
/// pointer, return the `(vout, offset_in_outpoint)` where the
/// inscription settles. Mirrors ord-tap
/// `index/updater/inscription_updater.rs:520-632`:
///
///   let offset = payload.pointer()
///       .filter(|&p| p < total_output_value)
///       .unwrap_or(default_offset /* = total_input_value before our input */);
///   // then walk outputs accumulating value; land where offset < end.
///
/// For fresh inscriptions on input 0 of a reveal tx,
/// `total_input_value` before input 0 is 0, so the default is sat 0 —
/// always output 0 at offset 0. A valid pointer reroutes to the sat
/// it addresses within the combined output range.
fn resolve_landing(tx: &bitcoin::Transaction, pointer: Option<u64>) -> (usize, u64) {
    let total_output_value: u64 = tx.output.iter().map(|o| o.value.to_sat()).sum();
    let target_sat = match pointer {
        Some(p) if p < total_output_value => p,
        _ => 0,
    };
    let mut cumulative: u64 = 0;
    for (vout, o) in tx.output.iter().enumerate() {
        let v = o.value.to_sat();
        let end = cumulative.saturating_add(v);
        if target_sat < end {
            return (vout, target_sat - cumulative);
        }
        cumulative = end;
    }
    // Total_output_value == 0 or target_sat == 0 & no outputs.
    (0, 0)
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

fn wallet_is_blocked(wtx: &redb::WriteTransaction, ticker: &str, address: &str) -> Result<bool> {
    let table = wtx.open_table(WALLET_STATE)?;
    let Some(raw) = table.get((ticker, address))? else {
        return Ok(false);
    };
    let state: WalletState = match decode(raw.value()) {
        Ok(s) => s,
        Err(_) => return Ok(false),
    };
    Ok(state.transferables_blocked)
}

/// Record a permanent marker that `address` has received a DMT coinbase
/// reward credit. The marker is the prerequisite signal for the
/// miner-reward-transfer-execution shield at height >= 942,002.
/// Mirrors ord-tap's `dmtrwd/<addr>` key.
fn dmt_reward_mark(wtx: &redb::WriteTransaction, address: &str) -> Result<()> {
    let mut table = wtx.open_table(DMT_REWARD_ADDRESSES)?;
    if table.get(address)?.is_none() {
        table.insert(address, 1u8)?;
    }
    Ok(())
}

fn is_dmt_reward_address(wtx: &redb::WriteTransaction, address: &str) -> Result<bool> {
    let table = wtx.open_table(DMT_REWARD_ADDRESSES)?;
    let found = table.get(address)?.is_some();
    Ok(found)
}

fn read_mint_total(wtx: &redb::WriteTransaction, ticker: &str) -> Result<u128> {
    let table = wtx.open_table(MINT_TOTALS)?;
    let Some(raw) = table.get(ticker)? else {
        return Ok(0);
    };
    let bytes = raw.value();
    if bytes.len() != 16 {
        return Ok(0);
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(bytes);
    Ok(u128::from_le_bytes(arr))
}

fn bump_mint_total(wtx: &redb::WriteTransaction, ticker: &str, delta: u128) -> Result<()> {
    let mut table = wtx.open_table(MINT_TOTALS)?;
    let current: u128 = match table.get(ticker)? {
        Some(raw) => {
            let bytes = raw.value();
            if bytes.len() == 16 {
                let mut a = [0u8; 16];
                a.copy_from_slice(bytes);
                u128::from_le_bytes(a)
            } else {
                0
            }
        }
        None => 0,
    };
    let next = current.saturating_add(delta);
    table.insert(ticker, next.to_le_bytes().as_slice())?;
    Ok(())
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
