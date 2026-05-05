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
use crate::inscription::envelope::{Envelope, JUBILEE_HEIGHT};
use crate::inscription::tracker::{
    BurnReason, InscriptionKind, InscriptionTracker, TrackedCarrier, TrackedInscription,
    TrackerMove,
};
use crate::ledger::coinbase::distribute;
use crate::ledger::deploy::Deployment;
use crate::ledger::event::{EventDelta, EventFamily, EventType, LedgerEvent};
use crate::ledger::mint::{resolve_mints, MintCandidate};
use crate::ledger::transfer::{
    resolve_transfer_inscribes, InscribeResolution, TransferBalanceSnapshot,
    TransferInscribeCandidate,
};
use crate::protocol::address::{
    address_from_script, is_valid_tap_address_at_height, normalize_address,
};
use crate::protocol::auth::TokenAuthForm;
use crate::protocol::control::ControlOp;
use crate::protocol::element::{DmtElementPayload, ElementField};
use crate::protocol::envelope::{
    decode_tap_payload_at_height, tap_compatible_content_type, TapEnvelope,
};
use crate::protocol::privilege::PrivilegeAuthForm;
use crate::protocol::ticker::ticker_is_valid_at_height;
use crate::protocol::token::{parse_scaled_u128, MAX_DEC_U64_STR};
use crate::protocol::trade::TokenTradePayload;
use crate::store::codec::{decode, encode};
use crate::store::tables::{
    self, cursor_get, cursor_set, Cursor, DailyStats, InscriptionIndex, InscriptionOwner,
    MintClaim, PendingControl as StoredPendingControl, ValidTransfer, WalletState, ACTIVITY_RECENT,
    DAILY_STATS, DEPLOYMENTS, DMT_REWARD_ADDRESSES, EVENTS, EVENTS_BY_ID, INSCRIPTIONS,
    INSCRIPTION_OWNERS, MINT_CLAIMS, MINT_TOTALS, PENDING_CONTROLS, STATS, TRANSFERABLES_BY_SENDER,
    VALID_TRANSFERS, WALLET_ACTIVITY, WALLET_STATE,
};
use crate::store::Store;
use crate::sync::reorg::{detect_reorg, rewind_cursor};

pub type EventBus = Arc<broadcast::Sender<LedgerEvent>>;

const TAP_VALUE_STRINGIFY_ACTIVATION_HEIGHT: u64 = 885_588;

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
    /// In-memory tracker state carried across commit batches. It is
    /// reloaded from redb only on startup or after a rewind.
    tracker: Option<InscriptionTracker>,
    /// Outpoints whose carrier set changed in the current commit batch.
    /// Persist only these keys instead of rewriting the full carrier map.
    tracker_dirty_keys: HashSet<String>,
    /// Count of blocks applied in the current (uncommitted) batch.
    pending_blocks: u64,
    /// Events queued for broadcast after commit.
    pending_broadcasts: Vec<LedgerEvent>,
}

struct ControlInscribe {
    inscription_id: String,
    ticker: String,
    address: String,
    op: ControlOp,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct TapOrderKey {
    tx_index: u32,
    landing_vout: u32,
    landing_offset: u64,
    phase: u8,
    rank: i64,
}

impl TapOrderKey {
    fn new(tx_index: u32, landing_vout: u32, landing_offset: u64, phase: u8, rank: i64) -> Self {
        Self {
            tx_index,
            landing_vout,
            landing_offset,
            phase,
            rank,
        }
    }
}

struct DmtElementReveal {
    inscription_id: String,
    owner: String,
    txid: String,
    landing_vout: u32,
    payload: DmtElementPayload,
}

struct PendingSendReveal {
    inscription_id: String,
    creator: String,
    tx_index: u32,
    event_ticker: String,
    payload: crate::protocol::send::TokenSendPayload,
}

struct PendingAuthReveal {
    inscription_id: String,
    creator: String,
    tx_index: u32,
    form: crate::store::tables::PendingAuthForm,
}

struct TokenDeployReveal {
    inscription_id: String,
    tx_index: u32,
    owner: String,
    payload: crate::protocol::token::TokenDeployPayload,
}

struct TokenMintReveal {
    inscription_id: String,
    tx_index: u32,
    owner: String,
    payload: crate::protocol::token::TokenMintPayload,
}

struct DmtDeployReveal {
    inscription_id: String,
    owner: String,
    payload: crate::protocol::deploy::DeployPayload,
}

struct DmtMintReveal {
    inscription_id: String,
    owner: String,
    payload: crate::protocol::mint::MintPayload,
    referenced_bits: u32,
    referenced_nonce: u32,
    blk_repr: String,
}

struct PendingTradeReveal {
    inscription_id: String,
    creator: String,
    tx_index: u32,
    payload: TokenTradePayload,
}

struct PendingPrivilegeAuthReveal {
    inscription_id: String,
    creator: String,
    form: crate::store::tables::PendingPrivilegeAuthForm,
}

/// Redeems execute at reveal time (unlike create/cancel which settle at
/// move). Carries the fully-parsed payload plus tx_index + inscriber
/// owner address.
struct AuthRedeemReveal {
    inscription_id: String,
    tx_index: u32,
    landing_vout: u32,
    landing_offset: u64,
    rank: i64,
    inscriber_addr: String,
    payload: crate::protocol::auth::TokenAuthRedeem,
}

struct OrderedTransferInscribe {
    inscription_id: String,
    inscription_number: i64,
    inscribed_block_height: u64,
    ticker: String,
    raw_ticker: String,
    address: crate::protocol::address::NormalizedAddress,
    amount_raw: String,
    tx_index: u32,
    landing_vout: u32,
    landing_offset: u64,
}

enum OrderedTapOp {
    Move { tx_index: u32, mv: TrackerMove },
    DmtElement(TapOrderKey, DmtElementReveal),
    DmtDeploy(TapOrderKey, DmtDeployReveal),
    DmtMint(TapOrderKey, DmtMintReveal),
    TokenDeploy(TapOrderKey, TokenDeployReveal),
    TokenMint(TapOrderKey, TokenMintReveal),
    SendReveal(TapOrderKey, PendingSendReveal),
    TradeReveal(TapOrderKey, PendingTradeReveal),
    AuthReveal(TapOrderKey, PendingAuthReveal),
    PrivilegeAuthReveal(TapOrderKey, PendingPrivilegeAuthReveal),
    Control(TapOrderKey, ControlInscribe),
    AuthRedeem(AuthRedeemReveal),
    TransferInscribe(OrderedTransferInscribe),
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
            tracker_dirty_keys: HashSet::new(),
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
        self.migrate_events_by_id()?;
        self.seed_next_event_id()?;
        self.migrate_transferables_by_sender()?;

        loop {
            if let Some(rewind) =
                detect_reorg(&self.store, &self.rpc, self.cfg.sync.finality_depth).await?
            {
                rewind_cursor(&self.store, rewind)?;
                self.tracker = None;
                self.tracker_dirty_keys.clear();
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

        let nat_activation_height = 817_709;
        if self.cfg.sync.start_height <= nat_activation_height {
            return Ok(());
        }

        let known = vec![Deployment {
            ticker: normalize_ticker("dmt-nat").unwrap(),
            deploy_inscription_id:
                "4d967af36dcacd7e6199c39bda855d7b1b37268f4c8031fed5403a99ac57fe67i0".to_string(),
            dmt: true,
            element_inscription_id:
                "63b5bd2e28c043c4812981718e65d202ab8f68c0f6a1834d9ebea49d8fac7e62i0".to_string(),
            element_field: ElementField::Bits,
            dt: Some("n".into()),
            dim: None,
            bits_mode: BitsMode::RawHex,
            decimals: 0,
            mint_limit: 0,
            privilege_auth: None,
            activation_height: nat_activation_height,
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
        if let Some(next_event_id) = tables::next_event_id_get(&tx)? {
            self.next_event_id = next_event_id;
            return Ok(());
        }

        let table = tx.open_table(EVENTS_BY_ID)?;
        let max_id: u64 = table
            .iter()?
            .rev()
            .next()
            .transpose()?
            .map(|(k, _)| k.value())
            .unwrap_or(0);
        self.next_event_id = max_id + 1;
        drop(table);
        drop(tx);

        let wtx = self.store.write()?;
        tables::next_event_id_set(&wtx, self.next_event_id)?;
        wtx.commit()?;
        Ok(())
    }

    /// Back-fill the global event stream index from the legacy
    /// per-ticker event table. New writes maintain both tables.
    fn migrate_events_by_id(&self) -> Result<()> {
        const FLAG: &str = "migration_events_by_id_v1";
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
            let events = wtx.open_table(EVENTS)?;
            let mut by_id = wtx.open_table(EVENTS_BY_ID)?;
            for row in events.iter()? {
                let Ok((_k, v)) = row else { continue };
                let Ok(ev) = decode::<LedgerEvent>(v.value()) else {
                    continue;
                };
                by_id.insert(ev.event_id, v.value())?;
                inserted += 1;
            }
        }
        {
            let done: &[u8] = b"done";
            let mut meta = wtx.open_table(tables::META)?;
            meta.insert(FLAG, done)?;
        }
        wtx.commit()?;
        info!(inserted, "backfilled events_by_id index");
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
            let tracker = self.load_tracker_from(&wtx)?;
            self.tracker = Some(tracker);
            self.tracker_dirty_keys.clear();
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
            self.save_dirty_carriers(&wtx, tracker)?;
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
        tables::next_event_id_set(&wtx, self.next_event_id)?;
        wtx.commit()?;
        self.tracker_dirty_keys.clear();
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
        let mut deployments = self.load_deployments_from(wtx)?;
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
            /// default sat-flow (inscription lands at the output-range
            /// offset equal to the cumulative input value before this
            /// envelope's input).
            pointer: Option<u64>,
            /// Input index that carried this envelope. ord-tap uses the
            /// cumulative input value before this input as the default
            /// fresh-inscription offset.
            input_index: u32,
            env: Envelope,
            payload: Option<ParsedPayload>,
        }
        #[derive(Clone)]
        enum ParsedPayload {
            Tap(TapEnvelope),
            DmtElement(DmtElementPayload),
        }
        struct PreParsedTx {
            tx_index: u32,
            txid: bitcoin::Txid,
            envelopes: Vec<ParsedEnvelope>,
        }
        let block_height = block.height;
        let decode_payload = |env: &Envelope| -> Option<ParsedPayload> {
            if let Ok(Some(payload)) = decode_tap_payload_at_height(env, block_height) {
                Some(ParsedPayload::Tap(payload))
            } else if tap_compatible_content_type(env) {
                let body = env.decoded_content();
                std::str::from_utf8(&body)
                    .ok()
                    .and_then(|s| {
                        crate::protocol::element::parse_dmt_element(s)
                            .ok()
                            .flatten()
                    })
                    .map(ParsedPayload::DmtElement)
            } else {
                None
            }
        };
        let pre_parsed: Vec<PreParsedTx> = block
            .txs
            .par_iter()
            .enumerate()
            .map(|(tx_index, txv)| {
                let envelopes =
                    crate::inscription::envelope::parse_envelopes_at_height(&txv.tx, block_height);
                let mut with_payloads = Vec::new();
                for (env_pos, env) in envelopes.iter().enumerate() {
                    let payload = if env.delegate.is_none() {
                        decode_payload(env)
                    } else {
                        None
                    };
                    if payload.is_some() || env.delegate.is_some() {
                        let global_index = match env.kind {
                            crate::inscription::envelope::EnvelopeKind::Inscription { index } => {
                                index
                            }
                        };
                        with_payloads.push(ParsedEnvelope {
                            global_index,
                            env_pos: env_pos as u32,
                            pointer: env.pointer,
                            input_index: env.input_index,
                            env: env.clone(),
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
        let mut transfer_inscribes_unresolved: Vec<OrderedTransferInscribe> = Vec::new();
        let mut ordered_reveals: Vec<OrderedTapOp> = Vec::new();
        let token_deploy_reveals: Vec<TokenDeployReveal> = Vec::new();
        let token_mint_reveals: Vec<TokenMintReveal> = Vec::new();
        let mut auth_redeem_reveals: Vec<AuthRedeemReveal> = Vec::new();
        let mut fresh_carriers: Vec<(OutPoint, TrackedCarrier, Option<String>)> = Vec::new();
        let mut moves: Vec<(u32, TrackerMove)> = Vec::new();
        let mut fee_prefixes: Option<Vec<u64>> = None;
        let mut inscription_rows: Vec<(String, InscriptionIndex)> = Vec::new();
        let mut delegate_payload_cache: HashMap<String, Option<Envelope>> = HashMap::new();

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
            let total_output_value: u64 = txv.tx.output.iter().map(|o| o.value.to_sat()).sum();
            let needs_input_offsets = pre.envelopes.iter().any(|pe| {
                let pointer_lands = matches!(pe.pointer, Some(p) if p < total_output_value);
                pe.input_index > 0 && !pointer_lands
            });
            let input_start_offsets = if needs_input_offsets {
                let input_values = self
                    .load_input_values(&txv.tx, tracker, &block_utxos)
                    .await?;
                let mut offsets = Vec::with_capacity(txv.tx.input.len());
                let mut cumulative = 0u64;
                for txin in &txv.tx.input {
                    offsets.push(cumulative);
                    cumulative = cumulative.saturating_add(
                        input_values
                            .get(&txin.previous_output)
                            .copied()
                            .unwrap_or(0),
                    );
                }
                Some(offsets)
            } else {
                None
            };
            for pe in &pre.envelopes {
                if block.height < JUBILEE_HEIGHT
                    && pe.pointer.is_none()
                    && txv
                        .tx
                        .input
                        .get(pe.input_index as usize)
                        .map(|txin| tracker.has_at_offset(&txin.previous_output, 0))
                        .unwrap_or(false)
                {
                    continue;
                }
                let payload = if let Some(payload) = pe.payload.clone() {
                    payload
                } else {
                    let Some(effective) = self
                        .resolve_effective_delegate_envelope(&pe.env, &mut delegate_payload_cache)
                        .await?
                    else {
                        continue;
                    };
                    let Some(payload) = decode_payload(&effective) else {
                        continue;
                    };
                    payload
                };
                // Ord-tap's default sat-flow: each fresh envelope starts
                // at `total_input_value` before the input that carried it.
                // A pointer tag reroutes the inscription to `pointer` if
                // `pointer < total_output_value`.
                // Ref: ord-tap `index/updater/inscription_updater.rs:458,520-524`.
                //
                // Resolve that absolute sat offset into `(vout,
                // offset_in_outpoint)` by walking outputs once.
                let default_offset = input_start_offsets
                    .as_ref()
                    .and_then(|offsets| offsets.get(pe.input_index as usize).copied())
                    .unwrap_or(0);
                let Some((landing_vout, landing_offset)) =
                    resolve_landing(&txv.tx, pe.pointer, default_offset)
                else {
                    continue;
                };
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
                let sequence_rank = inscription_sequence_rank(block.height, within_block_rank);
                let insc_id = format!("{}i{}", pre.txid, pe.global_index);
                let order_key = TapOrderKey::new(
                    tx_index,
                    landing_vout as u32,
                    landing_offset,
                    1,
                    within_block_rank,
                );
                match payload {
                    ParsedPayload::DmtElement(ep) => {
                        let Some(addr) = to_addr.as_deref().and_then(normalize_address) else {
                            continue;
                        };
                        ordered_reveals.push(OrderedTapOp::DmtElement(
                            order_key,
                            DmtElementReveal {
                                inscription_id: insc_id.clone(),
                                owner: addr.as_str().to_string(),
                                txid: pre.txid.to_string(),
                                landing_vout: landing_vout as u32,
                                payload: ep.clone(),
                            },
                        ));
                        inscription_rows.push((
                            insc_id,
                            InscriptionIndex {
                                ticker: format!("dmt-el:{}", ep.name),
                                kind: "dmt-element".to_string(),
                                original_amount: None,
                                inscribed_height: block.height,
                                current_owner_address: to_addr.clone(),
                                consumed_height: None,
                            },
                        ));
                        continue;
                    }
                    ParsedPayload::Tap(payload) => {
                        // Admit all supported TAP envelopes. Per-op deployment
                        // lookups below decide whether an inscription is active;
                        // this is required for token-trade parity because DMT-NAT
                        // trades can settle against non-DMT TAP tokens.
                        let tick_canon = canonical_ticker(payload.ticker());
                        match payload {
                            TapEnvelope::TokenDeploy(dp) => {
                                let Some(addr) = to_addr.as_deref().and_then(normalize_address)
                                else {
                                    continue;
                                };
                                ordered_reveals.push(OrderedTapOp::TokenDeploy(
                                    order_key,
                                    TokenDeployReveal {
                                        inscription_id: insc_id.clone(),
                                        tx_index,
                                        owner: addr.as_str().to_string(),
                                        payload: dp.clone(),
                                    },
                                ));
                                inscription_rows.push((
                                    insc_id,
                                    InscriptionIndex {
                                        ticker: tick_canon.clone(),
                                        kind: "token-deploy".to_string(),
                                        original_amount: Some(dp.max_supply),
                                        inscribed_height: block.height,
                                        current_owner_address: to_addr.clone(),
                                        consumed_height: None,
                                    },
                                ));
                            }
                            TapEnvelope::TokenMint(mp) => {
                                let Some(addr) = to_addr.as_deref().and_then(normalize_address)
                                else {
                                    continue;
                                };
                                ordered_reveals.push(OrderedTapOp::TokenMint(
                                    order_key,
                                    TokenMintReveal {
                                        inscription_id: insc_id.clone(),
                                        tx_index,
                                        owner: addr.as_str().to_string(),
                                        payload: mp.clone(),
                                    },
                                ));
                                inscription_rows.push((
                                    insc_id,
                                    InscriptionIndex {
                                        ticker: tick_canon.clone(),
                                        kind: "token-mint".to_string(),
                                        original_amount: None,
                                        inscribed_height: block.height,
                                        current_owner_address: to_addr.clone(),
                                        consumed_height: None,
                                    },
                                ));
                            }
                            TapEnvelope::Deploy(dp) => {
                                if let Some(addr) = to_addr.as_deref().and_then(normalize_address) {
                                    ordered_reveals.push(OrderedTapOp::DmtDeploy(
                                        order_key,
                                        DmtDeployReveal {
                                            inscription_id: insc_id.clone(),
                                            owner: addr.as_str().to_string(),
                                            payload: dp.clone(),
                                        },
                                    ));
                                }
                                inscription_rows.push((
                                    insc_id,
                                    InscriptionIndex {
                                        ticker: dmt_effective_ticker(dp.ticker.as_str()),
                                        kind: "dmt-deploy".to_string(),
                                        original_amount: None,
                                        inscribed_height: block.height,
                                        current_owner_address: to_addr.clone(),
                                        consumed_height: None,
                                    },
                                ));
                            }
                            TapEnvelope::Mint(mp) => {
                                // Short-circuit bogus `blk` values so we don't
                                // RPC a non-existent height (which would fail
                                // with code=-8 and kill the tick). Downstream
                                // rejects them as blk_out_of_range.
                                let (referenced_bits, referenced_nonce) = if mp.block_number
                                    > block.height
                                {
                                    (0, 0)
                                } else if mp.block_number == block.height {
                                    (block.bits, block.nonce)
                                } else if let Some(&b) = self.bits_cache.get(&mp.block_number) {
                                    let (_bits, nonce) =
                                        fetch_block_header_fields_retry(&self.rpc, mp.block_number)
                                            .await?;
                                    (b, nonce)
                                } else {
                                    let (b, nonce) =
                                        fetch_block_header_fields_retry(&self.rpc, mp.block_number)
                                            .await?;
                                    if self.bits_cache.len() > 4096 {
                                        self.bits_cache.clear();
                                    }
                                    self.bits_cache.insert(mp.block_number, b);
                                    (b, nonce)
                                };
                                let Some(addr) = to_addr.as_deref().and_then(normalize_address)
                                else {
                                    continue;
                                };
                                let dmt_ticker = dmt_effective_ticker(mp.ticker.as_str());
                                if dmt_ticker == "dmt-nat" {
                                    if let Some(dep) = deployments.get(dmt_ticker.as_str()) {
                                        mint_cands
                                            .entry(dep.ticker.as_str().to_string())
                                            .or_default()
                                            .push(MintCandidate {
                                                inscription_id: insc_id.clone(),
                                                inscription_number: within_block_rank,
                                                inscribed_block_height: block.height,
                                                address: addr.clone(),
                                                payload: mp.clone(),
                                                referenced_bits,
                                                tx_index,
                                            });
                                    }
                                } else {
                                    ordered_reveals.push(OrderedTapOp::DmtMint(
                                        order_key,
                                        DmtMintReveal {
                                            inscription_id: insc_id.clone(),
                                            owner: addr.as_str().to_string(),
                                            payload: mp.clone(),
                                            referenced_bits,
                                            referenced_nonce,
                                            blk_repr: mp.block_raw.clone(),
                                        },
                                    ));
                                }
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
                                            ticker: dmt_ticker.clone(),
                                            kind: InscriptionKind::Mint,
                                            sequence_rank,
                                        },
                                        offset_in_outpoint: landing_offset,
                                        outpoint_value_sats: reveal_value,
                                    },
                                    to_addr.clone(),
                                ));
                                inscription_rows.push((
                                    insc_id,
                                    InscriptionIndex {
                                        ticker: dmt_ticker.clone(),
                                        kind: "dmt-mint".to_string(),
                                        original_amount: None,
                                        inscribed_height: block.height,
                                        current_owner_address: to_addr.clone(),
                                        consumed_height: None,
                                    },
                                ));
                            }
                            TapEnvelope::Transfer(tp) => {
                                if block.height >= TAP_VALUE_STRINGIFY_ACTIVATION_HEIGHT
                                    && tp.amount_was_number
                                {
                                    continue;
                                }
                                let Some(addr) = to_addr.as_deref().and_then(normalize_address)
                                else {
                                    continue;
                                };
                                transfer_inscribes_unresolved.push(OrderedTransferInscribe {
                                    inscription_id: insc_id.clone(),
                                    inscription_number: within_block_rank,
                                    inscribed_block_height: block.height,
                                    ticker: tick_canon.clone(),
                                    raw_ticker: tp.ticker.as_str().to_string(),
                                    address: addr.clone(),
                                    amount_raw: tp.amount_raw.clone(),
                                    tx_index,
                                    landing_vout: landing_vout as u32,
                                    landing_offset,
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
                                            kind: InscriptionKind::TokenTransfer,
                                            sequence_rank,
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
                                        original_amount: None,
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
                                let Some(addr) = to_addr.as_deref().and_then(normalize_address)
                                else {
                                    continue;
                                };
                                if is_dmt_reward_address_at_height(
                                    wtx,
                                    addr.as_str(),
                                    block.height,
                                )? {
                                    continue;
                                }
                                if block.height >= TAP_VALUE_STRINGIFY_ACTIVATION_HEIGHT
                                    && sp.items.iter().any(|it| it.amount_was_number)
                                {
                                    continue;
                                }
                                if sp.items.iter().any(|it| {
                                    !is_valid_tap_address_at_height(
                                        it.recipient.as_str(),
                                        block.height,
                                    )
                                }) {
                                    continue;
                                }
                                let index_ticker = sp
                                    .items
                                    .first()
                                    .map(|i| canonical_ticker(i.ticker.as_str()))
                                    .unwrap_or_else(|| "dmt-nat".to_string());
                                ordered_reveals.push(OrderedTapOp::SendReveal(
                                    order_key,
                                    PendingSendReveal {
                                        inscription_id: insc_id.clone(),
                                        creator: addr.as_str().to_string(),
                                        tx_index,
                                        event_ticker: index_ticker.clone(),
                                        payload: sp.clone(),
                                    },
                                ));
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
                                            ticker: index_ticker.clone(),
                                            kind: InscriptionKind::TokenSend,
                                            sequence_rank,
                                        },
                                        offset_in_outpoint: landing_offset,
                                        outpoint_value_sats: reveal_value,
                                    },
                                    to_addr.clone(),
                                ));
                                inscription_rows.push((
                                    insc_id,
                                    InscriptionIndex {
                                        ticker: index_ticker,
                                        kind: "token-send".to_string(),
                                        original_amount: None,
                                        inscribed_height: block.height,
                                        current_owner_address: to_addr.clone(),
                                        consumed_height: None,
                                    },
                                ));
                            }
                            TapEnvelope::Auth(ap) => {
                                let Some(addr) = to_addr.as_deref().and_then(normalize_address)
                                else {
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
                                            landing_vout: landing_vout as u32,
                                            landing_offset,
                                            rank: within_block_rank,
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
                                                    ticker: "dmt-nat".to_string(),
                                                    kind: InscriptionKind::TokenAuth,
                                                    sequence_rank,
                                                },
                                                offset_in_outpoint: landing_offset,
                                                outpoint_value_sats: reveal_value,
                                            },
                                            to_addr.clone(),
                                        ));
                                        inscription_rows.push((
                                            insc_id,
                                            InscriptionIndex {
                                                ticker: "dmt-nat".to_string(),
                                                kind: "token-auth".to_string(),
                                                original_amount: None,
                                                inscribed_height: block.height,
                                                current_owner_address: to_addr.clone(),
                                                consumed_height: None,
                                            },
                                        ));
                                    }
                                    TokenAuthForm::Create(c) => {
                                        let auth_tickers: Vec<String> = c
                                            .auth_tickers
                                            .iter()
                                            .map(|t| canonical_ticker(t))
                                            .collect();
                                        // Serialize the auth array byte-exact
                                        // for later re-hash at move time.
                                        let auth_array_json =
                                            match serde_json::to_string(&c.auth_array_raw) {
                                                Ok(s) => s,
                                                Err(_) => continue,
                                            };
                                        let form = crate::store::tables::PendingAuthForm::Create {
                                            auth_tickers,
                                            sig_v: c.sig.v.clone(),
                                            sig_r: c.sig.r.clone(),
                                            sig_s: c.sig.s.clone(),
                                            hash_hex: c.hash.clone(),
                                            salt: c.salt.clone(),
                                            auth_array_json,
                                        };
                                        ordered_reveals.push(OrderedTapOp::AuthReveal(
                                            order_key,
                                            PendingAuthReveal {
                                                inscription_id: insc_id.clone(),
                                                creator: creator.clone(),
                                                tx_index,
                                                form,
                                            },
                                        ));
                                        fresh_carriers.push((
                                            OutPoint {
                                                txid: txv.txid,
                                                vout: landing_vout as u32,
                                            },
                                            TrackedCarrier {
                                                inscription: TrackedInscription {
                                                    inscription_id: insc_id.clone(),
                                                    ticker: "dmt-nat".to_string(),
                                                    kind: InscriptionKind::TokenAuth,
                                                    sequence_rank,
                                                },
                                                offset_in_outpoint: landing_offset,
                                                outpoint_value_sats: reveal_value,
                                            },
                                            to_addr.clone(),
                                        ));
                                        inscription_rows.push((
                                            insc_id,
                                            InscriptionIndex {
                                                ticker: "dmt-nat".to_string(),
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
                                        ordered_reveals.push(OrderedTapOp::AuthReveal(
                                            order_key,
                                            PendingAuthReveal {
                                                inscription_id: insc_id.clone(),
                                                creator: creator.clone(),
                                                tx_index,
                                                form,
                                            },
                                        ));
                                        fresh_carriers.push((
                                            OutPoint {
                                                txid: txv.txid,
                                                vout: landing_vout as u32,
                                            },
                                            TrackedCarrier {
                                                inscription: TrackedInscription {
                                                    inscription_id: insc_id.clone(),
                                                    ticker: "dmt-nat".to_string(),
                                                    kind: InscriptionKind::TokenAuth,
                                                    sequence_rank,
                                                },
                                                offset_in_outpoint: landing_offset,
                                                outpoint_value_sats: reveal_value,
                                            },
                                            to_addr.clone(),
                                        ));
                                        inscription_rows.push((
                                            insc_id,
                                            InscriptionIndex {
                                                ticker: "dmt-nat".to_string(),
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
                            TapEnvelope::PrivilegeAuth(ap) => {
                                let Some(addr) = to_addr.as_deref().and_then(normalize_address)
                                else {
                                    continue;
                                };
                                let creator = addr.as_str().to_string();
                                let form = match ap.form {
                                    PrivilegeAuthForm::Create(c) => {
                                        crate::store::tables::PendingPrivilegeAuthForm::Create {
                                            auth_json: c.auth_json.clone(),
                                            sig_v: c.sig.v.clone(),
                                            sig_r: c.sig.r.clone(),
                                            sig_s: c.sig.s.clone(),
                                            hash_hex: c.hash.clone(),
                                            salt: c.salt.clone(),
                                        }
                                    }
                                    PrivilegeAuthForm::Cancel(c) => {
                                        crate::store::tables::PendingPrivilegeAuthForm::Cancel {
                                            cancel_id: c.cancel_id.clone(),
                                        }
                                    }
                                };
                                ordered_reveals.push(OrderedTapOp::PrivilegeAuthReveal(
                                    order_key,
                                    PendingPrivilegeAuthReveal {
                                        inscription_id: insc_id.clone(),
                                        creator: creator.clone(),
                                        form,
                                    },
                                ));
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
                                            ticker: "dmt-nat".to_string(),
                                            kind: InscriptionKind::PrivilegeAuth,
                                            sequence_rank,
                                        },
                                        offset_in_outpoint: landing_offset,
                                        outpoint_value_sats: reveal_value,
                                    },
                                    to_addr.clone(),
                                ));
                                inscription_rows.push((
                                    insc_id,
                                    InscriptionIndex {
                                        ticker: "dmt-nat".to_string(),
                                        kind: "privilege-auth".to_string(),
                                        original_amount: None,
                                        inscribed_height: block.height,
                                        current_owner_address: to_addr.clone(),
                                        consumed_height: None,
                                    },
                                ));
                            }
                            TapEnvelope::Trade(tp) => {
                                let Some(addr) = to_addr.as_deref().and_then(normalize_address)
                                else {
                                    continue;
                                };
                                if is_dmt_reward_address_at_height(
                                    wtx,
                                    addr.as_str(),
                                    block.height,
                                )? {
                                    continue;
                                }
                                ordered_reveals.push(OrderedTapOp::TradeReveal(
                                    order_key,
                                    PendingTradeReveal {
                                        inscription_id: insc_id.clone(),
                                        creator: addr.as_str().to_string(),
                                        tx_index,
                                        payload: tp.clone(),
                                    },
                                ));
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
                                            kind: InscriptionKind::TokenTrade,
                                            sequence_rank,
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
                                        kind: "token-trade".to_string(),
                                        original_amount: None,
                                        inscribed_height: block.height,
                                        current_owner_address: to_addr.clone(),
                                        consumed_height: None,
                                    },
                                ));
                            }
                            TapEnvelope::Control(cp) => {
                                let Some(addr) = to_addr.as_deref().and_then(normalize_address)
                                else {
                                    continue;
                                };
                                ordered_reveals.push(OrderedTapOp::Control(
                                    order_key,
                                    ControlInscribe {
                                        inscription_id: insc_id.clone(),
                                        ticker: "dmt-nat".to_string(),
                                        address: addr.as_str().to_string(),
                                        op: cp.op,
                                    },
                                ));
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
                                            sequence_rank,
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
                }
            }
            // Carrier movement for this tx. Runs BEFORE registering
            // this tx's own fresh carriers — a tx cannot spend its own
            // outputs, so nothing this tx just inscribed can be moved
            // by this tx. Skip the RPC round-trip when no input matches
            // a tracked outpoint.
            let spends_tracked = txv.tx.input.iter().any(|i| tracker.has(&i.previous_output));
            if spends_tracked {
                let input_values = self
                    .load_input_values(&txv.tx, &tracker, &block_utxos)
                    .await?;
                let tx_moves = tracker.apply_tx(&txv.tx, &input_values);
                for m in tx_moves {
                    match m {
                        TrackerMove::Burned {
                            inscription,
                            from,
                            reason: BurnReason::IntoFees { fee_offset },
                        } => {
                            if fee_prefixes.is_none() {
                                fee_prefixes =
                                    Some(self.compute_fee_prefixes(block, &block_utxos).await?);
                            }
                            let prefix_before = fee_prefixes
                                .as_ref()
                                .and_then(|prefixes| prefixes.get(tx_index as usize).copied())
                                .unwrap_or(0);
                            let mapped = map_fee_flotsam_to_coinbase(
                                block,
                                inscription,
                                from,
                                prefix_before,
                                fee_offset,
                            );
                            if let TrackerMove::Moved {
                                inscription,
                                to,
                                to_offset,
                                to_outpoint_value_sats,
                                ..
                            } = &mapped
                            {
                                tracker.insert(
                                    *to,
                                    TrackedCarrier {
                                        inscription: inscription.clone(),
                                        offset_in_outpoint: *to_offset,
                                        outpoint_value_sats: *to_outpoint_value_sats,
                                    },
                                );
                            }
                            self.mark_tracker_move_dirty(&mapped);
                            // ord-tap processes coinbase last
                            // (`skip(1).chain(take(1))`). Fee-fallen
                            // inscriptions therefore settle to the
                            // block coinbase after all non-coinbase txs,
                            // not at the spending tx's position.
                            moves.push((block.txs.len() as u32, mapped));
                        }
                        other => {
                            self.mark_tracker_move_dirty(&other);
                            moves.push((tx_index, other));
                        }
                    }
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
                self.mark_tracker_dirty(*outpoint);
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

        // 1. Standard TAP token deployments.
        for dr in token_deploy_reveals {
            if !ticker_is_valid_at_height(&dr.payload.ticker, block.height) {
                continue;
            }
            if deployments.contains_key(dr.payload.ticker.as_str()) {
                continue;
            }
            let dep = Deployment {
                ticker: dr.payload.ticker.clone(),
                deploy_inscription_id: dr.inscription_id.clone(),
                dmt: false,
                element_inscription_id: String::new(),
                element_field: crate::protocol::element::ElementField::Bits,
                dt: None,
                dim: None,
                bits_mode: crate::protocol::bits::BitsMode::RawHex,
                decimals: dr.payload.decimals,
                mint_limit: dr.payload.mint_limit,
                privilege_auth: dr.payload.prv.clone(),
                activation_height: block.height,
                coinbase_activation: None,
                miner_transfer_activation: None,
                max_supply: dr.payload.max_supply,
                registered_at: occurred_at,
            };
            {
                let mut table = wtx.open_table(DEPLOYMENTS)?;
                table.insert(dep.ticker.as_str(), encode(&dep)?.as_slice())?;
            }
            deployments.insert(dep.ticker.as_str().to_string(), dep.clone());
            events.push(self.new_event(
                dep.ticker.as_str(),
                EventFamily::Deploy,
                EventType::DmtDeployRegistered,
                block,
                occurred_at,
                Some(dr.tx_index),
                Some(dr.inscription_id),
                Some(dr.owner),
                None,
                EventDelta::default(),
                serde_json::json!({
                    "max": dep.max_supply.to_string(),
                    "lim": dep.mint_limit.to_string(),
                    "dec": dep.decimals,
                    "dmt": false,
                }),
                &block_hash,
            ));
        }

        // 1b. Standard TAP token mints.
        for mr in token_mint_reveals {
            let ticker = mr.payload.ticker.as_str().to_string();
            let Some(dep) = deployments.get(&ticker) else {
                continue;
            };
            if dep.dmt || dep.privilege_auth.is_some() {
                continue;
            }
            let Ok(mut amount) = parse_scaled_u128(&mr.payload.amount_raw, dep.decimals) else {
                continue;
            };
            let total = read_mint_total(wtx, &ticker)?;
            if dep.mint_limit > 0 && amount > dep.mint_limit {
                events.push(self.new_event(
                    &ticker,
                    EventFamily::Mint,
                    EventType::DmtMintDuplicateRejected,
                    block,
                    occurred_at,
                    Some(mr.tx_index),
                    Some(mr.inscription_id),
                    Some(mr.owner),
                    None,
                    EventDelta::default(),
                    serde_json::json!({ "reason": "limit_exceeded" }),
                    &block_hash,
                ));
                continue;
            }
            let remaining = dep.max_supply.saturating_sub(total);
            if amount > remaining {
                amount = remaining;
            }
            if amount == 0 {
                continue;
            }
            let a = i128::try_from(amount).unwrap_or(i128::MAX);
            apply_wallet_delta(wtx, &ticker, &mr.owner, a, a, 0, 0, occurred_at)?;
            bump_mint_total(wtx, &ticker, amount)?;
            events.push(self.new_event(
                &ticker,
                EventFamily::Mint,
                EventType::DmtMintCredit,
                block,
                occurred_at,
                Some(mr.tx_index),
                Some(mr.inscription_id),
                Some(mr.owner),
                None,
                EventDelta {
                    delta_total: a,
                    delta_available: a,
                    ..Default::default()
                },
                serde_json::json!({ "amount": amount.to_string(), "standard_tap": true }),
                &block_hash,
            ));
        }

        // (Transfer-inscribe resolution moved below the settlement /
        //  mint / coinbase / auth-redeem phases so it reads the LIVE
        //  post-those-events wallet state, matching Tapalytics'
        //  tx-index-ordered traversal. Same-block credits via a
        //  transfer-settle, token-send-execute, auth-redeem, mint, or
        //  coinbase at a lower tx_index now count toward the
        //  inscribing wallet's available balance.)

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

        // 5. Coinbase distribution. Tapalytics makes DMT/NAT coinbase
        //    rewards available to TAP operations in the same block. This
        //    is required for block 927124's ba529... token-send: without
        //    the same-block reward, the creator is short by exactly
        //    200,000,000 units and the send is incorrectly skipped.
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
                    apply_wallet_delta(&wtx, &ticker, &addr, a, a, 0, 0, occurred_at)?;
                    // Coinbase issuance counts toward the supply cap -
                    // ord-tap's `dc/<tick>` counter decrements on both
                    // `dmt-mint` admits and DMT-reward coinbase credits.
                    bump_mint_total(&wtx, &ticker, share.share_amount)?;
                    let mut did_lock_now = false;
                    if share.should_lock_on_first_credit {
                        // dmtrwd marker is PERMANENT. ord-tap auto-blocks
                        // only when the marker is first created; later
                        // reward credits must not re-block a miner who
                        // deliberately unblocked.
                        let inserted_mark = dmt_reward_mark(&wtx, &addr)?;
                        if inserted_mark {
                            set_wallet_locked(&wtx, &ticker, &addr)?;
                            did_lock_now = true;
                        }
                    }
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

        // 6. Ordered TAP operations. ord-tap mutates token state as
        //     inscriptions and transfers are encountered in transaction
        //     order; batching all moves before or after all transfer
        //     creates changes same-block balance visibility.
        let mut ordered_ops: Vec<OrderedTapOp> = Vec::with_capacity(
            moves.len()
                + ordered_reveals.len()
                + auth_redeem_reveals.len()
                + transfer_inscribes_unresolved.len(),
        );
        ordered_ops.extend(ordered_reveals.into_iter());
        ordered_ops.extend(
            moves
                .drain(..)
                .map(|(tx_index, mv)| OrderedTapOp::Move { tx_index, mv }),
        );
        ordered_ops.extend(
            auth_redeem_reveals
                .into_iter()
                .map(OrderedTapOp::AuthRedeem),
        );
        ordered_ops.extend(
            transfer_inscribes_unresolved
                .into_iter()
                .map(OrderedTapOp::TransferInscribe),
        );
        ordered_ops.sort_by_key(ordered_reveal_key);

        for op in ordered_ops {
            match op {
                OrderedTapOp::DmtElement(_, er) => {
                    self.process_dmt_element(wtx, er, block, occurred_at)?;
                }
                OrderedTapOp::DmtDeploy(_, dr) => {
                    self.process_dmt_deploy(
                        wtx,
                        dr,
                        block,
                        occurred_at,
                        &block_hash,
                        &mut deployments,
                        &mut events,
                    )?;
                }
                OrderedTapOp::DmtMint(_, mr) => {
                    self.process_dmt_mint(
                        wtx,
                        mr,
                        block,
                        occurred_at,
                        &block_hash,
                        &deployments,
                        &mut events,
                    )?;
                }
                OrderedTapOp::TokenDeploy(_, dr) => {
                    self.process_token_deploy(
                        wtx,
                        dr,
                        block,
                        occurred_at,
                        &block_hash,
                        &mut deployments,
                        &mut events,
                    )?;
                }
                OrderedTapOp::TokenMint(_, mr) => {
                    self.process_token_mint(
                        wtx,
                        mr,
                        block,
                        occurred_at,
                        &block_hash,
                        &deployments,
                        &mut events,
                    )?;
                }
                OrderedTapOp::SendReveal(_, sr) => {
                    self.process_send_reveal(
                        wtx,
                        sr,
                        block,
                        occurred_at,
                        &block_hash,
                        &deployments,
                        &mut events,
                    )?;
                }
                OrderedTapOp::TradeReveal(_, tr) => {
                    self.process_trade_reveal(
                        wtx,
                        tr,
                        block,
                        occurred_at,
                        &block_hash,
                        &deployments,
                        &mut events,
                    )?;
                }
                OrderedTapOp::AuthReveal(_, ar) => {
                    self.process_auth_reveal(
                        wtx,
                        ar,
                        block,
                        occurred_at,
                        &block_hash,
                        &deployments,
                        &mut events,
                    )?;
                }
                OrderedTapOp::PrivilegeAuthReveal(_, pr) => {
                    self.process_privilege_auth_reveal(wtx, pr, block)?;
                }
                OrderedTapOp::Control(_, c) => {
                    self.process_control_reveal(
                        wtx,
                        c,
                        block,
                        occurred_at,
                        &block_hash,
                        &mut events,
                    )?;
                }
                OrderedTapOp::Move { tx_index, mv } => {
                    self.apply_moves(
                        wtx,
                        vec![(tx_index, mv)],
                        block,
                        occurred_at,
                        &block_hash,
                        &mut events,
                    )?;
                }
                OrderedTapOp::AuthRedeem(rr) => {
                    self.process_auth_redeem(
                        wtx,
                        rr,
                        block,
                        occurred_at,
                        &block_hash,
                        &mut events,
                    )?;
                }
                OrderedTapOp::TransferInscribe(ti) => {
                    let Some(dep) = deployments.get(ti.ticker.as_str()) else {
                        continue;
                    };
                    if !raw_transfer_tick_matches_deployment_kind(&ti.raw_ticker, dep.dmt) {
                        continue;
                    }
                    let Ok(amount) = parse_scaled_u128(&ti.amount_raw, dep.decimals) else {
                        continue;
                    };
                    if amount == 0 {
                        continue;
                    }
                    let candidates = vec![TransferInscribeCandidate {
                        inscription_id: ti.inscription_id,
                        inscription_number: ti.inscription_number,
                        inscribed_block_height: ti.inscribed_block_height,
                        ticker: ti.ticker,
                        address: ti.address,
                        amount,
                        tx_index: ti.tx_index,
                    }];
                    let balance_snapshot =
                        self.snapshot_transfer_create_balance_from(wtx, &candidates)?;
                    let blocked_senders = self.snapshot_blocked_senders_from(wtx, &candidates)?;
                    let inscribe_resolutions =
                        resolve_transfer_inscribes(candidates, &balance_snapshot, &blocked_senders);
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
                                    table
                                        .insert(inscription_id.as_str(), encode(&v)?.as_slice())?;
                                }
                                {
                                    let mut idx = wtx.open_table(TRANSFERABLES_BY_SENDER)?;
                                    idx.insert(
                                        (
                                            ticker.as_str(),
                                            address.as_str(),
                                            inscription_id.as_str(),
                                        ),
                                        1u8,
                                    )?;
                                }
                                let a = i128::try_from(amount).unwrap_or(i128::MAX);
                                reconcile_wallet_available(
                                    &wtx,
                                    &ticker,
                                    address.as_str(),
                                    occurred_at,
                                )?;
                                apply_wallet_delta(
                                    &wtx,
                                    &ticker,
                                    address.as_str(),
                                    0,
                                    -a,
                                    a,
                                    0,
                                    occurred_at,
                                )?;
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
                                snapshot_balance,
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
                                        "balance": snapshot_balance.to_string(),
                                        "reason": reason,
                                    }),
                                    &block_hash,
                                ));
                            }
                        }
                    }
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
            let mut by_id = wtx.open_table(EVENTS_BY_ID)?;
            for ev in &events {
                let encoded = encode(ev)?;
                table.insert((ev.ticker.as_str(), ev.event_id), encoded.as_slice())?;
                by_id.insert(ev.event_id, encoded.as_slice())?;
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

    async fn compute_fee_prefixes(
        &mut self,
        block: &BlockView,
        block_utxos: &HashMap<bitcoin::Txid, Vec<u64>>,
    ) -> Result<Vec<u64>> {
        let mut missing: Vec<OutPoint> = Vec::new();
        for txv in block.txs.iter().skip(1) {
            for txin in &txv.tx.input {
                let op = txin.previous_output;
                if op.is_null() || block_utxos.contains_key(&op.txid) {
                    continue;
                }
                missing.push(op);
            }
        }

        let mut fetched: HashMap<bitcoin::Txid, Vec<u64>> = HashMap::new();
        if !missing.is_empty() {
            let mut unique_txids: Vec<bitcoin::Txid> = missing.iter().map(|op| op.txid).collect();
            unique_txids.sort();
            unique_txids.dedup();
            let txs = self.rpc.get_raw_transactions_batch(&unique_txids).await?;
            fetched = unique_txids
                .into_iter()
                .zip(txs.into_iter())
                .filter_map(|(txid, maybe_tx)| {
                    maybe_tx.map(|tx| (txid, tx.output.iter().map(|o| o.value.to_sat()).collect()))
                })
                .collect();
        }

        let mut prefixes = Vec::with_capacity(block.txs.len());
        let mut cumulative_fees = 0u64;
        for (idx, txv) in block.txs.iter().enumerate() {
            prefixes.push(cumulative_fees);
            if idx == 0 {
                continue;
            }
            let input_sum = txv.tx.input.iter().fold(0u64, |acc, txin| {
                let op = txin.previous_output;
                if op.is_null() {
                    return acc;
                }
                let value = block_utxos
                    .get(&op.txid)
                    .and_then(|vals| vals.get(op.vout as usize).copied())
                    .or_else(|| {
                        fetched
                            .get(&op.txid)
                            .and_then(|vals| vals.get(op.vout as usize).copied())
                    })
                    .unwrap_or(0);
                acc.saturating_add(value)
            });
            let output_sum: u64 = txv.tx.output.iter().map(|o| o.value.to_sat()).sum();
            cumulative_fees = cumulative_fees.saturating_add(input_sum.saturating_sub(output_sum));
        }
        Ok(prefixes)
    }

    fn load_tracker_from(&self, wtx: &redb::WriteTransaction) -> Result<InscriptionTracker> {
        let table = wtx.open_multimap_table(INSCRIPTION_OWNERS)?;
        let mut t = InscriptionTracker::new();
        for row in table.iter()? {
            let (k, values) = row?;
            let key_str = k.value().to_string();
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
                                "token-trade" => InscriptionKind::TokenTrade,
                                "privilege-auth" => InscriptionKind::PrivilegeAuth,
                                _ => InscriptionKind::Control,
                            },
                            sequence_rank: owner.sequence_rank,
                        },
                        offset_in_outpoint: owner.offset_in_outpoint,
                        outpoint_value_sats: owner.outpoint_value_sats,
                    },
                );
            }
        }
        Ok(t)
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

    #[allow(clippy::too_many_arguments)]
    fn process_token_deploy(
        &mut self,
        wtx: &redb::WriteTransaction,
        dr: TokenDeployReveal,
        block: &BlockView,
        occurred_at: DateTime<Utc>,
        block_hash: &str,
        deployments: &mut HashMap<String, Deployment>,
        events: &mut Vec<LedgerEvent>,
    ) -> Result<()> {
        if !ticker_is_valid_at_height(&dr.payload.ticker, block.height) {
            return Ok(());
        }
        if dr.payload.prv.is_some()
            && !is_privilege_auth_active(wtx, dr.payload.prv.as_deref().unwrap())?
        {
            return Ok(());
        }
        if deployments.contains_key(dr.payload.ticker.as_str()) {
            return Ok(());
        }
        let dep = Deployment {
            ticker: dr.payload.ticker.clone(),
            deploy_inscription_id: dr.inscription_id.clone(),
            dmt: false,
            element_inscription_id: String::new(),
            element_field: ElementField::Bits,
            dt: None,
            dim: None,
            bits_mode: crate::protocol::bits::BitsMode::RawHex,
            decimals: dr.payload.decimals,
            mint_limit: dr.payload.mint_limit,
            privilege_auth: dr.payload.prv.clone(),
            activation_height: block.height,
            coinbase_activation: None,
            miner_transfer_activation: None,
            max_supply: dr.payload.max_supply,
            registered_at: occurred_at,
        };
        {
            let mut table = wtx.open_table(DEPLOYMENTS)?;
            table.insert(dep.ticker.as_str(), encode(&dep)?.as_slice())?;
        }
        deployments.insert(dep.ticker.as_str().to_string(), dep.clone());
        events.push(self.new_event(
            dep.ticker.as_str(),
            EventFamily::Deploy,
            EventType::DmtDeployRegistered,
            block,
            occurred_at,
            Some(dr.tx_index),
            Some(dr.inscription_id),
            Some(dr.owner),
            None,
            EventDelta::default(),
            serde_json::json!({
                "max": dep.max_supply.to_string(),
                "lim": dep.mint_limit.to_string(),
                "dec": dep.decimals,
                "dmt": false,
            }),
            block_hash,
        ));
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn process_token_mint(
        &mut self,
        wtx: &redb::WriteTransaction,
        mr: TokenMintReveal,
        block: &BlockView,
        occurred_at: DateTime<Utc>,
        block_hash: &str,
        deployments: &HashMap<String, Deployment>,
        events: &mut Vec<LedgerEvent>,
    ) -> Result<()> {
        let ticker = mr.payload.ticker.as_str().to_string();
        let Some(dep) = deployments.get(&ticker) else {
            return Ok(());
        };
        if dep.dmt {
            return Ok(());
        }
        let Ok(mut amount) = parse_scaled_u128(&mr.payload.amount_raw, dep.decimals) else {
            return Ok(());
        };
        let mut fail = false;
        if dep.mint_limit > 0 && amount > dep.mint_limit {
            fail = true;
        }
        let total = read_mint_total(wtx, &ticker)?;
        if !fail {
            let remaining = dep.max_supply.saturating_sub(total);
            if amount > remaining {
                amount = remaining;
            }
            if amount == 0 {
                fail = true;
            }
        }
        if !fail {
            if let Some(prv) = dep.privilege_auth.as_deref() {
                let ok = self.verify_privilege_mint(
                    wtx,
                    prv,
                    mr.payload.prv.as_ref(),
                    "tap",
                    "token-mint",
                    mr.payload.ticker.as_str(),
                    &mr.payload.amount_raw,
                    None,
                    mr.payload.dta.as_deref(),
                    &mr.owner,
                )?;
                if !ok {
                    fail = true;
                }
            }
        }
        if fail {
            events.push(self.new_event(
                &ticker,
                EventFamily::Mint,
                EventType::DmtMintDuplicateRejected,
                block,
                occurred_at,
                Some(mr.tx_index),
                Some(mr.inscription_id),
                Some(mr.owner),
                None,
                EventDelta::default(),
                serde_json::json!({ "reason": "mint_failed" }),
                block_hash,
            ));
            return Ok(());
        }
        let a = i128::try_from(amount).unwrap_or(i128::MAX);
        apply_wallet_delta(wtx, &ticker, &mr.owner, a, a, 0, 0, occurred_at)?;
        bump_mint_total(wtx, &ticker, amount)?;
        events.push(self.new_event(
            &ticker,
            EventFamily::Mint,
            EventType::DmtMintCredit,
            block,
            occurred_at,
            Some(mr.tx_index),
            Some(mr.inscription_id),
            Some(mr.owner),
            None,
            EventDelta {
                delta_total: a,
                delta_available: a,
                ..Default::default()
            },
            serde_json::json!({ "amount": amount.to_string(), "standard_tap": true }),
            block_hash,
        ));
        Ok(())
    }

    fn process_dmt_element(
        &mut self,
        wtx: &redb::WriteTransaction,
        er: DmtElementReveal,
        block: &BlockView,
        occurred_at: DateTime<Utc>,
    ) -> Result<()> {
        let sig = format!(
            "{}{}",
            er.payload.pattern.clone().unwrap_or_default(),
            er.payload.field_token
        );
        {
            let table = wtx.open_table(tables::DMT_ELEMENTS)?;
            if table.get(er.payload.name.as_str())?.is_some() {
                return Ok(());
            }
        }
        {
            let table = wtx.open_table(tables::DMT_ELEMENT_SIGNATURES)?;
            if table.get(sig.as_str())?.is_some() {
                return Ok(());
            }
        }
        let rec = tables::DmtElementRecord {
            name: er.payload.name.clone(),
            pattern: er.payload.pattern,
            field: match er.payload.field {
                ElementField::BlockHeight => 4,
                ElementField::Nonce => 10,
                ElementField::Bits => 11,
            },
            inscription_id: er.inscription_id.clone(),
            inscribed_height: block.height,
            txid: er.txid,
            vout: er.landing_vout,
            address: er.owner,
            timestamp: occurred_at.timestamp(),
        };
        {
            let mut table = wtx.open_table(tables::DMT_ELEMENTS)?;
            table.insert(rec.name.as_str(), encode(&rec)?.as_slice())?;
        }
        {
            let mut table = wtx.open_table(tables::DMT_ELEMENT_BY_INSCRIPTION)?;
            table.insert(er.inscription_id.as_str(), rec.name.as_str())?;
        }
        {
            let mut table = wtx.open_table(tables::DMT_ELEMENT_SIGNATURES)?;
            table.insert(sig.as_str(), 1u8)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn process_dmt_deploy(
        &mut self,
        wtx: &redb::WriteTransaction,
        dr: DmtDeployReveal,
        block: &BlockView,
        occurred_at: DateTime<Utc>,
        block_hash: &str,
        deployments: &mut HashMap<String, Deployment>,
        events: &mut Vec<LedgerEvent>,
    ) -> Result<()> {
        if !ticker_is_valid_at_height(&dr.payload.ticker, block.height) {
            return Ok(());
        }
        let ticker = dmt_effective_ticker(dr.payload.ticker.as_str());
        if deployments.contains_key(ticker.as_str()) {
            return Ok(());
        }
        if let Some(prv) = dr.payload.prv.as_deref() {
            if !is_privilege_auth_active(wtx, prv)? {
                return Ok(());
            }
        }
        let elem_name = {
            let table = wtx.open_table(tables::DMT_ELEMENT_BY_INSCRIPTION)?;
            let Some(v) = table.get(dr.payload.element_inscription_id.as_str())? else {
                return Ok(());
            };
            v.value().to_string()
        };
        let elem: tables::DmtElementRecord = {
            let table = wtx.open_table(tables::DMT_ELEMENTS)?;
            let Some(v) = table.get(elem_name.as_str())? else {
                return Ok(());
            };
            decode(v.value())?
        };
        if elem.pattern.is_some() {
            match (elem.field, dr.payload.dt.as_deref()) {
                (4 | 10, Some("n")) => {}
                (11, Some("n" | "h")) => {}
                _ => return Ok(()),
            }
        }
        let Ok(ticker_norm) = crate::protocol::ticker::normalize_ticker(&ticker) else {
            return Ok(());
        };
        let dep = Deployment {
            ticker: ticker_norm,
            deploy_inscription_id: dr.inscription_id.clone(),
            dmt: true,
            element_inscription_id: dr.payload.element_inscription_id.clone(),
            element_field: match elem.field {
                4 => ElementField::BlockHeight,
                10 => ElementField::Nonce,
                _ => ElementField::Bits,
            },
            dt: dr.payload.dt.clone(),
            dim: dr.payload.dim.clone(),
            bits_mode: crate::protocol::bits::BitsMode::RawHex,
            decimals: 0,
            mint_limit: u64::MAX as u128,
            privilege_auth: dr.payload.prv.clone(),
            activation_height: block.height,
            coinbase_activation: if ticker == "dmt-nat" {
                Some(crate::ledger::deploy::NAT_COINBASE_ACTIVATION)
            } else {
                None
            },
            miner_transfer_activation: if ticker == "dmt-nat" {
                Some(crate::ledger::deploy::NAT_MINER_TRANSFER_ACTIVATION)
            } else {
                None
            },
            max_supply: u64::MAX as u128,
            registered_at: occurred_at,
        };
        {
            let mut table = wtx.open_table(DEPLOYMENTS)?;
            table.insert(dep.ticker.as_str(), encode(&dep)?.as_slice())?;
        }
        {
            let mut table = wtx.open_table(tables::DMT_DEPLOY_BY_INSCRIPTION)?;
            table.insert(dr.inscription_id.as_str(), dep.ticker.as_str())?;
        }
        deployments.insert(dep.ticker.as_str().to_string(), dep.clone());
        events.push(self.new_event(
            dep.ticker.as_str(),
            EventFamily::Deploy,
            EventType::DmtDeployRegistered,
            block,
            occurred_at,
            None,
            Some(dr.inscription_id),
            Some(dr.owner),
            None,
            EventDelta::default(),
            serde_json::json!({ "dmt": true, "elem": dep.element_inscription_id }),
            block_hash,
        ));
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn process_dmt_mint(
        &mut self,
        wtx: &redb::WriteTransaction,
        mr: DmtMintReveal,
        block: &BlockView,
        occurred_at: DateTime<Utc>,
        block_hash: &str,
        deployments: &HashMap<String, Deployment>,
        events: &mut Vec<LedgerEvent>,
    ) -> Result<()> {
        let ticker = dmt_effective_ticker(mr.payload.ticker.as_str());
        let Some(dep) = deployments.get(ticker.as_str()) else {
            return Ok(());
        };
        if !dep.dmt || ticker == "dmt-nat" {
            return Ok(());
        }
        if block.height >= TAP_VALUE_STRINGIFY_ACTIVATION_HEIGHT
            && mr.payload.block_raw != mr.payload.block_number.to_string()
        {
            return Ok(());
        }
        if mr.payload.block_number > block.height {
            return Ok(());
        }
        if mr
            .payload
            .deployment_inscription_id
            .as_deref()
            .filter(|d| *d == dep.deploy_inscription_id)
            .is_none()
        {
            return Ok(());
        }
        {
            let table = wtx.open_table(tables::DMT_MINT_BLOCKS)?;
            if table
                .get((ticker.as_str(), mr.payload.block_number))?
                .is_some()
            {
                return Ok(());
            }
        }
        let elem_name = {
            let table = wtx.open_table(tables::DMT_ELEMENT_BY_INSCRIPTION)?;
            let Some(v) = table.get(dep.element_inscription_id.as_str())? else {
                return Ok(());
            };
            v.value().to_string()
        };
        let elem: tables::DmtElementRecord = {
            let table = wtx.open_table(tables::DMT_ELEMENTS)?;
            let Some(v) = table.get(elem_name.as_str())? else {
                return Ok(());
            };
            decode(v.value())?
        };
        let value_str = match elem.field {
            4 => mr.blk_repr.clone(),
            10 => mr.referenced_nonce.to_string(),
            11 if elem.pattern.is_some() && dep.dt.as_deref() == Some("h") => {
                format!("{:x}", mr.referenced_bits)
            }
            11 => mr.referenced_bits.to_string(),
            _ => return Ok(()),
        };
        let mut amount = if let Some(pattern) = elem.pattern.as_deref() {
            let re = match regex::Regex::new(pattern) {
                Ok(re) => re,
                Err(_) => return Ok(()),
            };
            re.find_iter(&value_str).count() as u128
        } else {
            value_str.parse::<u128>().unwrap_or(0)
        };
        if dep.mint_limit > 0 && amount > dep.mint_limit {
            return Ok(());
        }
        let total = read_mint_total(wtx, &ticker)?;
        let remaining = dep.max_supply.saturating_sub(total);
        if amount > remaining {
            amount = remaining;
        }
        if amount == 0 {
            return Ok(());
        }
        if let Some(prv) = dep.privilege_auth.as_deref() {
            let ok = self.verify_privilege_mint(
                wtx,
                prv,
                mr.payload.prv.as_ref(),
                "tap",
                "dmt-mint",
                mr.payload.ticker.as_str(),
                &mr.blk_repr,
                mr.payload.deployment_inscription_id.as_deref(),
                mr.payload.dta.as_deref(),
                &mr.owner,
            )?;
            if !ok {
                return Ok(());
            }
        }
        {
            let mut table = wtx.open_table(tables::DMT_MINT_BLOCKS)?;
            table.insert((ticker.as_str(), mr.payload.block_number), 1u8)?;
        }
        {
            let mut table = wtx.open_table(MINT_CLAIMS)?;
            let v = MintClaim {
                winning_inscription_id: mr.inscription_id.clone(),
                winning_inscription_number: 0,
                inscribed_height: block.height,
                amount,
            };
            table.insert(
                (ticker.as_str(), mr.payload.block_number),
                encode(&v)?.as_slice(),
            )?;
        }
        let a = i128::try_from(amount).unwrap_or(i128::MAX);
        apply_wallet_delta(wtx, &ticker, &mr.owner, a, a, 0, 0, occurred_at)?;
        bump_mint_total(wtx, &ticker, amount)?;
        events.push(self.new_event(
            &ticker,
            EventFamily::Mint,
            EventType::DmtMintCredit,
            block,
            occurred_at,
            None,
            Some(mr.inscription_id),
            Some(mr.owner),
            None,
            EventDelta {
                delta_total: a,
                delta_available: a,
                ..Default::default()
            },
            serde_json::json!({ "blk": mr.payload.block_number, "amount": amount.to_string(), "dmt": true }),
            block_hash,
        ));
        Ok(())
    }

    fn process_privilege_auth_reveal(
        &mut self,
        wtx: &redb::WriteTransaction,
        pr: PendingPrivilegeAuthReveal,
        block: &BlockView,
    ) -> Result<()> {
        let mut table = wtx.open_table(tables::PENDING_PRIVILEGE_AUTHS)?;
        let row = tables::PendingPrivilegeAuth {
            creator: pr.creator,
            inscribed_height: block.height,
            form: pr.form,
            consumed_height: None,
        };
        table.insert(pr.inscription_id.as_str(), encode(&row)?.as_slice())?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn process_send_reveal(
        &mut self,
        wtx: &redb::WriteTransaction,
        sr: PendingSendReveal,
        block: &BlockView,
        occurred_at: DateTime<Utc>,
        block_hash: &str,
        _deployments: &HashMap<String, Deployment>,
        events: &mut Vec<LedgerEvent>,
    ) -> Result<()> {
        if is_dmt_reward_address_at_height(wtx, sr.creator.as_str(), block.height)? {
            return Ok(());
        }
        if block.height >= TAP_VALUE_STRINGIFY_ACTIVATION_HEIGHT
            && sr.payload.items.iter().any(|it| it.amount_was_number)
        {
            return Ok(());
        }
        if sr
            .payload
            .items
            .iter()
            .any(|it| !is_valid_tap_address_at_height(it.recipient.as_str(), block.height))
        {
            return Ok(());
        }
        let mut items = Vec::new();
        for it in sr.payload.items.iter() {
            let ticker = canonical_ticker(it.ticker.as_str());
            items.push(tables::PendingSendItem {
                ticker,
                recipient: it.recipient.as_str().to_string(),
                amount: 0,
                amount_raw: it.amount_raw.clone(),
                amount_was_number: it.amount_was_number,
                dta: it.dta.clone(),
            });
        }
        if items.is_empty() {
            return Ok(());
        }
        {
            let mut table = wtx.open_table(tables::PENDING_SENDS)?;
            let row = tables::PendingSend {
                creator: sr.creator.clone(),
                inscribed_height: block.height,
                items: items.clone(),
                consumed_height: None,
            };
            table.insert(sr.inscription_id.as_str(), encode(&row)?.as_slice())?;
        }
        events.push(self.new_event(
            sr.event_ticker.as_str(),
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
                "item_count": items.len(),
            }),
            block_hash,
        ));
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn process_trade_reveal(
        &mut self,
        wtx: &redb::WriteTransaction,
        tr: PendingTradeReveal,
        block: &BlockView,
        occurred_at: DateTime<Utc>,
        block_hash: &str,
        _deployments: &HashMap<String, Deployment>,
        events: &mut Vec<LedgerEvent>,
    ) -> Result<()> {
        if is_dmt_reward_address_at_height(wtx, tr.creator.as_str(), block.height)? {
            return Ok(());
        }
        let form = match tr.payload {
            TokenTradePayload::Offer {
                ticker,
                amount_raw,
                amount_was_number,
                valid_until,
                accept,
            } => {
                if block.height >= TAP_VALUE_STRINGIFY_ACTIVATION_HEIGHT
                    && (amount_was_number || accept.iter().any(|i| i.amount_was_number))
                {
                    return Ok(());
                }
                let offer_ticker = canonical_ticker(&ticker);
                let mut acc = Vec::new();
                for item in accept {
                    let t = canonical_ticker(&item.ticker);
                    acc.push(tables::PendingTradeAccept {
                        ticker: t,
                        amount_raw: item.amount_raw,
                        amount_was_number: item.amount_was_number,
                    });
                }
                if acc.is_empty() {
                    return Ok(());
                }
                tables::PendingTradeForm::Offer {
                    offer_ticker,
                    offer_amount_raw: amount_raw,
                    offer_amount_was_number: amount_was_number,
                    valid_until,
                    accept: acc,
                }
            }
            TokenTradePayload::Cancel { trade_id } => tables::PendingTradeForm::Cancel { trade_id },
            TokenTradePayload::Fill {
                ticker,
                amount_raw,
                amount_was_number,
                trade_id,
                fee_receiver,
            } => {
                if block.height >= TAP_VALUE_STRINGIFY_ACTIVATION_HEIGHT && amount_was_number {
                    return Ok(());
                }
                let accepted_ticker = canonical_ticker(&ticker);
                tables::PendingTradeForm::Fill {
                    accepted_ticker,
                    accepted_amount_raw: amount_raw,
                    accepted_amount_was_number: amount_was_number,
                    trade_id,
                    fee_receiver: fee_receiver.map(|a| a.as_str().to_string()),
                }
            }
        };
        {
            let mut table = wtx.open_table(tables::PENDING_TRADES)?;
            let row = tables::PendingTrade {
                creator: tr.creator.clone(),
                inscribed_height: block.height,
                form: form.clone(),
                consumed_height: None,
            };
            table.insert(tr.inscription_id.as_str(), encode(&row)?.as_slice())?;
        }
        let ticker = match &form {
            tables::PendingTradeForm::Offer { offer_ticker, .. } => offer_ticker.as_str(),
            tables::PendingTradeForm::Fill {
                accepted_ticker, ..
            } => accepted_ticker.as_str(),
            tables::PendingTradeForm::Cancel { .. } => "dmt-nat",
        };
        events.push(self.new_event(
            ticker,
            EventFamily::Transfer,
            EventType::TokenSendInscribeAdmitted,
            block,
            occurred_at,
            Some(tr.tx_index),
            Some(tr.inscription_id),
            Some(tr.creator),
            None,
            EventDelta::default(),
            serde_json::json!({ "op": "token-trade" }),
            block_hash,
        ));
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn process_auth_reveal(
        &mut self,
        wtx: &redb::WriteTransaction,
        ar: PendingAuthReveal,
        block: &BlockView,
        occurred_at: DateTime<Utc>,
        block_hash: &str,
        _deployments: &HashMap<String, Deployment>,
        events: &mut Vec<LedgerEvent>,
    ) -> Result<()> {
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
            "dmt-nat",
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
            block_hash,
        ));
        Ok(())
    }

    fn process_control_reveal(
        &mut self,
        wtx: &redb::WriteTransaction,
        c: ControlInscribe,
        block: &BlockView,
        occurred_at: DateTime<Utc>,
        block_hash: &str,
        events: &mut Vec<LedgerEvent>,
    ) -> Result<()> {
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
                consumed_height: None,
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
            block_hash,
        ));
        Ok(())
    }

    fn settle_privilege_auth(
        &mut self,
        wtx: &redb::WriteTransaction,
        inscription_id: &str,
        new_owner: Option<&str>,
        block: &BlockView,
    ) -> Result<()> {
        let pending: Option<tables::PendingPrivilegeAuth> = {
            let table = wtx.open_table(tables::PENDING_PRIVILEGE_AUTHS)?;
            let out = table
                .get(inscription_id)?
                .and_then(|v| decode::<tables::PendingPrivilegeAuth>(v.value()).ok());
            out
        };
        let Some(pending) = pending else {
            return Ok(());
        };
        if pending.consumed_height.is_some() {
            return Ok(());
        }
        let Some(owner) = new_owner else {
            let mut table = wtx.open_table(tables::PENDING_PRIVILEGE_AUTHS)?;
            let mut row = pending;
            row.consumed_height = Some(block.height);
            table.insert(inscription_id, encode(&row)?.as_slice())?;
            return Ok(());
        };
        if pending.creator != owner {
            let mut table = wtx.open_table(tables::PENDING_PRIVILEGE_AUTHS)?;
            let mut row = pending;
            row.consumed_height = Some(block.height);
            table.insert(inscription_id, encode(&row)?.as_slice())?;
            return Ok(());
        }
        let execution_result = (|| -> Result<()> {
            match pending.form.clone() {
                tables::PendingPrivilegeAuthForm::Cancel { cancel_id } => {
                    let active_owner = {
                        let table = wtx.open_table(tables::PRIVILEGE_AUTH_RECORDS)?;
                        let out = table
                            .get(cancel_id.as_str())?
                            .and_then(|v| decode::<tables::PrivilegeAuthRecord>(v.value()).ok())
                            .map(|r| r.authority_addr);
                        out
                    };
                    if active_owner.as_deref() == Some(owner) {
                        let mut table = wtx.open_table(tables::PRIVILEGE_AUTH_CANCELS)?;
                        table.insert(cancel_id.as_str(), 1u8)?;
                    }
                }
                tables::PendingPrivilegeAuthForm::Create {
                    auth_json,
                    sig_v,
                    sig_r,
                    sig_s,
                    hash_hex,
                    salt,
                } => {
                    let auth_bytes = serde_json::to_vec(&auth_json).unwrap_or_default();
                    let msg_hash =
                        crate::crypto::ecdsa_recover::compute_msg_hash(&auth_bytes, &salt);
                    let Ok((compact, pubkey_hex)) =
                        crate::crypto::ecdsa_recover::verify_ecdsa_recover(
                            &sig_v, &sig_r, &sig_s, &hash_hex, &msg_hash,
                        )
                    else {
                        return Ok(());
                    };
                    {
                        let table = wtx.open_table(tables::PRIVILEGE_AUTH_SIG_REPLAY)?;
                        if table.get(compact.as_str())?.is_some() {
                            return Ok(());
                        }
                    }
                    {
                        let mut table = wtx.open_table(tables::PRIVILEGE_AUTH_RECORDS)?;
                        let rec = tables::PrivilegeAuthRecord {
                            inscription_id: inscription_id.to_string(),
                            authority_addr: owner.to_string(),
                            auth_json,
                            sig_v,
                            sig_r,
                            sig_s,
                            hash_hex,
                            salt,
                            authority_pubkey_hex: pubkey_hex,
                            created_height: block.height,
                        };
                        table.insert(inscription_id, encode(&rec)?.as_slice())?;
                    }
                    {
                        let mut table = wtx.open_table(tables::PRIVILEGE_AUTH_SIG_REPLAY)?;
                        table.insert(compact.as_str(), 1u8)?;
                    }
                }
            }
            Ok(())
        })();
        execution_result?;
        {
            let mut table = wtx.open_table(tables::PENDING_PRIVILEGE_AUTHS)?;
            let mut row = pending;
            row.consumed_height = Some(block.height);
            table.insert(inscription_id, encode(&row)?.as_slice())?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn verify_privilege_mint(
        &mut self,
        wtx: &redb::WriteTransaction,
        authority_id: &str,
        prv_obj: Option<&serde_json::Value>,
        p: &str,
        op: &str,
        tick: &str,
        amt_or_blk: &str,
        dep: Option<&str>,
        dta: Option<&str>,
        owner: &str,
    ) -> Result<bool> {
        if !is_privilege_auth_active(wtx, authority_id)? {
            return Ok(false);
        }
        let Some(prv_obj) = prv_obj else {
            return Ok(false);
        };
        let Some(sig) = prv_obj.get("sig") else {
            return Ok(false);
        };
        let sig_v = json_string_field(sig, "v").unwrap_or_default();
        let sig_r = json_string_field(sig, "r").unwrap_or_default();
        let sig_s = json_string_field(sig, "s").unwrap_or_default();
        let hash_hex = prv_obj.get("hash").and_then(|v| v.as_str()).unwrap_or("");
        let salt = prv_obj.get("salt").and_then(|v| v.as_str()).unwrap_or("");
        let address_for_msg = prv_obj
            .get("address")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let mut msg = if let Some(dep) = dep {
            format!("{p}-{op}-{tick}-{amt_or_blk}-{dep}-{address_for_msg}")
        } else {
            format!("{p}-{op}-{tick}-{amt_or_blk}-{address_for_msg}")
        };
        if let Some(dta) = dta {
            msg.push('-');
            msg.push_str(dta);
        }
        msg.push('-');
        msg.push_str(salt);
        let msg_hash = sha256_bytes(msg.as_bytes());
        let Ok((compact, pubkey_hex)) = crate::crypto::ecdsa_recover::verify_ecdsa_recover(
            &sig_v, &sig_r, &sig_s, hash_hex, &msg_hash,
        ) else {
            return Ok(false);
        };
        {
            let table = wtx.open_table(tables::PRIVILEGE_AUTH_SIG_REPLAY)?;
            if table.get(compact.as_str())?.is_some() {
                return Ok(false);
            }
        }
        let rec: Option<tables::PrivilegeAuthRecord> = {
            let table = wtx.open_table(tables::PRIVILEGE_AUTH_RECORDS)?;
            let out = table
                .get(authority_id)?
                .and_then(|v| decode::<tables::PrivilegeAuthRecord>(v.value()).ok());
            out
        };
        let Some(rec) = rec else {
            return Ok(false);
        };
        if rec.authority_pubkey_hex != pubkey_hex || address_for_msg != owner {
            return Ok(false);
        }
        {
            let mut table = wtx.open_table(tables::PRIVILEGE_AUTH_SIG_REPLAY)?;
            table.insert(compact.as_str(), 1u8)?;
        }
        Ok(true)
    }

    fn snapshot_transfer_create_balance_from(
        &self,
        wtx: &redb::WriteTransaction,
        candidates: &[TransferInscribeCandidate],
    ) -> Result<HashMap<(String, String), TransferBalanceSnapshot>> {
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
            let snapshot = state
                .map(|s| TransferBalanceSnapshot {
                    total: s.total,
                    transferable: s.transferable,
                })
                .unwrap_or_default();
            out.insert(key, snapshot);
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

    fn mark_tracker_dirty(&mut self, outpoint: OutPoint) {
        self.tracker_dirty_keys
            .insert(format!("{}:{}", outpoint.txid, outpoint.vout));
    }

    fn mark_tracker_move_dirty(&mut self, mv: &TrackerMove) {
        match mv {
            TrackerMove::Moved { from, to, .. } => {
                self.mark_tracker_dirty(*from);
                self.mark_tracker_dirty(*to);
            }
            TrackerMove::Burned { from, .. } => self.mark_tracker_dirty(*from),
        }
    }

    fn save_dirty_carriers(
        &self,
        wtx: &redb::WriteTransaction,
        tracker: &InscriptionTracker,
    ) -> Result<()> {
        if self.tracker_dirty_keys.is_empty() {
            return Ok(());
        }
        let mut table = wtx.open_multimap_table(INSCRIPTION_OWNERS)?;

        // Multimap cannot replace one key's values in-place. For each
        // changed outpoint, clear that one key and write its current
        // carrier set. Untouched outpoints stay on disk unchanged.
        for key in &self.tracker_dirty_keys {
            table.remove_all(key.as_str())?;
            let Some(op) = parse_outpoint(key) else {
                continue;
            };
            let carriers = tracker.get(&op);
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
                        InscriptionKind::TokenTrade => "token-trade".to_string(),
                        InscriptionKind::PrivilegeAuth => "privilege-auth".to_string(),
                    },
                    sequence_rank: carrier.inscription.sequence_rank,
                    current_outpoint: key.clone(),
                    offset_in_outpoint: carrier.offset_in_outpoint,
                    outpoint_value_sats: carrier.outpoint_value_sats,
                };
                table.insert(key.as_str(), encode(&v)?.as_slice())?;
            }
        }
        Ok(())
    }

    async fn resolve_effective_delegate_envelope(
        &self,
        env: &Envelope,
        cache: &mut HashMap<String, Option<Envelope>>,
    ) -> Result<Option<Envelope>> {
        let Some(delegate_id) = env.delegate.as_deref() else {
            return Ok(Some(env.clone()));
        };
        if let Some(cached) = cache.get(delegate_id) {
            return Ok(cached.clone());
        }
        let Some((txid, index)) = parse_inscription_id(delegate_id) else {
            return Ok(Some(env.clone()));
        };
        let tx = match self.rpc.get_raw_transaction(txid).await {
            Ok(tx) => tx,
            Err(e) => {
                tracing::warn!(
                    delegate_id,
                    error = %e,
                    "delegate lookup failed; falling back to reveal payload"
                );
                return Ok(Some(env.clone()));
            }
        };
        let delegate = crate::inscription::envelope::parse_envelopes_at_height(&tx, u64::MAX)
            .into_iter()
            .find(|candidate| {
                matches!(
                    candidate.kind,
                    crate::inscription::envelope::EnvelopeKind::Inscription { index: i }
                        if i == index
                )
            });
        let Some(delegate) = delegate else {
            return Ok(Some(env.clone()));
        };
        if delegate.delegate.is_some() {
            cache.insert(delegate_id.to_string(), None);
            return Ok(None);
        }
        cache.insert(delegate_id.to_string(), Some(delegate.clone()));
        Ok(Some(delegate))
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
        for (tx_index, mv) in moves {
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
                            tx_index,
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
                            tx_index,
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
                            tx_index,
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
                            tx_index,
                            block_hash,
                            events,
                        )?;
                    }
                    InscriptionKind::TokenTrade => {
                        self.settle_token_trade(
                            wtx,
                            &inscription.inscription_id,
                            new_owner_address.as_deref(),
                            block,
                            occurred_at,
                            tx_index,
                            block_hash,
                            events,
                        )?;
                    }
                    InscriptionKind::PrivilegeAuth => {
                        self.settle_privilege_auth(
                            wtx,
                            &inscription.inscription_id,
                            new_owner_address.as_deref(),
                            block,
                        )?;
                    }
                },
                TrackerMove::Burned {
                    inscription,
                    reason,
                    ..
                } => {
                    if matches!(inscription.kind, InscriptionKind::TokenTransfer) {
                        if matches!(reason, BurnReason::OpReturn | BurnReason::Unaddressable) {
                            self.settle_transfer(
                                wtx,
                                &inscription.ticker,
                                &inscription.inscription_id,
                                Some("-"),
                                block,
                                occurred_at,
                                tx_index,
                                block_hash,
                                events,
                            )?;
                        } else {
                            self.burn_transfer(
                                wtx,
                                &inscription.ticker,
                                &inscription.inscription_id,
                                block,
                                occurred_at,
                                tx_index,
                                block_hash,
                                events,
                            )?;
                        }
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
        tx_index: u32,
        block_hash: &str,
        events: &mut Vec<LedgerEvent>,
    ) -> Result<()> {
        let valid = {
            let table = wtx.open_table(VALID_TRANSFERS)?;
            let got = table
                .get(inscription_id)?
                .and_then(|v| decode::<ValidTransfer>(v.value()).ok());
            got
        };
        let Some(mut valid) = valid else {
            events.push(self.new_event(
                ticker,
                EventFamily::Transfer,
                EventType::TokenTransferSkippedSemantic,
                block,
                occurred_at,
                Some(tx_index),
                Some(inscription_id.to_string()),
                new_owner.map(str::to_string),
                None,
                EventDelta::default(),
                serde_json::json!({ "reason": "no_valid_transfer_row" }),
                block_hash,
            ));
            return Ok(());
        };
        if valid.consumed_height.is_some() {
            events.push(self.new_event(
                ticker,
                EventFamily::Transfer,
                EventType::TokenTransferSkippedSemantic,
                block,
                occurred_at,
                Some(tx_index),
                Some(inscription_id.to_string()),
                new_owner.map(str::to_string),
                None,
                EventDelta::default(),
                serde_json::json!({ "reason": "already_consumed" }),
                block_hash,
            ));
            return Ok(());
        }
        let sender = valid.sender.clone();
        let amount = valid.amount;
        let a = i128::try_from(amount).unwrap_or(i128::MAX);

        let shield_blocks =
            dmt_reward_transfer_execution_blocks(wtx, ticker, &sender, block.height)?;
        if shield_blocks && new_owner == Some(sender.as_str()) {
            return Ok(());
        }

        valid.consumed_height = Some(block.height);
        {
            let mut table = wtx.open_table(VALID_TRANSFERS)?;
            table.insert(inscription_id, encode(&valid)?.as_slice())?;
        }
        // Remove from the sender's open-transferables index; this
        // inscription has settled.
        {
            let mut idx = wtx.open_table(TRANSFERABLES_BY_SENDER)?;
            idx.remove((ticker, sender.as_str(), inscription_id))?;
        }

        // Miner-reward transfer-execution shield (height >= 942,002):
        // ord-tap voids only while the DMT-reward wallet is still
        // blocked for this ticker. A permanent reward marker alone is
        // not enough after an explicit unblock-transferables op.
        if shield_blocks {
            let state = wallet_state_snapshot(wtx, ticker, &sender)?;
            let new_transferable = state.transferable.saturating_sub(a).max(0);
            let delta_transferable = new_transferable - state.transferable;
            let delta_available = -delta_transferable;
            if delta_available != 0 || delta_transferable != 0 {
                apply_wallet_delta(
                    wtx,
                    ticker,
                    &sender,
                    0,
                    delta_available,
                    delta_transferable,
                    0,
                    occurred_at,
                )?;
            }
            events.push(self.new_event(
                ticker,
                EventFamily::Transfer,
                EventType::TokenTransferShieldVoided,
                block,
                occurred_at,
                Some(tx_index),
                Some(inscription_id.to_string()),
                Some(sender),
                new_owner.map(str::to_string),
                EventDelta {
                    delta_available,
                    delta_transferable,
                    ..Default::default()
                },
                serde_json::json!({ "amount": amount.to_string(), "reason": "dmt_reward_execution_shield" }),
                block_hash,
            ));
            return Ok(());
        }

        let state = wallet_state_snapshot(wtx, ticker, &sender)?;
        let new_total = state.total.saturating_sub(a);
        let new_transferable = state.transferable.saturating_sub(a);
        if new_total < 0 || new_transferable < 0 {
            let clamped_transferable = new_transferable.max(0);
            let delta_transferable = clamped_transferable - state.transferable;
            let delta_available = -delta_transferable;
            if delta_available != 0 || delta_transferable != 0 {
                apply_wallet_delta(
                    wtx,
                    ticker,
                    &sender,
                    0,
                    delta_available,
                    delta_transferable,
                    0,
                    occurred_at,
                )?;
            }
            events.push(self.new_event(
                ticker,
                EventFamily::Transfer,
                EventType::TokenTransferSkippedSemantic,
                block,
                occurred_at,
                Some(tx_index),
                Some(inscription_id.to_string()),
                Some(sender),
                new_owner.map(str::to_string),
                EventDelta {
                    delta_available,
                    delta_transferable,
                    ..Default::default()
                },
                serde_json::json!({
                    "amount": amount.to_string(),
                    "reason": "execution_revalidation_failed",
                    "total": state.total.to_string(),
                    "transferable": state.transferable.to_string(),
                }),
                block_hash,
            ));
            return Ok(());
        }

        let receiver_is_sender = new_owner == Some(sender.as_str());
        let sender_delta_total = if receiver_is_sender { 0 } else { -a };
        let sender_delta_available = if receiver_is_sender { a } else { 0 };
        apply_wallet_delta(
            wtx,
            ticker,
            &sender,
            sender_delta_total,
            sender_delta_available,
            -a,
            0,
            occurred_at,
        )?;
        events.push(self.new_event(
            ticker,
            EventFamily::Transfer,
            EventType::TokenTransferDebit,
            block,
            occurred_at,
            Some(tx_index),
            Some(inscription_id.to_string()),
            Some(sender.clone()),
            new_owner.map(str::to_string),
            EventDelta {
                delta_total: sender_delta_total,
                delta_available: sender_delta_available,
                delta_transferable: -a,
                ..Default::default()
            },
            serde_json::json!({ "amount": amount.to_string() }),
            block_hash,
        ));
        // Credit recipient
        if let Some(owner) = new_owner.filter(|owner| *owner != sender.as_str()) {
            apply_wallet_delta(wtx, ticker, owner, a, a, 0, 0, occurred_at)?;
            events.push(self.new_event(
                ticker,
                EventFamily::Transfer,
                EventType::TokenTransferCredit,
                block,
                occurred_at,
                Some(tx_index),
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
        tx_index: u32,
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
        let state = wallet_state_snapshot(wtx, ticker, &sender)?;
        let new_total = state.total.saturating_sub(a);
        let new_transferable = state.transferable.saturating_sub(a);
        if new_total < 0 || new_transferable < 0 {
            let clamped_transferable = new_transferable.max(0);
            let delta_transferable = clamped_transferable - state.transferable;
            let delta_available = -delta_transferable;
            if delta_available != 0 || delta_transferable != 0 {
                apply_wallet_delta(
                    wtx,
                    ticker,
                    &sender,
                    0,
                    delta_available,
                    delta_transferable,
                    0,
                    occurred_at,
                )?;
            }
            events.push(self.new_event(
                ticker,
                EventFamily::Transfer,
                EventType::TokenTransferSkippedSemantic,
                block,
                occurred_at,
                Some(tx_index),
                Some(inscription_id.to_string()),
                Some(sender),
                None,
                EventDelta {
                    delta_available,
                    delta_transferable,
                    ..Default::default()
                },
                serde_json::json!({
                    "amount": amount.to_string(),
                    "reason": "burn_revalidation_failed",
                    "total": state.total.to_string(),
                    "transferable": state.transferable.to_string(),
                }),
                block_hash,
            ));
            return Ok(());
        }
        apply_wallet_delta(wtx, ticker, &sender, -a, 0, -a, a, occurred_at)?;
        events.push(self.new_event(
            ticker,
            EventFamily::Transfer,
            EventType::TokenTransferBurned,
            block,
            occurred_at,
            Some(tx_index),
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
        tx_index: u32,
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
        if p.consumed_height.is_some() {
            return Ok(());
        }
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
                Some(tx_index),
                Some(inscription_id.to_string()),
                Some(p.address.clone()),
                None,
                EventDelta::default(),
                serde_json::json!({ "op": p.op }),
                block_hash,
            ));
        }
        let mut table = wtx.open_table(PENDING_CONTROLS)?;
        let mut row = p;
        row.consumed_height = Some(block.height);
        table.insert(inscription_id, encode(&row)?.as_slice())?;
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
        tx_index: u32,
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
            // ord-tap deletes accumulator state on the first movement even
            // when the output owner does not match the creator.
            ps.consumed_height = Some(block.height);
            let mut table = wtx.open_table(tables::PENDING_SENDS)?;
            table.insert(inscription_id, encode(&ps)?.as_slice())?;
            return Ok(());
        }
        // Miner-reward shield at move time too — ord-tap /tmp/ot_send.rs:90-91.
        if is_dmt_reward_address_at_height(wtx, &ps.creator, block.height)? {
            ps.consumed_height = Some(block.height);
            let mut table = wtx.open_table(tables::PENDING_SENDS)?;
            table.insert(inscription_id, encode(&ps)?.as_slice())?;
            return Ok(());
        }
        use crate::ledger::send::{execute_send_item, SendItemOutcome};
        for item in &ps.items {
            if block.height >= TAP_VALUE_STRINGIFY_ACTIVATION_HEIGHT && item.amount_was_number {
                continue;
            }
            let Some(amount) = (if item.amount_raw.is_empty() {
                if item.amount == 0 {
                    None
                } else {
                    Some(item.amount)
                }
            } else {
                parse_deployed_amount(wtx, &item.ticker, &item.amount_raw)?
            }) else {
                continue;
            };
            let outcome = execute_send_item(
                wtx,
                &item.ticker,
                &ps.creator,
                &item.recipient,
                amount,
                occurred_at,
            )?;
            let a = i128::try_from(amount).unwrap_or(i128::MAX);
            match outcome {
                SendItemOutcome::Sent => {
                    events.push(self.new_event(
                        &item.ticker,
                        EventFamily::Send,
                        EventType::TokenSendDebit,
                        block,
                        occurred_at,
                        Some(tx_index),
                        Some(inscription_id.to_string()),
                        Some(ps.creator.clone()),
                        Some(item.recipient.clone()),
                        EventDelta {
                            delta_total: -a,
                            delta_available: -a,
                            ..Default::default()
                        },
                        serde_json::json!({ "amount": amount.to_string() }),
                        block_hash,
                    ));
                    events.push(self.new_event(
                        &item.ticker,
                        EventFamily::Send,
                        EventType::TokenSendCredit,
                        block,
                        occurred_at,
                        Some(tx_index),
                        Some(inscription_id.to_string()),
                        Some(item.recipient.clone()),
                        Some(ps.creator.clone()),
                        EventDelta {
                            delta_total: a,
                            delta_available: a,
                            ..Default::default()
                        },
                        serde_json::json!({ "amount": amount.to_string() }),
                        block_hash,
                    ));
                }
                SendItemOutcome::SelfSend => {
                    // ord-tap's internal send helper returns before
                    // writing transfer logs for self-sends.
                }
                SendItemOutcome::Skipped => {
                    events.push(self.new_event(
                        &item.ticker,
                        EventFamily::Send,
                        EventType::TokenSendSkipped,
                        block,
                        occurred_at,
                        Some(tx_index),
                        Some(inscription_id.to_string()),
                        Some(ps.creator.clone()),
                        Some(item.recipient.clone()),
                        EventDelta::default(),
                        serde_json::json!({
                            "amount": amount.to_string(),
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
        tx_index: u32,
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
            // ord-tap deletes accumulator state on the first movement even
            // when the output owner does not match the creator.
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
                    "dmt-nat",
                    EventFamily::Auth,
                    EventType::TokenAuthCancelTapped,
                    block,
                    occurred_at,
                    Some(tx_index),
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
                                "dmt-nat",
                                EventFamily::Auth,
                                EventType::TokenAuthCreateRejected,
                                block,
                                occurred_at,
                                Some(tx_index),
                                Some(inscription_id.to_string()),
                                Some(pa.creator.clone()),
                                None,
                                EventDelta::default(),
                                serde_json::json!({ "reason": "sig_replayed" }),
                                block_hash,
                            ));
                        } else {
                            for ticker in auth_tickers {
                                let table = wtx.open_table(DEPLOYMENTS)?;
                                if table.get(ticker.as_str())?.is_none() {
                                    pa.consumed_height = Some(block.height);
                                    let mut pending_table =
                                        wtx.open_table(tables::PENDING_AUTHS)?;
                                    pending_table
                                        .insert(inscription_id, encode(&pa)?.as_slice())?;
                                    return Ok(());
                                }
                            }
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
                                "dmt-nat",
                                EventFamily::Auth,
                                EventType::TokenAuthCreateRegistered,
                                block,
                                occurred_at,
                                Some(tx_index),
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
                            "dmt-nat",
                            EventFamily::Auth,
                            EventType::TokenAuthCreateRejected,
                            block,
                            occurred_at,
                            Some(tx_index),
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

    #[allow(clippy::too_many_arguments)]
    fn settle_token_trade(
        &mut self,
        wtx: &redb::WriteTransaction,
        inscription_id: &str,
        new_owner: Option<&str>,
        block: &BlockView,
        occurred_at: DateTime<Utc>,
        tx_index: u32,
        block_hash: &str,
        events: &mut Vec<LedgerEvent>,
    ) -> Result<()> {
        let pending: Option<tables::PendingTrade> = {
            let table = wtx.open_table(tables::PENDING_TRADES)?;
            let raw = table.get(inscription_id)?;
            raw.and_then(|v| decode::<tables::PendingTrade>(v.value()).ok())
        };
        let Some(mut pt) = pending else {
            return Ok(());
        };
        if pt.consumed_height.is_some() {
            return Ok(());
        }
        let self_tap = matches!(new_owner, Some(o) if o == pt.creator);
        if !self_tap {
            // ord-tap deletes accumulator state on the first movement even
            // when the output owner does not match the creator.
            pt.consumed_height = Some(block.height);
            let mut table = wtx.open_table(tables::PENDING_TRADES)?;
            table.insert(inscription_id, encode(&pt)?.as_slice())?;
            return Ok(());
        }
        if is_dmt_reward_address_at_height(wtx, &pt.creator, block.height)? {
            pt.consumed_height = Some(block.height);
            let mut table = wtx.open_table(tables::PENDING_TRADES)?;
            table.insert(inscription_id, encode(&pt)?.as_slice())?;
            return Ok(());
        }

        let execution_result = (|| -> Result<()> {
            match &pt.form {
                tables::PendingTradeForm::Cancel { trade_id } => {
                    let lock: Option<tables::TradeLock> = {
                        let table = wtx.open_table(tables::TRADE_LOCKS)?;
                        let out = table
                            .get(trade_id.as_str())?
                            .and_then(|v| decode(v.value()).ok());
                        out
                    };
                    if lock.as_ref().map(|l| l.creator.as_str()) == Some(pt.creator.as_str()) {
                        let mut locks = wtx.open_table(tables::TRADE_LOCKS)?;
                        locks.remove(trade_id.as_str())?;
                        drop(locks);
                        remove_trade_offers(wtx, trade_id)?;
                    }
                }
                tables::PendingTradeForm::Offer {
                    offer_ticker,
                    offer_amount_raw,
                    valid_until,
                    accept,
                    ..
                } => {
                    let Some(offer_amount) =
                        parse_deployed_amount(wtx, offer_ticker, offer_amount_raw)?
                    else {
                        return Ok(());
                    };
                    if *valid_until < 0 || block.height as i64 > *valid_until {
                        return Ok(());
                    }
                    if wallet_available(wtx, offer_ticker, &pt.creator)?
                        < i128::try_from(offer_amount).unwrap_or(i128::MAX)
                    {
                        return Ok(());
                    }
                    {
                        let mut locks = wtx.open_table(tables::TRADE_LOCKS)?;
                        if locks.get(inscription_id)?.is_none() {
                            let lock = tables::TradeLock {
                                creator: pt.creator.clone(),
                                offer_ticker: offer_ticker.clone(),
                                offer_amount,
                                valid_until: *valid_until,
                                opened_height: block.height,
                            };
                            locks.insert(inscription_id, encode(&lock)?.as_slice())?;
                        }
                    }
                    {
                        let mut offers = wtx.open_table(tables::TRADE_OFFERS)?;
                        for item in accept {
                            let Some(accepted_amount) =
                                parse_deployed_amount(wtx, &item.ticker, &item.amount_raw)?
                            else {
                                continue;
                            };
                            let offer = tables::TradeOffer {
                                seller: pt.creator.clone(),
                                offer_ticker: offer_ticker.clone(),
                                offer_amount,
                                accepted_ticker: item.ticker.clone(),
                                accepted_amount,
                                valid_until: *valid_until,
                                opened_height: block.height,
                            };
                            offers.insert(
                                (inscription_id, item.ticker.as_str()),
                                encode(&offer)?.as_slice(),
                            )?;
                        }
                    }
                }
                tables::PendingTradeForm::Fill {
                    accepted_ticker,
                    accepted_amount_raw,
                    trade_id,
                    fee_receiver,
                    ..
                } => {
                    let Some(accepted_amount) =
                        parse_deployed_amount(wtx, accepted_ticker, accepted_amount_raw)?
                    else {
                        return Ok(());
                    };
                    let lock: Option<tables::TradeLock> = {
                        let table = wtx.open_table(tables::TRADE_LOCKS)?;
                        let out = table
                            .get(trade_id.as_str())?
                            .and_then(|v| decode(v.value()).ok());
                        out
                    };
                    if lock.is_none() {
                        return Ok(());
                    }
                    let offer: Option<tables::TradeOffer> = {
                        let table = wtx.open_table(tables::TRADE_OFFERS)?;
                        let out = table
                            .get((trade_id.as_str(), accepted_ticker.as_str()))?
                            .and_then(|v| decode(v.value()).ok());
                        out
                    };
                    let Some(offer) = offer else {
                        return Ok(());
                    };
                    if offer.seller == pt.creator
                        || is_dmt_reward_address_at_height(wtx, &offer.seller, block.height)?
                    {
                        return Ok(());
                    }
                    if accepted_amount != offer.accepted_amount {
                        return Ok(());
                    }
                    if block.height as i64 > offer.valid_until {
                        return Ok(());
                    }
                    let fee = fee_receiver
                        .as_ref()
                        .map(|_| trade_fee(accepted_amount))
                        .unwrap_or(0);
                    let offered_i = i128::try_from(offer.offer_amount).unwrap_or(i128::MAX);
                    let accepted_i = i128::try_from(accepted_amount).unwrap_or(i128::MAX);
                    let fee_i = i128::try_from(fee).unwrap_or(i128::MAX);
                    if wallet_available(wtx, &offer.offer_ticker, &offer.seller)? < offered_i {
                        return Ok(());
                    }
                    if wallet_available(wtx, accepted_ticker, &pt.creator)?
                        < accepted_i.saturating_add(fee_i)
                    {
                        return Ok(());
                    }

                    reconcile_wallet_available(
                        wtx,
                        &offer.offer_ticker,
                        &offer.seller,
                        occurred_at,
                    )?;
                    reconcile_wallet_available(wtx, accepted_ticker, &pt.creator, occurred_at)?;
                    apply_wallet_delta(
                        wtx,
                        &offer.offer_ticker,
                        &offer.seller,
                        -offered_i,
                        -offered_i,
                        0,
                        0,
                        occurred_at,
                    )?;
                    apply_wallet_delta(
                        wtx,
                        &offer.offer_ticker,
                        &pt.creator,
                        offered_i,
                        offered_i,
                        0,
                        0,
                        occurred_at,
                    )?;
                    apply_wallet_delta(
                        wtx,
                        accepted_ticker,
                        &pt.creator,
                        -(accepted_i + fee_i),
                        -(accepted_i + fee_i),
                        0,
                        0,
                        occurred_at,
                    )?;
                    apply_wallet_delta(
                        wtx,
                        accepted_ticker,
                        &offer.seller,
                        accepted_i,
                        accepted_i,
                        0,
                        0,
                        occurred_at,
                    )?;
                    if let Some(rcv) = fee_receiver {
                        if fee_i > 0 {
                            apply_wallet_delta(
                                wtx,
                                accepted_ticker,
                                rcv,
                                fee_i,
                                fee_i,
                                0,
                                0,
                                occurred_at,
                            )?;
                        }
                    }

                    events.push(self.new_event(
                    &offer.offer_ticker,
                    EventFamily::Send,
                    EventType::TokenSendDebit,
                    block,
                    occurred_at,
                    Some(tx_index),
                    Some(inscription_id.to_string()),
                    Some(offer.seller.clone()),
                    Some(pt.creator.clone()),
                    EventDelta {
                        delta_total: -offered_i,
                        delta_available: -offered_i,
                        ..Default::default()
                    },
                    serde_json::json!({ "amount": offer.offer_amount.to_string(), "trade": trade_id }),
                    block_hash,
                ));
                    events.push(self.new_event(
                    &offer.offer_ticker,
                    EventFamily::Send,
                    EventType::TokenSendCredit,
                    block,
                    occurred_at,
                    Some(tx_index),
                    Some(inscription_id.to_string()),
                    Some(pt.creator.clone()),
                    Some(offer.seller.clone()),
                    EventDelta {
                        delta_total: offered_i,
                        delta_available: offered_i,
                        ..Default::default()
                    },
                    serde_json::json!({ "amount": offer.offer_amount.to_string(), "trade": trade_id }),
                    block_hash,
                ));
                    events.push(self.new_event(
                    accepted_ticker,
                    EventFamily::Send,
                    EventType::TokenSendDebit,
                    block,
                    occurred_at,
                    Some(tx_index),
                    Some(inscription_id.to_string()),
                    Some(pt.creator.clone()),
                    Some(offer.seller.clone()),
                    EventDelta {
                        delta_total: -(accepted_i + fee_i),
                        delta_available: -(accepted_i + fee_i),
                        ..Default::default()
                    },
                    serde_json::json!({ "amount": accepted_amount.to_string(), "fee": fee.to_string(), "trade": trade_id }),
                    block_hash,
                ));
                    events.push(self.new_event(
                    accepted_ticker,
                    EventFamily::Send,
                    EventType::TokenSendCredit,
                    block,
                    occurred_at,
                    Some(tx_index),
                    Some(inscription_id.to_string()),
                    Some(offer.seller.clone()),
                    Some(pt.creator.clone()),
                    EventDelta {
                        delta_total: accepted_i,
                        delta_available: accepted_i,
                        ..Default::default()
                    },
                    serde_json::json!({ "amount": accepted_amount.to_string(), "trade": trade_id }),
                    block_hash,
                ));
                    if let Some(rcv) = fee_receiver {
                        if fee_i > 0 {
                            events.push(self.new_event(
                            accepted_ticker,
                            EventFamily::Send,
                            EventType::TokenSendCredit,
                            block,
                            occurred_at,
                            Some(tx_index),
                            Some(inscription_id.to_string()),
                            Some(rcv.clone()),
                            Some(pt.creator.clone()),
                            EventDelta {
                                delta_total: fee_i,
                                delta_available: fee_i,
                                ..Default::default()
                            },
                            serde_json::json!({ "amount": fee.to_string(), "fee": true, "trade": trade_id }),
                            block_hash,
                        ));
                        }
                    }

                    {
                        let mut locks = wtx.open_table(tables::TRADE_LOCKS)?;
                        locks.remove(trade_id.as_str())?;
                    }
                    remove_trade_offers(wtx, trade_id)?;
                }
            }
            Ok(())
        })();
        execution_result?;

        pt.consumed_height = Some(block.height);
        {
            let mut table = wtx.open_table(tables::PENDING_TRADES)?;
            table.insert(inscription_id, encode(&pt)?.as_slice())?;
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
        if block.height >= TAP_VALUE_STRINGIFY_ACTIVATION_HEIGHT
            && p.items.iter().any(|it| it.amount_was_number)
        {
            events.push(self.new_event(
                "dmt-nat",
                EventFamily::Auth,
                EventType::TokenAuthRedeemRejected,
                block,
                occurred_at,
                Some(rr.tx_index),
                Some(rr.inscription_id.clone()),
                Some(rr.inscriber_addr.clone()),
                None,
                EventDelta::default(),
                serde_json::json!({ "reason": "numeric_amount_after_value_stringify" }),
                block_hash,
            ));
            return Ok(());
        }
        if p.items
            .iter()
            .any(|it| !is_valid_tap_address_at_height(&it.address_raw, block.height))
        {
            events.push(self.new_event(
                "dmt-nat",
                EventFamily::Auth,
                EventType::TokenAuthRedeemRejected,
                block,
                occurred_at,
                Some(rr.tx_index),
                Some(rr.inscription_id.clone()),
                Some(rr.inscriber_addr.clone()),
                None,
                EventDelta::default(),
                serde_json::json!({ "reason": "invalid_recipient_address" }),
                block_hash,
            ));
            return Ok(());
        }
        // 1. Verify ECDSA over sha256(serde_json(redeem_subtree) || salt).
        let redeem_json = match serde_json::to_vec(&p.redeem_subtree_raw) {
            Ok(v) => v,
            Err(_) => {
                events.push(self.new_event(
                    "dmt-nat",
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
                    "dmt-nat",
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
                "dmt-nat",
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
                "dmt-nat",
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
                "dmt-nat",
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
                let canon = canonical_ticker(&it.ticker);
                if !rec.whitelisted_tickers.iter().any(|t| t == &canon) {
                    events.push(self.new_event(
                        "dmt-nat",
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
                "dmt-nat",
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
            let Some(amount) = parse_deployed_amount(wtx, &canon, &item.amount_raw)? else {
                continue;
            };
            let a = i128::try_from(amount).unwrap_or(i128::MAX);
            let outcome = execute_redeem_item(
                wtx,
                &canon,
                &rec.authority_addr,
                &item.address,
                amount,
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
                            "amount": amount.to_string(),
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
                            "amount": amount.to_string(),
                            "auth_id": rec.inscription_id,
                        }),
                        block_hash,
                    ));
                }
                RedeemItemOutcome::SelfSend => {
                    // ord-tap's internal send helper returns before
                    // writing transfer logs for self-sends.
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
                            "amount": amount.to_string(),
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

/// Fold a raw on-chain ticker string to its canonical TAP key.
///
/// ord-tap stores DMT deployments under `dmt-<tick>` and plain TAP
/// deployments under `<tick>`. Keep that namespace boundary intact so
/// a plain `nat` token cannot shadow DMT NAT.
fn canonical_ticker(raw: &str) -> String {
    raw.to_ascii_lowercase()
}

fn raw_transfer_tick_matches_deployment_kind(raw: &str, deployment_is_dmt: bool) -> bool {
    // ord-tap stores DMT deployments under dmt-<tick> and token-transfer
    // creation looks up the raw lowercased transfer tick. A bare "nat"
    // transfer therefore must not target the DMT NAT deployment.
    !deployment_is_dmt || raw.to_ascii_lowercase().starts_with("dmt-")
}

fn dmt_effective_ticker(raw: &str) -> String {
    let lower = raw.to_ascii_lowercase();
    format!("dmt-{lower}")
}

fn ordered_reveal_key(op: &OrderedTapOp) -> TapOrderKey {
    match op {
        OrderedTapOp::DmtElement(k, _)
        | OrderedTapOp::DmtDeploy(k, _)
        | OrderedTapOp::DmtMint(k, _)
        | OrderedTapOp::TokenDeploy(k, _)
        | OrderedTapOp::TokenMint(k, _)
        | OrderedTapOp::SendReveal(k, _)
        | OrderedTapOp::TradeReveal(k, _)
        | OrderedTapOp::AuthReveal(k, _)
        | OrderedTapOp::PrivilegeAuthReveal(k, _)
        | OrderedTapOp::Control(k, _) => *k,
        OrderedTapOp::AuthRedeem(rr) => {
            TapOrderKey::new(rr.tx_index, rr.landing_vout, rr.landing_offset, 1, rr.rank)
        }
        OrderedTapOp::TransferInscribe(ti) => TapOrderKey::new(
            ti.tx_index,
            ti.landing_vout,
            ti.landing_offset,
            1,
            ti.inscription_number,
        ),
        OrderedTapOp::Move { tx_index, mv } => match mv {
            TrackerMove::Moved { to, to_offset, .. } => {
                TapOrderKey::new(*tx_index, to.vout, *to_offset, 0, 0)
            }
            TrackerMove::Burned { .. } => TapOrderKey::new(*tx_index, u32::MAX, u64::MAX, 0, 0),
        },
    }
}

async fn fetch_block_header_fields_retry(rpc: &RpcClient, height: u64) -> Result<(u32, u32)> {
    let mut delay_ms = 200u64;
    loop {
        match rpc.get_block_header_fields(height).await {
            Ok(v) => return Ok(v),
            Err(e) => {
                tracing::warn!(height, error = %e, "block header fields fetch failed; retrying");
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                delay_ms = (delay_ms * 2).min(5_000);
            }
        }
    }
}

fn is_privilege_auth_active(wtx: &redb::WriteTransaction, auth_id: &str) -> Result<bool> {
    let records = wtx.open_table(tables::PRIVILEGE_AUTH_RECORDS)?;
    let exists = records.get(auth_id)?.is_some();
    if !exists {
        return Ok(false);
    }
    let cancels = wtx.open_table(tables::PRIVILEGE_AUTH_CANCELS)?;
    let active = cancels.get(auth_id)?.is_none();
    Ok(active)
}

fn json_string_field(v: &serde_json::Value, field: &str) -> Option<String> {
    match v.get(field)? {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn sha256_bytes(bytes: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    let out = h.finalize();
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&out);
    arr
}

fn describe_move(mv: &TrackerMove, _height: u64) -> Option<(String, Option<String>, bool)> {
    let consumed = |k: InscriptionKind| {
        matches!(
            k,
            InscriptionKind::TokenTransfer
                | InscriptionKind::TokenSend
                | InscriptionKind::TokenAuth
                | InscriptionKind::TokenTrade
                | InscriptionKind::PrivilegeAuth
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
        TrackerMove::Burned {
            inscription,
            reason,
            ..
        } => Some((
            inscription.inscription_id.clone(),
            if matches!(reason, BurnReason::OpReturn | BurnReason::Unaddressable) {
                Some("-".to_string())
            } else {
                None
            },
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

fn parse_inscription_id(s: &str) -> Option<(bitcoin::Txid, u32)> {
    let (txid, index) = s.split_once('i')?;
    Some((txid.parse().ok()?, index.parse().ok()?))
}

/// Ord-style landing resolution: given a fresh envelope's optional
/// pointer and default flotsam offset, return the `(vout,
/// offset_in_outpoint)` where the inscription settles. Mirrors ord-tap
/// `index/updater/inscription_updater.rs:520-632`:
///
///   let offset = payload.pointer()
///       .filter(|&p| p < total_output_value)
///       .unwrap_or(default_offset /* = total_input_value before our input */);
///   // then walk outputs accumulating value; land where offset < end.
///
/// For fresh inscriptions on input 0 of a reveal tx, `default_offset` is
/// usually 0. For post-Jubilee non-input-0 inscriptions, it is the sum of
/// prior input values. A valid pointer reroutes to the sat it addresses
/// within the combined output range.
fn resolve_landing(
    tx: &bitcoin::Transaction,
    pointer: Option<u64>,
    default_offset: u64,
) -> Option<(usize, u64)> {
    let total_output_value: u64 = tx.output.iter().map(|o| o.value.to_sat()).sum();
    let target_sat = match pointer {
        Some(p) if p < total_output_value => p,
        _ => default_offset,
    };
    let mut cumulative: u64 = 0;
    for (vout, o) in tx.output.iter().enumerate() {
        let v = o.value.to_sat();
        let end = cumulative.saturating_add(v);
        if target_sat < end {
            return Some((vout, target_sat - cumulative));
        }
        cumulative = end;
    }
    None
}

fn inscription_sequence_rank(block_height: u64, within_block_rank: i64) -> u64 {
    let rank = u64::try_from(within_block_rank).unwrap_or(0);
    block_height.saturating_mul(1_000_000_000_000) + rank
}

fn map_fee_flotsam_to_coinbase(
    block: &BlockView,
    inscription: TrackedInscription,
    from: OutPoint,
    fee_prefix_before_tx: u64,
    fee_offset: u64,
) -> TrackerMove {
    let coinbase = &block.txs[0];
    let target_sat = block_subsidy_sats(block.height)
        .saturating_add(fee_prefix_before_tx)
        .saturating_add(fee_offset);
    let mut cumulative = 0u64;
    for (vout, output) in coinbase.tx.output.iter().enumerate() {
        let value = output.value.to_sat();
        let end = cumulative.saturating_add(value);
        if target_sat < end {
            if output.script_pubkey.is_op_return() {
                return TrackerMove::Burned {
                    inscription,
                    from,
                    reason: BurnReason::OpReturn,
                };
            }
            if let Some(addr) = address_from_script(&output.script_pubkey) {
                return TrackerMove::Moved {
                    inscription,
                    from,
                    to: OutPoint {
                        txid: coinbase.txid,
                        vout: vout as u32,
                    },
                    to_offset: target_sat - cumulative,
                    to_outpoint_value_sats: value,
                    new_owner_address: Some(addr),
                };
            }
            return TrackerMove::Burned {
                inscription,
                from,
                reason: BurnReason::Unaddressable,
            };
        }
        cumulative = end;
    }
    TrackerMove::Burned {
        inscription,
        from,
        reason: BurnReason::IntoFees { fee_offset },
    }
}

fn block_subsidy_sats(height: u64) -> u64 {
    let halvings = height / 210_000;
    if halvings >= 64 {
        0
    } else {
        5_000_000_000u64 >> halvings
    }
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

fn wallet_available(wtx: &redb::WriteTransaction, ticker: &str, address: &str) -> Result<i128> {
    let table = wtx.open_table(WALLET_STATE)?;
    let Some(raw) = table.get((ticker, address))? else {
        return Ok(0);
    };
    let state: WalletState = match decode(raw.value()) {
        Ok(s) => s,
        Err(_) => return Ok(0),
    };
    Ok(state.total.saturating_sub(state.transferable))
}

fn reconcile_wallet_available(
    wtx: &redb::WriteTransaction,
    ticker: &str,
    address: &str,
    occurred_at: DateTime<Utc>,
) -> Result<i128> {
    let mut state = wallet_state_snapshot(wtx, ticker, address)?;
    let spendable = state.total.saturating_sub(state.transferable);
    if state.available != spendable {
        state.available = spendable;
        if state.first_activity.is_none() {
            state.first_activity = Some(occurred_at);
        }
        state.last_activity = Some(occurred_at);
        let mut table = wtx.open_table(WALLET_STATE)?;
        table.insert((ticker, address), encode(&state)?.as_slice())?;
    }
    Ok(spendable)
}

fn wallet_state_snapshot(
    wtx: &redb::WriteTransaction,
    ticker: &str,
    address: &str,
) -> Result<WalletState> {
    let table = wtx.open_table(WALLET_STATE)?;
    let Some(raw) = table.get((ticker, address))? else {
        return Ok(WalletState::default());
    };
    Ok(decode(raw.value()).unwrap_or_default())
}

fn parse_deployed_amount(
    wtx: &redb::WriteTransaction,
    ticker: &str,
    raw: &str,
) -> Result<Option<u128>> {
    let table = wtx.open_table(DEPLOYMENTS)?;
    let Some(dep_raw) = table.get(ticker)? else {
        return Ok(None);
    };
    let dep: Deployment = decode(dep_raw.value())?;
    let Ok(amount) = parse_scaled_u128(raw, dep.decimals) else {
        return Ok(None);
    };
    let cap = parse_scaled_u128(MAX_DEC_U64_STR, dep.decimals).unwrap_or(u128::MAX);
    if amount == 0 || amount > cap {
        return Ok(None);
    }
    Ok(Some(amount))
}

fn is_dmt_reward_address_at_height(
    wtx: &redb::WriteTransaction,
    address: &str,
    height: u64,
) -> Result<bool> {
    if height < crate::ledger::deploy::NAT_MINER_TRANSFER_ACTIVATION {
        return Ok(false);
    }
    is_dmt_reward_address(wtx, address)
}

fn dmt_reward_transfer_execution_blocks(
    wtx: &redb::WriteTransaction,
    ticker: &str,
    address: &str,
    height: u64,
) -> Result<bool> {
    if height < crate::ledger::deploy::NAT_MINER_TRANSFER_EXECUTION_SHIELD {
        return Ok(false);
    }
    Ok(is_dmt_reward_address_at_height(wtx, address, height)?
        && wallet_is_blocked(wtx, ticker, address)?)
}

fn remove_trade_offers(wtx: &redb::WriteTransaction, trade_id: &str) -> Result<()> {
    let mut table = wtx.open_table(tables::TRADE_OFFERS)?;
    let lo = (trade_id, "");
    let hi = (trade_id, "\u{10ffff}");
    let keys: Vec<String> = table
        .range(lo..=hi)?
        .filter_map(|r| r.ok())
        .map(|(k, _)| {
            let (_, accepted) = k.value();
            accepted.to_string()
        })
        .collect();
    for accepted in keys {
        table.remove((trade_id, accepted.as_str()))?;
    }
    Ok(())
}

fn trade_fee(amount: u128) -> u128 {
    amount.saturating_mul(30) / 10_000
}

/// Record a permanent marker that `address` has received a DMT coinbase
/// reward credit. The marker is the prerequisite signal for the
/// miner-reward-transfer-execution shield at height >= 942,002.
/// Mirrors ord-tap's `dmtrwd/<addr>` key.
fn dmt_reward_mark(wtx: &redb::WriteTransaction, address: &str) -> Result<bool> {
    let mut table = wtx.open_table(DMT_REWARD_ADDRESSES)?;
    if table.get(address)?.is_none() {
        table.insert(address, 1u8)?;
        return Ok(true);
    }
    Ok(false)
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

#[cfg(test)]
mod tests {
    use super::{
        canonical_ticker, dmt_effective_ticker, raw_transfer_tick_matches_deployment_kind,
        resolve_landing,
    };
    use bitcoin::{
        absolute::LockTime, transaction::Version, Amount, ScriptBuf, Transaction, TxOut,
    };

    fn tx_with_outputs(values: &[u64]) -> Transaction {
        Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: Vec::new(),
            output: values
                .iter()
                .map(|sats| TxOut {
                    value: Amount::from_sat(*sats),
                    script_pubkey: ScriptBuf::new(),
                })
                .collect(),
        }
    }

    #[test]
    fn dmt_nat_keeps_ord_tap_namespace() {
        assert_eq!(dmt_effective_ticker("nat"), "dmt-nat");
        assert_eq!(canonical_ticker("dmt-nat"), "dmt-nat");
        assert_eq!(canonical_ticker("nat"), "nat");
    }

    #[test]
    fn dmt_transfer_create_requires_dmt_prefixed_raw_tick() {
        assert!(!raw_transfer_tick_matches_deployment_kind("nat", true));
        assert!(raw_transfer_tick_matches_deployment_kind("dmt-nat", true));
        assert!(raw_transfer_tick_matches_deployment_kind("DMT-NAT", true));
    }

    #[test]
    fn standard_tap_transfer_create_keeps_raw_tick_behavior() {
        assert!(raw_transfer_tick_matches_deployment_kind("nat", false));
        assert!(raw_transfer_tick_matches_deployment_kind("dmt-nat", false));
    }

    #[test]
    fn fresh_inscription_lands_when_default_offset_is_inside_outputs() {
        let tx = tx_with_outputs(&[100, 50]);
        assert_eq!(resolve_landing(&tx, None, 125), Some((1, 25)));
    }

    #[test]
    fn fresh_inscription_does_not_fallback_to_vout_zero_when_offset_is_fee_flotsam() {
        let tx = tx_with_outputs(&[7_722]);
        assert_eq!(resolve_landing(&tx, None, 41_704), None);
    }

    #[test]
    fn valid_pointer_overrides_fee_flotsam_default_offset() {
        let tx = tx_with_outputs(&[100, 50]);
        assert_eq!(resolve_landing(&tx, Some(25), 41_704), Some((0, 25)));
    }
}
