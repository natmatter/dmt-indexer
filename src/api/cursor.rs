//! `GET /cursor` — a reorg-detection primitive for downstream consumers.
//!
//! Returns:
//!   max_event_id    the highest event_id currently persisted
//!   tip_height      tip from the indexer's own Cursor
//!   tip_hash        tip block hash
//!   recent_blocks   newest-first list of the last N (height, hash) pairs
//!                   the indexer has actually emitted events for
//!
//! Intended use (see dmt-backend/src/ingest/reorg.rs): on every
//! reconnect / periodic tick, a consumer compares its own block-hashes
//! at those heights against the indexer's. Highest disagreement =
//! fork point; everything at-or-above that height should be discarded.
//! This works for in-flight reorgs (event_ids keep climbing) AND for
//! restart-after-reorg reorgs (event_ids reused after `seed_next_event_id`),
//! because block_hash divergence is the ground truth.

use std::collections::HashSet;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use redb::ReadableTable;
use serde::{Deserialize, Serialize};

use crate::api::state::AppState;
use crate::ledger::event::LedgerEvent;
use crate::store::codec::decode;
use crate::store::tables::{Cursor, EVENTS, META};

pub fn routes() -> Router<Arc<AppState>> {
    Router::new().route("/cursor", get(cursor_handler))
}

#[derive(Deserialize)]
struct CursorQuery {
    #[serde(default = "default_recent")]
    recent: usize,
}
fn default_recent() -> usize { 20 }

#[derive(Serialize)]
struct CursorBody {
    max_event_id: u64,
    tip_height: u64,
    tip_hash: String,
    recent_blocks: Vec<RecentBlock>,
}

#[derive(Serialize)]
struct RecentBlock {
    height: u64,
    hash: String,
}

async fn cursor_handler(
    State(s): State<Arc<AppState>>,
    Query(q): Query<CursorQuery>,
) -> Response {
    let want_recent = q.recent.clamp(1, 256);
    let rtx = match s.store.read() {
        Ok(t) => t,
        Err(e) => return Json(serde_json::json!({ "error": e.to_string() })).into_response(),
    };

    // Tip = the persisted Cursor in META.
    let (tip_height, tip_hash) = match rtx.open_table(META) {
        Ok(t) => t
            .get("cursor")
            .ok()
            .flatten()
            .and_then(|raw| decode::<Cursor>(raw.value()).ok())
            .map(|c| (c.height, c.block_hash))
            .unwrap_or((0, String::new())),
        Err(_) => (0, String::new()),
    };

    // Scan EVENTS newest-first. Redb iterator returns entries in key
    // order; `.rev()` gives us descending. We collect the last N
    // distinct (height, hash) pairs.
    let events = match rtx.open_table(EVENTS) {
        Ok(t) => t,
        Err(e) => return Json(serde_json::json!({ "error": e.to_string() })).into_response(),
    };

    let mut max_event_id: u64 = 0;
    let mut recent_blocks: Vec<RecentBlock> = Vec::new();
    let mut seen: HashSet<u64> = HashSet::new();

    if let Ok(iter) = events.iter() {
        for row in iter.rev() {
            let Ok((k, v)) = row else { continue };
            let (_ticker, event_id) = k.value();
            if max_event_id == 0 {
                max_event_id = event_id;
            }
            if recent_blocks.len() >= want_recent {
                // Still continue to compute max_event_id on the first
                // row only — which we already did.
                break;
            }
            let Ok(ev) = decode::<LedgerEvent>(v.value()) else { continue };
            if seen.insert(ev.block_height) {
                recent_blocks.push(RecentBlock {
                    height: ev.block_height,
                    hash: ev.block_hash,
                });
            }
        }
    }

    Json(CursorBody {
        max_event_id,
        tip_height,
        tip_hash,
        recent_blocks,
    })
    .into_response()
}
