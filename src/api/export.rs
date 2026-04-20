//! `/export/events` — cursor-paginated full-history export.
//!
//! Unlike `/activity` (backed by the rolling `ACTIVITY_RECENT` buffer,
//! capped at the most recent ~10k events), this endpoint streams the
//! authoritative `EVENTS` table in chronological order so downstream
//! services like dmt-backend can build a complete mirror.
//!
//! Query:
//!   ticker            required
//!   since_event_id    exclusive cursor; start with 0 for a cold seed
//!   limit             default = api.max_page_size (200), cap-clamped
//!
//! Response: array of LedgerEvent, sorted ascending by event_id.
//! Pagination: pass the last returned event_id as `since_event_id` on
//! the next call. Empty response ⇒ caught up.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;

use crate::api::state::AppState;
use crate::ledger::event::LedgerEvent;
use crate::store::codec::decode;
use crate::store::tables::EVENTS;

pub fn routes() -> Router<Arc<AppState>> {
    Router::new().route("/export/events", get(export_handler))
}

#[derive(Deserialize)]
struct ExportQuery {
    ticker: String,
    #[serde(default)]
    since_event_id: u64,
    limit: Option<u32>,
}

async fn export_handler(State(s): State<Arc<AppState>>, Query(q): Query<ExportQuery>) -> Response {
    let limit = q
        .limit
        .unwrap_or(s.cfg.api.max_page_size)
        .min(s.cfg.api.max_page_size) as usize;
    let ticker = q.ticker.to_lowercase();
    let since = q.since_event_id;

    let rtx = match s.store.read() {
        Ok(t) => t,
        Err(e) => return Json(serde_json::json!({ "error": e.to_string() })).into_response(),
    };
    let table = match rtx.open_table(EVENTS) {
        Ok(t) => t,
        Err(e) => return Json(serde_json::json!({ "error": e.to_string() })).into_response(),
    };

    // EVENTS is keyed by (ticker, event_id). A range scan from
    // (ticker, since + 1) through (ticker, u64::MAX) yields events in
    // ascending event_id order — exactly what the backend wants for a
    // deterministic, append-only backfill.
    let lo = (ticker.as_str(), since.saturating_add(1));
    let hi = (ticker.as_str(), u64::MAX);
    let range = match table.range(lo..=hi) {
        Ok(r) => r,
        Err(e) => return Json(serde_json::json!({ "error": e.to_string() })).into_response(),
    };

    let mut out: Vec<LedgerEvent> = Vec::with_capacity(limit);
    for row in range {
        if out.len() >= limit {
            break;
        }
        let Ok((_k, v)) = row else { continue };
        if let Ok(ev) = decode::<LedgerEvent>(v.value()) {
            out.push(ev);
        }
    }
    Json(out).into_response()
}
