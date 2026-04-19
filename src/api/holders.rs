//! `/holders` — leaderboard, paginated.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use redb::ReadableTable;
use serde::{Deserialize, Serialize};

use crate::api::state::AppState;
use crate::store::codec::decode;
use crate::store::tables::{WalletState, WALLET_STATE};

pub fn routes() -> Router<Arc<AppState>> {
    Router::new().route("/holders", get(holders_handler))
}

#[derive(Deserialize)]
struct HoldersQuery {
    ticker: Option<String>,
    limit: Option<u32>,
    offset: Option<u32>,
    search: Option<String>,
}

#[derive(Serialize)]
struct HolderRow {
    ticker: String,
    address: String,
    total: i128,
    available: i128,
    transferable: i128,
    transferables_blocked: bool,
}

async fn holders_handler(
    State(s): State<Arc<AppState>>,
    Query(q): Query<HoldersQuery>,
) -> axum::response::Response {
    let limit = q
        .limit
        .unwrap_or(s.cfg.api.max_page_size)
        .min(s.cfg.api.max_page_size);
    let offset = q.offset.unwrap_or(0);
    let rtx = match s.store.read() {
        Ok(t) => t,
        Err(e) => return Json(serde_json::json!({ "error": e.to_string() })).into_response(),
    };
    let table = match rtx.open_table(WALLET_STATE) {
        Ok(t) => t,
        Err(e) => return Json(serde_json::json!({ "error": e.to_string() })).into_response(),
    };
    let mut rows: Vec<HolderRow> = Vec::new();
    let want_ticker = q.ticker.as_deref().map(str::to_lowercase);
    let search = q.search.as_deref().map(str::to_lowercase);
    for row in table.iter().unwrap() {
        let (k, v) = row.unwrap();
        let (t, a) = k.value();
        if let Some(ref wt) = want_ticker {
            if t != wt.as_str() {
                continue;
            }
        }
        let Ok(state) = decode::<WalletState>(v.value()) else {
            continue;
        };
        if state.total <= 0 {
            continue;
        }
        if let Some(ref s) = search {
            if !a.to_lowercase().contains(s) {
                continue;
            }
        }
        rows.push(HolderRow {
            ticker: t.to_string(),
            address: a.to_string(),
            total: state.total,
            available: state.available,
            transferable: state.transferable,
            transferables_blocked: state.transferables_blocked,
        });
    }
    rows.sort_by(|a, b| b.total.cmp(&a.total));
    let total = rows.len() as u64;
    let slice: Vec<_> = rows
        .into_iter()
        .skip(offset as usize)
        .take(limit as usize)
        .collect();
    crate::api::activity::response_with_total(Json(slice).into_response(), total)
}
