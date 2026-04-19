//! `/status`, `/summary`, `/deployments`, `/metrics`.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use redb::ReadableTable;
use serde::{Deserialize, Serialize};

use crate::api::state::AppState;
use crate::ledger::deploy::Deployment;
use crate::store::codec::decode;
use crate::store::tables::{
    cursor_get, Cursor, DailyStats, WalletState, DAILY_STATS, DEPLOYMENTS, WALLET_STATE,
};

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/status", get(status_handler))
        .route("/summary", get(summary_handler))
        .route("/deployments", get(deployments_handler))
        .route("/metrics", get(metrics_handler))
}

/// Admin routes only mounted when `api.enable_admin = true`.
pub fn admin_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/admin/invariants", get(invariants_handler))
        .route("/admin/reconcile", post(reconcile_handler))
        .route("/admin/reindex", post(reindex_handler))
}

async fn reconcile_handler(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    // Alias for /admin/invariants; `reconcile` is the conventional name
    // downstream ops tooling (nat-admin-api) uses for the same check.
    invariants_handler(State(s)).await.into_response()
}

async fn reindex_handler(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    let target = s.cfg.sync.start_height.saturating_sub(1);
    match crate::sync::reorg::rewind_cursor(&s.store, target) {
        Ok(()) => Json(serde_json::json!({
            "ok": true,
            "rewound_to": target,
        }))
        .into_response(),
        Err(e) => (
            http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn invariants_handler(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    match crate::verify::check_invariants(&s.store) {
        Ok(diffs) => Json(serde_json::json!({
            "ok": diffs.is_empty(),
            "violations": diffs,
        }))
        .into_response(),
        Err(e) => (
            http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

#[derive(Serialize)]
struct StatusBody {
    tip_height: u64,
    tip_hash: String,
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
    deployments: Vec<DeploymentStatus>,
}

#[derive(Serialize)]
struct DeploymentStatus {
    ticker: String,
    total_supply: u128,
    holder_count: u64,
    activation_height: u64,
}

async fn status_handler(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    let rtx = match s.store.read() {
        Ok(t) => t,
        Err(e) => return Json(serde_json::json!({ "error": e.to_string() })).into_response(),
    };
    let cursor: Cursor = cursor_get(&rtx).ok().flatten().unwrap_or_default();
    let mut depls = Vec::new();
    if let Ok(table) = rtx.open_table(DEPLOYMENTS) {
        for row in table.iter().unwrap() {
            let (k, v) = row.unwrap();
            let d: Deployment = match decode(v.value()) {
                Ok(d) => d,
                Err(_) => continue,
            };
            let (total, holders) = supply_for(&s, k.value()).unwrap_or((0, 0));
            depls.push(DeploymentStatus {
                ticker: k.value().to_string(),
                total_supply: total,
                holder_count: holders,
                activation_height: d.activation_height,
            });
        }
    }
    Json(StatusBody {
        tip_height: cursor.height,
        tip_hash: cursor.block_hash,
        updated_at: cursor.updated_at,
        deployments: depls,
    })
    .into_response()
}

#[derive(Deserialize)]
struct SummaryQuery {
    ticker: Option<String>,
}

#[derive(Serialize)]
struct SummaryEntry {
    ticker: String,
    total_supply: u128,
    holder_count: u64,
    recent: Vec<(u32, DailyStats)>,
}

async fn summary_handler(
    State(s): State<Arc<AppState>>,
    Query(q): Query<SummaryQuery>,
) -> impl IntoResponse {
    let rtx = match s.store.read() {
        Ok(t) => t,
        Err(e) => return Json(serde_json::json!({ "error": e.to_string() })).into_response(),
    };
    let mut tickers: Vec<String> = Vec::new();
    if let Some(t) = q.ticker {
        tickers.push(t.to_lowercase());
    } else if let Ok(table) = rtx.open_table(DEPLOYMENTS) {
        for row in table.iter().unwrap() {
            tickers.push(row.unwrap().0.value().to_string());
        }
    }
    let mut out = Vec::new();
    for t in tickers {
        let (total, holders) = supply_for(&s, &t).unwrap_or((0, 0));
        let recent = recent_daily(&s, &t, 14).unwrap_or_default();
        out.push(SummaryEntry {
            ticker: t,
            total_supply: total,
            holder_count: holders,
            recent,
        });
    }
    Json(out).into_response()
}

async fn deployments_handler(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    let rtx = match s.store.read() {
        Ok(t) => t,
        Err(e) => return Json(serde_json::json!({ "error": e.to_string() })).into_response(),
    };
    let mut out = Vec::new();
    if let Ok(table) = rtx.open_table(DEPLOYMENTS) {
        for row in table.iter().unwrap() {
            let (_k, v) = row.unwrap();
            if let Ok(d) = decode::<Deployment>(v.value()) {
                out.push(d);
            }
        }
    }
    Json(out).into_response()
}

async fn metrics_handler(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    use std::fmt::Write;
    let mut body = String::new();
    let rtx = match s.store.read() {
        Ok(t) => t,
        Err(e) => return (http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    let cursor = cursor_get(&rtx).ok().flatten().unwrap_or_default();
    let _ = writeln!(body, "# HELP dmt_indexer_cursor_height block cursor");
    let _ = writeln!(body, "# TYPE dmt_indexer_cursor_height gauge");
    let _ = writeln!(body, "dmt_indexer_cursor_height {}", cursor.height);
    if let Ok(table) = rtx.open_table(DEPLOYMENTS) {
        for row in table.iter().unwrap() {
            let (k, _v) = row.unwrap();
            let t = k.value().to_string();
            let (total, holders) = supply_for(&s, &t).unwrap_or((0, 0));
            let _ = writeln!(
                body,
                "dmt_indexer_total_supply{{ticker=\"{}\"}} {}",
                t, total
            );
            let _ = writeln!(
                body,
                "dmt_indexer_holder_count{{ticker=\"{}\"}} {}",
                t, holders
            );
        }
    }
    (
        [(http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}

fn supply_for(s: &AppState, ticker: &str) -> crate::error::Result<(u128, u64)> {
    let rtx = s.store.read()?;
    let table = rtx.open_table(WALLET_STATE)?;
    let mut total: u128 = 0;
    let mut holders: u64 = 0;
    for row in table.iter()? {
        let (k, v) = row?;
        let (t, _) = k.value();
        if t != ticker {
            continue;
        }
        if let Ok(st) = decode::<WalletState>(v.value()) {
            if st.total > 0 {
                total = total.saturating_add(st.total as u128);
                holders += 1;
            }
        }
    }
    Ok((total, holders))
}

fn recent_daily(
    s: &AppState,
    ticker: &str,
    days: u32,
) -> crate::error::Result<Vec<(u32, DailyStats)>> {
    let rtx = s.store.read()?;
    let table = rtx.open_table(DAILY_STATS)?;
    let now = chrono::Utc::now().timestamp() / 86400;
    let floor = (now as u32).saturating_sub(days);
    let mut out = Vec::new();
    for row in table.range((ticker, floor)..=(ticker, u32::MAX))? {
        let (k, v) = row?;
        let (_t, day) = k.value();
        if let Ok(d) = decode::<DailyStats>(v.value()) {
            out.push((day, d));
        }
    }
    Ok(out)
}

#[allow(dead_code)]
fn _keep_hashmap_used() -> HashMap<(), ()> {
    HashMap::new()
}
