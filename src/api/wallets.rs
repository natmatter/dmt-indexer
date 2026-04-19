//! `/wallets/:address`, `/wallets/:address/transferables`.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use redb::ReadableTable;
use serde::{Deserialize, Serialize};

use crate::api::state::AppState;
use crate::ledger::event::LedgerEvent;
use crate::store::codec::decode;
use crate::store::tables::{
    ValidTransfer, DEPLOYMENTS, TRANSFERABLES_BY_SENDER, VALID_TRANSFERS, WALLET_ACTIVITY,
    WALLET_STATE,
};

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/wallets/{address}", get(wallet_handler))
        .route(
            "/wallets/{address}/transferables",
            get(transferables_handler),
        )
        .route("/wallets/{address}/activity", get(wallet_activity_handler))
}

#[derive(Deserialize)]
struct WalletActivityQuery {
    ticker: Option<String>,
    family: Option<String>,
    limit: Option<u32>,
    offset: Option<u32>,
}

async fn wallet_activity_handler(
    State(s): State<Arc<AppState>>,
    Path(address): Path<String>,
    Query(q): Query<WalletActivityQuery>,
) -> axum::response::Response {
    let limit = q
        .limit
        .unwrap_or(s.cfg.api.max_page_size)
        .min(s.cfg.api.max_page_size) as usize;
    let offset = q.offset.unwrap_or(0) as usize;
    let rtx = match s.store.read() {
        Ok(t) => t,
        Err(e) => return Json(serde_json::json!({ "error": e.to_string() })).into_response(),
    };
    let tickers: Vec<String> = if let Some(t) = q.ticker.as_deref() {
        vec![t.to_string()]
    } else {
        match rtx.open_table(DEPLOYMENTS) {
            Ok(t) => t
                .iter()
                .ok()
                .into_iter()
                .flatten()
                .filter_map(|r| r.ok())
                .map(|(k, _)| k.value().to_string())
                .collect(),
            Err(_) => Vec::new(),
        }
    };
    let mut page: Vec<LedgerEvent> = Vec::with_capacity(limit);
    let mut total: u64 = 0;
    if let Ok(table) = rtx.open_table(WALLET_ACTIVITY) {
        for ticker in &tickers {
            let lo = (ticker.as_str(), address.as_str(), 0u64, 0u64);
            let hi = (ticker.as_str(), address.as_str(), u64::MAX, u64::MAX);
            if let Ok(range) = table.range(lo..=hi) {
                for row in range {
                    let Ok((_k, v)) = row else { continue };
                    let Ok(ev) = decode::<LedgerEvent>(v.value()) else {
                        continue;
                    };
                    // WALLET_ACTIVITY indexes each transfer under both
                    // the sender's and the receiver's address so both
                    // wallets can look up their history. When viewing
                    // wallet X, only surface events where X is the
                    // actual subject — otherwise each transfer shows
                    // up twice (once as Sent, once as Received).
                    if ev.address.as_deref() != Some(address.as_str()) {
                        continue;
                    }
                    if let Some(fam) = q.family.as_deref() {
                        if !crate::api::activity::matches_family(ev.event_type.as_str(), fam) {
                            continue;
                        }
                    }
                    total += 1;
                    if total as usize > offset && page.len() < limit {
                        page.push(ev);
                    }
                }
            }
        }
    }
    crate::api::activity::response_with_total(Json(page).into_response(), total)
}

#[derive(Serialize)]
struct WalletBody {
    address: String,
    balances: Vec<WalletBalance>,
    recent_activity: Vec<LedgerEvent>,
}

#[derive(Serialize)]
struct WalletBalance {
    ticker: String,
    total: i128,
    available: i128,
    transferable: i128,
    burned: i128,
    transferables_blocked: bool,
}

async fn wallet_handler(
    State(s): State<Arc<AppState>>,
    Path(address): Path<String>,
) -> impl IntoResponse {
    let rtx = match s.store.read() {
        Ok(t) => t,
        Err(e) => return Json(serde_json::json!({ "error": e.to_string() })).into_response(),
    };
    // Enumerate registered tickers once, then do point lookups per
    // ticker rather than scanning the full WALLET_STATE / WALLET_ACTIVITY
    // tables. This keeps the handler O(deployments) + O(recent activity
    // per wallet) instead of O(total wallets).
    let tickers: Vec<String> = match rtx.open_table(DEPLOYMENTS) {
        Ok(t) => t
            .iter()
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|r| r.ok())
            .map(|(k, _)| k.value().to_string())
            .collect(),
        Err(_) => Vec::new(),
    };

    let mut balances = Vec::new();
    if let Ok(table) = rtx.open_table(WALLET_STATE) {
        for ticker in &tickers {
            if let Ok(Some(raw)) = table.get((ticker.as_str(), address.as_str())) {
                if let Ok(st) = decode::<crate::store::tables::WalletState>(raw.value()) {
                    balances.push(WalletBalance {
                        ticker: ticker.clone(),
                        total: st.total,
                        available: st.available,
                        transferable: st.transferable,
                        burned: st.burned,
                        transferables_blocked: st.transferables_blocked,
                    });
                }
            }
        }
    }

    let mut recent = Vec::new();
    if let Ok(table) = rtx.open_table(WALLET_ACTIVITY) {
        'outer: for ticker in &tickers {
            // Keys are (ticker, address, inv_ts, event_id). Range the
            // narrow slice for this wallet only — avoids the full-table
            // scan the previous implementation did.
            let lo = (ticker.as_str(), address.as_str(), 0u64, 0u64);
            let hi = (ticker.as_str(), address.as_str(), u64::MAX, u64::MAX);
            if let Ok(range) = table.range(lo..=hi) {
                for row in range {
                    let Ok((_k, v)) = row else { continue };
                    if let Ok(ev) = decode::<LedgerEvent>(v.value()) {
                        // See note in wallet_activity_handler: drop
                        // duplicate rows where this wallet is only the
                        // counterparty.
                        if ev.address.as_deref() != Some(address.as_str()) {
                            continue;
                        }
                        recent.push(ev);
                    }
                    if recent.len() >= 50 {
                        break 'outer;
                    }
                }
            }
        }
    }
    Json(WalletBody {
        address,
        balances,
        recent_activity: recent,
    })
    .into_response()
}

#[derive(Serialize)]
struct Transferable {
    inscription_id: String,
    ticker: String,
    amount: u128,
    inscribed_height: u64,
}

async fn transferables_handler(
    State(s): State<Arc<AppState>>,
    Path(address): Path<String>,
) -> impl IntoResponse {
    let rtx = match s.store.read() {
        Ok(t) => t,
        Err(e) => return Json(serde_json::json!({ "error": e.to_string() })).into_response(),
    };
    let tickers: Vec<String> = match rtx.open_table(DEPLOYMENTS) {
        Ok(t) => t
            .iter()
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|r| r.ok())
            .map(|(k, _)| k.value().to_string())
            .collect(),
        Err(_) => Vec::new(),
    };
    let mut out = Vec::new();
    // Pull ids from the secondary index via a narrow range scan, then
    // hydrate each with its ValidTransfer row. No full-table scan.
    let idx = match rtx.open_table(TRANSFERABLES_BY_SENDER) {
        Ok(t) => t,
        Err(e) => return Json(serde_json::json!({ "error": e.to_string() })).into_response(),
    };
    let vt_table = match rtx.open_table(VALID_TRANSFERS) {
        Ok(t) => t,
        Err(e) => return Json(serde_json::json!({ "error": e.to_string() })).into_response(),
    };
    for ticker in &tickers {
        let lo = (ticker.as_str(), address.as_str(), "");
        let hi = (ticker.as_str(), address.as_str(), "\u{10FFFF}");
        let Ok(range) = idx.range(lo..=hi) else {
            continue;
        };
        for row in range {
            let Ok((k, _)) = row else { continue };
            let (_t, _a, insc_id) = k.value();
            if let Ok(Some(v)) = vt_table.get(insc_id) {
                if let Ok(vt) = decode::<ValidTransfer>(v.value()) {
                    if vt.consumed_height.is_none() {
                        out.push(Transferable {
                            inscription_id: insc_id.to_string(),
                            ticker: vt.ticker,
                            amount: vt.amount,
                            inscribed_height: vt.inscribed_height,
                        });
                    }
                }
            }
        }
    }
    Json(out).into_response()
}
