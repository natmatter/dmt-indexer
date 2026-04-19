//! `/activity` — event feed. Filters by ticker / event_type / family / address.
//! Paginated with offset+limit. Returns an X-Total-Count header so the UI can
//! show "page N of M".

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{header, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use redb::ReadableTable;
use serde::Deserialize;

use crate::api::state::AppState;
use crate::ledger::event::LedgerEvent;
use crate::store::codec::decode;
use crate::store::tables::ACTIVITY_RECENT;

pub fn routes() -> Router<Arc<AppState>> {
    Router::new().route("/activity", get(activity_handler))
}

#[derive(Deserialize)]
struct ActivityQuery {
    ticker: Option<String>,
    event_type: Option<String>,
    family: Option<String>,
    address: Option<String>,
    limit: Option<u32>,
    offset: Option<u32>,
}

/// High-level category filter. Keep aligned with the web UI's filter pills.
pub(crate) fn matches_family(ev_type: &str, family: &str) -> bool {
    let is_transfer = ev_type == "token_transfer_credit" || ev_type == "token_transfer_debit";
    let is_mint = ev_type == "coinbase_reward_credit" || ev_type == "dmt_mint_credit";
    let is_burn = ev_type.contains("burn");
    match family {
        "transfers" => ev_type == "token_transfer_debit",
        "sent" => ev_type == "token_transfer_debit",
        "received" => ev_type == "token_transfer_credit",
        "mint" => is_mint,
        "burn" => is_burn,
        "other" => !is_transfer && !is_mint && !is_burn,
        // legacy aliases
        "transfer" => is_transfer,
        "in" => ev_type == "token_transfer_credit",
        "out" => ev_type == "token_transfer_debit",
        _ => true,
    }
}

async fn activity_handler(
    State(s): State<Arc<AppState>>,
    Query(q): Query<ActivityQuery>,
) -> Response {
    let limit = q
        .limit
        .unwrap_or(s.cfg.api.max_page_size)
        .min(s.cfg.api.max_page_size) as usize;
    let offset = q.offset.unwrap_or(0) as usize;
    let rtx = match s.store.read() {
        Ok(t) => t,
        Err(e) => return Json(serde_json::json!({ "error": e.to_string() })).into_response(),
    };
    let table = match rtx.open_table(ACTIVITY_RECENT) {
        Ok(t) => t,
        Err(e) => return Json(serde_json::json!({ "error": e.to_string() })).into_response(),
    };
    let want_ticker = q.ticker.as_deref().map(str::to_lowercase);
    let want_type = q.event_type.as_deref();
    let want_addr = q.address.as_deref();
    let want_family = q.family.as_deref();

    let mut page: Vec<LedgerEvent> = Vec::with_capacity(limit);
    let mut total: u64 = 0;

    for row in table.iter().unwrap() {
        let (k, v) = row.unwrap();
        let (t, _, _) = k.value();
        if let Some(ref wt) = want_ticker {
            if t != wt.as_str() {
                continue;
            }
        }
        let Ok(ev) = decode::<LedgerEvent>(v.value()) else {
            continue;
        };
        if let Some(want) = want_type {
            if ev.event_type.as_str() != want {
                continue;
            }
        }
        if let Some(fam) = want_family {
            if !matches_family(ev.event_type.as_str(), fam) {
                continue;
            }
        }
        if let Some(want) = want_addr {
            if ev.address.as_deref() != Some(want) {
                continue;
            }
        }
        total += 1;
        if total as usize > offset && page.len() < limit {
            page.push(ev);
        }
    }

    response_with_total(Json(page).into_response(), total)
}

pub(crate) fn response_with_total(mut resp: Response, total: u64) -> Response {
    let headers = resp.headers_mut();
    headers.insert(
        header::HeaderName::from_static("x-total-count"),
        HeaderValue::from_str(&total.to_string()).unwrap_or(HeaderValue::from_static("0")),
    );
    headers.insert(
        header::HeaderName::from_static("access-control-expose-headers"),
        HeaderValue::from_static("x-total-count"),
    );
    resp
}
