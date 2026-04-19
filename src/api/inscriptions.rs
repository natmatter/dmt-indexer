//! `/inscriptions/:id` — per-inscription lookup.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};

use crate::api::state::AppState;
use crate::store::codec::decode;
use crate::store::tables::{InscriptionIndex, ValidTransfer, INSCRIPTIONS, VALID_TRANSFERS};

pub fn routes() -> Router<Arc<AppState>> {
    Router::new().route("/inscriptions/{id}", get(inscription_handler))
}

async fn inscription_handler(
    State(s): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let rtx = match s.store.read() {
        Ok(t) => t,
        Err(e) => return Json(serde_json::json!({ "error": e.to_string() })).into_response(),
    };
    let entry: Option<InscriptionIndex> = rtx
        .open_table(INSCRIPTIONS)
        .ok()
        .and_then(|t| {
            t.get(id.as_str())
                .ok()
                .flatten()
                .map(|v| v.value().to_vec())
        })
        .and_then(|b| decode(&b).ok());
    let validity: Option<ValidTransfer> = rtx
        .open_table(VALID_TRANSFERS)
        .ok()
        .and_then(|t| {
            t.get(id.as_str())
                .ok()
                .flatten()
                .map(|v| v.value().to_vec())
        })
        .and_then(|b| decode(&b).ok());
    match entry {
        Some(e) => Json(serde_json::json!({
            "inscription_id": id,
            "ticker": e.ticker,
            "kind": e.kind,
            "original_amount": e.original_amount,
            "inscribed_height": e.inscribed_height,
            "current_owner_address": e.current_owner_address,
            "consumed_height": e.consumed_height,
            "valid_transfer": validity,
        }))
        .into_response(),
        None => (
            http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "not found" })),
        )
            .into_response(),
    }
}
