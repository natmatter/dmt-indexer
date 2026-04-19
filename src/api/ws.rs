//! `/ws/activity` — WebSocket activity push feed.
//!
//! Clients subscribe with optional filters via query string:
//! `?ticker=nat&event_type=token_transfer_credit&address=bc1...`.

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use futures_util::SinkExt;
use serde::Deserialize;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt as _;
use tracing::debug;

use crate::api::state::AppState;
use crate::ledger::event::LedgerEvent;

pub fn routes() -> Router<Arc<AppState>> {
    Router::new().route("/ws/activity", get(ws_handler))
}

#[derive(Deserialize, Clone)]
pub struct WsQuery {
    pub ticker: Option<String>,
    pub event_type: Option<String>,
    pub address: Option<String>,
}

async fn ws_handler(
    State(s): State<Arc<AppState>>,
    Query(q): Query<WsQuery>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, s, q))
}

async fn handle_socket(mut socket: WebSocket, s: Arc<AppState>, q: WsQuery) {
    let rx = s.bus.subscribe();
    let mut stream = BroadcastStream::new(rx).filter_map(move |r| match r {
        Ok(ev) => Some(ev),
        Err(_) => None,
    });
    while let Some(ev) = stream.next().await {
        if !filter_match(&ev, &q) {
            continue;
        }
        let bytes = match serde_json::to_string(&ev) {
            Ok(s) => s,
            Err(_) => continue,
        };
        if socket.send(Message::Text(bytes.into())).await.is_err() {
            debug!("ws: send failed, closing");
            break;
        }
    }
    let _ = socket.close().await;
}

fn filter_match(ev: &LedgerEvent, q: &WsQuery) -> bool {
    if let Some(ref t) = q.ticker {
        if !ev.ticker.eq_ignore_ascii_case(t) {
            return false;
        }
    }
    if let Some(ref et) = q.event_type {
        if ev.event_type.as_str() != et {
            return false;
        }
    }
    if let Some(ref a) = q.address {
        if ev.address.as_deref() != Some(a.as_str()) {
            return false;
        }
    }
    true
}
