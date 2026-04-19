//! `/` — the NAT Observatory. A single embedded HTML page served from the indexer.

use std::sync::Arc;

use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;

use crate::api::state::AppState;

const INDEX_HTML: &str = include_str!("web/index.html");

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/", get(index))
        .route("/index.html", get(index))
}

async fn index() -> Response {
    let mut res = (StatusCode::OK, INDEX_HTML).into_response();
    res.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    res.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=60"),
    );
    res
}
