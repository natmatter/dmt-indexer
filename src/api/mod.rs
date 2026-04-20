pub mod activity;
pub mod cache;
pub mod cursor;
pub mod export;
pub mod holders;
pub mod inscriptions;
pub mod ratelimit;
pub mod state;
pub mod status;
pub mod wallets;
pub mod web;
pub mod ws;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::Router;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::config::Config;
use crate::error::Result;
use crate::sync::SyncHandle;
pub use state::AppState;

pub fn build_router(cfg: &Config, handle: SyncHandle) -> Result<Router> {
    let state = Arc::new(AppState {
        store: handle.store.clone(),
        bus: handle.bus.clone(),
        cfg: cfg.clone(),
    });
    let cors = build_cors(cfg)?;
    let mut router = Router::new()
        .merge(status::routes())
        .merge(holders::routes())
        .merge(activity::routes())
        .merge(cursor::routes())
        .merge(export::routes())
        .merge(wallets::routes())
        .merge(inscriptions::routes())
        .merge(ws::routes())
        .merge(web::routes());
    if cfg.api.enable_admin {
        let admin_token = cfg.api.admin_token.clone();
        if admin_token.as_deref().map(str::is_empty).unwrap_or(true) {
            tracing::warn!(
                "api.enable_admin = true but api.admin_token is unset — /admin/* endpoints are OPEN"
            );
        }
        let admin = status::admin_routes().layer(middleware::from_fn(
            move |req: Request<Body>, next: Next| {
                let token = admin_token.clone();
                async move { require_admin_token(token, req, next).await }
            },
        ));
        router = router.merge(admin);
    }
    let router = router.with_state(state);

    // Expose X-Total-Count through CORS and cache layer.
    let cache = Arc::new(cache::ResponseCache::new(cfg.api.cache.clone()));
    let limiter = Arc::new(ratelimit::Limiter::new(cfg.api.rate_limit.clone()));

    let router = router
        .layer(middleware::from_fn_with_state(cache.clone(), cache::layer))
        .layer(middleware::from_fn_with_state(
            limiter.clone(),
            ratelimit::layer,
        ))
        .layer(TraceLayer::new_for_http())
        .layer(cors);
    Ok(router)
}

async fn require_admin_token(expected: Option<String>, req: Request<Body>, next: Next) -> Response {
    let expected = match expected.filter(|s| !s.is_empty()) {
        Some(t) => t,
        // No token configured → open. Warned at startup; don't reject.
        None => return next.run(req).await,
    };
    let ok = req
        .headers()
        .get("x-admin-token")
        .and_then(|v| v.to_str().ok())
        .map(|v| constant_time_eq(v.as_bytes(), expected.as_bytes()))
        .unwrap_or(false);
    if ok {
        next.run(req).await
    } else {
        Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header("content-type", "application/json")
            .body(Body::from(r#"{"error":"admin token required"}"#))
            .unwrap()
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn build_cors(cfg: &Config) -> Result<CorsLayer> {
    let mut layer = CorsLayer::new();
    if cfg.api.cors.allowed_origins.iter().any(|o| o == "*") {
        layer = layer.allow_origin(Any);
    } else {
        let mut origins = Vec::new();
        for o in &cfg.api.cors.allowed_origins {
            origins.push(
                o.parse::<http::HeaderValue>().map_err(|e| {
                    crate::error::Error::Config(format!("bad cors origin {o}: {e}"))
                })?,
            );
        }
        if !origins.is_empty() {
            layer = layer.allow_origin(origins);
        }
    }
    Ok(layer
        .allow_methods([http::Method::GET, http::Method::POST])
        .allow_headers(Any)
        .expose_headers([http::HeaderName::from_static("x-total-count")]))
}
