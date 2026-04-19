//! Tiny in-process response cache for hot, low-cardinality GET endpoints.
//!
//! Why: the home page fires /status + /holders + /activity and a stampede
//! would otherwise scan redb on every request. A short-TTL cache (below
//! block time) absorbs the herd without affecting freshness meaningfully.
//!
//! Key: method + path + raw query.
//! Value: status + content-type + body bytes + headers we want to pass
//!        through (specifically X-Total-Count for pagination).
//!
//! Policy: tiny FIFO eviction when capacity is exceeded. Expired entries
//! are purged lazily on access. No LRU bookkeeping — keeps the hot path
//! lock-light.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use axum::body::{to_bytes, Body};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use bytes::Bytes;

use crate::config::CacheConfig;

const MAX_BODY_BYTES: usize = 2 * 1024 * 1024; // never cache bodies larger than 2 MiB
const TOTAL_HEADER: &str = "x-total-count";

#[derive(Clone)]
struct Entry {
    status: StatusCode,
    content_type: Option<HeaderValue>,
    total_count: Option<HeaderValue>,
    body: Bytes,
    expires_at: Instant,
}

pub struct ResponseCache {
    cfg: CacheConfig,
    inner: Mutex<Inner>,
}

struct Inner {
    map: HashMap<String, Entry>,
    order: Vec<String>, // FIFO insertion order, used only on eviction
}

impl ResponseCache {
    pub fn new(cfg: CacheConfig) -> Self {
        Self {
            cfg,
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                order: Vec::new(),
            }),
        }
    }

    pub fn ttl(&self) -> Duration {
        Duration::from_millis(self.cfg.ttl_ms)
    }

    pub fn enabled(&self) -> bool {
        self.cfg.enabled
    }

    fn get(&self, key: &str) -> Option<Entry> {
        let mut g = self.inner.lock().ok()?;
        if let Some(e) = g.map.get(key) {
            if Instant::now() < e.expires_at {
                return Some(e.clone());
            }
            g.map.remove(key);
            g.order.retain(|k| k != key);
        }
        None
    }

    fn put(&self, key: String, entry: Entry) {
        let Ok(mut g) = self.inner.lock() else { return };
        if !g.map.contains_key(&key) {
            g.order.push(key.clone());
            while g.order.len() > self.cfg.capacity.max(1) {
                if let Some(evict) = g.order.first().cloned() {
                    g.order.remove(0);
                    g.map.remove(&evict);
                } else {
                    break;
                }
            }
        }
        g.map.insert(key, entry);
    }
}

/// Paths we cache. Keep this list narrow: only low-cardinality, hot,
/// idempotent reads. Per-wallet queries are fine too — the LRU-like
/// bound keeps memory in check.
fn is_cacheable_path(p: &str) -> bool {
    p == "/status" || p == "/holders" || p == "/activity" || p.starts_with("/wallets/")
}

pub async fn layer(
    State(cache): State<std::sync::Arc<ResponseCache>>,
    req: Request,
    next: Next,
) -> Response {
    if !cache.enabled() || req.method() != axum::http::Method::GET {
        return next.run(req).await;
    }
    let path = req.uri().path();
    if !is_cacheable_path(path) {
        return next.run(req).await;
    }
    let key = format!(
        "{} {}?{}",
        req.method(),
        path,
        req.uri().query().unwrap_or("")
    );

    if let Some(hit) = cache.get(&key) {
        return build_response(&hit, true);
    }

    let resp = next.run(req).await;
    let (parts, body) = resp.into_parts();
    // Only cache 200-range responses with a small enough body.
    let ok = parts.status.is_success();
    let body_bytes = match to_bytes(body, MAX_BODY_BYTES).await {
        Ok(b) => b,
        Err(_) => return build_from_parts(parts, Bytes::new()),
    };

    if ok {
        let ct = parts.headers.get(axum::http::header::CONTENT_TYPE).cloned();
        let tc = parts.headers.get(TOTAL_HEADER).cloned();
        let entry = Entry {
            status: parts.status,
            content_type: ct,
            total_count: tc,
            body: body_bytes.clone(),
            expires_at: Instant::now() + cache.ttl(),
        };
        cache.put(key, entry);
    }
    build_from_parts(parts, body_bytes)
}

fn build_response(e: &Entry, hit: bool) -> Response {
    let mut r = Response::builder().status(e.status);
    if let Some(ct) = &e.content_type {
        r = r.header(axum::http::header::CONTENT_TYPE, ct);
    }
    if let Some(tc) = &e.total_count {
        r = r.header(TOTAL_HEADER, tc);
        r = r.header(
            "access-control-expose-headers",
            HeaderValue::from_static("x-total-count"),
        );
    }
    r = r.header(
        "x-cache",
        HeaderValue::from_static(if hit { "HIT" } else { "MISS" }),
    );
    r.body(Body::from(e.body.clone()))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

fn build_from_parts(parts: axum::http::response::Parts, body: Bytes) -> Response {
    // Reconstruct response with a concrete Bytes body so we can still
    // return after consuming it for cache insertion.
    let mut resp = Response::from_parts(parts, Body::from(body));
    // Mark as a miss for observability.
    let _ = resp
        .headers_mut()
        .append("x-cache", HeaderValue::from_static("MISS"));
    expose_total_header(resp.headers_mut());
    resp
}

fn expose_total_header(headers: &mut HeaderMap) {
    if headers.get(TOTAL_HEADER).is_some() {
        headers.insert(
            "access-control-expose-headers",
            HeaderValue::from_static("x-total-count"),
        );
    }
}
