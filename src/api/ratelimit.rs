//! Simple per-IP token-bucket rate limiter.
//!
//! Intentional footprint — no new deps. A single Mutex<HashMap<IpAddr,
//! Bucket>> with lazy refill. Idle buckets are reaped when the map
//! grows past a soft cap.
//!
//! Resolution order for the "client IP":
//!   1. x-forwarded-for (first hop)
//!   2. x-real-ip
//!   3. the immediate socket peer
//!
//! Off by default. Turn on in config.toml:
//!   [api.rate_limit]
//!   enabled     = true
//!   per_second  = 20
//!   burst       = 40

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Mutex;
use std::time::Instant;

use axum::body::Body;
use axum::extract::{ConnectInfo, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;

use crate::config::RateLimitConfig;

const SOFT_CAP: usize = 100_000;

struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

pub struct Limiter {
    cfg: RateLimitConfig,
    buckets: Mutex<HashMap<IpAddr, Bucket>>,
}

impl Limiter {
    pub fn new(cfg: RateLimitConfig) -> Self {
        Self {
            cfg,
            buckets: Mutex::new(HashMap::new()),
        }
    }
    pub fn enabled(&self) -> bool {
        self.cfg.enabled
    }
    fn allow(&self, ip: IpAddr) -> bool {
        let now = Instant::now();
        let rate = self.cfg.per_second as f64;
        let burst = self.cfg.burst.max(1) as f64;
        let Ok(mut g) = self.buckets.lock() else {
            return true; // fail-open if the mutex is poisoned
        };
        if g.len() > SOFT_CAP {
            // Soft reap: drop buckets that haven't been touched in 60s.
            let cutoff = now - std::time::Duration::from_secs(60);
            g.retain(|_, b| b.last_refill > cutoff);
        }
        let b = g.entry(ip).or_insert_with(|| Bucket {
            tokens: burst,
            last_refill: now,
        });
        let dt = now.duration_since(b.last_refill).as_secs_f64();
        b.tokens = (b.tokens + dt * rate).min(burst);
        b.last_refill = now;
        if b.tokens >= 1.0 {
            b.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Best-effort client IP extraction. Trusts the first x-forwarded-for
/// hop — safe only if you're behind a trusted reverse proxy. Falls back
/// to the actual peer for direct connections.
fn client_ip(req: &Request) -> IpAddr {
    if let Some(xff) = req
        .headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
    {
        if let Some(first) = xff.split(',').next() {
            if let Ok(ip) = first.trim().parse::<IpAddr>() {
                return ip;
            }
        }
    }
    if let Some(xri) = req.headers().get("x-real-ip").and_then(|v| v.to_str().ok()) {
        if let Ok(ip) = xri.trim().parse::<IpAddr>() {
            return ip;
        }
    }
    req.extensions()
        .get::<ConnectInfo<std::net::SocketAddr>>()
        .map(|c| c.0.ip())
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
}

pub async fn layer(
    State(lim): State<std::sync::Arc<Limiter>>,
    req: Request,
    next: Next,
) -> Response {
    if !lim.enabled() {
        return next.run(req).await;
    }
    let ip = client_ip(&req);
    if lim.allow(ip) {
        next.run(req).await
    } else {
        Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .header("retry-after", "1")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"error":"rate limit exceeded"}"#))
            .unwrap()
    }
}
