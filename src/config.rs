use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::{Error, Result};

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub bitcoin: BitcoinConfig,
    pub storage: StorageConfig,
    #[serde(default)]
    pub sync: SyncConfig,
    #[serde(default)]
    pub api: ApiConfig,
    #[serde(default)]
    pub conformance: Option<ConformanceConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct BitcoinConfig {
    pub rpc_url: String,
    pub cookie_file: Option<PathBuf>,
    pub rpc_user: Option<String>,
    pub rpc_password: Option<String>,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct StorageConfig {
    pub db_path: PathBuf,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SyncConfig {
    #[serde(default = "default_start_height")]
    pub start_height: u64,
    #[serde(default = "default_finality_depth")]
    pub finality_depth: u64,
    #[serde(default = "default_max_blocks_per_tick")]
    pub max_blocks_per_tick: u64,
    #[serde(default = "default_parallelism")]
    pub parallelism: usize,
    #[serde(default = "default_poll_idle_ms")]
    pub poll_idle_ms: u64,
    /// Number of blocks applied per redb write transaction commit.
    /// Larger values amortize fsync cost but risk losing more work on
    /// crash (cursor advances only on commit). Default 200; ord uses
    /// 5000 but redb's memory cost per pending write is higher than
    /// ord's redb usage pattern, so we're conservative.
    #[serde(default = "default_commit_interval")]
    pub commit_interval: u64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            start_height: default_start_height(),
            finality_depth: default_finality_depth(),
            max_blocks_per_tick: default_max_blocks_per_tick(),
            parallelism: default_parallelism(),
            poll_idle_ms: default_poll_idle_ms(),
            commit_interval: default_commit_interval(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ApiConfig {
    #[serde(default = "default_bind_addr")]
    pub bind_addr: SocketAddr,
    #[serde(default)]
    pub enable_admin: bool,
    /// Shared secret required in `X-Admin-Token` header for any
    /// `/admin/*` call. When admin endpoints are enabled but this is
    /// unset, admin calls are open — log-warns on startup. Production
    /// deployments must set a non-empty value.
    #[serde(default)]
    pub admin_token: Option<String>,
    #[serde(default = "default_max_page_size")]
    pub max_page_size: u32,
    #[serde(default)]
    pub cors: CorsConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            bind_addr: default_bind_addr(),
            enable_admin: false,
            admin_token: None,
            max_page_size: default_max_page_size(),
            cors: CorsConfig::default(),
            cache: CacheConfig::default(),
            rate_limit: RateLimitConfig::default(),
        }
    }
}

/// In-process response cache for hot GET endpoints.
#[derive(Clone, Debug, Deserialize)]
pub struct CacheConfig {
    /// Master switch. Default on — it's safe and always a win.
    #[serde(default = "default_cache_enabled")]
    pub enabled: bool,
    /// TTL in milliseconds. Below block time by design.
    #[serde(default = "default_cache_ttl_ms")]
    pub ttl_ms: u64,
    /// Max number of distinct cached responses retained.
    #[serde(default = "default_cache_capacity")]
    pub capacity: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: default_cache_enabled(),
            ttl_ms: default_cache_ttl_ms(),
            capacity: default_cache_capacity(),
        }
    }
}

/// Per-IP token-bucket rate limit. Off by default.
#[derive(Clone, Debug, Deserialize)]
pub struct RateLimitConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Sustained requests per second per client IP.
    #[serde(default = "default_rl_per_second")]
    pub per_second: u32,
    /// Burst size (tokens available on a quiet bucket).
    #[serde(default = "default_rl_burst")]
    pub burst: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            per_second: default_rl_per_second(),
            burst: default_rl_burst(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct CorsConfig {
    #[serde(default)]
    pub allowed_origins: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ConformanceConfig {
    pub postgres_url: Option<String>,
}

fn default_timeout_secs() -> u64 {
    60
}
fn default_start_height() -> u64 {
    817_709
}
fn default_finality_depth() -> u64 {
    6
}
fn default_max_blocks_per_tick() -> u64 {
    10
}
fn default_parallelism() -> usize {
    8
}
fn default_poll_idle_ms() -> u64 {
    2000
}
fn default_commit_interval() -> u64 {
    200
}
fn default_bind_addr() -> SocketAddr {
    "127.0.0.1:8080".parse().unwrap()
}
fn default_max_page_size() -> u32 {
    200
}
fn default_cache_enabled() -> bool {
    true
}
fn default_cache_ttl_ms() -> u64 {
    2000
}
fn default_cache_capacity() -> usize {
    1024
}
fn default_rl_per_second() -> u32 {
    // Intentionally generous. Catches scrapers and abuse, not humans.
    // A human clicking furiously won't exceed ~5 req/s; a normal page
    // load fires <10. Multiple users behind a shared NAT still fit.
    100
}
fn default_rl_burst() -> u32 {
    // Allows aggressive page loads and tab-opens without tripping.
    200
}

impl Config {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("reading {}: {e}", path.display())))?;
        let cfg: Config = toml::from_str(&text)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if self.bitcoin.cookie_file.is_none()
            && (self.bitcoin.rpc_user.is_none() || self.bitcoin.rpc_password.is_none())
        {
            return Err(Error::Config(
                "bitcoin auth: must set cookie_file OR (rpc_user + rpc_password)".into(),
            ));
        }
        if self.sync.parallelism == 0 {
            return Err(Error::Config("sync.parallelism must be >= 1".into()));
        }
        if self.api.max_page_size == 0 {
            return Err(Error::Config("api.max_page_size must be >= 1".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_cookie_auth() {
        let toml_str = r#"
            [bitcoin]
            rpc_url = "http://127.0.0.1:8332"
            cookie_file = "/var/bitcoind/.cookie"

            [storage]
            db_path = "/var/dmt/index.redb"
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.sync.start_height, 817_709);
        assert_eq!(cfg.sync.finality_depth, 6);
        assert_eq!(cfg.api.bind_addr.port(), 8080);
    }

    #[test]
    fn rejects_missing_auth() {
        let toml_str = r#"
            [bitcoin]
            rpc_url = "http://127.0.0.1:8332"

            [storage]
            db_path = "/tmp/x.redb"
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn accepts_userpass() {
        let toml_str = r#"
            [bitcoin]
            rpc_url      = "http://127.0.0.1:8332"
            rpc_user     = "u"
            rpc_password = "p"

            [storage]
            db_path = "/tmp/x.redb"

            [sync]
            parallelism = 4
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.sync.parallelism, 4);
    }
}
