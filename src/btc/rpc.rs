//! Minimal Bitcoin Core JSON-RPC client.
//!
//! We only need a handful of methods and prefer a thin layer over
//! `bitcoincore-rpc`:
//! - cookie auth is the default for running alongside a local node;
//! - we fetch full raw blocks (`getblock <hash> 0`) and decode them with
//!   `bitcoin::consensus` ourselves so we have access to witnesses;
//! - for block identity we call `getblockcount` and `getblockhash`.
//!
//! Errors are wrapped in our unified [`Error`] taxonomy.

use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use bitcoin::consensus::Decodable;
use bitcoin::{Block, BlockHash, Transaction, Txid};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{debug, warn};

use crate::btc::block::{BlockView, TxView};
use crate::config::BitcoinConfig;
use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct RpcClient {
    http: Client,
    url: String,
    /// Base URL for Bitcoin Core's REST interface
    /// (`http://host:port/rest`). Derived from `url` by dropping any
    /// path component. Built once at construction.
    rest_base: String,
    auth: Auth,
}

#[derive(Debug, Clone)]
enum Auth {
    Cookie { path: PathBuf },
    UserPass { user: String, pass: String },
}

#[derive(Debug, Serialize)]
struct RpcRequest<'a> {
    jsonrpc: &'a str,
    id: u64,
    method: &'a str,
    params: Value,
}

#[derive(Debug, Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
    #[allow(dead_code)]
    id: u64,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct BatchResult {
    id: usize,
    result: Option<String>,
    #[serde(default)]
    error: Option<RpcError>,
}

impl RpcClient {
    pub fn from_config(cfg: &BitcoinConfig) -> Result<Self> {
        let auth = if let Some(path) = &cfg.cookie_file {
            Auth::Cookie { path: path.clone() }
        } else {
            let user = cfg
                .rpc_user
                .clone()
                .ok_or_else(|| Error::Config("rpc_user missing".into()))?;
            let pass = cfg
                .rpc_password
                .clone()
                .ok_or_else(|| Error::Config("rpc_password missing".into()))?;
            Auth::UserPass { user, pass }
        };
        let http = Client::builder()
            .timeout(Duration::from_secs(cfg.timeout_secs))
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .pool_max_idle_per_host(16)
            .build()?;
        // REST endpoint lives at the same host:port as JSON-RPC but at
        // path `/rest/*`. Strip any trailing slash or path from the
        // configured URL.
        let rest_base = {
            let parsed = url::Url::parse(&cfg.rpc_url)?;
            format!(
                "{}://{}{}",
                parsed.scheme(),
                parsed.host_str().unwrap_or("127.0.0.1"),
                parsed
                    .port_or_known_default()
                    .map(|p| format!(":{}", p))
                    .unwrap_or_default(),
            )
        };
        Ok(Self {
            http,
            url: cfg.rpc_url.clone(),
            rest_base,
            auth,
        })
    }

    fn userpass(&self) -> Result<(String, String)> {
        match &self.auth {
            Auth::Cookie { path } => {
                let raw = fs::read_to_string(path)
                    .map_err(|e| Error::Rpc(format!("read cookie file {}: {e}", path.display())))?;
                let (u, p) = raw.trim().split_once(':').ok_or_else(|| {
                    Error::Rpc(format!("malformed cookie file {}", path.display()))
                })?;
                Ok((u.to_string(), p.to_string()))
            }
            Auth::UserPass { user, pass } => Ok((user.clone(), pass.clone())),
        }
    }

    async fn call<T: for<'de> Deserialize<'de>>(&self, method: &str, params: Value) -> Result<T> {
        let (user, pass) = self.userpass()?;
        let req = RpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method,
            params,
        };
        let resp = self
            .http
            .post(&self.url)
            .basic_auth(user, Some(pass))
            .json(&req)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Rpc(format!("{method}: http {status}: {body}")));
        }
        let parsed: RpcResponse<T> = resp.json().await?;
        if let Some(err) = parsed.error {
            return Err(Error::Rpc(format!(
                "{method}: code={} msg={}",
                err.code, err.message
            )));
        }
        parsed
            .result
            .ok_or_else(|| Error::Rpc(format!("{method}: missing result")))
    }

    pub async fn get_block_count(&self) -> Result<u64> {
        self.call::<u64>("getblockcount", json!([])).await
    }

    pub async fn get_block_hash(&self, height: u64) -> Result<BlockHash> {
        let s: String = self.call("getblockhash", json!([height])).await?;
        s.parse::<BlockHash>()
            .map_err(|e| Error::Rpc(format!("parse block hash {s}: {e}")))
    }

    pub async fn get_best_block_hash(&self) -> Result<BlockHash> {
        let s: String = self.call("getbestblockhash", json!([])).await?;
        s.parse::<BlockHash>()
            .map_err(|e| Error::Rpc(format!("parse best hash {s}: {e}")))
    }

    /// Fetch just the `nBits` field of a block header at `height`.
    ///
    /// Mints reference an arbitrary prior block to derive their reward
    /// amount via the bits field. In the early NAT era this can be
    /// hundreds of distinct heights per block; fetching the full block
    /// each time (1–4 MB over REST) crushes sync. Pull the 80-byte
    /// header via `getblockheader` instead.
    pub async fn get_block_bits(&self, height: u64) -> Result<u32> {
        let hash = self.get_block_hash(height).await?;
        let info: Value = self
            .call("getblockheader", json!([hash.to_string(), true]))
            .await?;
        let bits_hex = info
            .get("bits")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Rpc("getblockheader: missing bits".into()))?;
        u32::from_str_radix(bits_hex, 16)
            .map_err(|e| Error::Rpc(format!("parse bits {bits_hex}: {e}")))
    }

    /// Fetch a block by hash via Bitcoin Core's REST binary endpoint.
    /// Much faster than JSON-RPC because it skips hex encoding, JSON
    /// wrapping, and basic-auth overhead. Requires `rest=1` in
    /// `bitcoin.conf` (the standard setting on indexer nodes).
    ///
    /// Caller must supply the height (we know it from `getblockhash`
    /// and don't need to round-trip it separately).
    ///
    /// Retries on transient HTTP errors (connection close, timeout,
    /// read error) — bitcoind's REST server can drop concurrent
    /// requests under load and we don't want a single flaky response
    /// to kill the whole sync.
    pub async fn get_block(&self, hash: BlockHash, height: u64) -> Result<BlockView> {
        let url = format!("{}/rest/block/{}.bin", self.rest_base, hash);
        let bytes = self.get_with_retry(&url).await?;
        let block = Block::consensus_decode(&mut bytes.as_ref())?;

        let prev_hash = block.header.prev_blockhash;
        let bits = block.header.bits.to_consensus();
        let nonce = block.header.nonce;
        let timestamp = block.header.time;
        let txs: Vec<TxView> = block
            .txdata
            .into_iter()
            .enumerate()
            .map(|(i, tx)| TxView {
                txid: tx.compute_txid(),
                is_coinbase: i == 0,
                tx,
            })
            .collect();
        Ok(BlockView {
            height,
            hash,
            prev_hash,
            bits,
            nonce,
            timestamp,
            txs,
        })
    }

    /// Fetch a single transaction via the REST binary endpoint.
    /// Requires `txindex=1` for historical lookups. Used by the
    /// element-inscription resolver and the sat-tracker's input-value
    /// resolver. Retries on transient HTTP errors.
    pub async fn get_raw_transaction(&self, txid: Txid) -> Result<Transaction> {
        let url = format!("{}/rest/tx/{}.bin", self.rest_base, txid);
        let bytes = self.get_with_retry(&url).await?;
        let tx = Transaction::consensus_decode(&mut bytes.as_ref())?;
        Ok(tx)
    }

    /// Batched transaction fetch. Packs many `getrawtransaction`
    /// calls into a single HTTP POST using JSON-RPC's array syntax
    /// (ord's Fetcher pattern). bitcoind processes them in one
    /// round-trip; for N inputs this replaces N RTTs with one.
    ///
    /// Returns txs in the SAME order as `txids`. Non-existent txids
    /// are reported as `None`.
    pub async fn get_raw_transactions_batch(
        &self,
        txids: &[Txid],
    ) -> Result<Vec<Option<Transaction>>> {
        if txids.is_empty() {
            return Ok(Vec::new());
        }
        // Chunk so bitcoind doesn't time out on very large batches
        // and we don't hold its RPC thread hostage (other services on
        // the same node use it too). Smaller chunks retry faster and
        // on transient timeouts only re-request ~100 txs rather than
        // the whole batch.
        const CHUNK: usize = 100;
        if txids.len() > CHUNK {
            let mut all = Vec::with_capacity(txids.len());
            for chunk in txids.chunks(CHUNK) {
                match self.get_raw_transactions_batch_chunk(chunk).await {
                    Ok(part) => all.extend(part),
                    Err(e) => {
                        // Chunk-level fallback: if the batch JSON-RPC
                        // times out we fall back to per-tx REST. This
                        // is slower for that chunk but avoids killing
                        // the whole tick.
                        tracing::warn!(
                            chunk_size = chunk.len(),
                            error = %e,
                            "batch chunk failed; falling back to per-tx REST"
                        );
                        for txid in chunk {
                            match self.get_raw_transaction(*txid).await {
                                Ok(tx) => all.push(Some(tx)),
                                Err(_) => all.push(None),
                            }
                        }
                    }
                }
            }
            return Ok(all);
        }
        self.get_raw_transactions_batch_chunk(txids).await
    }

    async fn get_raw_transactions_batch_chunk(
        &self,
        txids: &[Txid],
    ) -> Result<Vec<Option<Transaction>>> {
        let (user, pass) = self.userpass()?;

        // Build the request array. Each item has a numeric `id`
        // matching its index so we can re-order responses.
        let batch: Vec<Value> = txids
            .iter()
            .enumerate()
            .map(|(i, txid)| {
                json!({
                    "jsonrpc": "2.0",
                    "id": i,
                    "method": "getrawtransaction",
                    "params": [ txid.to_string() ],
                })
            })
            .collect();
        let body = Value::Array(batch);

        let mut delay_ms = 100u64;
        let mut last_err: Option<Error> = None;
        for attempt in 0..4 {
            let resp = self
                .http
                .post(&self.url)
                .basic_auth(&user, Some(&pass))
                .json(&body)
                .send()
                .await;
            match resp {
                Err(e) => last_err = Some(Error::Http(e)),
                Ok(resp) if !resp.status().is_success() && resp.status().is_client_error() => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    return Err(Error::Rpc(format!("batch: http {status}: {body}")));
                }
                Ok(resp) => match resp.json::<Vec<BatchResult>>().await {
                    Ok(results) => return Self::decode_batch(txids.len(), results),
                    Err(e) => last_err = Some(Error::Http(e)),
                },
            }
            if attempt < 3 {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                delay_ms = (delay_ms * 2).min(2000);
            }
        }
        Err(last_err.unwrap_or_else(|| Error::Rpc("batch: exhausted retries".into())))
    }

    fn decode_batch(n: usize, mut results: Vec<BatchResult>) -> Result<Vec<Option<Transaction>>> {
        // Responses may come back out of order; sort by `id` to
        // align with request indices.
        results.sort_by_key(|r| r.id);
        let mut out: Vec<Option<Transaction>> = vec![None; n];
        for r in results {
            if r.id >= n {
                continue;
            }
            match (r.result, r.error) {
                (Some(hex_str), _) => {
                    let bytes = hex::decode(hex_str.trim())?;
                    let tx = Transaction::consensus_decode(&mut bytes.as_slice())?;
                    out[r.id] = Some(tx);
                }
                (None, Some(_)) => {
                    // -5 "no such tx" etc → leave as None
                }
                _ => {}
            }
        }
        Ok(out)
    }

    /// Fetch a URL with up to 4 tries on transient HTTP errors. Works
    /// around bitcoind's REST server dropping connections under load
    /// (`hyper::IncompleteMessage`, reset, etc). Non-transient errors
    /// (4xx) are returned immediately.
    async fn get_with_retry(&self, url: &str) -> Result<bytes::Bytes> {
        let mut delay_ms = 100u64;
        let mut last_err: Option<Error> = None;
        for attempt in 0..4 {
            match self.http.get(url).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        match resp.bytes().await {
                            Ok(b) => return Ok(b),
                            Err(e) => {
                                last_err = Some(Error::Http(e));
                            }
                        }
                    } else if status.is_client_error() {
                        let body = resp.text().await.unwrap_or_default();
                        return Err(Error::Rpc(format!("{url}: http {status}: {body}")));
                    } else {
                        let body = resp.text().await.unwrap_or_default();
                        last_err = Some(Error::Rpc(format!("{url}: http {status}: {body}")));
                    }
                }
                Err(e) => {
                    last_err = Some(Error::Http(e));
                }
            }
            if attempt < 3 {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                delay_ms = (delay_ms * 2).min(2000);
            }
        }
        Err(last_err.unwrap_or_else(|| Error::Rpc(format!("{url}: exhausted retries"))))
    }

    /// Returns (best_height, best_hash).
    pub async fn tip(&self) -> Result<(u64, BlockHash)> {
        let height = self.get_block_count().await?;
        let hash = self.get_best_block_hash().await?;
        debug!(height, %hash, "tip");
        Ok((height, hash))
    }

    /// Compatibility check: warn if Bitcoin Core version is < 24 or
    /// txindex is disabled. Does not fail — many users want to run
    /// with a local node they trust.
    pub async fn preflight(&self) -> Result<()> {
        let net_info: Value = self.call("getnetworkinfo", json!([])).await?;
        if let Some(version) = net_info.get("version").and_then(Value::as_u64) {
            if version < 240000 {
                warn!(
                    version,
                    "Bitcoin Core < 24.0 detected; inscription parsing is best-effort"
                );
            }
        }
        let index_info: Result<Value> = self.call("getindexinfo", json!([])).await;
        if let Ok(info) = index_info {
            if info
                .get("txindex")
                .and_then(|t| t.get("synced"))
                .and_then(Value::as_bool)
                != Some(true)
            {
                warn!("txindex is not fully synced (required for historical lookups)");
            }
        }
        Ok(())
    }
}
