# dmt-indexer

A standalone **$NAT** indexer for Bitcoin. Talks directly to Bitcoin
Core, writes a single `index.redb` file, serves an HTTP API.

No `ord`, no Postgres, no third-party P2P. One binary, one file, one
config.

## Scope

**Today — `nat` only.** This release indexes the `nat` ticker on
Bitcoin L1:

- `dmt-deploy`, `dmt-mint`, `token-transfer`,
  `block-transferables` / `unblock-transferables`
- Both short-form (`"nat"`) and long-form (`"dmt-nat"`) ticker strings
  in TAP payloads
- Coinbase-reward distribution from block **885,588**
- Miner transferability lock from block **941,848**

**On the roadmap.** Full DMT token support — multi-ticker deployments
beyond `nat` — is planned. The storage schema, event model, and API
are already ticker-keyed, so extending coverage is a matter of
protocol plumbing rather than a redesign. Contributions welcome.

Does **not** index (today or on the near-term roadmap): BRC-20, Runes,
Bitmap, `token-auth`, `token-trade`, `token-send`, `privilege-auth`,
or UNAT collectible rendering. If you need those, run
[`tap-reader`](https://github.com/Trac-Systems/tap-reader) or an
ordinals indexer.

## Requirements

- **Bitcoin Core 24+**, with:
  - `txindex=1` (we read raw tx witnesses)
  - `rest=1` (we fetch full blocks via the REST binary endpoint)
- **Rust 1.75+** to build from source
- **~2 GB free disk** for `index.redb` plus headroom
- Read access to the cookie file, or RPC user/password credentials

## Quickstart

### 1. Build

```sh
git clone https://github.com/natmatter/dmt-indexer
cd dmt-indexer
cargo build --release
```

### 2. Configure

```sh
cp config.example.toml config.toml
```

Edit `config.toml` — at minimum set `cookie_file` (or `rpc_user` +
`rpc_password`) and `db_path`.

### 3. Run

```sh
./target/release/dmt-indexer run --config config.toml
```

First sync from block 817,709 to tip takes **3–5 hours** on a local
node (~350–2,000 blocks/min depending on era). Progress is written
every 200 blocks; safe to restart at any time.

Hit `http://127.0.0.1:8080/status` to watch.

### Docker

```sh
docker run -d \
  --name dmt-indexer \
  -v /var/lib/dmt-indexer:/var/lib/dmt-indexer \
  -v /etc/dmt-indexer/config.toml:/etc/dmt-indexer/config.toml:ro \
  -v /var/lib/bitcoind/.cookie:/bitcoind.cookie:ro \
  -p 127.0.0.1:8080:8080 \
  ghcr.io/natmatter/dmt-indexer:latest
```

## Configuration

```toml
[bitcoin]
rpc_url      = "http://127.0.0.1:8332"
cookie_file  = "/var/lib/bitcoind/.cookie"
# or: rpc_user = "...", rpc_password = "..."
timeout_secs = 60

[storage]
db_path = "/var/lib/dmt-indexer/index.redb"

[sync]
start_height        = 817709   # NAT activation; ignored if a cursor already exists
finality_depth      = 0        # blocks behind tip to keep indexed (0 = index to tip)
max_blocks_per_tick = 500      # upper bound on blocks applied per tick
commit_interval     = 200      # blocks per redb write transaction
parallelism         = 8        # concurrent block prefetch + parse
poll_idle_ms        = 2000     # sleep between ticks when caught up

[api]
bind_addr    = "127.0.0.1:8080"
enable_admin = false           # set true to expose /admin/* endpoints
max_page_size = 200

[api.cors]
allowed_origins = []           # "*" for all, or a list of origins
```

## CLI

```
dmt-indexer run              sync loop + API in one process
dmt-indexer sync             sync loop only
dmt-indexer serve            API only (against an existing db)
dmt-indexer status           print current cursor + supply
dmt-indexer reindex          rewind cursor to start_height-1, resync on next run
dmt-indexer export-snapshot  write a JSON snapshot of ledger state
dmt-indexer verify           diff against a snapshot or a Postgres reference
```

## HTTP API

The indexer serves an embedded web explorer at the root path (`GET /`) —
a single static page that queries the endpoints below. Point a browser
at your `api.bind_addr` to use it. All other endpoints return JSON.

| Endpoint | Purpose |
|---|---|
| `GET /` | Embedded web explorer (wallet lookup, activity, holders) |
| `GET /status` | Cursor height + hash, per-deployment supply + holder count |
| `GET /summary` | Per-deployment stats + recent daily rollups |
| `GET /deployments` | Registered DMT deployments (just `nat` in this build) |
| `GET /holders` | Leaderboard, paginated. Query: `ticker`, `limit`, `offset`. Returns `X-Total-Count`. |
| `GET /activity` | Event feed. Query: `ticker`, `family` (transfers, sent, received, mint, burn, other), `event_type`, `address`, `limit`, `offset`. Returns `X-Total-Count`. |
| `GET /wallets/:address` | Balances + flags across all deployments; recent activity for this wallet |
| `GET /wallets/:address/activity` | Paginated activity for this wallet. Query: `ticker`, `family`, `limit`, `offset`. Returns `X-Total-Count`. |
| `GET /wallets/:address/transferables` | Unsettled `token-transfer` inscriptions this wallet can list for sale |
| `GET /inscriptions/:id` | Kind, current owner, original amount, consumed height |
| `GET /metrics` | Prometheus text format |
| `WS /ws/activity` | Live event push feed; filter query: `ticker`, `event_type`, `address` |

Admin endpoints (only mounted when `enable_admin = true`):

| Endpoint | Purpose |
|---|---|
| `GET /admin/invariants` | Runs the correctness invariants, returns any diffs |

## Architecture

```
┌──────────────┐      ┌───────────────────────┐      ┌──────────┐
│ Bitcoin Core │──RPC─▶│      dmt-indexer      │◀─HTTP┤ frontend │
│ (txindex=1)  │      │  sync loop + redb db  │      │ consumer │
└──────────────┘      └───────────────────────┘      └──────────┘
                              │
                       index.redb (single file)
```

## License

MIT. See `LICENSE`.

## Acknowledgments

- Bitcoin Core
- The [ord](https://github.com/ordinals/ord) project (≥ 0.14) — our
  inscription envelope parser follows ord's current format
- [Trac Systems](https://github.com/Trac-Systems/tap-protocol-specs) — TAP protocol
- [Digital Matter Theory](https://digital-matter-theory.gitbook.io/digital-matter-theory) — the token model $NAT is built on
