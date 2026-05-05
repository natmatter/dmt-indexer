# dmt-indexer

A standalone **DMT/TAP** indexer for Bitcoin. It talks directly to
Bitcoin Core, writes a single `index.redb` file, and serves HTTP and
WebSocket APIs for downstream services such as `dmt-backend`,
`nat-explorer`, and operational tools.

No `ord`, no Postgres, no third-party P2P. One binary, one file, one
config.

## Scope

This indexer follows TAP/DMT inscription history and stores every
registered deployment by ticker. `$NAT` / `dmt-nat` is a first-class
deployment, but it is not the only indexed token.

Currently supported protocol surface:

- DMT deploy/mint, including `$NAT` coinbase rewards and miner transfer
  shields
- Standard TAP token deploy/mint
- `token-transfer` creation, settlement, burn, and fee-fallen behavior
- `token-send`, `token-auth`, `token-trade`, `privilege-auth`, and
  control operations
- Delegate envelope resolution, same-block inscription movement, and
  inscription owner tracking
- Global event export for complete downstream mirrors

The embedded web explorer is still NAT-oriented. The storage, ledger
events, and JSON APIs are multi-token.

Does **not** index: BRC-20, Runes, Bitmap rendering, ord media serving,
or UNAT collectible rendering.

## Requirements

- **Bitcoin Core 24+**, with:
  - `txindex=1` because witnesses are read from raw transactions
  - `rest=1` because blocks are fetched through the REST binary endpoint
- **Rust 1.75+** to build from source
- **40+ GB free disk** for a current full-history `index.redb`, with
  extra headroom as chain history grows
- Read access to the Bitcoin Core cookie file, or RPC user/password
  credentials

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

Edit `config.toml`. At minimum set `cookie_file` or `rpc_user` /
`rpc_password`, and set `db_path` to a writable location.

For a parity-oriented cold sync, use the full TAP history floor:
`start_height = 779832`. Existing databases ignore this setting once a
cursor has been persisted.

### 3. Run

```sh
./target/release/dmt-indexer run --config config.toml
```

A first full-history sync can take many hours depending on node,
storage, and CPU. Progress is committed at `commit_interval` block
boundaries and the process is safe to restart.

With the default bind address, watch progress at:

```sh
curl http://127.0.0.1:8080/status
curl http://127.0.0.1:8080/cursor
```

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
start_height        = 779832   # full TAP history floor; ignored if a cursor exists
finality_depth      = 0        # blocks behind tip to keep indexed; 0 = index to tip
max_blocks_per_tick = 500      # upper bound on blocks applied per tick
commit_interval     = 200      # blocks per redb write transaction
parallelism         = 8        # concurrent block prefetch + parse
poll_idle_ms        = 2000     # sleep between ticks when caught up

[api]
bind_addr     = "127.0.0.1:8080"
enable_admin  = false          # set true to expose /admin/* endpoints
# admin_token = "change-me"    # required by /admin/* when set
max_page_size = 200

[api.cors]
allowed_origins = []           # "*" for all, or a list of origins

[api.cache]
enabled  = true
ttl_ms   = 2000
capacity = 1024

[api.rate_limit]
enabled    = false
per_second = 100
burst      = 200
```

## CLI

```text
dmt-indexer run              sync loop + API in one process
dmt-indexer sync             sync loop only
dmt-indexer serve            API only, against an existing db
dmt-indexer status           print current cursor
dmt-indexer reindex          rewind cursor to start_height-1
dmt-indexer export-snapshot  write a JSON snapshot of ledger state
dmt-indexer verify           diff against a snapshot, Postgres, or invariants
```

`reindex` rewinds the persisted cursor only. It does not delete
`index.redb`, but the next sync will replay from `start_height`.

## HTTP API

The indexer serves an embedded web explorer at `GET /`, `GET
/index.html`, and wallet routes such as `GET /w/{address}`. All other
endpoints return JSON unless noted.

| Endpoint | Purpose |
|---|---|
| `GET /status` | Cursor height/hash, deployment supplies, holder counts |
| `GET /summary` | Deployment stats and recent daily rollups |
| `GET /deployments` | Registered DMT/TAP deployments |
| `GET /cursor?recent=20` | Reorg-safe cursor for downstream mirrors |
| `GET /holders?ticker=nat&limit=50&offset=0` | Holder leaderboard. Returns `X-Total-Count`. |
| `GET /activity` | Recent event feed. Query: `ticker`, `family`, `event_type`, `address`, `limit`, `offset`. Returns `X-Total-Count`. |
| `GET /export/events` | Complete chronological event export. Query: optional `ticker`, `since_event_id`, `limit`. |
| `GET /wallets/{address}` | Balances and recent activity across deployments |
| `GET /wallets/{address}/activity` | Wallet activity. Query: `ticker`, `family`, `limit`, `offset`. Returns `X-Total-Count`. |
| `GET /wallets/{address}/transferables` | Unsettled transfer inscriptions currently controlled by the wallet |
| `GET /inscriptions/{id}` | Inscription kind, owner, original amount, and consumed height |
| `GET /metrics` | Prometheus text format |
| `WS /ws/activity` | Live event feed. Query: `ticker`, `event_type`, `address`. |

Admin endpoints are mounted only when `api.enable_admin = true`.
If `api.admin_token` is set, send it as `X-Admin-Token`.

| Endpoint | Purpose |
|---|---|
| `GET /admin/invariants` | Run local correctness invariants |
| `POST /admin/reconcile` | Rebuild derived balance/activity indexes from events |
| `POST /admin/reindex` | Rewind the cursor to `start_height - 1` |

## Downstream Backfill

Consumers that need a complete mirror should read `/cursor` for source
height/hash and then page `/export/events` with `since_event_id`.
Leaving `ticker` unset exports the global event stream in event order;
passing `ticker` restricts the stream to that deployment.

## Architecture

```text
Bitcoin Core --RPC/REST--> dmt-indexer --HTTP/WS--> API consumers
                              |
                           index.redb
```

## License

MIT. See `LICENSE`.

## Acknowledgments

- Bitcoin Core
- The [ord](https://github.com/ordinals/ord) project; the inscription
  envelope parser follows ord-style envelope semantics
- [Trac Systems](https://github.com/Trac-Systems/tap-protocol-specs)
  for TAP protocol specifications
- [Digital Matter Theory](https://digital-matter-theory.gitbook.io/digital-matter-theory)
  for the DMT token model
