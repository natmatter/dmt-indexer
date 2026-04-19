//! Conformance tool implementation.
//!
//! - `export_snapshot` writes a deterministic JSON of {deployments, wallet_state, mint_claims}
//!   suitable for CI diffing.
//! - `verify_against_snapshot` diffs a local store against a committed snapshot.
//! - `verify_against_postgres` diffs against a reference `nat-backend` DB
//!   (read-only). Only available when built with `--features verify-postgres`.

use std::collections::BTreeMap;
use std::path::Path;

use redb::ReadableTable;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::ledger::deploy::Deployment;
use crate::store::codec::decode;
use crate::store::tables::{MintClaim, WalletState, DEPLOYMENTS, MINT_CLAIMS, WALLET_STATE};
use crate::store::Store;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub deployments: BTreeMap<String, Deployment>,
    pub wallet_state: BTreeMap<String, WalletState>, // key = ticker::address
    pub mint_claims: BTreeMap<String, MintClaim>,    // key = ticker:blk
}

/// Run invariant self-checks against the local DB. Fails loudly on
/// any violation. Matches the 6 invariants listed in
/// IMPLEMENTATION_PLAN "Lessons from nat-backend":
///
/// 1. `total = available + transferable` for every wallet row
/// 2. no negative balance in any column
/// 3. `count(dmt_mint_credit events) == count(mint_claims)`
/// 4. `count(token_transfer_debit) == count(valid_transfers with
///    consumed_height) == count(token_transfer_credit)` (burn paths
///    replace credit with burned; the three stay linked)
/// 5. `sum(wallet_state.total) == sum(credit_deltas) - sum(debit_deltas)`
///    across the events table (debits and credits both carry
///    delta_total; sum should equal current balances)
/// 6. `count(wallet_state.total > 0) == count(balances_by_value rows)`
pub fn check_invariants(store: &Store) -> Result<Vec<String>> {
    use crate::ledger::event::{EventType, LedgerEvent};
    use crate::store::tables::{
        ValidTransfer as VTRow, WalletState, BALANCES_BY_VALUE, EVENTS, MINT_CLAIMS,
        VALID_TRANSFERS, WALLET_STATE,
    };
    let rtx = store.read()?;
    let mut diffs = Vec::new();

    // Collect wallet_state
    let mut wallet_sum: i128 = 0;
    let mut wallet_count_positive: usize = 0;
    {
        let t = rtx.open_table(WALLET_STATE)?;
        for r in t.iter()? {
            let (k, v) = r?;
            let (ticker, addr) = k.value();
            let w: WalletState = decode(v.value())?;
            if w.total != w.available + w.transferable {
                diffs.push(format!(
                    "inv1 {}/{}: total={} != available {} + transferable {}",
                    ticker, addr, w.total, w.available, w.transferable
                ));
            }
            if w.total < 0 || w.available < 0 || w.transferable < 0 || w.burned < 0 {
                diffs.push(format!(
                    "inv2 {}/{}: negative column total={} avail={} xfer={} burned={}",
                    ticker, addr, w.total, w.available, w.transferable, w.burned
                ));
            }
            wallet_sum += w.total;
            if w.total > 0 {
                wallet_count_positive += 1;
            }
        }
    }

    // Events counters
    let mut mint_credits = 0usize;
    let mut transfer_debits = 0usize;
    let mut transfer_credits = 0usize;
    let mut transfer_burns = 0usize;
    let mut total_delta: i128 = 0;
    {
        let t = rtx.open_table(EVENTS)?;
        for r in t.iter()? {
            let (_, v) = r?;
            let ev: LedgerEvent = decode(v.value())?;
            total_delta += ev.delta.delta_total;
            match ev.event_type {
                EventType::DmtMintCredit => mint_credits += 1,
                EventType::TokenTransferDebit => transfer_debits += 1,
                EventType::TokenTransferCredit => transfer_credits += 1,
                EventType::TokenTransferBurned => transfer_burns += 1,
                _ => {}
            }
        }
    }

    let mut mint_claims_count = 0usize;
    {
        let t = rtx.open_table(MINT_CLAIMS)?;
        for r in t.iter()? {
            let _ = r?;
            mint_claims_count += 1;
        }
    }
    if mint_credits != mint_claims_count {
        diffs.push(format!(
            "inv3 dmt_mint_credit events={} != mint_claims={}",
            mint_credits, mint_claims_count
        ));
    }

    let mut consumed_valid_transfers = 0usize;
    {
        let t = rtx.open_table(VALID_TRANSFERS)?;
        for r in t.iter()? {
            let (_, v) = r?;
            let vt: VTRow = decode(v.value())?;
            if vt.consumed_height.is_some() {
                consumed_valid_transfers += 1;
            }
        }
    }
    if transfer_debits != consumed_valid_transfers {
        diffs.push(format!(
            "inv4a token_transfer_debit={} != consumed valid_transfers={}",
            transfer_debits, consumed_valid_transfers
        ));
    }
    if transfer_debits != transfer_credits + transfer_burns {
        diffs.push(format!(
            "inv4b debits={} != credits+burns={}+{}",
            transfer_debits, transfer_credits, transfer_burns
        ));
    }

    if total_delta != wallet_sum {
        diffs.push(format!(
            "inv5 sum(event.delta_total)={} != sum(wallet_state.total)={}",
            total_delta, wallet_sum
        ));
    }

    let mut bbv_count = 0usize;
    {
        let t = rtx.open_table(BALANCES_BY_VALUE)?;
        for r in t.iter()? {
            let _ = r?;
            bbv_count += 1;
        }
    }
    if wallet_count_positive != bbv_count {
        diffs.push(format!(
            "inv6 wallet_state with total>0 ={} != balances_by_value rows ={}",
            wallet_count_positive, bbv_count
        ));
    }

    Ok(diffs)
}

pub fn export_snapshot(store: &Store, out: &Path) -> Result<()> {
    let rtx = store.read()?;
    let mut snap = Snapshot {
        deployments: BTreeMap::new(),
        wallet_state: BTreeMap::new(),
        mint_claims: BTreeMap::new(),
    };
    {
        let table = rtx.open_table(DEPLOYMENTS)?;
        for row in table.iter()? {
            let (k, v) = row?;
            if let Ok(d) = decode::<Deployment>(v.value()) {
                snap.deployments.insert(k.value().to_string(), d);
            }
        }
    }
    {
        let table = rtx.open_table(WALLET_STATE)?;
        for row in table.iter()? {
            let (k, v) = row?;
            let (t, a) = k.value();
            if let Ok(w) = decode::<WalletState>(v.value()) {
                if w.total == 0 && w.available == 0 && w.transferable == 0 {
                    continue;
                }
                snap.wallet_state.insert(format!("{}::{}", t, a), w);
            }
        }
    }
    {
        let table = rtx.open_table(MINT_CLAIMS)?;
        for row in table.iter()? {
            let (k, v) = row?;
            let (t, b) = k.value();
            if let Ok(m) = decode::<MintClaim>(v.value()) {
                snap.mint_claims.insert(format!("{}:{}", t, b), m);
            }
        }
    }
    std::fs::write(out, serde_json::to_vec_pretty(&snap)?)?;
    Ok(())
}

pub fn verify_against_snapshot(store: &Store, path: &Path) -> Result<Vec<String>> {
    let bytes = std::fs::read(path)?;
    let expect: Snapshot = serde_json::from_slice(&bytes)?;
    let tmp = tempfile::NamedTempFile::new()?;
    export_snapshot(store, tmp.path())?;
    let actual: Snapshot = serde_json::from_slice(&std::fs::read(tmp.path())?)?;
    let mut diffs = Vec::new();
    for (k, v) in &expect.deployments {
        match actual.deployments.get(k) {
            Some(av) if av != v => diffs.push(format!("deployment {k} differs")),
            None => diffs.push(format!("deployment {k} missing")),
            _ => {}
        }
    }
    for (k, v) in &expect.wallet_state {
        match actual.wallet_state.get(k) {
            Some(av) => {
                if av.total != v.total
                    || av.available != v.available
                    || av.transferable != v.transferable
                {
                    diffs.push(format!("wallet {k} balance differs"));
                }
            }
            None => diffs.push(format!("wallet {k} missing")),
        }
    }
    for (k, v) in &expect.mint_claims {
        match actual.mint_claims.get(k) {
            Some(av) if av.winning_inscription_id != v.winning_inscription_id => {
                diffs.push(format!("mint claim {k} winner differs"));
            }
            None => diffs.push(format!("mint claim {k} missing")),
            _ => {}
        }
    }
    Ok(diffs)
}

#[cfg(feature = "verify-postgres")]
pub async fn verify_against_postgres(store: &Store, url: &str) -> Result<Vec<String>> {
    use sqlx::postgres::PgPoolOptions;
    use sqlx::Row;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(url)
        .await
        .map_err(|e| crate::error::Error::Other(anyhow::anyhow!("pg: {e}")))?;
    let rtx = store.read()?;
    let mut diffs = Vec::new();

    // Compare per-address NAT balance between redb and Postgres.
    let pg_rows: Vec<(String, i64, i64, i64)> = sqlx::query(
        "SELECT address, \
                COALESCE(total_balance,0)::bigint, \
                COALESCE(available_balance,0)::bigint, \
                COALESCE(transferable_balance,0)::bigint \
         FROM wallet_balances \
         WHERE deployment_id = (SELECT id FROM deployments WHERE ticker='nat') \
         AND total_balance > 0",
    )
    .fetch_all(&pool)
    .await
    .map_err(|e| crate::error::Error::Other(anyhow::anyhow!("pg query: {e}")))?
    .into_iter()
    .map(|r| {
        (
            r.get::<String, _>(0),
            r.get::<i64, _>(1),
            r.get::<i64, _>(2),
            r.get::<i64, _>(3),
        )
    })
    .collect();

    let table = rtx.open_table(WALLET_STATE)?;
    let mut local: std::collections::HashMap<String, (i128, i128, i128)> =
        std::collections::HashMap::new();
    for row in table.iter()? {
        let (k, v) = row?;
        let (t, a) = k.value();
        if t != "nat" {
            continue;
        }
        if let Ok(ws) = decode::<WalletState>(v.value()) {
            local.insert(a.to_string(), (ws.total, ws.available, ws.transferable));
        }
    }
    for (addr, total, avail, xfer) in pg_rows {
        match local.get(&addr) {
            Some(&(lt, la, lx))
                if lt == i128::from(total) && la == i128::from(avail) && lx == i128::from(xfer) => {
            }
            Some(&(lt, la, lx)) => diffs.push(format!(
                "balance {addr} pg=({total}/{avail}/{xfer}) local=({lt}/{la}/{lx})"
            )),
            None => diffs.push(format!("balance {addr} missing in local")),
        }
    }
    Ok(diffs)
}
