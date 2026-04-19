use dmt_indexer::ledger::deploy::Deployment;
use dmt_indexer::store::codec::decode;
use dmt_indexer::store::tables::*;
use dmt_indexer::store::Store;
use redb::ReadableTable;
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::args().nth(1).expect("db path");
    let store = Store::open(&path)?;
    let rtx = store.read()?;

    println!("=== cursor ===");
    let cur = cursor_get(&rtx)?.unwrap_or_default();
    println!("  height: {}", cur.height);
    println!("  hash:   {}", cur.block_hash);

    println!("\n=== table row counts ===");
    macro_rules! cnt {
        ($def:expr) => {
            rtx.open_table($def)
                .map(|t| {
                    let mut n = 0u64;
                    for _ in t.iter().unwrap() {
                        n += 1
                    }
                    n
                })
                .unwrap_or(0)
        };
    }
    println!("  meta                {}", cnt!(META));
    println!("  deployments         {}", cnt!(DEPLOYMENTS));
    println!("  events              {}", cnt!(EVENTS));
    println!("  wallet_state        {}", cnt!(WALLET_STATE));
    println!("  balances_by_value   {}", cnt!(BALANCES_BY_VALUE));
    println!("  mint_claims         {}", cnt!(MINT_CLAIMS));
    println!("  valid_transfers     {}", cnt!(VALID_TRANSFERS));
    println!("  pending_controls    {}", cnt!(PENDING_CONTROLS));
    println!("  inscription_owners  {}", cnt!(INSCRIPTION_OWNERS));
    println!("  daily_stats         {}", cnt!(DAILY_STATS));
    println!("  activity_recent     {}", cnt!(ACTIVITY_RECENT));
    println!("  wallet_activity     {}", cnt!(WALLET_ACTIVITY));
    println!("  inscriptions        {}", cnt!(INSCRIPTIONS));
    println!("  stats               {}", cnt!(STATS));

    println!("\n=== deployment rows ===");
    let t = rtx.open_table(DEPLOYMENTS)?;
    for r in t.iter()? {
        let (k, v) = r?;
        let d: Deployment = decode(v.value())?;
        println!("  {}: bits_mode={} elem={:?} dt={:?} activation={} coinbase_act={:?} miner_tx_act={:?}",
            k.value(), d.bits_mode.as_str(), d.element_field, d.dt,
            d.activation_height, d.coinbase_activation, d.miner_transfer_activation);
    }

    drop(rtx);
    println!("\n=== invariants ===");
    let violations = dmt_indexer::verify::check_invariants(&store)?;
    if violations.is_empty() {
        println!("  all invariants OK");
    } else {
        for v in &violations {
            println!("  VIOLATION: {}", v);
        }
    }

    Ok(())
}
