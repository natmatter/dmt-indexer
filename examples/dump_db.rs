//! Deep-dump every table in the redb for analysis.
//!
//! Usage: cargo run --example dump_db -- /var/lib/dmt-indexer/index.redb

use std::env;

use redb::{Database, ReadableTable};

use dmt_indexer::ledger::deploy::Deployment;
use dmt_indexer::ledger::event::LedgerEvent;
use dmt_indexer::store::codec::decode;
use dmt_indexer::store::tables::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::args().nth(1).expect("usage: dump_db <path>");
    let db = Database::open(&path)?;
    let rtx = db.begin_read()?;

    println!("== meta ==");
    {
        let t = rtx.open_table(META)?;
        for r in t.iter()? {
            let (k, v) = r?;
            let key = k.value();
            // Try to decode as cursor, then stats, then leave as bytes
            if let Ok(c) = decode::<Cursor>(v.value()) {
                println!(
                    "{} => height={} hash={} updated_at={:?}",
                    key, c.height, c.block_hash, c.updated_at
                );
            } else {
                let s = std::str::from_utf8(v.value()).unwrap_or("<bin>");
                println!("{} => {}", key, s);
            }
        }
    }

    println!("\n== stats ==");
    {
        let t = rtx.open_table(STATS)?;
        for r in t.iter()? {
            let (k, v) = r?;
            let body: serde_json::Value = decode(v.value()).unwrap_or(serde_json::json!(null));
            println!("{}: {}", k.value(), serde_json::to_string_pretty(&body)?);
        }
    }

    println!("\n== deployments ==");
    {
        let t = rtx.open_table(DEPLOYMENTS)?;
        for r in t.iter()? {
            let (k, v) = r?;
            let d: Deployment = decode(v.value())?;
            println!(
                "{} => deploy={} elem={} mode={:?} field={:?} cb_act={:?} lock_act={:?}",
                k.value(),
                d.deploy_inscription_id,
                d.element_inscription_id,
                d.bits_mode,
                d.element_field,
                d.coinbase_activation,
                d.miner_transfer_activation
            );
        }
    }

    println!("\n== wallet_state ==");
    let mut total_totals: i128 = 0;
    let mut row_count = 0usize;
    {
        let t = rtx.open_table(WALLET_STATE)?;
        for r in t.iter()? {
            let (k, v) = r?;
            let (ticker, address) = k.value();
            let w: WalletState = decode(v.value())?;
            row_count += 1;
            total_totals += w.total;
            println!(
                "  {:>3}/{:<62} total={:>12} available={:>12} xfer={:>12} burned={:>4} locked={}",
                ticker,
                address,
                w.total,
                w.available,
                w.transferable,
                w.burned,
                w.transferables_blocked
            );
        }
    }
    println!("  (rows={} sum_total={})", row_count, total_totals);

    println!("\n== mint_claims ==");
    {
        let t = rtx.open_table(MINT_CLAIMS)?;
        let mut n = 0;
        for r in t.iter()? {
            let (k, v) = r?;
            let (ticker, blk) = k.value();
            let m: MintClaim = decode(v.value())?;
            println!(
                "  {}/{} => winner={} amount={} inscribed_height={}",
                ticker, blk, m.winning_inscription_id, m.amount, m.inscribed_height
            );
            n += 1;
        }
        println!("  ({} rows)", n);
    }

    println!("\n== valid_transfers ==");
    {
        let t = rtx.open_table(VALID_TRANSFERS)?;
        let mut n = 0;
        for r in t.iter()? {
            let (k, v) = r?;
            let vt: ValidTransfer = decode(v.value())?;
            println!(
                "  {} => ticker={} sender={} amount={} inscribed_h={} consumed_h={:?}",
                k.value(),
                vt.ticker,
                vt.sender,
                vt.amount,
                vt.inscribed_height,
                vt.consumed_height
            );
            n += 1;
        }
        println!("  ({} rows)", n);
    }

    println!("\n== pending_controls ==");
    {
        let t = rtx.open_table(PENDING_CONTROLS)?;
        let mut n = 0;
        for r in t.iter()? {
            let (k, v) = r?;
            let p: PendingControl = decode(v.value())?;
            println!(
                "  {} => ticker={} address={} op={} inscribed_h={}",
                k.value(),
                p.ticker,
                p.address,
                p.op,
                p.inscribed_height
            );
            n += 1;
        }
        println!("  ({} rows)", n);
    }

    println!("\n== inscription_owners ==");
    {
        let t = rtx.open_table(INSCRIPTION_OWNERS)?;
        let mut n = 0;
        for r in t.iter()? {
            let (k, v) = r?;
            let o: InscriptionOwner = decode(v.value())?;
            println!(
                "  {} => insc={} ticker={} kind={}",
                k.value(),
                o.inscription_id,
                o.ticker,
                o.kind
            );
            n += 1;
        }
        println!("  ({} rows)", n);
    }

    println!("\n== inscriptions ==");
    {
        let t = rtx.open_table(INSCRIPTIONS)?;
        let mut n = 0;
        for r in t.iter()? {
            let (k, v) = r?;
            let ix: InscriptionIndex = decode(v.value())?;
            println!(
                "  {} => ticker={} kind={} amt={:?} h={} owner={:?} consumed={:?}",
                k.value(),
                ix.ticker,
                ix.kind,
                ix.original_amount,
                ix.inscribed_height,
                ix.current_owner_address,
                ix.consumed_height
            );
            n += 1;
        }
        println!("  ({} rows)", n);
    }

    println!("\n== daily_stats ==");
    {
        let t = rtx.open_table(DAILY_STATS)?;
        for r in t.iter()? {
            let (k, v) = r?;
            let (ticker, day) = k.value();
            let d: DailyStats = decode(v.value())?;
            println!(
                "  {}/{} => mints={} xfer_count={} vol={} burned={} active={}",
                ticker, day, d.minted, d.transfer_count, d.volume, d.burned, d.active_addresses
            );
        }
    }

    println!("\n== event_key uniqueness ==");
    {
        let t = rtx.open_table(EVENTS)?;
        let mut seen = std::collections::HashSet::new();
        let mut dupes = 0;
        for r in t.iter()? {
            let (_, v) = r?;
            let ev: LedgerEvent = decode(v.value())?;
            if !seen.insert(ev.event_key.clone()) {
                dupes += 1;
                println!("  DUPE: {}", ev.event_key);
            }
        }
        println!("  unique_keys={} dupes={}", seen.len(), dupes);
    }

    println!("\n== events (first 20) ==");
    {
        let t = rtx.open_table(EVENTS)?;
        let mut n = 0;
        let mut max_event_id = 0u64;
        let mut count_by_type = std::collections::HashMap::<String, usize>::new();
        for r in t.iter()? {
            let (k, v) = r?;
            let (ticker, eid) = k.value();
            max_event_id = max_event_id.max(eid);
            let ev: LedgerEvent = decode(v.value())?;
            *count_by_type
                .entry(ev.event_type.as_str().to_string())
                .or_default() += 1;
            if n < 20 {
                println!(
                    "  [{:>2}] {} h={} type={} addr={:?} delta_total={}",
                    eid,
                    ticker,
                    ev.block_height,
                    ev.event_type.as_str(),
                    ev.address,
                    ev.delta.delta_total
                );
            }
            n += 1;
        }
        println!("  ({} total rows; max_event_id={})", n, max_event_id);
        println!("  count_by_type:");
        let mut counts: Vec<_> = count_by_type.into_iter().collect();
        counts.sort();
        for (k, v) in counts {
            println!("    {:<40} {}", k, v);
        }
    }

    println!("\n== activity_recent sample ==");
    {
        let t = rtx.open_table(ACTIVITY_RECENT)?;
        let mut n = 0;
        for r in t.iter()? {
            let (k, v) = r?;
            let (ticker, _inv, eid) = k.value();
            let ev: LedgerEvent = decode(v.value())?;
            if n < 5 {
                println!(
                    "  {}/{}: h={} type={}",
                    ticker,
                    eid,
                    ev.block_height,
                    ev.event_type.as_str()
                );
            }
            n += 1;
        }
        println!("  ({} total rows)", n);
    }

    println!("\n== wallet_activity sample ==");
    {
        let t = rtx.open_table(WALLET_ACTIVITY)?;
        let mut n = 0;
        for _ in t.iter()? {
            n += 1;
        }
        println!("  ({} rows)", n);
    }

    println!("\n== balances_by_value ==");
    {
        let t = rtx.open_table(BALANCES_BY_VALUE)?;
        let mut n = 0;
        for _ in t.iter()? {
            n += 1;
        }
        println!("  ({} rows)", n);
    }
    Ok(())
}
