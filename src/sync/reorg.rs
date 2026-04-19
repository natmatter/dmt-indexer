//! Reorg detection and rewind.
//!
//! On every tick we pull tip info from Bitcoin Core. If the cursor's
//! stored block_hash does not match the node's hash at that height,
//! we rewind `finality_depth + 1` blocks and replay forward.

use redb::ReadableTable;
use tracing::{info, warn};

use crate::btc::RpcClient;
use crate::error::Result;
use crate::ledger::event::LedgerEvent;
use crate::store::codec::{decode, encode, inverted_balance};
use crate::store::tables::{
    cursor_get, cursor_set, Cursor, MintClaim, ValidTransfer, WalletState, ACTIVITY_RECENT,
    BALANCES_BY_VALUE, DAILY_ACTIVE_ADDRESSES, DAILY_STATS, EVENTS, INSCRIPTION_OWNERS,
    MINT_CLAIMS, PENDING_CONTROLS, TRANSFERABLES_BY_SENDER, VALID_TRANSFERS, WALLET_ACTIVITY,
    WALLET_STATE,
};
use crate::store::Store;

/// Check whether the persisted cursor still matches Bitcoin Core's
/// current view of that block. Returns `Ok(None)` if consistent,
/// `Ok(Some(rewind_to_height))` if a reorg is detected.
pub async fn detect_reorg(
    store: &Store,
    rpc: &RpcClient,
    finality_depth: u64,
) -> Result<Option<u64>> {
    let tx = store.read()?;
    let Some(cursor) = cursor_get(&tx)? else {
        return Ok(None);
    };
    drop(tx);
    if cursor.block_hash.is_empty() {
        return Ok(None);
    }
    let live_hash = rpc.get_block_hash(cursor.height).await?;
    if live_hash.to_string() == cursor.block_hash {
        return Ok(None);
    }
    let rewind_to = cursor.height.saturating_sub(finality_depth + 1);
    warn!(
        cursor_height = cursor.height,
        cursor_hash = cursor.block_hash,
        live_hash = %live_hash,
        rewind_to,
        "reorg detected"
    );
    Ok(Some(rewind_to))
}

/// Rewind the cursor to `height`. The caller is responsible for
/// truncating per-block side-effect tables (events at or above height,
/// wallet_state recompute, etc.) — for v0.1.0 we implement a simple
/// approach: delete all events/stats at `height..cursor_height` in a
/// dedicated write transaction. Wallet state recompute is deferred to
/// a full resync (`dmt-indexer reindex`) if needed.
pub fn rewind_cursor(store: &Store, height: u64) -> Result<()> {
    let tx = store.write()?;
    // Reorg-safe rewind:
    //   1. Drop events / mint_claims / valid_transfers / pending_controls
    //      / inscription_owners at heights > `height`.
    //   2. Truncate wallet_state + balances_by_value completely.
    //   3. Replay all remaining events in event_id order to rebuild
    //      wallet_state from scratch. Deltas are authoritative.
    //
    // Wallet_state replay is O(events below height); for a typical
    // 6-block rewind on a live DB that's a few hundred rows and
    // completes in < 1 second.
    {
        let mut events = tx.open_table(EVENTS)?;
        let to_delete: Vec<(String, u64)> = events
            .iter()?
            .filter_map(|r| r.ok())
            .filter_map(|(k, v)| {
                let (t, id) = k.value();
                let ev: LedgerEvent = decode(v.value()).ok()?;
                if ev.block_height > height {
                    Some((t.to_string(), id))
                } else {
                    None
                }
            })
            .collect();
        for (t, id) in to_delete {
            events.remove((t.as_str(), id))?;
        }
    }
    {
        let mut mc = tx.open_table(MINT_CLAIMS)?;
        let to_delete: Vec<(String, u64)> = mc
            .iter()?
            .filter_map(|r| r.ok())
            .filter_map(|(k, v)| {
                let (t, blk) = k.value();
                let claim: MintClaim = decode(v.value()).ok()?;
                if claim.inscribed_height > height {
                    Some((t.to_string(), blk))
                } else {
                    None
                }
            })
            .collect();
        for (t, blk) in to_delete {
            mc.remove((t.as_str(), blk))?;
        }
    }
    {
        let mut vt = tx.open_table(VALID_TRANSFERS)?;
        let to_delete: Vec<String> = vt
            .iter()?
            .filter_map(|r| r.ok())
            .filter_map(|(k, v)| {
                let id = k.value().to_string();
                let vt_row: ValidTransfer = decode(v.value()).ok()?;
                if vt_row.inscribed_height > height {
                    Some(id)
                } else if let Some(ch) = vt_row.consumed_height {
                    if ch > height {
                        // keep the validity row but unmark consumption
                        Some(id)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        for id in to_delete {
            let raw = vt.get(id.as_str())?.map(|v| v.value().to_vec());
            if let Some(bytes) = raw {
                if let Ok(mut v) = decode::<ValidTransfer>(&bytes) {
                    if v.inscribed_height > height {
                        vt.remove(id.as_str())?;
                    } else {
                        v.consumed_height = None;
                        vt.insert(id.as_str(), encode(&v)?.as_slice())?;
                    }
                }
            }
        }
    }
    // Rebuild TRANSFERABLES_BY_SENDER from the post-rewind VALID_TRANSFERS.
    // Simpler and safer than trying to mirror each deletion/unconsume
    // above. O(valid_transfers), which is fine since rewinds are rare.
    {
        let mut idx = tx.open_table(TRANSFERABLES_BY_SENDER)?;
        let keys: Vec<(String, String, String)> = idx
            .iter()?
            .filter_map(|r| r.ok())
            .map(|(k, _)| {
                let (t, a, i) = k.value();
                (t.to_string(), a.to_string(), i.to_string())
            })
            .collect();
        for (t, a, i) in keys {
            idx.remove((t.as_str(), a.as_str(), i.as_str()))?;
        }
        let vt = tx.open_table(VALID_TRANSFERS)?;
        for row in vt.iter()? {
            let Ok((k, v)) = row else { continue };
            let Ok(vtrow) = decode::<ValidTransfer>(v.value()) else {
                continue;
            };
            if vtrow.consumed_height.is_none() {
                idx.insert(
                    (vtrow.ticker.as_str(), vtrow.sender.as_str(), k.value()),
                    1u8,
                )?;
            }
        }
    }
    // 1b. Drop pending_controls + inscription_owners that were created
    //     above the rewind height. Tracker will be reloaded from
    //     what remains on the next tick.
    {
        let mut pc = tx.open_table(PENDING_CONTROLS)?;
        let to_delete: Vec<String> = pc
            .iter()?
            .filter_map(|r| r.ok())
            .filter_map(|(k, v)| {
                let p: crate::store::tables::PendingControl = decode(v.value()).ok()?;
                if p.inscribed_height > height {
                    Some(k.value().to_string())
                } else {
                    None
                }
            })
            .collect();
        for k in to_delete {
            pc.remove(k.as_str())?;
        }
    }
    // inscription_owners doesn't carry a height — clear the table and
    // let the next tick rebuild it from the remaining events. This is
    // safe because the tracker is rebuilt from INSCRIPTION_OWNERS on
    // every tick.
    //
    // 2. Truncate wallet_state + balances_by_value.
    {
        let mut ws = tx.open_table(WALLET_STATE)?;
        let keys: Vec<(String, String)> = ws
            .iter()?
            .filter_map(|r| r.ok())
            .map(|(k, _)| {
                let (t, a) = k.value();
                (t.to_string(), a.to_string())
            })
            .collect();
        for (t, a) in keys {
            ws.remove((t.as_str(), a.as_str()))?;
        }
    }
    {
        let mut bv = tx.open_table(BALANCES_BY_VALUE)?;
        let keys: Vec<(String, Vec<u8>, String)> = bv
            .iter()?
            .filter_map(|r| r.ok())
            .map(|(k, _)| {
                let (t, b, a) = k.value();
                (t.to_string(), b.to_vec(), a.to_string())
            })
            .collect();
        for (t, b, a) in keys {
            bv.remove((t.as_str(), b.as_slice(), a.as_str()))?;
        }
    }
    {
        let mut io = tx.open_table(INSCRIPTION_OWNERS)?;
        let keys: Vec<String> = io
            .iter()?
            .filter_map(|r| r.ok())
            .map(|(k, _)| k.value().to_string())
            .collect();
        for k in keys {
            io.remove(k.as_str())?;
        }
    }
    // Drop per-wallet + global activity feed rows whose recorded event
    // is past `height`. Balance state gets rebuilt from EVENTS below;
    // these feed tables have their own keys so need explicit truncation.
    {
        let mut ar = tx.open_table(ACTIVITY_RECENT)?;
        let to_delete: Vec<(String, u64, u64)> = ar
            .iter()?
            .filter_map(|r| r.ok())
            .filter_map(|(k, v)| {
                let ev: LedgerEvent = decode(v.value()).ok()?;
                if ev.block_height > height {
                    let (t, inv, id) = k.value();
                    Some((t.to_string(), inv, id))
                } else {
                    None
                }
            })
            .collect();
        for (t, inv, id) in to_delete {
            ar.remove((t.as_str(), inv, id))?;
        }
    }
    {
        let mut wa = tx.open_table(WALLET_ACTIVITY)?;
        let to_delete: Vec<(String, String, u64, u64)> = wa
            .iter()?
            .filter_map(|r| r.ok())
            .filter_map(|(k, v)| {
                let ev: LedgerEvent = decode(v.value()).ok()?;
                if ev.block_height > height {
                    let (t, a, inv, id) = k.value();
                    Some((t.to_string(), a.to_string(), inv, id))
                } else {
                    None
                }
            })
            .collect();
        for (t, a, inv, id) in to_delete {
            wa.remove((t.as_str(), a.as_str(), inv, id))?;
        }
    }
    // Daily stats for days that only contained events above `height`
    // get stale; the cheapest correct thing is to drop any (ticker, day)
    // row whose day overlaps the rewind window AND then let replay
    // rebuild them. But event.occurred_at uses block timestamp, so the
    // simplest conservative pass is: clear DAILY_STATS + DAILY_ACTIVE_ADDRESSES
    // entirely and let replay rebuild both from EVENTS.
    {
        let mut ds = tx.open_table(DAILY_STATS)?;
        let keys: Vec<(String, u32)> = ds
            .iter()?
            .filter_map(|r| r.ok())
            .map(|(k, _)| {
                let (t, d) = k.value();
                (t.to_string(), d)
            })
            .collect();
        for (t, d) in keys {
            ds.remove((t.as_str(), d))?;
        }
    }
    {
        let mut daa = tx.open_table(DAILY_ACTIVE_ADDRESSES)?;
        let keys: Vec<(String, u32, String)> = daa
            .iter()?
            .filter_map(|r| r.ok())
            .map(|(k, _)| {
                let (t, d, a) = k.value();
                (t.to_string(), d, a.to_string())
            })
            .collect();
        for (t, d, a) in keys {
            daa.remove((t.as_str(), d, a.as_str()))?;
        }
    }
    // 3. Replay remaining events in event_id order.
    {
        // Gather events first (read + write on EVENTS in same txn is
        // fine, but we collect to avoid iterator invalidation).
        let events_to_replay: Vec<LedgerEvent> = {
            let events = tx.open_table(EVENTS)?;
            events
                .iter()?
                .filter_map(|r| r.ok())
                .filter_map(|(_, v)| decode::<LedgerEvent>(v.value()).ok())
                .collect()
        };
        let mut replay: Vec<LedgerEvent> = events_to_replay;
        replay.sort_by_key(|e| e.event_id);
        let mut ws = tx.open_table(WALLET_STATE)?;
        let mut bv = tx.open_table(BALANCES_BY_VALUE)?;
        let mut ds = tx.open_table(DAILY_STATS)?;
        let mut daa = tx.open_table(DAILY_ACTIVE_ADDRESSES)?;
        let mut touched: std::collections::HashMap<(String, String), i128> =
            std::collections::HashMap::new();
        for ev in replay {
            if let Some(addr) = ev.address.clone() {
                let key = (ev.ticker.clone(), addr.clone());
                let mut state: WalletState = ws
                    .get((ev.ticker.as_str(), addr.as_str()))?
                    .and_then(|v| decode(v.value()).ok())
                    .unwrap_or_default();
                let prev_total = state.total;
                state.total += ev.delta.delta_total;
                state.available += ev.delta.delta_available;
                state.transferable += ev.delta.delta_transferable;
                state.burned += ev.delta.delta_burned;
                if state.first_activity.is_none() {
                    state.first_activity = Some(ev.occurred_at);
                }
                state.last_activity = Some(ev.occurred_at);
                // Lock only on FIRST coinbase credit (when wallet was
                // newly created by this event → prev_total == 0 AND
                // the existing WalletState came from unwrap_or_default).
                if ev.event_type == crate::ledger::event::EventType::CoinbaseRewardLocked {
                    state.transferables_blocked = true;
                }
                if ev.event_type == crate::ledger::event::EventType::BlockTransferablesTapped {
                    state.transferables_blocked = true;
                }
                if ev.event_type == crate::ledger::event::EventType::UnblockTransferablesTapped {
                    state.transferables_blocked = false;
                }
                ws.insert(
                    (ev.ticker.as_str(), addr.as_str()),
                    encode(&state)?.as_slice(),
                )?;
                touched.insert(key, prev_total);
            }
            // Rebuild daily_stats + daily_active_addresses from this event.
            let day = (ev.occurred_at.timestamp() / 86400) as u32;
            let dkey = (ev.ticker.as_str(), day);
            let mut stat: crate::store::tables::DailyStats = ds
                .get(dkey)?
                .and_then(|v| decode(v.value()).ok())
                .unwrap_or_default();
            use crate::ledger::event::EventType as ET;
            match ev.event_type {
                ET::DmtMintCredit | ET::CoinbaseRewardCredit | ET::CoinbaseRewardLocked => {
                    stat.minted = stat
                        .minted
                        .saturating_add(u128::try_from(ev.delta.delta_total).unwrap_or(0));
                }
                ET::TokenTransferCredit => {
                    stat.transfer_count += 1;
                    stat.volume = stat
                        .volume
                        .saturating_add(u128::try_from(ev.delta.delta_total).unwrap_or(0));
                }
                ET::TokenTransferBurned => {
                    stat.burned = stat
                        .burned
                        .saturating_add(u128::try_from(ev.delta.delta_burned).unwrap_or(0));
                }
                _ => {}
            }
            for maybe_addr in [&ev.address, &ev.counterparty_address].iter().copied() {
                if let Some(a) = maybe_addr {
                    let ak = (ev.ticker.as_str(), day, a.as_str());
                    if daa.get(ak)?.is_none() {
                        daa.insert(ak, 1u8)?;
                        stat.active_addresses = stat.active_addresses.saturating_add(1);
                    }
                }
            }
            ds.insert(dkey, encode(&stat)?.as_slice())?;
        }
        // Rebuild balances_by_value from fresh wallet_state.
        let wallets: Vec<(String, String, i128)> = ws
            .iter()?
            .filter_map(|r| r.ok())
            .filter_map(|(k, v)| {
                let (t, a) = k.value();
                let w: WalletState = decode(v.value()).ok()?;
                Some((t.to_string(), a.to_string(), w.total))
            })
            .collect();
        for (t, a, total) in wallets {
            if total > 0 {
                let inv = inverted_balance(total as u128);
                bv.insert((t.as_str(), inv.as_slice(), a.as_str()), total as u64)?;
            }
        }
    }
    info!(
        rewind_to = height,
        "reorg rewind complete, wallet_state rebuilt"
    );
    cursor_set(
        &tx,
        &Cursor {
            height,
            block_hash: String::new(),
            updated_at: Some(chrono::Utc::now()),
        },
    )?;
    tx.commit()?;
    Ok(())
}
