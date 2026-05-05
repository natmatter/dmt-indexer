use std::env;

use dmt_indexer::{store::tables, Config, Result};

fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let config_path = args
        .next()
        .expect("usage: set_next_event_id <config.toml> <next_event_id>");
    let next_event_id: u64 = args
        .next()
        .expect("usage: set_next_event_id <config.toml> <next_event_id>")
        .parse()
        .expect("next_event_id must be u64");

    let cfg = Config::from_file(config_path)?;
    let store = dmt_indexer::store::Store::open(&cfg.storage.db_path)?;
    let tx = store.write()?;
    tables::next_event_id_set(&tx, next_event_id)?;
    tx.commit()?;
    println!("set next_event_id={next_event_id}");
    Ok(())
}
