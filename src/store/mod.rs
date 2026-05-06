pub mod codec;
pub mod tables;

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Once};

use redb::{Database, ReadTransaction, RepairSession, WriteTransaction};
use tracing::{info, warn};

use crate::error::Result;

pub use tables::*;

#[derive(Clone)]
pub struct Store {
    db: Arc<Database>,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let existed = path.exists();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let repair_path = path.clone();
        let once = Once::new();
        let last_progress = AtomicU64::new(u64::MAX);
        let repair_callback = move |progress: &mut RepairSession| {
            once.call_once(|| {
                warn!(
                    path = %repair_path.display(),
                    "index database needs redb recovery; startup may take a long time"
                );
            });

            let bucket = ((progress.progress() * 100.0).floor() as u64 / 10) * 10;
            if bucket <= 100 && last_progress.swap(bucket, Ordering::Relaxed) != bucket {
                info!(
                    path = %repair_path.display(),
                    progress_pct = bucket,
                    "index database recovery progress"
                );
            }
        };
        let db = Database::builder()
            .set_repair_callback(repair_callback)
            .create(&path)?;
        // Initialise all tables on a cold DB. On an existing large redb,
        // opening every table in a write transaction can dominate startup;
        // normal read paths already handle missing optional indexes.
        if !existed {
            let mut tx = db.begin_write()?;
            tx.set_quick_repair(true);
            tables::init_all(&tx)?;
            tx.commit()?;
        }
        Ok(Self { db: Arc::new(db) })
    }

    pub fn read(&self) -> Result<ReadTransaction> {
        Ok(self.db.begin_read()?)
    }

    pub fn write(&self) -> Result<WriteTransaction> {
        let mut tx = self.db.begin_write()?;
        tx.set_quick_repair(true);
        Ok(tx)
    }

    pub fn raw(&self) -> &Database {
        &self.db
    }
}
