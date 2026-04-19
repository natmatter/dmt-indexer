pub mod codec;
pub mod tables;

use std::path::Path;
use std::sync::Arc;

use redb::{Database, ReadTransaction, WriteTransaction};

use crate::error::Result;

pub use tables::*;

#[derive(Clone)]
pub struct Store {
    db: Arc<Database>,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Database::create(path)?;
        // Initialise all tables so downstream reads cannot fail on
        // a cold DB.
        let tx = db.begin_write()?;
        tables::init_all(&tx)?;
        tx.commit()?;
        Ok(Self { db: Arc::new(db) })
    }

    pub fn read(&self) -> Result<ReadTransaction> {
        Ok(self.db.begin_read()?)
    }

    pub fn write(&self) -> Result<WriteTransaction> {
        Ok(self.db.begin_write()?)
    }

    pub fn raw(&self) -> &Database {
        &self.db
    }
}
