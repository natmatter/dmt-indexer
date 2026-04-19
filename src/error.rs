use std::io;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] io::Error),

    #[error("config: {0}")]
    Config(String),

    #[error("bitcoin rpc: {0}")]
    Rpc(String),

    #[error("json decode: {0}")]
    Json(#[from] serde_json::Error),

    #[error("http: {0}")]
    Http(#[from] reqwest::Error),

    #[error("url parse: {0}")]
    Url(#[from] url::ParseError),

    #[error("bitcoin consensus: {0}")]
    BitcoinConsensus(String),

    #[error("hex decode: {0}")]
    Hex(#[from] hex::FromHexError),

    #[error("store: {0}")]
    Store(String),

    #[error("redb: {0}")]
    Redb(String),

    #[error("protocol: {0}")]
    Protocol(String),

    #[error("ledger: {0}")]
    Ledger(String),

    #[error("api: {0}")]
    Api(String),

    #[error("reorg detected at height {height}")]
    Reorg { height: u64 },

    #[error("not implemented: {0}")]
    NotImplemented(&'static str),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<redb::Error> for Error {
    fn from(e: redb::Error) -> Self {
        Error::Redb(e.to_string())
    }
}

impl From<redb::DatabaseError> for Error {
    fn from(e: redb::DatabaseError) -> Self {
        Error::Redb(e.to_string())
    }
}

impl From<redb::TransactionError> for Error {
    fn from(e: redb::TransactionError) -> Self {
        Error::Redb(e.to_string())
    }
}

impl From<redb::TableError> for Error {
    fn from(e: redb::TableError) -> Self {
        Error::Redb(e.to_string())
    }
}

impl From<redb::StorageError> for Error {
    fn from(e: redb::StorageError) -> Self {
        Error::Redb(e.to_string())
    }
}

impl From<redb::CommitError> for Error {
    fn from(e: redb::CommitError) -> Self {
        Error::Redb(e.to_string())
    }
}

impl From<bitcoin::consensus::encode::Error> for Error {
    fn from(e: bitcoin::consensus::encode::Error) -> Self {
        Error::BitcoinConsensus(e.to_string())
    }
}

impl From<toml::de::Error> for Error {
    fn from(e: toml::de::Error) -> Self {
        Error::Config(e.to_string())
    }
}
