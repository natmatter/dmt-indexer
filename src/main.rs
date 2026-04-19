use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use tokio::sync::broadcast;
use tracing::info;
use tracing_subscriber::EnvFilter;

use dmt_indexer::api;
use dmt_indexer::btc::RpcClient;
use dmt_indexer::config::Config;
use dmt_indexer::error::Result;
use dmt_indexer::store::tables::cursor_get;
use dmt_indexer::store::Store;
use dmt_indexer::sync::Syncer;

#[derive(Parser)]
#[command(name = "dmt-indexer")]
#[command(about = "Standalone DMT/TAP indexer for Bitcoin")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run sync + API in one process
    Run {
        #[arg(long)]
        config: PathBuf,
    },
    /// Run sync only
    Sync {
        #[arg(long)]
        config: PathBuf,
    },
    /// Run API only (expects the DB to exist)
    Serve {
        #[arg(long)]
        config: PathBuf,
    },
    /// Print current cursor + deployments
    Status {
        #[arg(long)]
        config: PathBuf,
    },
    /// Rewind cursor to activation height (destructive)
    Reindex {
        #[arg(long)]
        config: PathBuf,
    },
    /// Export a JSON snapshot of wallet_state + balances
    ExportSnapshot {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        out: PathBuf,
    },
    /// Conformance diff
    Verify {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        snapshot: Option<PathBuf>,
        #[arg(long)]
        against_postgres: Option<String>,
        /// Run balance / event-count invariant self-checks against the
        /// local DB (no external source required).
        #[arg(long)]
        invariants: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    match cli.command {
        Command::Run { config } => {
            let cfg = Config::from_file(&config)?;
            let store = Store::open(&cfg.storage.db_path)?;
            let rpc = RpcClient::from_config(&cfg.bitcoin)?;
            let (tx, _rx) = broadcast::channel(1024);
            let bus = Arc::new(tx);
            let syncer = Syncer::new(cfg.clone(), store.clone(), rpc, bus.clone());
            let handle = syncer.handle();
            let router = api::build_router(&cfg, handle.clone())?;
            let addr = cfg.api.bind_addr;
            let listener = tokio::net::TcpListener::bind(addr).await?;
            info!(%addr, "api listening");
            let api_task = tokio::spawn(async move {
                axum::serve(
                    listener,
                    router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
                )
                .await
                .map_err(|e| anyhow::anyhow!("api server: {e}"))
            });
            let sync_task = tokio::spawn(async move { syncer.run().await });
            let result: Result<()> = tokio::select! {
                r = sync_task => {
                    info!("sync exited");
                    match r {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(e)) => Err(e),
                        Err(e) => Err(dmt_indexer::error::Error::Other(anyhow::anyhow!("sync join: {e}"))),
                    }
                }
                r = api_task => {
                    info!("api exited");
                    match r {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(e)) => Err(dmt_indexer::error::Error::Other(e)),
                        Err(e) => Err(dmt_indexer::error::Error::Other(anyhow::anyhow!("api join: {e}"))),
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("shutdown signal");
                    Ok(())
                }
            };
            if let Err(ref e) = result {
                eprintln!("error: {e:?}");
            }
            result
        }
        Command::Sync { config } => {
            let cfg = Config::from_file(&config)?;
            let store = Store::open(&cfg.storage.db_path)?;
            let rpc = RpcClient::from_config(&cfg.bitcoin)?;
            let (tx, _rx) = broadcast::channel(1024);
            let bus = Arc::new(tx);
            let syncer = Syncer::new(cfg, store, rpc, bus);
            syncer.run().await
        }
        Command::Serve { config } => {
            let cfg = Config::from_file(&config)?;
            let store = Store::open(&cfg.storage.db_path)?;
            let (tx, _rx) = broadcast::channel(1024);
            let bus = Arc::new(tx);
            let handle = dmt_indexer::sync::SyncHandle {
                store,
                bus,
                shutdown: Arc::new(tokio::sync::Notify::new()),
            };
            let router = api::build_router(&cfg, handle)?;
            let listener = tokio::net::TcpListener::bind(cfg.api.bind_addr).await?;
            info!(addr = %cfg.api.bind_addr, "api listening");
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .ok();
            Ok(())
        }
        Command::Status { config } => {
            let cfg = Config::from_file(&config)?;
            let store = Store::open(&cfg.storage.db_path)?;
            let rtx = store.read()?;
            let cursor = cursor_get(&rtx)?.unwrap_or_default();
            println!(
                "cursor_height={} cursor_hash={}",
                cursor.height, cursor.block_hash
            );
            Ok(())
        }
        Command::Reindex { config } => {
            let cfg = Config::from_file(&config)?;
            let store = Store::open(&cfg.storage.db_path)?;
            dmt_indexer::sync::reorg::rewind_cursor(&store, cfg.sync.start_height - 1)?;
            println!("rewound to {}", cfg.sync.start_height - 1);
            Ok(())
        }
        Command::ExportSnapshot { config, out } => {
            let cfg = Config::from_file(&config)?;
            let store = Store::open(&cfg.storage.db_path)?;
            dmt_indexer::verify::export_snapshot(&store, &out)?;
            println!("snapshot written to {}", out.display());
            Ok(())
        }
        Command::Verify {
            config,
            snapshot,
            against_postgres,
            invariants,
        } => {
            let cfg = Config::from_file(&config)?;
            let store = Store::open(&cfg.storage.db_path)?;
            if invariants {
                let diffs = dmt_indexer::verify::check_invariants(&store)?;
                if diffs.is_empty() {
                    println!("OK (all invariants hold)");
                } else {
                    println!("{} invariant violations:", diffs.len());
                    for d in diffs {
                        println!("- {d}");
                    }
                    std::process::exit(1);
                }
                return Ok(());
            }
            if let Some(path) = snapshot {
                let diffs = dmt_indexer::verify::verify_against_snapshot(&store, &path)?;
                if diffs.is_empty() {
                    println!("OK (snapshot matches)");
                } else {
                    println!("{} diffs", diffs.len());
                    for d in diffs {
                        println!("- {d}");
                    }
                    std::process::exit(1);
                }
            } else if let Some(url) = against_postgres {
                #[cfg(feature = "verify-postgres")]
                {
                    let diffs = dmt_indexer::verify::verify_against_postgres(&store, &url).await?;
                    if diffs.is_empty() {
                        println!("OK (postgres matches)");
                    } else {
                        println!("{} diffs", diffs.len());
                        for d in diffs.iter().take(50) {
                            println!("- {d}");
                        }
                        std::process::exit(1);
                    }
                }
                #[cfg(not(feature = "verify-postgres"))]
                {
                    let _ = url;
                    eprintln!("build with --features verify-postgres");
                    std::process::exit(2);
                }
            } else {
                eprintln!("verify requires --snapshot or --against-postgres");
                std::process::exit(2);
            }
            Ok(())
        }
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,dmt_indexer=debug"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}
