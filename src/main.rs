mod bridge;
mod config;
mod db;
pub mod ha;
mod reddit;
mod telegram;

use std::{path::PathBuf, time::Duration};

use anyhow::Result;
use clap::{Parser, Subcommand};
use tokio::time::sleep;
use tracing::error;
use tracing_subscriber::EnvFilter;

use crate::{
    bridge::Bridge, config::Config, db::Db, reddit::RedditClient, telegram::TelegramClient,
};

#[derive(Debug, Parser)]
#[command(name = "reddit2tg", version, about = "Reddit Chat <-> Telegram bridge")]
struct Cli {
    #[arg(
        long,
        default_value = "/etc/reddit2tg/config.toml",
        env = "REDDIT2TG_CONFIG"
    )]
    config: PathBuf,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Run,
    Check,
    ReconcileNames {
        #[arg(long)]
        dry_run: bool,
    },
}

const RUNTIME_RECOVERY_DELAY_SECS: u64 = 5;

fn build_bridge(cfg: &Config, db: &Db) -> Result<Bridge> {
    let reddit = RedditClient::new(cfg, db.clone())?;
    let telegram = TelegramClient::new(
        &cfg.telegram.bot_token,
        cfg.telegram.chat_id,
        cfg.telegram.operator_user_id,
    )?;
    Ok(Bridge::new(cfg.clone(), db.clone(), reddit, telegram))
}

async fn run_supervised(cfg: &Config, db: &Db) -> Result<()> {
    let mut recovery_generation = 0u64;
    let mut bridge = build_bridge(cfg, db)?;
    bridge.probe_runtime_config().await?;

    loop {
        match bridge.run().await {
            Ok(()) => return Ok(()),
            Err(error) => {
                recovery_generation = recovery_generation.saturating_add(1);
                error!(
                    error = %format!("{error:#}"),
                    recovery_generation,
                    delay_secs = RUNTIME_RECOVERY_DELAY_SECS,
                    "bridge runtime unhealthy; recreating Reddit and Telegram clients"
                );
                sleep(Duration::from_secs(RUNTIME_RECOVERY_DELAY_SECS)).await;
                bridge = build_bridge(cfg, db)?;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .compact()
        .init();

    let cli = Cli::parse();
    let cfg = Config::load(&cli.config)?;
    let db = Db::new(&cfg.storage.path)?;

    match cli.command.unwrap_or(Command::Run) {
        Command::Run => run_supervised(&cfg, &db).await,
        Command::Check => build_bridge(&cfg, &db)?.check().await,
        Command::ReconcileNames { dry_run } => {
            let report = build_bridge(&cfg, &db)?.reconcile_names(dry_run).await?;
            println!("{report}");
            Ok(())
        }
    }
}
