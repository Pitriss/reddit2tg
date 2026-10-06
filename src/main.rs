mod bridge;
mod config;
mod db;
mod reddit;
mod telegram;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use crate::{bridge::Bridge, config::Config, db::Db, reddit::RedditClient, telegram::TelegramClient};

#[derive(Debug, Parser)]
#[command(name = "reddit2tg", version, about = "Reddit Chat <-> Telegram bridge")]
struct Cli {
    #[arg(long, default_value = "/etc/reddit2tg/config.toml", env = "REDDIT2TG_CONFIG")]
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_target(false)
        .compact()
        .init();

    let cli = Cli::parse();
    let cfg = Config::load(&cli.config)?;
    let db = Db::new(&cfg.storage.path)?;
    let reddit = RedditClient::new(&cfg, db.clone())?;
    let telegram = TelegramClient::new(
        &cfg.telegram.bot_token,
        cfg.telegram.chat_id,
        cfg.telegram.operator_user_id,
    )?;
    let bridge = Bridge::new(cfg, db, reddit, telegram);

    match cli.command.unwrap_or(Command::Run) {
        Command::Run => bridge.run().await,
        Command::Check => bridge.check().await,
        Command::ReconcileNames { dry_run } => {
            let report = bridge.reconcile_names(dry_run).await?;
            println!("{report}");
            Ok(())
        }
    }
}
