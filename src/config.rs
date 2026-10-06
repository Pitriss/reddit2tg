use std::{fs, path::Path};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub reddit: RedditConfig,
    pub telegram: TelegramConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub bridge: BridgeConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedditConfig {
    pub session: String,
    #[serde(default = "default_homeserver")]
    pub homeserver: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TelegramConfig {
    pub bot_token: String,
    pub chat_id: i64,
    pub operator_user_id: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_db_path")]
    pub path: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self { path: default_db_path() }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BridgeConfig {
    #[serde(default = "default_matrix_timeout_ms")]
    pub matrix_timeout_ms: u64,
    #[serde(default = "default_telegram_timeout_secs")]
    pub telegram_timeout_secs: u64,
    #[serde(default = "default_refresh_before_secs")]
    pub refresh_before_secs: u64,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            matrix_timeout_ms: default_matrix_timeout_ms(),
            telegram_timeout_secs: default_telegram_timeout_secs(),
            refresh_before_secs: default_refresh_before_secs(),
        }
    }
}

fn default_homeserver() -> String {
    "https://matrix.redditspace.com".to_owned()
}

fn default_db_path() -> String {
    "/var/lib/reddit2tg/reddit2tg.sqlite3".to_owned()
}

const fn default_matrix_timeout_ms() -> u64 { 10_000 }
const fn default_telegram_timeout_secs() -> u64 { 45 }
const fn default_refresh_before_secs() -> u64 { 3_600 }

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("cannot read config {}", path.display()))?;
        let cfg: Self = toml::from_str(&raw)
            .with_context(|| format!("invalid TOML in {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        if self.reddit.session.trim().is_empty() {
            bail!("reddit.session must not be empty");
        }
        if self.telegram.bot_token.trim().is_empty() {
            bail!("telegram.bot_token must not be empty");
        }
        if self.telegram.chat_id == 0 {
            bail!("telegram.chat_id must not be 0");
        }
        if self.telegram.operator_user_id <= 0 {
            bail!("telegram.operator_user_id must be positive");
        }
        if self.bridge.matrix_timeout_ms < 1_000 || self.bridge.matrix_timeout_ms > 60_000 {
            bail!("bridge.matrix_timeout_ms must be in 1000..60000");
        }
        if self.bridge.telegram_timeout_secs < 1 || self.bridge.telegram_timeout_secs > 50 {
            bail!("bridge.telegram_timeout_secs must be in 1..50");
        }
        Ok(())
    }

    pub fn reddit_cookie_header(&self) -> String {
        let raw = self.reddit.session.trim();
        if raw.contains('=') {
            raw.to_owned()
        } else {
            format!("reddit_session={raw}")
        }
    }
}
