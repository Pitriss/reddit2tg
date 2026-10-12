use std::{fs, path::Path};

use anyhow::{bail, Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub reddit: RedditConfig,
    pub telegram: TelegramConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub bridge: BridgeConfig,
    #[serde(default)]
    pub ha: HaConfig,
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
        Self {
            path: default_db_path(),
        }
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

#[derive(Debug, Clone, Deserialize)]
pub struct HaConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub cluster_id: String,
    #[serde(default)]
    pub instance_id: String,
    #[serde(default)]
    pub control_bot_token: String,
    #[serde(default)]
    pub control_thread_id: i64,
    #[serde(default = "default_ha_lease_ttl_secs")]
    pub lease_ttl_secs: u64,
    #[serde(default = "default_ha_renew_interval_secs")]
    pub renew_interval_secs: u64,
    #[serde(default = "default_ha_claim_settle_secs")]
    pub claim_settle_secs: u64,
    #[serde(default = "default_ha_fail_closed_margin_secs")]
    pub fail_closed_margin_secs: u64,
    #[serde(default)]
    pub instances: Vec<HaInstanceConfig>,
}

impl Default for HaConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cluster_id: String::new(),
            instance_id: String::new(),
            control_bot_token: String::new(),
            control_thread_id: 0,
            lease_ttl_secs: default_ha_lease_ttl_secs(),
            renew_interval_secs: default_ha_renew_interval_secs(),
            claim_settle_secs: default_ha_claim_settle_secs(),
            fail_closed_margin_secs: default_ha_fail_closed_margin_secs(),
            instances: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct HaInstanceConfig {
    pub id: String,
    pub priority: u32,
    #[serde(default = "default_true")]
    pub auto_failover: bool,
    #[serde(default = "default_true")]
    pub allow_fallback: bool,
    #[serde(default)]
    pub auto_failback: bool,
}

fn default_homeserver() -> String {
    "https://matrix.redditspace.com".to_owned()
}

fn default_db_path() -> String {
    "/var/lib/reddit2tg/reddit2tg.sqlite3".to_owned()
}

const fn default_matrix_timeout_ms() -> u64 {
    10_000
}
const fn default_telegram_timeout_secs() -> u64 {
    45
}
const fn default_refresh_before_secs() -> u64 {
    3_600
}
const fn default_ha_lease_ttl_secs() -> u64 {
    30
}
const fn default_ha_renew_interval_secs() -> u64 {
    10
}
const fn default_ha_claim_settle_secs() -> u64 {
    5
}
const fn default_ha_fail_closed_margin_secs() -> u64 {
    5
}
const fn default_true() -> bool {
    true
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("cannot read config {}", path.display()))?;
        let cfg: Self =
            toml::from_str(&raw).with_context(|| format!("invalid TOML in {}", path.display()))?;
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
        if self.ha.enabled && self.ha.control_bot_token.trim() == self.telegram.bot_token.trim() {
            bail!("ha.control_bot_token must use a separate bot from telegram.bot_token");
        }
        if self.bridge.matrix_timeout_ms < 1_000 || self.bridge.matrix_timeout_ms > 60_000 {
            bail!("bridge.matrix_timeout_ms must be in 1000..60000");
        }
        if self.bridge.telegram_timeout_secs < 1 || self.bridge.telegram_timeout_secs > 50 {
            bail!("bridge.telegram_timeout_secs must be in 1..50");
        }
        crate::ha::validate_config(&self.ha)?;
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
