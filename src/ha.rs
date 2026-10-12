use std::{
    collections::{BTreeMap, HashSet},
    process,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    sync::{Mutex, OwnedRwLockReadGuard, RwLock},
    time::{sleep, timeout},
};
use tracing::{info, warn};

use crate::{
    config::{HaConfig, HaInstanceConfig},
    reddit::RedditClient,
};

pub const HA_PROTOCOL_VERSION: u32 = 1;
pub const HA_CONTROL_PREFIX: &str = "reddit2tg-ha-v1\n";
const CONTROL_POLL_SECS: u64 = 2;
const CONTROL_WRITE_RETRIES: usize = 8;
const MAX_CONTROL_RATE_LIMIT_RETRIES: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum InstanceMode {
    #[default]
    Auto,
    Disconnected,
    Maintenance,
}

impl InstanceMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Disconnected => "disconnected",
            Self::Maintenance => "maintenance",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ControlPhase {
    Paused,
    #[default]
    Lease,
    Claim,
    Command,
}

impl ControlPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Paused => "paused",
            Self::Lease => "lease",
            Self::Claim => "claim",
            Self::Command => "command",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestKind {
    Connect,
    Disconnect,
    Switch,
    MaintenanceOn,
    MaintenanceOff,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HaRequest {
    pub request_id: String,
    pub kind: RequestKind,
    pub target: String,
    #[serde(default)]
    pub force: bool,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HaDocument {
    pub protocol: u32,
    pub cluster_id: String,
    pub members_fingerprint: String,
    pub generation: u64,
    #[serde(default)]
    pub written_at_ms: i64,
    #[serde(default)]
    pub phase: ControlPhase,
    pub owner: Option<String>,
    pub owner_nonce: Option<String>,
    pub owner_priority: Option<u32>,
    pub previous_owner: Option<String>,
    pub previous_owner_priority: Option<u32>,
    #[serde(default)]
    pub claim_started_at_ms: i64,
    #[serde(default)]
    pub lease_expires_at_ms: i64,
    #[serde(default)]
    pub failover_allowed: bool,
    pub handover_to: Option<String>,
    #[serde(default)]
    pub modes: BTreeMap<String, InstanceMode>,
    pub request: Option<HaRequest>,
}

impl HaDocument {
    pub fn new(cfg: &HaConfig, now_ms: i64) -> Self {
        let modes = cfg
            .instances
            .iter()
            .map(|member| (member.id.clone(), InstanceMode::Auto))
            .collect();
        Self {
            protocol: HA_PROTOCOL_VERSION,
            cluster_id: cfg.cluster_id.clone(),
            members_fingerprint: members_fingerprint(cfg),
            generation: 0,
            written_at_ms: now_ms,
            phase: ControlPhase::Lease,
            owner: None,
            owner_nonce: None,
            owner_priority: None,
            previous_owner: None,
            previous_owner_priority: None,
            claim_started_at_ms: 0,
            lease_expires_at_ms: now_ms,
            failover_allowed: true,
            handover_to: None,
            modes,
            request: None,
        }
    }

    pub fn mode(&self, instance_id: &str) -> InstanceMode {
        self.modes
            .get(instance_id)
            .copied()
            .unwrap_or(InstanceMode::Auto)
    }

    pub fn owner_is_live(&self, now_ms: i64) -> bool {
        matches!(self.phase, ControlPhase::Lease | ControlPhase::Command)
            && self.owner.is_some()
            && self.lease_expires_at_ms > now_ms
    }

    pub fn claim_is_live(&self, now_ms: i64) -> bool {
        self.phase == ControlPhase::Claim
            && self.owner.is_some()
            && self.lease_expires_at_ms > now_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HaCommand {
    ListInstances,
    RedditStatus,
    Connect { instance_id: String, force: bool },
    Disconnect { instance_id: String },
    Switch { instance_id: String },
    MaintenanceShow { instance_id: String },
    MaintenanceSet { instance_id: String, enabled: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalRole {
    Standby,
    Claiming,
    Active,
    Draining,
    Maintenance,
    Disconnected,
}

impl LocalRole {
    fn as_str(self) -> &'static str {
        match self {
            Self::Standby => "standby",
            Self::Claiming => "claiming",
            Self::Active => "active",
            Self::Draining => "draining",
            Self::Maintenance => "maintenance",
            Self::Disconnected => "disconnected",
        }
    }
}

pub(crate) struct ActiveLeasePermit {
    _guard: OwnedRwLockReadGuard<LocalRole>,
    safe_for: Duration,
}

impl ActiveLeasePermit {
    pub(crate) fn safe_for(&self) -> Duration {
        self.safe_for
    }
}

#[derive(Clone)]
pub struct HaController {
    cfg: Arc<HaConfig>,
    api: HaTelegramClient,
    nonce: Arc<String>,
    role: Arc<RwLock<LocalRole>>,
    lease_expires_at_ms: Arc<Mutex<i64>>,
    claim_cooldown_until_ms: Arc<Mutex<i64>>,
}

#[derive(Debug, Clone)]
struct PinnedDocument {
    message_id: i64,
    document: HaDocument,
}

#[derive(Clone)]
struct HaTelegramClient {
    http: Client,
    base: String,
    chat_id: i64,
}

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
    parameters: Option<ResponseParameters>,
}

#[derive(Debug, Deserialize)]
struct ResponseParameters {
    retry_after: Option<u64>,
}

impl HaTelegramClient {
    fn new(bot_token: &str, chat_id: i64) -> Result<Self> {
        let http = Client::builder().timeout(Duration::from_secs(30)).build()?;
        Ok(Self {
            http,
            base: format!("https://api.telegram.org/bot{}", bot_token.trim()),
            chat_id,
        })
    }

    async fn probe(&self) -> Result<()> {
        let me: Value = self.call("getMe", &serde_json::json!({})).await?;
        let bot_id = me
            .get("id")
            .and_then(Value::as_i64)
            .context("HA control bot getMe response has no numeric bot id")?;

        let chat: Value = self
            .call("getChat", &serde_json::json!({"chat_id": self.chat_id}))
            .await?;
        let chat_type = chat.get("type").and_then(Value::as_str).unwrap_or("");
        if chat_type != "group" && chat_type != "supergroup" {
            bail!("ha.control_chat_id must point to a Telegram group or supergroup");
        }

        let member: Value = self
            .call(
                "getChatMember",
                &serde_json::json!({
                    "chat_id": self.chat_id,
                    "user_id": bot_id,
                }),
            )
            .await?;
        let status = member.get("status").and_then(Value::as_str).unwrap_or("");
        let can_pin_messages = member
            .get("can_pin_messages")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if status != "creator" && !(status == "administrator" && can_pin_messages) {
            bail!("HA control bot must be an administrator with permission to pin messages");
        }
        Ok(())
    }

    async fn read_pinned(&self) -> Result<Option<PinnedDocument>> {
        let chat: Value = self
            .call("getChat", &serde_json::json!({"chat_id": self.chat_id}))
            .await?;
        let Some(message) = chat.get("pinned_message") else {
            return Ok(None);
        };
        let message_id = message
            .get("message_id")
            .and_then(Value::as_i64)
            .context("HA pinned message has no numeric message_id")?;
        let text = message
            .get("text")
            .and_then(Value::as_str)
            .context("latest pinned HA control message is not a text message")?;
        let Some(decoded) = decode_document(text) else {
            bail!("latest pinned message in HA control chat is not a reddit2tg HA document; use a dedicated control chat");
        };
        Ok(Some(PinnedDocument {
            message_id,
            document: decoded?,
        }))
    }

    async fn send_document(&self, document: &HaDocument) -> Result<i64> {
        let text = encode_document(document)?;
        let message: Value = self
            .call(
                "sendMessage",
                &serde_json::json!({
                    "chat_id": self.chat_id,
                    "text": text,
                    "disable_notification": true,
                }),
            )
            .await?;
        message
            .get("message_id")
            .and_then(Value::as_i64)
            .context("HA sendMessage response has no message_id")
    }

    async fn pin_message(&self, message_id: i64) -> Result<()> {
        let _: bool = self
            .call(
                "pinChatMessage",
                &serde_json::json!({
                    "chat_id": self.chat_id,
                    "message_id": message_id,
                    "disable_notification": true,
                }),
            )
            .await?;
        Ok(())
    }

    async fn edit_document(&self, message_id: i64, document: &HaDocument) -> Result<()> {
        let text = encode_document(document)?;
        let _: Value = self
            .call(
                "editMessageText",
                &serde_json::json!({
                    "chat_id": self.chat_id,
                    "message_id": message_id,
                    "text": text,
                }),
            )
            .await?;
        Ok(())
    }

    async fn call<T: for<'de> Deserialize<'de>, B: Serialize + ?Sized>(
        &self,
        method: &str,
        body: &B,
    ) -> Result<T> {
        let url = format!("{}/{}", self.base, method);

        for retry in 0..=MAX_CONTROL_RATE_LIMIT_RETRIES {
            let response = self
                .http
                .post(&url)
                .json(body)
                .send()
                .await
                .with_context(|| format!("HA Telegram {method} request failed"))?;
            let status = response.status();
            let parsed: ApiResponse<T> = response.json().await.with_context(|| {
                format!("HA Telegram {method} returned invalid JSON (HTTP {status})")
            })?;

            if parsed.ok {
                return parsed.result.ok_or_else(|| {
                    anyhow::anyhow!("HA Telegram {method} returned ok without result")
                });
            }

            if let Some(retry_after) = parsed
                .parameters
                .as_ref()
                .and_then(|parameters| parameters.retry_after)
            {
                if retry < MAX_CONTROL_RATE_LIMIT_RETRIES {
                    sleep(Duration::from_secs(retry_after.saturating_add(1))).await;
                    continue;
                }
            }

            bail!(
                "HA Telegram {method} failed (HTTP {status}): {}",
                parsed
                    .description
                    .unwrap_or_else(|| "unknown error".to_owned())
            );
        }

        unreachable!()
    }
}

impl HaController {
    pub fn process_nonce(cfg: &HaConfig) -> Option<String> {
        if !cfg.enabled {
            return None;
        }
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Some(format!("{}-{}-{nanos}", cfg.instance_id, process::id()))
    }

    pub fn new(cfg: &HaConfig, process_nonce: Option<&str>) -> Result<Option<Self>> {
        if !cfg.enabled {
            return Ok(None);
        }
        validate_config(cfg)?;
        let nonce = process_nonce
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| {
                Self::process_nonce(cfg).expect("HA process nonce exists when HA is enabled")
            });
        Ok(Some(Self {
            cfg: Arc::new(cfg.clone()),
            api: HaTelegramClient::new(&cfg.control_bot_token, cfg.control_chat_id)?,
            nonce: Arc::new(nonce),
            role: Arc::new(RwLock::new(LocalRole::Standby)),
            lease_expires_at_ms: Arc::new(Mutex::new(0)),
            claim_cooldown_until_ms: Arc::new(Mutex::new(0)),
        }))
    }

    pub async fn probe(&self) -> Result<()> {
        self.api.probe().await?;
        let document = self.ensure_document().await?;
        self.validate_document(&document.document)?;
        Ok(())
    }

    pub async fn run(&self, reddit: RedditClient) -> Result<()> {
        self.force_local_standby(&reddit).await?;
        self.ensure_document().await?;

        let mut backoff = 1u64;
        loop {
            match self.tick(&reddit).await {
                Ok(()) => {
                    backoff = 1;
                    sleep(Duration::from_secs(CONTROL_POLL_SECS)).await;
                }
                Err(error) => {
                    warn!(
                        error = %format!("{error:#}"),
                        backoff_secs = backoff,
                        instance_id = %self.cfg.instance_id,
                        "HA control iteration failed"
                    );
                    self.fail_closed_if_needed(&reddit).await;
                    sleep(Duration::from_secs(backoff)).await;
                    backoff = (backoff * 2).min(10);
                }
            }
        }
    }

    pub async fn shutdown(&self, reddit: &RedditClient) {
        if let Err(error) = self.graceful_shutdown(reddit).await {
            warn!(
                error = %format!("{error:#}"),
                instance_id = %self.cfg.instance_id,
                "HA graceful shutdown could not release lease; expiry will fence this instance"
            );
        }
    }

    pub(crate) async fn active_permit(&self) -> Option<ActiveLeasePermit> {
        let guard = self.role.clone().read_owned().await;
        if *guard != LocalRole::Active {
            return None;
        }

        let expires_at_ms = *self.lease_expires_at_ms.lock().await;
        let safe_until_ms =
            expires_at_ms.saturating_sub((self.cfg.fail_closed_margin_secs as i64) * 1000);
        let now = now_ms();
        if safe_until_ms <= now {
            return None;
        }

        Some(ActiveLeasePermit {
            _guard: guard,
            safe_for: Duration::from_millis((safe_until_ms - now) as u64),
        })
    }

    pub async fn inactive_message(&self) -> String {
        let role = *self.role.read().await;
        match self.read_document().await {
            Ok(pinned) => {
                let now = now_ms();
                let active = if pinned.document.owner_is_live(now)
                    || pinned.document.claim_is_live(now)
                {
                    pinned.document.owner.as_deref().unwrap_or("unknown")
                } else {
                    "none"
                };
                format!(
                    "Instance {} není aktivní pro Reddit (local={}, cluster owner={active}). Použij /reddit-status nebo /list-instances.",
                    self.cfg.instance_id,
                    role.as_str()
                )
            }
            Err(_) => format!(
                "Instance {} není aktivní pro Reddit (local={}). HA control plane se nepodařilo přečíst.",
                self.cfg.instance_id,
                role.as_str()
            ),
        }
    }

    pub async fn execute_command(&self, command: HaCommand, update_id: i64) -> Result<String> {
        match command {
            HaCommand::ListInstances => self.list_instances().await,
            HaCommand::RedditStatus => self.reddit_status().await,
            HaCommand::MaintenanceShow { instance_id } => {
                if member(&self.cfg, &instance_id).is_none() {
                    return Ok(self.unknown_instance(&instance_id));
                }
                let pinned = self.read_document().await?;
                Ok(format!(
                    "maintenance {}: {}",
                    instance_id,
                    if pinned.document.mode(&instance_id) == InstanceMode::Maintenance {
                        "on"
                    } else {
                        "off"
                    }
                ))
            }
            HaCommand::Connect { instance_id, force } => {
                self.command_connect(&instance_id, force, update_id).await
            }
            HaCommand::Disconnect { instance_id } => {
                self.command_disconnect(&instance_id, update_id).await
            }
            HaCommand::Switch { instance_id } => self.command_switch(&instance_id, update_id).await,
            HaCommand::MaintenanceSet {
                instance_id,
                enabled,
            } => {
                self.command_maintenance(&instance_id, enabled, update_id)
                    .await
            }
        }
    }

    async fn tick(&self, reddit: &RedditClient) -> Result<()> {
        let pinned = self.read_document().await?;
        let document = pinned.document;
        let now = now_ms();
        let instance_id = self.cfg.instance_id.as_str();
        let mode = document.mode(instance_id);

        if mode != InstanceMode::Auto {
            let target_role = match mode {
                InstanceMode::Maintenance => LocalRole::Maintenance,
                InstanceMode::Disconnected => LocalRole::Disconnected,
                InstanceMode::Auto => unreachable!(),
            };
            if self.owned_by_this_process(&document) {
                self.drain_and_release(reddit, target_role).await?;
            } else {
                self.ensure_local_role(reddit, target_role).await?;
            }
            return Ok(());
        }

        if document.owner_is_live(now) {
            if self.owned_by_this_process(&document) {
                if document
                    .handover_to
                    .as_deref()
                    .is_some_and(|target| target != instance_id)
                {
                    self.drain_and_release(reddit, LocalRole::Standby).await?;
                    return Ok(());
                }

                if *self.role.read().await != LocalRole::Active {
                    self.resume_owned_lease(reddit, &document).await?;
                    return Ok(());
                }

                let renew_at_ms = document.lease_expires_at_ms.saturating_sub(
                    ((self.cfg.lease_ttl_secs - self.cfg.renew_interval_secs) as i64) * 1000,
                );
                if now >= renew_at_ms {
                    self.renew_lease().await?;
                }
                return Ok(());
            }

            self.ensure_local_role(reddit, LocalRole::Standby).await?;
            if document.handover_to.is_none()
                && should_auto_failback(&self.cfg, &document, instance_id, now)
            {
                self.request_auto_failback().await?;
            }
            return Ok(());
        }

        if document.claim_is_live(now) {
            if self.owned_by_this_process(&document) {
                *self.role.write().await = LocalRole::Claiming;
                let settled_at = document
                    .claim_started_at_ms
                    .saturating_add((self.cfg.claim_settle_secs as i64) * 1000);
                if now >= settled_at {
                    self.activate_settled_claim(reddit, &document).await?;
                }
                return Ok(());
            }

            self.ensure_local_role(reddit, LocalRole::Standby).await?;
            if claim_preemption_allowed(&self.cfg, &document, instance_id, now) {
                self.try_claim().await?;
            }
            return Ok(());
        }

        self.ensure_local_role(reddit, LocalRole::Standby).await?;

        if document.phase == ControlPhase::Paused {
            return Ok(());
        }

        if *self.claim_cooldown_until_ms.lock().await > now {
            return Ok(());
        }

        let Some(rank) = claim_rank(&self.cfg, &document, now, instance_id) else {
            return Ok(());
        };
        let base_ms = document.lease_expires_at_ms.max(document.written_at_ms);
        let delay_ms = if document.handover_to.as_deref() == Some(instance_id) {
            0
        } else {
            (rank as i64)
                .saturating_mul(self.cfg.claim_settle_secs as i64)
                .saturating_mul(1000)
        };
        if now >= base_ms.saturating_add(delay_ms) {
            self.try_claim().await?;
        }
        Ok(())
    }

    async fn try_claim(&self) -> Result<()> {
        let instance_id = self.cfg.instance_id.clone();
        let nonce = self.nonce.to_string();
        let priority = member(&self.cfg, &instance_id)
            .context("local HA instance missing from config")?
            .priority;
        let ttl_ms = (self.cfg.lease_ttl_secs as i64) * 1000;

        let claimed = self
            .update_document(|document| {
                let now = now_ms();
                if document.owner_is_live(now) {
                    bail!("HA lease became live while claiming");
                }
                if document.claim_is_live(now)
                    && !claim_preemption_allowed(&self.cfg, document, &instance_id, now)
                {
                    bail!("another HA claim has priority");
                }
                let Some(rank) = claim_rank(&self.cfg, document, now, &instance_id) else {
                    bail!("local instance is no longer an eligible HA candidate");
                };
                let base_ms = document.lease_expires_at_ms.max(document.written_at_ms);
                let delay_ms = if document.handover_to.as_deref() == Some(instance_id.as_str()) {
                    0
                } else {
                    (rank as i64)
                        .saturating_mul(self.cfg.claim_settle_secs as i64)
                        .saturating_mul(1000)
                };
                if now < base_ms.saturating_add(delay_ms) {
                    bail!("HA claim priority delay has not elapsed");
                }

                if document.phase != ControlPhase::Claim {
                    document.previous_owner = document.owner.clone();
                    document.previous_owner_priority = document.owner_priority;
                }
                document.phase = ControlPhase::Claim;
                document.owner = Some(instance_id.clone());
                document.owner_nonce = Some(nonce.clone());
                document.owner_priority = Some(priority);
                document.claim_started_at_ms = now;
                document.lease_expires_at_ms = now.saturating_add(ttl_ms);
                document.failover_allowed =
                    member(&self.cfg, &instance_id).is_some_and(|member| member.allow_fallback);
                Ok(())
            })
            .await?;

        if self.owned_by_this_process(&claimed) {
            *self.role.write().await = LocalRole::Claiming;
            info!(
                instance_id = %self.cfg.instance_id,
                priority,
                generation = claimed.generation,
                "HA lease claim published; waiting for settle window"
            );
        }
        Ok(())
    }

    async fn activate_settled_claim(
        &self,
        reddit: &RedditClient,
        observed: &HaDocument,
    ) -> Result<()> {
        let now = now_ms();
        if !self.owned_by_this_process(observed)
            || !observed.claim_is_live(now)
            || now
                < observed
                    .claim_started_at_ms
                    .saturating_add((self.cfg.claim_settle_secs as i64) * 1000)
        {
            return Ok(());
        }

        let mut role_guard = self.role.write().await;
        *role_guard = LocalRole::Claiming;

        let current = self.read_document().await?.document;
        if !self.owned_by_this_process(&current) || !current.claim_is_live(now_ms()) {
            reddit.soft_disconnect().await?;
            *role_guard = LocalRole::Standby;
            return Ok(());
        }

        let auth_timeout_secs = (self.cfg.lease_ttl_secs / 2).clamp(3, 20);
        let authentication = timeout(
            Duration::from_secs(auth_timeout_secs),
            reddit.authenticate(),
        )
        .await;

        let session = match authentication {
            Ok(Ok(session)) => session,
            Ok(Err(error)) => {
                warn!(
                    error = %format!("{error:#}"),
                    instance_id = %self.cfg.instance_id,
                    "HA claimant could not authenticate Reddit"
                );
                reddit.soft_disconnect().await?;
                *role_guard = LocalRole::Standby;
                drop(role_guard);
                self.release_failed_claim().await?;
                *self.claim_cooldown_until_ms.lock().await =
                    now_ms().saturating_add((self.cfg.lease_ttl_secs as i64) * 1000);
                return Ok(());
            }
            Err(_) => {
                warn!(
                    instance_id = %self.cfg.instance_id,
                    timeout_secs = auth_timeout_secs,
                    "HA claimant Reddit authentication timed out"
                );
                reddit.soft_disconnect().await?;
                *role_guard = LocalRole::Standby;
                drop(role_guard);
                self.release_failed_claim().await?;
                *self.claim_cooldown_until_ms.lock().await =
                    now_ms().saturating_add((self.cfg.lease_ttl_secs as i64) * 1000);
                return Ok(());
            }
        };

        let verify = self.read_document().await?.document;
        if !self.owned_by_this_process(&verify) || !verify.claim_is_live(now_ms()) {
            warn!(
                instance_id = %self.cfg.instance_id,
                "HA claim changed during Reddit authentication; discarding authenticated session"
            );
            reddit.soft_disconnect().await?;
            *role_guard = LocalRole::Standby;
            return Ok(());
        }

        let promoted = self.promote_claim().await?;
        *self.lease_expires_at_ms.lock().await = promoted.lease_expires_at_ms;
        *role_guard = LocalRole::Active;
        info!(
            instance_id = %self.cfg.instance_id,
            user_id = %session.user_id,
            expires_in_secs = ?session.expires_in_secs(),
            lease_expires_at_ms = promoted.lease_expires_at_ms,
            "HA instance became active Reddit owner"
        );
        Ok(())
    }

    async fn resume_owned_lease(&self, reddit: &RedditClient, observed: &HaDocument) -> Result<()> {
        let safe_until_ms = observed
            .lease_expires_at_ms
            .saturating_sub((self.cfg.fail_closed_margin_secs as i64) * 1000);
        let remaining_ms = safe_until_ms.saturating_sub(now_ms());
        if remaining_ms <= 1000 {
            return Ok(());
        }

        let mut role_guard = self.role.write().await;
        *role_guard = LocalRole::Claiming;
        let auth_window = Duration::from_millis((remaining_ms as u64).min(20_000));
        match timeout(auth_window, reddit.authenticate()).await {
            Ok(Ok(session)) => {
                let verify = self.read_document().await?.document;
                if self.owned_by_this_process(&verify) && verify.owner_is_live(now_ms()) {
                    *self.lease_expires_at_ms.lock().await = verify.lease_expires_at_ms;
                    *role_guard = LocalRole::Active;
                    info!(
                        instance_id = %self.cfg.instance_id,
                        user_id = %session.user_id,
                        "HA runtime resumed an existing lease after local runtime recovery"
                    );
                    return Ok(());
                }
            }
            Ok(Err(error)) => {
                warn!(
                    error = %format!("{error:#}"),
                    instance_id = %self.cfg.instance_id,
                    "Reddit authentication failed while resuming owned HA lease"
                );
            }
            Err(_) => {
                warn!(
                    instance_id = %self.cfg.instance_id,
                    "Reddit authentication timed out while resuming owned HA lease"
                );
            }
        }

        reddit.soft_disconnect().await?;
        *role_guard = LocalRole::Standby;
        drop(role_guard);
        self.release_owned_after_auth_failure().await?;
        Ok(())
    }

    async fn renew_lease(&self) -> Result<()> {
        let instance_id = self.cfg.instance_id.clone();
        let nonce = self.nonce.to_string();
        let ttl_ms = (self.cfg.lease_ttl_secs as i64) * 1000;
        let allow_fallback = member(&self.cfg, &instance_id)
            .context("local HA instance missing from config")?
            .allow_fallback;

        let renewed = self
            .update_document(|document| {
                let now = now_ms();
                if document.owner.as_deref() != Some(instance_id.as_str())
                    || document.owner_nonce.as_deref() != Some(nonce.as_str())
                    || !document.owner_is_live(now)
                {
                    bail!("HA lease ownership changed before renewal");
                }
                if document.mode(&instance_id) != InstanceMode::Auto {
                    bail!("local HA mode changed before renewal");
                }
                if document
                    .handover_to
                    .as_deref()
                    .is_some_and(|target| target != instance_id)
                {
                    bail!("HA handover requested before renewal");
                }

                document.phase = ControlPhase::Lease;
                document.lease_expires_at_ms = now.saturating_add(ttl_ms);
                document.failover_allowed = allow_fallback;
                document.claim_started_at_ms = 0;
                document.previous_owner = None;
                document.previous_owner_priority = None;
                Ok(())
            })
            .await?;

        *self.lease_expires_at_ms.lock().await = renewed.lease_expires_at_ms;
        Ok(())
    }

    async fn promote_claim(&self) -> Result<HaDocument> {
        let instance_id = self.cfg.instance_id.clone();
        let nonce = self.nonce.to_string();
        let ttl_ms = (self.cfg.lease_ttl_secs as i64) * 1000;
        let configured = member(&self.cfg, &instance_id)
            .context("local HA instance missing from config")?
            .clone();

        self.update_document(|document| {
            let now = now_ms();
            if document.phase != ControlPhase::Claim
                || document.owner.as_deref() != Some(instance_id.as_str())
                || document.owner_nonce.as_deref() != Some(nonce.as_str())
                || !document.claim_is_live(now)
            {
                bail!("HA claim changed before promotion");
            }
            let settled_at = document
                .claim_started_at_ms
                .saturating_add((self.cfg.claim_settle_secs as i64) * 1000);
            if now < settled_at {
                bail!("HA claim settle window has not elapsed");
            }

            document.phase = ControlPhase::Lease;
            document.owner = Some(instance_id.clone());
            document.owner_nonce = Some(nonce.clone());
            document.owner_priority = Some(configured.priority);
            document.previous_owner = None;
            document.previous_owner_priority = None;
            document.claim_started_at_ms = 0;
            document.lease_expires_at_ms = now.saturating_add(ttl_ms);
            document.failover_allowed = configured.allow_fallback;
            document.handover_to = None;
            Ok(())
        })
        .await
    }

    async fn release_failed_claim(&self) -> Result<()> {
        let instance_id = self.cfg.instance_id.clone();
        let nonce = self.nonce.to_string();
        self.update_document(|document| {
            if document.phase != ControlPhase::Claim
                || document.owner.as_deref() != Some(instance_id.as_str())
                || document.owner_nonce.as_deref() != Some(nonce.as_str())
            {
                return Ok(());
            }

            let restored_owner = document.previous_owner.take();
            let restored_priority = document.previous_owner_priority.take();
            document.phase = ControlPhase::Lease;
            document.owner = restored_owner.clone();
            document.owner_priority = restored_priority;
            document.owner_nonce = None;
            document.claim_started_at_ms = 0;
            document.lease_expires_at_ms = now_ms();
            document.failover_allowed = restored_owner
                .as_deref()
                .and_then(|owner| member(&self.cfg, owner))
                .is_none_or(|owner| owner.allow_fallback);
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn release_owned_after_auth_failure(&self) -> Result<()> {
        let instance_id = self.cfg.instance_id.clone();
        let nonce = self.nonce.to_string();
        let allow_fallback = member(&self.cfg, &instance_id)
            .context("local HA instance missing from config")?
            .allow_fallback;
        self.update_document(|document| {
            if document.owner.as_deref() != Some(instance_id.as_str())
                || document.owner_nonce.as_deref() != Some(nonce.as_str())
            {
                return Ok(());
            }
            document.phase = if allow_fallback {
                ControlPhase::Lease
            } else {
                ControlPhase::Paused
            };
            document.lease_expires_at_ms = now_ms();
            document.failover_allowed = allow_fallback;
            document.owner_nonce = None;
            document.claim_started_at_ms = 0;
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn drain_and_release(&self, reddit: &RedditClient, target_role: LocalRole) -> Result<()> {
        let mut role_guard = self.role.write().await;
        *role_guard = LocalRole::Draining;
        *self.lease_expires_at_ms.lock().await = 0;

        reddit.soft_disconnect().await?;

        let instance_id = self.cfg.instance_id.clone();
        let nonce = self.nonce.to_string();
        let local_member = member(&self.cfg, &instance_id)
            .context("local HA instance missing from config")?
            .clone();

        self.update_document(|document| {
            if document.owner.as_deref() != Some(instance_id.as_str())
                || document.owner_nonce.as_deref() != Some(nonce.as_str())
            {
                return Ok(());
            }

            let now = now_ms();
            let mode = document.mode(&instance_id);
            document.lease_expires_at_ms = now;
            document.owner_nonce = None;
            document.claim_started_at_ms = 0;

            if document
                .handover_to
                .as_deref()
                .is_some_and(|target| target != instance_id)
            {
                document.phase = ControlPhase::Command;
                document.failover_allowed = true;
            } else if mode == InstanceMode::Maintenance && local_member.allow_fallback {
                document.phase = ControlPhase::Lease;
                document.failover_allowed = true;
                document.handover_to = None;
            } else if mode == InstanceMode::Auto {
                document.phase = if local_member.allow_fallback {
                    ControlPhase::Lease
                } else {
                    ControlPhase::Paused
                };
                document.failover_allowed = local_member.allow_fallback;
            } else {
                document.phase = ControlPhase::Paused;
                document.failover_allowed = false;
                document.handover_to = None;
            }
            Ok(())
        })
        .await?;

        *role_guard = target_role;
        info!(
            instance_id = %self.cfg.instance_id,
            local_role = target_role.as_str(),
            "HA Reddit owner drained and released"
        );
        Ok(())
    }

    async fn graceful_shutdown(&self, reddit: &RedditClient) -> Result<()> {
        let mut role_guard = self.role.write().await;
        let was_active = matches!(
            *role_guard,
            LocalRole::Active | LocalRole::Claiming | LocalRole::Draining
        );
        *role_guard = LocalRole::Draining;
        *self.lease_expires_at_ms.lock().await = 0;
        reddit.soft_disconnect().await?;

        if was_active {
            let instance_id = self.cfg.instance_id.clone();
            let nonce = self.nonce.to_string();
            let configured = member(&self.cfg, &instance_id)
                .context("local HA instance missing from config")?
                .clone();
            self.update_document(|document| {
                if document.owner.as_deref() != Some(instance_id.as_str())
                    || document.owner_nonce.as_deref() != Some(nonce.as_str())
                {
                    return Ok(());
                }
                let mode = document.mode(&instance_id);
                let allow_fallback =
                    mode != InstanceMode::Disconnected && configured.allow_fallback;
                document.lease_expires_at_ms = now_ms();
                document.owner_nonce = None;
                document.claim_started_at_ms = 0;
                document.handover_to = None;
                document.phase = if allow_fallback {
                    ControlPhase::Lease
                } else {
                    ControlPhase::Paused
                };
                document.failover_allowed = allow_fallback;
                Ok(())
            })
            .await?;
        }

        *role_guard = LocalRole::Standby;
        Ok(())
    }

    async fn force_local_standby(&self, reddit: &RedditClient) -> Result<()> {
        let mut role_guard = self.role.write().await;
        *role_guard = LocalRole::Draining;
        *self.lease_expires_at_ms.lock().await = 0;
        reddit.soft_disconnect().await?;
        *role_guard = LocalRole::Standby;
        Ok(())
    }

    async fn ensure_local_role(&self, reddit: &RedditClient, requested: LocalRole) -> Result<()> {
        let current = *self.role.read().await;
        if matches!(
            current,
            LocalRole::Active | LocalRole::Claiming | LocalRole::Draining
        ) {
            let mut role_guard = self.role.write().await;
            *role_guard = LocalRole::Draining;
            *self.lease_expires_at_ms.lock().await = 0;
            reddit.soft_disconnect().await?;
            *role_guard = requested;
        } else if current != requested {
            *self.role.write().await = requested;
        }
        Ok(())
    }

    async fn fail_closed_if_needed(&self, reddit: &RedditClient) {
        if *self.role.read().await != LocalRole::Active {
            return;
        }
        let expires_at = *self.lease_expires_at_ms.lock().await;
        let cutoff = expires_at.saturating_sub((self.cfg.fail_closed_margin_secs as i64) * 1000);
        if now_ms() < cutoff {
            return;
        }

        warn!(
            instance_id = %self.cfg.instance_id,
            lease_expires_at_ms = expires_at,
            "HA control plane unavailable near lease expiry; failing closed"
        );
        if let Err(error) = self.ensure_local_role(reddit, LocalRole::Standby).await {
            warn!(error = %error, "HA fail-closed Reddit disconnect failed");
        }
    }

    async fn request_auto_failback(&self) -> Result<()> {
        let instance_id = self.cfg.instance_id.clone();
        self.update_document(|document| {
            let now = now_ms();
            if !document.owner_is_live(now)
                || document.handover_to.is_some()
                || !should_auto_failback(&self.cfg, document, &instance_id, now)
            {
                return Ok(());
            }
            document.phase = ControlPhase::Command;
            document.handover_to = Some(instance_id.clone());
            Ok(())
        })
        .await?;
        info!(
            instance_id = %self.cfg.instance_id,
            "HA automatic failback requested"
        );
        Ok(())
    }

    async fn command_connect(
        &self,
        instance_id: &str,
        force: bool,
        update_id: i64,
    ) -> Result<String> {
        if member(&self.cfg, instance_id).is_none() {
            return Ok(self.unknown_instance(instance_id));
        }

        let current = self.read_document().await?.document;
        if current.mode(instance_id) == InstanceMode::Maintenance {
            return Ok(format!(
                "Nelze připojit {instance_id}: instance je v maintenance mode. Použij /maintenance {instance_id} off."
            ));
        }
        if current.owner_is_live(now_ms())
            && current.owner.as_deref() != Some(instance_id)
            && !force
        {
            return Ok(format!(
                "Nelze připojit {instance_id}: Reddit je aktivní na {}. Použij /reddit-connect {instance_id} force nebo /reddit-switch {instance_id}.",
                current.owner.as_deref().unwrap_or("unknown")
            ));
        }

        let request_id = self.request_id(update_id);
        let target = instance_id.to_owned();
        let updated = self
            .update_document(|document| {
                if document
                    .request
                    .as_ref()
                    .is_some_and(|request| request.request_id == request_id)
                {
                    return Ok(());
                }
                if document.mode(&target) == InstanceMode::Maintenance {
                    bail!("target entered maintenance mode");
                }
                let now = now_ms();
                if document.owner_is_live(now)
                    && document.owner.as_deref() != Some(target.as_str())
                    && !force
                {
                    bail!("another instance owns a live Reddit lease");
                }

                document.modes.insert(target.clone(), InstanceMode::Auto);
                if document.owner.as_deref() != Some(target.as_str())
                    || !document.owner_is_live(now)
                {
                    document.phase = ControlPhase::Command;
                    document.handover_to = Some(target.clone());
                }
                document.request = Some(HaRequest {
                    request_id: request_id.clone(),
                    kind: RequestKind::Connect,
                    target: target.clone(),
                    force,
                    created_at_ms: now,
                });
                Ok(())
            })
            .await?;

        if updated.owner_is_live(now_ms()) && updated.owner.as_deref() == Some(instance_id) {
            Ok(format!("{instance_id} už drží aktivní Reddit lease."))
        } else {
            Ok(format!(
                "Požadavek na připojení {instance_id} byl zapsán do HA control plane{}.",
                if force { " (force handover)" } else { "" }
            ))
        }
    }

    async fn command_switch(&self, instance_id: &str, update_id: i64) -> Result<String> {
        if member(&self.cfg, instance_id).is_none() {
            return Ok(self.unknown_instance(instance_id));
        }
        let current = self.read_document().await?.document;
        if current.mode(instance_id) == InstanceMode::Maintenance {
            return Ok(format!(
                "Nelze přepnout na {instance_id}: instance je v maintenance mode. Použij /maintenance {instance_id} off."
            ));
        }

        let request_id = self.request_id(update_id);
        let target = instance_id.to_owned();
        self.update_document(|document| {
            if document
                .request
                .as_ref()
                .is_some_and(|request| request.request_id == request_id)
            {
                return Ok(());
            }
            if document.mode(&target) == InstanceMode::Maintenance {
                bail!("target entered maintenance mode");
            }
            document.modes.insert(target.clone(), InstanceMode::Auto);
            if document.owner.as_deref() != Some(target.as_str())
                || !document.owner_is_live(now_ms())
            {
                document.phase = ControlPhase::Command;
                document.handover_to = Some(target.clone());
            }
            document.request = Some(HaRequest {
                request_id: request_id.clone(),
                kind: RequestKind::Switch,
                target: target.clone(),
                force: true,
                created_at_ms: now_ms(),
            });
            Ok(())
        })
        .await?;

        Ok(format!(
            "Řízený Reddit handover na {instance_id} byl vyžádán."
        ))
    }

    async fn command_disconnect(&self, instance_id: &str, update_id: i64) -> Result<String> {
        if member(&self.cfg, instance_id).is_none() {
            return Ok(self.unknown_instance(instance_id));
        }
        let request_id = self.request_id(update_id);
        let target = instance_id.to_owned();

        self.update_document(|document| {
            if document
                .request
                .as_ref()
                .is_some_and(|request| request.request_id == request_id)
            {
                return Ok(());
            }
            document
                .modes
                .insert(target.clone(), InstanceMode::Disconnected);
            if document.owner.as_deref() == Some(target.as_str()) {
                document.phase = ControlPhase::Paused;
                document.failover_allowed = false;
                document.handover_to = None;
            }
            document.request = Some(HaRequest {
                request_id: request_id.clone(),
                kind: RequestKind::Disconnect,
                target: target.clone(),
                force: false,
                created_at_ms: now_ms(),
            });
            Ok(())
        })
        .await?;

        Ok(format!(
            "{instance_id}: disconnected; automatický handover tímto příkazem není spuštěn."
        ))
    }

    async fn command_maintenance(
        &self,
        instance_id: &str,
        enabled: bool,
        update_id: i64,
    ) -> Result<String> {
        let Some(target_member) = member(&self.cfg, instance_id).cloned() else {
            return Ok(self.unknown_instance(instance_id));
        };
        let request_id = self.request_id(update_id);
        let target = instance_id.to_owned();

        self.update_document(|document| {
            if document
                .request
                .as_ref()
                .is_some_and(|request| request.request_id == request_id)
            {
                return Ok(());
            }

            let mode = if enabled {
                InstanceMode::Maintenance
            } else {
                InstanceMode::Auto
            };
            document.modes.insert(target.clone(), mode);

            if enabled && document.owner.as_deref() == Some(target.as_str()) {
                document.handover_to = None;
                document.failover_allowed = target_member.allow_fallback;
                if !target_member.allow_fallback {
                    document.phase = ControlPhase::Paused;
                } else if document.phase == ControlPhase::Claim {
                    document.phase = ControlPhase::Lease;
                    document.lease_expires_at_ms = now_ms();
                    document.owner_nonce = None;
                }
            }

            document.request = Some(HaRequest {
                request_id: request_id.clone(),
                kind: if enabled {
                    RequestKind::MaintenanceOn
                } else {
                    RequestKind::MaintenanceOff
                },
                target: target.clone(),
                force: false,
                created_at_ms: now_ms(),
            });
            Ok(())
        })
        .await?;

        if enabled {
            Ok(format!(
                "{instance_id}: maintenance on{}.",
                if target_member.allow_fallback {
                    "; po uvolnění lease je povolen automatický fallback"
                } else {
                    "; automatický fallback je zakázán"
                }
            ))
        } else {
            Ok(format!(
                "{instance_id}: maintenance off; instance je znovu kandidát podle své HA policy."
            ))
        }
    }

    async fn list_instances(&self) -> Result<String> {
        let pinned = self.read_document().await?;
        let document = pinned.document;
        let now = now_ms();
        let mut lines = vec![format!(
            "reddit2tg instances · cluster={} · generation={}",
            self.cfg.cluster_id, document.generation
        )];

        for configured in members_by_priority(&self.cfg) {
            let mode = document.mode(&configured.id);
            let state = if mode == InstanceMode::Maintenance {
                "maintenance"
            } else if mode == InstanceMode::Disconnected {
                "disconnected"
            } else if document.owner.as_deref() == Some(configured.id.as_str())
                && document.owner_is_live(now)
            {
                "active"
            } else if document.owner.as_deref() == Some(configured.id.as_str())
                && document.claim_is_live(now)
            {
                "claiming"
            } else if document.handover_to.as_deref() == Some(configured.id.as_str()) {
                "requested"
            } else {
                "standby"
            };
            let marker = if document.owner.as_deref() == Some(configured.id.as_str())
                && (document.owner_is_live(now) || document.claim_is_live(now))
            {
                "*"
            } else {
                "-"
            };
            let this = if configured.id == self.cfg.instance_id {
                " this"
            } else {
                ""
            };
            lines.push(format!(
                "{marker} {} priority={} state={} mode={} failover={} failback={}{}",
                configured.id,
                configured.priority,
                state,
                mode.as_str(),
                yes_no(configured.auto_failover),
                yes_no(configured.auto_failback),
                this
            ));
        }
        lines.push(
            "Pozn.: V2 odvozuje clusterovou živost z lease/claim; per-node last-seen heartbeat přijde v další vrstvě."
                .to_owned(),
        );
        Ok(lines.join("\n"))
    }

    async fn reddit_status(&self) -> Result<String> {
        let pinned = self.read_document().await?;
        let document = pinned.document;
        let now = now_ms();
        let role = *self.role.read().await;
        let lease_remaining = document.lease_expires_at_ms.saturating_sub(now).max(0) / 1000;
        Ok(format!(
            "reddit2tg HA status\ncluster: {}\nthis: {}\nlocal role: {}\nphase: {}\nowner: {}\nowner priority: {}\nlease remaining: {}s\nhandover: {}\ncontrol generation: {}",
            self.cfg.cluster_id,
            self.cfg.instance_id,
            role.as_str(),
            document.phase.as_str(),
            document.owner.as_deref().unwrap_or("none"),
            document
                .owner_priority
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            lease_remaining,
            document.handover_to.as_deref().unwrap_or("none"),
            document.generation
        ))
    }

    async fn ensure_document(&self) -> Result<PinnedDocument> {
        if let Some(pinned) = self.api.read_pinned().await? {
            self.validate_document(&pinned.document)?;
            return Ok(pinned);
        }

        let document = HaDocument::new(&self.cfg, now_ms());
        let message_id = self.api.send_document(&document).await?;
        self.api.pin_message(message_id).await?;
        sleep(Duration::from_millis(200)).await;

        let pinned = self.api.read_pinned().await?.context(
            "HA control message was sent and pinned but getChat returned no pinned message",
        )?;
        self.validate_document(&pinned.document)?;
        Ok(pinned)
    }

    async fn read_document(&self) -> Result<PinnedDocument> {
        let pinned = self
            .api
            .read_pinned()
            .await?
            .context("HA control chat has no pinned control document")?;
        self.validate_document(&pinned.document)?;
        Ok(pinned)
    }

    async fn update_document<F>(&self, mut mutate: F) -> Result<HaDocument>
    where
        F: FnMut(&mut HaDocument) -> Result<()>,
    {
        for attempt in 0..CONTROL_WRITE_RETRIES {
            let pinned = self.read_document().await?;
            let mut next = pinned.document.clone();
            mutate(&mut next)?;

            if next == pinned.document {
                return Ok(next);
            }

            next.generation = pinned.document.generation.saturating_add(1);
            next.written_at_ms = now_ms();
            self.api.edit_document(pinned.message_id, &next).await?;

            sleep(Duration::from_millis(120)).await;
            let verify = self.read_document().await?;
            if verify.message_id == pinned.message_id && verify.document == next {
                return Ok(next);
            }

            sleep(Duration::from_millis(80 * (attempt as u64 + 1))).await;
        }

        bail!("HA control document update lost concurrent writes too many times")
    }

    fn validate_document(&self, document: &HaDocument) -> Result<()> {
        if !document_matches_config(&self.cfg, document) {
            bail!(
                "HA control document does not match local cluster configuration (cluster_id or members fingerprint differs)"
            );
        }
        Ok(())
    }

    fn owned_by_this_process(&self, document: &HaDocument) -> bool {
        document.owner.as_deref() == Some(self.cfg.instance_id.as_str())
            && document.owner_nonce.as_deref() == Some(self.nonce.as_str())
    }

    fn request_id(&self, update_id: i64) -> String {
        format!("{}-{update_id}", self.cfg.instance_id)
    }

    fn unknown_instance(&self, instance_id: &str) -> String {
        format!("Neznámá instance: {instance_id}. Použij /list-instances.")
    }
}

pub fn validate_config(cfg: &HaConfig) -> Result<()> {
    if !cfg.enabled {
        return Ok(());
    }

    validate_identifier("ha.cluster_id", &cfg.cluster_id)?;
    validate_identifier("ha.instance_id", &cfg.instance_id)?;

    if cfg.control_bot_token.trim().is_empty() {
        bail!("ha.control_bot_token must not be empty when HA is enabled");
    }
    if cfg.control_chat_id == 0 {
        bail!("ha.control_chat_id must not be 0 when HA is enabled");
    }
    if !(15..=300).contains(&cfg.lease_ttl_secs) {
        bail!("ha.lease_ttl_secs must be in 15..300");
    }
    if !(2..cfg.lease_ttl_secs).contains(&cfg.renew_interval_secs) {
        bail!("ha.renew_interval_secs must be at least 2 and lower than lease_ttl_secs");
    }
    if !(1..cfg.lease_ttl_secs).contains(&cfg.claim_settle_secs) {
        bail!("ha.claim_settle_secs must be at least 1 and lower than lease_ttl_secs");
    }
    if !(1..cfg.lease_ttl_secs).contains(&cfg.fail_closed_margin_secs) {
        bail!("ha.fail_closed_margin_secs must be at least 1 and lower than lease_ttl_secs");
    }
    if cfg
        .renew_interval_secs
        .saturating_add(cfg.fail_closed_margin_secs)
        >= cfg.lease_ttl_secs
    {
        bail!("ha.renew_interval_secs + ha.fail_closed_margin_secs must be lower than ha.lease_ttl_secs");
    }
    if cfg.instances.is_empty() {
        bail!("ha.instances must contain at least one instance when HA is enabled");
    }

    let mut ids = HashSet::new();
    for configured in &cfg.instances {
        validate_identifier("ha.instances[].id", &configured.id)?;
        if !ids.insert(configured.id.as_str()) {
            bail!("duplicate HA instance id: {}", configured.id);
        }
    }

    if !cfg
        .instances
        .iter()
        .any(|configured| configured.id == cfg.instance_id)
    {
        bail!(
            "ha.instance_id {} is not present in ha.instances",
            cfg.instance_id
        );
    }

    Ok(())
}

fn validate_identifier(name: &str, value: &str) -> Result<()> {
    let value = value.trim();
    if value.is_empty() {
        bail!("{name} must not be empty when HA is enabled");
    }
    if value.len() > 64 {
        bail!("{name} must not exceed 64 bytes");
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!("{name} may contain only ASCII letters, digits, '.', '_' and '-'");
    }
    Ok(())
}

pub fn member<'a>(cfg: &'a HaConfig, instance_id: &str) -> Option<&'a HaInstanceConfig> {
    cfg.instances
        .iter()
        .find(|configured| configured.id == instance_id)
}

pub fn members_by_priority(cfg: &HaConfig) -> Vec<&HaInstanceConfig> {
    let mut members = cfg.instances.iter().collect::<Vec<_>>();
    members.sort_by(|left, right| {
        left.priority
            .cmp(&right.priority)
            .then_with(|| left.id.cmp(&right.id))
    });
    members
}

pub fn members_fingerprint(cfg: &HaConfig) -> String {
    let mut bytes = Vec::new();
    for configured in members_by_priority(cfg) {
        bytes.extend_from_slice(configured.id.as_bytes());
        bytes.push(b':');
        bytes.extend_from_slice(configured.priority.to_string().as_bytes());
        bytes.push(b':');
        bytes.push(u8::from(configured.auto_failover));
        bytes.push(b':');
        bytes.push(u8::from(configured.allow_fallback));
        bytes.push(b':');
        bytes.push(u8::from(configured.auto_failback));
        bytes.push(b';');
    }

    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

pub fn document_matches_config(cfg: &HaConfig, document: &HaDocument) -> bool {
    document.protocol == HA_PROTOCOL_VERSION
        && document.cluster_id == cfg.cluster_id
        && document.members_fingerprint == members_fingerprint(cfg)
}

pub fn encode_document(document: &HaDocument) -> Result<String> {
    let json = serde_json::to_string(document).context("serialize HA control document")?;
    let encoded = format!("{HA_CONTROL_PREFIX}{json}");
    if encoded.chars().count() > 3900 {
        bail!("HA control document exceeds Telegram message safety limit");
    }
    Ok(encoded)
}

pub fn decode_document(text: &str) -> Option<Result<HaDocument>> {
    let json = text.strip_prefix(HA_CONTROL_PREFIX)?;
    Some(serde_json::from_str(json).context("parse HA control document"))
}

fn election_previous_owner<'a>(
    cfg: &'a HaConfig,
    document: &HaDocument,
) -> Option<&'a HaInstanceConfig> {
    let id = if document.phase == ControlPhase::Claim {
        document.previous_owner.as_deref()
    } else {
        document.owner.as_deref()
    }?;
    member(cfg, id)
}

fn eligible_candidates<'a>(cfg: &'a HaConfig, document: &HaDocument) -> Vec<&'a HaInstanceConfig> {
    if let Some(target) = document.handover_to.as_deref() {
        return member(cfg, target)
            .filter(|target| document.mode(&target.id) == InstanceMode::Auto)
            .into_iter()
            .collect();
    }

    if document.phase == ControlPhase::Paused {
        return Vec::new();
    }

    let previous_owner = election_previous_owner(cfg, document);
    members_by_priority(cfg)
        .into_iter()
        .filter(|candidate| {
            if !candidate.auto_failover || document.mode(&candidate.id) != InstanceMode::Auto {
                return false;
            }
            !matches!(
                previous_owner,
                Some(previous)
                    if candidate.priority > previous.priority && !document.failover_allowed
            )
        })
        .collect()
}

pub fn automatic_candidate<'a>(
    cfg: &'a HaConfig,
    document: &HaDocument,
    now_ms: i64,
) -> Option<&'a HaInstanceConfig> {
    if !document_matches_config(cfg, document)
        || document.owner_is_live(now_ms)
        || document.claim_is_live(now_ms)
    {
        return None;
    }
    eligible_candidates(cfg, document).into_iter().next()
}

pub fn claim_rank(
    cfg: &HaConfig,
    document: &HaDocument,
    now_ms: i64,
    instance_id: &str,
) -> Option<usize> {
    if !document_matches_config(cfg, document) || document.owner_is_live(now_ms) {
        return None;
    }
    eligible_candidates(cfg, document)
        .into_iter()
        .position(|candidate| candidate.id == instance_id)
}

pub fn claim_preemption_allowed(
    cfg: &HaConfig,
    document: &HaDocument,
    instance_id: &str,
    now_ms: i64,
) -> bool {
    if !document.claim_is_live(now_ms) {
        return false;
    }
    let settle_at = document
        .claim_started_at_ms
        .saturating_add((cfg.claim_settle_secs as i64) * 1000);
    if now_ms >= settle_at {
        return false;
    }
    let Some(current_owner) = document.owner.as_deref() else {
        return false;
    };
    let candidates = eligible_candidates(cfg, document);
    let candidate_rank = candidates
        .iter()
        .position(|candidate| candidate.id == instance_id);
    let owner_rank = candidates
        .iter()
        .position(|candidate| candidate.id == current_owner);
    matches!((candidate_rank, owner_rank), (Some(left), Some(right)) if left < right)
}

pub fn should_auto_failback(
    cfg: &HaConfig,
    document: &HaDocument,
    instance_id: &str,
    now_ms: i64,
) -> bool {
    if !document_matches_config(cfg, document) || !document.owner_is_live(now_ms) {
        return false;
    }
    let Some(owner_id) = document.owner.as_deref() else {
        return false;
    };
    if owner_id == instance_id || document.mode(instance_id) != InstanceMode::Auto {
        return false;
    }

    let Some(candidate) = member(cfg, instance_id) else {
        return false;
    };
    let Some(owner) = member(cfg, owner_id) else {
        return false;
    };

    candidate.auto_failback && candidate.priority < owner.priority
}

pub fn parse_command(input: &str) -> Option<std::result::Result<HaCommand, &'static str>> {
    let mut parts = input.split_whitespace();
    let first = parts.next()?;
    let command = first.split('@').next().unwrap_or(first);

    match command {
        "/list-instances" | "/list_instances" => {
            if parts.next().is_none() {
                Some(Ok(HaCommand::ListInstances))
            } else {
                Some(Err("Použití: /list-instances"))
            }
        }
        "/reddit-status" | "/reddit_status" => {
            if parts.next().is_none() {
                Some(Ok(HaCommand::RedditStatus))
            } else {
                Some(Err("Použití: /reddit-status"))
            }
        }
        "/reddit-connect" | "/reddit_connect" => {
            let Some(instance_id) = parts.next() else {
                return Some(Err("Použití: /reddit-connect <instance-id> [force]"));
            };
            let force = match parts.next() {
                None => false,
                Some("force") => true,
                Some(_) => {
                    return Some(Err("Použití: /reddit-connect <instance-id> [force]"));
                }
            };
            if parts.next().is_some() {
                return Some(Err("Použití: /reddit-connect <instance-id> [force]"));
            }
            Some(Ok(HaCommand::Connect {
                instance_id: instance_id.to_owned(),
                force,
            }))
        }
        "/reddit-disconnect" | "/reddit_disconnect" => {
            let Some(instance_id) = parts.next() else {
                return Some(Err("Použití: /reddit-disconnect <instance-id>"));
            };
            if parts.next().is_some() {
                return Some(Err("Použití: /reddit-disconnect <instance-id>"));
            }
            Some(Ok(HaCommand::Disconnect {
                instance_id: instance_id.to_owned(),
            }))
        }
        "/reddit-switch" | "/reddit_switch" => {
            let Some(instance_id) = parts.next() else {
                return Some(Err("Použití: /reddit-switch <instance-id>"));
            };
            if parts.next().is_some() {
                return Some(Err("Použití: /reddit-switch <instance-id>"));
            }
            Some(Ok(HaCommand::Switch {
                instance_id: instance_id.to_owned(),
            }))
        }
        "/maintenance" => {
            let Some(instance_id) = parts.next() else {
                return Some(Err("Použití: /maintenance <instance-id> [on|off]"));
            };
            let result = match parts.next() {
                None => HaCommand::MaintenanceShow {
                    instance_id: instance_id.to_owned(),
                },
                Some("on") => HaCommand::MaintenanceSet {
                    instance_id: instance_id.to_owned(),
                    enabled: true,
                },
                Some("off") => HaCommand::MaintenanceSet {
                    instance_id: instance_id.to_owned(),
                    enabled: false,
                },
                Some(_) => {
                    return Some(Err("Použití: /maintenance <instance-id> [on|off]"));
                }
            };
            if parts.next().is_some() {
                return Some(Err("Použití: /maintenance <instance-id> [on|off]"));
            }
            Some(Ok(result))
        }
        _ => None,
    }
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> HaConfig {
        HaConfig {
            enabled: true,
            cluster_id: "prod".to_owned(),
            instance_id: "ostrava".to_owned(),
            control_bot_token: "123:test".to_owned(),
            control_chat_id: -100123,
            lease_ttl_secs: 30,
            renew_interval_secs: 10,
            claim_settle_secs: 5,
            fail_closed_margin_secs: 5,
            instances: vec![
                HaInstanceConfig {
                    id: "ostrava".to_owned(),
                    priority: 10,
                    auto_failover: true,
                    allow_fallback: true,
                    auto_failback: false,
                },
                HaInstanceConfig {
                    id: "brno".to_owned(),
                    priority: 20,
                    auto_failover: true,
                    allow_fallback: true,
                    auto_failback: false,
                },
                HaInstanceConfig {
                    id: "praha".to_owned(),
                    priority: 30,
                    auto_failover: true,
                    allow_fallback: true,
                    auto_failback: false,
                },
            ],
        }
    }

    #[test]
    fn config_accepts_valid_cluster() {
        validate_config(&cfg()).unwrap();
    }

    #[test]
    fn config_rejects_duplicate_instance_ids() {
        let mut config = cfg();
        config.instances.push(config.instances[0].clone());
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn member_fingerprint_is_order_independent() {
        let first = cfg();
        let mut second = cfg();
        second.instances.reverse();
        assert_eq!(members_fingerprint(&first), members_fingerprint(&second));
    }

    #[test]
    fn control_document_round_trips() {
        let config = cfg();
        let document = HaDocument::new(&config, 1000);
        let encoded = encode_document(&document).unwrap();
        let decoded = decode_document(&encoded).unwrap().unwrap();
        assert_eq!(decoded, document);
        assert!(document_matches_config(&config, &decoded));
    }

    #[test]
    fn automatic_election_chooses_best_priority() {
        let config = cfg();
        let document = HaDocument::new(&config, 1000);
        assert_eq!(
            automatic_candidate(&config, &document, 1001).map(|member| member.id.as_str()),
            Some("ostrava")
        );
        assert_eq!(claim_rank(&config, &document, 1001, "ostrava"), Some(0));
        assert_eq!(claim_rank(&config, &document, 1001, "brno"), Some(1));
        assert_eq!(claim_rank(&config, &document, 1001, "praha"), Some(2));
    }

    #[test]
    fn maintenance_excludes_instance_from_election() {
        let config = cfg();
        let mut document = HaDocument::new(&config, 1000);
        document
            .modes
            .insert("ostrava".to_owned(), InstanceMode::Maintenance);
        assert_eq!(
            automatic_candidate(&config, &document, 1001).map(|member| member.id.as_str()),
            Some("brno")
        );
    }

    #[test]
    fn expired_owner_can_forbid_lower_priority_fallback() {
        let config = cfg();
        let mut document = HaDocument::new(&config, 1000);
        document.owner = Some("ostrava".to_owned());
        document.owner_priority = Some(10);
        document.lease_expires_at_ms = 1500;
        document.failover_allowed = false;
        document
            .modes
            .insert("ostrava".to_owned(), InstanceMode::Disconnected);
        assert_eq!(
            automatic_candidate(&config, &document, 2000).map(|member| member.id.as_str()),
            None
        );
    }

    #[test]
    fn explicit_handover_targets_only_requested_instance() {
        let config = cfg();
        let mut document = HaDocument::new(&config, 1000);
        document.phase = ControlPhase::Command;
        document.handover_to = Some("praha".to_owned());
        assert_eq!(
            automatic_candidate(&config, &document, 1001).map(|member| member.id.as_str()),
            Some("praha")
        );
        assert_eq!(claim_rank(&config, &document, 1001, "ostrava"), None);
        assert_eq!(claim_rank(&config, &document, 1001, "praha"), Some(0));
    }

    #[test]
    fn auto_failback_requires_explicit_member_policy() {
        let mut config = cfg();
        config.instances[0].auto_failback = true;
        let mut document = HaDocument::new(&config, 1000);
        document.phase = ControlPhase::Lease;
        document.owner = Some("brno".to_owned());
        document.owner_nonce = Some("brno-1".to_owned());
        document.owner_priority = Some(20);
        document.lease_expires_at_ms = 10_000;
        assert!(should_auto_failback(&config, &document, "ostrava", 2_000));
        assert!(!should_auto_failback(&config, &document, "praha", 2_000));
    }

    #[test]
    fn better_candidate_can_preempt_only_during_claim_settle_window() {
        let config = cfg();
        let mut document = HaDocument::new(&config, 1000);
        document.phase = ControlPhase::Claim;
        document.owner = Some("brno".to_owned());
        document.owner_nonce = Some("brno-1".to_owned());
        document.owner_priority = Some(20);
        document.claim_started_at_ms = 2000;
        document.lease_expires_at_ms = 30_000;
        assert!(claim_preemption_allowed(
            &config, &document, "ostrava", 3000
        ));
        assert!(!claim_preemption_allowed(&config, &document, "praha", 3000));
        assert!(!claim_preemption_allowed(
            &config, &document, "ostrava", 8000
        ));
    }

    #[test]
    fn process_incarnation_is_part_of_ownership() {
        let config = cfg();
        let controller = HaController::new(&config, Some("ostrava-new"))
            .unwrap()
            .unwrap();
        let mut document = HaDocument::new(&config, 1000);
        document.owner = Some("ostrava".to_owned());
        document.owner_nonce = Some("ostrava-old".to_owned());
        document.owner_priority = Some(10);
        document.lease_expires_at_ms = 10_000;
        assert!(!controller.owned_by_this_process(&document));
        document.owner_nonce = Some("ostrava-new".to_owned());
        assert!(controller.owned_by_this_process(&document));
    }

    #[test]
    fn parses_requested_general_commands() {
        assert_eq!(
            parse_command("/reddit-connect brno force"),
            Some(Ok(HaCommand::Connect {
                instance_id: "brno".to_owned(),
                force: true,
            }))
        );
        assert_eq!(
            parse_command("/reddit-disconnect ostrava"),
            Some(Ok(HaCommand::Disconnect {
                instance_id: "ostrava".to_owned(),
            }))
        );
        assert_eq!(
            parse_command("/reddit-switch praha"),
            Some(Ok(HaCommand::Switch {
                instance_id: "praha".to_owned(),
            }))
        );
        assert_eq!(
            parse_command("/maintenance brno on"),
            Some(Ok(HaCommand::MaintenanceSet {
                instance_id: "brno".to_owned(),
                enabled: true,
            }))
        );
        assert_eq!(
            parse_command("/maintenance brno"),
            Some(Ok(HaCommand::MaintenanceShow {
                instance_id: "brno".to_owned(),
            }))
        );
        assert_eq!(
            parse_command("/list-instances@reddit2tg_bot"),
            Some(Ok(HaCommand::ListInstances))
        );
        assert_eq!(
            parse_command("/reddit-status"),
            Some(Ok(HaCommand::RedditStatus))
        );
    }

    #[test]
    fn rejects_bad_command_shapes() {
        assert!(parse_command("/reddit-connect").unwrap().is_err());
        assert!(parse_command("/reddit-connect brno now").unwrap().is_err());
        assert!(parse_command("/maintenance brno maybe").unwrap().is_err());
        assert!(parse_command("/list-instances extra").unwrap().is_err());
    }
}
