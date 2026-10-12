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
    db::{Db, ReplicationChange, ReplicationSnapshot},
    reddit::RedditClient,
    replication::{derive_key, open_json, random_epoch, seal_json, sha256_hex, split_chunks},
};

pub const HA_PROTOCOL_VERSION: u32 = 3;
pub const HA_CONTROL_PREFIX: &str = "reddit2tg-ha-v3\n";
const HA_LEGACY_CONTROL_PREFIX: &str = "reddit2tg-ha-v2\n";
pub const HA_CONTROL_TOPIC_NAME: &str = "ha-control-room";
pub const HA_SYNC_TOPIC_NAME: &str = "ha-sync";
const CONTROL_POLL_SECS: u64 = 2;
const REPLICATION_PAYLOAD_PROTOCOL: u32 = 2;
const REPLICATION_EPOCH_META: &str = "ha_replication_epoch";
const REPLICATION_DELTA_CHANGE_LIMIT: usize = 512;
const REPLICATION_MAX_CHAIN: usize = 10_000;
const TELEGRAM_FILE_DOWNLOAD_LIMIT: usize = 20 * 1024 * 1024;
const CONTROL_CLAIM_STAGGER_MS: i64 = 1_000;
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
pub struct ReplicationFilePart {
    pub file_id: String,
    pub message_id: i64,
    pub sha256: String,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationHeadRef {
    pub generation: u64,
    pub file_id: String,
    pub message_id: i64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationSnapshotRef {
    pub generation: u64,
    pub created_at_ms: i64,
    pub state_sha256: String,
    pub manifest_file_id: String,
    pub manifest_message_id: i64,
    pub manifest_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationGcRef {
    pub file_id: String,
    pub message_id: i64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReplicationDelta {
    protocol: u32,
    cluster_id: String,
    epoch: String,
    generation: u64,
    previous_generation: u64,
    previous_file_id: Option<String>,
    previous_message_id: Option<i64>,
    previous_sha256: Option<String>,
    source_instance: String,
    changes: Vec<ReplicationChange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReplicationSnapshotEnvelope {
    protocol: u32,
    cluster_id: String,
    epoch: String,
    generation: u64,
    source_instance: String,
    snapshot: ReplicationSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReplicationSnapshotManifest {
    protocol: u32,
    cluster_id: String,
    epoch: String,
    generation: u64,
    state_sha256: String,
    payload_sha256: String,
    parts: Vec<ReplicationFilePart>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReplicationGcPlan {
    protocol: u32,
    cluster_id: String,
    epoch: String,
    created_at_ms: i64,
    message_ids: Vec<i64>,
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
    #[serde(default)]
    pub control_owner: Option<String>,
    #[serde(default)]
    pub control_owner_nonce: Option<String>,
    #[serde(default)]
    pub control_lease_expires_at_ms: i64,
    #[serde(default)]
    pub control_update_offset: i64,
    #[serde(default)]
    pub data_update_offset: i64,
    #[serde(default)]
    pub sync_thread_id: Option<i64>,
    #[serde(default)]
    pub replication_epoch: Option<String>,
    #[serde(default)]
    pub replication_generation: u64,
    #[serde(default)]
    pub replication_head: Option<ReplicationHeadRef>,
    #[serde(default)]
    pub replication_snapshot: Option<ReplicationSnapshotRef>,
    #[serde(default)]
    pub replication_previous_snapshot: Option<ReplicationSnapshotRef>,
    #[serde(default)]
    pub replication_previous_head: Option<ReplicationHeadRef>,
    #[serde(default)]
    pub replication_gc: Option<ReplicationGcRef>,
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
            control_owner: None,
            control_owner_nonce: None,
            control_lease_expires_at_ms: now_ms,
            control_update_offset: 0,
            data_update_offset: 0,
            sync_thread_id: None,
            replication_epoch: None,
            replication_generation: 0,
            replication_head: None,
            replication_snapshot: None,
            replication_previous_snapshot: None,
            replication_previous_head: None,
            replication_gc: None,
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

    pub fn control_owner_is_live(&self, now_ms: i64) -> bool {
        self.control_owner.is_some() && self.control_lease_expires_at_ms > now_ms
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
    state_message_id: Arc<Mutex<Option<i64>>>,
    sync_thread_id_cache: Arc<Mutex<Option<i64>>>,
    document_write_lock: Arc<Mutex<()>>,
    replication_lock: Arc<Mutex<()>>,
    last_replication_flush_ms: Arc<Mutex<i64>>,
    replication_key: Arc<[u8; 32]>,
}

#[derive(Debug, Clone)]
struct PinnedDocument {
    message_id: i64,
    document: HaDocument,
}

#[derive(Debug, Clone)]
struct SentReplicationFile {
    file_id: String,
    message_id: i64,
}

#[derive(Clone)]
struct HaTelegramClient {
    http: Client,
    base: String,
    file_base: String,
    chat_id: i64,
    thread_id: i64,
    operator_user_id: i64,
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

#[derive(Debug, Deserialize)]
struct HaUpdate {
    update_id: i64,
    message: Option<HaIncomingMessage>,
}

#[derive(Debug, Deserialize)]
struct HaIncomingMessage {
    message_thread_id: Option<i64>,
    chat: HaIncomingChat,
    from: Option<HaIncomingUser>,
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct HaIncomingChat {
    id: i64,
}

#[derive(Debug, Deserialize)]
struct HaIncomingUser {
    id: i64,
    #[serde(default)]
    is_bot: bool,
}

#[derive(Debug, Serialize)]
struct HaGetUpdatesBody {
    offset: i64,
    timeout: u64,
    allowed_updates: [&'static str; 1],
}

impl HaTelegramClient {
    fn new(bot_token: &str, chat_id: i64, thread_id: i64, operator_user_id: i64) -> Result<Self> {
        let http = Client::builder().timeout(Duration::from_secs(30)).build()?;
        let token = bot_token.trim();
        Ok(Self {
            http,
            base: format!("https://api.telegram.org/bot{token}"),
            file_base: format!("https://api.telegram.org/file/bot{token}"),
            chat_id,
            thread_id,
            operator_user_id,
        })
    }

    async fn probe(&self) -> Result<()> {
        let me: Value = self.call("getMe", &serde_json::json!({})).await?;
        let bot_id = me
            .get("id")
            .and_then(Value::as_i64)
            .context("HA control bot getMe response has no numeric bot id")?;

        let webhook: Value = self.call("getWebhookInfo", &serde_json::json!({})).await?;
        if webhook
            .get("url")
            .and_then(Value::as_str)
            .is_some_and(|url| !url.is_empty())
        {
            bail!("HA control bot currently has a webhook configured; remove it before HA getUpdates polling");
        }

        let chat: Value = self
            .call("getChat", &serde_json::json!({"chat_id": self.chat_id}))
            .await?;
        if chat.get("type").and_then(Value::as_str) != Some("supergroup") {
            bail!("HA control bot must use the same Telegram supergroup as telegram.chat_id");
        }
        if chat.get("is_forum").and_then(Value::as_bool) != Some(true) {
            bail!(
                "HA control bot requires the configured Telegram supergroup to have Topics enabled"
            );
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
        let can_manage_topics = member
            .get("can_manage_topics")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if status != "creator"
            && !(status == "administrator" && can_pin_messages && can_manage_topics)
        {
            bail!(
                "HA bot must be an administrator with Pin Messages and Manage Topics permissions"
            );
        }
        Ok(())
    }

    async fn create_sync_topic(&self) -> Result<i64> {
        let topic: Value = self
            .call(
                "createForumTopic",
                &serde_json::json!({
                    "chat_id": self.chat_id,
                    "name": HA_SYNC_TOPIC_NAME,
                }),
            )
            .await?;
        topic
            .get("message_thread_id")
            .and_then(Value::as_i64)
            .context("HA createForumTopic response has no message_thread_id")
    }

    async fn send_replication_file(
        &self,
        thread_id: i64,
        file_name: &str,
        caption: &str,
        data: &[u8],
    ) -> Result<SentReplicationFile> {
        let url = format!("{}/sendDocument", self.base);

        for retry in 0..=MAX_CONTROL_RATE_LIMIT_RETRIES {
            let part = reqwest::multipart::Part::bytes(data.to_vec())
                .file_name(file_name.to_owned())
                .mime_str("application/octet-stream")?;
            let form = reqwest::multipart::Form::new()
                .text("chat_id", self.chat_id.to_string())
                .text("message_thread_id", thread_id.to_string())
                .text("caption", caption.to_owned())
                .text("disable_notification", "true")
                .part("document", part);

            let response = self
                .http
                .post(&url)
                .multipart(form)
                .send()
                .await
                .context("HA Telegram sendDocument request failed")?;
            let status = response.status();
            let parsed: ApiResponse<Value> = response.json().await.with_context(|| {
                format!("HA Telegram sendDocument returned invalid JSON (HTTP {status})")
            })?;

            if parsed.ok {
                let message = parsed
                    .result
                    .context("HA Telegram sendDocument returned ok without result")?;
                let message_id = message
                    .get("message_id")
                    .and_then(Value::as_i64)
                    .context("HA Telegram sendDocument response has no message_id")?;
                let file_id = message
                    .get("document")
                    .and_then(|document| document.get("file_id"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .context("HA Telegram sendDocument response has no document.file_id")?;
                return Ok(SentReplicationFile {
                    file_id,
                    message_id,
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
                "HA Telegram sendDocument failed (HTTP {status}): {}",
                parsed
                    .description
                    .unwrap_or_else(|| "unknown error".to_owned())
            );
        }

        unreachable!()
    }

    async fn delete_messages_best_effort(&self, message_ids: &[i64]) -> (usize, usize) {
        let mut deleted = 0usize;
        let mut failed = 0usize;

        for chunk in message_ids.chunks(100) {
            if chunk.is_empty() {
                continue;
            }

            let bulk: Result<bool> = self
                .call(
                    "deleteMessages",
                    &serde_json::json!({
                        "chat_id": self.chat_id,
                        "message_ids": chunk,
                    }),
                )
                .await;

            if bulk.is_ok() {
                deleted = deleted.saturating_add(chunk.len());
                continue;
            }

            warn!(
                count = chunk.len(),
                "HA replication bulk GC delete failed; retrying messages individually"
            );
            for message_id in chunk {
                let single: Result<bool> = self
                    .call(
                        "deleteMessage",
                        &serde_json::json!({
                            "chat_id": self.chat_id,
                            "message_id": message_id,
                        }),
                    )
                    .await;
                match single {
                    Ok(_) => deleted = deleted.saturating_add(1),
                    Err(error) => {
                        failed = failed.saturating_add(1);
                        warn!(
                            error = %format!("{error:#}"),
                            message_id,
                            "HA replication artifact could not be deleted; leaving harmless orphan"
                        );
                    }
                }
            }
        }

        (deleted, failed)
    }

    async fn download_replication_file(&self, file_id: &str) -> Result<Vec<u8>> {
        let file: Value = self
            .call("getFile", &serde_json::json!({"file_id": file_id}))
            .await?;
        let file_path = file
            .get("file_path")
            .and_then(Value::as_str)
            .context("HA Telegram getFile response has no file_path")?;
        let response = self
            .http
            .get(format!("{}/{}", self.file_base, file_path))
            .send()
            .await
            .context("HA Telegram replication file download failed")?
            .error_for_status()
            .context("HA Telegram replication file download returned HTTP error")?;
        let bytes = response
            .bytes()
            .await
            .context("HA Telegram replication file body could not be read")?;
        if bytes.len() > TELEGRAM_FILE_DOWNLOAD_LIMIT {
            bail!(
                "HA replication file exceeds Telegram Bot API download safety limit: {} bytes",
                bytes.len()
            );
        }
        Ok(bytes.to_vec())
    }

    async fn read_pinned(&self) -> Result<Option<PinnedDocument>> {
        let chat: Value = self
            .call("getChat", &serde_json::json!({"chat_id": self.chat_id}))
            .await?;
        let Some(message) = chat.get("pinned_message") else {
            return Ok(None);
        };

        let thread_id = message.get("message_thread_id").and_then(Value::as_i64);
        if thread_id != Some(self.thread_id) {
            bail!(
                "latest pinned Telegram message is outside configured HA thread {}; HA fails closed until the HA state message is the latest pin",
                self.thread_id
            );
        }

        let message_id = message
            .get("message_id")
            .and_then(Value::as_i64)
            .context("HA pinned message has no numeric message_id")?;
        let text = message
            .get("text")
            .and_then(Value::as_str)
            .context("HA pinned control message is not a text message")?;
        let Some(decoded) = decode_document(text) else {
            bail!(
                "latest pinned message in configured HA thread {} is not a reddit2tg HA document",
                self.thread_id
            );
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
                    "message_thread_id": self.thread_id,
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

    async fn ensure_pinned(&self, message_id: i64) -> Result<()> {
        match self.pin_message(message_id).await {
            Ok(()) => Ok(()),
            Err(error)
                if format!("{error:#}")
                    .to_ascii_lowercase()
                    .contains("already pinned") =>
            {
                Ok(())
            }
            Err(error) => Err(error),
        }
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
        self.ensure_pinned(message_id).await?;
        Ok(())
    }

    async fn get_updates(&self, offset: i64, timeout_secs: u64) -> Result<Vec<HaUpdate>> {
        self.call(
            "getUpdates",
            &HaGetUpdatesBody {
                offset,
                timeout: timeout_secs,
                allowed_updates: ["message"],
            },
        )
        .await
    }

    fn accepts_admin(&self, message: &HaIncomingMessage) -> bool {
        if message.chat.id != self.chat_id || message.message_thread_id != Some(self.thread_id) {
            return false;
        }
        let Some(from) = message.from.as_ref() else {
            return false;
        };
        from.id == self.operator_user_id && !from.is_bot
    }

    async fn send_admin(&self, text: &str) -> Result<()> {
        for chunk in split_control_text(text, 3900) {
            let _: Value = self
                .call(
                    "sendMessage",
                    &serde_json::json!({
                        "chat_id": self.chat_id,
                        "message_thread_id": self.thread_id,
                        "text": chunk,
                    }),
                )
                .await?;
        }
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

    pub fn new(
        cfg: &HaConfig,
        telegram_chat_id: i64,
        operator_user_id: i64,
        process_nonce: Option<&str>,
    ) -> Result<Option<Self>> {
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
            api: HaTelegramClient::new(
                &cfg.control_bot_token,
                telegram_chat_id,
                cfg.control_thread_id,
                operator_user_id,
            )?,
            nonce: Arc::new(nonce),
            role: Arc::new(RwLock::new(LocalRole::Standby)),
            lease_expires_at_ms: Arc::new(Mutex::new(0)),
            claim_cooldown_until_ms: Arc::new(Mutex::new(0)),
            state_message_id: Arc::new(Mutex::new(None)),
            sync_thread_id_cache: Arc::new(Mutex::new(None)),
            document_write_lock: Arc::new(Mutex::new(())),
            replication_lock: Arc::new(Mutex::new(())),
            last_replication_flush_ms: Arc::new(Mutex::new(0)),
            replication_key: Arc::new(derive_key(&cfg.control_bot_token, &cfg.cluster_id)),
        }))
    }

    pub async fn probe(&self) -> Result<()> {
        self.api.probe().await?;
        let document = self.ensure_document().await?;
        self.validate_document(&document.document)?;
        Ok(())
    }

    pub async fn run(&self, reddit: RedditClient, db: Db) -> Result<()> {
        self.force_local_standby(&reddit).await?;
        self.ensure_document().await?;

        let mut backoff = 1u64;
        loop {
            match self.runtime_tick(&reddit, &db).await {
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

    pub async fn shutdown(&self, reddit: &RedditClient, db: &Db) {
        if let Err(error) = self.graceful_shutdown(reddit, db).await {
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

        // The local expiry cache is only an optimization. Another node may
        // have replaced the canonical Telegram document after our previous
        // successful write, so verify ownership before every new active
        // bridge operation.
        let canonical = match self.read_document().await {
            Ok(pinned) => pinned.document,
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    instance_id = %self.cfg.instance_id,
                    "HA active permit denied because canonical lease could not be verified"
                );
                return None;
            }
        };
        let now = now_ms();
        if !self.owned_by_this_process(&canonical) || !canonical.owner_is_live(now) {
            return None;
        }

        *self.lease_expires_at_ms.lock().await = canonical.lease_expires_at_ms;
        let safe_until_ms = canonical
            .lease_expires_at_ms
            .saturating_sub((self.cfg.fail_closed_margin_secs as i64) * 1000);
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

    async fn runtime_tick(&self, reddit: &RedditClient, db: &Db) -> Result<()> {
        if let Err(error) = self.control_poller_tick().await {
            warn!(
                error = %format!("{error:#}"),
                instance_id = %self.cfg.instance_id,
                "HA admin control poller iteration failed"
            );
        }
        self.ensure_sync_topic().await?;
        self.replication_tick(db).await?;
        self.tick(reddit, db).await
    }

    async fn control_poller_tick(&self) -> Result<()> {
        let pinned = self.read_document().await?;
        let document = pinned.document;
        let now = now_ms();

        // During a live Reddit lease/claim, the Reddit owner is also the only
        // HA-document writer allowed to drive the command poller. This avoids
        // a second node renewing control state from a stale document and
        // overwriting the Reddit lease.
        if (document.owner_is_live(now) || document.claim_is_live(now))
            && !self.owned_by_this_process(&document)
        {
            return Ok(());
        }

        if document.control_owner_is_live(now) {
            if !self.control_owned_by_this_process(&document) {
                return Ok(());
            }

            let renew_at_ms = document.control_lease_expires_at_ms.saturating_sub(
                ((self.cfg.lease_ttl_secs - self.cfg.renew_interval_secs) as i64) * 1000,
            );
            if now >= renew_at_ms {
                let instance_id = self.cfg.instance_id.clone();
                let nonce = self.nonce.as_str().to_owned();
                let ttl_ms = (self.cfg.lease_ttl_secs as i64) * 1000;
                self.update_document(|current| {
                    if current.control_owner.as_deref() == Some(instance_id.as_str())
                        && current.control_owner_nonce.as_deref() == Some(nonce.as_str())
                    {
                        current.control_lease_expires_at_ms = now_ms().saturating_add(ttl_ms);
                    }
                    Ok(())
                })
                .await?;
            }

            return self.poll_control_updates().await;
        }

        let rank = members_by_priority(&self.cfg)
            .iter()
            .position(|member| member.id == self.cfg.instance_id)
            .context("local HA member disappeared from configuration")?;
        let base_ms = document
            .control_lease_expires_at_ms
            .max(document.written_at_ms);
        let claim_at_ms =
            base_ms.saturating_add((rank as i64).saturating_mul(CONTROL_CLAIM_STAGGER_MS));
        if now < claim_at_ms {
            return Ok(());
        }

        let instance_id = self.cfg.instance_id.clone();
        let nonce = self.nonce.as_str().to_owned();
        let ttl_ms = (self.cfg.lease_ttl_secs as i64) * 1000;
        let claimed = self
            .update_document(|current| {
                let current_now = now_ms();
                if current.control_owner_is_live(current_now) {
                    return Ok(());
                }
                current.control_owner = Some(instance_id.clone());
                current.control_owner_nonce = Some(nonce.clone());
                current.control_lease_expires_at_ms = current_now.saturating_add(ttl_ms);
                Ok(())
            })
            .await?;

        if self.control_owned_by_this_process(&claimed) {
            info!(
                instance_id = %self.cfg.instance_id,
                "HA admin control poller lease acquired"
            );
            self.poll_control_updates().await?;
        }
        Ok(())
    }

    async fn poll_control_updates(&self) -> Result<()> {
        let offset = self.read_document().await?.document.control_update_offset;
        let updates = self.api.get_updates(offset, 0).await?;

        for update in updates {
            let next_offset = update.update_id.saturating_add(1);
            if let Some(message) = update.message.as_ref() {
                if self.api.accepts_admin(message) {
                    if let Some(text) = message.text.as_deref() {
                        let mut parts = text.split_whitespace();
                        let first = parts.next().unwrap_or("");
                        let base = first.split('@').next().unwrap_or(first);
                        if base == "/help" {
                            if parts.next().is_none() {
                                self.api.send_admin(control_help_text()).await?;
                            } else {
                                self.api.send_admin("Použití: /help").await?;
                            }
                        } else if let Some(command) = parse_command(text) {
                            let response = match command {
                                Ok(command) => {
                                    self.execute_command(command, update.update_id).await?
                                }
                                Err(usage) => usage.to_owned(),
                            };
                            self.api.send_admin(&response).await?;
                        }
                    }
                }
            }
            self.commit_control_update_offset(next_offset).await?;
        }

        Ok(())
    }

    async fn commit_control_update_offset(&self, offset: i64) -> Result<()> {
        let instance_id = self.cfg.instance_id.clone();
        let nonce = self.nonce.to_string();
        self.update_document(|document| {
            if document.control_owner.as_deref() != Some(instance_id.as_str())
                || document.control_owner_nonce.as_deref() != Some(nonce.as_str())
                || !document.control_owner_is_live(now_ms())
            {
                return Ok(());
            }
            document.control_update_offset = document.control_update_offset.max(offset);
            Ok(())
        })
        .await?;
        Ok(())
    }

    pub async fn data_update_offset(&self) -> Result<i64> {
        Ok(self.read_document().await?.document.data_update_offset)
    }

    pub async fn commit_data_update_offset(&self, offset: i64) -> Result<()> {
        let instance_id = self.cfg.instance_id.clone();
        let nonce = self.nonce.to_string();
        self.update_document(|document| {
            if document.owner.as_deref() != Some(instance_id.as_str())
                || document.owner_nonce.as_deref() != Some(nonce.as_str())
                || !document.owner_is_live(now_ms())
            {
                return Ok(());
            }
            document.data_update_offset = document.data_update_offset.max(offset);
            Ok(())
        })
        .await?;
        Ok(())
    }

    pub async fn is_reserved_thread(&self, message_thread_id: Option<i64>) -> bool {
        if message_thread_id == Some(self.cfg.control_thread_id) {
            return true;
        }
        let sync_thread_id = *self.sync_thread_id_cache.lock().await;
        message_thread_id.is_some() && message_thread_id == sync_thread_id
    }

    async fn ensure_sync_topic(&self) -> Result<Option<i64>> {
        let document = self.read_document().await?.document;
        if let Some(thread_id) = document.sync_thread_id {
            *self.sync_thread_id_cache.lock().await = Some(thread_id);
            return Ok(Some(thread_id));
        }

        if !self.control_owned_by_this_process(&document)
            || !document.control_owner_is_live(now_ms())
        {
            return Ok(None);
        }

        let created_thread_id = self.api.create_sync_topic().await?;
        let instance_id = self.cfg.instance_id.clone();
        let nonce = self.nonce.to_string();
        let updated = self
            .update_document(|current| {
                if current.sync_thread_id.is_some() {
                    return Ok(());
                }
                if current.control_owner.as_deref() != Some(instance_id.as_str())
                    || current.control_owner_nonce.as_deref() != Some(nonce.as_str())
                    || !current.control_owner_is_live(now_ms())
                {
                    bail!("HA control ownership changed while creating ha-sync");
                }
                current.sync_thread_id = Some(created_thread_id);
                Ok(())
            })
            .await?;

        *self.sync_thread_id_cache.lock().await = updated.sync_thread_id;
        if updated.sync_thread_id == Some(created_thread_id) {
            info!(
                instance_id = %self.cfg.instance_id,
                telegram_thread_id = created_thread_id,
                "HA replication topic created"
            );
        } else {
            warn!(
                created_thread_id,
                canonical_thread_id = ?updated.sync_thread_id,
                "HA sync topic was created concurrently; unused topic may remain"
            );
        }
        Ok(updated.sync_thread_id)
    }

    fn control_owned_by_this_process(&self, document: &HaDocument) -> bool {
        document.control_owner.as_deref() == Some(self.cfg.instance_id.as_str())
            && document.control_owner_nonce.as_deref() == Some(self.nonce.as_str())
    }

    pub async fn flush_replication(&self, db: &Db) -> Result<()> {
        let _guard = self.replication_lock.lock().await;
        self.flush_all_pending_replication_locked(db).await?;
        *self.last_replication_flush_ms.lock().await = now_ms();
        Ok(())
    }

    async fn flush_all_pending_replication_locked(&self, db: &Db) -> Result<()> {
        loop {
            if db.pending_replication_changes(1)?.is_empty() {
                return Ok(());
            }

            let before_generation = db.replication_generation()?;
            self.publish_pending_replication_locked(db).await?;
            let after_generation = db.replication_generation()?;
            if after_generation == before_generation {
                bail!("HA replication flush made no progress at generation {before_generation}");
            }
        }
    }

    async fn flush_replication_before_release(&self, db: &Db) -> Result<()> {
        let _guard = self.replication_lock.lock().await;
        self.flush_all_pending_replication_locked(db).await?;

        let document = self.read_document().await?.document;
        if !self.owned_by_this_process(&document) || !document.owner_is_live(now_ms()) {
            bail!("HA lease changed or expired before final replication flush");
        }
        if !self.replication_ready(db, &document)? {
            bail!("HA replication is not fully synchronized after final flush");
        }

        *self.last_replication_flush_ms.lock().await = now_ms();
        Ok(())
    }

    async fn replication_tick(&self, db: &Db) -> Result<()> {
        let _guard = self.replication_lock.lock().await;
        let mut document = self.read_document().await?.document;

        if document.sync_thread_id.is_none() {
            return Ok(());
        }

        if document.replication_epoch.is_none() {
            self.bootstrap_replication_locked(db, &document).await?;
            document = self.read_document().await?.document;
            if document.replication_epoch.is_none() {
                return Ok(());
            }
        }

        let owned_active =
            self.owned_by_this_process(&document) && document.owner_is_live(now_ms());
        if owned_active {
            if let Err(error) = self.retry_replication_gc_locked().await {
                warn!(
                    error = %format!("{error:#}"),
                    instance_id = %self.cfg.instance_id,
                    "HA replication garbage collection failed; bridge continues and GC will retry"
                );
            }

            let now = now_ms();
            let last = *self.last_replication_flush_ms.lock().await;
            if now.saturating_sub(last) >= self.cfg.replication_batch_ms.min(i64::MAX as u64) as i64
            {
                self.publish_pending_replication_locked(db).await?;
                *self.last_replication_flush_ms.lock().await = now;
            }
            self.maybe_publish_snapshot_locked(db).await?;

            if let Err(error) = self.retry_replication_gc_locked().await {
                warn!(
                    error = %format!("{error:#}"),
                    instance_id = %self.cfg.instance_id,
                    "HA replication garbage collection failed after checkpoint; bridge continues"
                );
            }
        } else {
            self.catch_up_replication_locked(db, &document).await?;
        }
        Ok(())
    }

    fn replication_ready(&self, db: &Db, document: &HaDocument) -> Result<bool> {
        let Some(epoch) = document.replication_epoch.as_deref() else {
            return Ok(false);
        };
        let Some(snapshot) = document.replication_snapshot.as_ref() else {
            return Ok(false);
        };
        if document.sync_thread_id.is_none() {
            return Ok(false);
        }
        if db.get_meta(REPLICATION_EPOCH_META)?.as_deref() != Some(epoch) {
            return Ok(false);
        }
        if db.replication_generation()? != document.replication_generation {
            return Ok(false);
        }
        if document.replication_generation > snapshot.generation {
            if document
                .replication_head
                .as_ref()
                .is_none_or(|head| head.generation != document.replication_generation)
            {
                return Ok(false);
            }
        } else if document.replication_generation == snapshot.generation
            && document.replication_head.is_some()
        {
            return Ok(false);
        }
        if !db.pending_replication_changes(1)?.is_empty() {
            return Ok(false);
        }
        Ok(true)
    }

    async fn bootstrap_replication_locked(&self, db: &Db, observed: &HaDocument) -> Result<()> {
        if observed.replication_epoch.is_some()
            || observed.owner_is_live(now_ms())
            || observed.claim_is_live(now_ms())
        {
            return Ok(());
        }
        let Some(thread_id) = observed.sync_thread_id else {
            return Ok(());
        };
        if claim_rank(&self.cfg, observed, now_ms(), &self.cfg.instance_id) != Some(0) {
            return Ok(());
        }

        let (snapshot, baseline_through_seq) = db.export_replication_snapshot_with_watermark()?;
        let epoch = random_epoch()?;
        let upload = self.upload_snapshot(thread_id, &epoch, &snapshot).await?;
        let local_telegram_offset = db
            .get_meta("telegram_offset")?
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);

        let updated = self
            .update_document(|document| {
                if document.replication_epoch.is_some()
                    || document.owner_is_live(now_ms())
                    || document.claim_is_live(now_ms())
                {
                    return Ok(());
                }
                if document.sync_thread_id != Some(thread_id) {
                    bail!("ha-sync topic changed during replication bootstrap");
                }
                if claim_rank(&self.cfg, document, now_ms(), &self.cfg.instance_id) != Some(0) {
                    return Ok(());
                }

                document.replication_epoch = Some(epoch.clone());
                document.replication_generation = snapshot.generation;
                document.replication_head = None;
                document.replication_snapshot = Some(upload.clone());
                document.replication_previous_snapshot = None;
                document.replication_previous_head = None;
                document.replication_gc = None;
                document.data_update_offset =
                    document.data_update_offset.max(local_telegram_offset);
                Ok(())
            })
            .await?;

        if updated.replication_epoch.as_deref() == Some(epoch.as_str()) {
            db.establish_replication_baseline(snapshot.generation, baseline_through_seq)?;
            db.set_meta(REPLICATION_EPOCH_META, &epoch)?;
            info!(
                instance_id = %self.cfg.instance_id,
                replication_generation = snapshot.generation,
                data_update_offset = updated.data_update_offset,
                telegram_thread_id = thread_id,
                "HA replication baseline snapshot published"
            );
        }
        Ok(())
    }

    async fn publish_pending_replication_locked(&self, db: &Db) -> Result<()> {
        let document = self.read_document().await?.document;
        let now = now_ms();
        if !self.owned_by_this_process(&document) || !document.owner_is_live(now) {
            return Ok(());
        }

        let epoch = document
            .replication_epoch
            .as_deref()
            .context("active HA owner has no replication epoch")?;
        let thread_id = document
            .sync_thread_id
            .context("active HA owner has no ha-sync topic")?;
        if db.get_meta(REPLICATION_EPOCH_META)?.as_deref() != Some(epoch) {
            bail!("active HA node has a different replication epoch than the cluster");
        }
        let local_generation = db.replication_generation()?;
        if local_generation != document.replication_generation {
            bail!(
                "active HA node replication generation is stale: local={}, cluster={}",
                local_generation,
                document.replication_generation
            );
        }

        let mut changes = db.pending_replication_changes(REPLICATION_DELTA_CHANGE_LIMIT)?;
        if changes.is_empty() {
            return Ok(());
        }

        let max_payload = (self.cfg.replication_snapshot_chunk_mib as usize) * 1024 * 1024;
        let (sealed, delta) = loop {
            let candidate = ReplicationDelta {
                protocol: REPLICATION_PAYLOAD_PROTOCOL,
                cluster_id: self.cfg.cluster_id.clone(),
                epoch: epoch.to_owned(),
                generation: document.replication_generation.saturating_add(1),
                previous_generation: document.replication_generation,
                previous_file_id: document
                    .replication_head
                    .as_ref()
                    .map(|head| head.file_id.clone()),
                previous_message_id: document
                    .replication_head
                    .as_ref()
                    .map(|head| head.message_id),
                previous_sha256: document
                    .replication_head
                    .as_ref()
                    .map(|head| head.sha256.clone()),
                source_instance: self.cfg.instance_id.clone(),
                changes: changes.clone(),
            };
            let aad = format!("delta:{}:{}", self.cfg.cluster_id, epoch);
            let (candidate_sealed, _) = seal_json(&candidate, &self.replication_key, &aad)?;
            if candidate_sealed.len() <= max_payload {
                break (candidate_sealed, candidate);
            }
            if changes.len() <= 1 {
                bail!(
                    "single HA replication change exceeds configured {} MiB payload limit",
                    self.cfg.replication_snapshot_chunk_mib
                );
            }
            changes.truncate(changes.len().div_ceil(2));
        };

        let sealed_sha256 = sha256_hex(&sealed);
        let file_name = format!("reddit2tg-ha-delta-{:020}.bin", delta.generation);
        let caption = format!(
            "reddit2tg HA delta · generation {} · source {}",
            delta.generation, self.cfg.instance_id
        );
        let sent = self
            .api
            .send_replication_file(thread_id, &file_name, &caption, &sealed)
            .await?;

        let instance_id = self.cfg.instance_id.clone();
        let nonce = self.nonce.to_string();
        let expected_epoch = epoch.to_owned();
        let expected_previous_generation = document.replication_generation;
        let committed = self
            .update_document(|current| {
                if current.owner.as_deref() != Some(instance_id.as_str())
                    || current.owner_nonce.as_deref() != Some(nonce.as_str())
                    || !current.owner_is_live(now_ms())
                {
                    bail!("HA ownership changed before replication delta commit");
                }
                if current.replication_epoch.as_deref() != Some(expected_epoch.as_str())
                    || current.replication_generation != expected_previous_generation
                {
                    bail!("HA replication head changed before delta commit");
                }
                current.replication_generation = delta.generation;
                current.replication_head = Some(ReplicationHeadRef {
                    generation: delta.generation,
                    file_id: sent.file_id.clone(),
                    message_id: sent.message_id,
                    sha256: sealed_sha256.clone(),
                });
                Ok(())
            })
            .await?;

        if committed.replication_generation != delta.generation
            || committed.replication_head.as_ref().is_none_or(|head| {
                head.file_id != sent.file_id || head.message_id != sent.message_id
            })
        {
            bail!("HA replication delta upload was not committed as canonical head");
        }

        let through_seq = changes
            .last()
            .context("HA replication delta has no last change")?
            .seq;
        db.mark_replication_published(through_seq, delta.generation)?;
        info!(
            instance_id = %self.cfg.instance_id,
            replication_generation = delta.generation,
            changes = changes.len(),
            "HA replication delta published"
        );
        Ok(())
    }

    async fn maybe_publish_snapshot_locked(&self, db: &Db) -> Result<()> {
        let document = self.read_document().await?.document;
        let now = now_ms();
        if !self.owned_by_this_process(&document) || !document.owner_is_live(now) {
            return Ok(());
        }
        if document.replication_gc.is_some() {
            return Ok(());
        }
        if !replication_snapshot_due(&self.cfg, &document, now) {
            return Ok(());
        }
        if db.replication_generation()? != document.replication_generation {
            return Ok(());
        }
        if !db.pending_replication_changes(1)?.is_empty() {
            return Ok(());
        }

        let epoch = document
            .replication_epoch
            .as_deref()
            .context("HA replication snapshot has no epoch")?;
        let thread_id = document
            .sync_thread_id
            .context("HA replication snapshot has no ha-sync topic")?;
        let snapshot = db.export_replication_snapshot()?;
        if snapshot.generation != document.replication_generation {
            bail!("HA checkpoint snapshot generation changed during export");
        }

        let garbage_ids = self
            .collect_previous_recovery_message_ids(&document)
            .await?;
        let gc_ref = if garbage_ids.is_empty() {
            None
        } else {
            Some(self.upload_gc_plan(thread_id, epoch, garbage_ids).await?)
        };
        let uploaded_snapshot = self.upload_snapshot(thread_id, epoch, &snapshot).await?;

        let instance_id = self.cfg.instance_id.clone();
        let nonce = self.nonce.to_string();
        let expected_epoch = epoch.to_owned();
        let expected_snapshot = document.replication_snapshot.clone();
        let expected_head = document.replication_head.clone();
        let committed = self
            .update_document(|current| {
                if current.owner.as_deref() != Some(instance_id.as_str())
                    || current.owner_nonce.as_deref() != Some(nonce.as_str())
                    || !current.owner_is_live(now_ms())
                {
                    return Ok(());
                }
                if current.replication_epoch.as_deref() != Some(expected_epoch.as_str())
                    || current.replication_generation != snapshot.generation
                    || current.replication_snapshot != expected_snapshot
                    || current.replication_head != expected_head
                    || current.replication_gc.is_some()
                {
                    return Ok(());
                }

                rotate_replication_checkpoint(current, uploaded_snapshot.clone(), gc_ref.clone())
            })
            .await?;

        if committed.replication_snapshot.as_ref() != Some(&uploaded_snapshot) {
            bail!("HA replication checkpoint snapshot was not committed as canonical root");
        }

        info!(
            instance_id = %self.cfg.instance_id,
            replication_generation = snapshot.generation,
            previous_snapshot_generation = committed
                .replication_previous_snapshot
                .as_ref()
                .map(|snapshot| snapshot.generation),
            gc_pending = committed.replication_gc.is_some(),
            "HA replication checkpoint snapshot published"
        );
        Ok(())
    }

    async fn retry_replication_gc_locked(&self) -> Result<()> {
        let document = self.read_document().await?.document;
        if !self.owned_by_this_process(&document) || !document.owner_is_live(now_ms()) {
            return Ok(());
        }
        let Some(gc_ref) = document.replication_gc.clone() else {
            return Ok(());
        };
        let epoch = document
            .replication_epoch
            .as_deref()
            .context("HA replication GC has no epoch")?;

        let sealed = self.api.download_replication_file(&gc_ref.file_id).await?;
        if sha256_hex(&sealed) != gc_ref.sha256 {
            bail!("HA replication GC plan SHA256 mismatch");
        }
        let aad = format!("gc:{}:{}", self.cfg.cluster_id, epoch);
        let (plan, _): (ReplicationGcPlan, String) =
            open_json(&sealed, &self.replication_key, &aad)?;
        if plan.protocol != REPLICATION_PAYLOAD_PROTOCOL
            || plan.cluster_id != self.cfg.cluster_id
            || plan.epoch != epoch
        {
            bail!("HA replication GC plan does not match canonical state");
        }

        let (deleted_messages, orphan_messages) = self
            .api
            .delete_messages_best_effort(&plan.message_ids)
            .await;

        let instance_id = self.cfg.instance_id.clone();
        let nonce = self.nonce.to_string();
        let expected_gc = gc_ref.clone();
        let cleared = self
            .update_document(|current| {
                if current.owner.as_deref() != Some(instance_id.as_str())
                    || current.owner_nonce.as_deref() != Some(nonce.as_str())
                    || !current.owner_is_live(now_ms())
                {
                    return Ok(());
                }
                if current.replication_gc.as_ref() == Some(&expected_gc) {
                    current.replication_gc = None;
                }
                Ok(())
            })
            .await?;

        if cleared.replication_gc.is_none() {
            let (_, plan_orphans) = self
                .api
                .delete_messages_best_effort(&[gc_ref.message_id])
                .await;
            info!(
                instance_id = %self.cfg.instance_id,
                deleted_messages,
                orphan_messages = orphan_messages.saturating_add(plan_orphans),
                "HA replication garbage collection completed"
            );
        }
        Ok(())
    }

    async fn upload_gc_plan(
        &self,
        thread_id: i64,
        epoch: &str,
        mut message_ids: Vec<i64>,
    ) -> Result<ReplicationGcRef> {
        message_ids.sort_unstable();
        message_ids.dedup();
        let plan = ReplicationGcPlan {
            protocol: REPLICATION_PAYLOAD_PROTOCOL,
            cluster_id: self.cfg.cluster_id.clone(),
            epoch: epoch.to_owned(),
            created_at_ms: now_ms(),
            message_ids,
        };
        let aad = format!("gc:{}:{}", self.cfg.cluster_id, epoch);
        let (sealed, _) = seal_json(&plan, &self.replication_key, &aad)?;
        let sha256 = sha256_hex(&sealed);
        let sent = self
            .api
            .send_replication_file(
                thread_id,
                &format!(
                    "reddit2tg-ha-gc-{:020}.bin",
                    self.read_document().await?.document.replication_generation
                ),
                "reddit2tg HA replication GC plan",
                &sealed,
            )
            .await?;
        Ok(ReplicationGcRef {
            file_id: sent.file_id,
            message_id: sent.message_id,
            sha256,
        })
    }

    async fn collect_previous_recovery_message_ids(
        &self,
        document: &HaDocument,
    ) -> Result<Vec<i64>> {
        let Some(snapshot) = document.replication_previous_snapshot.as_ref() else {
            return Ok(Vec::new());
        };
        let epoch = document
            .replication_epoch
            .as_deref()
            .context("HA previous recovery checkpoint has no epoch")?;
        let manifest = self.download_snapshot_manifest(epoch, snapshot).await?;

        let mut ids = manifest
            .parts
            .iter()
            .map(|part| part.message_id)
            .collect::<Vec<_>>();
        ids.push(snapshot.manifest_message_id);

        if let Some(head) = document.replication_previous_head.as_ref() {
            ids.extend(
                self.collect_delta_message_ids(epoch, head, snapshot.generation)
                    .await?,
            );
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
    }

    async fn collect_delta_message_ids(
        &self,
        epoch: &str,
        head: &ReplicationHeadRef,
        stop_generation: u64,
    ) -> Result<Vec<i64>> {
        if head.generation < stop_generation {
            bail!("HA previous delta head is older than its snapshot");
        }
        if head.generation == stop_generation {
            return Ok(Vec::new());
        }

        let mut generation = head.generation;
        let mut file_id = head.file_id.clone();
        let mut message_id = head.message_id;
        let mut expected_sha256 = head.sha256.clone();
        let mut seen_files = HashSet::new();
        let mut ids = Vec::new();

        while generation > stop_generation {
            if ids.len() >= REPLICATION_MAX_CHAIN {
                bail!("HA replication GC delta chain exceeds safety limit");
            }
            if !seen_files.insert(file_id.clone()) {
                bail!("HA replication GC delta chain contains a loop");
            }

            ids.push(message_id);
            let sealed = self.api.download_replication_file(&file_id).await?;
            if sha256_hex(&sealed) != expected_sha256 {
                bail!("HA replication GC delta SHA256 mismatch at generation {generation}");
            }
            let aad = format!("delta:{}:{}", self.cfg.cluster_id, epoch);
            let (delta, _): (ReplicationDelta, String) =
                open_json(&sealed, &self.replication_key, &aad)?;
            if delta.protocol != REPLICATION_PAYLOAD_PROTOCOL
                || delta.cluster_id != self.cfg.cluster_id
                || delta.epoch != epoch
                || delta.generation != generation
                || delta.previous_generation.saturating_add(1) != delta.generation
            {
                bail!("HA replication GC delta does not match canonical chain");
            }

            generation = delta.previous_generation;
            if generation > stop_generation {
                file_id = delta
                    .previous_file_id
                    .context("HA replication GC delta chain ended too early")?;
                message_id = delta
                    .previous_message_id
                    .context("HA replication GC delta chain has no previous message_id")?;
                expected_sha256 = delta
                    .previous_sha256
                    .context("HA replication GC delta chain has no previous SHA256")?;
            }
        }

        if generation != stop_generation {
            bail!("HA replication GC delta chain cannot reach previous snapshot");
        }
        Ok(ids)
    }

    async fn catch_up_replication_locked(&self, db: &Db, document: &HaDocument) -> Result<()> {
        let epoch = document
            .replication_epoch
            .as_deref()
            .context("HA replication epoch is not initialized")?;
        if document.sync_thread_id.is_none() {
            bail!("HA replication epoch exists but ha-sync topic is missing");
        }

        let local_epoch = db.get_meta(REPLICATION_EPOCH_META)?;
        let pending_local_changes = !db.pending_replication_changes(1)?.is_empty();
        let mut local_generation = db.replication_generation()?;
        let current_snapshot = document
            .replication_snapshot
            .as_ref()
            .context("HA replication snapshot is missing")?;

        let must_restore_snapshot = local_epoch.as_deref() != Some(epoch)
            || pending_local_changes
            || local_generation > document.replication_generation
            || local_generation < current_snapshot.generation;

        if must_restore_snapshot {
            self.apply_snapshot_from_document(db, document).await?;
            local_generation = db.replication_generation()?;
        }

        if local_generation < document.replication_generation {
            self.apply_delta_chain(db, document, local_generation)
                .await?;
            local_generation = db.replication_generation()?;
        }

        if local_generation != document.replication_generation {
            bail!(
                "HA replication catch-up incomplete: local={}, cluster={}",
                local_generation,
                document.replication_generation
            );
        }
        Ok(())
    }

    async fn apply_snapshot_from_document(&self, db: &Db, document: &HaDocument) -> Result<()> {
        let epoch = document
            .replication_epoch
            .as_deref()
            .context("HA snapshot has no replication epoch")?;
        let current = document
            .replication_snapshot
            .as_ref()
            .context("HA current snapshot reference is missing")?;

        match self.apply_snapshot_ref(db, epoch, current).await {
            Ok(()) => Ok(()),
            Err(current_error) => {
                let Some(previous) = document.replication_previous_snapshot.as_ref() else {
                    return Err(current_error).context(
                        "current HA snapshot failed and no previous recovery checkpoint exists",
                    );
                };
                warn!(
                    error = %format!("{current_error:#}"),
                    current_generation = current.generation,
                    previous_generation = previous.generation,
                    "current HA snapshot failed; trying previous recovery checkpoint"
                );
                self.apply_snapshot_ref(db, epoch, previous).await?;
                if previous.generation < current.generation {
                    let head = document
                        .replication_previous_head
                        .as_ref()
                        .context("previous HA snapshot needs a retained delta chain")?;
                    self.apply_delta_chain_from_head(
                        db,
                        epoch,
                        previous.generation,
                        current.generation,
                        head,
                    )
                    .await?;
                }
                if db.replication_generation()? != current.generation {
                    bail!("previous HA recovery checkpoint did not reconstruct current snapshot generation");
                }
                Ok(())
            }
        }
    }

    async fn apply_snapshot_ref(
        &self,
        db: &Db,
        epoch: &str,
        snapshot: &ReplicationSnapshotRef,
    ) -> Result<()> {
        let manifest = self.download_snapshot_manifest(epoch, snapshot).await?;
        let mut sealed = Vec::new();
        for part in &manifest.parts {
            let bytes = self.api.download_replication_file(&part.file_id).await?;
            if bytes.len() as u64 != part.size {
                bail!("HA replication snapshot part size mismatch");
            }
            if sha256_hex(&bytes) != part.sha256 {
                bail!("HA replication snapshot part SHA256 mismatch");
            }
            sealed.extend_from_slice(&bytes);
        }
        if sha256_hex(&sealed) != manifest.payload_sha256 {
            bail!("HA replication snapshot payload SHA256 mismatch");
        }

        let aad = format!("snapshot:{}:{}", self.cfg.cluster_id, epoch);
        let (envelope, state_sha256): (ReplicationSnapshotEnvelope, String) =
            open_json(&sealed, &self.replication_key, &aad)?;
        if state_sha256 != manifest.state_sha256
            || envelope.protocol != REPLICATION_PAYLOAD_PROTOCOL
            || envelope.cluster_id != self.cfg.cluster_id
            || envelope.epoch != epoch
            || envelope.generation != manifest.generation
            || envelope.snapshot.generation != manifest.generation
        {
            bail!("HA replication snapshot payload does not match manifest");
        }

        db.apply_replication_snapshot(&envelope.snapshot)?;
        db.set_meta(REPLICATION_EPOCH_META, epoch)?;
        info!(
            instance_id = %self.cfg.instance_id,
            replication_generation = envelope.snapshot.generation,
            "HA replication snapshot applied"
        );
        Ok(())
    }

    async fn download_snapshot_manifest(
        &self,
        epoch: &str,
        snapshot: &ReplicationSnapshotRef,
    ) -> Result<ReplicationSnapshotManifest> {
        let manifest_bytes = self
            .api
            .download_replication_file(&snapshot.manifest_file_id)
            .await?;
        if sha256_hex(&manifest_bytes) != snapshot.manifest_sha256 {
            bail!("HA replication snapshot manifest SHA256 mismatch");
        }
        let manifest: ReplicationSnapshotManifest =
            serde_json::from_slice(&manifest_bytes).context("parse HA snapshot manifest")?;
        if manifest.protocol != REPLICATION_PAYLOAD_PROTOCOL
            || manifest.cluster_id != self.cfg.cluster_id
            || manifest.epoch != epoch
            || manifest.generation != snapshot.generation
            || manifest.state_sha256 != snapshot.state_sha256
        {
            bail!("HA replication snapshot manifest does not match canonical state");
        }
        Ok(manifest)
    }

    async fn apply_delta_chain(
        &self,
        db: &Db,
        document: &HaDocument,
        local_generation: u64,
    ) -> Result<()> {
        if local_generation >= document.replication_generation {
            return Ok(());
        }
        let epoch = document
            .replication_epoch
            .as_deref()
            .context("HA delta chain has no replication epoch")?;
        let head = document
            .replication_head
            .as_ref()
            .context("HA replication head is missing")?;
        self.apply_delta_chain_from_head(
            db,
            epoch,
            local_generation,
            document.replication_generation,
            head,
        )
        .await
    }

    async fn apply_delta_chain_from_head(
        &self,
        db: &Db,
        epoch: &str,
        local_generation: u64,
        target_generation: u64,
        head: &ReplicationHeadRef,
    ) -> Result<()> {
        if head.generation != target_generation {
            bail!("HA replication head generation does not match target");
        }
        let mut generation = target_generation;
        let mut file_id = head.file_id.clone();
        let mut expected_sha256 = head.sha256.clone();
        let mut seen_files = HashSet::new();
        let mut reverse_chain = Vec::new();

        while generation > local_generation {
            if reverse_chain.len() >= REPLICATION_MAX_CHAIN {
                bail!("HA replication delta chain exceeds safety limit");
            }
            if !seen_files.insert(file_id.clone()) {
                bail!("HA replication delta chain contains a loop");
            }

            let sealed = self.api.download_replication_file(&file_id).await?;
            if sha256_hex(&sealed) != expected_sha256 {
                bail!("HA replication delta SHA256 mismatch at generation {generation}");
            }
            let aad = format!("delta:{}:{}", self.cfg.cluster_id, epoch);
            let (delta, _): (ReplicationDelta, String) =
                open_json(&sealed, &self.replication_key, &aad)?;

            if delta.protocol != REPLICATION_PAYLOAD_PROTOCOL
                || delta.cluster_id != self.cfg.cluster_id
                || delta.epoch != epoch
                || delta.generation != generation
                || delta.previous_generation.saturating_add(1) != delta.generation
            {
                bail!("HA replication delta does not match canonical chain");
            }

            generation = delta.previous_generation;
            if generation > local_generation {
                file_id = delta
                    .previous_file_id
                    .clone()
                    .context("HA replication delta chain ended before local generation")?;
                expected_sha256 = delta
                    .previous_sha256
                    .clone()
                    .context("HA replication delta chain has no previous SHA256")?;
            }
            reverse_chain.push(delta);
        }

        if generation != local_generation {
            bail!(
                "HA replication delta chain cannot reach local generation {}",
                local_generation
            );
        }

        for delta in reverse_chain.into_iter().rev() {
            db.apply_replication_changes(
                delta.previous_generation,
                delta.generation,
                &delta.changes,
            )?;
            info!(
                instance_id = %self.cfg.instance_id,
                replication_generation = delta.generation,
                changes = delta.changes.len(),
                "HA replication delta applied"
            );
        }
        Ok(())
    }

    async fn upload_snapshot(
        &self,
        thread_id: i64,
        epoch: &str,
        snapshot: &ReplicationSnapshot,
    ) -> Result<ReplicationSnapshotRef> {
        let envelope = ReplicationSnapshotEnvelope {
            protocol: REPLICATION_PAYLOAD_PROTOCOL,
            cluster_id: self.cfg.cluster_id.clone(),
            epoch: epoch.to_owned(),
            generation: snapshot.generation,
            source_instance: self.cfg.instance_id.clone(),
            snapshot: snapshot.clone(),
        };
        let aad = format!("snapshot:{}:{}", self.cfg.cluster_id, epoch);
        let (sealed, state_sha256) = seal_json(&envelope, &self.replication_key, &aad)?;
        let payload_sha256 = sha256_hex(&sealed);
        let chunk_size = (self.cfg.replication_snapshot_chunk_mib as usize) * 1024 * 1024;
        let chunks = split_chunks(&sealed, chunk_size)?;
        let mut parts = Vec::with_capacity(chunks.len());

        for (index, chunk) in chunks.iter().enumerate() {
            let file_name = format!(
                "reddit2tg-ha-snapshot-{:020}.part{:04}.bin",
                snapshot.generation,
                index + 1
            );
            let caption = format!(
                "reddit2tg HA snapshot · generation {} · part {}/{} · source {}",
                snapshot.generation,
                index + 1,
                chunks.len(),
                self.cfg.instance_id
            );
            let sent = self
                .api
                .send_replication_file(thread_id, &file_name, &caption, chunk)
                .await?;
            parts.push(ReplicationFilePart {
                file_id: sent.file_id,
                message_id: sent.message_id,
                sha256: sha256_hex(chunk),
                size: chunk.len() as u64,
            });
        }

        let manifest = ReplicationSnapshotManifest {
            protocol: REPLICATION_PAYLOAD_PROTOCOL,
            cluster_id: self.cfg.cluster_id.clone(),
            epoch: epoch.to_owned(),
            generation: snapshot.generation,
            state_sha256: state_sha256.clone(),
            payload_sha256,
            parts,
        };
        let manifest_bytes =
            serde_json::to_vec(&manifest).context("serialize HA snapshot manifest")?;
        let manifest_sha256 = sha256_hex(&manifest_bytes);
        let manifest_file_name = format!(
            "reddit2tg-ha-snapshot-{:020}.manifest.json",
            snapshot.generation
        );
        let manifest_caption = format!(
            "reddit2tg HA snapshot manifest · generation {} · source {}",
            snapshot.generation, self.cfg.instance_id
        );
        let manifest_sent = self
            .api
            .send_replication_file(
                thread_id,
                &manifest_file_name,
                &manifest_caption,
                &manifest_bytes,
            )
            .await?;

        Ok(ReplicationSnapshotRef {
            generation: snapshot.generation,
            created_at_ms: now_ms(),
            state_sha256,
            manifest_file_id: manifest_sent.file_id,
            manifest_message_id: manifest_sent.message_id,
            manifest_sha256,
        })
    }

    async fn tick(&self, reddit: &RedditClient, db: &Db) -> Result<()> {
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
                self.drain_and_release(reddit, db, target_role).await?;
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
                    self.drain_and_release(reddit, db, LocalRole::Standby)
                        .await?;
                    return Ok(());
                }

                if *self.role.read().await != LocalRole::Active {
                    self.resume_owned_lease(reddit, db, &document).await?;
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
                    self.activate_settled_claim(reddit, db, &document).await?;
                }
                return Ok(());
            }

            self.ensure_local_role(reddit, LocalRole::Standby).await?;
            if claim_preemption_allowed(&self.cfg, &document, instance_id, now)
                && self.replication_ready(db, &document)?
            {
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

        if !self.replication_ready(db, &document)? {
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
                    return Ok(());
                }
                if document.claim_is_live(now)
                    && !claim_preemption_allowed(&self.cfg, document, &instance_id, now)
                {
                    return Ok(());
                }
                let Some(rank) = claim_rank(&self.cfg, document, now, &instance_id) else {
                    return Ok(());
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
                    return Ok(());
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
                document.control_owner = Some(instance_id.clone());
                document.control_owner_nonce = Some(nonce.clone());
                document.control_lease_expires_at_ms = document.lease_expires_at_ms;
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
        db: &Db,
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
        if !self.owned_by_this_process(&current)
            || !current.claim_is_live(now_ms())
            || !self.replication_ready(db, &current)?
        {
            reddit.soft_disconnect().await?;
            *role_guard = LocalRole::Standby;
            drop(role_guard);
            self.release_failed_claim().await?;
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

    async fn resume_owned_lease(
        &self,
        reddit: &RedditClient,
        db: &Db,
        observed: &HaDocument,
    ) -> Result<()> {
        if !self.replication_ready(db, observed)? {
            warn!(
                instance_id = %self.cfg.instance_id,
                replication_generation = observed.replication_generation,
                "HA owned lease will not resume because local replication is not ready"
            );
            reddit.soft_disconnect().await?;
            *self.role.write().await = LocalRole::Standby;
            self.release_owned_after_resume_failure().await?;
            return Ok(());
        }

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
                let replication_ready = self.replication_ready(db, &verify)?;
                if self.owned_by_this_process(&verify)
                    && verify.owner_is_live(now_ms())
                    && replication_ready
                {
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
        self.release_owned_after_resume_failure().await?;
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
                document.control_owner = Some(instance_id.clone());
                document.control_owner_nonce = Some(nonce.clone());
                document.control_lease_expires_at_ms = document.lease_expires_at_ms;
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
            document.control_owner = Some(instance_id.clone());
            document.control_owner_nonce = Some(nonce.clone());
            document.control_lease_expires_at_ms = document.lease_expires_at_ms;
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

    async fn release_owned_after_resume_failure(&self) -> Result<()> {
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
            if document.control_owner.as_deref() == Some(instance_id.as_str())
                && document.control_owner_nonce.as_deref() == Some(nonce.as_str())
            {
                document.control_owner = None;
                document.control_owner_nonce = None;
                document.control_lease_expires_at_ms = now_ms();
            }
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn drain_and_release(
        &self,
        reddit: &RedditClient,
        db: &Db,
        target_role: LocalRole,
    ) -> Result<()> {
        let mut role_guard = self.role.write().await;
        *role_guard = LocalRole::Draining;
        *self.lease_expires_at_ms.lock().await = 0;

        // The write lock waits for outstanding ActiveLeasePermit readers and
        // prevents new bridge operations from starting while we drain.
        reddit.soft_disconnect().await?;
        self.flush_replication_before_release(db).await?;

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
            if document.control_owner.as_deref() == Some(instance_id.as_str())
                && document.control_owner_nonce.as_deref() == Some(nonce.as_str())
            {
                document.control_owner = None;
                document.control_owner_nonce = None;
                document.control_lease_expires_at_ms = now;
            }

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

    async fn graceful_shutdown(&self, reddit: &RedditClient, db: &Db) -> Result<()> {
        let mut role_guard = self.role.write().await;
        let previous_role = *role_guard;
        let was_owner_runtime = matches!(
            previous_role,
            LocalRole::Active | LocalRole::Claiming | LocalRole::Draining
        );
        let needs_final_flush = matches!(previous_role, LocalRole::Active | LocalRole::Draining);
        *role_guard = LocalRole::Draining;
        *self.lease_expires_at_ms.lock().await = 0;
        reddit.soft_disconnect().await?;

        if needs_final_flush {
            self.flush_replication_before_release(db).await?;
        }

        if was_owner_runtime {
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
                let now = now_ms();
                document.lease_expires_at_ms = now;
                document.owner_nonce = None;
                document.claim_started_at_ms = 0;
                if document.control_owner.as_deref() == Some(instance_id.as_str())
                    && document.control_owner_nonce.as_deref() == Some(nonce.as_str())
                {
                    document.control_owner = None;
                    document.control_owner_nonce = None;
                    document.control_lease_expires_at_ms = now;
                }
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
        let mut lines = vec![
            format!(
                "reddit2tg HA · cluster={} · generation={}",
                self.cfg.cluster_id, document.generation
            ),
            format!(
                "Reddit owner: {} · phase={}",
                if document.owner_is_live(now) || document.claim_is_live(now) {
                    document.owner.as_deref().unwrap_or("none")
                } else {
                    "none"
                },
                document.phase.as_str()
            ),
            format!(
                "HA command poller: {}",
                if document.control_owner_is_live(now) {
                    document.control_owner.as_deref().unwrap_or("none")
                } else {
                    "none"
                }
            ),
        ];

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
            let control = if document.control_owner.as_deref() == Some(configured.id.as_str())
                && document.control_owner_is_live(now)
            {
                " control"
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
                control
            ));
        }

        Ok(lines.join("\n"))
    }

    async fn reddit_status(&self) -> Result<String> {
        let pinned = self.read_document().await?;
        let document = pinned.document;
        let now = now_ms();
        let lease_remaining = document.lease_expires_at_ms.saturating_sub(now).max(0) / 1000;
        let control_remaining = document
            .control_lease_expires_at_ms
            .saturating_sub(now)
            .max(0)
            / 1000;
        Ok(format!(
            "reddit2tg HA status\ncluster: {}\nReddit phase: {}\nReddit owner: {}\nReddit lease: {}s\nhandover: {}\nHA command poller: {}\nHA command lease: {}s\nha-sync thread: {}\nreplication generation: {}\nsnapshot generation: {}\nprevious snapshot: {}\nreplication GC: {}\ndata update offset: {}\ncontrol update offset: {}\ngeneration: {}",
            self.cfg.cluster_id,
            document.phase.as_str(),
            if document.owner_is_live(now) || document.claim_is_live(now) {
                document.owner.as_deref().unwrap_or("none")
            } else {
                "none"
            },
            lease_remaining,
            document.handover_to.as_deref().unwrap_or("none"),
            if document.control_owner_is_live(now) {
                document.control_owner.as_deref().unwrap_or("none")
            } else {
                "none"
            },
            control_remaining,
            document
                .sync_thread_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned()),
            document.replication_generation,
            document
                .replication_snapshot
                .as_ref()
                .map(|snapshot| snapshot.generation.to_string())
                .unwrap_or_else(|| "none".to_owned()),
            document
                .replication_previous_snapshot
                .as_ref()
                .map(|snapshot| snapshot.generation.to_string())
                .unwrap_or_else(|| "none".to_owned()),
            if document.replication_gc.is_some() {
                "pending"
            } else {
                "clean"
            },
            document.data_update_offset,
            document.control_update_offset,
            document.generation
        ))
    }

    async fn ensure_document(&self) -> Result<PinnedDocument> {
        if let Some(pinned) = self.api.read_pinned().await? {
            let pinned = if pinned.document.protocol == 2 {
                self.migrate_legacy_document(pinned).await?
            } else {
                pinned
            };
            self.validate_document(&pinned.document)?;
            *self.state_message_id.lock().await = Some(pinned.message_id);
            *self.sync_thread_id_cache.lock().await = pinned.document.sync_thread_id;
            return Ok(pinned);
        }

        let document = HaDocument::new(&self.cfg, now_ms());
        let message_id = self.api.send_document(&document).await?;
        self.api.ensure_pinned(message_id).await?;
        *self.state_message_id.lock().await = Some(message_id);
        sleep(Duration::from_millis(200)).await;

        let pinned = self.api.read_pinned().await?.context(
            "HA control state was sent into ha-control-room and pinned but getChat returned no pinned message",
        )?;
        self.validate_document(&pinned.document)?;
        *self.state_message_id.lock().await = Some(pinned.message_id);
        *self.sync_thread_id_cache.lock().await = pinned.document.sync_thread_id;
        Ok(pinned)
    }

    async fn migrate_legacy_document(&self, pinned: PinnedDocument) -> Result<PinnedDocument> {
        if !legacy_v2_document_matches_config(&self.cfg, &pinned.document) {
            bail!("legacy HA protocol 2 document does not match this cluster configuration");
        }
        let now = now_ms();
        if pinned.document.owner_is_live(now)
            || pinned.document.claim_is_live(now)
            || pinned.document.control_owner_is_live(now)
        {
            bail!(
                "legacy HA protocol 2 lease is still live; stop all v0.4.0-alpha1 nodes and wait for the lease to expire before protocol 3 migration"
            );
        }

        let _write_guard = self.document_write_lock.lock().await;
        let fresh = self
            .api
            .read_pinned()
            .await?
            .context("legacy HA control document disappeared during migration")?;
        if fresh.document.protocol == HA_PROTOCOL_VERSION {
            self.validate_document(&fresh.document)?;
            return Ok(fresh);
        }
        if !legacy_v2_document_matches_config(&self.cfg, &fresh.document) {
            bail!("legacy HA control document changed during protocol migration");
        }

        let now = now_ms();
        if fresh.document.owner_is_live(now)
            || fresh.document.claim_is_live(now)
            || fresh.document.control_owner_is_live(now)
        {
            bail!("legacy HA lease became live during protocol migration");
        }

        let mut next = fresh.document.clone();
        next.protocol = HA_PROTOCOL_VERSION;
        next.members_fingerprint = members_fingerprint(&self.cfg);
        next.generation = next.generation.saturating_add(1);
        next.written_at_ms = now;
        if next.phase != ControlPhase::Paused {
            next.phase = ControlPhase::Lease;
        }
        next.owner_nonce = None;
        next.previous_owner = None;
        next.previous_owner_priority = None;
        next.claim_started_at_ms = 0;
        next.lease_expires_at_ms = now;
        next.handover_to = None;
        next.request = None;
        next.control_owner = None;
        next.control_owner_nonce = None;
        next.control_lease_expires_at_ms = now;
        next.sync_thread_id = None;
        next.replication_epoch = None;
        next.replication_generation = 0;
        next.replication_head = None;
        next.replication_snapshot = None;
        next.replication_previous_snapshot = None;
        next.replication_previous_head = None;
        next.replication_gc = None;

        self.api.edit_document(fresh.message_id, &next).await?;
        sleep(Duration::from_millis(150)).await;
        let migrated = self
            .api
            .read_pinned()
            .await?
            .context("migrated HA control document is no longer pinned")?;
        self.validate_document(&migrated.document)?;
        info!(
            instance_id = %self.cfg.instance_id,
            data_update_offset = migrated.document.data_update_offset,
            control_update_offset = migrated.document.control_update_offset,
            "HA control document migrated from protocol 2 to protocol 3"
        );
        Ok(migrated)
    }

    async fn read_document(&self) -> Result<PinnedDocument> {
        match self.api.read_pinned().await {
            Ok(Some(pinned)) => {
                self.validate_document(&pinned.document)?;
                *self.state_message_id.lock().await = Some(pinned.message_id);
                *self.sync_thread_id_cache.lock().await = pinned.document.sync_thread_id;
                return Ok(pinned);
            }
            Ok(None) => {}
            Err(first_error) => {
                if let Some(message_id) = *self.state_message_id.lock().await {
                    warn!(
                        error = %format!("{first_error:#}"),
                        message_id,
                        "HA state is no longer the latest Telegram pin; trying to restore the cached state pin"
                    );
                    self.api.ensure_pinned(message_id).await?;
                    sleep(Duration::from_millis(150)).await;
                    let pinned = self
                        .api
                        .read_pinned()
                        .await?
                        .context("cached HA state message was re-pinned but getChat returned no pinned message")?;
                    self.validate_document(&pinned.document)?;
                    *self.state_message_id.lock().await = Some(pinned.message_id);
                    *self.sync_thread_id_cache.lock().await = pinned.document.sync_thread_id;
                    return Ok(pinned);
                }
                return Err(first_error);
            }
        }

        anyhow::bail!("Telegram supergroup has no pinned HA control document")
    }

    async fn update_document<F>(&self, mut mutate: F) -> Result<HaDocument>
    where
        F: FnMut(&mut HaDocument) -> Result<()>,
    {
        // All clones of one HaController share this mutex. Without it the
        // local lease-renewal task and data/control offset writers can race
        // each other through Telegram's non-CAS editMessageText API.
        let _write_guard = self.document_write_lock.lock().await;

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
            *self.state_message_id.lock().await = Some(pinned.message_id);

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
        format!("control-{update_id}")
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
    if cfg.control_thread_id <= 1 {
        bail!("ha.control_thread_id must be the numeric thread id of the reserved ha-control-room topic");
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
    if !cfg.replicate_state {
        bail!("ha.replicate_state must be true for HA protocol 3");
    }
    if !(250..=60_000).contains(&cfg.replication_batch_ms) {
        bail!("ha.replication_batch_ms must be in 250..60000");
    }
    if !(10..=10_000).contains(&cfg.replication_snapshot_every) {
        bail!("ha.replication_snapshot_every must be in 10..10000");
    }
    if !(3_600..=64_800).contains(&cfg.replication_snapshot_max_age_secs) {
        bail!("ha.replication_snapshot_max_age_secs must be in 3600..64800 so two retained checkpoints stay below Telegram's 48-hour deletion window");
    }
    if !(1..=16).contains(&cfg.replication_snapshot_chunk_mib) {
        bail!("ha.replication_snapshot_chunk_mib must be in 1..16");
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

fn legacy_members_fingerprint_v2(cfg: &HaConfig) -> String {
    let mut bytes = format!(
        "thread={};ttl={};renew={};settle={};margin={};",
        cfg.control_thread_id,
        cfg.lease_ttl_secs,
        cfg.renew_interval_secs,
        cfg.claim_settle_secs,
        cfg.fail_closed_margin_secs
    )
    .into_bytes();
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

fn legacy_v2_document_matches_config(cfg: &HaConfig, document: &HaDocument) -> bool {
    document.protocol == 2
        && document.cluster_id == cfg.cluster_id
        && document.members_fingerprint == legacy_members_fingerprint_v2(cfg)
}

pub fn members_fingerprint(cfg: &HaConfig) -> String {
    let mut bytes = format!(
        "thread={};ttl={};renew={};settle={};margin={};replicate={};batch={};snapshot={};snapshot_age={};chunk={};",
        cfg.control_thread_id,
        cfg.lease_ttl_secs,
        cfg.renew_interval_secs,
        cfg.claim_settle_secs,
        cfg.fail_closed_margin_secs,
        cfg.replicate_state,
        cfg.replication_batch_ms,
        cfg.replication_snapshot_every,
        cfg.replication_snapshot_max_age_secs,
        cfg.replication_snapshot_chunk_mib
    )
    .into_bytes();
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

fn replication_snapshot_due(cfg: &HaConfig, document: &HaDocument, now_ms: i64) -> bool {
    let Some(snapshot) = document.replication_snapshot.as_ref() else {
        return false;
    };
    let generation_due = document
        .replication_generation
        .saturating_sub(snapshot.generation)
        >= cfg.replication_snapshot_every;
    let max_age_ms = (cfg.replication_snapshot_max_age_secs as i64).saturating_mul(1000);
    let age_due = now_ms.saturating_sub(snapshot.created_at_ms) >= max_age_ms;
    generation_due || age_due
}

fn rotate_replication_checkpoint(
    document: &mut HaDocument,
    new_snapshot: ReplicationSnapshotRef,
    gc: Option<ReplicationGcRef>,
) -> Result<()> {
    if document.replication_gc.is_some() {
        bail!("cannot rotate HA replication checkpoint while GC is pending");
    }
    if new_snapshot.generation != document.replication_generation {
        bail!("new HA snapshot generation does not match canonical replication generation");
    }

    let current_snapshot = document
        .replication_snapshot
        .clone()
        .context("current HA snapshot is missing before checkpoint rotation")?;

    if document.replication_generation > current_snapshot.generation {
        let current_head = document
            .replication_head
            .clone()
            .context("HA delta head is missing before checkpoint rotation")?;
        if current_head.generation != document.replication_generation {
            bail!("HA delta head generation is stale before checkpoint rotation");
        }
        document.replication_previous_head = Some(current_head);
    } else if document.replication_generation == current_snapshot.generation {
        if document.replication_head.is_some() {
            bail!("HA replication head must be empty at snapshot generation");
        }
        document.replication_previous_head = None;
    } else {
        bail!("HA snapshot generation is ahead of canonical replication generation");
    }

    document.replication_previous_snapshot = Some(current_snapshot);
    document.replication_snapshot = Some(new_snapshot);
    document.replication_head = None;
    document.replication_gc = gc;
    Ok(())
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
    if let Some(json) = text.strip_prefix(HA_CONTROL_PREFIX) {
        return Some(serde_json::from_str(json).context("parse HA control document"));
    }
    let json = text.strip_prefix(HA_LEGACY_CONTROL_PREFIX)?;
    Some(serde_json::from_str(json).context("parse legacy HA control document"))
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

pub fn control_help_text() -> &'static str {
    "reddit2tg HA control\n\
/help — tato nápověda\n\
/status — stav Reddit lease a HA command polleru\n\
/instances — seznam nakonfigurovaných instancí\n\
/switch <instance-id> — řízený handover\n\
/connect <instance-id> [force] — aktivace cílové instance\n\
/disconnect <instance-id> — administrativní odpojení bez automatického fallbacku\n\
/maintenance <instance-id> [on|off] — stav nebo změna maintenance"
}

fn split_control_text(text: &str, limit: usize) -> Vec<String> {
    if text.chars().count() <= limit {
        return vec![text.to_owned()];
    }

    let mut parts = Vec::new();
    let mut current = String::new();
    for ch in text.chars() {
        if current.chars().count() >= limit {
            parts.push(current);
            current = String::new();
        }
        current.push(ch);
    }
    if !current.is_empty() {
        parts.push(current);
    }
    parts
}

pub fn parse_command(input: &str) -> Option<std::result::Result<HaCommand, &'static str>> {
    let mut parts = input.split_whitespace();
    let first = parts.next()?;
    let command = first.split('@').next().unwrap_or(first);

    match command {
        "/instances" | "/list-instances" | "/list_instances" => {
            if parts.next().is_none() {
                Some(Ok(HaCommand::ListInstances))
            } else {
                Some(Err("Použití: /instances"))
            }
        }
        "/status" | "/reddit-status" | "/reddit_status" => {
            if parts.next().is_none() {
                Some(Ok(HaCommand::RedditStatus))
            } else {
                Some(Err("Použití: /status"))
            }
        }
        "/connect" | "/reddit-connect" | "/reddit_connect" => {
            let Some(instance_id) = parts.next() else {
                return Some(Err("Použití: /connect <instance-id> [force]"));
            };
            let force = match parts.next() {
                None => false,
                Some("force") => true,
                Some(_) => {
                    return Some(Err("Použití: /connect <instance-id> [force]"));
                }
            };
            if parts.next().is_some() {
                return Some(Err("Použití: /connect <instance-id> [force]"));
            }
            Some(Ok(HaCommand::Connect {
                instance_id: instance_id.to_owned(),
                force,
            }))
        }
        "/disconnect" | "/reddit-disconnect" | "/reddit_disconnect" => {
            let Some(instance_id) = parts.next() else {
                return Some(Err("Použití: /disconnect <instance-id>"));
            };
            if parts.next().is_some() {
                return Some(Err("Použití: /disconnect <instance-id>"));
            }
            Some(Ok(HaCommand::Disconnect {
                instance_id: instance_id.to_owned(),
            }))
        }
        "/switch" | "/reddit-switch" | "/reddit_switch" => {
            let Some(instance_id) = parts.next() else {
                return Some(Err("Použití: /switch <instance-id>"));
            };
            if parts.next().is_some() {
                return Some(Err("Použití: /switch <instance-id>"));
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
            control_thread_id: 777,
            lease_ttl_secs: 30,
            renew_interval_secs: 10,
            claim_settle_secs: 5,
            fail_closed_margin_secs: 5,
            replicate_state: true,
            replication_batch_ms: 2_000,
            replication_snapshot_every: 100,
            replication_snapshot_max_age_secs: 43_200,
            replication_snapshot_chunk_mib: 8,
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

    fn temp_replication_db(label: &str) -> Db {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "reddit2tg-ha-transport-{label}-{}-{unique}.sqlite3",
            std::process::id()
        ));
        Db::new(path).unwrap()
    }

    #[test]
    fn logical_replication_transport_converges_two_sqlite_databases() {
        let source = temp_replication_db("source");
        let standby = temp_replication_db("standby");
        let epoch = "00112233445566778899aabbccddeeff";
        let key = derive_key("123:test-ha-token", "prod");

        source
            .save_matrix_session(&crate::db::StoredMatrixSession {
                access_token: "source-local-token".to_owned(),
                user_id: "@source:reddit".to_owned(),
                expires_at_ms: Some(1111),
            })
            .unwrap();
        standby
            .save_matrix_session(&crate::db::StoredMatrixSession {
                access_token: "standby-local-token".to_owned(),
                user_id: "@standby:reddit".to_owned(),
                expires_at_ms: Some(2222),
            })
            .unwrap();

        source
            .upsert_room("!room:reddit", 1001, "Alice", "joined")
            .unwrap();
        source.set_allow_delete("!room:reddit", true).unwrap();
        source
            .set_meta("matrix_next_batch", "baseline-batch")
            .unwrap();

        // Turn the seeded logical state into generation 1 before taking the
        // baseline snapshot. This mirrors a cluster checkpoint with an empty
        // pending change log.
        let baseline_changes = source.pending_replication_changes(1000).unwrap();
        assert!(!baseline_changes.is_empty());
        let baseline_through = baseline_changes.last().unwrap().seq;
        source
            .mark_replication_published(baseline_through, 1)
            .unwrap();

        let snapshot = source.export_replication_snapshot().unwrap();
        assert_eq!(snapshot.generation, 1);

        let snapshot_envelope = ReplicationSnapshotEnvelope {
            protocol: REPLICATION_PAYLOAD_PROTOCOL,
            cluster_id: "prod".to_owned(),
            epoch: epoch.to_owned(),
            generation: snapshot.generation,
            source_instance: "doma".to_owned(),
            snapshot,
        };
        let snapshot_aad = format!("snapshot:prod:{epoch}");
        let (sealed_snapshot, snapshot_sha) =
            seal_json(&snapshot_envelope, &key, &snapshot_aad).unwrap();
        let (decoded_snapshot, decoded_snapshot_sha): (ReplicationSnapshotEnvelope, String) =
            open_json(&sealed_snapshot, &key, &snapshot_aad).unwrap();
        assert_eq!(decoded_snapshot_sha, snapshot_sha);
        assert_eq!(decoded_snapshot, snapshot_envelope);

        standby
            .apply_replication_snapshot(&decoded_snapshot.snapshot)
            .unwrap();
        source.set_meta(REPLICATION_EPOCH_META, epoch).unwrap();
        standby.set_meta(REPLICATION_EPOCH_META, epoch).unwrap();

        assert_eq!(
            standby.load_matrix_session().unwrap().unwrap().access_token,
            "standby-local-token"
        );
        assert_eq!(
            source.load_matrix_session().unwrap().unwrap().access_token,
            "source-local-token"
        );

        // Generate bridge changes after the checkpoint and transport them as
        // the exact delta structure used by HA.
        source.set_allow_delete("!room:reddit", false).unwrap();
        source
            .record_matrix_reply("!room:reddit", "$child", "$parent")
            .unwrap();
        source
            .record_matrix_delivery_kind("!room:reddit", "$event", 1001, &[2001, 2002], "text")
            .unwrap();
        source
            .save_message_snapshot(
                "$event",
                1001,
                2001,
                Some("encrypted replication test"),
                None,
                None,
            )
            .unwrap();
        source.set_meta("matrix_next_batch", "delta-batch").unwrap();

        let changes = source.pending_replication_changes(1000).unwrap();
        assert!(!changes.is_empty());
        let delta = ReplicationDelta {
            protocol: REPLICATION_PAYLOAD_PROTOCOL,
            cluster_id: "prod".to_owned(),
            epoch: epoch.to_owned(),
            generation: 2,
            previous_generation: 1,
            previous_file_id: None,
            previous_message_id: None,
            previous_sha256: None,
            source_instance: "doma".to_owned(),
            changes: changes.clone(),
        };

        let delta_aad = format!("delta:prod:{epoch}");
        let (sealed_delta, _) = seal_json(&delta, &key, &delta_aad).unwrap();
        assert!(
            !String::from_utf8_lossy(&sealed_delta).contains("encrypted replication test"),
            "plaintext message leaked into encrypted replication payload"
        );
        let (decoded_delta, _): (ReplicationDelta, String) =
            open_json(&sealed_delta, &key, &delta_aad).unwrap();
        assert_eq!(decoded_delta, delta);

        standby
            .apply_replication_changes(
                decoded_delta.previous_generation,
                decoded_delta.generation,
                &decoded_delta.changes,
            )
            .unwrap();
        let through_seq = changes.last().unwrap().seq;
        source.mark_replication_published(through_seq, 2).unwrap();

        assert_eq!(source.replication_generation().unwrap(), 2);
        assert_eq!(standby.replication_generation().unwrap(), 2);
        assert_eq!(
            source.export_replication_snapshot().unwrap(),
            standby.export_replication_snapshot().unwrap()
        );
        assert_eq!(
            standby
                .room_by_matrix("!room:reddit")
                .unwrap()
                .unwrap()
                .telegram_thread_id,
            1001
        );
        assert!(!standby.allow_delete("!room:reddit").unwrap());
        assert_eq!(
            standby
                .matrix_event_for_telegram(1001, 2001)
                .unwrap()
                .as_deref(),
            Some("$event")
        );
        assert_eq!(
            standby.get_meta("matrix_next_batch").unwrap().as_deref(),
            Some("delta-batch")
        );
        assert_eq!(
            standby.get_meta(REPLICATION_EPOCH_META).unwrap().as_deref(),
            Some(epoch)
        );
        assert!(source.pending_replication_changes(1).unwrap().is_empty());
        assert!(standby.pending_replication_changes(1).unwrap().is_empty());

        // Matrix authentication remains node-local even after both snapshot
        // and delta application.
        assert_eq!(
            standby.load_matrix_session().unwrap().unwrap().access_token,
            "standby-local-token"
        );
    }

    fn snapshot_ref(generation: u64, created_at_ms: i64, suffix: &str) -> ReplicationSnapshotRef {
        ReplicationSnapshotRef {
            generation,
            created_at_ms,
            state_sha256: format!("state-{suffix}"),
            manifest_file_id: format!("manifest-file-{suffix}"),
            manifest_message_id: generation as i64 + 10_000,
            manifest_sha256: format!("manifest-sha-{suffix}"),
        }
    }

    fn head_ref(generation: u64, suffix: &str) -> ReplicationHeadRef {
        ReplicationHeadRef {
            generation,
            file_id: format!("delta-file-{suffix}"),
            message_id: generation as i64 + 20_000,
            sha256: format!("delta-sha-{suffix}"),
        }
    }

    #[test]
    fn checkpoint_rotation_keeps_previous_recovery_path_and_resets_head() {
        let config = cfg();
        let mut document = HaDocument::new(&config, 1_000);
        document.replication_generation = 120;
        document.replication_snapshot = Some(snapshot_ref(100, 1_000, "current"));
        document.replication_head = Some(head_ref(120, "current-head"));
        document.replication_previous_snapshot = Some(snapshot_ref(50, 500, "old"));
        document.replication_previous_head = Some(head_ref(100, "old-head"));

        let new_snapshot = snapshot_ref(120, 2_000, "new");
        let gc = ReplicationGcRef {
            file_id: "gc-file".to_owned(),
            message_id: 30_001,
            sha256: "gc-sha".to_owned(),
        };
        rotate_replication_checkpoint(&mut document, new_snapshot.clone(), Some(gc.clone()))
            .unwrap();

        assert_eq!(document.replication_snapshot.as_ref(), Some(&new_snapshot));
        assert_eq!(
            document
                .replication_previous_snapshot
                .as_ref()
                .unwrap()
                .generation,
            100
        );
        assert_eq!(
            document
                .replication_previous_head
                .as_ref()
                .unwrap()
                .generation,
            120
        );
        assert!(document.replication_head.is_none());
        assert_eq!(document.replication_gc.as_ref(), Some(&gc));
    }

    #[test]
    fn repeated_checkpoint_rotation_keeps_exactly_two_recovery_roots() {
        let config = cfg();
        let mut document = HaDocument::new(&config, 1_000);
        document.replication_generation = 100;
        document.replication_snapshot = Some(snapshot_ref(50, 500, "s50"));
        document.replication_head = Some(head_ref(100, "h100"));

        let s100 = snapshot_ref(100, 1_000, "s100");
        rotate_replication_checkpoint(&mut document, s100.clone(), None).unwrap();
        assert_eq!(document.replication_snapshot.as_ref(), Some(&s100));
        assert_eq!(
            document
                .replication_previous_snapshot
                .as_ref()
                .unwrap()
                .generation,
            50
        );
        assert_eq!(
            document
                .replication_previous_head
                .as_ref()
                .unwrap()
                .generation,
            100
        );
        assert!(document.replication_head.is_none());

        document.replication_generation = 120;
        document.replication_head = Some(head_ref(120, "h120"));
        let s120 = snapshot_ref(120, 2_000, "s120");
        let gc = ReplicationGcRef {
            file_id: "gc-s50".to_owned(),
            message_id: 31_000,
            sha256: "gc-s50-sha".to_owned(),
        };
        rotate_replication_checkpoint(&mut document, s120.clone(), Some(gc.clone())).unwrap();

        assert_eq!(document.replication_snapshot.as_ref(), Some(&s120));
        assert_eq!(document.replication_previous_snapshot.as_ref(), Some(&s100));
        assert_eq!(
            document
                .replication_previous_head
                .as_ref()
                .unwrap()
                .generation,
            120
        );
        assert!(document.replication_head.is_none());
        assert_eq!(document.replication_gc.as_ref(), Some(&gc));
    }

    #[test]
    fn idle_checkpoint_refresh_is_due_before_telegram_delete_window() {
        let config = cfg();
        let mut document = HaDocument::new(&config, 1_000);
        document.replication_generation = 10;
        document.replication_snapshot = Some(snapshot_ref(10, 1_000, "same-generation"));

        assert!(!replication_snapshot_due(
            &config,
            &document,
            1_000 + 60 * 60 * 1000
        ));
        assert!(replication_snapshot_due(
            &config,
            &document,
            1_000 + (config.replication_snapshot_max_age_secs as i64) * 1000
        ));

        let refreshed = snapshot_ref(10, 2_000, "refreshed");
        rotate_replication_checkpoint(&mut document, refreshed.clone(), None).unwrap();
        assert_eq!(document.replication_snapshot.as_ref(), Some(&refreshed));
        assert_eq!(
            document
                .replication_previous_snapshot
                .as_ref()
                .unwrap()
                .generation,
            10
        );
        assert!(document.replication_previous_head.is_none());
    }

    #[test]
    fn config_rejects_snapshot_age_that_can_outlive_telegram_gc_window() {
        let mut config = cfg();
        config.replication_snapshot_max_age_secs = 64_801;
        assert!(validate_config(&config).is_err());
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
    fn legacy_v2_fingerprint_differs_from_protocol3_fingerprint() {
        let config = cfg();
        assert_ne!(
            legacy_members_fingerprint_v2(&config),
            members_fingerprint(&config)
        );
    }

    #[test]
    fn legacy_v2_document_is_decoded_with_replication_defaults() {
        let config = cfg();
        let mut legacy = HaDocument::new(&config, 1000);
        legacy.protocol = 2;
        legacy.members_fingerprint = legacy_members_fingerprint_v2(&config);
        legacy.data_update_offset = 1234;
        let json = serde_json::to_string(&legacy).unwrap();
        let text = format!("{HA_LEGACY_CONTROL_PREFIX}{json}");
        let decoded = decode_document(&text).unwrap().unwrap();
        assert_eq!(decoded.protocol, 2);
        assert_eq!(decoded.data_update_offset, 1234);
        assert!(legacy_v2_document_matches_config(&config, &decoded));
    }

    #[test]
    fn replication_settings_are_part_of_cluster_fingerprint() {
        let first = cfg();
        let mut second = cfg();
        second.replication_batch_ms += 1;
        assert_ne!(members_fingerprint(&first), members_fingerprint(&second));
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
        let controller = HaController::new(&config, -100123, 123456, Some("ostrava-new"))
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
        assert_eq!(parse_command("/status"), Some(Ok(HaCommand::RedditStatus)));
        assert_eq!(
            parse_command("/instances"),
            Some(Ok(HaCommand::ListInstances))
        );
        assert_eq!(
            parse_command("/switch brno"),
            Some(Ok(HaCommand::Switch {
                instance_id: "brno".to_owned(),
            }))
        );
    }

    #[test]
    fn control_document_starts_with_shared_offsets_and_no_control_owner() {
        let config = cfg();
        let document = HaDocument::new(&config, 1_000);
        assert_eq!(document.control_update_offset, 0);
        assert_eq!(document.data_update_offset, 0);
        assert_eq!(document.control_owner, None);
        assert!(!document.control_owner_is_live(1_001));
    }

    #[test]
    fn rejects_bad_command_shapes() {
        assert!(parse_command("/reddit-connect").unwrap().is_err());
        assert!(parse_command("/reddit-connect brno now").unwrap().is_err());
        assert!(parse_command("/maintenance brno maybe").unwrap().is_err());
        assert!(parse_command("/list-instances extra").unwrap().is_err());
    }
}
