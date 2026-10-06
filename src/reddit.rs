use std::{
    error::Error as StdError,
    fmt,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::{
    config::Config,
    db::{Db, StoredMatrixSession},
};

const REDDIT_BASE_URL: &str = "https://www.reddit.com";
const REDDIT_TOKEN_URL: &str = "https://www.reddit.com/svc/shreddit/token";
const REDDIT_CHAT_URL: &str = "https://www.reddit.com/chat/";
const REDDIT_CLIENT_VERSION: &str = "2026-06-24T12:00Z~unknown";
const UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.3.1 Safari/605.1.15";
const LOGIN_DEVICE_NAME: &str = "reddit2tg";

#[derive(Debug, Clone)]
pub struct MatrixSession {
    pub access_token: String,
    pub user_id: String,
    pub expires_at_ms: Option<i64>,
}

impl MatrixSession {
    pub fn expires_in_secs(&self) -> Option<i64> {
        let expires = self.expires_at_ms?;
        Some((expires - now_ms()) / 1000)
    }
}

#[derive(Debug)]
pub struct AuthRefreshError {
    message: String,
}

impl AuthRefreshError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for AuthRefreshError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl StdError for AuthRefreshError {}

pub fn is_auth_refresh_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.downcast_ref::<AuthRefreshError>().is_some())
}

fn auth_error(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(AuthRefreshError::new(message))
}

#[derive(Clone)]
pub struct RedditClient {
    http: Client,
    reddit_http: Client,
    homeserver: String,
    cookie_header: String,
    session: Arc<RwLock<Option<MatrixSession>>>,
    cached_own_display_name: Arc<RwLock<Option<String>>>,
    db: Db,
}

#[derive(Debug, Deserialize)]
struct TokenBlob {
    token: String,
    expires: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct SyncResponse {
    pub next_batch: String,
    #[serde(default)]
    pub rooms: SyncRooms,
}

#[derive(Debug, Default, Deserialize)]
pub struct SyncRooms {
    #[serde(default)]
    pub join: std::collections::HashMap<String, JoinedRoom>,
    #[serde(default)]
    pub invite: std::collections::HashMap<String, InvitedRoom>,
}

#[derive(Debug, Default, Deserialize)]
pub struct JoinedRoom {
    #[serde(default)]
    pub timeline: EventBlock,
    #[serde(default)]
    pub state: EventBlock,
}

#[derive(Debug, Default, Deserialize)]
pub struct InvitedRoom {
    #[serde(default)]
    pub invite_state: EventBlock,
}

#[derive(Debug, Default, Deserialize)]
pub struct EventBlock {
    #[serde(default)]
    pub events: Vec<MatrixEvent>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MatrixEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(default)]
    pub event_id: String,
    #[serde(default)]
    pub sender: String,
    #[serde(default)]
    pub state_key: Option<String>,
    #[serde(default)]
    pub content: Value,
}

#[derive(Debug, Deserialize)]
struct WhoAmI {
    user_id: String,
}

#[derive(Debug, Deserialize)]
struct SendResponse {
    event_id: String,
}

impl RedditClient {
    pub fn new(cfg: &Config, db: Db) -> Result<Self> {
        let http = Client::builder()
            .user_agent(UA)
            .use_rustls_tls()
            .timeout(Duration::from_secs(70))
            .build()?;
        let reddit_http = Client::builder()
            .user_agent(UA)
            .use_native_tls()
            .timeout(Duration::from_secs(70))
            .build()?;
        Ok(Self {
            http,
            reddit_http,
            homeserver: cfg.reddit.homeserver.trim_end_matches('/').to_owned(),
            cookie_header: cfg.reddit_cookie_header(),
            session: Arc::new(RwLock::new(None)),
            cached_own_display_name: Arc::new(RwLock::new(None)),
            db,
        })
    }

    pub async fn authenticate(&self) -> Result<MatrixSession> {
        if cookie_value(&self.cookie_header, "reddit_session").is_none() {
            return Err(auth_error("reddit.session is missing reddit_session cookie"));
        }

        if let Some(stored) = self.db.load_matrix_session()? {
            let expires_at_ms = stored
                .expires_at_ms
                .or_else(|| jwt_expires_at_ms(&stored.access_token));
            if !expires_soon(expires_at_ms, 0) {
                match self
                    .matrix_session_from_token(stored.access_token, expires_at_ms)
                    .await
                {
                    Ok(session) => {
                        self.install_session(session.clone()).await?;
                        info!(
                            user_id = %session.user_id,
                            expires_in_secs = ?session.expires_in_secs(),
                            "Reddit Matrix session restored from SQLite"
                        );
                        return Ok(session);
                    }
                    Err(error) => {
                        warn!(error = %error, "stored Matrix session was rejected; refreshing");
                    }
                }
            }
            self.invalidate_session().await?;
        }

        if let Some(token_v2) = cookie_value(&self.cookie_header, "token_v2") {
            match self.matrix_session_from_token(token_v2, None).await {
                Ok(session) => {
                    self.install_session(session.clone()).await?;
                    info!(
                        user_id = %session.user_id,
                        expires_in_secs = ?session.expires_in_secs(),
                        "Reddit Matrix session authenticated from token_v2"
                    );
                    return Ok(session);
                }
                Err(error) => {
                    warn!(error = %error, "stored Reddit token_v2 was rejected; refreshing chat token");
                }
            }
        }

        self.refresh_matrix_session().await
    }

    pub async fn ensure_session(&self, refresh_before_secs: u64) -> Result<MatrixSession> {
        if let Some(current) = self.session.read().await.clone() {
            if !expires_soon(current.expires_at_ms, refresh_before_secs) {
                return Ok(current);
            }
            info!(
                expires_in_secs = ?current.expires_in_secs(),
                refresh_before_secs,
                "Matrix session is near expiry; refreshing"
            );
            return self.refresh_matrix_session().await;
        }

        self.authenticate().await?;
        let current = self
            .session
            .read()
            .await
            .clone()
            .ok_or_else(|| anyhow!("authentication succeeded without Matrix session"))?;
        if expires_soon(current.expires_at_ms, refresh_before_secs) {
            return self.refresh_matrix_session().await;
        }
        Ok(current)
    }

    async fn refresh_matrix_session(&self) -> Result<MatrixSession> {
        if cookie_value(&self.cookie_header, "reddit_session").is_none() {
            return Err(auth_error("reddit.session is missing reddit_session cookie"));
        }

        let token = match self.mint_token_via_chat().await {
            Ok(token) => token,
            Err(chat_error) => {
                warn!(error = %chat_error, "Reddit /chat/ token mint failed; trying shreddit fallback");
                match self.mint_token_via_shreddit().await {
                    Ok(token) => token,
                    Err(shreddit_error) => {
                        return Err(auth_error(format!(
                            "automatic Reddit authentication refresh failed (/chat/: {chat_error}; shreddit: {shreddit_error})"
                        )));
                    }
                }
            }
        };

        self.register_matrix_session(&token.token).await.map_err(|error| {
            auth_error(format!("Matrix rejected freshly minted Reddit token: {error}"))
        })?;

        let expires_at_ms = token
            .expires
            .map(|value| value as i64)
            .or_else(|| jwt_expires_at_ms(&token.token));
        let session = self
            .matrix_session_from_token(token.token, expires_at_ms)
            .await
            .map_err(|error| auth_error(format!("fresh Matrix token failed whoami: {error}")))?;
        self.install_session(session.clone()).await?;
        info!(
            user_id = %session.user_id,
            expires_in_secs = ?session.expires_in_secs(),
            "Reddit Matrix session refreshed"
        );
        Ok(session)
    }

    async fn mint_token_via_chat(&self) -> Result<TokenBlob> {
        let cookie_header = strip_cookie(&self.cookie_header, "token_v2");
        let response = self
            .reddit_http
            .get(REDDIT_CHAT_URL)
            .header("Cookie", cookie_header)
            .header(
                "Accept",
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            )
            .header("Referer", REDDIT_BASE_URL)
            .header("Cache-Control", "no-cache")
            .send()
            .await
            .context("GET Reddit /chat/ for Matrix token")?;

        if !response.status().is_success() {
            bail!("HTTP {}", response.status());
        }
        let html = response.text().await.context("read Reddit /chat/ HTML")?;
        extract_token_blob(&html).context("no usable <rs-app token=...> found in Reddit /chat/")
    }

    async fn mint_token_via_shreddit(&self) -> Result<TokenBlob> {
        let csrf = cookie_value(&self.cookie_header, "csrf_token")
            .ok_or_else(|| anyhow!("csrf_token cookie is unavailable"))?;
        let form = format!("csrf_token={}", urlencoding::encode(&csrf));
        let response = self
            .reddit_http
            .post(REDDIT_TOKEN_URL)
            .header("Cookie", &self.cookie_header)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Origin", REDDIT_BASE_URL)
            .header("Referer", REDDIT_CHAT_URL)
            .header("X-Original-Referer", REDDIT_CHAT_URL)
            .header("X-Reddit-Client-Version", REDDIT_CLIENT_VERSION)
            .header("Accept", "application/json,text/plain,*/*")
            .body(form)
            .send()
            .await
            .context("POST Reddit /svc/shreddit/token")?;

        if !response.status().is_success() {
            bail!("HTTP {}", response.status());
        }
        let token: TokenBlob = response
            .json()
            .await
            .context("invalid JSON from Reddit /svc/shreddit/token")?;
        if token.token.trim().is_empty() {
            bail!("Reddit /svc/shreddit/token returned no token");
        }
        Ok(token)
    }

    async fn register_matrix_session(&self, token: &str) -> Result<()> {
        let url = format!("{}/_matrix/client/v3/login", self.homeserver);
        let response = self
            .http
            .post(url)
            .json(&serde_json::json!({
                "type": "com.reddit.token",
                "token": token,
                "initial_device_display_name": LOGIN_DEVICE_NAME,
            }))
            .send()
            .await
            .context("Matrix com.reddit.token login failed")?;
        if !response.status().is_success() {
            bail!("HTTP {}", response.status());
        }
        Ok(())
    }

    async fn matrix_session_from_token(
        &self,
        token: String,
        expires_at_ms: Option<i64>,
    ) -> Result<MatrixSession> {
        let whoami_url = format!("{}/_matrix/client/v3/account/whoami", self.homeserver);
        let response = self
            .http
            .get(whoami_url)
            .bearer_auth(&token)
            .send()
            .await
            .context("Matrix whoami request failed")?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!(
                "Matrix whoami rejected Reddit chat token: HTTP {status}: {}",
                truncate(&body, 300)
            );
        }
        let whoami: WhoAmI = response.json().await.context("invalid Matrix whoami JSON")?;
        Ok(MatrixSession {
            expires_at_ms: expires_at_ms.or_else(|| jwt_expires_at_ms(&token)),
            access_token: token,
            user_id: whoami.user_id,
        })
    }

    async fn install_session(&self, session: MatrixSession) -> Result<()> {
        self.db.save_matrix_session(&StoredMatrixSession {
            access_token: session.access_token.clone(),
            user_id: session.user_id.clone(),
            expires_at_ms: session.expires_at_ms,
        })?;
        *self.session.write().await = Some(session);
        Ok(())
    }

    async fn invalidate_session(&self) -> Result<()> {
        *self.session.write().await = None;
        self.db.clear_matrix_session()?;
        Ok(())
    }

    async fn refresh_after_unauthorized(&self, operation: &str) -> Result<()> {
        warn!(operation, "Matrix returned 401; invalidating session and refreshing once");
        self.invalidate_session().await?;
        self.refresh_matrix_session().await?;
        Ok(())
    }

    pub async fn sync(
        &self,
        since: Option<&str>,
        timeout_ms: u64,
        refresh_before_secs: u64,
    ) -> Result<SyncResponse> {
        for attempt in 0..2 {
            let session = self.ensure_session(refresh_before_secs).await?;
            let url = format!("{}/_matrix/client/v3/sync", self.homeserver);
            let filter = r#"{"room":{"timeline":{"unread_thread_notifications":true,"not_types":["com.reddit.review_open","com.reddit.review_close"],"lazy_load_members":true},"state":{"lazy_load_members":true}}}"#;
            let mut req = self.http.get(url).bearer_auth(&session.access_token).query(&[
                ("timeout", timeout_ms.to_string()),
                ("set_presence", "offline".to_owned()),
                ("filter", filter.to_owned()),
            ]);
            if let Some(since) = since {
                req = req.query(&[("since", since)]);
            }
            let response = req.send().await.context("Matrix /sync request failed")?;
            if response.status() == StatusCode::UNAUTHORIZED {
                if attempt == 0 {
                    self.refresh_after_unauthorized("sync").await?;
                    continue;
                }
                self.invalidate_session().await?;
                return Err(auth_error("Matrix /sync rejected the refreshed access token"));
            }
            let response = response.error_for_status().context("Matrix /sync failed")?;
            return Ok(response.json().await?);
        }
        unreachable!()
    }

    pub async fn send_text(
        &self,
        room_id: &str,
        body: &str,
        txn_id: &str,
        refresh_before_secs: u64,
    ) -> Result<String> {
        for attempt in 0..2 {
            let session = self.ensure_session(refresh_before_secs).await?;
            let room = urlencoding::encode(room_id);
            let txn = urlencoding::encode(txn_id);
            let url = format!(
                "{}/_matrix/client/v3/rooms/{room}/send/m.room.message/{txn}",
                self.homeserver
            );
            let response = self
                .http
                .put(url)
                .bearer_auth(&session.access_token)
                .json(&serde_json::json!({"msgtype":"m.text","body":body}))
                .send()
                .await
                .context("Matrix send message request failed")?;
            if response.status() == StatusCode::UNAUTHORIZED {
                if attempt == 0 {
                    self.refresh_after_unauthorized("send message").await?;
                    continue;
                }
                self.invalidate_session().await?;
                return Err(auth_error("Matrix send rejected the refreshed access token"));
            }
            let response: SendResponse = response
                .error_for_status()
                .context("Matrix send message failed")?
                .json()
                .await?;
            return Ok(response.event_id);
        }
        unreachable!()
    }

    pub async fn join_room(&self, room_id: &str, refresh_before_secs: u64) -> Result<()> {
        self.room_membership_request(room_id, "join", refresh_before_secs)
            .await
    }

    pub async fn leave_room(&self, room_id: &str, refresh_before_secs: u64) -> Result<()> {
        self.room_membership_request(room_id, "leave", refresh_before_secs)
            .await
    }

    async fn room_membership_request(
        &self,
        room_id: &str,
        action: &str,
        refresh_before_secs: u64,
    ) -> Result<()> {
        for attempt in 0..2 {
            let session = self.ensure_session(refresh_before_secs).await?;
            let room = urlencoding::encode(room_id);
            let url = format!(
                "{}/_matrix/client/v3/rooms/{room}/{action}",
                self.homeserver
            );
            let response = self
                .http
                .post(url)
                .bearer_auth(&session.access_token)
                .json(&serde_json::json!({}))
                .send()
                .await
                .with_context(|| format!("Matrix {action} room request failed"))?;
            if response.status() == StatusCode::UNAUTHORIZED {
                if attempt == 0 {
                    self.refresh_after_unauthorized(action).await?;
                    continue;
                }
                self.invalidate_session().await?;
                return Err(auth_error(format!(
                    "Matrix {action} rejected the refreshed access token"
                )));
            }
            response
                .error_for_status()
                .with_context(|| format!("Matrix {action} room failed"))?;
            return Ok(());
        }
        unreachable!()
    }

    pub async fn set_read_marker(
        &self,
        room_id: &str,
        event_id: &str,
        refresh_before_secs: u64,
    ) -> Result<()> {
        for attempt in 0..2 {
            let session = self.ensure_session(refresh_before_secs).await?;
            let room = urlencoding::encode(room_id);
            let url = format!(
                "{}/_matrix/client/v3/rooms/{room}/read_markers",
                self.homeserver
            );
            let response = self
                .http
                .post(url)
                .bearer_auth(&session.access_token)
                .json(&serde_json::json!({
                    "m.fully_read": event_id,
                    "m.read": event_id,
                    "m.read.private": event_id,
                }))
                .send()
                .await
                .context("Matrix read_markers request failed")?;
            if response.status() == StatusCode::UNAUTHORIZED {
                if attempt == 0 {
                    self.refresh_after_unauthorized("read markers").await?;
                    continue;
                }
                self.invalidate_session().await?;
                return Err(auth_error(
                    "Matrix read_markers rejected the refreshed access token",
                ));
            }
            response
                .error_for_status()
                .context("Matrix read_markers failed")?;
            return Ok(());
        }
        unreachable!()
    }

    pub async fn own_display_name(&self, refresh_before_secs: u64) -> Option<String> {
        if let Some(name) = self.cached_own_display_name.read().await.clone() {
            return Some(name);
        }

        let user_id = self.ensure_session(refresh_before_secs).await.ok()?.user_id;
        let name = self.profile_name(&user_id, refresh_before_secs).await?;
        let name = name.trim();
        if name.is_empty() {
            return None;
        }

        let name = name.to_owned();
        *self.cached_own_display_name.write().await = Some(name.clone());
        Some(name)
    }

    pub async fn profile_name(&self, user_id: &str, refresh_before_secs: u64) -> Option<String> {
        let session = self.ensure_session(refresh_before_secs).await.ok()?;
        let user = urlencoding::encode(user_id);
        let url = format!("{}/_matrix/client/v3/profile/{user}", self.homeserver);
        let response = self
            .http
            .get(url)
            .bearer_auth(&session.access_token)
            .send()
            .await
            .ok()?;
        if response.status() == StatusCode::UNAUTHORIZED {
            let _ = self.invalidate_session().await;
            return None;
        }
        let value: Value = response.json().await.ok()?;
        value
            .get("displayname")
            .and_then(Value::as_str)
            .map(str::to_owned)
    }

    pub async fn room_counterpart(
        &self,
        room_id: &str,
        refresh_before_secs: u64,
    ) -> Option<(String, String)> {
        let session = self.ensure_session(refresh_before_secs).await.ok()?;
        let room = urlencoding::encode(room_id);

        let joined_url = format!(
            "{}/_matrix/client/v3/rooms/{room}/joined_members",
            self.homeserver
        );
        if let Ok(response) = self
            .http
            .get(joined_url)
            .bearer_auth(&session.access_token)
            .send()
            .await
        {
            if response.status() == StatusCode::UNAUTHORIZED {
                let _ = self.invalidate_session().await;
                return None;
            }
            if response.status().is_success() {
                if let Ok(value) = response.json::<Value>().await {
                    if let Some(counterpart) = counterpart_from_joined_members(&value, &session.user_id) {
                        return Some(counterpart);
                    }
                }
            }
        }

        // When we initiate a Reddit chat, the other user can still have
        // membership=invite. joined_members does not include that user, so
        // inspect the full room membership before falling back to a generic
        // Telegram topic title.
        let members_url = format!(
            "{}/_matrix/client/v3/rooms/{room}/members",
            self.homeserver
        );
        let response = self
            .http
            .get(members_url)
            .bearer_auth(&session.access_token)
            .send()
            .await
            .ok()?;
        if response.status() == StatusCode::UNAUTHORIZED {
            let _ = self.invalidate_session().await;
            return None;
        }
        if !response.status().is_success() {
            return None;
        }
        let value: Value = response.json().await.ok()?;
        counterpart_from_members(&value, &session.user_id)
    }
}


fn counterpart_from_joined_members(value: &Value, own_user_id: &str) -> Option<(String, String)> {
    let joined = value.get("joined")?.as_object()?;
    for (user_id, member) in joined {
        if user_id == own_user_id {
            continue;
        }
        let name = member
            .get("display_name")
            .or_else(|| member.get("displayname"))
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| matrix_localpart(user_id));
        return Some((user_id.to_owned(), name));
    }
    None
}

fn counterpart_from_members(value: &Value, own_user_id: &str) -> Option<(String, String)> {
    for event in value.get("chunk")?.as_array()? {
        if event.get("type").and_then(Value::as_str) != Some("m.room.member") {
            continue;
        }
        let Some(user_id) = event.get("state_key").and_then(Value::as_str) else {
            continue;
        };
        if user_id == own_user_id {
            continue;
        }
        let Some(content) = event.get("content") else {
            continue;
        };
        let membership = content.get("membership").and_then(Value::as_str).unwrap_or("");
        if membership != "join" && membership != "invite" {
            continue;
        }
        let name = content
            .get("displayname")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| matrix_localpart(user_id));
        return Some((user_id.to_owned(), name));
    }
    None
}

pub fn member_display_name(
    events: impl Iterator<Item = MatrixEvent>,
    own_user_id: &str,
) -> Option<(String, String)> {
    let mut fallback = None;
    for event in events {
        if event.event_type != "m.room.member" {
            continue;
        }
        let Some(user_id) = event.state_key.as_deref() else {
            continue;
        };
        if user_id == own_user_id {
            continue;
        }
        let membership = event
            .content
            .get("membership")
            .and_then(Value::as_str)
            .unwrap_or("");
        if membership != "join" && membership != "invite" {
            continue;
        }
        let display = event
            .content
            .get("displayname")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty());
        let name = display
            .map(str::to_owned)
            .unwrap_or_else(|| matrix_localpart(user_id));
        if fallback.is_none() {
            fallback = Some((user_id.to_owned(), name));
        }
    }
    fallback
}

pub fn matrix_localpart(user_id: &str) -> String {
    user_id
        .strip_prefix('@')
        .unwrap_or(user_id)
        .split(':')
        .next()
        .unwrap_or(user_id)
        .to_owned()
}

fn cookie_value(cookie_header: &str, name: &str) -> Option<String> {
    cookie_header
        .split(';')
        .map(str::trim)
        .filter_map(|pair| pair.split_once('='))
        .find_map(|(key, value)| (key.trim() == name).then(|| value.trim().to_owned()))
        .filter(|value| !value.is_empty())
}

fn strip_cookie(cookie_header: &str, name: &str) -> String {
    cookie_header
        .split(';')
        .map(str::trim)
        .filter(|pair| {
            pair.split_once('=')
                .map(|(key, _)| key.trim() != name)
                .unwrap_or(true)
        })
        .filter(|pair| !pair.is_empty())
        .collect::<Vec<_>>()
        .join("; ")
}

fn extract_token_blob(html: &str) -> Result<TokenBlob> {
    let app_start = html
        .find("<rs-app")
        .ok_or_else(|| anyhow!("rs-app element is missing"))?;
    let app = &html[app_start..];
    let tag_end = app
        .find('>')
        .ok_or_else(|| anyhow!("rs-app start tag is incomplete"))?;
    let tag = &app[..tag_end];
    let marker = "token=\"";
    let value_start = tag
        .find(marker)
        .ok_or_else(|| anyhow!("rs-app token attribute is missing"))?
        + marker.len();
    let rest = &tag[value_start..];
    let value_end = rest
        .find('"')
        .ok_or_else(|| anyhow!("rs-app token attribute is unterminated"))?;
    let decoded = html_unescape(&rest[..value_end]);
    let token: TokenBlob = serde_json::from_str(&decoded).context("invalid rs-app token JSON")?;
    if token.token.trim().is_empty() {
        bail!("rs-app token JSON has an empty token");
    }
    Ok(token)
}

fn html_unescape(value: &str) -> String {
    value
        .replace("&quot;", "\"")
        .replace("&#34;", "\"")
        .replace("&#x22;", "\"")
        .replace("&#39;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

fn jwt_expires_at_ms(token: &str) -> Option<i64> {
    let payload = token.split('.').nth(1)?;
    let decoded = URL_SAFE_NO_PAD.decode(payload.as_bytes()).ok()?;
    let value: Value = serde_json::from_slice(&decoded).ok()?;
    let exp = value.get("exp")?.as_f64()?;
    Some((exp * 1000.0) as i64)
}

fn expires_soon(expires_at_ms: Option<i64>, refresh_before_secs: u64) -> bool {
    let Some(expires_at_ms) = expires_at_ms else {
        return false;
    };
    expires_at_ms <= now_ms().saturating_add((refresh_before_secs as i64) * 1000)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn truncate(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

#[cfg(test)]
mod tests {
    use super::{counterpart_from_members, extract_token_blob, jwt_expires_at_ms, strip_cookie};
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

    #[test]
    fn strips_only_token_v2_cookie() {
        let value = "reddit_session=abc; token_v2=old; loid=xyz";
        assert_eq!(strip_cookie(value, "token_v2"), "reddit_session=abc; loid=xyz");
    }

    #[test]
    fn extracts_html_escaped_rs_app_token() {
        let html = r#"<html><rs-app foo="x" token="{&quot;token&quot;:&quot;abc.def.ghi&quot;,&quot;expires&quot;:1770000000000}"></rs-app></html>"#;
        let token = extract_token_blob(html).expect("token");
        assert_eq!(token.token, "abc.def.ghi");
        assert_eq!(token.expires.map(|v| v as i64), Some(1_770_000_000_000));
    }

    #[test]
    fn detects_invited_counterpart_when_we_started_the_chat() {
        let members = serde_json::json!({
            "chunk": [
                {
                    "type": "m.room.member",
                    "state_key": "@me:reddit.com",
                    "content": {"membership": "join", "displayname": "Me"}
                },
                {
                    "type": "m.room.member",
                    "state_key": "@them:reddit.com",
                    "content": {"membership": "invite", "displayname": "pit-test"}
                }
            ]
        });
        assert_eq!(
            counterpart_from_members(&members, "@me:reddit.com"),
            Some(("@them:reddit.com".to_owned(), "pit-test".to_owned()))
        );
    }

    #[test]
    fn ignores_left_users_when_detecting_counterpart() {
        let members = serde_json::json!({
            "chunk": [
                {
                    "type": "m.room.member",
                    "state_key": "@old:reddit.com",
                    "content": {"membership": "leave", "displayname": "old-user"}
                }
            ]
        });
        assert_eq!(counterpart_from_members(&members, "@me:reddit.com"), None);
    }

    #[test]
    fn extracts_jwt_expiry() {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#);
        let payload = URL_SAFE_NO_PAD.encode(br#"{"exp":1770000000}"#);
        let token = format!("{header}.{payload}.sig");
        assert_eq!(jwt_expires_at_ms(&token), Some(1_770_000_000_000));
    }
}
