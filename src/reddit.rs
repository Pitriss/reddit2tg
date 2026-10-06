use std::{sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::config::Config;

const REDDIT_BASE_URL: &str = "https://www.reddit.com";
const REDDIT_TOKEN_URL: &str = "https://www.reddit.com/svc/shreddit/token";
const REDDIT_CHAT_URL: &str = "https://www.reddit.com/chat/";
const REDDIT_CLIENT_VERSION: &str = "2026-06-24T12:00Z~unknown";
const UA: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36";

#[derive(Debug, Clone)]
pub struct MatrixSession {
    pub access_token: String,
    pub user_id: String,
    pub expires_at_ms: Option<i64>,
}

#[derive(Clone)]
pub struct RedditClient {
    http: Client,
    homeserver: String,
    cookie_header: String,
    session: Arc<RwLock<Option<MatrixSession>>>,
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
    pub fn new(cfg: &Config) -> Result<Self> {
        let http = Client::builder()
            .user_agent(UA)
            .timeout(Duration::from_secs(70))
            .build()?;
        Ok(Self {
            http,
            homeserver: cfg.reddit.homeserver.trim_end_matches('/').to_owned(),
            cookie_header: cfg.reddit_cookie_header(),
            session: Arc::new(RwLock::new(None)),
        })
    }

    pub async fn authenticate(&self) -> Result<MatrixSession> {
        if cookie_value(&self.cookie_header, "reddit_session").is_none() {
            bail!("reddit.session is missing reddit_session cookie");
        }

        // Firefox commonly keeps Reddit Chat's current Matrix JWT in token_v2
        // even when there is no csrf_token cookie. Reuse it when Matrix still
        // accepts it; if it is stale, continue with the refresh flow below.
        if let Some(token_v2) = cookie_value(&self.cookie_header, "token_v2") {
            match self.matrix_session_from_token(token_v2, None).await {
                Ok(session) => {
                    *self.session.write().await = Some(session.clone());
                    info!(user_id = %session.user_id, "Reddit Matrix session authenticated from token_v2");
                    return Ok(session);
                }
                Err(error) => {
                    warn!(error = %error, "stored Reddit token_v2 was rejected; refreshing chat token");
                }
            }
        }

        let csrf = match cookie_value(&self.cookie_header, "csrf_token") {
            Some(value) => value,
            None => self.fetch_csrf_token().await?,
        };

        let form = format!("csrf_token={}", urlencoding::encode(&csrf));
        let response = self.http
            .post(REDDIT_TOKEN_URL)
            .header("Cookie", &self.cookie_header)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Origin", REDDIT_BASE_URL)
            .header("Referer", REDDIT_CHAT_URL)
            .header("X-Original-Referer", REDDIT_CHAT_URL)
            .header("X-Reddit-Client-Version", REDDIT_CLIENT_VERSION)
            .header("Accept", "application/json,text/plain,*/*")
            .body(form)
            .send().await.context("POST reddit /svc/shreddit/token")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!(
                "Reddit chat-token refresh failed: HTTP {status}: {}",
                truncate(&body, 500)
            );
        }
        let token: TokenBlob = response
            .json().await
            .context("invalid JSON from Reddit /svc/shreddit/token")?;
        if token.token.trim().is_empty() {
            bail!("Reddit /svc/shreddit/token returned no token");
        }

        let session = self
            .matrix_session_from_token(token.token, token.expires.map(|v| v as i64))
            .await?;
        *self.session.write().await = Some(session.clone());
        info!(user_id = %session.user_id, "Reddit Matrix session authenticated");
        Ok(session)
    }

    async fn fetch_csrf_token(&self) -> Result<String> {
        let response = self.http
            .get(format!("{REDDIT_BASE_URL}/login/"))
            .header("Cookie", &self.cookie_header)
            .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
            .header("Referer", REDDIT_BASE_URL)
            .send().await
            .context("GET reddit /login/ for csrf token")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!(
                "Reddit login page could not provide csrf_token: HTTP {status}: {}",
                truncate(&body, 500)
            );
        }

        let html = response.text().await.context("read Reddit login page")?;
        extract_csrf_token(&html).ok_or_else(|| anyhow!(
            "Reddit session has no csrf_token cookie and no csrf token was found in /login/ HTML; open reddit.com/chat in Firefox and rerun the cookie helper"
        ))
    }

    async fn matrix_session_from_token(&self, token: String, expires_at_ms: Option<i64>) -> Result<MatrixSession> {
        let whoami_url = format!("{}/_matrix/client/v3/account/whoami", self.homeserver);
        let response = self.http
            .get(whoami_url)
            .bearer_auth(&token)
            .send().await
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
            access_token: token,
            user_id: whoami.user_id,
            expires_at_ms,
        })
    }

    pub async fn ensure_session(&self, refresh_before_secs: u64) -> Result<MatrixSession> {
        if let Some(current) = self.session.read().await.clone() {
            if !expires_soon(current.expires_at_ms, refresh_before_secs) {
                return Ok(current);
            }
        }
        self.authenticate().await
    }

    pub async fn sync(&self, since: Option<&str>, timeout_ms: u64, refresh_before_secs: u64) -> Result<SyncResponse> {
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
        let response = req.send().await?;
        if response.status() == StatusCode::UNAUTHORIZED {
            *self.session.write().await = None;
            bail!("Matrix access token expired or was rejected");
        }
        let response = response.error_for_status().context("Matrix /sync failed")?;
        Ok(response.json().await?)
    }

    pub async fn send_text(&self, room_id: &str, body: &str, txn_id: &str, refresh_before_secs: u64) -> Result<String> {
        let session = self.ensure_session(refresh_before_secs).await?;
        let room = urlencoding::encode(room_id);
        let txn = urlencoding::encode(txn_id);
        let url = format!("{}/_matrix/client/v3/rooms/{room}/send/m.room.message/{txn}", self.homeserver);
        let response = self.http
            .put(url)
            .bearer_auth(&session.access_token)
            .json(&serde_json::json!({"msgtype":"m.text","body":body}))
            .send().await?;
        if response.status() == StatusCode::UNAUTHORIZED {
            *self.session.write().await = None;
            bail!("Matrix access token expired or was rejected while sending a message");
        }
        let response: SendResponse = response
            .error_for_status().context("Matrix send message failed")?
            .json().await?;
        Ok(response.event_id)
    }

    pub async fn join_room(&self, room_id: &str, refresh_before_secs: u64) -> Result<()> {
        let session = self.ensure_session(refresh_before_secs).await?;
        let room = urlencoding::encode(room_id);
        let url = format!("{}/_matrix/client/v3/rooms/{room}/join", self.homeserver);
        let response = self.http.post(url).bearer_auth(&session.access_token).json(&serde_json::json!({})).send().await?;
        if response.status() == StatusCode::UNAUTHORIZED {
            *self.session.write().await = None;
            bail!("Matrix access token expired or was rejected while joining a room");
        }
        response.error_for_status().context("Matrix join room failed")?;
        Ok(())
    }

    pub async fn leave_room(&self, room_id: &str, refresh_before_secs: u64) -> Result<()> {
        let session = self.ensure_session(refresh_before_secs).await?;
        let room = urlencoding::encode(room_id);
        let url = format!("{}/_matrix/client/v3/rooms/{room}/leave", self.homeserver);
        let response = self.http.post(url).bearer_auth(&session.access_token).json(&serde_json::json!({})).send().await?;
        if response.status() == StatusCode::UNAUTHORIZED {
            *self.session.write().await = None;
            bail!("Matrix access token expired or was rejected while leaving a room");
        }
        response.error_for_status().context("Matrix leave room failed")?;
        Ok(())
    }

    pub async fn profile_name(&self, user_id: &str, refresh_before_secs: u64) -> Option<String> {
        let session = self.ensure_session(refresh_before_secs).await.ok()?;
        let user = urlencoding::encode(user_id);
        let url = format!("{}/_matrix/client/v3/profile/{user}", self.homeserver);
        let value: Value = self.http.get(url).bearer_auth(&session.access_token).send().await.ok()?.json().await.ok()?;
        value.get("displayname").and_then(Value::as_str).map(str::to_owned)
    }

    pub async fn joined_counterpart(&self, room_id: &str, refresh_before_secs: u64) -> Option<(String, String)> {
        let session = self.ensure_session(refresh_before_secs).await.ok()?;
        let room = urlencoding::encode(room_id);
        let url = format!("{}/_matrix/client/v3/rooms/{room}/joined_members", self.homeserver);
        let response = self.http
            .get(url)
            .bearer_auth(&session.access_token)
            .send().await.ok()?;
        if !response.status().is_success() {
            return None;
        }
        let value: Value = response.json().await.ok()?;
        let joined = value.get("joined")?.as_object()?;
        for (user_id, member) in joined {
            if user_id == &session.user_id {
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
}

pub fn member_display_name(events: impl Iterator<Item = MatrixEvent>, own_user_id: &str) -> Option<(String, String)> {
    let mut fallback = None;
    for event in events {
        if event.event_type != "m.room.member" {
            continue;
        }
        let Some(user_id) = event.state_key.as_deref() else { continue };
        if user_id == own_user_id {
            continue;
        }
        let membership = event.content.get("membership").and_then(Value::as_str).unwrap_or("");
        if membership != "join" && membership != "invite" {
            continue;
        }
        let display = event.content.get("displayname").and_then(Value::as_str).filter(|v| !v.is_empty());
        let name = display.map(str::to_owned).unwrap_or_else(|| matrix_localpart(user_id));
        if fallback.is_none() {
            fallback = Some((user_id.to_owned(), name));
        }
    }
    fallback
}

pub fn matrix_localpart(user_id: &str) -> String {
    user_id.strip_prefix('@').unwrap_or(user_id).split(':').next().unwrap_or(user_id).to_owned()
}

fn cookie_value(cookie_header: &str, name: &str) -> Option<String> {
    cookie_header
        .split(';')
        .map(str::trim)
        .filter_map(|pair| pair.split_once('='))
        .find_map(|(key, value)| (key.trim() == name).then(|| value.trim().to_owned()))
        .filter(|value| !value.is_empty())
}

fn extract_csrf_token(html: &str) -> Option<String> {
    let decoded = html.replace("&quot;", "\"").replace("&amp;", "&");
    for marker in ["csrf_token\\\":\\\"", "csrf_token\":\"", "CSRF\":\""] {
        if let Some(start) = decoded.find(marker) {
            let rest = &decoded[start + marker.len()..];
            if let Some(end) = rest.find('"') {
                let token = &rest[..end];
                if !token.is_empty() {
                    return Some(token.to_owned());
                }
            }
        }
    }
    None
}

fn expires_soon(expires_at_ms: Option<i64>, refresh_before_secs: u64) -> bool {
    let Some(expires_at_ms) = expires_at_ms else { return false };
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
    expires_at_ms <= now_ms.saturating_add((refresh_before_secs as i64) * 1000)
}

fn truncate(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}
