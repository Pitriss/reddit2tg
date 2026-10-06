use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone)]
pub struct TelegramClient {
    http: Client,
    base: String,
    chat_id: i64,
    operator_user_id: i64,
}

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Update {
    pub update_id: i64,
    pub message: Option<Message>,
}

#[derive(Debug, Deserialize)]
pub struct Message {
    pub message_id: i64,
    pub message_thread_id: Option<i64>,
    pub is_topic_message: Option<bool>,
    pub chat: Chat,
    pub from: Option<User>,
    pub text: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Chat { pub id: i64 }

#[derive(Debug, Deserialize)]
pub struct User {
    pub id: i64,
    #[serde(default)]
    pub is_bot: bool,
}

#[derive(Debug, Deserialize)]
struct ForumTopic {
    message_thread_id: i64,
}

#[derive(Debug, Serialize)]
struct GetUpdatesBody {
    offset: i64,
    timeout: u64,
    allowed_updates: [&'static str; 1],
}

impl TelegramClient {
    pub fn new(bot_token: &str, chat_id: i64, operator_user_id: i64) -> Result<Self> {
        let http = Client::builder().timeout(Duration::from_secs(60)).build()?;
        Ok(Self {
            http,
            base: format!("https://api.telegram.org/bot{}", bot_token.trim()),
            chat_id,
            operator_user_id,
        })
    }

    pub async fn probe(&self) -> Result<Value> {
        let me: Value = self.call("getMe", &serde_json::json!({})).await?;
        let bot_id = me.get("id").and_then(Value::as_i64)
            .ok_or_else(|| anyhow::anyhow!("Telegram getMe response has no numeric bot id"))?;

        let webhook: Value = self.call("getWebhookInfo", &serde_json::json!({})).await?;
        if webhook.get("url").and_then(Value::as_str).is_some_and(|url| !url.is_empty()) {
            bail!("Telegram bot currently has a webhook configured; remove it before using getUpdates polling");
        }

        let chat: Value = self.call("getChat", &serde_json::json!({"chat_id": self.chat_id})).await?;
        if chat.get("type").and_then(Value::as_str) != Some("supergroup") {
            bail!("telegram.chat_id must point to a supergroup");
        }
        if chat.get("is_forum").and_then(Value::as_bool) != Some(true) {
            bail!("Telegram supergroup must have Topics enabled");
        }

        let member: Value = self.call("getChatMember", &serde_json::json!({
            "chat_id": self.chat_id,
            "user_id": bot_id,
        })).await?;
        let status = member.get("status").and_then(Value::as_str).unwrap_or("");
        let can_manage_topics = member.get("can_manage_topics").and_then(Value::as_bool).unwrap_or(false);
        if status != "creator" && !(status == "administrator" && can_manage_topics) {
            bail!("Telegram bot must be an administrator with Manage Topics permission");
        }
        Ok(me)
    }

    pub async fn create_topic(&self, title: &str) -> Result<i64> {
        let title = sanitize_topic_title(title);
        let topic: ForumTopic = self.call("createForumTopic", &serde_json::json!({
            "chat_id": self.chat_id,
            "name": title,
        })).await?;
        Ok(topic.message_thread_id)
    }

    pub async fn rename_topic(&self, thread_id: i64, title: &str) -> Result<()> {
        let title = sanitize_topic_title(title);
        let _: bool = self.call("editForumTopic", &serde_json::json!({
            "chat_id": self.chat_id,
            "message_thread_id": thread_id,
            "name": title,
        })).await?;
        Ok(())
    }

    pub async fn send_text(&self, thread_id: i64, text: &str) -> Result<()> {
        for chunk in split_text(text, 4000) {
            let sent: Message = self.call("sendMessage", &serde_json::json!({
                "chat_id": self.chat_id,
                "message_thread_id": thread_id,
                "text": chunk,
            })).await?;
            if sent.message_thread_id != Some(thread_id) {
                tracing::warn!(
                    requested_thread_id = thread_id,
                    returned_thread_id = ?sent.message_thread_id,
                    message_id = sent.message_id,
                    "Telegram sendMessage returned a different or missing message_thread_id"
                );
            }
        }
        Ok(())
    }

    pub async fn get_updates(&self, offset: i64, timeout: u64) -> Result<Vec<Update>> {
        self.call("getUpdates", &GetUpdatesBody {
            offset,
            timeout,
            allowed_updates: ["message"],
        }).await
    }

    pub fn accepts(&self, message: &Message) -> bool {
        if message.chat.id != self.chat_id {
            return false;
        }
        let Some(from) = message.from.as_ref() else { return false };
        from.id == self.operator_user_id && !from.is_bot
    }

    async fn call<T: for<'de> Deserialize<'de>, B: Serialize + ?Sized>(&self, method: &str, body: &B) -> Result<T> {
        let url = format!("{}/{}", self.base, method);
        let response = self.http.post(url).json(body).send().await
            .with_context(|| format!("Telegram {method} request failed"))?;
        let status = response.status();
        let parsed: ApiResponse<T> = response.json().await
            .with_context(|| format!("Telegram {method} returned invalid JSON (HTTP {status})"))?;
        if !parsed.ok {
            bail!("Telegram {method} failed (HTTP {status}): {}", parsed.description.unwrap_or_else(|| "unknown error".to_owned()));
        }
        parsed.result.ok_or_else(|| anyhow::anyhow!("Telegram {method} returned ok without result"))
    }
}

fn sanitize_topic_title(input: &str) -> String {
    let trimmed = input.trim();
    let fallback = if trimmed.is_empty() { "Reddit chat" } else { trimmed };
    fallback.chars().take(128).collect()
}

fn split_text(input: &str, max_chars: usize) -> Vec<String> {
    if input.is_empty() || max_chars == 0 {
        return vec![String::new()];
    }

    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut count = 0usize;

    for ch in input.chars() {
        if count == max_chars {
            chunks.push(current);
            current = String::new();
            count = 0;
        }
        current.push(ch);
        count += 1;
    }

    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}
