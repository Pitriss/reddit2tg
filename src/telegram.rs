use std::time::Duration;

use anyhow::{bail, Context, Result};
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
    parameters: Option<ResponseParameters>,
}

#[derive(Debug, Deserialize)]
struct ResponseParameters {
    retry_after: Option<u64>,
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
    pub entities: Option<Vec<Value>>,
    pub reply_to_message: Option<Box<Message>>,
}

#[derive(Debug, Deserialize)]
pub struct Chat {
    pub id: i64,
}

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
        let bot_id = me
            .get("id")
            .and_then(Value::as_i64)
            .ok_or_else(|| anyhow::anyhow!("Telegram getMe response has no numeric bot id"))?;

        let webhook: Value = self.call("getWebhookInfo", &serde_json::json!({})).await?;
        if webhook
            .get("url")
            .and_then(Value::as_str)
            .is_some_and(|url| !url.is_empty())
        {
            bail!("Telegram bot currently has a webhook configured; remove it before using getUpdates polling");
        }

        let chat: Value = self
            .call("getChat", &serde_json::json!({"chat_id": self.chat_id}))
            .await?;
        if chat.get("type").and_then(Value::as_str) != Some("supergroup") {
            bail!("telegram.chat_id must point to a supergroup");
        }
        if chat.get("is_forum").and_then(Value::as_bool) != Some(true) {
            bail!("Telegram supergroup must have Topics enabled");
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
        let can_manage_topics = member
            .get("can_manage_topics")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if status != "creator" && !(status == "administrator" && can_manage_topics) {
            bail!("Telegram bot must be an administrator with Manage Topics permission");
        }
        Ok(me)
    }

    pub async fn create_topic(&self, title: &str) -> Result<i64> {
        let title = sanitize_topic_title(title);
        let topic: ForumTopic = self
            .call(
                "createForumTopic",
                &serde_json::json!({
                    "chat_id": self.chat_id,
                    "name": title,
                }),
            )
            .await?;
        Ok(topic.message_thread_id)
    }

    pub async fn rename_topic(&self, thread_id: i64, title: &str) -> Result<()> {
        let title = sanitize_topic_title(title);
        let _: bool = self
            .call(
                "editForumTopic",
                &serde_json::json!({
                    "chat_id": self.chat_id,
                    "message_thread_id": thread_id,
                    "name": title,
                }),
            )
            .await?;
        Ok(())
    }

    pub async fn close_topic(&self, thread_id: i64) -> Result<()> {
        let _: bool = self
            .call(
                "closeForumTopic",
                &serde_json::json!({
                    "chat_id": self.chat_id,
                    "message_thread_id": thread_id,
                }),
            )
            .await?;
        Ok(())
    }

    pub async fn send_text(&self, thread_id: i64, text: &str) -> Result<()> {
        self.send_text_with_reply(thread_id, text, None)
            .await
            .map(|_| ())
    }

    pub async fn send_text_with_reply(
        &self,
        thread_id: i64,
        text: &str,
        reply_to_message_id: Option<i64>,
    ) -> Result<Vec<i64>> {
        let mut message_ids = Vec::new();
        for (part_index, chunk) in split_text(text, 4000).into_iter().enumerate() {
            let mut body = serde_json::json!({
                "chat_id": self.chat_id,
                "message_thread_id": thread_id,
                "text": chunk,
            });
            if part_index == 0 {
                if let Some(reply_to_message_id) = reply_to_message_id {
                    body["reply_parameters"] = serde_json::json!({
                        "message_id": reply_to_message_id,
                        "allow_sending_without_reply": true,
                    });
                }
            }

            let sent: Message = self.call("sendMessage", &body).await?;
            if sent.message_thread_id != Some(thread_id) {
                tracing::warn!(
                    requested_thread_id = thread_id,
                    returned_thread_id = ?sent.message_thread_id,
                    message_id = sent.message_id,
                    "Telegram sendMessage returned a different or missing message_thread_id"
                );
            }
            message_ids.push(sent.message_id);
        }
        Ok(message_ids)
    }

    pub async fn send_history_message(
        &self,
        thread_id: i64,
        outgoing: bool,
        sender_name: &str,
        body: &str,
        reply_to_message_id: Option<i64>,
    ) -> Result<Vec<i64>> {
        let direction = if outgoing { ">>" } else { "<<" };
        let header: String = format!("{direction} {sender_name}:")
            .chars()
            .take(200)
            .collect();
        let header = escape_html(&header);
        let body_limit = 3500usize;
        let mut message_ids = Vec::new();

        for (part_index, chunk) in split_text(body, body_limit).into_iter().enumerate() {
            let chunk = escape_html(&chunk);
            let text = format!("<b>{header}</b>\n{chunk}");
            let mut request = serde_json::json!({
                "chat_id": self.chat_id,
                "message_thread_id": thread_id,
                "text": text,
                "parse_mode": "HTML",
            });
            if part_index == 0 {
                if let Some(reply_to_message_id) = reply_to_message_id {
                    request["reply_parameters"] = serde_json::json!({
                        "message_id": reply_to_message_id,
                        "allow_sending_without_reply": true,
                    });
                }
            }

            let sent: Message = self.call("sendMessage", &request).await?;

            if sent.message_thread_id != Some(thread_id) {
                tracing::warn!(
                    requested_thread_id = thread_id,
                    returned_thread_id = ?sent.message_thread_id,
                    message_id = sent.message_id,
                    "Telegram history sendMessage returned a different or missing message_thread_id"
                );
            }

            let has_bold = sent.entities.as_ref().is_some_and(|entities| {
                entities.iter().any(|entity| {
                    entity.get("type").and_then(Value::as_str) == Some("bold")
                        && entity.get("offset").and_then(Value::as_u64) == Some(0)
                })
            });
            if !has_bold {
                tracing::warn!(
                    telegram_thread_id = thread_id,
                    message_id = sent.message_id,
                    "Telegram did not return a bold entity for replay header"
                );
            }

            message_ids.push(sent.message_id);

            // Keep history replay below Telegram group flood-control limits.
            tokio::time::sleep(Duration::from_millis(3200)).await;
        }
        Ok(message_ids)
    }

    pub async fn send_general(&self, text: &str) -> Result<()> {
        for chunk in split_text(text, 4000) {
            let _: Message = self
                .call(
                    "sendMessage",
                    &serde_json::json!({
                        "chat_id": self.chat_id,
                        "text": chunk,
                    }),
                )
                .await?;
        }
        Ok(())
    }

    pub async fn get_updates(&self, offset: i64, timeout: u64) -> Result<Vec<Update>> {
        self.call(
            "getUpdates",
            &GetUpdatesBody {
                offset,
                timeout,
                allowed_updates: ["message"],
            },
        )
        .await
    }

    pub fn accepts(&self, message: &Message) -> bool {
        if message.chat.id != self.chat_id {
            return false;
        }
        let Some(from) = message.from.as_ref() else {
            return false;
        };
        from.id == self.operator_user_id && !from.is_bot
    }

    async fn call<T: for<'de> Deserialize<'de>, B: Serialize + ?Sized>(
        &self,
        method: &str,
        body: &B,
    ) -> Result<T> {
        let url = format!("{}/{}", self.base, method);
        const MAX_RATE_LIMIT_RETRIES: usize = 5;

        for retry in 0..=MAX_RATE_LIMIT_RETRIES {
            let response = self
                .http
                .post(&url)
                .json(body)
                .send()
                .await
                .with_context(|| format!("Telegram {method} request failed"))?;
            let status = response.status();
            let parsed: ApiResponse<T> = response.json().await.with_context(|| {
                format!("Telegram {method} returned invalid JSON (HTTP {status})")
            })?;

            if parsed.ok {
                return parsed.result.ok_or_else(|| {
                    anyhow::anyhow!("Telegram {method} returned ok without result")
                });
            }

            if let Some(retry_after) = parsed
                .parameters
                .as_ref()
                .and_then(|parameters| parameters.retry_after)
            {
                if retry < MAX_RATE_LIMIT_RETRIES {
                    let wait_secs = retry_after.saturating_add(1);
                    tracing::warn!(
                        method,
                        retry_after_secs = retry_after,
                        wait_secs,
                        retry = retry + 1,
                        "Telegram flood control; waiting before retry"
                    );
                    tokio::time::sleep(Duration::from_secs(wait_secs)).await;
                    continue;
                }
            }

            bail!(
                "Telegram {method} failed (HTTP {status}): {}",
                parsed
                    .description
                    .unwrap_or_else(|| "unknown error".to_owned())
            );
        }

        unreachable!()
    }
}

fn sanitize_topic_title(input: &str) -> String {
    let trimmed = input.trim();
    let fallback = if trimmed.is_empty() {
        "Reddit chat"
    } else {
        trimmed
    };
    fallback.chars().take(128).collect()
}

fn escape_html(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            _ => escaped.push(ch),
        }
    }
    escaped
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
