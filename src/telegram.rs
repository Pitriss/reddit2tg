use std::time::Duration;

use anyhow::{bail, Context, Result};
use reqwest::{
    multipart::{Form, Part},
    Client,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone)]
pub struct TelegramClient {
    http: Client,
    base: String,
    file_base: String,
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
    pub caption: Option<String>,
    pub entities: Option<Vec<Value>>,
    pub reply_to_message: Option<Box<Message>>,
    pub photo: Option<Vec<PhotoSize>>,
    pub document: Option<Document>,
    pub animation: Option<Animation>,
    pub video: Option<Value>,
    pub audio: Option<Value>,
    pub voice: Option<Value>,
    pub sticker: Option<Value>,
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
pub struct PhotoSize {
    pub file_id: String,
    pub file_size: Option<u64>,
    pub width: i64,
    pub height: i64,
}

#[derive(Debug, Deserialize)]
pub struct Document {
    pub file_id: String,
    pub file_name: Option<String>,
    pub mime_type: Option<String>,
    pub file_size: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Animation {
    pub file_id: String,
    pub file_name: Option<String>,
    pub mime_type: Option<String>,
    pub file_size: Option<u64>,
    pub width: i64,
    pub height: i64,
}

#[derive(Debug, Clone)]
pub struct TelegramImageAttachment {
    pub file_id: String,
    pub file_name: String,
    pub file_size: Option<u64>,
    pub width: Option<i64>,
    pub height: Option<i64>,
    pub caption: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TelegramFile {
    file_path: Option<String>,
    file_size: Option<u64>,
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

pub const TELEGRAM_DOWNLOAD_LIMIT: u64 = 20 << 20;
pub const TELEGRAM_PHOTO_UPLOAD_LIMIT: usize = 10 << 20;
pub const TELEGRAM_FILE_UPLOAD_LIMIT: usize = 50 << 20;
pub const TELEGRAM_TOPIC_COLORS: [u32; 6] = [
    0x6FB9F0, // blue
    0xFFD67E, // yellow
    0xCB86DB, // purple
    0x8EEE98, // green
    0xFF93B2, // pink
    0xFB6F5F, // red
];

impl Message {
    pub fn image_attachment(&self) -> Option<TelegramImageAttachment> {
        if let Some(photo) = self.photo.as_ref().and_then(|sizes| {
            sizes
                .iter()
                .max_by_key(|item| item.width.saturating_mul(item.height))
        }) {
            return Some(TelegramImageAttachment {
                file_id: photo.file_id.clone(),
                file_name: "telegram-photo.jpg".to_owned(),
                file_size: photo.file_size,
                width: Some(photo.width),
                height: Some(photo.height),
                caption: self.caption.clone(),
            });
        }

        if let Some(animation) = self.animation.as_ref() {
            if animation
                .mime_type
                .as_deref()
                .is_some_and(|mime| mime.starts_with("image/"))
            {
                return Some(TelegramImageAttachment {
                    file_id: animation.file_id.clone(),
                    file_name: animation
                        .file_name
                        .clone()
                        .unwrap_or_else(|| "telegram-animation.gif".to_owned()),
                    file_size: animation.file_size,
                    width: Some(animation.width),
                    height: Some(animation.height),
                    caption: self.caption.clone(),
                });
            }
        }

        if let Some(document) = self.document.as_ref() {
            let image_mime = document
                .mime_type
                .as_deref()
                .is_some_and(|mime| mime.starts_with("image/"));
            let image_name = document
                .file_name
                .as_deref()
                .is_some_and(looks_like_image_name);
            if image_mime || image_name {
                return Some(TelegramImageAttachment {
                    file_id: document.file_id.clone(),
                    file_name: document
                        .file_name
                        .clone()
                        .unwrap_or_else(|| "telegram-image".to_owned()),
                    file_size: document.file_size,
                    width: None,
                    height: None,
                    caption: self.caption.clone(),
                });
            }
        }

        None
    }

    pub fn has_attachment(&self) -> bool {
        self.photo.is_some()
            || self.document.is_some()
            || self.animation.is_some()
            || self.video.is_some()
            || self.audio.is_some()
            || self.voice.is_some()
            || self.sticker.is_some()
    }
}

impl TelegramClient {
    pub fn new(bot_token: &str, chat_id: i64, operator_user_id: i64) -> Result<Self> {
        let http = Client::builder().timeout(Duration::from_secs(60)).build()?;
        Ok(Self {
            http,
            base: format!("https://api.telegram.org/bot{}", bot_token.trim()),
            file_base: format!("https://api.telegram.org/file/bot{}", bot_token.trim()),
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

    pub async fn create_topic(&self, title: &str, icon_color: Option<u32>) -> Result<i64> {
        let title = sanitize_topic_title(title);
        let mut body = serde_json::json!({
            "chat_id": self.chat_id,
            "name": title,
        });
        if let Some(icon_color) = icon_color {
            body["icon_color"] = serde_json::json!(icon_color);
        }
        let topic: ForumTopic = self.call("createForumTopic", &body).await?;
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

    pub async fn edit_message_text(&self, message_id: i64, text: &str) -> Result<()> {
        let _: Message = self
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

    pub async fn edit_message_caption(&self, message_id: i64, caption: &str) -> Result<()> {
        let _: Message = self
            .call(
                "editMessageCaption",
                &serde_json::json!({
                    "chat_id": self.chat_id,
                    "message_id": message_id,
                    "caption": caption,
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

    pub async fn delete_topic(&self, thread_id: i64) -> Result<()> {
        let _: bool = self
            .call(
                "deleteForumTopic",
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

    pub async fn send_history_date_separator(&self, thread_id: i64, date: &str) -> Result<()> {
        let text = format!("──── {} ────", escape_html(date));
        let _: Message = self
            .call(
                "sendMessage",
                &serde_json::json!({
                    "chat_id": self.chat_id,
                    "message_thread_id": thread_id,
                    "text": text,
                    "parse_mode": "HTML",
                }),
            )
            .await?;

        // Date separators are replay messages too and count against Telegram flood control.
        tokio::time::sleep(Duration::from_millis(3200)).await;
        Ok(())
    }

    pub async fn send_history_message(
        &self,
        thread_id: i64,
        outgoing: bool,
        sender_name: &str,
        time: &str,
        body: &str,
        reply_to_message_id: Option<i64>,
    ) -> Result<Vec<i64>> {
        let direction = if outgoing { ">>" } else { "<<" };
        let header: String = format!("[{time}] {direction} {sender_name}:")
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

    pub async fn download_file(&self, file_id: &str) -> Result<Vec<u8>> {
        let file: TelegramFile = self
            .call("getFile", &serde_json::json!({"file_id": file_id}))
            .await?;
        if file
            .file_size
            .is_some_and(|size| size > TELEGRAM_DOWNLOAD_LIMIT)
        {
            bail!(
                "Telegram file exceeds the Bot API download limit of {} bytes",
                TELEGRAM_DOWNLOAD_LIMIT
            );
        }
        let file_path = file
            .file_path
            .ok_or_else(|| anyhow::anyhow!("Telegram getFile returned no file_path"))?;
        let url = format!("{}/{}", self.file_base, file_path.trim_start_matches('/'));
        let response = self
            .http
            .get(url)
            .send()
            .await
            .context("Telegram file download request failed")?
            .error_for_status()
            .context("Telegram file download failed")?;
        let data = response
            .bytes()
            .await
            .context("Telegram file download body failed")?;
        if data.len() as u64 > TELEGRAM_DOWNLOAD_LIMIT {
            bail!(
                "Telegram file exceeds the Bot API download limit of {} bytes",
                TELEGRAM_DOWNLOAD_LIMIT
            );
        }
        Ok(data.to_vec())
    }

    pub async fn send_media_with_reply(
        &self,
        thread_id: i64,
        data: &[u8],
        file_name: &str,
        mime_type: &str,
        caption: Option<&str>,
        reply_to_message_id: Option<i64>,
    ) -> Result<i64> {
        if data.len() > TELEGRAM_FILE_UPLOAD_LIMIT {
            bail!(
                "media exceeds Telegram Bot API upload limit of {} bytes",
                TELEGRAM_FILE_UPLOAD_LIMIT
            );
        }

        let (method, field_name) = if mime_type == "image/gif" {
            ("sendAnimation", "animation")
        } else if matches!(mime_type, "image/jpeg" | "image/png")
            && data.len() <= TELEGRAM_PHOTO_UPLOAD_LIMIT
        {
            ("sendPhoto", "photo")
        } else {
            ("sendDocument", "document")
        };

        let url = format!("{}/{}", self.base, method);
        const MAX_RATE_LIMIT_RETRIES: usize = 5;
        for retry in 0..=MAX_RATE_LIMIT_RETRIES {
            let part = Part::bytes(data.to_vec())
                .file_name(file_name.to_owned())
                .mime_str(mime_type)
                .with_context(|| format!("invalid media MIME type {mime_type}"))?;
            let mut form = Form::new()
                .text("chat_id", self.chat_id.to_string())
                .text("message_thread_id", thread_id.to_string())
                .part(field_name.to_owned(), part);
            if let Some(caption) = caption
                .map(str::trim)
                .filter(|caption| !caption.is_empty() && *caption != "Image")
            {
                form = form.text("caption", caption.chars().take(1000).collect::<String>());
            }
            if let Some(reply_to_message_id) = reply_to_message_id {
                form = form.text(
                    "reply_parameters",
                    serde_json::json!({
                        "message_id": reply_to_message_id,
                        "allow_sending_without_reply": true,
                    })
                    .to_string(),
                );
            }

            let response = self
                .http
                .post(&url)
                .multipart(form)
                .send()
                .await
                .with_context(|| format!("Telegram {method} request failed"))?;
            let status = response.status();
            let parsed: ApiResponse<Message> = response.json().await.with_context(|| {
                format!("Telegram {method} returned invalid JSON (HTTP {status})")
            })?;
            if parsed.ok {
                let message = parsed.result.ok_or_else(|| {
                    anyhow::anyhow!("Telegram {method} returned ok without result")
                })?;
                return Ok(message.message_id);
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
                        "Telegram flood control during media upload; waiting before retry"
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

fn looks_like_image_name(file_name: &str) -> bool {
    let lower = file_name.to_ascii_lowercase();
    [".jpg", ".jpeg", ".png", ".gif", ".webp"]
        .iter()
        .any(|suffix| lower.ends_with(suffix))
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
