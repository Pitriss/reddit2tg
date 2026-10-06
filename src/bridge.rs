use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use serde_json::Value;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::{
    config::Config,
    db::{Db, RoomMap},
    reddit::{InvitedRoom, JoinedRoom, RedditClient, member_display_name, matrix_localpart},
    telegram::{Message, TelegramClient},
};

#[derive(Clone)]
pub struct Bridge {
    cfg: Arc<Config>,
    db: Db,
    reddit: RedditClient,
    telegram: TelegramClient,
}

impl Bridge {
    pub fn new(cfg: Config, db: Db, reddit: RedditClient, telegram: TelegramClient) -> Self {
        Self { cfg: Arc::new(cfg), db, reddit, telegram }
    }

    pub async fn check(&self) -> Result<()> {
        let session = self.reddit.authenticate().await?;
        self.telegram.probe().await?;
        info!(user_id = %session.user_id, "configuration probe successful");
        Ok(())
    }

    pub async fn run(self) -> Result<()> {
        self.check().await?;
        let matrix = self.clone();
        let telegram = self.clone();
        let matrix_task = tokio::spawn(async move { matrix.matrix_loop().await });
        let telegram_task = tokio::spawn(async move { telegram.telegram_loop().await });

        tokio::select! {
            result = matrix_task => result.context("matrix task join failed")??,
            result = telegram_task => result.context("telegram task join failed")??,
            _ = tokio::signal::ctrl_c() => info!("shutdown requested"),
        }
        Ok(())
    }

    async fn matrix_loop(&self) -> Result<()> {
        let mut backoff = 1u64;
        loop {
            match self.matrix_iteration().await {
                Ok(()) => backoff = 1,
                Err(err) => {
                    error!(error = %err, backoff_secs = backoff, "Matrix sync iteration failed");
                    sleep(Duration::from_secs(backoff)).await;
                    backoff = (backoff * 2).min(60);
                }
            }
        }
    }

    async fn matrix_iteration(&self) -> Result<()> {
        let since = self.db.get_meta("matrix_next_batch")?;
        let first_sync = since.is_none();
        let sync = self.reddit.sync(
            since.as_deref(),
            self.cfg.bridge.matrix_timeout_ms,
            self.cfg.bridge.refresh_before_secs,
        ).await?;

        if first_sync {
            for (room_id, room) in sync.rooms.invite {
                self.process_invite(&room_id, room).await
                    .with_context(|| format!("initial invite room {room_id}"))?;
            }
            self.db.set_meta("matrix_next_batch", &sync.next_batch)?;
            info!("initial Matrix checkpoint established; historical joined-room timeline was not forwarded");
            return Ok(());
        }

        for (room_id, room) in sync.rooms.invite {
            self.process_invite(&room_id, room).await
                .with_context(|| format!("invite room {room_id}"))?;
        }
        for (room_id, room) in sync.rooms.join {
            if let Err(err) = self.process_joined_room(&room_id, room).await {
                return Err(err).with_context(|| format!("room {room_id}"));
            }
        }

        self.db.set_meta("matrix_next_batch", &sync.next_batch)?;
        let _ = self.db.prune_seen_events(30 * 24 * 3600);
        Ok(())
    }

    async fn process_invite(&self, room_id: &str, room: InvitedRoom) -> Result<()> {
        if self.db.room_by_matrix(room_id)?.is_some() {
            return Ok(());
        }
        let own = self.reddit.ensure_session(self.cfg.bridge.refresh_before_secs).await?.user_id;
        let (user_id, name) = member_display_name(room.invite_state.events.clone().into_iter(), &own)
            .unwrap_or_else(|| ("unknown".to_owned(), "Unknown Reddit user".to_owned()));
        let title = format!("REQUEST · {}", clean_name(&name));
        let thread_id = self.telegram.create_topic(&title).await?;
        info!(matrix_room_id = %room_id, telegram_thread_id = thread_id, title = %title, status = "invite", "Telegram topic created for Reddit room");
        self.db.upsert_room(room_id, thread_id, &title, "invite")?;
        self.telegram.send_text(
            thread_id,
            &format!("Nový Reddit message request od {name} ({user_id}).\n\nNapiš /accept pro přijetí nebo /decline pro odmítnutí."),
        ).await?;
        Ok(())
    }

    async fn process_joined_room(&self, room_id: &str, room: JoinedRoom) -> Result<()> {
        let own = self.reddit.ensure_session(self.cfg.bridge.refresh_before_secs).await?.user_id;
        let mut member_events = room.state.events.clone();
        member_events.extend(room.timeline.events.iter().filter(|e| e.event_type == "m.room.member").cloned());
        let mut counterpart = member_display_name(member_events.into_iter(), &own);
        if counterpart.is_none() {
            if let Some(sender) = room.timeline.events.iter()
                .find(|event| !event.sender.is_empty() && event.sender != own)
                .map(|event| event.sender.clone())
            {
                let name = self.reddit.profile_name(&sender, self.cfg.bridge.refresh_before_secs).await
                    .unwrap_or_else(|| matrix_localpart(&sender));
                counterpart = Some((sender, name));
            }
        }
        if counterpart.is_none() {
            counterpart = self.reddit.joined_counterpart(room_id, self.cfg.bridge.refresh_before_secs).await;
        }
        let mapping = self.ensure_room_mapping(room_id, counterpart.as_ref().map(|(_, n)| n.as_str()), "joined").await?;

        for event in room.timeline.events {
            if event.event_type != "m.room.message" || event.event_id.is_empty() {
                continue;
            }
            if self.db.matrix_event_seen(&event.event_id)? {
                continue;
            }
            let Some(body) = event.content.get("body").and_then(Value::as_str) else { continue };
            let msgtype = event.content.get("msgtype").and_then(Value::as_str).unwrap_or("");
            if msgtype != "m.text" && msgtype != "m.notice" {
                continue;
            }
            let rendered = if event.sender == own {
                format!("Ty (Reddit): {body}")
            } else {
                body.to_owned()
            };
            self.telegram.send_text(mapping.telegram_thread_id, &rendered).await?;
            self.db.mark_matrix_event_seen(&event.event_id, room_id)?;
        }
        Ok(())
    }

    async fn ensure_room_mapping(&self, room_id: &str, title_hint: Option<&str>, status: &str) -> Result<RoomMap> {
        if let Some(mut mapping) = self.db.room_by_matrix(room_id)? {
            if mapping.status != status {
                self.db.set_room_status(room_id, status)?;
                mapping.status = status.to_owned();
            }
            if let Some(title_hint) = title_hint {
                let title = clean_name(title_hint);
                if title != "Reddit chat" && title != mapping.title {
                    self.telegram.rename_topic(mapping.telegram_thread_id, &title).await?;
                    self.db.set_room_title(room_id, &title)?;
                    info!(
                        matrix_room_id = %room_id,
                        telegram_thread_id = mapping.telegram_thread_id,
                        old_title = %mapping.title,
                        new_title = %title,
                        "Telegram topic renamed after Reddit identity resolved"
                    );
                    mapping.title = title;
                }
            }
            return Ok(mapping);
        }
        let title = clean_name(title_hint.unwrap_or("Reddit chat"));
        let thread_id = self.telegram.create_topic(&title).await?;
        info!(matrix_room_id = %room_id, telegram_thread_id = thread_id, title = %title, status = %status, "Telegram topic created for Reddit room");
        self.db.upsert_room(room_id, thread_id, &title, status)?;
        self.db.room_by_matrix(room_id)?.context("new room mapping disappeared")
    }

    async fn telegram_loop(&self) -> Result<()> {
        let mut backoff = 1u64;
        loop {
            match self.telegram_iteration().await {
                Ok(()) => backoff = 1,
                Err(err) => {
                    error!(error = %err, backoff_secs = backoff, "Telegram polling iteration failed");
                    sleep(Duration::from_secs(backoff)).await;
                    backoff = (backoff * 2).min(60);
                }
            }
        }
    }

    async fn telegram_iteration(&self) -> Result<()> {
        let offset = self.db.get_meta("telegram_offset")?.and_then(|v| v.parse::<i64>().ok()).unwrap_or(0);
        let updates = self.telegram.get_updates(offset, self.cfg.bridge.telegram_timeout_secs).await?;
        let mut next_offset = offset;
        for update in updates {
            next_offset = next_offset.max(update.update_id + 1);
            if let Some(message) = update.message.as_ref() {
                if self.telegram.accepts(message) {
                    info!(
                        update_id = update.update_id,
                        message_id = message.message_id,
                        telegram_thread_id = ?message.message_thread_id,
                        is_topic_message = ?message.is_topic_message,
                        "Telegram operator message received"
                    );
                    self.process_telegram_message(update.update_id, message).await
                        .with_context(|| format!("Telegram update {}", update.update_id))?;
                }
            }
            self.db.set_meta("telegram_offset", &next_offset.to_string())?;
        }
        Ok(())
    }

    async fn process_telegram_message(&self, update_id: i64, message: &Message) -> Result<()> {
        let Some(thread_id) = message.message_thread_id else {
            warn!(message_id = message.message_id, is_topic_message = ?message.is_topic_message, "Telegram operator message has no message_thread_id; ignoring General/non-topic message");
            return Ok(());
        };
        let Some(text) = message.text.as_deref() else { return Ok(()) };
        let Some(room) = self.db.room_by_thread(thread_id)? else {
            warn!(thread_id, "message arrived in an unmapped Telegram topic");
            return Ok(());
        };

        if room.status == "invite" {
            match text.trim() {
                "/accept" => {
                    self.reddit.join_room(&room.matrix_room_id, self.cfg.bridge.refresh_before_secs).await?;
                    self.db.set_room_status(&room.matrix_room_id, "joined")?;
                    self.telegram.send_text(thread_id, "Reddit message request přijat.").await?;
                }
                "/decline" => {
                    self.reddit.leave_room(&room.matrix_room_id, self.cfg.bridge.refresh_before_secs).await?;
                    self.db.set_room_status(&room.matrix_room_id, "declined")?;
                    self.telegram.send_text(thread_id, "Reddit message request odmítnut.").await?;
                }
                _ => self.telegram.send_text(thread_id, "Nejdřív napiš /accept nebo /decline.").await?,
            }
            return Ok(());
        }
        if room.status != "joined" {
            return Ok(());
        }

        let txn_id = format!("tg-{update_id}-{}", message.message_id);
        let event_id = self.reddit.send_text(
            &room.matrix_room_id,
            text,
            &txn_id,
            self.cfg.bridge.refresh_before_secs,
        ).await?;
        info!(
            telegram_thread_id = thread_id,
            matrix_room_id = %room.matrix_room_id,
            matrix_event_id = %event_id,
            "Telegram message forwarded to Reddit"
        );
        self.db.mark_matrix_event_seen(&event_id, &room.matrix_room_id)?;
        Ok(())
    }
}

fn clean_name(value: &str) -> String {
    let v = value.trim();
    let base = if v.is_empty() { "Reddit chat" } else { v };
    base.chars().take(110).collect()
}

