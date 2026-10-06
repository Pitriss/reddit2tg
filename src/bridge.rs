use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use serde_json::Value;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::{
    config::Config,
    db::{Db, RoomMap},
    reddit::{
        InvitedRoom, JoinedRoom, RedditClient, is_auth_refresh_error, member_display_name,
        matrix_localpart,
    },
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
        self.telegram.probe().await?;
        let session = self.reddit.authenticate().await?;
        info!(
            user_id = %session.user_id,
            expires_in_secs = ?session.expires_in_secs(),
            "configuration probe successful"
        );
        Ok(())
    }

    pub async fn reconcile_names(&self, dry_run: bool) -> Result<String> {
        self.reddit
            .ensure_session(self.cfg.bridge.refresh_before_secs)
            .await?;

        let rooms = self.db.rooms()?;
        let mut checked = 0usize;
        let mut renamed = 0usize;
        let mut unchanged = 0usize;
        let mut unresolved = 0usize;
        let mut errors = 0usize;
        let mut details = Vec::new();

        for room in rooms {
            checked += 1;
            let Some((user_id, mut name)) = self
                .reddit
                .room_counterpart(&room.matrix_room_id, self.cfg.bridge.refresh_before_secs)
                .await
            else {
                unresolved += 1;
                details.push(format!(
                    "? topic {}: {} (protistranu se nepodařilo zjistit)",
                    room.telegram_thread_id, room.title
                ));
                continue;
            };

            let fallback_name = matrix_localpart(&user_id);
            if name == fallback_name {
                if let Some(profile_name) = self
                    .reddit
                    .profile_name(&user_id, self.cfg.bridge.refresh_before_secs)
                    .await
                    .filter(|value| !value.trim().is_empty())
                {
                    name = profile_name;
                }
            }

            let expected = reconciled_title(&room.status, &name);
            if expected == room.title {
                unchanged += 1;
                continue;
            }

            if dry_run {
                renamed += 1;
                details.push(format!(
                    "~ topic {}: {} -> {}",
                    room.telegram_thread_id, room.title, expected
                ));
                continue;
            }

            match self.telegram.rename_topic(room.telegram_thread_id, &expected).await {
                Ok(()) => {
                    self.db.set_room_title(&room.matrix_room_id, &expected)?;
                    renamed += 1;
                    details.push(format!(
                        "✓ topic {}: {} -> {}",
                        room.telegram_thread_id, room.title, expected
                    ));
                    info!(
                        matrix_room_id = %room.matrix_room_id,
                        telegram_thread_id = room.telegram_thread_id,
                        old_title = %room.title,
                        new_title = %expected,
                        "Telegram topic renamed by reconcile-names"
                    );
                }
                Err(error) => {
                    errors += 1;
                    details.push(format!(
                        "! topic {}: přejmenování {} -> {} selhalo: {}",
                        room.telegram_thread_id, room.title, expected, error
                    ));
                }
            }
        }

        let mode = if dry_run { "dry-run" } else { "apply" };
        let mut report = format!(
            "reddit2tg reconcile-names ({mode})\nchecked: {checked}\nrenamed: {renamed}\nunchanged: {unchanged}\nunresolved: {unresolved}\nerrors: {errors}"
        );
        if !details.is_empty() {
            report.push_str("\n\n");
            report.push_str(&details.join("\n"));
        }
        Ok(report)
    }

    pub async fn run(self) -> Result<()> {
        self.telegram.probe().await?;
        match self.reddit.authenticate().await {
            Ok(session) => {
                info!(
                    user_id = %session.user_id,
                    expires_in_secs = ?session.expires_in_secs(),
                    "configuration probe successful"
                );
                self.notify_auth_recovered_once().await;
            }
            Err(error) if is_auth_refresh_error(&error) => {
                error!(error = %error, "Reddit authentication unavailable at startup; bridge will keep retrying");
                self.notify_auth_failure_once().await;
            }
            Err(error) => return Err(error),
        }

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
                Ok(()) => {
                    backoff = 1;
                    self.notify_auth_recovered_once().await;
                }
                Err(err) => {
                    if is_auth_refresh_error(&err) {
                        self.notify_auth_failure_once().await;
                    }
                    error!(error = %err, backoff_secs = backoff, "Matrix sync iteration failed");
                    sleep(Duration::from_secs(backoff)).await;
                    backoff = (backoff * 2).min(60);
                }
            }
        }
    }

    async fn matrix_iteration(&self) -> Result<()> {
        self.flush_pending_read_receipts().await?;
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
            counterpart = self
                .reddit
                .room_counterpart(room_id, self.cfg.bridge.refresh_before_secs)
                .await;
        }
        if counterpart.is_none()
            && room
                .timeline
                .events
                .iter()
                .any(|event| event.event_type == "m.room.message" && event.sender == own)
        {
            // Reddit may expose the invite membership a fraction later than
            // the first locally-sent event. Give it two short chances so an
            // outgoing-first conversation gets a human topic name immediately.
            for delay_ms in [150u64, 400u64] {
                sleep(Duration::from_millis(delay_ms)).await;
                counterpart = self
                    .reddit
                    .room_counterpart(room_id, self.cfg.bridge.refresh_before_secs)
                    .await;
                if counterpart.is_some() {
                    break;
                }
            }
        }
        if let Some((user_id, name)) = counterpart.as_mut() {
            let fallback_name = matrix_localpart(user_id);
            if name.as_str() == fallback_name.as_str() {
                if let Some(profile_name) = self
                    .reddit
                    .profile_name(user_id, self.cfg.bridge.refresh_before_secs)
                    .await
                    .filter(|value| !value.trim().is_empty())
                {
                    *name = profile_name;
                }
            }
        }
        let mapping = self.ensure_room_mapping(room_id, counterpart.as_ref().map(|(_, n)| n.as_str()), "joined").await?;
        let own_display_name = self.reddit.own_display_name(self.cfg.bridge.refresh_before_secs).await;

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
                let sender_name = own_display_name.as_deref().unwrap_or("Ty (Reddit)");
                format!("{sender_name}: {body}")
            } else {
                body.to_owned()
            };
            self.telegram.send_text(mapping.telegram_thread_id, &rendered).await?;
            self.db.mark_matrix_event_seen(&event.event_id, room_id)?;
            if event.sender != own {
                self.db.queue_read_receipt(room_id, &event.event_id)?;
                self.flush_read_receipt(room_id, &event.event_id).await?;
            }
        }
        Ok(())
    }


    async fn flush_pending_read_receipts(&self) -> Result<()> {
        for (room_id, event_id) in self.db.pending_read_receipts()? {
            self.flush_read_receipt(&room_id, &event_id).await?;
        }
        Ok(())
    }

    async fn flush_read_receipt(&self, room_id: &str, event_id: &str) -> Result<()> {
        match self
            .reddit
            .set_read_marker(room_id, event_id, self.cfg.bridge.refresh_before_secs)
            .await
        {
            Ok(()) => {
                self.db.clear_read_receipt(room_id, event_id)?;
                info!(
                    matrix_room_id = %room_id,
                    matrix_event_id = %event_id,
                    "Reddit read marker updated after Telegram delivery"
                );
            }
            Err(error) if is_auth_refresh_error(&error) => return Err(error),
            Err(error) => {
                warn!(
                    error = %error,
                    matrix_room_id = %room_id,
                    matrix_event_id = %event_id,
                    "Reddit read marker update failed; queued for retry"
                );
            }
        }
        Ok(())
    }

    async fn notify_auth_failure_once(&self) {
        match self.db.auth_alert_active() {
            Ok(true) => return,
            Ok(false) => {}
            Err(error) => {
                warn!(error = %error, "cannot read auth alert state");
                return;
            }
        }

        let text = "⚠ reddit2tg: automatická obnova Reddit autentizace selhala. Bridge bude dál zkoušet obnovu. Pokud se stav sám neopraví, obnov reddit.session v /etc/reddit2tg/config.toml a restartuj službu. Žádné přihlašovací údaje nejsou součástí této zprávy.";
        match self.telegram.send_general(text).await {
            Ok(()) => {
                if let Err(error) = self.db.set_auth_alert_active(true) {
                    warn!(error = %error, "auth alert was sent but its state could not be persisted");
                }
            }
            Err(error) => warn!(error = %error, "failed to send Reddit authentication alert to Telegram General"),
        }
    }

    async fn notify_auth_recovered_once(&self) {
        match self.db.auth_alert_active() {
            Ok(false) => return,
            Ok(true) => {}
            Err(error) => {
                warn!(error = %error, "cannot read auth alert state");
                return;
            }
        }

        match self
            .telegram
            .send_general("✅ reddit2tg: Reddit autentizace byla obnovena a bridge znovu funguje.")
            .await
        {
            Ok(()) => {
                if let Err(error) = self.db.set_auth_alert_active(false) {
                    warn!(error = %error, "auth recovery was sent but its state could not be persisted");
                }
            }
            Err(error) => warn!(error = %error, "failed to send Reddit authentication recovery to Telegram General"),
        }
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
                    if is_auth_refresh_error(&err) {
                        self.notify_auth_failure_once().await;
                    }
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
        let Some(text) = message.text.as_deref() else { return Ok(()) };

        if let Some(command) = parse_reconcile_names_command(text) {
            match command {
                Ok(dry_run) => {
                    let report = self.reconcile_names(dry_run).await?;
                    self.telegram.send_general(&report).await?;
                }
                Err(()) => {
                    self.telegram
                        .send_general(
                            "Použití: /reconcile-names nebo /reconcile-names dry-run",
                        )
                        .await?;
                }
            }
            return Ok(());
        }

        let Some(thread_id) = message.message_thread_id else {
            warn!(message_id = message.message_id, is_topic_message = ?message.is_topic_message, "Telegram operator message has no message_thread_id; ignoring General/non-topic message");
            return Ok(());
        };
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

fn reconciled_title(status: &str, name: &str) -> String {
    let name = clean_name(name);
    if status == "invite" {
        format!("REQUEST · {name}")
    } else {
        name
    }
}

fn parse_reconcile_names_command(input: &str) -> Option<std::result::Result<bool, ()>> {
    let mut parts = input.split_whitespace();
    let first = parts.next()?;
    let command = first.split('@').next().unwrap_or(first);
    if command != "/reconcile-names" && command != "/reconcile_names" {
        return None;
    }

    match (parts.next(), parts.next()) {
        (None, None) => Some(Ok(false)),
        (Some("dry-run" | "--dry-run"), None) => Some(Ok(true)),
        _ => Some(Err(())),
    }
}

fn clean_name(value: &str) -> String {
    let v = value.trim();
    let base = if v.is_empty() { "Reddit chat" } else { v };
    base.chars().take(110).collect()
}

#[cfg(test)]
mod tests {
    use super::parse_reconcile_names_command;

    #[test]
    fn reconcile_names_command_accepts_hyphen_and_underscore_forms() {
        assert_eq!(parse_reconcile_names_command("/reconcile-names"), Some(Ok(false)));
        assert_eq!(
            parse_reconcile_names_command("/reconcile_names dry-run"),
            Some(Ok(true))
        );
        assert_eq!(
            parse_reconcile_names_command("/reconcile_names@reddit2tg_bot --dry-run"),
            Some(Ok(true))
        );
    }

    #[test]
    fn reconcile_names_command_rejects_extra_arguments() {
        assert_eq!(
            parse_reconcile_names_command("/reconcile-names dry-run now"),
            Some(Err(()))
        );
        assert_eq!(parse_reconcile_names_command("hello"), None);
    }
}
