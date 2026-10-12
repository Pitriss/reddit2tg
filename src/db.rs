use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct Db {
    path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct RoomMap {
    pub matrix_room_id: String,
    pub telegram_thread_id: i64,
    pub title: String,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct StoredMatrixSession {
    pub access_token: String,
    pub user_id: String,
    pub expires_at_ms: Option<i64>,
}

struct MessageMapInsert<'a> {
    event_id: &'a str,
    room_id: &'a str,
    thread_id: i64,
    message_id: i64,
    direction: &'a str,
    message_kind: &'a str,
    part_index: i64,
}

#[derive(Debug, Clone)]
pub struct TelegramDelivery {
    pub message_id: i64,
    pub direction: String,
    pub message_kind: String,
    pub part_index: i64,
}

#[derive(Debug, Clone)]
pub struct MessageSnapshot {
    pub rendered_text: Option<String>,
    pub media_file_id: Option<String>,
    pub media_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationChange {
    pub seq: i64,
    pub table: String,
    pub operation: String,
    pub row_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationSnapshotRow {
    pub table: String,
    pub row_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationSnapshot {
    pub generation: u64,
    pub rows: Vec<ReplicationSnapshotRow>,
}

impl Db {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("cannot create {}", parent.display()))?;
        }
        let db = Self { path };
        db.init()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&db.path, fs::Permissions::from_mode(0o600))
                .with_context(|| format!("cannot chmod 0600 {}", db.path.display()))?;
        }
        Ok(db)
    }

    fn connect(&self) -> Result<Connection> {
        let conn = Connection::open(&self.path)
            .with_context(|| format!("cannot open sqlite {}", self.path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        Ok(conn)
    }

    fn init(&self) -> Result<()> {
        let conn = self.connect()?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS meta (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS rooms (
              matrix_room_id TEXT PRIMARY KEY,
              telegram_thread_id INTEGER NOT NULL UNIQUE,
              title TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'joined',
              updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS seen_matrix_events (
              event_id TEXT PRIMARY KEY,
              matrix_room_id TEXT NOT NULL,
              created_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_seen_matrix_room
              ON seen_matrix_events(matrix_room_id, created_at);

            CREATE TABLE IF NOT EXISTS matrix_auth (
              singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
              access_token TEXT NOT NULL,
              user_id TEXT NOT NULL,
              expires_at_ms INTEGER,
              updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS pending_read_receipts (
              matrix_room_id TEXT PRIMARY KEY,
              event_id TEXT NOT NULL,
              updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS archived_topics (
              telegram_thread_id INTEGER PRIMARY KEY,
              title TEXT NOT NULL,
              archived_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS profile_intro_messages (
              telegram_thread_id INTEGER PRIMARY KEY,
              telegram_message_id INTEGER NOT NULL,
              message_kind TEXT NOT NULL,
              intro_text TEXT NOT NULL,
              updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS room_settings (
              matrix_room_id TEXT PRIMARY KEY,
              allow_delete INTEGER NOT NULL DEFAULT 1,
              updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS message_snapshots (
              matrix_event_id TEXT NOT NULL,
              telegram_thread_id INTEGER NOT NULL,
              telegram_message_id INTEGER NOT NULL,
              rendered_text TEXT,
              media_file_id TEXT,
              media_type TEXT,
              created_at INTEGER NOT NULL,
              PRIMARY KEY(matrix_event_id, telegram_thread_id, telegram_message_id)
            );

            CREATE TABLE IF NOT EXISTS matrix_reply_edges (
              matrix_room_id TEXT NOT NULL,
              child_event_id TEXT NOT NULL,
              parent_event_id TEXT NOT NULL,
              created_at INTEGER NOT NULL,
              PRIMARY KEY(matrix_room_id, child_event_id)
            );

            CREATE INDEX IF NOT EXISTS idx_matrix_reply_edges_parent
              ON matrix_reply_edges(matrix_room_id, parent_event_id);

            CREATE TABLE IF NOT EXISTS message_map (
              matrix_event_id TEXT NOT NULL,
              matrix_room_id TEXT NOT NULL,
              telegram_thread_id INTEGER NOT NULL,
              telegram_message_id INTEGER NOT NULL,
              direction TEXT NOT NULL,
              message_kind TEXT NOT NULL DEFAULT 'text',
              part_index INTEGER NOT NULL DEFAULT 0,
              created_at INTEGER NOT NULL,
              PRIMARY KEY(matrix_event_id, telegram_thread_id, telegram_message_id),
              UNIQUE(telegram_thread_id, telegram_message_id)
            );

            CREATE INDEX IF NOT EXISTS idx_message_map_matrix
              ON message_map(matrix_event_id, telegram_thread_id);

            CREATE INDEX IF NOT EXISTS idx_message_map_room
              ON message_map(matrix_room_id, created_at);


            CREATE TABLE IF NOT EXISTS ha_replication_state (
              singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
              applied_generation INTEGER NOT NULL DEFAULT 0,
              published_seq INTEGER NOT NULL DEFAULT 0,
              applying INTEGER NOT NULL DEFAULT 0
            );

            INSERT OR IGNORE INTO ha_replication_state(
              singleton, applied_generation, published_seq, applying
            ) VALUES(1, 0, 0, 0);

            CREATE TABLE IF NOT EXISTS ha_change_log (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              table_name TEXT NOT NULL,
              operation TEXT NOT NULL,
              row_json TEXT NOT NULL,
              created_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_ha_change_log_seq
              ON ha_change_log(seq);

            CREATE TRIGGER IF NOT EXISTS ha_rep_meta_insert
            AFTER INSERT ON meta
            WHEN NEW.key = 'matrix_next_batch'
             AND (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('meta', 'upsert', json_object(
                'key', NEW.key, 'value', NEW.value
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_meta_update
            AFTER UPDATE ON meta
            WHEN NEW.key = 'matrix_next_batch'
             AND (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('meta', 'upsert', json_object(
                'key', NEW.key, 'value', NEW.value
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_rooms_insert
            AFTER INSERT ON rooms
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('rooms', 'upsert', json_object(
                'matrix_room_id', NEW.matrix_room_id,
                'telegram_thread_id', NEW.telegram_thread_id,
                'title', NEW.title,
                'status', NEW.status,
                'updated_at', NEW.updated_at
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_rooms_update
            AFTER UPDATE ON rooms
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('rooms', 'upsert', json_object(
                'matrix_room_id', NEW.matrix_room_id,
                'telegram_thread_id', NEW.telegram_thread_id,
                'title', NEW.title,
                'status', NEW.status,
                'updated_at', NEW.updated_at
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_rooms_delete
            AFTER DELETE ON rooms
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('rooms', 'delete', json_object(
                'matrix_room_id', OLD.matrix_room_id
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_seen_insert
            AFTER INSERT ON seen_matrix_events
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('seen_matrix_events', 'upsert', json_object(
                'event_id', NEW.event_id,
                'matrix_room_id', NEW.matrix_room_id,
                'created_at', NEW.created_at
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_seen_update
            AFTER UPDATE ON seen_matrix_events
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('seen_matrix_events', 'upsert', json_object(
                'event_id', NEW.event_id,
                'matrix_room_id', NEW.matrix_room_id,
                'created_at', NEW.created_at
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_seen_delete
            AFTER DELETE ON seen_matrix_events
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('seen_matrix_events', 'delete', json_object(
                'event_id', OLD.event_id
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_receipts_insert
            AFTER INSERT ON pending_read_receipts
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('pending_read_receipts', 'upsert', json_object(
                'matrix_room_id', NEW.matrix_room_id,
                'event_id', NEW.event_id,
                'updated_at', NEW.updated_at
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_receipts_update
            AFTER UPDATE ON pending_read_receipts
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('pending_read_receipts', 'upsert', json_object(
                'matrix_room_id', NEW.matrix_room_id,
                'event_id', NEW.event_id,
                'updated_at', NEW.updated_at
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_receipts_delete
            AFTER DELETE ON pending_read_receipts
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('pending_read_receipts', 'delete', json_object(
                'matrix_room_id', OLD.matrix_room_id
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_archived_insert
            AFTER INSERT ON archived_topics
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('archived_topics', 'upsert', json_object(
                'telegram_thread_id', NEW.telegram_thread_id,
                'title', NEW.title,
                'archived_at', NEW.archived_at
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_archived_update
            AFTER UPDATE ON archived_topics
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('archived_topics', 'upsert', json_object(
                'telegram_thread_id', NEW.telegram_thread_id,
                'title', NEW.title,
                'archived_at', NEW.archived_at
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_archived_delete
            AFTER DELETE ON archived_topics
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('archived_topics', 'delete', json_object(
                'telegram_thread_id', OLD.telegram_thread_id
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_intro_insert
            AFTER INSERT ON profile_intro_messages
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('profile_intro_messages', 'upsert', json_object(
                'telegram_thread_id', NEW.telegram_thread_id,
                'telegram_message_id', NEW.telegram_message_id,
                'message_kind', NEW.message_kind,
                'intro_text', NEW.intro_text,
                'updated_at', NEW.updated_at
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_intro_update
            AFTER UPDATE ON profile_intro_messages
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('profile_intro_messages', 'upsert', json_object(
                'telegram_thread_id', NEW.telegram_thread_id,
                'telegram_message_id', NEW.telegram_message_id,
                'message_kind', NEW.message_kind,
                'intro_text', NEW.intro_text,
                'updated_at', NEW.updated_at
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_intro_delete
            AFTER DELETE ON profile_intro_messages
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('profile_intro_messages', 'delete', json_object(
                'telegram_thread_id', OLD.telegram_thread_id
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_settings_insert
            AFTER INSERT ON room_settings
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('room_settings', 'upsert', json_object(
                'matrix_room_id', NEW.matrix_room_id,
                'allow_delete', NEW.allow_delete,
                'updated_at', NEW.updated_at
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_settings_update
            AFTER UPDATE ON room_settings
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('room_settings', 'upsert', json_object(
                'matrix_room_id', NEW.matrix_room_id,
                'allow_delete', NEW.allow_delete,
                'updated_at', NEW.updated_at
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_settings_delete
            AFTER DELETE ON room_settings
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('room_settings', 'delete', json_object(
                'matrix_room_id', OLD.matrix_room_id
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_snapshots_insert
            AFTER INSERT ON message_snapshots
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('message_snapshots', 'upsert', json_object(
                'matrix_event_id', NEW.matrix_event_id,
                'telegram_thread_id', NEW.telegram_thread_id,
                'telegram_message_id', NEW.telegram_message_id,
                'rendered_text', NEW.rendered_text,
                'media_file_id', NEW.media_file_id,
                'media_type', NEW.media_type,
                'created_at', NEW.created_at
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_snapshots_update
            AFTER UPDATE ON message_snapshots
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('message_snapshots', 'upsert', json_object(
                'matrix_event_id', NEW.matrix_event_id,
                'telegram_thread_id', NEW.telegram_thread_id,
                'telegram_message_id', NEW.telegram_message_id,
                'rendered_text', NEW.rendered_text,
                'media_file_id', NEW.media_file_id,
                'media_type', NEW.media_type,
                'created_at', NEW.created_at
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_snapshots_delete
            AFTER DELETE ON message_snapshots
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('message_snapshots', 'delete', json_object(
                'matrix_event_id', OLD.matrix_event_id,
                'telegram_thread_id', OLD.telegram_thread_id,
                'telegram_message_id', OLD.telegram_message_id
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_reply_insert
            AFTER INSERT ON matrix_reply_edges
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('matrix_reply_edges', 'upsert', json_object(
                'matrix_room_id', NEW.matrix_room_id,
                'child_event_id', NEW.child_event_id,
                'parent_event_id', NEW.parent_event_id,
                'created_at', NEW.created_at
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_reply_update
            AFTER UPDATE ON matrix_reply_edges
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('matrix_reply_edges', 'upsert', json_object(
                'matrix_room_id', NEW.matrix_room_id,
                'child_event_id', NEW.child_event_id,
                'parent_event_id', NEW.parent_event_id,
                'created_at', NEW.created_at
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_reply_delete
            AFTER DELETE ON matrix_reply_edges
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('matrix_reply_edges', 'delete', json_object(
                'matrix_room_id', OLD.matrix_room_id,
                'child_event_id', OLD.child_event_id
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_map_insert
            AFTER INSERT ON message_map
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('message_map', 'upsert', json_object(
                'matrix_event_id', NEW.matrix_event_id,
                'matrix_room_id', NEW.matrix_room_id,
                'telegram_thread_id', NEW.telegram_thread_id,
                'telegram_message_id', NEW.telegram_message_id,
                'direction', NEW.direction,
                'message_kind', NEW.message_kind,
                'part_index', NEW.part_index,
                'created_at', NEW.created_at
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_map_update
            AFTER UPDATE ON message_map
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('message_map', 'upsert', json_object(
                'matrix_event_id', NEW.matrix_event_id,
                'matrix_room_id', NEW.matrix_room_id,
                'telegram_thread_id', NEW.telegram_thread_id,
                'telegram_message_id', NEW.telegram_message_id,
                'direction', NEW.direction,
                'message_kind', NEW.message_kind,
                'part_index', NEW.part_index,
                'created_at', NEW.created_at
              ), CAST(strftime('%s','now') AS INTEGER));
            END;

            CREATE TRIGGER IF NOT EXISTS ha_rep_map_delete
            AFTER DELETE ON message_map
            WHEN (SELECT applying FROM ha_replication_state WHERE singleton = 1) = 0
            BEGIN
              INSERT INTO ha_change_log(table_name, operation, row_json, created_at)
              VALUES('message_map', 'delete', json_object(
                'telegram_thread_id', OLD.telegram_thread_id,
                'telegram_message_id', OLD.telegram_message_id
              ), CAST(strftime('%s','now') AS INTEGER));
            END;
            "#,
        )?;
        Ok(())
    }

    pub fn replication_generation(&self) -> Result<u64> {
        let conn = self.connect()?;
        let generation: i64 = conn.query_row(
            "SELECT applied_generation FROM ha_replication_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        u64::try_from(generation).context("negative HA replication generation in SQLite")
    }

    pub fn pending_replication_changes(&self, limit: usize) -> Result<Vec<ReplicationChange>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let conn = self.connect()?;
        let published_seq: i64 = conn.query_row(
            "SELECT published_seq FROM ha_replication_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        let mut stmt = conn.prepare(
            r#"SELECT seq, table_name, operation, row_json
               FROM ha_change_log
               WHERE seq > ?1
               ORDER BY seq
               LIMIT ?2"#,
        )?;
        let rows = stmt.query_map(params![published_seq, limit as i64], |row| {
            Ok(ReplicationChange {
                seq: row.get(0)?,
                table: row.get(1)?,
                operation: row.get(2)?,
                row_json: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn mark_replication_published(&self, through_seq: i64, generation: u64) -> Result<()> {
        let generation = i64::try_from(generation).context("HA replication generation overflow")?;
        let mut conn = self.connect()?;
        let tx = conn.transaction()?;
        let (current_generation, published_seq): (i64, i64) = tx.query_row(
            "SELECT applied_generation, published_seq FROM ha_replication_state WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;

        if generation != current_generation.saturating_add(1) {
            bail!(
                "cannot publish HA replication generation {generation}; local generation is {current_generation}"
            );
        }
        if through_seq < published_seq {
            bail!(
                "cannot move HA published sequence backwards from {published_seq} to {through_seq}"
            );
        }

        tx.execute(
            r#"UPDATE ha_replication_state
               SET applied_generation = ?1, published_seq = ?2
               WHERE singleton = 1"#,
            params![generation, through_seq],
        )?;
        tx.execute("DELETE FROM ha_change_log WHERE seq <= ?1", [through_seq])?;
        tx.commit()?;
        Ok(())
    }

    pub fn apply_replication_changes(
        &self,
        previous_generation: u64,
        generation: u64,
        changes: &[ReplicationChange],
    ) -> Result<()> {
        if generation != previous_generation.saturating_add(1) {
            bail!(
                "HA replication delta must advance exactly one generation: previous={previous_generation}, next={generation}"
            );
        }

        let previous_generation =
            i64::try_from(previous_generation).context("HA replication generation overflow")?;
        let generation = i64::try_from(generation).context("HA replication generation overflow")?;
        let mut conn = self.connect()?;
        let tx = conn.transaction()?;
        let current_generation: i64 = tx.query_row(
            "SELECT applied_generation FROM ha_replication_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        if current_generation != previous_generation {
            bail!(
                "HA replication delta starts at generation {previous_generation}, but local database is at {current_generation}"
            );
        }

        tx.execute(
            "UPDATE ha_replication_state SET applying = 1 WHERE singleton = 1",
            [],
        )?;
        for change in changes {
            apply_replication_change(&tx, change)?;
        }
        tx.execute(
            r#"UPDATE ha_replication_state
               SET applied_generation = ?1, applying = 0
               WHERE singleton = 1"#,
            [generation],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn export_replication_snapshot(&self) -> Result<ReplicationSnapshot> {
        Ok(self.export_replication_snapshot_with_watermark()?.0)
    }

    pub fn export_replication_snapshot_with_watermark(&self) -> Result<(ReplicationSnapshot, i64)> {
        let mut conn = self.connect()?;
        let tx = conn.transaction()?;
        let generation: i64 = tx.query_row(
            "SELECT applied_generation FROM ha_replication_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        let generation =
            u64::try_from(generation).context("negative HA replication generation in SQLite")?;
        let published_seq: i64 = tx.query_row(
            "SELECT published_seq FROM ha_replication_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        let max_change_seq: Option<i64> =
            tx.query_row("SELECT MAX(seq) FROM ha_change_log", [], |row| row.get(0))?;
        let through_seq = max_change_seq.unwrap_or(published_seq).max(published_seq);
        let mut rows = Vec::new();

        append_snapshot_rows(
            &tx,
            "meta",
            r#"SELECT json_object('key', key, 'value', value)
               FROM meta
               WHERE key = 'matrix_next_batch'
               ORDER BY key"#,
            &mut rows,
        )?;
        append_snapshot_rows(
            &tx,
            "rooms",
            r#"SELECT json_object(
                 'matrix_room_id', matrix_room_id,
                 'telegram_thread_id', telegram_thread_id,
                 'title', title,
                 'status', status,
                 'updated_at', updated_at
               ) FROM rooms ORDER BY matrix_room_id"#,
            &mut rows,
        )?;
        append_snapshot_rows(
            &tx,
            "seen_matrix_events",
            r#"SELECT json_object(
                 'event_id', event_id,
                 'matrix_room_id', matrix_room_id,
                 'created_at', created_at
               ) FROM seen_matrix_events ORDER BY event_id"#,
            &mut rows,
        )?;
        append_snapshot_rows(
            &tx,
            "pending_read_receipts",
            r#"SELECT json_object(
                 'matrix_room_id', matrix_room_id,
                 'event_id', event_id,
                 'updated_at', updated_at
               ) FROM pending_read_receipts ORDER BY matrix_room_id"#,
            &mut rows,
        )?;
        append_snapshot_rows(
            &tx,
            "archived_topics",
            r#"SELECT json_object(
                 'telegram_thread_id', telegram_thread_id,
                 'title', title,
                 'archived_at', archived_at
               ) FROM archived_topics ORDER BY telegram_thread_id"#,
            &mut rows,
        )?;
        append_snapshot_rows(
            &tx,
            "profile_intro_messages",
            r#"SELECT json_object(
                 'telegram_thread_id', telegram_thread_id,
                 'telegram_message_id', telegram_message_id,
                 'message_kind', message_kind,
                 'intro_text', intro_text,
                 'updated_at', updated_at
               ) FROM profile_intro_messages ORDER BY telegram_thread_id"#,
            &mut rows,
        )?;
        append_snapshot_rows(
            &tx,
            "room_settings",
            r#"SELECT json_object(
                 'matrix_room_id', matrix_room_id,
                 'allow_delete', allow_delete,
                 'updated_at', updated_at
               ) FROM room_settings ORDER BY matrix_room_id"#,
            &mut rows,
        )?;
        append_snapshot_rows(
            &tx,
            "message_snapshots",
            r#"SELECT json_object(
                 'matrix_event_id', matrix_event_id,
                 'telegram_thread_id', telegram_thread_id,
                 'telegram_message_id', telegram_message_id,
                 'rendered_text', rendered_text,
                 'media_file_id', media_file_id,
                 'media_type', media_type,
                 'created_at', created_at
               ) FROM message_snapshots
               ORDER BY matrix_event_id, telegram_thread_id, telegram_message_id"#,
            &mut rows,
        )?;
        append_snapshot_rows(
            &tx,
            "matrix_reply_edges",
            r#"SELECT json_object(
                 'matrix_room_id', matrix_room_id,
                 'child_event_id', child_event_id,
                 'parent_event_id', parent_event_id,
                 'created_at', created_at
               ) FROM matrix_reply_edges
               ORDER BY matrix_room_id, child_event_id"#,
            &mut rows,
        )?;
        append_snapshot_rows(
            &tx,
            "message_map",
            r#"SELECT json_object(
                 'matrix_event_id', matrix_event_id,
                 'matrix_room_id', matrix_room_id,
                 'telegram_thread_id', telegram_thread_id,
                 'telegram_message_id', telegram_message_id,
                 'direction', direction,
                 'message_kind', message_kind,
                 'part_index', part_index,
                 'created_at', created_at
               ) FROM message_map
               ORDER BY telegram_thread_id, telegram_message_id"#,
            &mut rows,
        )?;

        tx.commit()?;
        Ok((ReplicationSnapshot { generation, rows }, through_seq))
    }

    pub fn establish_replication_baseline(&self, generation: u64, through_seq: i64) -> Result<()> {
        let generation = i64::try_from(generation).context("HA replication generation overflow")?;
        let mut conn = self.connect()?;
        let tx = conn.transaction()?;
        let (current_generation, published_seq): (i64, i64) = tx.query_row(
            "SELECT applied_generation, published_seq FROM ha_replication_state WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;

        if current_generation != generation {
            bail!(
                "cannot establish HA replication baseline at generation {generation}; local generation is {current_generation}"
            );
        }
        if through_seq < published_seq {
            bail!(
                "cannot move HA baseline sequence backwards from {published_seq} to {through_seq}"
            );
        }

        tx.execute(
            "UPDATE ha_replication_state SET published_seq = ?1 WHERE singleton = 1",
            [through_seq],
        )?;
        tx.execute("DELETE FROM ha_change_log WHERE seq <= ?1", [through_seq])?;
        tx.commit()?;
        Ok(())
    }

    pub fn apply_replication_snapshot(&self, snapshot: &ReplicationSnapshot) -> Result<()> {
        let generation =
            i64::try_from(snapshot.generation).context("HA replication generation overflow")?;
        let mut conn = self.connect()?;
        let tx = conn.transaction()?;

        tx.execute(
            "UPDATE ha_replication_state SET applying = 1 WHERE singleton = 1",
            [],
        )?;

        tx.execute("DELETE FROM message_map", [])?;
        tx.execute("DELETE FROM matrix_reply_edges", [])?;
        tx.execute("DELETE FROM message_snapshots", [])?;
        tx.execute("DELETE FROM room_settings", [])?;
        tx.execute("DELETE FROM profile_intro_messages", [])?;
        tx.execute("DELETE FROM archived_topics", [])?;
        tx.execute("DELETE FROM pending_read_receipts", [])?;
        tx.execute("DELETE FROM seen_matrix_events", [])?;
        tx.execute("DELETE FROM rooms", [])?;
        tx.execute("DELETE FROM meta WHERE key = 'matrix_next_batch'", [])?;

        for row in &snapshot.rows {
            apply_replication_change(
                &tx,
                &ReplicationChange {
                    seq: 0,
                    table: row.table.clone(),
                    operation: "upsert".to_owned(),
                    row_json: row.row_json.clone(),
                },
            )?;
        }

        tx.execute("DELETE FROM ha_change_log", [])?;
        tx.execute(
            r#"UPDATE ha_replication_state
               SET applied_generation = ?1, published_seq = 0, applying = 0
               WHERE singleton = 1"#,
            [generation],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn get_meta(&self, key: &str) -> Result<Option<String>> {
        let conn = self.connect()?;
        Ok(conn
            .query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| {
                row.get(0)
            })
            .optional()?)
    }

    pub fn set_meta(&self, key: &str, value: &str) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "INSERT INTO meta(key, value) VALUES(?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn auth_alert_active(&self) -> Result<bool> {
        Ok(self.get_meta("auth_alert_active")?.as_deref() == Some("1"))
    }

    pub fn set_auth_alert_active(&self, active: bool) -> Result<()> {
        self.set_meta("auth_alert_active", if active { "1" } else { "0" })
    }

    pub fn record_archived_topic(&self, thread_id: i64, title: &str) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "INSERT INTO archived_topics(telegram_thread_id, title, archived_at) VALUES(?1, ?2, ?3) ON CONFLICT(telegram_thread_id) DO UPDATE SET title = excluded.title, archived_at = excluded.archived_at",
            params![thread_id, title, now_unix()],
        )?;
        Ok(())
    }

    pub fn archived_topics(&self) -> Result<Vec<(i64, String)>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            "SELECT telegram_thread_id, title FROM archived_topics ORDER BY archived_at, telegram_thread_id",
        )?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn remove_archived_topic(&self, thread_id: i64) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "DELETE FROM archived_topics WHERE telegram_thread_id = ?1",
            [thread_id],
        )?;
        Ok(())
    }

    pub fn load_matrix_session(&self) -> Result<Option<StoredMatrixSession>> {
        let conn = self.connect()?;
        Ok(conn
            .query_row(
                "SELECT access_token, user_id, expires_at_ms FROM matrix_auth WHERE singleton = 1",
                [],
                |row| {
                    Ok(StoredMatrixSession {
                        access_token: row.get(0)?,
                        user_id: row.get(1)?,
                        expires_at_ms: row.get(2)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn save_matrix_session(&self, session: &StoredMatrixSession) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            r#"INSERT INTO matrix_auth(singleton, access_token, user_id, expires_at_ms, updated_at)
               VALUES(1, ?1, ?2, ?3, ?4)
               ON CONFLICT(singleton) DO UPDATE SET
                 access_token = excluded.access_token,
                 user_id = excluded.user_id,
                 expires_at_ms = excluded.expires_at_ms,
                 updated_at = excluded.updated_at"#,
            params![
                &session.access_token,
                &session.user_id,
                session.expires_at_ms,
                now_unix()
            ],
        )?;
        Ok(())
    }

    pub fn clear_matrix_session(&self) -> Result<()> {
        let conn = self.connect()?;
        conn.execute("DELETE FROM matrix_auth WHERE singleton = 1", [])?;
        Ok(())
    }

    pub fn room_by_matrix(&self, room_id: &str) -> Result<Option<RoomMap>> {
        let conn = self.connect()?;
        Ok(conn
            .query_row(
                "SELECT matrix_room_id, telegram_thread_id, title, status FROM rooms WHERE matrix_room_id = ?1",
                [room_id],
                room_from_row,
            )
            .optional()?)
    }

    pub fn room_by_thread(&self, thread_id: i64) -> Result<Option<RoomMap>> {
        let conn = self.connect()?;
        Ok(conn
            .query_row(
                "SELECT matrix_room_id, telegram_thread_id, title, status FROM rooms WHERE telegram_thread_id = ?1",
                [thread_id],
                room_from_row,
            )
            .optional()?)
    }

    pub fn rooms(&self) -> Result<Vec<RoomMap>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            "SELECT matrix_room_id, telegram_thread_id, title, status FROM rooms ORDER BY telegram_thread_id",
        )?;
        let rows = stmt.query_map([], room_from_row)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn upsert_room(
        &self,
        room_id: &str,
        thread_id: i64,
        title: &str,
        status: &str,
    ) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            r#"INSERT INTO rooms(matrix_room_id, telegram_thread_id, title, status, updated_at)
               VALUES(?1, ?2, ?3, ?4, ?5)
               ON CONFLICT(matrix_room_id) DO UPDATE SET
                 telegram_thread_id = excluded.telegram_thread_id,
                 title = excluded.title,
                 status = excluded.status,
                 updated_at = excluded.updated_at"#,
            params![room_id, thread_id, title, status, now_unix()],
        )?;
        Ok(())
    }

    pub fn set_room_status(&self, room_id: &str, status: &str) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "UPDATE rooms SET status = ?2, updated_at = ?3 WHERE matrix_room_id = ?1",
            params![room_id, status, now_unix()],
        )?;
        Ok(())
    }

    pub fn set_room_title(&self, room_id: &str, title: &str) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "UPDATE rooms SET title = ?2, updated_at = ?3 WHERE matrix_room_id = ?1",
            params![room_id, title, now_unix()],
        )?;
        Ok(())
    }

    pub fn profile_intro_message(&self, thread_id: i64) -> Result<Option<(i64, String, String)>> {
        let conn = self.connect()?;
        Ok(conn
            .query_row(
                "SELECT telegram_message_id, message_kind, intro_text FROM profile_intro_messages WHERE telegram_thread_id = ?1",
                [thread_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?)
    }

    pub fn set_profile_intro_message(
        &self,
        thread_id: i64,
        message_id: i64,
        message_kind: &str,
        intro_text: &str,
    ) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            r#"INSERT INTO profile_intro_messages(
                 telegram_thread_id, telegram_message_id, message_kind, intro_text, updated_at
               )
               VALUES(?1, ?2, ?3, ?4, ?5)
               ON CONFLICT(telegram_thread_id) DO UPDATE SET
                 telegram_message_id = excluded.telegram_message_id,
                 message_kind = excluded.message_kind,
                 intro_text = excluded.intro_text,
                 updated_at = excluded.updated_at"#,
            params![thread_id, message_id, message_kind, intro_text, now_unix()],
        )?;
        Ok(())
    }

    pub fn allow_delete(&self, room_id: &str) -> Result<bool> {
        let conn = self.connect()?;
        let value: Option<i64> = conn
            .query_row(
                "SELECT allow_delete FROM room_settings WHERE matrix_room_id = ?1",
                [room_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(value.unwrap_or(1) != 0)
    }

    pub fn set_allow_delete(&self, room_id: &str, allow_delete: bool) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            r#"INSERT INTO room_settings(matrix_room_id, allow_delete, updated_at)
               VALUES(?1, ?2, ?3)
               ON CONFLICT(matrix_room_id) DO UPDATE SET
                 allow_delete = excluded.allow_delete,
                 updated_at = excluded.updated_at"#,
            params![room_id, i64::from(allow_delete), now_unix()],
        )?;
        Ok(())
    }

    pub fn save_message_snapshot(
        &self,
        event_id: &str,
        thread_id: i64,
        message_id: i64,
        rendered_text: Option<&str>,
        media_file_id: Option<&str>,
        media_type: Option<&str>,
    ) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            r#"INSERT INTO message_snapshots(
                 matrix_event_id, telegram_thread_id, telegram_message_id,
                 rendered_text, media_file_id, media_type, created_at
               )
               VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
               ON CONFLICT(matrix_event_id, telegram_thread_id, telegram_message_id) DO UPDATE SET
                 rendered_text = excluded.rendered_text,
                 media_file_id = excluded.media_file_id,
                 media_type = excluded.media_type,
                 created_at = excluded.created_at"#,
            params![
                event_id,
                thread_id,
                message_id,
                rendered_text,
                media_file_id,
                media_type,
                now_unix()
            ],
        )?;
        Ok(())
    }

    pub fn message_snapshot(
        &self,
        event_id: &str,
        thread_id: i64,
        message_id: i64,
    ) -> Result<Option<MessageSnapshot>> {
        let conn = self.connect()?;
        Ok(conn
            .query_row(
                r#"SELECT rendered_text, media_file_id, media_type
                   FROM message_snapshots
                   WHERE matrix_event_id = ?1
                     AND telegram_thread_id = ?2
                     AND telegram_message_id = ?3"#,
                params![event_id, thread_id, message_id],
                |row| {
                    Ok(MessageSnapshot {
                        rendered_text: row.get(0)?,
                        media_file_id: row.get(1)?,
                        media_type: row.get(2)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn record_matrix_reply(
        &self,
        room_id: &str,
        child_event_id: &str,
        parent_event_id: &str,
    ) -> Result<()> {
        if child_event_id.is_empty()
            || parent_event_id.is_empty()
            || child_event_id == parent_event_id
        {
            return Ok(());
        }
        let conn = self.connect()?;
        conn.execute(
            r#"INSERT INTO matrix_reply_edges(
                 matrix_room_id, child_event_id, parent_event_id, created_at
               )
               VALUES(?1, ?2, ?3, ?4)
               ON CONFLICT(matrix_room_id, child_event_id) DO UPDATE SET
                 parent_event_id = excluded.parent_event_id,
                 created_at = excluded.created_at"#,
            params![room_id, child_event_id, parent_event_id, now_unix()],
        )?;
        Ok(())
    }

    pub fn matrix_reply_subtree(&self, room_id: &str, root_event_id: &str) -> Result<Vec<String>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"WITH RECURSIVE subtree(event_id) AS (
                 SELECT ?2
                 UNION
                 SELECT e.child_event_id
                 FROM matrix_reply_edges e
                 JOIN subtree s ON e.parent_event_id = s.event_id
                 WHERE e.matrix_room_id = ?1
               )
               SELECT event_id
               FROM subtree
               ORDER BY CASE WHEN event_id = ?2 THEN 0 ELSE 1 END, event_id"#,
        )?;
        let rows = stmt.query_map(params![room_id, root_event_id], |row| row.get(0))?;
        Ok(rows.collect::<rusqlite::Result<Vec<String>>>()?)
    }

    pub fn matrix_event_seen(&self, event_id: &str) -> Result<bool> {
        let conn = self.connect()?;
        let found: Option<i64> = conn
            .query_row(
                "SELECT 1 FROM seen_matrix_events WHERE event_id = ?1",
                [event_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(found.is_some())
    }

    pub fn mark_matrix_event_seen(&self, event_id: &str, room_id: &str) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "INSERT OR IGNORE INTO seen_matrix_events(event_id, matrix_room_id, created_at) VALUES(?1, ?2, ?3)",
            params![event_id, room_id, now_unix()],
        )?;
        Ok(())
    }

    pub fn record_matrix_delivery_kind(
        &self,
        room_id: &str,
        event_id: &str,
        thread_id: i64,
        telegram_message_ids: &[i64],
        message_kind: &str,
    ) -> Result<()> {
        let mut conn = self.connect()?;
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT OR IGNORE INTO seen_matrix_events(event_id, matrix_room_id, created_at) VALUES(?1, ?2, ?3)",
            params![event_id, room_id, now_unix()],
        )?;
        for (part_index, message_id) in telegram_message_ids.iter().enumerate() {
            insert_message_map(
                &tx,
                MessageMapInsert {
                    event_id,
                    room_id,
                    thread_id,
                    message_id: *message_id,
                    direction: "matrix_to_telegram",
                    message_kind,
                    part_index: part_index as i64,
                },
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn record_telegram_delivery(
        &self,
        room_id: &str,
        event_id: &str,
        thread_id: i64,
        telegram_message_id: i64,
    ) -> Result<()> {
        self.record_telegram_delivery_kind(
            room_id,
            event_id,
            thread_id,
            telegram_message_id,
            "text",
        )
    }

    pub fn record_telegram_delivery_kind(
        &self,
        room_id: &str,
        event_id: &str,
        thread_id: i64,
        telegram_message_id: i64,
        message_kind: &str,
    ) -> Result<()> {
        let mut conn = self.connect()?;
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT OR IGNORE INTO seen_matrix_events(event_id, matrix_room_id, created_at) VALUES(?1, ?2, ?3)",
            params![event_id, room_id, now_unix()],
        )?;
        insert_message_map(
            &tx,
            MessageMapInsert {
                event_id,
                room_id,
                thread_id,
                message_id: telegram_message_id,
                direction: "telegram_to_matrix",
                message_kind,
                part_index: 0,
            },
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn telegram_message_for_matrix(
        &self,
        matrix_event_id: &str,
        thread_id: i64,
    ) -> Result<Option<i64>> {
        let conn = self.connect()?;
        Ok(conn
            .query_row(
                r#"SELECT telegram_message_id
                   FROM message_map
                   WHERE matrix_event_id = ?1 AND telegram_thread_id = ?2
                   ORDER BY telegram_message_id DESC
                   LIMIT 1"#,
                params![matrix_event_id, thread_id],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub fn telegram_deliveries_for_matrix(
        &self,
        matrix_event_id: &str,
        thread_id: i64,
    ) -> Result<Vec<TelegramDelivery>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"SELECT telegram_message_id, direction, message_kind, part_index
               FROM message_map
               WHERE matrix_event_id = ?1 AND telegram_thread_id = ?2
               ORDER BY part_index, telegram_message_id"#,
        )?;
        let rows = stmt.query_map(params![matrix_event_id, thread_id], |row| {
            Ok(TelegramDelivery {
                message_id: row.get(0)?,
                direction: row.get(1)?,
                message_kind: row.get(2)?,
                part_index: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn matrix_event_for_telegram(
        &self,
        thread_id: i64,
        telegram_message_id: i64,
    ) -> Result<Option<String>> {
        let conn = self.connect()?;
        Ok(conn
            .query_row(
                r#"SELECT matrix_event_id
                   FROM message_map
                   WHERE telegram_thread_id = ?1 AND telegram_message_id = ?2
                   LIMIT 1"#,
                params![thread_id, telegram_message_id],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub fn replace_room_mapping_and_record_deliveries(
        &self,
        room_id: &str,
        thread_id: i64,
        title: &str,
        status: &str,
        deliveries: &[(String, i64, i64, String)],
    ) -> Result<()> {
        let mut conn = self.connect()?;
        let tx = conn.transaction()?;
        tx.execute(
            r#"INSERT INTO rooms(matrix_room_id, telegram_thread_id, title, status, updated_at)
               VALUES(?1, ?2, ?3, ?4, ?5)
               ON CONFLICT(matrix_room_id) DO UPDATE SET
                 telegram_thread_id = excluded.telegram_thread_id,
                 title = excluded.title,
                 status = excluded.status,
                 updated_at = excluded.updated_at"#,
            params![room_id, thread_id, title, status, now_unix()],
        )?;
        for (event_id, message_id, part_index, message_kind) in deliveries {
            tx.execute(
                "INSERT OR IGNORE INTO seen_matrix_events(event_id, matrix_room_id, created_at) VALUES(?1, ?2, ?3)",
                params![event_id, room_id, now_unix()],
            )?;
            insert_message_map(
                &tx,
                MessageMapInsert {
                    event_id,
                    room_id,
                    thread_id,
                    message_id: *message_id,
                    direction: "matrix_to_telegram",
                    message_kind,
                    part_index: *part_index,
                },
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn queue_read_receipt(&self, room_id: &str, event_id: &str) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            r#"INSERT INTO pending_read_receipts(matrix_room_id, event_id, updated_at)
               VALUES(?1, ?2, ?3)
               ON CONFLICT(matrix_room_id) DO UPDATE SET
                 event_id = excluded.event_id,
                 updated_at = excluded.updated_at"#,
            params![room_id, event_id, now_unix()],
        )?;
        Ok(())
    }

    pub fn pending_read_receipts(&self) -> Result<Vec<(String, String)>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            "SELECT matrix_room_id, event_id FROM pending_read_receipts ORDER BY updated_at",
        )?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn clear_read_receipt(&self, room_id: &str, event_id: &str) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "DELETE FROM pending_read_receipts WHERE matrix_room_id = ?1 AND event_id = ?2",
            params![room_id, event_id],
        )?;
        Ok(())
    }

    pub fn prune_seen_events(&self, older_than_secs: i64) -> Result<usize> {
        let conn = self.connect()?;
        let cutoff = now_unix().saturating_sub(older_than_secs);
        Ok(conn.execute(
            "DELETE FROM seen_matrix_events WHERE created_at < ?1",
            [cutoff],
        )?)
    }
}

fn append_snapshot_rows(
    tx: &rusqlite::Transaction<'_>,
    table: &str,
    sql: &str,
    rows: &mut Vec<ReplicationSnapshotRow>,
) -> Result<()> {
    let mut stmt = tx.prepare(sql)?;
    let values = stmt.query_map([], |row| row.get::<_, String>(0))?;
    for row_json in values {
        rows.push(ReplicationSnapshotRow {
            table: table.to_owned(),
            row_json: row_json?,
        });
    }
    Ok(())
}

fn json_string(row: &Value, key: &str) -> Result<String> {
    row.get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .with_context(|| format!("HA replication row has no string field {key}"))
}

fn json_i64(row: &Value, key: &str) -> Result<i64> {
    row.get(key)
        .and_then(Value::as_i64)
        .with_context(|| format!("HA replication row has no integer field {key}"))
}

fn json_optional_string(row: &Value, key: &str) -> Result<Option<String>> {
    match row.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => bail!("HA replication row field {key} is not string or null"),
    }
}

fn apply_replication_change(
    tx: &rusqlite::Transaction<'_>,
    change: &ReplicationChange,
) -> Result<()> {
    let row: Value = serde_json::from_str(&change.row_json)
        .with_context(|| format!("invalid HA replication JSON for {}", change.table))?;

    match (change.table.as_str(), change.operation.as_str()) {
        ("meta", "upsert") => {
            let key = json_string(&row, "key")?;
            if key != "matrix_next_batch" {
                bail!("refusing to replicate unsupported meta key {key}");
            }
            tx.execute(
                r#"INSERT INTO meta(key, value) VALUES(?1, ?2)
                   ON CONFLICT(key) DO UPDATE SET value = excluded.value"#,
                params![key, json_string(&row, "value")?],
            )?;
        }
        ("rooms", "upsert") => {
            tx.execute(
                r#"INSERT INTO rooms(matrix_room_id, telegram_thread_id, title, status, updated_at)
                   VALUES(?1, ?2, ?3, ?4, ?5)
                   ON CONFLICT(matrix_room_id) DO UPDATE SET
                     telegram_thread_id = excluded.telegram_thread_id,
                     title = excluded.title,
                     status = excluded.status,
                     updated_at = excluded.updated_at"#,
                params![
                    json_string(&row, "matrix_room_id")?,
                    json_i64(&row, "telegram_thread_id")?,
                    json_string(&row, "title")?,
                    json_string(&row, "status")?,
                    json_i64(&row, "updated_at")?
                ],
            )?;
        }
        ("rooms", "delete") => {
            tx.execute(
                "DELETE FROM rooms WHERE matrix_room_id = ?1",
                [json_string(&row, "matrix_room_id")?],
            )?;
        }
        ("seen_matrix_events", "upsert") => {
            tx.execute(
                r#"INSERT INTO seen_matrix_events(event_id, matrix_room_id, created_at)
                   VALUES(?1, ?2, ?3)
                   ON CONFLICT(event_id) DO UPDATE SET
                     matrix_room_id = excluded.matrix_room_id,
                     created_at = excluded.created_at"#,
                params![
                    json_string(&row, "event_id")?,
                    json_string(&row, "matrix_room_id")?,
                    json_i64(&row, "created_at")?
                ],
            )?;
        }
        ("seen_matrix_events", "delete") => {
            tx.execute(
                "DELETE FROM seen_matrix_events WHERE event_id = ?1",
                [json_string(&row, "event_id")?],
            )?;
        }
        ("pending_read_receipts", "upsert") => {
            tx.execute(
                r#"INSERT INTO pending_read_receipts(matrix_room_id, event_id, updated_at)
                   VALUES(?1, ?2, ?3)
                   ON CONFLICT(matrix_room_id) DO UPDATE SET
                     event_id = excluded.event_id,
                     updated_at = excluded.updated_at"#,
                params![
                    json_string(&row, "matrix_room_id")?,
                    json_string(&row, "event_id")?,
                    json_i64(&row, "updated_at")?
                ],
            )?;
        }
        ("pending_read_receipts", "delete") => {
            tx.execute(
                "DELETE FROM pending_read_receipts WHERE matrix_room_id = ?1",
                [json_string(&row, "matrix_room_id")?],
            )?;
        }
        ("archived_topics", "upsert") => {
            tx.execute(
                r#"INSERT INTO archived_topics(telegram_thread_id, title, archived_at)
                   VALUES(?1, ?2, ?3)
                   ON CONFLICT(telegram_thread_id) DO UPDATE SET
                     title = excluded.title,
                     archived_at = excluded.archived_at"#,
                params![
                    json_i64(&row, "telegram_thread_id")?,
                    json_string(&row, "title")?,
                    json_i64(&row, "archived_at")?
                ],
            )?;
        }
        ("archived_topics", "delete") => {
            tx.execute(
                "DELETE FROM archived_topics WHERE telegram_thread_id = ?1",
                [json_i64(&row, "telegram_thread_id")?],
            )?;
        }
        ("profile_intro_messages", "upsert") => {
            tx.execute(
                r#"INSERT INTO profile_intro_messages(
                     telegram_thread_id, telegram_message_id, message_kind, intro_text, updated_at
                   ) VALUES(?1, ?2, ?3, ?4, ?5)
                   ON CONFLICT(telegram_thread_id) DO UPDATE SET
                     telegram_message_id = excluded.telegram_message_id,
                     message_kind = excluded.message_kind,
                     intro_text = excluded.intro_text,
                     updated_at = excluded.updated_at"#,
                params![
                    json_i64(&row, "telegram_thread_id")?,
                    json_i64(&row, "telegram_message_id")?,
                    json_string(&row, "message_kind")?,
                    json_string(&row, "intro_text")?,
                    json_i64(&row, "updated_at")?
                ],
            )?;
        }
        ("profile_intro_messages", "delete") => {
            tx.execute(
                "DELETE FROM profile_intro_messages WHERE telegram_thread_id = ?1",
                [json_i64(&row, "telegram_thread_id")?],
            )?;
        }
        ("room_settings", "upsert") => {
            tx.execute(
                r#"INSERT INTO room_settings(matrix_room_id, allow_delete, updated_at)
                   VALUES(?1, ?2, ?3)
                   ON CONFLICT(matrix_room_id) DO UPDATE SET
                     allow_delete = excluded.allow_delete,
                     updated_at = excluded.updated_at"#,
                params![
                    json_string(&row, "matrix_room_id")?,
                    json_i64(&row, "allow_delete")?,
                    json_i64(&row, "updated_at")?
                ],
            )?;
        }
        ("room_settings", "delete") => {
            tx.execute(
                "DELETE FROM room_settings WHERE matrix_room_id = ?1",
                [json_string(&row, "matrix_room_id")?],
            )?;
        }
        ("message_snapshots", "upsert") => {
            tx.execute(
                r#"INSERT INTO message_snapshots(
                     matrix_event_id, telegram_thread_id, telegram_message_id,
                     rendered_text, media_file_id, media_type, created_at
                   ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
                   ON CONFLICT(matrix_event_id, telegram_thread_id, telegram_message_id) DO UPDATE SET
                     rendered_text = excluded.rendered_text,
                     media_file_id = excluded.media_file_id,
                     media_type = excluded.media_type,
                     created_at = excluded.created_at"#,
                params![
                    json_string(&row, "matrix_event_id")?,
                    json_i64(&row, "telegram_thread_id")?,
                    json_i64(&row, "telegram_message_id")?,
                    json_optional_string(&row, "rendered_text")?,
                    json_optional_string(&row, "media_file_id")?,
                    json_optional_string(&row, "media_type")?,
                    json_i64(&row, "created_at")?
                ],
            )?;
        }
        ("message_snapshots", "delete") => {
            tx.execute(
                r#"DELETE FROM message_snapshots
                   WHERE matrix_event_id = ?1
                     AND telegram_thread_id = ?2
                     AND telegram_message_id = ?3"#,
                params![
                    json_string(&row, "matrix_event_id")?,
                    json_i64(&row, "telegram_thread_id")?,
                    json_i64(&row, "telegram_message_id")?
                ],
            )?;
        }
        ("matrix_reply_edges", "upsert") => {
            tx.execute(
                r#"INSERT INTO matrix_reply_edges(
                     matrix_room_id, child_event_id, parent_event_id, created_at
                   ) VALUES(?1, ?2, ?3, ?4)
                   ON CONFLICT(matrix_room_id, child_event_id) DO UPDATE SET
                     parent_event_id = excluded.parent_event_id,
                     created_at = excluded.created_at"#,
                params![
                    json_string(&row, "matrix_room_id")?,
                    json_string(&row, "child_event_id")?,
                    json_string(&row, "parent_event_id")?,
                    json_i64(&row, "created_at")?
                ],
            )?;
        }
        ("matrix_reply_edges", "delete") => {
            tx.execute(
                r#"DELETE FROM matrix_reply_edges
                   WHERE matrix_room_id = ?1 AND child_event_id = ?2"#,
                params![
                    json_string(&row, "matrix_room_id")?,
                    json_string(&row, "child_event_id")?
                ],
            )?;
        }
        ("message_map", "upsert") => {
            tx.execute(
                r#"INSERT INTO message_map(
                     matrix_event_id, matrix_room_id, telegram_thread_id,
                     telegram_message_id, direction, message_kind, part_index, created_at
                   ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                   ON CONFLICT(telegram_thread_id, telegram_message_id) DO UPDATE SET
                     matrix_event_id = excluded.matrix_event_id,
                     matrix_room_id = excluded.matrix_room_id,
                     direction = excluded.direction,
                     message_kind = excluded.message_kind,
                     part_index = excluded.part_index,
                     created_at = excluded.created_at"#,
                params![
                    json_string(&row, "matrix_event_id")?,
                    json_string(&row, "matrix_room_id")?,
                    json_i64(&row, "telegram_thread_id")?,
                    json_i64(&row, "telegram_message_id")?,
                    json_string(&row, "direction")?,
                    json_string(&row, "message_kind")?,
                    json_i64(&row, "part_index")?,
                    json_i64(&row, "created_at")?
                ],
            )?;
        }
        ("message_map", "delete") => {
            tx.execute(
                r#"DELETE FROM message_map
                   WHERE telegram_thread_id = ?1 AND telegram_message_id = ?2"#,
                params![
                    json_i64(&row, "telegram_thread_id")?,
                    json_i64(&row, "telegram_message_id")?
                ],
            )?;
        }
        _ => bail!(
            "unsupported HA replication operation {} on table {}",
            change.operation,
            change.table
        ),
    }

    Ok(())
}

fn insert_message_map(tx: &rusqlite::Transaction<'_>, entry: MessageMapInsert<'_>) -> Result<()> {
    tx.execute(
        r#"INSERT INTO message_map(
             matrix_event_id, matrix_room_id, telegram_thread_id,
             telegram_message_id, direction, message_kind, part_index, created_at
           )
           VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
           ON CONFLICT(telegram_thread_id, telegram_message_id) DO UPDATE SET
             matrix_event_id = excluded.matrix_event_id,
             matrix_room_id = excluded.matrix_room_id,
             direction = excluded.direction,
             message_kind = excluded.message_kind,
             part_index = excluded.part_index,
             created_at = excluded.created_at"#,
        params![
            entry.event_id,
            entry.room_id,
            entry.thread_id,
            entry.message_id,
            entry.direction,
            entry.message_kind,
            entry.part_index,
            now_unix()
        ],
    )?;
    Ok(())
}

fn room_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RoomMap> {
    Ok(RoomMap {
        matrix_room_id: row.get(0)?,
        telegram_thread_id: row.get(1)?,
        title: row.get(2)?,
        status: row.get(3)?,
    })
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod replication_tests {
    use super::*;

    fn temp_db(label: &str) -> Db {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "reddit2tg-{label}-{}-{unique}.sqlite3",
            std::process::id()
        ));
        Db::new(path).unwrap()
    }

    #[test]
    fn replication_change_log_excludes_matrix_auth_and_private_meta() {
        let db = temp_db("replication-filter");
        db.set_meta("auth_alert_active", "1").unwrap();
        db.save_matrix_session(&StoredMatrixSession {
            access_token: "secret-token".to_owned(),
            user_id: "@me:example".to_owned(),
            expires_at_ms: Some(1234),
        })
        .unwrap();

        assert!(db.pending_replication_changes(100).unwrap().is_empty());

        db.set_meta("matrix_next_batch", "s1").unwrap();
        let changes = db.pending_replication_changes(100).unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].table, "meta");
        assert!(changes[0].row_json.contains("matrix_next_batch"));
        assert!(!changes[0].row_json.contains("secret-token"));
    }

    #[test]
    fn replication_bootstrap_baseline_preserves_post_snapshot_changes() {
        let db = temp_db("replication-bootstrap-baseline");

        db.upsert_room("!baseline:reddit", 7001, "Baseline", "joined")
            .unwrap();
        let (snapshot, through_seq) = db.export_replication_snapshot_with_watermark().unwrap();
        assert_eq!(snapshot.generation, 0);
        assert!(through_seq > 0);

        db.set_allow_delete("!baseline:reddit", false).unwrap();
        let before = db.pending_replication_changes(100).unwrap();
        assert!(before.len() >= 2);
        assert!(before.iter().any(|change| change.seq <= through_seq));
        assert!(before.iter().any(|change| change.seq > through_seq));

        db.establish_replication_baseline(snapshot.generation, through_seq)
            .unwrap();

        let after = db.pending_replication_changes(100).unwrap();
        assert!(!after.is_empty());
        assert!(after.iter().all(|change| change.seq > through_seq));
        assert!(after.iter().any(|change| change.table == "room_settings"));
        assert_eq!(db.replication_generation().unwrap(), snapshot.generation);
    }

    #[test]
    fn replication_delta_round_trips_bridge_state() {
        let source = temp_db("replication-source");
        let target = temp_db("replication-target");

        source
            .upsert_room("!room:reddit", 1001, "Alice", "joined")
            .unwrap();
        source.set_allow_delete("!room:reddit", false).unwrap();
        source
            .record_matrix_reply("!room:reddit", "$child", "$parent")
            .unwrap();
        source
            .record_matrix_delivery_kind("!room:reddit", "$event", 1001, &[2001, 2002], "text")
            .unwrap();
        source
            .save_message_snapshot("$event", 1001, 2001, Some("hello"), None, None)
            .unwrap();
        source.set_meta("matrix_next_batch", "s-next").unwrap();

        let changes = source.pending_replication_changes(1000).unwrap();
        assert!(!changes.is_empty());
        let through_seq = changes.last().unwrap().seq;

        target.apply_replication_changes(0, 1, &changes).unwrap();
        source.mark_replication_published(through_seq, 1).unwrap();

        assert_eq!(source.replication_generation().unwrap(), 1);
        assert_eq!(target.replication_generation().unwrap(), 1);
        assert_eq!(
            target
                .room_by_matrix("!room:reddit")
                .unwrap()
                .unwrap()
                .telegram_thread_id,
            1001
        );
        assert!(!target.allow_delete("!room:reddit").unwrap());
        assert_eq!(
            target
                .matrix_event_for_telegram(1001, 2001)
                .unwrap()
                .as_deref(),
            Some("$event")
        );
        assert_eq!(
            target.get_meta("matrix_next_batch").unwrap().as_deref(),
            Some("s-next")
        );
        assert!(target.pending_replication_changes(100).unwrap().is_empty());
    }

    #[test]
    fn replication_snapshot_replaces_only_replicated_state() {
        let source = temp_db("snapshot-source");
        let target = temp_db("snapshot-target");

        source
            .upsert_room("!source:reddit", 3001, "Source", "joined")
            .unwrap();
        source.set_allow_delete("!source:reddit", false).unwrap();
        source
            .set_meta("matrix_next_batch", "source-batch")
            .unwrap();

        let changes = source.pending_replication_changes(100).unwrap();
        let through_seq = changes.last().unwrap().seq;
        source.mark_replication_published(through_seq, 1).unwrap();

        target
            .upsert_room("!stale:reddit", 9999, "Stale", "joined")
            .unwrap();
        target.set_meta("matrix_next_batch", "stale-batch").unwrap();
        target
            .save_matrix_session(&StoredMatrixSession {
                access_token: "target-local-token".to_owned(),
                user_id: "@target:reddit".to_owned(),
                expires_at_ms: Some(9999),
            })
            .unwrap();

        let snapshot = source.export_replication_snapshot().unwrap();
        assert_eq!(snapshot.generation, 1);
        target.apply_replication_snapshot(&snapshot).unwrap();

        assert!(target.room_by_matrix("!stale:reddit").unwrap().is_none());
        assert_eq!(
            target
                .room_by_matrix("!source:reddit")
                .unwrap()
                .unwrap()
                .telegram_thread_id,
            3001
        );
        assert_eq!(
            target.get_meta("matrix_next_batch").unwrap().as_deref(),
            Some("source-batch")
        );
        assert_eq!(
            target.load_matrix_session().unwrap().unwrap().access_token,
            "target-local-token"
        );
        assert_eq!(target.replication_generation().unwrap(), 1);
        assert!(target.pending_replication_changes(100).unwrap().is_empty());
    }

    #[test]
    fn replication_rejects_generation_gap() {
        let db = temp_db("replication-gap");
        let error = db.apply_replication_changes(0, 2, &[]).unwrap_err();
        assert!(format!("{error:#}").contains("advance exactly one generation"));
    }
}
