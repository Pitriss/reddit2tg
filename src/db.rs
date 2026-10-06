use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};

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
            "#,
        )?;
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

    pub fn record_matrix_delivery(
        &self,
        room_id: &str,
        event_id: &str,
        thread_id: i64,
        telegram_message_ids: &[i64],
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
                    message_kind: "text",
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
                message_kind: "text",
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
        deliveries: &[(String, i64, i64)],
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
        for (event_id, message_id, part_index) in deliveries {
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
                    message_kind: "text",
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
