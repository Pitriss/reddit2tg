use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};

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
            "#,
        )?;
        Ok(())
    }

    pub fn get_meta(&self, key: &str) -> Result<Option<String>> {
        let conn = self.connect()?;
        Ok(conn
            .query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| row.get(0))
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

    pub fn upsert_room(&self, room_id: &str, thread_id: i64, title: &str, status: &str) -> Result<()> {
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

    pub fn mark_matrix_event_seen(&self, event_id: &str, room_id: &str) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "INSERT OR IGNORE INTO seen_matrix_events(event_id, matrix_room_id, created_at) VALUES(?1, ?2, ?3)",
            params![event_id, room_id, now_unix()],
        )?;
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
        Ok(conn.execute("DELETE FROM seen_matrix_events WHERE created_at < ?1", [cutoff])?)
    }
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
