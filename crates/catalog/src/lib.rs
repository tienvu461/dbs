use backupd_core::{BackupManifest, BackupRecord, BackupStatus, ChunkManifest};
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("backup or job not found")]
    NotFound,
    #[error("invalid backup record: {0}")]
    InvalidRecord(String),
}

#[derive(Clone)]
pub struct Catalog {
    pool: SqlitePool,
}

impl Catalog {
    pub async fn connect(database_url: &str) -> Result<Self, CatalogError> {
        let options = sqlx::sqlite::SqliteConnectOptions::from_str(database_url)
            .map_err(|error| CatalogError::InvalidRecord(error.to_string()))?
            .create_if_missing(true)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await?;
        let catalog = Self { pool };
        catalog.migrate().await?;
        Ok(catalog)
    }

    async fn migrate(&self) -> Result<(), CatalogError> {
        sqlx::query("CREATE TABLE IF NOT EXISTS backups (id TEXT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL, completed_at TEXT, original_size INTEGER NOT NULL DEFAULT 0, stored_size INTEGER NOT NULL DEFAULT 0, checksum TEXT NOT NULL DEFAULT '', tags_json TEXT NOT NULL DEFAULT '{}', manifest_json TEXT, filename TEXT NOT NULL DEFAULT '', error TEXT, source_path TEXT)").execute(&self.pool).await?;
        let _ = sqlx::query("ALTER TABLE backups ADD COLUMN filename TEXT NOT NULL DEFAULT ''")
            .execute(&self.pool)
            .await;
        let _ = sqlx::query("ALTER TABLE backups ADD COLUMN source_path TEXT")
            .execute(&self.pool)
            .await;
        sqlx::query("CREATE TABLE IF NOT EXISTS blobs (id TEXT PRIMARY KEY, stored_hash TEXT NOT NULL, stored_size INTEGER NOT NULL, created_at TEXT NOT NULL)").execute(&self.pool).await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS blob_locations (blob_id TEXT NOT NULL REFERENCES blobs(id) ON DELETE CASCADE, transport TEXT NOT NULL, remote_locator TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'verified', verified_at TEXT, PRIMARY KEY (blob_id, transport))").execute(&self.pool).await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS artifact_blobs (backup_id TEXT NOT NULL REFERENCES backups(id) ON DELETE CASCADE, sequence INTEGER NOT NULL, blob_id TEXT NOT NULL REFERENCES blobs(id), plaintext_hash TEXT NOT NULL, plaintext_size INTEGER NOT NULL, PRIMARY KEY (backup_id, sequence))").execute(&self.pool).await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS blob_locations (blob_id TEXT NOT NULL REFERENCES blobs(id) ON DELETE CASCADE, transport TEXT NOT NULL, remote_locator TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'verified', verified_at TEXT, PRIMARY KEY (blob_id, transport))").execute(&self.pool).await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY, job_type TEXT NOT NULL, payload_json TEXT NOT NULL,
            status TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
            available_at TEXT NOT NULL, locked_at TEXT, locked_by TEXT,
            error TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS gc_queue (id INTEGER PRIMARY KEY AUTOINCREMENT, blob_id TEXT NOT NULL, transport TEXT NOT NULL, remote_locator TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued', attempt INTEGER NOT NULL DEFAULT 0, available_at TEXT NOT NULL, created_at TEXT NOT NULL, error TEXT)").execute(&self.pool).await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS jobs_claim_idx ON jobs(status, available_at, created_at)",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn enqueue_job(
        &self,
        job_id: &str,
        job_type: &str,
        payload_json: &str,
    ) -> Result<(), CatalogError> {
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query("INSERT INTO jobs (id, job_type, payload_json, status, available_at, created_at, updated_at) VALUES (?, ?, ?, 'queued', ?, ?, ?)")
            .bind(job_id).bind(job_type).bind(payload_json).bind(&now).bind(&now).bind(&now)
            .execute(&self.pool).await?;
        Ok(())
    }

    pub async fn get_job(&self, id: &str) -> Result<JobView, CatalogError> {
        sqlx::query_as::<_, JobView>("SELECT id, job_type, status, attempt, error, created_at, updated_at FROM jobs WHERE id = ?")
            .bind(id).fetch_optional(&self.pool).await?.ok_or(CatalogError::NotFound)
    }

    pub async fn claim_gc_item(&self) -> Result<Option<GcItem>, CatalogError> {
        let now = chrono::Utc::now().to_rfc3339();
        let row = sqlx::query_as::<_, GcItem>("SELECT id, blob_id, transport, remote_locator, attempt FROM gc_queue WHERE status = 'queued' AND available_at <= ? ORDER BY id LIMIT 1")
            .bind(&now).fetch_optional(&self.pool).await?;
        if let Some(item) = &row {
            sqlx::query("UPDATE gc_queue SET status = 'running', attempt = attempt + 1 WHERE id = ? AND status = 'queued'")
                .bind(item.id).execute(&self.pool).await?;
        }
        Ok(row)
    }

    pub async fn finish_gc_item(&self, id: i64) -> Result<(), CatalogError> {
        sqlx::query("DELETE FROM gc_queue WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn retry_gc_item(&self, id: i64, error: &str) -> Result<(), CatalogError> {
        let delay = 30i64;
        sqlx::query(
            "UPDATE gc_queue SET status = 'queued', available_at = ?, error = ? WHERE id = ?",
        )
        .bind((chrono::Utc::now() + chrono::Duration::seconds(delay)).to_rfc3339())
        .bind(error)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn claim_job(&self, worker_id: &str) -> Result<Option<JobRecord>, CatalogError> {
        let mut tx = self.pool.begin().await?;
        let now = chrono::Utc::now().to_rfc3339();
        let row = sqlx::query_as::<_, JobRow>("SELECT id, job_type, payload_json, attempt FROM jobs WHERE status = 'queued' AND available_at <= ? ORDER BY created_at LIMIT 1")
            .bind(&now).fetch_optional(&mut *tx).await?;
        let Some(row) = row else {
            tx.commit().await?;
            return Ok(None);
        };
        let locked = sqlx::query("UPDATE jobs SET status = 'running', locked_at = ?, locked_by = ?, attempt = attempt + 1, updated_at = ? WHERE id = ? AND status = 'queued'")
            .bind(&now).bind(worker_id).bind(&now).bind(&row.id).execute(&mut *tx).await?;
        tx.commit().await?;
        if locked.rows_affected() == 0 {
            return Ok(None);
        }
        Ok(Some(JobRecord {
            id: row.id,
            job_type: row.job_type,
            payload_json: row.payload_json,
            attempt: row.attempt + 1,
        }))
    }

    pub async fn finish_job(&self, id: &str) -> Result<(), CatalogError> {
        sqlx::query("UPDATE jobs SET status = 'complete', locked_at = NULL, locked_by = NULL, error = NULL, updated_at = ? WHERE id = ?")
            .bind(chrono::Utc::now().to_rfc3339()).bind(id).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn retry_job(
        &self,
        id: &str,
        error: &str,
        delay_seconds: i64,
        max_attempts: i64,
    ) -> Result<(), CatalogError> {
        let now = chrono::Utc::now();
        let available_at = (now + chrono::Duration::seconds(delay_seconds)).to_rfc3339();
        sqlx::query("UPDATE jobs SET status = CASE WHEN attempt >= ? THEN 'failed' ELSE 'queued' END, available_at = ?, locked_at = NULL, locked_by = NULL, error = ?, updated_at = ? WHERE id = ?")
            .bind(max_attempts).bind(available_at).bind(error).bind(now.to_rfc3339()).bind(id).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn recover_stale_jobs(
        &self,
        stale_before: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64, CatalogError> {
        let now = chrono::Utc::now().to_rfc3339();
        let mut tx = self.pool.begin().await?;
        let stale_backups: Vec<String> = sqlx::query_scalar::<_, String>("SELECT payload_json FROM jobs WHERE status = 'running' AND locked_at < ? AND job_type = 'ingest_file'")
            .bind(stale_before.to_rfc3339()).fetch_all(&mut *tx).await?
            .into_iter().filter_map(|payload| serde_json::from_str::<serde_json::Value>(&payload).ok()?.get("backup_id")?.as_str().map(str::to_owned)).collect();
        let result = sqlx::query("UPDATE jobs SET status = 'queued', locked_at = NULL, locked_by = NULL, updated_at = ? WHERE status = 'running' AND locked_at < ?")
            .bind(&now).bind(stale_before.to_rfc3339()).execute(&mut *tx).await?;
        for backup_id in stale_backups {
            sqlx::query("UPDATE backups SET status = 'queued', error = NULL WHERE id = ? AND status != 'complete'")
                .bind(backup_id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(result.rows_affected())
    }

    pub async fn complete_backup(
        &self,
        record: &BackupRecord,
        chunks: &[ChunkManifest],
        manifest: &BackupManifest,
    ) -> Result<(), CatalogError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO backups (id, name, status, created_at, completed_at, original_size, stored_size, checksum, tags_json, manifest_json, filename) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&record.id).bind(&record.name).bind(record.status.as_str()).bind(record.created_at.to_rfc3339())
            .bind(record.completed_at.map(|value| value.to_rfc3339())).bind(to_i64(record.original_size, "original_size")?)
            .bind(to_i64(record.stored_size, "stored_size")?).bind(&record.checksum)
            .bind(serde_json::to_string(&record.tags).map_err(|e| CatalogError::InvalidRecord(e.to_string()))?)
            .bind(serde_json::to_string(manifest).map_err(|e| CatalogError::InvalidRecord(e.to_string()))?)
            .bind(&record.filename)
            .execute(&mut *tx).await?;
        for chunk in chunks {
            sqlx::query("INSERT OR IGNORE INTO blobs (id, stored_hash, stored_size, created_at) VALUES (?, ?, ?, ?)")
                .bind(&chunk.stored_hash).bind(&chunk.stored_hash).bind(to_i64(chunk.stored_size, "stored_size")?)
                .bind(record.created_at.to_rfc3339()).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO artifact_blobs (backup_id, sequence, blob_id, plaintext_hash, plaintext_size) VALUES (?, ?, ?, ?, ?)")
                .bind(&record.id).bind(to_i64(chunk.sequence, "sequence")?).bind(&chunk.blob_id).bind(&chunk.plaintext_hash)
                .bind(to_i64(chunk.plaintext_size, "plaintext_size")?).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn list_backups(&self) -> Result<Vec<BackupRecord>, CatalogError> {
        let rows = sqlx::query_as::<_, BackupRow>("SELECT id, name, status, created_at, completed_at, original_size, stored_size, checksum, tags_json, filename FROM backups ORDER BY created_at DESC").fetch_all(&self.pool).await?;
        rows.into_iter().map(BackupRecord::try_from).collect()
    }

    pub async fn get_backup(&self, id: &str) -> Result<BackupRecord, CatalogError> {
        sqlx::query_as::<_, BackupRow>("SELECT id, name, status, created_at, completed_at, original_size, stored_size, checksum, tags_json, filename FROM backups WHERE id = ?")
            .bind(id).fetch_optional(&self.pool).await?.ok_or(CatalogError::NotFound)?.try_into()
    }

    pub async fn get_manifest(&self, id: &str) -> Result<BackupManifest, CatalogError> {
        let value: Option<String> =
            sqlx::query_scalar("SELECT manifest_json FROM backups WHERE id = ?")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?
                .ok_or(CatalogError::NotFound)?;
        serde_json::from_str(value.as_deref().ok_or(CatalogError::NotFound)?)
            .map_err(|e| CatalogError::InvalidRecord(e.to_string()))
    }

    pub async fn delete_backup(&self, id: &str) -> Result<(), CatalogError> {
        let mut tx = self.pool.begin().await?;
        let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM backups WHERE id = ?")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
        if exists == 0 {
            return Err(CatalogError::NotFound);
        }
        let stored_blobs: Vec<String> = sqlx::query_scalar("SELECT blobs.id FROM artifact_blobs JOIN blobs ON blobs.id = artifact_blobs.blob_id WHERE artifact_blobs.backup_id = ?")
            .bind(id).fetch_all(&mut *tx).await?;
        sqlx::query("DELETE FROM backups WHERE id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        for blob_id in stored_blobs {
            let references: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM artifact_blobs WHERE blob_id = ?")
                    .bind(&blob_id)
                    .fetch_one(&self.pool)
                    .await?;
            if references == 0 {
                let locations: Vec<(String, String)> = sqlx::query_as(
                    "SELECT transport, remote_locator FROM blob_locations WHERE blob_id = ?",
                )
                .bind(&blob_id)
                .fetch_all(&self.pool)
                .await?;
                let no_locations = locations.is_empty();
                for (transport, locator) in locations {
                    sqlx::query("INSERT OR IGNORE INTO gc_queue (blob_id, transport, remote_locator, status, available_at, created_at) VALUES (?, ?, ?, 'queued', ?, ?)")
                        .bind(&blob_id).bind(transport).bind(locator).bind(chrono::Utc::now().to_rfc3339()).bind(chrono::Utc::now().to_rfc3339())
                        .execute(&self.pool).await?;
                }
                if no_locations {
                    sqlx::query("DELETE FROM blobs WHERE id = ? AND NOT EXISTS (SELECT 1 FROM artifact_blobs WHERE artifact_blobs.blob_id = blobs.id)")
                        .bind(blob_id).execute(&self.pool).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn garbage_collect_blob(
        &self,
        blob_id: &str,
    ) -> Result<Option<String>, CatalogError> {
        let references: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM artifact_blobs WHERE blob_id = ?")
                .bind(blob_id)
                .fetch_one(&self.pool)
                .await?;
        if references > 0 {
            return Ok(None);
        }
        self.blob_locator(blob_id).await
    }

    pub async fn blob_is_referenced(&self, blob_id: &str) -> Result<bool, CatalogError> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM artifact_blobs JOIN blobs ON blobs.id = artifact_blobs.blob_id WHERE blobs.id = ? OR blobs.stored_hash = ?")
                .bind(blob_id).bind(blob_id)
                .fetch_one(&self.pool)
                .await?;
        Ok(count > 0)
    }

    pub async fn get_blob_locator(&self, blob_id: &str) -> Result<String, CatalogError> {
        Ok(sqlx::query_scalar("SELECT remote_locator FROM blob_locations WHERE blob_id = (SELECT id FROM blobs WHERE id = ? OR stored_hash = ? LIMIT 1) ORDER BY verified_at DESC LIMIT 1")
            .bind(blob_id).bind(blob_id).fetch_optional(&self.pool).await?.ok_or(CatalogError::NotFound)?)
    }

    pub async fn create_queued_backup(&self, record: &BackupRecord) -> Result<(), CatalogError> {
        sqlx::query("INSERT INTO backups (id, name, status, created_at, original_size, stored_size, checksum, tags_json, filename, source_path) VALUES (?, ?, 'queued', ?, 0, 0, '', ?, ?, ?)")
            .bind(&record.id).bind(&record.name).bind(record.created_at.to_rfc3339())
            .bind(serde_json::to_string(&record.tags).map_err(|error| CatalogError::InvalidRecord(error.to_string()))?)
            .bind(&record.filename).bind(record.source_path.as_deref()).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn complete_backup_job(
        &self,
        record: &BackupRecord,
        chunks: &[ChunkManifest],
        manifest: &BackupManifest,
    ) -> Result<(), CatalogError> {
        let mut tx = self.pool.begin().await?;
        let manifest_json = serde_json::to_string(manifest)
            .map_err(|error| CatalogError::InvalidRecord(error.to_string()))?;
        sqlx::query("UPDATE backups SET status = 'complete', completed_at = ?, original_size = ?, stored_size = ?, checksum = ?, tags_json = ?, manifest_json = ?, filename = ?, source_path = NULL WHERE id = ?")
            .bind(record.completed_at.map(|value| value.to_rfc3339())).bind(to_i64(record.original_size, "original_size")?)
            .bind(to_i64(record.stored_size, "stored_size")?).bind(&record.checksum)
            .bind(serde_json::to_string(&record.tags).map_err(|error| CatalogError::InvalidRecord(error.to_string()))?)
            .bind(serde_json::to_string(manifest).map_err(|error| CatalogError::InvalidRecord(error.to_string()))?)
            .bind(&record.filename).bind(&record.id).execute(&mut *tx).await?;
        for chunk in chunks {
            let logical_id = chunk.stored_hash.clone();
            sqlx::query("INSERT OR IGNORE INTO blobs (id, stored_hash, stored_size, created_at) VALUES (?, ?, ?, ?)")
                .bind(&logical_id).bind(&chunk.stored_hash).bind(to_i64(chunk.stored_size, "stored_size")?)
                .bind(record.created_at.to_rfc3339()).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO artifact_blobs (backup_id, sequence, blob_id, plaintext_hash, plaintext_size) VALUES (?, ?, ?, ?, ?)")
            .bind(&record.id).bind(to_i64(chunk.sequence, "sequence")?).bind(&logical_id)
                .bind(&chunk.plaintext_hash).bind(to_i64(chunk.plaintext_size, "plaintext_size")?)
                .execute(&mut *tx).await?;
            let manifest_value: serde_json::Value = serde_json::from_str(&manifest_json)
                .map_err(|error| CatalogError::InvalidRecord(error.to_string()))?;
            let location = manifest_value
                .get("artifacts")
                .and_then(serde_json::Value::as_array)
                .and_then(|artifacts| {
                    artifacts
                        .iter()
                        .flat_map(|artifact| {
                            artifact
                                .get("chunks")
                                .and_then(serde_json::Value::as_array)
                                .into_iter()
                                .flatten()
                        })
                        .find(|candidate| {
                            candidate
                                .get("sequence")
                                .and_then(serde_json::Value::as_u64)
                                == Some(chunk.sequence)
                        })
                })
                .and_then(|candidate| candidate.get("blob_id"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let transport = location
                .split_once("://")
                .map(|(scheme, _)| scheme)
                .unwrap_or("filesystem");
            sqlx::query("INSERT OR REPLACE INTO blob_locations (blob_id, transport, remote_locator, status, verified_at) VALUES (?, ?, ?, 'verified', ?)")
                .bind(&logical_id).bind(transport).bind(&location).bind(chrono::Utc::now().to_rfc3339())
                .execute(&mut *tx).await?;
        }
        tx.commit().await?;
        for chunk in chunks {
            self.record_artifact_blob_location(&record.id, chunk.sequence, &chunk.blob_id)
                .await?;
        }
        Ok(())
    }

    pub async fn record_blob_location(
        &self,
        blob_id: &str,
        locator: &str,
        size: u64,
    ) -> Result<(), CatalogError> {
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query("INSERT OR IGNORE INTO blobs (id, stored_hash, stored_size, created_at) VALUES (?, ?, ?, ?)")
            .bind(blob_id).bind(blob_id).bind(to_i64(size, "stored_size")?).bind(&now).execute(&self.pool).await?;
        let transport = locator
            .split_once("://")
            .map(|(scheme, _)| scheme)
            .unwrap_or("filesystem");
        sqlx::query("INSERT OR REPLACE INTO blob_locations (blob_id, transport, remote_locator, status, verified_at) VALUES (?, ?, ?, 'verified', ?)")
            .bind(blob_id).bind(transport).bind(locator).bind(now).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn record_artifact_blob_location(
        &self,
        backup_id: &str,
        sequence: u64,
        locator: &str,
    ) -> Result<(), CatalogError> {
        let blob_id: String = sqlx::query_scalar(
            "SELECT blob_id FROM artifact_blobs WHERE backup_id = ? AND sequence = ?",
        )
        .bind(backup_id)
        .bind(to_i64(sequence, "sequence")?)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(CatalogError::NotFound)?;
        let transport = locator
            .split_once("://")
            .map(|(scheme, _)| scheme)
            .unwrap_or("filesystem");
        sqlx::query("INSERT OR REPLACE INTO blob_locations (blob_id, transport, remote_locator, status, verified_at) VALUES (?, ?, ?, 'verified', ?)")
            .bind(blob_id).bind(transport).bind(locator).bind(chrono::Utc::now().to_rfc3339()).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn blob_locator(&self, blob_id: &str) -> Result<Option<String>, CatalogError> {
        Ok(sqlx::query_scalar("SELECT remote_locator FROM blob_locations WHERE blob_id = ? ORDER BY verified_at DESC LIMIT 1")
            .bind(blob_id).fetch_optional(&self.pool).await?)
    }

    pub async fn remove_blob_location(
        &self,
        blob_id: &str,
        locator: &str,
    ) -> Result<(), CatalogError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM blob_locations WHERE blob_id = ? AND remote_locator = ?")
            .bind(blob_id)
            .bind(locator)
            .execute(&mut *tx)
            .await?;
        let remaining: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM blob_locations WHERE blob_id = ?")
                .bind(blob_id)
                .fetch_one(&mut *tx)
                .await?;
        let references: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM artifact_blobs WHERE blob_id = ?")
                .bind(blob_id)
                .fetch_one(&mut *tx)
                .await?;
        if remaining == 0 && references == 0 {
            sqlx::query("DELETE FROM blobs WHERE id = ?")
                .bind(blob_id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn delete_blob_if_unreferenced(&self, blob_id: &str) -> Result<bool, CatalogError> {
        let mut tx = self.pool.begin().await?;
        let references: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM artifact_blobs WHERE blob_id = ?")
                .bind(blob_id)
                .fetch_one(&mut *tx)
                .await?;
        if references > 0 {
            tx.commit().await?;
            return Ok(false);
        }
        sqlx::query("DELETE FROM blobs WHERE id = ?")
            .bind(blob_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(true)
    }

    pub async fn set_backup_status(
        &self,
        id: &str,
        status: &str,
        error: Option<&str>,
    ) -> Result<(), CatalogError> {
        let result = sqlx::query("UPDATE backups SET status = ?, error = ? WHERE id = ?")
            .bind(status)
            .bind(error)
            .bind(id)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(CatalogError::NotFound);
        }
        Ok(())
    }

    pub async fn ready(&self) -> Result<(), CatalogError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }
}

fn to_i64(value: u64, field: &str) -> Result<i64, CatalogError> {
    i64::try_from(value)
        .map_err(|_| CatalogError::InvalidRecord(format!("{field} exceeds SQLite integer range")))
}

#[derive(sqlx::FromRow)]
struct BackupRow {
    id: String,
    name: String,
    status: String,
    created_at: String,
    completed_at: Option<String>,
    original_size: i64,
    stored_size: i64,
    checksum: String,
    tags_json: String,
    filename: String,
}

#[derive(sqlx::FromRow)]
struct JobRow {
    id: String,
    job_type: String,
    payload_json: String,
    attempt: i64,
}

#[derive(Clone, Debug)]
pub struct JobRecord {
    pub id: String,
    pub job_type: String,
    pub payload_json: String,
    pub attempt: i64,
}

#[derive(Clone, Debug, sqlx::FromRow, serde::Serialize)]
pub struct JobView {
    pub id: String,
    pub job_type: String,
    pub status: String,
    pub attempt: i64,
    pub error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, sqlx::FromRow)]
pub struct GcItem {
    pub id: i64,
    pub blob_id: String,
    pub transport: String,
    pub remote_locator: String,
    pub attempt: i64,
}

impl TryFrom<BackupRow> for BackupRecord {
    type Error = CatalogError;
    fn try_from(row: BackupRow) -> Result<Self, Self::Error> {
        let status = match row.status.as_str() {
            "processing" => BackupStatus::Processing,
            "complete" => BackupStatus::Complete,
            "failed" => BackupStatus::Failed,
            other => {
                return Err(CatalogError::InvalidRecord(format!(
                    "unknown status {other}"
                )))
            }
        };
        Ok(Self {
            id: row.id,
            name: row.name,
            status,
            created_at: chrono::DateTime::parse_from_rfc3339(&row.created_at)
                .map_err(|e| CatalogError::InvalidRecord(e.to_string()))?
                .with_timezone(&chrono::Utc),
            completed_at: row
                .completed_at
                .map(|value| {
                    chrono::DateTime::parse_from_rfc3339(&value)
                        .map(|dt| dt.with_timezone(&chrono::Utc))
                })
                .transpose()
                .map_err(|e| CatalogError::InvalidRecord(e.to_string()))?,
            original_size: u64::try_from(row.original_size)
                .map_err(|e| CatalogError::InvalidRecord(e.to_string()))?,
            stored_size: u64::try_from(row.stored_size)
                .map_err(|e| CatalogError::InvalidRecord(e.to_string()))?,
            checksum: row.checksum,
            tags: serde_json::from_str(&row.tags_json)
                .map_err(|e| CatalogError::InvalidRecord(e.to_string()))?,
            source_path: None,
            filename: row.filename,
        })
    }
}
