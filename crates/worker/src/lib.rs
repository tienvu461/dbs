use anyhow::{anyhow, Context, Result};
use backupd_catalog::Catalog;
use backupd_core::{ArtifactManifest, BackupManifest, BackupStatus};
use backupd_pipeline::Pipeline;
use backupd_transport::tiktok::{embed_payload_png, TikTokBusinessUploader};
use backupd_transport::{BlobTransport, BlobUpload};
use serde::Deserialize;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

pub struct Worker<T: ?Sized> {
    catalog: Catalog,
    transport: Arc<T>,
    pipeline: Arc<Pipeline>,
    concurrency: usize,
    max_attempts: i64,
    tiktok: Option<Arc<TikTokBusinessUploader>>,
    tiktok_origin: Option<String>,
    pipeline_keyed: bool,
}

impl<T: ?Sized> Clone for Worker<T> {
    fn clone(&self) -> Self {
        Self {
            catalog: self.catalog.clone(),
            transport: Arc::clone(&self.transport),
            pipeline: Arc::clone(&self.pipeline),
            concurrency: self.concurrency,
            max_attempts: self.max_attempts,
            tiktok: self.tiktok.clone(),
            tiktok_origin: self.tiktok_origin.clone(),
            pipeline_keyed: self.pipeline_keyed,
        }
    }
}

#[derive(Deserialize)]
struct BackupJob {
    backup_id: String,
    source_path: String,
    filename: String,
}

impl Worker<dyn BlobTransport> {
    pub fn new(
        catalog: Catalog,
        transport: Arc<dyn BlobTransport>,
        pipeline: Arc<Pipeline>,
        concurrency: usize,
        max_attempts: i64,
    ) -> Self {
        Self {
            catalog,
            transport,
            pipeline,
            concurrency: concurrency.max(1),
            max_attempts: max_attempts.max(1),
            tiktok: None,
            tiktok_origin: None,
            pipeline_keyed: false,
        }
    }

    pub fn with_tiktok(
        mut self,
        uploader: TikTokBusinessUploader,
        origin: String,
        pipeline_keyed: bool,
    ) -> Self {
        self.tiktok = Some(Arc::new(uploader));
        self.tiktok_origin = Some(origin);
        self.pipeline_keyed = pipeline_keyed;
        self
    }

    pub async fn run(self) -> Result<()> {
        let worker_id = ulid::Ulid::new().to_string();
        self.catalog
            .recover_stale_jobs(chrono::Utc::now() - chrono::Duration::minutes(10))
            .await?;
        let mut workers = tokio::task::JoinSet::new();
        let mut last_recovery = tokio::time::Instant::now();
        loop {
            if last_recovery.elapsed() >= Duration::from_secs(60) {
                self.catalog
                    .recover_stale_jobs(chrono::Utc::now() - chrono::Duration::minutes(10))
                    .await?;
                last_recovery = tokio::time::Instant::now();
            }
            while workers.len() < self.concurrency {
                match self.catalog.claim_job(&worker_id).await? {
                    Some(job) => {
                        let worker = self.clone();
                        workers.spawn(async move { worker.execute(job).await });
                    }
                    None => break,
                }
            }
            if let Some(gc) = self.catalog.claim_gc_item().await? {
                let transport = Arc::clone(&self.transport);
                let catalog = self.catalog.clone();
                tokio::spawn(async move {
                    match transport.delete(&gc.remote_locator).await {
                        Ok(()) => {
                            let _ = catalog
                                .remove_blob_location(&gc.blob_id, &gc.remote_locator)
                                .await;
                            let _ = catalog.delete_blob_if_unreferenced(&gc.blob_id).await;
                            let _ = catalog.finish_gc_item(gc.id).await;
                        }
                        Err(error) => {
                            let _ = catalog.retry_gc_item(gc.id, &error.to_string()).await;
                        }
                    }
                });
            }
            if workers.is_empty() {
                tokio::time::sleep(Duration::from_millis(500)).await;
            } else {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(250)), if workers.len() < self.concurrency => {},
                    result = workers.join_next() => {
                        match result {
                            Some(Err(error)) => tracing::error!(%error, "backup worker task panicked"),
                            Some(Ok(Err(error))) => tracing::error!(%error, "backup worker execution failed"),
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    async fn execute(&self, job: backupd_catalog::JobRecord) -> Result<()> {
        let payload = if job.job_type == "ingest_file" {
            Some(
                serde_json::from_str::<BackupJob>(&job.payload_json)
                    .context("decode backup job")?,
            )
        } else {
            None
        };
        let result = match payload {
            Some(payload) => self.process_backup(payload).await,
            None => Err(anyhow!("unknown durable job type {}", job.job_type)),
        };
        match result {
            Ok(()) => self.catalog.finish_job(&job.id).await.map_err(Into::into),
            Err(error) => {
                let message = error.to_string();
                if let Ok(payload) = serde_json::from_str::<BackupJob>(&job.payload_json) {
                    let status = if job.attempt >= self.max_attempts {
                        "failed"
                    } else {
                        "queued"
                    };
                    let _ = self
                        .catalog
                        .set_backup_status(&payload.backup_id, status, Some(&message))
                        .await;
                }
                let delay = (1i64 << job.attempt.min(8)).min(300);
                self.catalog
                    .retry_job(&job.id, &message, delay, self.max_attempts)
                    .await?;
                Err(error)
            }
        }
    }

    async fn process_backup(&self, job: BackupJob) -> Result<()> {
        let mut file = tokio::fs::File::open(&job.source_path)
            .await
            .context("open staged upload")?;
        let mut record = self.catalog.get_backup(&job.backup_id).await?;
        if record.status == BackupStatus::Complete {
            let _ = tokio::fs::remove_file(&job.source_path).await;
            return Ok(());
        }
        record.status = BackupStatus::Processing;
        record.filename = job.filename.clone();
        self.catalog
            .set_backup_status(&record.id, "processing", None)
            .await?;
        let chunks = Arc::new(Mutex::new(Vec::new()));
        let chunk_sink = Arc::clone(&chunks);
        let (size, checksum) = self
            .pipeline
            .process(&mut file, |chunk| {
                let transport = Arc::clone(&self.transport);
                let chunk_sink = Arc::clone(&chunk_sink);
                async move {
                    let object_id = format!(
                        "chunks/{}",
                        chunk
                            .manifest
                            .blob_id
                            .strip_prefix("blake3:")
                            .unwrap_or(&chunk.manifest.blob_id)
                    );
                    transport
                        .put(BlobUpload {
                            id: object_id.clone(),
                            logical_id: chunk.manifest.blob_id.clone(),
                            bytes: chunk.bytes.into(),
                            stored_hash: chunk.manifest.stored_hash.clone(),
                        })
                        .await?;
                    let mut manifest = chunk.manifest;
                    let locator = transport.locator_for(&object_id)?;
                    let _ = transport.head(&locator).await?;
                    manifest.blob_id = locator;
                    let _ = transport.head(&manifest.blob_id).await?;
                    chunk_sink
                        .lock()
                        .expect("chunk list mutex poisoned")
                        .push(manifest);
                    Ok(())
                }
            })
            .await?;
        let mut chunks = Arc::try_unwrap(chunks)
            .map_err(|_| anyhow!("chunk processing did not finish"))?
            .into_inner()
            .map_err(|_| anyhow!("chunk list mutex poisoned"))?;
        record.status = BackupStatus::Complete;
        record.completed_at = Some(chrono::Utc::now());
        record.original_size = size;
        record.stored_size = chunks
            .iter()
            .map(|chunk: &backupd_core::ChunkManifest| chunk.stored_size)
            .sum();
        record.checksum = checksum;
        record.filename = job.filename.clone();
        let mut tiktok_urls = Vec::new();
        let mut encryption_metadata = Vec::new();
        if let Some(uploader) = &self.tiktok {
            if !self.pipeline_keyed {
                return Err(anyhow!("TikTok public CDN requires BACKUP_MASTER_KEY to be set; refusing to publish plaintext chunks"));
            }
            let origin = self.tiktok_origin.as_deref().unwrap_or_default();
            for chunk in &mut chunks {
                let blob_locator = self.catalog.get_blob_locator(&chunk.stored_hash).await?;
                let raw_locator = self.transport.raw_locator(&blob_locator)?;
                let raw = self.transport.get(&raw_locator).await?;
                if raw.len() < 40 {
                    return Err(anyhow!("encrypted stored chunk is truncated"));
                }
                let cdn_payload = self.pipeline.rewrap_cdn_ciphertext(chunk, &raw)?;
                let nonce = cdn_payload[..24].to_vec();
                let png = embed_payload_png(&cdn_payload, &chunk.blob_id);
                let temp = std::env::temp_dir().join(format!("{}.png", ulid::Ulid::new()));
                tokio::fs::write(&temp, &png).await?;
                let url = uploader
                    .upload_path(&format!("{}.png", chunk.sequence), &temp)
                    .await?;
                let _ = tokio::fs::remove_file(temp).await;
                let cdn_url = if url.starts_with("http") {
                    url
                } else {
                    format!(
                        "{}{}",
                        origin.trim_end_matches('/'),
                        url.trim_start_matches('/')
                    )
                };
                chunk.envelope = Some("tiktok-png-itxt-v1".into());
                chunk.cdn_url = Some(cdn_url.clone());
                chunk.cdn_encrypted = Some(true);
                tiktok_urls.push(cdn_url);
                encryption_metadata.push(nonce);
            }
        }
        let mut artifacts = vec![ArtifactManifest {
            filename: job.filename,
            size,
            chunks: chunks.clone(),
            content: None,
        }];
        if !tiktok_urls.is_empty() {
            let playlist = serde_json::to_string(&serde_json::json!({
                "format": "backupd-tiktok-png-v1",
                "backup_id": record.id,
                "algorithm": "xchacha20-poly1305+zstd",
                "key_required": true,
                "chunks": tiktok_urls.iter().enumerate().map(|(sequence, url)| serde_json::json!({ "sequence": sequence, "url": url, "encoding": "png-itxt-base64", "encrypted": true, "compression": "zstd", "encryption": "xchacha20-poly1305", "nonce": encryption_metadata.get(sequence).map(|nonce| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, nonce)) })).collect::<Vec<_>>()
            }))?;
            artifacts.push(ArtifactManifest {
                filename: format!("{}.tiktok.json", record.name),
                size: playlist.len() as u64,
                chunks: Vec::new(),
                content: Some(playlist),
            });
            record.tags.insert("cdn.tiktok".into(), "enabled".into());
        }
        let manifest = BackupManifest {
            format: "backupd-v1".into(),
            backup: record.clone(),
            artifacts,
        };
        self.catalog
            .complete_backup_job(&record, &chunks, &manifest)
            .await?;
        for chunk in &chunks {
            self.catalog
                .record_artifact_blob_location(&record.id, chunk.sequence, &chunk.blob_id)
                .await?;
        }
        tokio::fs::remove_file(PathBuf::from(job.source_path))
            .await
            .context("remove staged upload after durable completion")?;
        Ok(())
    }
}
