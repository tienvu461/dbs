use anyhow::{anyhow, Context, Result};
use axum::{
    body::Body,
    extract::{multipart::MultipartError, Multipart, Path, Request, State},
    http::{header, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use backupd_catalog::{Catalog, CatalogError};
use backupd_core::{BackupRecord, BackupStatus};
use backupd_pipeline::Pipeline;
use backupd_transport::tiktok::TikTokBusinessUploader;
use backupd_transport::{BlobTransport, FilesystemTransport, S3CompatibleTransport};
use backupd_worker::Worker;
use serde::Deserialize;
use std::{collections::BTreeMap, env, net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::io::AsyncWriteExt;

async fn authenticate(
    State(token): State<Option<Arc<String>>>,
    request: Request,
    next: Next,
) -> Response {
    if request.uri().path().starts_with("/v1/") {
        if let Some(token) = token {
            let provided = request
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "));
            // Compare fixed-size digests without an early exit on differing bytes.
            let valid = provided
                .map(|provided| {
                    let expected = blake3::hash(token.as_bytes());
                    let actual = blake3::hash(provided.as_bytes());
                    use subtle::ConstantTimeEq;
                    bool::from(expected.as_bytes().ct_eq(actual.as_bytes()))
                })
                .unwrap_or(false);
            if !valid {
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(serde_json::json!({"error": "authentication required"})),
                )
                    .into_response();
            }
        }
    }
    next.run(request).await
}

#[derive(Clone)]
struct AppState {
    catalog: Catalog,
    transport: Arc<dyn BlobTransport>,
    pipeline: Arc<Pipeline>,
    max_upload_bytes: u64,
}

struct ApiError {
    status: StatusCode,
    message: String,
}
impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let message = if self.status.is_server_error() {
            "internal service or storage error".to_owned()
        } else {
            self.message
        };
        (self.status, Json(serde_json::json!({"error": message}))).into_response()
    }
}
impl From<CatalogError> for ApiError {
    fn from(error: CatalogError) -> Self {
        match error {
            CatalogError::NotFound => Self::new(StatusCode::NOT_FOUND, "backup not found"),
            other => Self::new(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        }
    }
}
impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
    }
}
impl From<std::io::Error> for ApiError {
    fn from(error: std::io::Error) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
    }
}
impl From<reqwest::Error> for ApiError {
    fn from(error: reqwest::Error) -> Self {
        Self::new(StatusCode::BAD_GATEWAY, error.to_string())
    }
}

#[derive(Deserialize, Default)]
struct UploadTags(BTreeMap<String, String>);

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "backupd=info".into()),
        )
        .init();
    let bind: SocketAddr = env::var("BACKUPD_BIND")
        .unwrap_or_else(|_| "127.0.0.1:8080".into())
        .parse()?;
    let api_token = env::var("BACKUPD_API_TOKEN").ok().map(Arc::new);
    if let Some(token) = &api_token {
        if token.len() < 32 {
            anyhow::bail!("BACKUPD_API_TOKEN must contain at least 32 characters");
        }
    }
    if !bind.ip().is_loopback() && api_token.is_none() {
        anyhow::bail!("BACKUPD_API_TOKEN is required when listening on a non-loopback address");
    }
    let database_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://data/backup.db".into());
    prepare_sqlite_parent(&database_url).await?;
    let catalog = Catalog::connect(&database_url).await?;
    let storage_root =
        PathBuf::from(env::var("BACKUPD_STORAGE_ROOT").unwrap_or_else(|_| "data/blobs".into()));
    tokio::fs::create_dir_all(&storage_root).await?;
    let is_s3 = env::var("BACKUPD_TRANSPORT").unwrap_or_else(|_| "filesystem".into()) == "s3";
    let transport: Arc<dyn BlobTransport> = if is_s3 {
        let endpoint =
            env::var("S3_ENDPOINT_URL").context("S3_ENDPOINT_URL is required for S3 transport")?;
        let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
        let bucket = env::var("S3_BUCKET").context("S3_BUCKET is required for S3 transport")?;
        let prefix = env::var("S3_PREFIX").unwrap_or_else(|_| "backupd".into());
        let access_key = env::var("AWS_ACCESS_KEY_ID").ok();
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").ok();
        Arc::new(
            S3CompatibleTransport::new(endpoint, region, bucket, prefix, access_key, secret_key)
                .await?,
        )
    } else {
        Arc::new(FilesystemTransport::new(storage_root).await?)
    };
    let key = env::var("BACKUP_MASTER_KEY")
        .ok()
        .map(parse_key)
        .transpose()?;
    let encryption_enabled = key.is_some();
    if env::var("TIKTOK_ORG_ID").is_ok() && env::var("TIKTOK_COOKIE").is_ok() && !encryption_enabled
    {
        anyhow::bail!("TikTok publishing requires BACKUP_MASTER_KEY to protect public CDN content");
    }
    let chunk_size = env::var("BACKUPD_CHUNK_SIZE")
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(64 * 1024 * 1024);
    let pipeline = Pipeline::new(chunk_size, 6, key)?;
    let max_upload_bytes = env::var("BACKUPD_MAX_UPLOAD_BYTES")
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(u64::MAX);
    let state = AppState {
        catalog,
        transport: Arc::clone(&transport),
        pipeline: Arc::new(pipeline),
        max_upload_bytes,
    };
    let worker_transport: Arc<dyn BlobTransport> = if is_s3 {
        let endpoint =
            env::var("S3_ENDPOINT_URL").context("S3_ENDPOINT_URL is required for S3 transport")?;
        let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
        let bucket = env::var("S3_BUCKET").context("S3_BUCKET is required for S3 transport")?;
        let prefix = env::var("S3_PREFIX").unwrap_or_else(|_| "backupd".into());
        let access_key = env::var("AWS_ACCESS_KEY_ID").ok();
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").ok();
        Arc::new(
            S3CompatibleTransport::new(endpoint, region, bucket, prefix, access_key, secret_key)
                .await?,
        )
    } else {
        Arc::new(
            FilesystemTransport::new(PathBuf::from(
                env::var("BACKUPD_STORAGE_ROOT").unwrap_or_else(|_| "data/blobs".into()),
            ))
            .await?,
        )
    };
    let mut worker = Worker::new(
        state.catalog.clone(),
        worker_transport,
        Arc::clone(&state.pipeline),
        4,
        5,
    );
    if let (Ok(org_id), Ok(cookie)) = (env::var("TIKTOK_ORG_ID"), env::var("TIKTOK_COOKIE")) {
        let uploader =
            TikTokBusinessUploader::new(org_id, cookie, env::var("TIKTOK_CSRF_TOKEN").ok())?;
        let origin = env::var("TIKTOK_CDN_ORIGIN")
            .unwrap_or_else(|_| "https://p16-ad-sg.tiktokcdn.com/origin/".into());
        worker = worker.with_tiktok(uploader, origin, encryption_enabled);
    }
    tokio::spawn(async move {
        if let Err(error) = worker.run().await {
            tracing::error!(%error, "durable worker stopped");
        }
    });
    let app = Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/v1/backups/upload", axum::routing::post(upload))
        .route("/v1/backups", get(list_backups))
        .route("/v1/backups/:id", get(get_backup).delete(delete_backup))
        .route("/v1/jobs/:id", get(get_job))
        .route("/v1/backups/:id/download", get(download_backup))
        .route("/v1/backups/:id/tiktok", get(download_tiktok_index))
        .layer(middleware::from_fn_with_state(api_token, authenticate))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, "backupd listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn prepare_sqlite_parent(url: &str) -> Result<()> {
    let path = url
        .strip_prefix("sqlite://")
        .ok_or_else(|| anyhow!("only SQLite DATABASE_URL is currently supported"))?;
    if path != ":memory:" {
        if let Some(parent) = PathBuf::from(path).parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .context("create SQLite database directory")?;
        }
    }
    Ok(())
}

fn parse_key(encoded: String) -> Result<[u8; 32]> {
    let bytes = hex::decode(encoded).context("BACKUP_MASTER_KEY must be hex-encoded")?;
    bytes
        .try_into()
        .map_err(|_| anyhow!("BACKUP_MASTER_KEY must decode to exactly 32 bytes"))
}

async fn live() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({"status": "ok"})))
}
async fn ready(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    state.catalog.ready().await?;
    Ok((StatusCode::OK, Json(serde_json::json!({"status": "ready"}))))
}

async fn upload(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, ApiError> {
    let mut name = None;
    let mut tags = BTreeMap::new();
    let mut file_name = "backup.bin".to_owned();
    let mut file_stream = None;
    let mut field_count = 0usize;
    while let Some(mut field) = multipart.next_field().await.map_err(multipart_error)? {
        field_count += 1;
        if field_count > 16 {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "too many multipart fields",
            ));
        }
        match field.name().unwrap_or_default() {
            "tags" => {
                let raw = field.text().await.map_err(multipart_error)?;
                tags = serde_json::from_str::<UploadTags>(&raw)
                    .map_err(|error| {
                        ApiError::new(
                            StatusCode::BAD_REQUEST,
                            format!("tags must be a string map: {error}"),
                        )
                    })?
                    .0;
            }
            "name" => name = Some(field.text().await.map_err(multipart_error)?),
            "file" => {
                file_name = field.file_name().unwrap_or("backup.bin").to_owned();
                let staging_root = PathBuf::from(
                    env::var("BACKUPD_STAGING_ROOT").unwrap_or_else(|_| "data/staging".into()),
                );
                tokio::fs::create_dir_all(&staging_root)
                    .await
                    .map_err(ApiError::from)?;
                let staged = staging_root.join(format!("{}.upload", ulid::Ulid::new()));
                let mut output = tokio::fs::File::create(&staged)
                    .await
                    .map_err(ApiError::from)?;
                let mut size = 0u64;
                while let Some(bytes) = field.chunk().await.map_err(multipart_error)? {
                    size = size.saturating_add(bytes.len() as u64);
                    if size > state.max_upload_bytes {
                        return Err(ApiError::new(
                            StatusCode::PAYLOAD_TOO_LARGE,
                            "upload exceeds configured size limit",
                        ));
                    }
                    output.write_all(&bytes).await.map_err(ApiError::from)?;
                }
                output.flush().await.map_err(ApiError::from)?;
                file_stream = Some((staged, size));
                break;
            }
            _ => {}
        }
    }
    let (staged_path, _size) = file_stream.ok_or_else(|| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "multipart field 'file' is required",
        )
    })?;
    let mut record = BackupRecord::new(
        name.filter(|value| !value.is_empty())
            .unwrap_or_else(|| file_name.clone()),
        tags,
    );
    record.status = BackupStatus::Processing;
    record.filename = file_name;
    record.source_path = Some(staged_path.to_string_lossy().to_string());
    state.catalog.create_queued_backup(&record).await?;
    let job_id = ulid::Ulid::new().to_string();
    let payload = serde_json::json!({ "backup_id": record.id, "source_path": staged_path, "filename": record.filename });
    state
        .catalog
        .enqueue_job(&job_id, "ingest_file", &payload.to_string())
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "id": record.id, "job_id": job_id, "status": "queued" })),
    ))
}

fn multipart_error(error: MultipartError) -> ApiError {
    ApiError::new(StatusCode::BAD_REQUEST, error.to_string())
}
async fn list_backups(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.catalog.list_backups().await?))
}
async fn get_backup(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.catalog.get_backup(&id).await?))
}
async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .catalog
        .get_job(&id)
        .await
        .map(Json)
        .map_err(ApiError::from)
}
async fn delete_backup(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let manifest = state.catalog.get_manifest(&id).await?;
    let mut manifest = manifest;
    if let Some(cdn_index) = manifest
        .artifacts
        .iter()
        .find(|artifact| artifact.filename.ends_with(".tiktok.json"))
    {
        let index: serde_json::Value =
            serde_json::from_str(cdn_index.content.as_deref().unwrap_or("{}"))
                .map_err(|error| ApiError::new(StatusCode::BAD_GATEWAY, error.to_string()))?;
        if let Some(items) = index.get("chunks").and_then(serde_json::Value::as_array) {
            for item in items {
                let sequence = item
                    .get("sequence")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(u64::MAX);
                if let Some(chunk) = manifest
                    .artifacts
                    .iter_mut()
                    .flat_map(|artifact| artifact.chunks.iter_mut())
                    .find(|chunk| chunk.sequence == sequence)
                {
                    chunk.cdn_url = item
                        .get("url")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned);
                }
            }
        }
    }
    state.catalog.delete_backup(&id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn download_backup(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let manifest = state.catalog.get_manifest(&id).await?;
    let artifact = manifest
        .artifacts
        .first()
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "backup has no artifact"))?;
    let mut output = Vec::new();
    if let Some(content) = &artifact.content {
        output.extend_from_slice(content.as_bytes());
    } else {
        let mut full_hasher = blake3::Hasher::new();
        let mut total_size = 0u64;
        for chunk in &artifact.chunks {
            let plaintext = if let Some(url) = &chunk.cdn_url {
                let png = backupd_transport::tiktok::download_public_png(url)
                    .await
                    .map_err(|_| ApiError::new(StatusCode::BAD_GATEWAY, "CDN download failed"))?;
                let payload = backupd_transport::tiktok::extract_payload_png(&png)
                    .map_err(|error| ApiError::new(StatusCode::BAD_GATEWAY, error.to_string()))?;
                state
                    .pipeline
                    .decode_cdn_chunk(chunk, &payload)
                    .map_err(|error| ApiError::new(StatusCode::BAD_GATEWAY, error.to_string()))?
            } else {
                let locator = state.catalog.get_blob_locator(&chunk.stored_hash).await?;
                let raw_locator = state
                    .transport
                    .raw_locator(&locator)
                    .map_err(|error| ApiError::new(StatusCode::BAD_GATEWAY, error.to_string()))?;
                let bytes =
                    state.transport.get(&raw_locator).await.map_err(|error| {
                        ApiError::new(StatusCode::BAD_GATEWAY, error.to_string())
                    })?;
                state
                    .pipeline
                    .decode(chunk, &bytes)
                    .map_err(|error| ApiError::new(StatusCode::BAD_GATEWAY, error.to_string()))?
            };
            full_hasher.update(&plaintext);
            total_size += plaintext.len() as u64;
            output.extend_from_slice(&plaintext);
        }
        let checksum = format!("blake3:{}", full_hasher.finalize().to_hex());
        if checksum != manifest.backup.checksum || total_size != artifact.size {
            return Err(ApiError::new(
                StatusCode::BAD_GATEWAY,
                "artifact integrity check failed",
            ));
        }
    }
    let mut response = Response::new(Body::from(output));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(if artifact.filename.ends_with(".m3u8") {
            "application/vnd.apple.mpegurl"
        } else if artifact.filename.ends_with(".json") {
            "application/json"
        } else {
            "application/octet-stream"
        }),
    );
    let disposition = format!(
        "attachment; filename=\"{}\"",
        sanitize_filename(&artifact.filename)
    );
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&disposition)
            .unwrap_or_else(|_| HeaderValue::from_static("attachment")),
    );
    Ok(response)
}

async fn download_tiktok_index(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let manifest = state.catalog.get_manifest(&id).await?;
    let artifact = manifest
        .artifacts
        .iter()
        .find(|artifact| artifact.filename.ends_with(".tiktok.json"))
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "TikTok CDN index is unavailable"))?;
    let body = artifact.content.clone().ok_or_else(|| {
        ApiError::new(
            StatusCode::NOT_FOUND,
            "TikTok CDN playlist content is missing",
        )
    })?;
    let mut response = Response::new(Body::from(body));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    Ok(response)
}

fn sanitize_filename(name: &str) -> String {
    name.chars()
        .filter(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_')
        })
        .collect()
}

#[cfg(test)]
mod security_tests {
    use super::*;

    #[tokio::test]
    async fn api_requires_token_while_health_remains_available() {
        let token = Arc::new(format!("{}{}", ulid::Ulid::new(), ulid::Ulid::new()));
        let app = Router::new()
            .route("/v1/backups", get(|| async { StatusCode::OK }))
            .route("/health/live", get(live))
            .layer(middleware::from_fn_with_state(
                Some(token.clone()),
                authenticate,
            ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = reqwest::Client::new();
        let endpoint = format!("http://{address}/v1/backups");
        assert_eq!(
            client.get(&endpoint).send().await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            client
                .get(&endpoint)
                .bearer_auth("invalid")
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            client
                .get(&endpoint)
                .bearer_auth(token.as_str())
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            client
                .get(format!("http://{address}/health/live"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        server.abort();
    }
}
