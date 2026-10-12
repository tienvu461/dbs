use super::UploadEvent;
use async_trait::async_trait;
use backupd_lib::{BlobStore, Error as BackupError, Manifest, Uploader};
use futures::{StreamExt, TryStreamExt};
use worker::*;

struct R2Store {
    bucket: Bucket,
    name: String,
}
#[async_trait(?Send)]
impl BlobStore for R2Store {
    async fn put(&self, key: &str, bytes: &[u8]) -> backupd_lib::Result<String> {
        self.bucket
            .put(key, bytes.to_vec())
            .execute()
            .await
            .map_err(|_| BackupError::Transport)?
            .ok_or(BackupError::Transport)?;
        Ok(format!("r2://{}/{key}", self.name))
    }
    async fn get(&self, locator: &str, max: usize) -> backupd_lib::Result<Vec<u8>> {
        let prefix = format!("r2://{}/", self.name);
        let key = locator.strip_prefix(&prefix).ok_or(BackupError::Invalid)?;
        if !key.starts_with("chunks/") {
            return Err(BackupError::Invalid);
        }
        let object = self
            .bucket
            .get(key)
            .execute()
            .await
            .map_err(|_| BackupError::Transport)?
            .ok_or(BackupError::Transport)?;
        if object.size() > max as u64 {
            return Err(BackupError::Limit);
        }
        read_stream(
            object
                .body()
                .ok_or(BackupError::Transport)?
                .stream()
                .map_err(|_| BackupError::Transport)?,
            max,
        )
        .await
    }
}

async fn read_stream(mut stream: ByteStream, max: usize) -> backupd_lib::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    while let Some(part) = stream.next().await {
        let part = part.map_err(|_| BackupError::Transport)?;
        if part.len() > max - bytes.len() {
            return Err(BackupError::Limit);
        }
        bytes.extend_from_slice(&part);
    }
    Ok(bytes)
}

struct TikTokStore {
    org_id: String,
    cookie: String,
    csrf: Option<String>,
    origin: Url,
}
impl TikTokStore {
    async fn api(
        &self,
        endpoint: &str,
        body: Vec<u8>,
        content_type: &str,
    ) -> backupd_lib::Result<serde_json::Value> {
        let mut url = Url::parse(endpoint).map_err(|_| BackupError::Invalid)?;
        url.query_pairs_mut().append_pair("org_id", &self.org_id);
        let headers = Headers::new();
        headers
            .set("cookie", &self.cookie)
            .map_err(|_| BackupError::Invalid)?;
        headers
            .set("content-type", content_type)
            .map_err(|_| BackupError::Invalid)?;
        headers
            .set("origin", "https://business.tiktok.com")
            .map_err(|_| BackupError::Invalid)?;
        headers
            .set(
                "referer",
                "https://business.tiktok.com/manage/material/image",
            )
            .map_err(|_| BackupError::Invalid)?;
        if let Some(csrf) = &self.csrf {
            headers
                .set("x-csrftoken", csrf)
                .map_err(|_| BackupError::Invalid)?;
        }
        let mut init = RequestInit::new();
        init.with_method(Method::Post)
            .with_headers(headers)
            .with_redirect(RequestRedirect::Manual)
            .with_body(Some(js_sys::Uint8Array::from(body.as_slice()).into()));
        let request =
            Request::new_with_init(url.as_str(), &init).map_err(|_| BackupError::Transport)?;
        let mut response = Fetch::Request(request)
            .send()
            .await
            .map_err(|_| BackupError::Transport)?;
        if response.status_code() != 200 {
            return Err(BackupError::Transport);
        }
        let bytes = read_stream(
            response.stream().map_err(|_| BackupError::Transport)?,
            1024 * 1024,
        )
        .await?;
        serde_json::from_slice(&bytes).map_err(|_| BackupError::Transport)
    }
}
#[async_trait(?Send)]
impl BlobStore for TikTokStore {
    async fn put(&self, key: &str, bytes: &[u8]) -> backupd_lib::Result<String> {
        let png = backupd_lib::png::encode(bytes)?;
        let mut random = [0; 16];
        getrandom::getrandom(&mut random).map_err(|_| BackupError::Random)?;
        let boundary = format!("backupd-{}", hex::encode(random));
        let name = format!("{}.png", key.replace('/', "-"));
        let mut body = format!("--{boundary}\r\nContent-Disposition: form-data; name=\"Filedata\"; filename=\"{name}\"\r\nContent-Type: image/png\r\n\r\n").into_bytes();
        body.extend(png);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        let uploaded = self
            .api(
                "https://business.tiktok.com/api/v3/bm/material/image/upload/",
                body,
                &format!("multipart/form-data; boundary={boundary}"),
            )
            .await?;
        let uri = uploaded
            .pointer("/data/image_info/web_uri")
            .and_then(serde_json::Value::as_str)
            .ok_or(BackupError::Transport)?;
        let url = self.origin.join(uri).map_err(|_| BackupError::Invalid)?;
        if url.scheme() != "https"
            || !url
                .host_str()
                .unwrap_or_default()
                .ends_with(".tiktokcdn.com")
        {
            return Err(BackupError::Invalid);
        }
        let registration = serde_json::to_vec(&serde_json::json!({"web_uri": uri, "original_web_uri": uri, "name": name, "show_error": false})).map_err(|_| BackupError::Invalid)?;
        let response = self
            .api(
                "https://business.tiktok.com/api/v3/bm/material/image/create/",
                registration,
                "application/json",
            )
            .await?;
        if response.get("code").and_then(serde_json::Value::as_i64) != Some(0) {
            return Err(BackupError::Transport);
        }
        Ok(url.to_string())
    }
    async fn get(&self, locator: &str, max: usize) -> backupd_lib::Result<Vec<u8>> {
        let url = Url::parse(locator).map_err(|_| BackupError::Invalid)?;
        if url.scheme() != "https"
            || !url
                .host_str()
                .unwrap_or_default()
                .ends_with(".tiktokcdn.com")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.port().is_some_and(|p| p != 443)
        {
            return Err(BackupError::Invalid);
        }
        let mut init = RequestInit::new();
        init.with_redirect(RequestRedirect::Manual);
        // This request has no Cookie/Authorization headers: verify anonymous retrieval.
        let request =
            Request::new_with_init(url.as_str(), &init).map_err(|_| BackupError::Transport)?;
        let mut response = Fetch::Request(request)
            .send()
            .await
            .map_err(|_| BackupError::Transport)?;
        if response.status_code() != 200 {
            return Err(BackupError::Transport);
        }
        let png = read_stream(
            response.stream().map_err(|_| BackupError::Transport)?,
            backupd_lib::png::MAX_PNG_BYTES,
        )
        .await?;
        let data = backupd_lib::png::decode(&png)?;
        if data.len() > max {
            return Err(BackupError::Limit);
        }
        Ok(data)
    }
}

fn config(env: &Env, name: &str) -> Result<String> {
    Ok(env.var(name)?.to_string())
}
fn failure() -> Error {
    Error::RustError("backup processing failed; source retained and message will retry".into())
}

async fn process(event: &UploadEvent, env: &Env) -> Result<()> {
    let input_name = config(env, "INPUT_BUCKET_NAME")?;
    let input_prefix = config(env, "INPUT_PREFIX")?;
    if !event.is_input(&input_name, &input_prefix) {
        return Ok(());
    }
    let max_input: u64 = config(env, "MAX_INPUT_BYTES")?
        .parse()
        .map_err(|_| failure())?;
    let size = event.object.size.ok_or_else(failure)?;
    if size > max_input {
        return Err(failure());
    }
    let etag = event.object.etag.as_ref().ok_or_else(failure)?;
    let key_id = config(env, "KEY_ID")?;
    let encoded = env.secret("BACKUP_MASTER_KEY")?.to_string();
    let key: [u8; 32] = hex::decode(encoded)
        .map_err(|_| failure())?
        .try_into()
        .map_err(|_| failure())?;
    let chunk_size: usize = config(env, "CHUNK_SIZE")?.parse().map_err(|_| failure())?;
    let engine =
        Uploader::new(key, key_id.clone(), chunk_size, max_input).map_err(|_| failure())?;
    let output = env.bucket("MANIFEST_BUCKET")?;
    if config(env, "MANIFEST_BUCKET_NAME")? == input_name {
        return Err(failure());
    }
    let manifest_key = event.manifest_key(&key_id);
    if let Some(existing) = output.get(&manifest_key).execute().await? {
        let bytes = read_stream(existing.body().ok_or_else(failure)?.stream()?, 1024 * 1024)
            .await
            .map_err(|_| failure())?;
        let manifest = Manifest::from_json(&bytes).map_err(|_| failure())?;
        engine.verify_manifest(&manifest).map_err(|_| failure())?;
        return Ok(());
    }
    let input = env.bucket("INPUT_BUCKET")?;
    let object = input
        .get(&event.object.key)
        .only_if(Conditional {
            etag_matches: Some(etag.clone()),
            ..Default::default()
        })
        .execute()
        .await?
        .ok_or_else(failure)?;
    if object.etag() != *etag || object.size() != size {
        return Err(failure());
    }
    let stream = object
        .body()
        .ok_or_else(failure)?
        .stream()?
        .map_err(|_| std::io::Error::other("R2 source read failed"));
    let reader = stream.into_async_read();
    let destination: Box<dyn BlobStore> = match config(env, "DESTINATION")?.as_str() {
        "r2" => {
            let name = config(env, "BLOB_BUCKET_NAME")?;
            if name == input_name {
                return Err(failure());
            }
            Box::new(R2Store {
                bucket: env.bucket("BLOB_BUCKET")?,
                name,
            })
        }
        "tiktok" => Box::new(TikTokStore {
            org_id: env.secret("TIKTOK_ORG_ID")?.to_string(),
            cookie: env.secret("TIKTOK_COOKIE")?.to_string(),
            csrf: env.secret("TIKTOK_CSRF_TOKEN").ok().map(|v| v.to_string()),
            origin: config(env, "TIKTOK_CDN_ORIGIN")?
                .parse()
                .map_err(|_| failure())?,
        }),
        _ => return Err(failure()),
    };
    let filename = event
        .object
        .key
        .rsplit('/')
        .next()
        .unwrap_or("backup.bin")
        .to_owned();
    let manifest = engine
        .upload(reader, filename, destination.as_ref())
        .await
        .map_err(|_| failure())?;
    if manifest.original_size != size {
        return Err(failure());
    }
    let json = manifest.to_json().map_err(|_| failure())?;
    if json.len() > 1024 * 1024 {
        return Err(failure());
    }
    output
        .put(&manifest_key, json.clone())
        .only_if(Conditional {
            etag_does_not_match: Some("*".into()),
            ..Default::default()
        })
        .http_metadata(HttpMetadata {
            content_type: Some("application/json".into()),
            ..Default::default()
        })
        .execute()
        .await?;
    // Publish then read back before acknowledging. Notification integrations may consume this object.
    let saved = output
        .get(&manifest_key)
        .execute()
        .await?
        .ok_or_else(failure)?;
    let bytes = read_stream(saved.body().ok_or_else(failure)?.stream()?, 1024 * 1024)
        .await
        .map_err(|_| failure())?;
    engine
        .verify_manifest(&Manifest::from_json(&bytes).map_err(|_| failure())?)
        .map_err(|_| failure())?;
    Ok(())
}

#[event(queue)]
pub async fn queue(batch: MessageBatch<UploadEvent>, env: Env, _ctx: Context) -> Result<()> {
    for message in batch.messages()? {
        match process(message.body(), &env).await {
            Ok(()) => message.ack(),
            Err(_) => {
                console_error!("Backup processing failed; message retained for retry");
                message.retry();
            }
        }
    }
    Ok(())
}
