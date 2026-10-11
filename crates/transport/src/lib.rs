use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    config::{Credentials, Region},
    primitives::ByteStream,
    Client,
};
use bytes::Bytes;
use futures::stream::BoxStream;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::{fs, io::AsyncReadExt};

pub mod tiktok;

pub type BlobStream = BoxStream<'static, Result<Bytes, TransportError>>;
#[derive(Clone, Debug)]
pub struct BlobUpload {
    pub id: String,
    pub logical_id: String,
    pub bytes: Bytes,
    pub stored_hash: String,
}
#[derive(Clone, Debug)]
pub struct RemoteBlob {
    pub locator: String,
    pub size: u64,
}
#[derive(Clone, Debug)]
pub struct RemoteBlobMetadata {
    pub locator: String,
    pub size: u64,
}

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("transport I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("blob integrity check failed")]
    Integrity,
    #[error("invalid locator")]
    InvalidLocator,
    #[error("S3 request failed: {0}")]
    S3(String),
}

#[async_trait]
pub trait BlobTransport: Send + Sync {
    fn locator_for(&self, id: &str) -> Result<String, TransportError>;
    async fn put(&self, blob: BlobUpload) -> Result<RemoteBlob, TransportError>;
    async fn get(&self, locator: &str) -> Result<Vec<u8>, TransportError>;
    async fn head(&self, locator: &str) -> Result<RemoteBlobMetadata, TransportError>;
    async fn delete(&self, locator: &str) -> Result<(), TransportError>;
    fn raw_locator(&self, locator: &str) -> Result<String, TransportError> {
        Ok(locator.to_owned())
    }
}

#[derive(Clone)]
pub struct S3CompatibleTransport {
    client: Client,
    bucket: String,
    prefix: String,
    endpoint: String,
}

impl S3CompatibleTransport {
    pub async fn new(
        endpoint: impl Into<String>,
        region: impl Into<String>,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        access_key: Option<String>,
        secret_key: Option<String>,
    ) -> Result<Self, TransportError> {
        let endpoint = endpoint.into();
        let region = region.into();
        let bucket = bucket.into();
        let prefix = prefix.into().trim_matches('/').to_owned();
        let mut loader =
            aws_config::defaults(BehaviorVersion::latest()).region(Region::new(region));
        if let (Some(access), Some(secret)) = (access_key, secret_key) {
            loader = loader.credentials_provider(Credentials::new(
                access,
                secret,
                None,
                None,
                "backupd-static-credentials",
            ));
        }
        let shared = loader.load().await;
        let config = aws_sdk_s3::config::Builder::from(&shared)
            .endpoint_url(&endpoint)
            .force_path_style(true)
            .build();
        Ok(Self {
            client: Client::from_conf(config),
            bucket,
            prefix,
            endpoint,
        })
    }

    fn object_key(&self, id: &str) -> Result<String, TransportError> {
        if id.is_empty()
            || id
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
            || id.contains('\\')
        {
            return Err(TransportError::InvalidLocator);
        }
        Ok(
            if self.prefix.is_empty() || id.starts_with(&format!("{}/", self.prefix)) {
                id.to_owned()
            } else {
                format!("{}/{id}", self.prefix)
            },
        )
    }

    fn parse_locator(&self, locator: &str) -> Result<String, TransportError> {
        let key = locator
            .strip_prefix("s3://")
            .ok_or(TransportError::InvalidLocator)?;
        let (bucket, key) = key.split_once('/').ok_or(TransportError::InvalidLocator)?;
        if bucket != self.bucket || key.is_empty() || key.contains("..") {
            return Err(TransportError::InvalidLocator);
        }
        Ok(key.to_owned())
    }

    pub fn public_object_url(&self, locator: &str) -> Result<String, TransportError> {
        let key = self.parse_locator(locator)?;
        let base = self.endpoint.trim_end_matches('/');
        let encoded = key
            .split('/')
            .map(|part| url::form_urlencoded::byte_serialize(part.as_bytes()).collect::<String>())
            .collect::<Vec<_>>()
            .join("/");
        Ok(format!("{base}/{encoded}"))
    }
}

#[async_trait]
impl BlobTransport for S3CompatibleTransport {
    fn locator_for(&self, id: &str) -> Result<String, TransportError> {
        let key = if id.starts_with(&format!("{}/", self.prefix)) {
            id.to_owned()
        } else {
            self.object_key(id)?
        };
        Ok(format!("s3://{}/{}", self.bucket, key))
    }

    fn raw_locator(&self, locator: &str) -> Result<String, TransportError> {
        let key = self.parse_locator(locator)?;
        Ok(key)
    }

    async fn put(&self, blob: BlobUpload) -> Result<RemoteBlob, TransportError> {
        if blake3::hash(&blob.bytes).to_hex().to_string()
            != blob.stored_hash.strip_prefix("blake3:").unwrap_or_default()
        {
            return Err(TransportError::Integrity);
        }
        let key = self.object_key(&blob.id)?;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(blob.bytes.clone()))
            .send()
            .await
            .map_err(|_| TransportError::S3("object upload failed".into()))?;
        let metadata = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|_| TransportError::S3("object upload verification failed".into()))?;
        if metadata.content_length().unwrap_or(-1) != blob.bytes.len() as i64 {
            return Err(TransportError::Integrity);
        }
        Ok(RemoteBlob {
            locator: format!("s3://{}/{}", self.bucket, key),
            size: blob.bytes.len() as u64,
        })
    }

    async fn get(&self, locator: &str) -> Result<Vec<u8>, TransportError> {
        let key = self.parse_locator(locator)?;
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|_| TransportError::S3("object download failed".into()))?;
        output
            .body
            .collect()
            .await
            .map(|data| data.into_bytes().to_vec())
            .map_err(|_| TransportError::S3("object body read failed".into()))
    }

    async fn head(&self, locator: &str) -> Result<RemoteBlobMetadata, TransportError> {
        let key = self.parse_locator(locator)?;
        let output = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|_| TransportError::S3("object metadata request failed".into()))?;
        Ok(RemoteBlobMetadata {
            locator: locator.to_owned(),
            size: output.content_length().unwrap_or_default().max(0) as u64,
        })
    }

    async fn delete(&self, locator: &str) -> Result<(), TransportError> {
        let key = self.parse_locator(locator)?;
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|_| TransportError::S3("object deletion failed".into()))?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct FilesystemTransport {
    root: PathBuf,
}

impl FilesystemTransport {
    pub async fn new(root: impl Into<PathBuf>) -> Result<Self, TransportError> {
        let root = root.into();
        fs::create_dir_all(&root).await?;
        Ok(Self { root })
    }
    fn path_for(&self, locator: &str) -> Result<PathBuf, TransportError> {
        let name = locator
            .strip_prefix("fs://")
            .ok_or(TransportError::InvalidLocator)?;
        if name.is_empty()
            || name
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
            || name.contains('\\')
        {
            return Err(TransportError::InvalidLocator);
        }
        let path = self.root.join(name);
        if !path.starts_with(&self.root) {
            return Err(TransportError::InvalidLocator);
        }
        Ok(path)
    }
    fn object_path(&self, id: &str) -> Result<PathBuf, TransportError> {
        if id.is_empty()
            || id
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
            || id.contains('\\')
        {
            return Err(TransportError::InvalidLocator);
        }
        let path = self.root.join(id);
        if !path.starts_with(&self.root) {
            return Err(TransportError::InvalidLocator);
        }
        Ok(path)
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
}

#[async_trait]
impl BlobTransport for FilesystemTransport {
    fn locator_for(&self, id: &str) -> Result<String, TransportError> {
        self.object_path(id)?;
        Ok(format!("fs://{id}"))
    }

    async fn put(&self, blob: BlobUpload) -> Result<RemoteBlob, TransportError> {
        if blake3::hash(&blob.bytes).to_hex().to_string()
            != blob.stored_hash.strip_prefix("blake3:").unwrap_or_default()
        {
            return Err(TransportError::Integrity);
        }
        let path = self.object_path(&blob.id)?;
        let logical_path = self.object_path(&blob.logical_id)?;
        if fs::try_exists(&path).await? {
            let existing = fs::read(&path).await?;
            if existing != blob.bytes {
                return Err(TransportError::Integrity);
            }
        } else {
            let tmp = path.with_extension(format!("tmp-{}", ulid::Ulid::new()));
            fs::write(&tmp, &blob.bytes).await?;
            if let Err(error) = fs::hard_link(&tmp, &path).await {
                let _ = fs::remove_file(&tmp).await;
                if error.kind() != std::io::ErrorKind::AlreadyExists {
                    return Err(error.into());
                }
            } else {
                fs::remove_file(&tmp).await?;
            }
        }
        if !fs::try_exists(&logical_path).await? {
            fs::hard_link(&path, &logical_path).await.or_else(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    Ok(())
                } else {
                    Err(error)
                }
            })?;
        } else {
            let existing = fs::read(&logical_path).await?;
            if existing != blob.bytes {
                return Err(TransportError::Integrity);
            }
        }
        Ok(RemoteBlob {
            locator: format!("fs://{}", blob.logical_id),
            size: blob.bytes.len() as u64,
        })
    }
    async fn get(&self, locator: &str) -> Result<Vec<u8>, TransportError> {
        let mut file = fs::File::open(self.path_for(locator)?).await?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;
        Ok(bytes)
    }
    async fn head(&self, locator: &str) -> Result<RemoteBlobMetadata, TransportError> {
        let metadata = fs::metadata(self.path_for(locator)?).await?;
        Ok(RemoteBlobMetadata {
            locator: locator.to_owned(),
            size: metadata.len(),
        })
    }
    async fn delete(&self, locator: &str) -> Result<(), TransportError> {
        match fs::remove_file(self.path_for(locator)?).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}
