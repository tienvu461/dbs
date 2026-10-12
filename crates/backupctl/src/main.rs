use anyhow::{Context, Result};
use async_trait::async_trait;
use backupd_lib::{BlobStore, Error, Manifest, Uploader, MAX_STORED_BYTES};
use backupd_transport::{
    tiktok::{download_public_png, TikTokBusinessUploader},
    BlobTransport, BlobUpload, S3CompatibleTransport,
};
use clap::{Parser, Subcommand, ValueEnum};
use std::{
    env,
    path::{Path, PathBuf},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::compat::TokioAsyncWriteCompatExt;

#[derive(Parser)]
#[command(about = "Stateless encrypted backup upload/restore; keep the returned manifest")]
struct Args {
    #[arg(long, value_enum, default_value = "filesystem", global = true)]
    store: StoreKind,
    #[arg(
        long,
        env = "BACKUPD_STORAGE_ROOT",
        default_value = "data/blobs",
        global = true
    )]
    root: PathBuf,
    #[arg(long, default_value = "default", global = true)]
    key_id: String,
    #[arg(long, default_value_t = 1024 * 1024, global = true)]
    chunk_size: usize,
    #[arg(long, default_value_t = 1024 * 1024 * 1024 * 1024, global = true)]
    max_input_bytes: u64,
    #[command(subcommand)]
    command: Command,
}
#[derive(Clone, Copy, ValueEnum)]
enum StoreKind {
    Filesystem,
    S3,
    Tiktok,
}
#[derive(Subcommand)]
enum Command {
    Upload {
        /// Local file, '-' for stdin, or HTTPS URL. Source URLs are not stored in manifests.
        source: String,
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        filename: Option<String>,
        /// Explicitly allow private addresses and HTTP for a trusted local export server.
        #[arg(long)]
        allow_private_source: bool,
    },
    Restore {
        manifest: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
}

struct FsStore {
    root: PathBuf,
}
impl FsStore {
    fn path(&self, key: &str) -> backupd_lib::Result<PathBuf> {
        if key.is_empty()
            || key.starts_with('/')
            || key.contains('\\')
            || key
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
        {
            return Err(Error::Invalid);
        }
        Ok(self.root.join(key))
    }
}
#[async_trait(?Send)]
impl BlobStore for FsStore {
    async fn put(&self, key: &str, bytes: &[u8]) -> backupd_lib::Result<String> {
        let path = self.path(key)?;
        let parent = path.parent().ok_or(Error::Invalid)?;
        tokio::fs::create_dir_all(parent).await?;
        if !tokio::fs::canonicalize(parent)
            .await?
            .starts_with(&self.root)
        {
            return Err(Error::Invalid);
        }
        let temp = parent.join(format!("{}.tmp", ulid::Ulid::new()));
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)
            .await?;
        file.write_all(bytes).await?;
        file.sync_all().await?;
        drop(file);
        let link = tokio::fs::hard_link(&temp, &path).await;
        let _ = tokio::fs::remove_file(temp).await;
        if let Err(error) = link {
            if error.kind() != std::io::ErrorKind::AlreadyExists {
                return Err(error.into());
            }
        }
        let locator = format!("fs://{key}");
        if self.get(&locator, MAX_STORED_BYTES).await? != bytes {
            return Err(Error::Integrity);
        }
        Ok(locator)
    }
    async fn get(&self, locator: &str, max: usize) -> backupd_lib::Result<Vec<u8>> {
        let path = self.path(locator.strip_prefix("fs://").ok_or(Error::Invalid)?)?;
        let canonical = tokio::fs::canonicalize(&path).await?;
        if !canonical.starts_with(&self.root) {
            return Err(Error::Invalid);
        }
        let file = tokio::fs::File::open(canonical).await?;
        let mut data = Vec::new();
        file.take(max as u64 + 1).read_to_end(&mut data).await?;
        if data.len() > max {
            return Err(Error::Limit);
        }
        Ok(data)
    }
}
struct S3Store(S3CompatibleTransport);
#[async_trait(?Send)]
impl BlobStore for S3Store {
    async fn put(&self, key: &str, bytes: &[u8]) -> backupd_lib::Result<String> {
        let uploaded = self
            .0
            .put(BlobUpload {
                id: key.into(),
                logical_id: key.into(),
                bytes: bytes.to_vec().into(),
                stored_hash: backupd_lib::digest(bytes),
            })
            .await
            .map_err(|_| Error::Transport)?;
        Ok(uploaded.locator)
    }
    async fn get(&self, locator: &str, max: usize) -> backupd_lib::Result<Vec<u8>> {
        // Resolve Workers binding locators through R2's S3 API without editing signed metadata.
        let translated;
        let locator = if let Some(path) = locator.strip_prefix("r2://") {
            translated = format!("s3://{path}");
            translated.as_str()
        } else {
            locator
        };
        let metadata = self.0.head(locator).await.map_err(|_| Error::Transport)?;
        if metadata.size > max as u64 {
            return Err(Error::Limit);
        }
        let bytes = self.0.get(locator).await.map_err(|_| Error::Transport)?;
        if bytes.len() > max {
            return Err(Error::Limit);
        }
        Ok(bytes)
    }
}
struct TikTokStore {
    uploader: Option<TikTokBusinessUploader>,
    origin: reqwest::Url,
}
#[async_trait(?Send)]
impl BlobStore for TikTokStore {
    async fn put(&self, key: &str, bytes: &[u8]) -> backupd_lib::Result<String> {
        let png = backupd_lib::png::encode(bytes)?;
        let uri = self
            .uploader
            .as_ref()
            .ok_or(Error::Invalid)?
            .upload_png(&format!("{}.png", key.replace('/', "-")), png)
            .await
            .map_err(|_| Error::Transport)?;
        let url = self.origin.join(&uri).map_err(|_| Error::Invalid)?;
        Ok(url.to_string())
    }
    async fn get(&self, locator: &str, max: usize) -> backupd_lib::Result<Vec<u8>> {
        let png = download_public_png(locator)
            .await
            .map_err(|_| Error::Transport)?;
        let bytes = backupd_lib::png::decode(&png)?;
        if bytes.len() > max {
            return Err(Error::Limit);
        }
        Ok(bytes)
    }
}

async fn store(args: &Args) -> Result<Box<dyn BlobStore>> {
    Ok(match args.store {
        StoreKind::Filesystem => {
            tokio::fs::create_dir_all(&args.root).await?;
            Box::new(FsStore {
                root: tokio::fs::canonicalize(&args.root).await?,
            })
        }
        StoreKind::S3 => Box::new(S3Store(
            S3CompatibleTransport::new(
                env::var("S3_ENDPOINT_URL").context("S3_ENDPOINT_URL is required")?,
                env::var("AWS_REGION").unwrap_or_else(|_| "auto".into()),
                env::var("S3_BUCKET").context("S3_BUCKET is required")?,
                env::var("S3_PREFIX").unwrap_or_else(|_| "backupd".into()),
                env::var("AWS_ACCESS_KEY_ID").ok(),
                env::var("AWS_SECRET_ACCESS_KEY").ok(),
            )
            .await?,
        )),
        StoreKind::Tiktok => {
            let uploader = if matches!(args.command, Command::Upload { .. }) {
                Some(TikTokBusinessUploader::new(
                    env::var("TIKTOK_ORG_ID").context("TIKTOK_ORG_ID is required")?,
                    env::var("TIKTOK_COOKIE").context("TIKTOK_COOKIE is required")?,
                    env::var("TIKTOK_CSRF_TOKEN").ok(),
                )?)
            } else {
                None
            };
            Box::new(TikTokStore {
                uploader,
                origin: env::var("TIKTOK_CDN_ORIGIN")
                    .unwrap_or_else(|_| "https://p16-ad-sg.tiktokcdn.com/origin/".into())
                    .parse()
                    .context("invalid CDN origin")?,
            })
        }
    })
}

async fn publish_file(temp: &Path, destination: &Path) -> Result<()> {
    tokio::fs::hard_link(temp, destination)
        .await
        .context("cannot publish output (destination must not already exist)")?;
    tokio::fs::remove_file(temp).await?;
    Ok(())
}

async fn create_output(path: &Path) -> Result<tokio::fs::File> {
    let mut options = tokio::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    options.mode(0o600);
    Ok(options.open(path).await?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let destination = match &args.command {
        Command::Upload { manifest, .. } => manifest,
        Command::Restore { output, .. } => output,
    };
    if tokio::fs::try_exists(destination).await? {
        anyhow::bail!("output destination already exists");
    }
    let key: [u8; 32] =
        hex::decode(env::var("BACKUP_MASTER_KEY").context("BACKUP_MASTER_KEY is required")?)
            .map_err(|_| anyhow::anyhow!("key must be hex encoded"))?
            .try_into()
            .map_err(|_| anyhow::anyhow!("key must decode to 32 bytes"))?;
    let uploader = Uploader::new(
        key,
        args.key_id.clone(),
        args.chunk_size,
        args.max_input_bytes,
    )?;
    let store = store(&args).await?;
    match args.command {
        Command::Upload {
            source: location,
            manifest,
            filename,
            allow_private_source,
        } => {
            let input = backupd_lib::native::open_source(&location, allow_private_source).await?;
            let filename = filename.unwrap_or_else(|| {
                if location.starts_with("http") || location == "-" {
                    "backup.bin".into()
                } else {
                    Path::new(&location)
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .into_owned()
                }
            });
            let metadata = uploader.upload(input, filename, store.as_ref()).await?;
            let temp = manifest.with_extension(format!("{}.tmp", ulid::Ulid::new()));
            let mut output = create_output(&temp).await?;
            output.write_all(&metadata.to_json()?).await?;
            output.sync_all().await?;
            drop(output);
            publish_file(&temp, &manifest).await?;
            println!("Manifest saved to {}", manifest.display());
        }
        Command::Restore { manifest, output } => {
            let input = tokio::fs::File::open(manifest).await?;
            let mut bytes = Vec::new();
            input
                .take(64 * 1024 * 1024 + 1)
                .read_to_end(&mut bytes)
                .await?;
            let metadata = Manifest::from_json(&bytes)?;
            uploader.verify_manifest(&metadata)?;
            let temp = output.with_extension(format!("{}.partial", ulid::Ulid::new()));
            let file = create_output(&temp).await?;
            let result = uploader
                .restore(&metadata, store.as_ref(), file.compat_write())
                .await;
            if let Err(error) = result {
                let _ = tokio::fs::remove_file(&temp).await;
                return Err(error.into());
            }
            tokio::fs::OpenOptions::new()
                .write(true)
                .open(&temp)
                .await?
                .sync_all()
                .await?;
            publish_file(&temp, &output).await?;
            println!("Restored to {}", output.display());
        }
    }
    Ok(())
}
