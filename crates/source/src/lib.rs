use std::{io, path::PathBuf};
use tokio::{fs::File, io::AsyncRead};

pub enum BackupSource {
    LocalFile(PathBuf),
    Stream(Box<dyn AsyncRead + Send + Unpin>),
}

impl BackupSource {
    pub async fn open(self) -> io::Result<Box<dyn AsyncRead + Send + Unpin>> {
        match self {
            Self::LocalFile(path) => Ok(Box::new(File::open(path).await?)),
            Self::Stream(stream) => Ok(stream),
        }
    }
}
