//! Stateless upload and verified restore. Callers own the manifest; no database is used.
use async_trait::async_trait;
use chacha20poly1305::{
    aead::{Aead, Payload},
    KeyInit, XChaCha20Poly1305, XNonce,
};
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use thiserror::Error;

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
pub mod native;
pub mod png;
pub const FORMAT: &str = "backupd-stateless-v2";
pub const MAX_CHUNK_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_STORED_BYTES: usize = MAX_CHUNK_BYTES + 128 * 1024;
pub const MAX_CHUNKS: usize = 100_000;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid configuration or manifest")]
    Invalid,
    #[error("source or output I/O failed")]
    Io(#[from] std::io::Error),
    #[error("transport operation failed")]
    Transport,
    #[error("integrity or authentication check failed")]
    Integrity,
    #[error("input exceeds configured limits")]
    Limit,
    #[error("secure randomness unavailable")]
    Random,
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Chunk {
    pub sequence: u64,
    pub plaintext_hash: String,
    pub plaintext_size: u64,
    pub stored_hash: String,
    pub stored_size: u64,
    pub locator: String,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub format: String,
    pub filename: String,
    pub key_id: String,
    pub compression: String,
    pub encryption: String,
    pub original_size: u64,
    pub plaintext_hash: String,
    pub chunks: Vec<Chunk>,
    pub authentication: String,
}
impl Manifest {
    pub fn to_json(&self) -> Result<Vec<u8>> {
        serde_json::to_vec_pretty(self).map_err(|_| Error::Invalid)
    }
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(Error::Limit);
        }
        serde_json::from_slice(bytes).map_err(|_| Error::Invalid)
    }
}

/// Provider envelopes belong in adapters. `get` returns original encrypted bytes.
#[async_trait(?Send)]
pub trait BlobStore {
    async fn put(&self, key: &str, bytes: &[u8]) -> Result<String>;
    async fn get(&self, locator: &str, max_bytes: usize) -> Result<Vec<u8>>;
}

/// Key material is never serialized or included in Debug output.
pub struct Uploader {
    cipher: XChaCha20Poly1305,
    manifest_key: [u8; 32],
    key_id: String,
    chunk_size: usize,
    max_input: u64,
}
impl Uploader {
    pub fn new(key: [u8; 32], key_id: String, chunk_size: usize, max_input: u64) -> Result<Self> {
        if !(1..=MAX_CHUNK_BYTES).contains(&chunk_size) || key_id.is_empty() || key_id.len() > 128 {
            return Err(Error::Invalid);
        }
        Ok(Self {
            cipher: XChaCha20Poly1305::new((&key).into()),
            manifest_key: blake3::derive_key("backupd-stateless-v2 manifest authentication", &key),
            key_id,
            chunk_size,
            max_input,
        })
    }
    pub async fn upload<R: AsyncRead + Unpin, S: BlobStore + ?Sized>(
        &self,
        mut input: R,
        filename: String,
        store: &S,
    ) -> Result<Manifest> {
        if filename.is_empty() || filename.len() > 1024 {
            return Err(Error::Invalid);
        }
        let mut manifest = Manifest {
            format: FORMAT.into(),
            filename,
            key_id: self.key_id.clone(),
            compression: "zlib".into(),
            encryption: "xchacha20-poly1305".into(),
            original_size: 0,
            plaintext_hash: String::new(),
            chunks: Vec::new(),
            authentication: String::new(),
        };
        let mut hasher = blake3::Hasher::new();
        loop {
            let mut plain = vec![0; self.chunk_size];
            let mut size = 0;
            while size < plain.len() {
                let count = input.read(&mut plain[size..]).await?;
                if count == 0 {
                    break;
                }
                size += count;
            }
            if size == 0 {
                break;
            }
            plain.truncate(size);
            manifest.original_size = manifest
                .original_size
                .checked_add(size as u64)
                .ok_or(Error::Limit)?;
            if manifest.original_size > self.max_input || manifest.chunks.len() >= MAX_CHUNKS {
                return Err(Error::Limit);
            }
            hasher.update(&plain);
            let mut chunk = Chunk {
                sequence: manifest.chunks.len() as u64,
                plaintext_hash: digest(&plain),
                plaintext_size: size as u64,
                stored_hash: String::new(),
                stored_size: 0,
                locator: String::new(),
            };
            let stored = self.encode(&chunk, &plain)?;
            chunk.stored_hash = digest(&stored);
            chunk.stored_size = stored.len() as u64;
            let key = format!("chunks/{}", chunk.stored_hash.trim_start_matches("blake3:"));
            let locator = store.put(&key, &stored).await?;
            if locator.is_empty() || locator.len() > 8192 {
                return Err(Error::Invalid);
            }
            let retrieved = store.get(&locator, MAX_STORED_BYTES).await?;
            if retrieved.len() != stored.len() || digest(&retrieved) != chunk.stored_hash {
                return Err(Error::Integrity);
            }
            chunk.locator = locator;
            manifest.chunks.push(chunk);
        }
        manifest.plaintext_hash = format!("blake3:{}", hasher.finalize().to_hex());
        manifest.authentication = self.tag(&manifest)?;
        Ok(manifest)
    }
    /// Discard output on failure; chunks authenticate before writing, whole-file verification happens last.
    pub async fn restore<W: AsyncWrite + Unpin, S: BlobStore + ?Sized>(
        &self,
        manifest: &Manifest,
        store: &S,
        mut output: W,
    ) -> Result<()> {
        self.verify_manifest(manifest)?;
        let mut hasher = blake3::Hasher::new();
        let mut size = 0u64;
        for chunk in &manifest.chunks {
            let stored = store.get(&chunk.locator, MAX_STORED_BYTES).await?;
            let plain = self.decode(chunk, &stored)?;
            hasher.update(&plain);
            size = size.checked_add(plain.len() as u64).ok_or(Error::Limit)?;
            output.write_all(&plain).await?;
        }
        if size != manifest.original_size
            || format!("blake3:{}", hasher.finalize().to_hex()) != manifest.plaintext_hash
        {
            return Err(Error::Integrity);
        }
        output.flush().await?;
        Ok(())
    }
    pub fn verify_manifest(&self, manifest: &Manifest) -> Result<()> {
        if manifest.format != FORMAT
            || manifest.key_id != self.key_id
            || manifest.compression != "zlib"
            || manifest.encryption != "xchacha20-poly1305"
            || manifest.chunks.len() > MAX_CHUNKS
            || manifest.original_size > self.max_input
        {
            return Err(Error::Invalid);
        }
        let supplied: [u8; 32] = hex::decode(&manifest.authentication)
            .map_err(|_| Error::Integrity)?
            .try_into()
            .map_err(|_| Error::Integrity)?;
        let expected: [u8; 32] = hex::decode(self.tag(manifest)?)
            .map_err(|_| Error::Integrity)?
            .try_into()
            .map_err(|_| Error::Integrity)?;
        if blake3::Hash::from(supplied) != blake3::Hash::from(expected) {
            return Err(Error::Integrity);
        }
        let mut size = 0u64;
        for (index, chunk) in manifest.chunks.iter().enumerate() {
            if chunk.sequence != index as u64
                || chunk.plaintext_size == 0
                || chunk.plaintext_size > MAX_CHUNK_BYTES as u64
                || chunk.stored_size > MAX_STORED_BYTES as u64
                || chunk.locator.is_empty()
                || chunk.locator.len() > 8192
            {
                return Err(Error::Invalid);
            }
            size = size
                .checked_add(chunk.plaintext_size)
                .ok_or(Error::Invalid)?;
        }
        if size != manifest.original_size {
            return Err(Error::Invalid);
        }
        Ok(())
    }
    fn encode(&self, chunk: &Chunk, plain: &[u8]) -> Result<Vec<u8>> {
        let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::new(6));
        encoder.write_all(plain)?;
        let compressed = encoder.finish()?;
        let mut nonce = [0; 24];
        getrandom::getrandom(&mut nonce).map_err(|_| Error::Random)?;
        let aad = aad(chunk);
        let encrypted = self
            .cipher
            .encrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: &compressed,
                    aad: aad.as_bytes(),
                },
            )
            .map_err(|_| Error::Integrity)?;
        let mut stored = nonce.to_vec();
        stored.extend_from_slice(&encrypted);
        if stored.len() > MAX_STORED_BYTES {
            return Err(Error::Limit);
        }
        Ok(stored)
    }
    fn decode(&self, chunk: &Chunk, stored: &[u8]) -> Result<Vec<u8>> {
        if stored.len() < 40
            || stored.len() as u64 != chunk.stored_size
            || digest(stored) != chunk.stored_hash
        {
            return Err(Error::Integrity);
        }
        let aad = aad(chunk);
        let compressed = self
            .cipher
            .decrypt(
                XNonce::from_slice(&stored[..24]),
                Payload {
                    msg: &stored[24..],
                    aad: aad.as_bytes(),
                },
            )
            .map_err(|_| Error::Integrity)?;
        let decoder = flate2::read::ZlibDecoder::new(compressed.as_slice());
        let mut plain = Vec::new();
        decoder
            .take(chunk.plaintext_size + 1)
            .read_to_end(&mut plain)?;
        if plain.len() as u64 != chunk.plaintext_size || digest(&plain) != chunk.plaintext_hash {
            return Err(Error::Integrity);
        }
        Ok(plain)
    }
    fn tag(&self, manifest: &Manifest) -> Result<String> {
        let mut unsigned = manifest.clone();
        unsigned.authentication.clear();
        let json = serde_json::to_vec(&unsigned).map_err(|_| Error::Invalid)?;
        Ok(blake3::keyed_hash(&self.manifest_key, &json)
            .to_hex()
            .to_string())
    }
}
pub fn digest(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}
fn aad(chunk: &Chunk) -> String {
    format!(
        "{FORMAT}:{}:{}:{}",
        chunk.sequence, chunk.plaintext_size, chunk.plaintext_hash
    )
}
#[cfg(test)]
mod tests;
