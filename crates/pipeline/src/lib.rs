use anyhow::{anyhow, Context, Result};
use backupd_core::ChunkManifest;
use chacha20poly1305::{aead::Aead, KeyInit, XChaCha20Poly1305, XNonce};
use rand::RngCore;
use std::io::Cursor;
use tokio::io::{AsyncRead, AsyncReadExt};

pub struct EncodedChunk {
    pub manifest: ChunkManifest,
    pub bytes: Vec<u8>,
}

pub struct Pipeline {
    chunk_size: usize,
    compression_level: i32,
    cipher: Option<XChaCha20Poly1305>,
}

impl Pipeline {
    pub fn new(
        chunk_size: usize,
        compression_level: i32,
        encryption_key: Option<[u8; 32]>,
    ) -> Result<Self> {
        if chunk_size == 0 {
            return Err(anyhow!("chunk size must be greater than zero"));
        }
        Ok(Self {
            chunk_size,
            compression_level,
            cipher: encryption_key.map(|key| XChaCha20Poly1305::new((&key).into())),
        })
    }

    pub async fn process<R, F, Fut>(&self, mut input: R, mut on_chunk: F) -> Result<(u64, String)>
    where
        R: AsyncRead + Unpin,
        F: FnMut(EncodedChunk) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let mut total = 0u64;
        let mut whole_hasher = blake3::Hasher::new();
        let mut sequence = 0u64;
        loop {
            let mut plaintext = vec![0u8; self.chunk_size];
            let mut filled = 0;
            while filled < plaintext.len() {
                let read = input.read(&mut plaintext[filled..]).await?;
                if read == 0 {
                    break;
                }
                filled += read;
            }
            if filled == 0 {
                break;
            }
            plaintext.truncate(filled);
            total = total
                .checked_add(filled as u64)
                .context("input size overflow")?;
            whole_hasher.update(&plaintext);
            let plaintext_hash = format!("blake3:{}", blake3::hash(&plaintext).to_hex());
            let compressed =
                zstd::stream::encode_all(Cursor::new(&plaintext), self.compression_level)?;
            let (stored, encryption) = if let Some(cipher) = &self.cipher {
                let mut nonce = [0u8; 24];
                rand::thread_rng().fill_bytes(&mut nonce);
                let encrypted = cipher
                    .encrypt(XNonce::from_slice(&nonce), compressed.as_ref())
                    .map_err(|_| anyhow!("chunk encryption failed"))?;
                let mut payload = Vec::with_capacity(nonce.len() + encrypted.len());
                payload.extend_from_slice(&nonce);
                payload.extend_from_slice(&encrypted);
                (payload, "xchacha20-poly1305".to_owned())
            } else {
                (compressed, "none".to_owned())
            };
            let stored_hash = format!("blake3:{}", blake3::hash(&stored).to_hex());
            let manifest = ChunkManifest {
                sequence,
                blob_id: stored_hash.clone(),
                plaintext_hash,
                stored_hash,
                plaintext_size: filled as u64,
                stored_size: stored.len() as u64,
                compression: "zstd".into(),
                encryption,
                envelope: Some("raw-v1".into()),
                cdn_url: None,
                cdn_encrypted: None,
            };
            on_chunk(EncodedChunk {
                manifest,
                bytes: stored,
            })
            .await?;
            sequence += 1;
            if filled < self.chunk_size {
                break;
            }
        }
        Ok((
            total,
            format!("blake3:{}", whole_hasher.finalize().to_hex()),
        ))
    }

    pub fn decode(&self, chunk: &ChunkManifest, bytes: &[u8]) -> Result<Vec<u8>> {
        let actual_stored = format!("blake3:{}", blake3::hash(bytes).to_hex());
        if actual_stored != chunk.stored_hash {
            return Err(anyhow!("stored hash mismatch for chunk {}", chunk.sequence));
        }
        let compressed = if chunk.encryption == "xchacha20-poly1305" {
            let cipher = self
                .cipher
                .as_ref()
                .ok_or_else(|| anyhow!("encryption key is not configured"))?;
            if bytes.len() < 24 {
                return Err(anyhow!("encrypted chunk is truncated"));
            }
            cipher
                .decrypt(XNonce::from_slice(&bytes[..24]), &bytes[24..])
                .map_err(|_| anyhow!("chunk authentication failed"))?
        } else if chunk.encryption == "none" {
            bytes.to_vec()
        } else {
            return Err(anyhow!(
                "unsupported encryption algorithm {}",
                chunk.encryption
            ));
        };
        let plaintext = zstd::stream::decode_all(Cursor::new(compressed))?;
        let actual_plaintext = format!("blake3:{}", blake3::hash(&plaintext).to_hex());
        if actual_plaintext != chunk.plaintext_hash
            || plaintext.len() as u64 != chunk.plaintext_size
        {
            return Err(anyhow!(
                "plaintext integrity check failed for chunk {}",
                chunk.sequence
            ));
        }
        Ok(plaintext)
    }

    pub fn encode_raw(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let compressed = zstd::stream::encode_all(Cursor::new(plaintext), self.compression_level)?;
        if let Some(cipher) = &self.cipher {
            let mut nonce = [0u8; 24];
            rand::thread_rng().fill_bytes(&mut nonce);
            let encrypted = cipher
                .encrypt(XNonce::from_slice(&nonce), compressed.as_ref())
                .map_err(|_| anyhow!("payload encryption failed"))?;
            let mut stored = Vec::with_capacity(nonce.len() + encrypted.len());
            stored.extend_from_slice(&nonce);
            stored.extend_from_slice(&encrypted);
            Ok(stored)
        } else {
            Ok(compressed)
        }
    }

    pub fn decode_raw(&self, encrypted: bool, bytes: &[u8]) -> Result<Vec<u8>> {
        let compressed = if encrypted {
            let cipher = self
                .cipher
                .as_ref()
                .ok_or_else(|| anyhow!("decryption key is not configured"))?;
            if bytes.len() < 24 {
                return Err(anyhow!("encrypted payload is truncated"));
            }
            cipher
                .decrypt(XNonce::from_slice(&bytes[..24]), &bytes[24..])
                .map_err(|_| anyhow!("payload authentication failed"))?
        } else {
            bytes.to_vec()
        };
        Ok(zstd::stream::decode_all(Cursor::new(compressed))?)
    }

    pub fn decode_cdn_chunk(&self, chunk: &ChunkManifest, cdn_bytes: &[u8]) -> Result<Vec<u8>> {
        if cdn_bytes.len() < 24 {
            return Err(anyhow!("CDN encrypted chunk is truncated"));
        }
        if chunk.encryption != "xchacha20-poly1305" {
            return Err(anyhow!(
                "CDN integration requires XChaCha20-Poly1305 chunks"
            ));
        }
        if chunk.stored_hash.is_empty() {
            return Err(anyhow!("missing stored hash"));
        }
        let nonce = &cdn_bytes[..24];
        let cipher_text = &cdn_bytes[24..];
        let cipher = self
            .cipher
            .as_ref()
            .ok_or_else(|| anyhow!("decryption key is not configured"))?;
        let compressed = cipher
            .decrypt(XNonce::from_slice(nonce), cipher_text)
            .map_err(|_| anyhow!("CDN chunk authentication failed"))?;
        let plaintext = zstd::stream::decode_all(Cursor::new(compressed))?;
        let hash = format!("blake3:{}", blake3::hash(&plaintext).to_hex());
        if hash != chunk.plaintext_hash || plaintext.len() as u64 != chunk.plaintext_size {
            return Err(anyhow!("CDN plaintext integrity check failed"));
        }
        Ok(plaintext)
    }

    pub fn rewrap_cdn_ciphertext(&self, chunk: &ChunkManifest, stored: &[u8]) -> Result<Vec<u8>> {
        if stored.len() < 40 {
            return Err(anyhow!("encrypted stored chunk is truncated"));
        }
        if chunk.encryption != "xchacha20-poly1305" {
            return Err(anyhow!("TikTok CDN requires XChaCha20-Poly1305"));
        }
        let cipher = self
            .cipher
            .as_ref()
            .ok_or_else(|| anyhow!("encryption key is not configured"))?;
        let compressed = cipher
            .decrypt(XNonce::from_slice(&stored[..24]), &stored[24..])
            .map_err(|_| anyhow!("stored chunk authentication failed"))?;
        let plaintext = zstd::stream::decode_all(Cursor::new(compressed.as_slice()))?;
        if blake3::hash(&plaintext).to_hex().to_string()
            != chunk
                .plaintext_hash
                .strip_prefix("blake3:")
                .unwrap_or_default()
            || plaintext.len() as u64 != chunk.plaintext_size
        {
            return Err(anyhow!("stored chunk plaintext integrity check failed"));
        }
        let mut nonce = [0u8; 24];
        rand::thread_rng().fill_bytes(&mut nonce);
        let ciphertext = cipher
            .encrypt(XNonce::from_slice(&nonce), compressed.as_ref())
            .map_err(|_| anyhow!("CDN encryption failed"))?;
        let mut output = Vec::with_capacity(24 + ciphertext.len());
        output.extend_from_slice(&nonce);
        output.extend_from_slice(&ciphertext);
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn chunks_compress_encrypt_and_round_trip() {
        let key = [7u8; 32];
        let pipeline = Pipeline::new(4, 3, Some(key)).expect("pipeline config");
        let input = b"abcdefghijk".to_vec();
        let expected = input.clone();
        let mut chunks = Vec::new();
        let (size, checksum) = pipeline
            .process(BufReader::new(input.as_slice()), |chunk| {
                chunks.push(chunk);
                async { Ok(()) }
            })
            .await
            .expect("process input");
        assert_eq!(size, expected.len() as u64);
        assert_eq!(
            checksum,
            format!("blake3:{}", blake3::hash(&expected).to_hex())
        );
        let decoded = chunks
            .iter()
            .flat_map(|chunk| {
                pipeline
                    .decode(&chunk.manifest, &chunk.bytes)
                    .expect("decode chunk")
            })
            .collect::<Vec<_>>();
        assert_eq!(decoded, expected);
        assert_eq!(chunks.len(), 3);
    }

    #[tokio::test]
    async fn empty_input_has_valid_digest_and_no_chunks() {
        let pipeline = Pipeline::new(16, 3, None).expect("pipeline config");
        let mut count = 0;
        let (size, digest) = pipeline
            .process(&b""[..], |_| {
                count += 1;
                async { Ok(()) }
            })
            .await
            .expect("process input");
        assert_eq!(size, 0);
        assert_eq!(digest, format!("blake3:{}", blake3::hash(b"").to_hex()));
        assert_eq!(count, 0);
    }
}
