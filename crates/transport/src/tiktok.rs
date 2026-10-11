use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::StreamExt;
use reqwest::{multipart, Client, StatusCode};
use serde::Deserialize;
use std::time::Duration;

pub async fn download_public_png(url: &str) -> Result<Vec<u8>> {
    let url = reqwest::Url::parse(url).map_err(|_| anyhow!("invalid CDN URL"))?;
    let host = url.host_str().unwrap_or_default();
    if url.scheme() != "https"
        || !host.ends_with(".tiktokcdn.com")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some_and(|port| port != 443)
    {
        return Err(anyhow!("untrusted CDN URL"));
    }
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(90))
        .build()?;
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|_| anyhow!("CDN download failed"))?;
    if !response.status().is_success() {
        return Err(anyhow!("CDN download returned an error"));
    }
    const LIMIT: usize = 128 * 1024 * 1024;
    if response
        .content_length()
        .is_some_and(|size| size > LIMIT as u64)
    {
        return Err(anyhow!("CDN response exceeds size limit"));
    }
    let mut stream = response.bytes_stream();
    let mut output = Vec::new();
    while let Some(bytes) = stream.next().await {
        let bytes = bytes.map_err(|_| anyhow!("CDN body read failed"))?;
        if bytes.len() > LIMIT - output.len() {
            return Err(anyhow!("CDN response exceeds size limit"));
        }
        output.extend_from_slice(&bytes);
    }
    Ok(output)
}

#[derive(Clone)]
pub struct TikTokBusinessUploader {
    client: Client,
    org_id: String,
    cookie: String,
    csrf_token: Option<String>,
}

pub fn embed_payload_png(payload: &[u8], seed: &str) -> Vec<u8> {
    const PNG_SIG: &[u8; 8] = b"\x89PNG\r\n\x1a\n";
    let b64 = STANDARD.encode(payload);
    let mut png = make_pattern_png(seed);
    let mut chunks = Vec::new();
    for (index, piece) in b64.as_bytes().chunks(32 * 1024).enumerate() {
        chunks.extend_from_slice(&make_itxt(&format!("payload-{:04}", index + 1), piece));
    }
    let insertion = png.len() - 12;
    if png.starts_with(PNG_SIG) {
        png.splice(insertion..insertion, chunks);
    }
    png
}

pub fn extract_payload_png(png: &[u8]) -> Result<Vec<u8>> {
    const PNG_SIG: &[u8; 8] = b"\x89PNG\r\n\x1a\n";
    if !png.starts_with(PNG_SIG) {
        return Err(anyhow!("invalid PNG signature"));
    }
    let mut offset = 8usize;
    let mut parts: Vec<(usize, Vec<u8>)> = Vec::new();
    while offset + 12 <= png.len() {
        let len = u32::from_be_bytes(png[offset..offset + 4].try_into()?) as usize;
        let end = offset
            .checked_add(12 + len)
            .ok_or_else(|| anyhow!("PNG chunk length overflow"))?;
        if end > png.len() {
            return Err(anyhow!("truncated PNG chunk"));
        }
        let kind = &png[offset + 4..offset + 8];
        if kind == b"iTXt" {
            let data = &png[offset + 8..offset + 8 + len];
            if let Some((keyword_end, keyword)) = split_zero(data) {
                if keyword.starts_with(b"payload-") && data.len() >= keyword_end + 5 {
                    let part_index = std::str::from_utf8(&keyword[8..])?.parse::<usize>()?;
                    let mut cursor = keyword_end + 1;
                    let compressed = data[cursor] == 1;
                    cursor += 2;
                    cursor += find_zero_from(data, cursor)? + 1;
                    cursor += find_zero_from(data, cursor)? + 1;
                    let text = if compressed {
                        decode_zlib(&data[cursor..])?
                    } else {
                        data[cursor..].to_vec()
                    };
                    parts.push((part_index, text));
                }
            }
        }
        offset = end;
        if kind == b"IEND" {
            break;
        }
    }
    parts.sort_by_key(|part| part.0);
    if parts.is_empty() {
        return Err(anyhow!("no embedded payload found"));
    }
    let encoded = parts
        .into_iter()
        .flat_map(|part| part.1)
        .collect::<Vec<_>>();
    STANDARD
        .decode(encoded)
        .context("decode embedded PNG payload")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn embedded_payload_is_extracted_in_sequence() {
        let payload = (0..90_000)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let png = embed_payload_png(&payload, "test-seed");
        assert_eq!(extract_payload_png(&png).expect("extract payload"), payload);
        assert!(png.starts_with(b"\x89PNG\r\n\x1a\n"));
    }

    #[tokio::test]
    async fn rejects_non_cdn_download_targets_before_network_access() {
        for url in [
            "http://127.0.0.1/",
            "https://example.com/",
            "https://evil.tiktokcdn.com.example.com/",
            "https://user:pass@p16-ad-sg.tiktokcdn.com/",
        ] {
            assert!(download_public_png(url).await.is_err());
        }
    }
}

fn make_pattern_png(seed: &str) -> Vec<u8> {
    let width = 64u32;
    let height = 64u32;
    let mut ihdr = Vec::with_capacity(13);
    ihdr.extend_from_slice(&width.to_be_bytes());
    ihdr.extend_from_slice(&height.to_be_bytes());
    ihdr.extend_from_slice(&[8, 6, 0, 0, 0]);
    let mut raw = vec![0u8; (width as usize * 4 + 1) * height as usize];
    let seed_hash = blake3::hash(seed.as_bytes());
    let rgb = [
        seed_hash.as_bytes()[0] | 0x40,
        seed_hash.as_bytes()[1] | 0x40,
        seed_hash.as_bytes()[2] | 0x40,
    ];
    let mut state = u32::from_be_bytes(
        seed_hash.as_bytes()[0..4]
            .try_into()
            .unwrap_or([1, 2, 3, 4]),
    );
    for y in 0..height as usize {
        for x in 0..width as usize {
            let p = y * (width as usize * 4 + 1) + 1 + x * 4;
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            if state & 1 == 1 {
                raw[p..p + 4].copy_from_slice(&[rgb[0], rgb[1], rgb[2], 255]);
            }
        }
    }
    let mut png = b"\x89PNG\r\n\x1a\n".to_vec();
    png.extend_from_slice(&png_chunk(b"IHDR", &ihdr));
    let idat = encode_zlib(&raw).unwrap_or_default();
    png.extend_from_slice(&png_chunk(b"IDAT", &idat));
    png.extend_from_slice(&png_chunk(b"IEND", &[]));
    png
}

fn make_itxt(keyword: &str, text: &[u8]) -> Vec<u8> {
    let compressed = encode_zlib(text).unwrap_or_else(|_| text.to_vec());
    let mut data = keyword.as_bytes().to_vec();
    data.extend_from_slice(&[0, 1, 0, 0, 0]);
    data.extend_from_slice(&compressed);
    png_chunk(b"iTXt", &data)
}

fn png_chunk(kind: &[u8; 4], data: &[u8]) -> Vec<u8> {
    let mut chunk = Vec::with_capacity(data.len() + 12);
    chunk.extend_from_slice(&(data.len() as u32).to_be_bytes());
    chunk.extend_from_slice(kind);
    chunk.extend_from_slice(data);
    chunk.extend_from_slice(&crc32(&chunk[4..]).to_be_bytes());
    chunk
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in data {
        crc ^= *byte as u32;
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb88320 & (0u32.wrapping_sub(crc & 1)));
        }
    }
    !crc
}

fn split_zero(data: &[u8]) -> Option<(usize, &[u8])> {
    let end = data.iter().position(|byte| *byte == 0)?;
    Some((end, &data[..end]))
}
fn find_zero_from(data: &[u8], from: usize) -> Result<usize> {
    data.get(from..)
        .and_then(|slice| slice.iter().position(|byte| *byte == 0))
        .ok_or_else(|| anyhow!("malformed iTXt"))
}

fn encode_zlib(input: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    use std::io::Write;
    encoder.write_all(input)?;
    Ok(encoder.finish()?)
}

fn decode_zlib(input: &[u8]) -> Result<Vec<u8>> {
    let decoder = flate2::read::ZlibDecoder::new(input);
    let mut output = Vec::new();
    use std::io::Read;
    decoder.take(32 * 1024 + 1).read_to_end(&mut output)?;
    if output.len() > 32 * 1024 {
        return Err(anyhow!("PNG text chunk exceeds size limit"));
    }
    Ok(output)
}

#[derive(Deserialize)]
struct UploadResponse {
    data: UploadData,
}
#[derive(Deserialize)]
struct UploadData {
    image_info: ImageInfo,
}
#[derive(Deserialize)]
struct ImageInfo {
    web_uri: String,
}

#[derive(Deserialize)]
struct CreateResponse {
    code: i64,
}

impl TikTokBusinessUploader {
    pub fn new(org_id: String, cookie: String, csrf_token: Option<String>) -> Result<Self> {
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(90))
            .user_agent(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131 Safari/537.36",
            )
            .build()?;
        Ok(Self {
            client,
            org_id,
            cookie,
            csrf_token,
        })
    }

    pub async fn upload_png(&self, file_name: &str, png: Vec<u8>) -> Result<String> {
        let file_part = multipart::Part::bytes(png)
            .file_name(file_name.to_owned())
            .mime_str("image/png")?;
        let form = multipart::Form::new().part("Filedata", file_part);
        let mut request = self
            .client
            .post("https://business.tiktok.com/api/v3/bm/material/image/upload/")
            .query(&[("org_id", &self.org_id)])
            .header("cookie", &self.cookie)
            .header("origin", "https://business.tiktok.com")
            .header(
                "referer",
                format!(
                    "https://business.tiktok.com/manage/material/image?org_id={}",
                    self.org_id
                ),
            )
            .multipart(form);
        if let Some(token) = &self.csrf_token {
            request = request.header("x-csrftoken", token);
        }
        let response = request
            .send()
            .await
            .map_err(|_| anyhow!("TikTok image upload request failed"))?;
        let status = response.status();
        let text = response.text().await?;
        if status != StatusCode::OK {
            return Err(anyhow!("TikTok upload returned {status}"));
        }
        let parsed: UploadResponse =
            serde_json::from_str(&text).context("parse TikTok upload response")?;
        let image_url = parsed.data.image_info.web_uri;
        self.register_image(&image_url, file_name).await?;
        Ok(image_url)
    }

    pub async fn upload_path(&self, file_name: &str, path: &std::path::Path) -> Result<String> {
        let file = tokio::fs::File::open(path).await?;
        let stream =
            tokio_util::io::ReaderStream::new(file).map(|item| item.map_err(std::io::Error::other));
        let body = reqwest::Body::wrap_stream(stream);
        let part = multipart::Part::stream(body)
            .file_name(file_name.to_owned())
            .mime_str("image/png")?;
        let form = multipart::Form::new().part("Filedata", part);
        let mut request = self
            .client
            .post("https://business.tiktok.com/api/v3/bm/material/image/upload/")
            .query(&[("org_id", &self.org_id)])
            .header("cookie", &self.cookie)
            .header("origin", "https://business.tiktok.com")
            .header(
                "referer",
                format!(
                    "https://business.tiktok.com/manage/material/image?org_id={}",
                    self.org_id
                ),
            )
            .multipart(form);
        if let Some(token) = &self.csrf_token {
            request = request.header("x-csrftoken", token);
        }
        let response = request
            .send()
            .await
            .map_err(|_| anyhow!("TikTok image upload request failed"))?;
        let status = response.status();
        let text = response.text().await?;
        if status != StatusCode::OK {
            return Err(anyhow!("TikTok upload returned {status}"));
        }
        let parsed: UploadResponse =
            serde_json::from_str(&text).context("parse TikTok upload response")?;
        let image_url = parsed.data.image_info.web_uri;
        self.register_image(&image_url, file_name).await?;
        Ok(image_url)
    }

    async fn register_image(&self, image_url: &str, name: &str) -> Result<()> {
        let body = serde_json::json!({
            "web_uri": image_url,
            "original_web_uri": image_url,
            "name": name,
            "show_error": false,
        });
        let mut request = self
            .client
            .post("https://business.tiktok.com/api/v3/bm/material/image/create/")
            .query(&[("org_id", &self.org_id)])
            .header("cookie", &self.cookie)
            .header("origin", "https://business.tiktok.com")
            .header(
                "referer",
                format!(
                    "https://business.tiktok.com/manage/material/image?org_id={}",
                    self.org_id
                ),
            )
            .json(&body);
        if let Some(token) = &self.csrf_token {
            request = request.header("x-csrftoken", token);
        }
        let response = request
            .send()
            .await
            .map_err(|_| anyhow!("TikTok image registration request failed"))?;
        let status = response.status();
        let text = response.text().await?;
        if status != StatusCode::OK {
            return Err(anyhow!("TikTok image registration returned {status}"));
        }
        let data: CreateResponse = serde_json::from_str(&text)
            .map_err(|_| anyhow!("invalid TikTok registration response"))?;
        if data.code != 0 {
            return Err(anyhow!(
                "TikTok image registration rejected (code {})",
                data.code
            ));
        }
        Ok(())
    }
}
