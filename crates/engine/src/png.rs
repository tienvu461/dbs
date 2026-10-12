//! PNG iTXt envelope compatible with the reference payload wrapper.
use crate::{Error, Result, MAX_STORED_BYTES};
use base64::{engine::general_purpose::STANDARD, Engine};
use std::io::{Read, Write};
const SIGNATURE: &[u8] = b"\x89PNG\r\n\x1a\n";
pub const MAX_PNG_BYTES: usize = 16 * 1024 * 1024;

pub fn encode(payload: &[u8]) -> Result<Vec<u8>> {
    if payload.is_empty() || payload.len() > MAX_STORED_BYTES {
        return Err(Error::Limit);
    }
    let mut png = SIGNATURE.to_vec();
    let mut ihdr = Vec::new();
    ihdr.extend_from_slice(&64u32.to_be_bytes());
    ihdr.extend_from_slice(&64u32.to_be_bytes());
    ihdr.extend_from_slice(&[8, 6, 0, 0, 0]);
    append(&mut png, b"IHDR", &ihdr);
    let hash = blake3::hash(payload);
    let mut raw = vec![0u8; (64 * 4 + 1) * 64];
    for y in 0..64 {
        for x in 0..64 {
            let p = y * 257 + 1 + x * 4;
            raw[p..p + 4].copy_from_slice(&[
                hash.as_bytes()[x % 32],
                hash.as_bytes()[y % 32],
                128,
                255,
            ]);
        }
    }
    append(&mut png, b"IDAT", &compress(&raw)?);
    for (index, part) in STANDARD
        .encode(payload)
        .as_bytes()
        .chunks(32 * 1024)
        .enumerate()
    {
        let mut data = format!("payload-{:04}", index + 1).into_bytes();
        data.extend_from_slice(&[0, 1, 0, 0, 0]);
        data.extend_from_slice(&compress(part)?);
        append(&mut png, b"iTXt", &data);
    }
    append(&mut png, b"IEND", &[]);
    Ok(png)
}
pub fn decode(png: &[u8]) -> Result<Vec<u8>> {
    if png.len() > MAX_PNG_BYTES || !png.starts_with(SIGNATURE) {
        return Err(Error::Invalid);
    }
    let mut offset = 8;
    let mut parts = std::collections::BTreeMap::new();
    let mut total = 0;
    let mut ended = false;
    while offset + 12 <= png.len() {
        let size = u32::from_be_bytes(
            png[offset..offset + 4]
                .try_into()
                .map_err(|_| Error::Invalid)?,
        ) as usize;
        let end = offset.checked_add(size + 12).ok_or(Error::Invalid)?;
        if end > png.len() {
            return Err(Error::Invalid);
        }
        let kind = &png[offset + 4..offset + 8];
        let data = &png[offset + 8..end - 4];
        let expected =
            u32::from_be_bytes(png[end - 4..end].try_into().map_err(|_| Error::Invalid)?);
        if crc(&png[offset + 4..end - 4]) != expected {
            return Err(Error::Integrity);
        }
        if kind == b"iTXt" {
            let nul = data.iter().position(|b| *b == 0).ok_or(Error::Invalid)?;
            let keyword = std::str::from_utf8(&data[..nul]).map_err(|_| Error::Invalid)?;
            if let Some(number) = keyword.strip_prefix("payload-") {
                let index: usize = number.parse().map_err(|_| Error::Invalid)?;
                if data.get(nul..nul + 5) != Some(&[0, 1, 0, 0, 0][..])
                    || index == 0
                    || parts.contains_key(&index)
                {
                    return Err(Error::Invalid);
                }
                let mut text = Vec::new();
                flate2::read::ZlibDecoder::new(&data[nul + 5..])
                    .take(32 * 1024 + 1)
                    .read_to_end(&mut text)?;
                if text.len() > 32 * 1024 {
                    return Err(Error::Limit);
                }
                total += text.len();
                if total > MAX_STORED_BYTES.div_ceil(3) * 4 {
                    return Err(Error::Limit);
                }
                parts.insert(index, text);
            }
        }
        offset = end;
        if kind == b"IEND" {
            ended = true;
            break;
        }
    }
    if !ended || parts.is_empty() || offset != png.len() {
        return Err(Error::Invalid);
    }
    let mut text = Vec::with_capacity(total);
    for (expected, (index, data)) in parts.into_iter().enumerate() {
        if index != expected + 1 {
            return Err(Error::Invalid);
        }
        text.extend(data);
    }
    let output = STANDARD.decode(text).map_err(|_| Error::Invalid)?;
    if output.len() > MAX_STORED_BYTES {
        return Err(Error::Limit);
    }
    Ok(output)
}
fn compress(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes)?;
    Ok(encoder.finish()?)
}
fn append(output: &mut Vec<u8>, kind: &[u8; 4], data: &[u8]) {
    output.extend_from_slice(&(data.len() as u32).to_be_bytes());
    let start = output.len();
    output.extend_from_slice(kind);
    output.extend_from_slice(data);
    let checksum = crc(&output[start..]);
    output.extend_from_slice(&checksum.to_be_bytes());
}
fn crc(bytes: &[u8]) -> u32 {
    let mut c = !0u32;
    for b in bytes {
        c ^= *b as u32;
        for _ in 0..8 {
            c = (c >> 1) ^ (0xedb88320 & (0u32.wrapping_sub(c & 1)));
        }
    }
    !c
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip_and_crc_validation() {
        let bytes = (0..90_000).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        let mut png = encode(&bytes).unwrap();
        assert_eq!(decode(&png).unwrap(), bytes);
        png[20] ^= 1;
        assert!(decode(&png).is_err());
    }
}
