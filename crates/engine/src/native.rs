//! Optional native file/stdin/URL inputs. URLs never appear in manifest metadata.
use crate::{BlobStore, Error, Manifest, Result, Uploader};
use futures::{io::AsyncRead, TryStreamExt};
use std::net::{IpAddr, SocketAddr};
use tokio_util::compat::TokioAsyncReadCompatExt;

pub async fn open_source(
    location: &str,
    allow_private: bool,
) -> Result<Box<dyn AsyncRead + Unpin>> {
    if location == "-" {
        return Ok(Box::new(tokio::io::stdin().compat()));
    }
    if !location.starts_with("http://") && !location.starts_with("https://") {
        return Ok(Box::new(tokio::fs::File::open(location).await?.compat()));
    }
    let url: reqwest::Url = location.parse().map_err(|_| Error::Invalid)?;
    if (!allow_private && url.scheme() != "https")
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(Error::Invalid);
    }
    let host = url.host_str().ok_or(Error::Invalid)?;
    let addresses: Vec<SocketAddr> =
        tokio::net::lookup_host((host, url.port_or_known_default().unwrap_or(443)))
            .await
            .map_err(|_| Error::Transport)?
            .collect();
    if addresses.is_empty()
        || (!allow_private && addresses.iter().any(|address| !public_ip(address.ip())))
    {
        return Err(Error::Invalid);
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(3600))
        .connect_timeout(std::time::Duration::from_secs(15))
        .resolve_to_addrs(host, &addresses)
        .build()
        .map_err(|_| Error::Transport)?;
    let response = client.get(url).send().await.map_err(|_| Error::Transport)?;
    if !response.status().is_success() {
        return Err(Error::Transport);
    }
    Ok(Box::new(
        response
            .bytes_stream()
            .map_err(|_| std::io::Error::other("source read failed"))
            .into_async_read(),
    ))
}

/// Native library entry point for a local file, stdin or HTTPS link.
pub async fn upload_source<S: BlobStore + ?Sized>(
    engine: &Uploader,
    location: &str,
    filename: String,
    store: &S,
    allow_private: bool,
) -> Result<Manifest> {
    engine
        .upload(open_source(location, allow_private).await?, filename, store)
        .await
}

fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_broadcast()
                && !ip.is_documentation()
                && !ip.is_multicast()
                && !ip.is_unspecified()
                && ip.octets()[0] != 0
                && ip.octets()[0] < 224
                && !(ip.octets()[0] == 100 && (64..=127).contains(&ip.octets()[1]))
                && !(ip.octets()[0] == 198 && (18..=19).contains(&ip.octets()[1]))
        }
        IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .map(|v4| public_ip(v4.into()))
            .unwrap_or_else(|| {
                (ip.segments()[0] & 0xe000) == 0x2000
                    && !(ip.segments()[0] == 0x2001 && ip.segments()[1] == 0xdb8)
            }),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_private_reserved_and_mapped_addresses() {
        for address in [
            "127.0.0.1",
            "10.1.2.3",
            "169.254.169.254",
            "100.64.0.1",
            "::1",
            "::ffff:127.0.0.1",
            "fc00::1",
        ] {
            assert!(!public_ip(address.parse().unwrap()));
        }
        assert!(public_ip("1.1.1.1".parse().unwrap()));
    }
    #[tokio::test]
    async fn private_http_requires_explicit_opt_in() {
        assert!(open_source("http://127.0.0.1/test", false).await.is_err());
        assert!(open_source("https://user:password@example.com/test", false)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn trusted_local_url_reader_streams_response() {
        use futures::io::AsyncReadExt;
        use tokio::io::{AsyncReadExt as TokioRead, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 2048];
            socket.read(&mut request).await.unwrap();
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\nbackup-data").await.unwrap();
        });
        let mut reader = open_source(&format!("http://{address}/export"), true)
            .await
            .unwrap();
        let mut result = Vec::new();
        reader.read_to_end(&mut result).await.unwrap();
        assert_eq!(result, b"backup-data");
        server.await.unwrap();
    }
}
