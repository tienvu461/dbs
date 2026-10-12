# backupd — stateless backup library

File or URL → bounded chunks → compress/encrypt → upload/read-back verify → portable JSON manifest. Restore needs the manifest, the original key and access to the referenced objects. No SQLite, service process, catalog or staging directory is needed on this path.

## Components

- `backupd-lib` (`crates/engine`): runtime-independent upload/restore library; native and WASM compatible.
- `backupctl` (`crates/backupctl`): one-shot file/stdin/HTTPS uploader and manifest-driven restore. Filesystem, S3/R2/Garage, and experimental TikTok destinations.
- `backupd-cloudflare` (`crates/cloudflare`): Rust Worker consuming R2 upload events through Queues, writing the returned manifest to a designated R2 bucket.
- The original SQLite/Axum service is retained as a **legacy implementation**, explicitly invoked using `cargo run -p backupd`. Its unresolved review findings remain in [SECURITY.md](SECURITY.md). It is not the default container command or the new library's execution path.

## Native upload and restore

Set `BACKUP_MASTER_KEY` to a securely generated 32-byte key encoded as 64 hex characters. Keep it separately from the returned metadata. `--key-id` is a nonsecret label for finding the right key later.

```sh
cargo run -p backupctl -- upload ./backup.dump --manifest ./backup.json
cargo run -p backupctl -- restore ./backup.json --output ./restored.dump
cmp ./backup.dump ./restored.dump

# URL input (HTTPS; public IPs only by default; redirects rejected)
cargo run -p backupctl -- upload 'https://exports.example.com/backup.dump' \
  --filename backup.dump --manifest ./remote-backup.json

# stdin input
pg_dump example | cargo run -p backupctl -- upload - \
  --filename database.dump --manifest ./database.json
```

Output files must not already exist. Restore writes to a temporary file and publishes it only after all checks pass. Source URLs, including their query tokens, are not included in manifests. For a trusted private HTTP export server, explicitly use `--allow-private-source`.

### S3, Cloudflare R2, or Garage

Set `S3_ENDPOINT_URL`, `S3_BUCKET`, `AWS_REGION`, and appropriate credentials (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, or the supported AWS SDK credential chain). `S3_PREFIX` defaults to `backupd`.

```sh
cargo run -p backupctl -- --store s3 upload ./backup.dump --manifest ./backup.json
cargo run -p backupctl -- --store s3 restore ./backup.json --output ./restored.dump
```

R2 uses `https://<account-id>.r2.cloudflarestorage.com` and region `auto`. AWS uses its regional S3 endpoint and actual region. Garage uses its S3 endpoint and configured region. The CLI defaults region to `auto`; set `AWS_REGION` explicitly for AWS/Garage.

### TikTok CDN (experimental)

Set `TIKTOK_ORG_ID`, `TIKTOK_COOKIE`, and optionally `TIKTOK_CSRF_TOKEN`. These are Business Center browser-session credentials, not an official storage API token. `TIKTOK_CDN_ORIGIN` defaults to `https://p16-ad-sg.tiktokcdn.com/origin/`.

```sh
cargo run -p backupctl -- --store tiktok upload ./backup.dump --manifest ./backup.json
cargo run -p backupctl -- --store tiktok restore ./backup.json --output ./restored.dump
```

Chunks are encrypted before PNG wrapping. The upload path performs anonymous CDN GET, extracts the PNG payload and checks the stored hash before returning metadata. Restore does not require the upload cookie, but does require the encryption key. No live TikTok verification has been performed without account credentials. Endpoints and CDN behavior are undocumented and may change.

## Cloudflare deployment

See [docs/CLOUDFLARE.md](docs/CLOUDFLARE.md) for R2 buckets, notification rule, Queues, Wrangler and secrets. R2 events identify private objects by bucket/key/ETag; the consumer reads through bindings rather than requiring a public source URL.

```text
R2 input object-create → Queue → Rust Worker → destination chunks
                                         → manifest R2 bucket
```

The first metadata-delivery channel is an R2 manifest bucket. Email/Slack delivery is intentionally deferred. The consumer acknowledges only after the manifest is persisted and read back successfully.

## Container

Pushes to `main` publish `ghcr.io/tienvu461/dbs` for amd64/arm64 with `latest` (when main is the default branch) and `sha-<commit>` tags. The container now runs `backupctl` and exits when the requested operation finishes:

```sh
docker run --rm -e BACKUP_MASTER_KEY -v "$PWD:/data" \
  ghcr.io/tienvu461/dbs --root /data/blobs \
  upload /data/backup.dump --manifest /data/backup.json
```

The image uses UID/GID 10001. Mounted input/output directories must have appropriate permissions. There is no HTTP port or SQLite volume requirement for the new command. Provider secrets are runtime-only.

## Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md): library contracts, metadata and lifecycle.
- [docs/STATELESS.md](docs/STATELESS.md): embedding, compatibility and ownership.
- [docs/CLOUDFLARE.md](docs/CLOUDFLARE.md): Worker setup and limits.
- [docs/OPERATIONS.md](docs/OPERATIONS.md): legacy server runbook.
- [SECURITY.md](SECURITY.md): secret scanning and review findings.
- [AGENTS.md](AGENTS.md): development conventions.

Keep manifests and encryption keys backed up: statelessness transfers catalog/retention/recovery responsibility to the caller. Interrupted attempts can leave unreferenced uploaded objects. Automated GC is not part of the library.
