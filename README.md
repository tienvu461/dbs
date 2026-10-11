# backupd

Standalone Rust backup ingestion and restore service. SQLite persists backup metadata and the durable worker queue. Storage can be local filesystem or S3-compatible object storage (AWS S3, Cloudflare R2, or Garage).

Project documentation:

- [`ARCHITECTURE.md`](ARCHITECTURE.md) — implementation structure, data flow, invariants, and known gaps.
- [`docs/OPERATIONS.md`](docs/OPERATIONS.md) — deployment configuration, smoke checks, recovery, and maintenance runbooks.
- [`AGENTS.md`](AGENTS.md) — repository conventions and change verification guidance.
- [`SECURITY.md`](SECURITY.md) — secret scanning, pre-commit setup, runtime authentication, and unresolved review findings.

## Run

Install a current stable Rust toolchain, then:

```sh
cargo run -p backupd
```

## Container image

Pushes to `main` build and publish multi-platform images to GitHub Container Registry:

```sh
docker pull ghcr.io/<owner>/backupd:latest
```

Each push also publishes an immutable `sha-<short-commit>` tag. The package is published using the workflow's `GITHUB_TOKEN` and is private by default until its visibility is changed in the GitHub Packages settings.

The service binds to `127.0.0.1:8080` by default. Override with `BACKUPD_BIND`, `DATABASE_URL`, and `BACKUPD_STORAGE_ROOT`. Set `BACKUPD_API_TOKEN` to a random token of at least 32 characters to authenticate `/v1/` requests with `Authorization: Bearer <token>`; this is required for non-loopback binds and container deployment. Set `BACKUP_MASTER_KEY` to exactly 32 bytes encoded as 64 hexadecimal characters to enable authenticated encryption. When TikTok publishing is enabled, the service refuses to proceed without this key.

### S3, R2, and Garage

Set `BACKUPD_TRANSPORT=s3`, `S3_ENDPOINT_URL`, `S3_BUCKET`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `S3_PREFIX`. IAM/workload credentials may be used by omitting the two static key variables.

- AWS S3: endpoint `https://s3.<region>.amazonaws.com`, region e.g. `ap-southeast-1`.
- Cloudflare R2: endpoint `https://<ACCOUNT_ID>.r2.cloudflarestorage.com`, region `auto`; create an R2 API token with Object Read & Write access scoped to the target bucket. Use its Access Key ID and Secret Access Key.
- Garage: set the endpoint to the Garage S3 API endpoint, region to the configured Garage region, and use a Garage access key/secret with read/write/delete permissions on the bucket.

The implementation uses path-style addressing, basic PutObject/GetObject/HeadObject/DeleteObject, and verifies object length after upload. For R2 region must be `auto`. R2’s docs list these object operations as implemented: https://developers.cloudflare.com/r2/api/s3/api/.

### Durable upload jobs

`POST /v1/backups/upload` streams the multipart file to a staging file, persists a queued backup and `ingest_file` job, then returns HTTP 202 with `id` and `job_id`. A Tokio worker pool claims jobs transactionally in SQLite, retries failures with backoff, recovers stale `running` locks on restart, and removes staged inputs after successful completion. Poll `GET /v1/jobs/{job_id}` and `GET /v1/backups/{backup_id}`. Keep `BACKUPD_STAGING_ROOT` on persistent disk; staged data must survive process/container restarts.

Multipart fields supported: `file`, optional `name`, and optional `tags` as a JSON string map. Put metadata fields before the file part.

### TikTok Business CDN (experimental)

The implementation follows https://github.com/thanhan7914/free-cdn-stream: encrypted backup chunks are wrapped in PNG `iTXt` payloads, uploaded as Business image materials, and registered through TikTok Business endpoints. Provide:

```sh
TIKTOK_ORG_ID=<Business Center organization ID>
TIKTOK_COOKIE='<authenticated business.tiktok.com browser session cookie>'
TIKTOK_CSRF_TOKEN=<current x-csrftoken value, if your account requires it>
TIKTOK_CDN_ORIGIN=https://p16-ad-sg.tiktokcdn.com/origin/
BACKUP_MASTER_KEY=<64 hex chars; required for TikTok publishing>
```

`TIKTOK_COOKIE` is a sensitive logged-in browser session credential, not an API key. Do not commit it or send it in chat. Obtain `org_id` from the Business Center material-management page and capture the cookie/CSRF token from your own authenticated browser session. TikTok may expire sessions or change these undocumented endpoints. Anonymous download is expected to work for returned `web_uri`s but has not been verified here. `GET /v1/backups/{id}/tiktok` returns a JSON index of CDN objects. Each payload is the XChaCha20-Poly1305 ciphertext (including its 24-byte nonce), with the zstd-compressed chunk encoded using the reference PNG iTXt/base64 wrapper. An authorized holder of `BACKUP_MASTER_KEY` can restore using the index and manifest metadata; normal `GET /v1/backups/{id}/download` remains catalog-driven and verifies/decrypts chunks itself.

TikTok uploads are disabled unless both `TIKTOK_ORG_ID` and `TIKTOK_COOKIE` are configured. This integration uses private web UI endpoints, not a supported public API. Validate TikTok’s terms and bandwidth rules before use.

## API

- `GET /health/live`
- `GET /health/ready`
- `POST /v1/backups/upload` — multipart fields `file`, `name` (optional, before `file`), `tags` (optional JSON object, before `file`)
- `GET /v1/backups`
- `GET /v1/backups/{id}`
- `GET /v1/backups/{id}/download`
- `DELETE /v1/backups/{id}`

Uploads are streamed from the multipart parser into a staging file, then processed in fixed-size chunks by a durable background job. Choose a conservative request limit with `BACKUPD_MAX_UPLOAD_BYTES`. Backups become complete only after the manifest and catalog transaction commit. Filesystem blobs are content-addressed by the stored representation hash.
