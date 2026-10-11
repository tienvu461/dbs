# backupd architecture

## Purpose and current scope

`backupd` accepts uploaded backup files, stages them on disk, queues ingestion in SQLite, transforms bytes into immutable encrypted/compressed chunks, stores them in a configured blob transport, catalogs a portable manifest, and later reconstructs the original bytes.

Current implementation includes:

- Axum HTTP API and Tokio service runtime.
- SQLite catalog, durable ingestion queue, and delayed garbage-collection queue.
- Fixed-size chunking (default 64 MiB), zstd compression, BLAKE3 integrity hashes, and optional XChaCha20-Poly1305 encryption.
- Filesystem and S3-compatible transports (AWS S3, Cloudflare R2, Garage through custom endpoint/region/credentials).
- Optional experimental TikTok Business image publishing through private web endpoints.

Some target architecture features are not present yet: PostgreSQL catalog, URL ingestion, `backupctl`, concurrent chunk uploads, provider fallback/replication policy, general resume from intermediate chunks, and true streaming restore responses.

## Crate responsibilities

```text
HTTP client
   |
   v
crates/backupd  -- multipart staging, API, health, application wiring
   |                         |
   | enqueue                 | HTTP download / metadata
   v                         v
crates/catalog <----> crates/worker
   |                     |
   |                     +--> crates/pipeline
   |                     +--> crates/transport
   v
SQLite catalog + jobs + GC queue
```

- **`backupd-core`** owns serializable domain records: `BackupRecord`, `ChunkManifest`, `ArtifactManifest`, and `BackupManifest`.
- **`backupd-catalog`** owns SQL schema and transactional persistence. Chunk identities are stored in `blobs`; `artifact_blobs` preserves ordered references; `blob_locations` maps logical blob IDs to one or more provider locators. `jobs` is the durable work queue and `gc_queue` retries physical object deletion.
- **`backupd-pipeline`** reads the input in fixed-size chunks. For each chunk it hashes plaintext, zstd-compresses, optionally encrypts, computes a stored hash, and calls its async chunk sink. Decode validates stored hash, decrypts, decompresses, and validates plaintext hash/size.
- **`backupd-transport`** provides `BlobTransport` with `put/get/head/delete/locator_for/raw_locator`. Filesystem stores blobs atomically in local files. `S3CompatibleTransport` uses the AWS SDK with a configured endpoint and path-style addressing; it is the same API for AWS, R2 and Garage.
- **`backupd-worker`** claims queued tasks, processes staged input, persists completion, retries errors with exponential delay, periodically requeues stale job locks, and runs GC tasks.
- **`backupd-source`** currently holds local-file/async-reader abstractions; the HTTP endpoint stages multipart data itself.

## Ingestion flow

1. `POST /v1/backups/upload` consumes a multipart request and writes the file part incrementally to `BACKUPD_STAGING_ROOT` (default `data/staging`). The upload is staged to disk before processing; it is not piped directly from the HTTP request into the chunk pipeline.
2. The API inserts a queued backup record and an `ingest_file` job in SQLite, then returns `202 Accepted` with backup ID and job ID.
3. A Tokio worker claims a queued job in a database transaction and marks it running with a lock owner and timestamp.
4. The worker processes chunks serially through the pipeline and uploads them. Object keys use a `chunks/` prefix plus the stored BLAKE3 digest. The catalog records ordered references and provider locators.
5. Once all chunks are uploaded and checked, the worker writes the complete manifest and record in a transaction, then removes the staged file.
6. Failures are retried with backoff; after max attempts the job and backup remain failed/observable. Stale running locks are requeued periodically.

The handler writes multipart data incrementally to a staging file before enqueueing, rather than keeping the entire upload in a `Vec`. If the client disconnects before job creation, an orphan staging file can remain; periodic staging-orphan cleanup is a maintenance gap. The worker streams the staged file into fixed-size pipeline chunks.

The API exposes `GET /v1/jobs/{job_id}` and backup list/get routes for polling.

## Chunk representation and identity

```text
plaintext chunk
  -> plaintext BLAKE3 + size
  -> zstd compression
  -> optional XChaCha20-Poly1305 encryption (nonce prepended)
  -> stored BLAKE3 (content key)
  -> transport object
```

The manifest's `stored_hash` is the content identity. Its `blob_id` is currently populated with a physical transport locator (`fs://...` or `s3://...`); the catalog `blobs` row is keyed by stored hash and `blob_locations` maps that key to the locator. Keep this mapping clear when changing storage. The backup ID and portable manifest remain independent from provider identity.

Encryption is optional in the general local/S3 flow. `BACKUP_MASTER_KEY` is decoded from 64 hexadecimal characters into 32 bytes. Keep this secret external to the database and backup files. The implementation does not yet provide key rotation, per-backup envelope key wrapping, or KMS integration.

## Durable jobs and recovery

`jobs` tracks job type, JSON payload, state, attempt count, availability, lock owner/time, and last error. `claim_job` selects an eligible queued row and updates it transactionally. The worker resets stale locks (10-minute cutoff; checks once per minute). Job processing must remain idempotent across at-least-once execution: content-addressed writes should tolerate a prior attempt, and a completed record should not be recreated.

Chunk offsets/states are not checkpointed yet. A reclaimed upload job rereads its staged file from the beginning and reuses/verifies content-addressed objects written by an earlier attempt.

The `gc_queue` stores physical object deletion work. Backup deletion removes references first and queues unreferenced transport locations. Workers retry failed deletion after a delay. Keep the GC queue distinct from ingestion jobs so object deletion failures never roll back catalog backup deletion.

## TikTok CDN (experimental)

The optional flow is based on `thanhan7914/free-cdn-stream`:

1. Re-encrypt the zstd-compressed chunk with XChaCha20-Poly1305 using a new nonce for the CDN copy.
2. Base64-encode that ciphertext and store it in ordered compressed PNG iTXt text chunks (`payload-0001`, ...).
3. Upload the PNG to `business.tiktok.com/api/v3/bm/material/image/upload/?org_id=...` as multipart field `Filedata`.
4. Register the returned `web_uri` with `/api/v3/bm/material/image/create/?org_id=...`.
5. Store public URLs and per-chunk nonces in a TikTok JSON artifact.

The browser/session cookie and CSRF token are sensitive credentials. These endpoints are undocumented private web APIs; anonymous CDN GET, returned URL semantics, service-specific size limits, image optimization/transcoding, and long-term durability are not guaranteed. A PNG extract/encode unit test verifies our own codec only; it is not a TikTok integration test. The CDN copy is re-encrypted with a fresh random nonce, so the CDN index records that nonce; the current normal backup download path still reads the authoritative blob location rather than consuming the CDN index. Never consider TikTok-only copies authoritative. The authoritative S3-compatible copy and the catalog manifest remain the restore source of record.

## Container and runtime

The multi-stage `Dockerfile` builds a release binary and runs it as UID 10001. `/data` is the writable volume containing SQLite, blob files (filesystem transport), and staged uploads. GitHub Actions builds `linux/amd64` and `linux/arm64` and publishes `ghcr.io/<owner>/<repo>:latest` and `sha-<short-commit>` on pushes to `main`.

## Main interfaces

| Method/path | Behavior |
|---|---|
| `GET /health/live` | Process liveness |
| `GET /health/ready` | SQLite connectivity |
| `POST /v1/backups/upload` | Stage multipart file and enqueue job (`202`) |
| `GET /v1/backups` | List backups |
| `GET /v1/backups/{id}` | Backup record |
| `GET /v1/jobs/{id}` | Durable job state |
| `GET /v1/backups/{id}/download` | Restore through configured primary transport and verify |
| `GET /v1/backups/{id}/tiktok` | TikTok CDN index JSON when configured |
| `DELETE /v1/backups/{id}` | Remove backup refs and enqueue GC |

## Key invariants

- A backup is not `complete` until all referenced chunks have an uploaded, verified location.
- Stored bytes and reconstructed plaintext are both verified before data is trusted.
- Provider locators remain behind the transport abstraction and are not backup identity.
- A deleted backup must not delete a blob still referenced by another backup.
- A worker restart must not require the client to resubmit the upload; staging data and jobs live on persistent disk/SQLite.
- Public CDN copies require encryption and are optional. Failure in that integration must not silently publish plaintext.
