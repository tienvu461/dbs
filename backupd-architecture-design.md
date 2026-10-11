# backupd — Standalone Rust Backup Gateway

## 1. Overview

`backupd` is a standalone backup ingestion, storage, catalog, and restore service written in Rust.

Its responsibility is intentionally narrow:

> Accept a backup file, URL, or byte stream → package it into durable chunks → compress, encrypt, hash, and upload those chunks to one or more pluggable blob transports → persist a stable backup record and manifest → support later query and reconstruction of the original backup.

The design targets a hybrid backup strategy such as:

- Periodic authoritative full backups to trusted storage such as S3.
- More frequent differential/content-addressed backups to a lower-cost or opportunistic secondary transport.
- Queryable backup metadata stored independently from the physical blob location.
- Restore through a stable `backup_id`, not through raw CDN URLs.

The primary architectural principle is:

> `backupd` owns backup identity, manifests, encryption, checksums, restore logic, and consistency. Remote storage systems are treated as dumb blob stores.

---

## 2. Goals

### Primary goals

- Standalone service, deployable as a single binary or container.
- Written in Rust.
- Accept backup input from:
  - Local file upload.
  - Remote HTTP/HTTPS URL.
  - stdin/stream through a CLI client.
- Store backup data through pluggable transports.
- Return a stable backup record after ingestion.
- Persist metadata in SQLite by default, with optional PostgreSQL support.
- Support later listing, querying, downloading, and restoring backups.
- Stream large files without loading them fully into memory.
- Resume interrupted uploads.
- Verify uploaded data before marking it durable.
- Support encryption before sending data to untrusted storage.
- Support multiple physical locations for the same logical blob.
- Allow transport replacement without changing the backup format.

### Secondary goals

- Content-addressed deduplication.
- Differential behavior across daily backups.
- Parallel uploads and restores with bounded concurrency.
- Restore fallback across multiple transports.
- Periodic restore testing.
- Portable manifest independent of the database.

### Non-goals

- Database-specific backup creation logic such as running `pg_dump` internally.
- Client asset delivery.
- HLS/video streaming.
- Browser playback.
- Application CDN behavior.
- Reliance on any single third-party provider-specific storage mechanism.

`backupd` stores and reconstructs backup bytes; external systems create those backup bytes.

---

## 3. High-Level Architecture

```text
                         Backup Producers

        PostgreSQL      Filesystem      Remote Export
            |                |               |
         pg_dump            tar           HTTPS URL
            |                |               |
            +----------------+---------------+
                             |
                             v
                  +-----------------------+
                  |        backupd        |
                  |                       |
                  |  HTTP API             |
                  |  Worker Pool          |
                  |  Catalog              |
                  |  Restore Engine       |
                  +-----------+-----------+
                              |
                     Streaming Pipeline
                              |
              +---------------+---------------+
              |               |               |
            Chunk          Compress        Encrypt
              |               |               |
              +---------------+---------------+
                              |
                              v
                            Hash
                              |
                         Dedup Lookup
                              |
                              v
                      Transport Manager
                       /             \
                      v               v
             Authoritative       Secondary
                 S3              Blob Transport
                      \             /
                       \           /
                        +----+-----+
                             |
                        Remote Blobs
                             |
                             v
                          Catalog
                     SQLite / Postgres
                             |
                             v
                      Portable Manifest
```

---

## 4. Backup Strategy

A recommended deployment policy is:

```text
Sunday
  FULL backup
    -> S3 / trusted storage

Monday-Saturday
  content-addressed or differential backup
    -> secondary transport
```

The full backup is authoritative.

The daily backup is optimized for lower storage cost and faster incremental transfer.

For secondary storage that may be less durable than S3, avoid strict chained incrementals where every day's backup depends on all previous daily backups.

Preferred models:

1. Content-addressed chunk manifests.
2. Differential backup relative to the latest trusted full backup.

### Why not chained incrementals?

A chain such as:

```text
FULL + INC1 + INC2 + INC3 + INC4
```

means loss of `INC2` may invalidate all later restore points.

A content-addressed manifest is more robust:

```text
Backup A -> [A, B, C, D]
Backup B -> [A, B, X, D]
```

Only the new chunk `X` needs uploading.

---

## 5. Core Domain Model

The service exposes three primary logical concepts.

### Backup

Represents one backup operation.

```text
Backup
  id
  name
  source
  created_at
  completed_at
  status
  metadata
```

### Artifact

Represents an individual logical file inside a backup.

For the first implementation, one Backup may contain one Artifact. The data model should still support multiple artifacts from the start.

```text
Backup
  +-- Artifact: postgres.dump
  +-- Artifact: uploads.tar.zst
```

### Blob

Represents one immutable content chunk.

```text
Artifact
  +-- Blob 0
  +-- Blob 1
  +-- Blob 2
```

A Blob may exist in multiple physical locations.

```text
Blob abc
  +-- S3
  +-- R2
  +-- Secondary Transport
```

The key rule is:

```text
remote URL != backup identity
```

A remote URL is only a locator for one physical copy of a logical blob.

---

## 6. Stable Identity Model

### Backup ID

Use ULIDs.

Example:

```text
01K7Q1XSCXMR32ZJ4KJ82QDGK2
```

Benefits:

- Globally unique.
- Time-sortable.
- URL-safe.
- Works well in SQLite and PostgreSQL.

### Blob ID

Use a content hash.

Recommended:

```text
blake3:<digest>
```

or:

```text
sha256:<digest>
```

BLAKE3 is attractive for internal use because it is fast and supports streaming naturally.

Store SHA-256 as an optional interoperability checksum if desired.

---

## 7. Input Model

The service supports multiple source types.

### Remote URL

```http
POST /v1/backups
Content-Type: application/json
```

```json
{
  "source": {
    "type": "url",
    "url": "https://internal.example.com/backups/prod.dump"
  },
  "name": "production-db",
  "policy": "daily",
  "tags": {
    "environment": "production",
    "type": "database"
  }
}
```

### Multipart upload

```http
POST /v1/backups/upload
Content-Type: multipart/form-data
```

```text
file=@backup.tar.zst
name=production-files
policy=daily
```

### CLI stream

```bash
pg_dump ... | backupctl upload - \
  --name prod-db \
  --policy daily
```

### Internal source abstraction

```rust
pub enum BackupSource {
    LocalFile(PathBuf),
    RemoteUrl(Url),
    Stream(ByteStream),
}
```

All source types must converge into the same streaming pipeline.

---

## 8. Ingestion Pipeline

```text
POST /backups
      |
      v
Create Backup record
status = QUEUED
      |
      v
Resolve source
      |
      v
Stream bytes
      |
      v
Chunk
      |
      v
Hash plaintext
      |
      v
Dedup lookup
      |
      +---------- existing ----------+
      |                              |
      |                              v
      |                         reuse blob
      |
      v
Compress
      |
      v
Encrypt
      |
      v
Provider Envelope
      |
      v
Hash stored representation
      |
      v
Transport::put()
      |
      v
Verify remote object
      |
      v
Persist BlobLocation
      |
      v
Commit chunk COMPLETE
      |
      v
Write manifest
      |
      v
Backup = COMPLETE
```

The entire pipeline should be streaming.

Avoid:

```text
Download 200 GB file
-> save complete temporary file
-> process afterward
```

Prefer:

```text
HTTP response stream
-> chunker
-> compressor
-> encryptor
-> uploader
```

This keeps memory usage nearly constant regardless of backup size.

---

## 9. Chunking

### Initial implementation

Use fixed-size chunks.

Recommended default:

```text
64 MiB
```

Example:

```text
150 MiB file

chunk 0 = 64 MiB
chunk 1 = 64 MiB
chunk 2 = 22 MiB
```

Advantages:

- Simple.
- Easy to implement.
- Easy to resume.
- Easy to parallelize.
- Good fit for large backup files.

### Later optimization

Add content-defined chunking using FastCDC.

Example configuration:

```text
minimum = 8 MiB
target  = 32 MiB
maximum = 64 MiB
```

This improves deduplication when files change slightly between backups.

### Per-transport chunk sizes

Allow transport-specific limits.

```yaml
transports:
  s3:
    chunk_size: 128MiB

  secondary:
    chunk_size: 32MiB
```

---

## 10. Compression

Recommended compression:

```text
zstd
```

Pipeline ordering:

```text
plaintext
  -> compress
  -> encrypt
```

Never compress after encryption because encrypted data is effectively incompressible.

Configuration:

```yaml
compression:
  algorithm: zstd
  level: 6
```

Compression should remain configurable because already-compressed backup formats may not benefit from another pass.

---

## 11. Encryption

If any transport is not fully trusted, encryption must happen locally before upload.

Recommended algorithms:

- XChaCha20-Poly1305.
- AES-256-GCM.

Preferred starting point:

```text
XChaCha20-Poly1305
```

Reasons:

- Authenticated encryption.
- Large nonce space.
- Good Rust ecosystem support.

### Key model

```text
Master Key
   |
   +-- Backup Key
         |
         +-- Chunk 0 nonce
         +-- Chunk 1 nonce
         +-- Chunk 2 nonce
```

Do not store encryption secrets alongside backup chunks.

Configuration example:

```yaml
encryption:
  algorithm: xchacha20-poly1305
  key_provider: env
  key_env: BACKUP_MASTER_KEY
```

Future key providers:

- AWS KMS.
- HashiCorp Vault.
- GCP KMS.
- Azure Key Vault.

---

## 12. Hashing and Integrity

Store both plaintext and stored-form hashes.

```text
plaintext_hash = BLAKE3(original chunk)
stored_hash    = BLAKE3(provider envelope bytes)
```

This allows two independent integrity checks.

### Upload verification

```text
upload
  -> remote locator
  -> HEAD or GET
  -> verify stored_hash
  -> mark location COMPLETE
```

### Restore verification

```text
GET remote object
  -> verify stored_hash
  -> decode envelope
  -> decrypt + authenticate
  -> decompress
  -> verify plaintext_hash
  -> emit bytes
```

Do not treat an upload HTTP 200 response as sufficient proof of a valid backup.

---

## 13. Provider Envelope Abstraction

A reusable idea from the reference JS implementation is the reversible payload-envelope concept.

The generalized abstraction should be:

```rust
pub trait Envelope: Send + Sync {
    fn encode(&self, input: Bytes) -> Result<Bytes, EnvelopeError>;
    fn decode(&self, input: Bytes) -> Result<Bytes, EnvelopeError>;
}
```

Possible implementations:

```text
RawEnvelope
ProviderNativeEnvelope
ImageEnvelope
CustomContainerEnvelope
```

The backup engine should not know why a provider needs a particular envelope.

The envelope belongs to the transport implementation or transport configuration.

### Important design constraint

Do not bake provider-specific disguise logic into the backup format.

The backup format should remain:

```text
chunk -> compress -> encrypt -> envelope -> transport
```

If the provider changes, only the envelope/transport implementation changes.

---

## 14. Transport Abstraction

This is the most important interface in the service.

```rust
#[async_trait]
pub trait BlobTransport: Send + Sync {
    async fn put(
        &self,
        blob: BlobUpload,
    ) -> Result<RemoteBlob, TransportError>;

    async fn get(
        &self,
        locator: &RemoteLocator,
    ) -> Result<BlobStream, TransportError>;

    async fn head(
        &self,
        locator: &RemoteLocator,
    ) -> Result<RemoteBlobMetadata, TransportError>;

    async fn delete(
        &self,
        locator: &RemoteLocator,
    ) -> Result<(), TransportError>;
}
```

Implementations may include:

```text
S3Transport
R2Transport
FilesystemTransport
GenericHttpObjectTransport
SecondaryTransport
```

The core system must only understand:

```text
put()
get()
head()
delete()
```

Provider-specific authentication, headers, registration/finalization steps, and locator parsing remain inside the transport implementation.

---

## 15. Transport Manager

A `TransportManager` chooses physical locations and handles fallback.

```text
Restore Blob abc
       |
       v
Preferred secondary available?
      / \
    yes  no
     |    |
     v    v
  download S3 fallback
     |
     v
verify
     |
corrupt?
  /   \
yes    no
 |      |
try     use
next
```

Conceptual API:

```rust
pub async fn retrieve(
    blob: &Blob,
    preferred: &[TransportId],
) -> Result<BlobStream, RetrieveError>;
```

The service should never expose transport fallback complexity to callers.

---

## 16. Upload Concurrency

The reference JS implementation uploads objects sequentially.

The Rust service should use bounded concurrency.

```text
chunk 001 ----+
chunk 002 ----+--> semaphore(6) --> transport
chunk 003 ----+
chunk 004 ----+
```

Configuration:

```yaml
upload:
  concurrency: 6
  retries: 5
  retry_base_ms: 1000
  retry_max_ms: 30000
```

Use exponential backoff with jitter.

Never use unbounded concurrency.

---

## 17. Resumability

Every chunk upload must be independently resumable.

Example state:

```text
seq | blob_hash | state      | locator
----+-----------+------------+----------------
0   | aaa       | COMPLETE   | remote://...
1   | bbb       | COMPLETE   | remote://...
2   | ccc       | UPLOADING  |
3   | ddd       | PENDING    |
```

On restart:

```text
COMPLETE -> skip
FAILED   -> retry
PENDING  -> upload
UPLOADING with stale lock -> return to PENDING
```

Do not require recreating the full backup after process restart.

---

## 18. Temporary Data Lifecycle

Do not delete a temporary chunk immediately after upload.

Correct sequence:

```text
upload
  -> obtain remote locator
  -> verify remote object
  -> verify stored hash
  -> commit DB transaction
  -> mark chunk COMPLETE
  -> delete local temp chunk
```

For less reliable transports, verification may perform a full GET rather than HEAD.

---

## 19. Database Schema

SQLite is recommended for standalone deployments.

PostgreSQL can be added using the same repository abstraction.

### backups

```sql
CREATE TABLE backups (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,

    source_type     TEXT NOT NULL,
    source_uri      TEXT,

    status          TEXT NOT NULL,

    original_size   INTEGER,
    stored_size     INTEGER,
    checksum        TEXT,

    created_at      TEXT NOT NULL,
    completed_at    TEXT,

    metadata_json   TEXT
);
```

### artifacts

```sql
CREATE TABLE artifacts (
    id              TEXT PRIMARY KEY,
    backup_id       TEXT NOT NULL,

    filename        TEXT,
    content_type    TEXT,
    original_size   INTEGER,

    FOREIGN KEY (backup_id)
        REFERENCES backups(id)
);
```

### blobs

```sql
CREATE TABLE blobs (
    id              TEXT PRIMARY KEY,

    plaintext_hash  TEXT NOT NULL,
    stored_hash     TEXT NOT NULL,

    original_size   INTEGER NOT NULL,
    stored_size     INTEGER NOT NULL,

    compression     TEXT,
    encryption      TEXT,
    envelope        TEXT,

    created_at      TEXT NOT NULL
);
```

### blob_locations

```sql
CREATE TABLE blob_locations (
    blob_id         TEXT NOT NULL,
    transport       TEXT NOT NULL,

    remote_id       TEXT,
    remote_locator  TEXT NOT NULL,

    status          TEXT NOT NULL,
    verified_at     TEXT,
    created_at      TEXT NOT NULL,

    PRIMARY KEY (
        blob_id,
        transport
    )
);
```

### artifact_blobs

```sql
CREATE TABLE artifact_blobs (
    artifact_id     TEXT NOT NULL,
    blob_id         TEXT NOT NULL,

    sequence        INTEGER NOT NULL,

    PRIMARY KEY (
        artifact_id,
        sequence
    )
);
```

### jobs

```sql
CREATE TABLE jobs (
    id              TEXT PRIMARY KEY,
    job_type        TEXT NOT NULL,
    backup_id       TEXT,

    status          TEXT NOT NULL,
    attempt         INTEGER NOT NULL DEFAULT 0,

    available_at    TEXT NOT NULL,
    locked_at       TEXT,
    locked_by       TEXT,

    error           TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);
```

---

## 20. Job Queue

Avoid adding Redis or RabbitMQ in the first version.

SQLite can act as a durable job queue.

```text
API
 |
 v
jobs table
 |
 v
Tokio worker pool
 |
 +-- FETCH
 +-- PROCESS
 +-- UPLOAD
 +-- VERIFY
 +-- COMPLETE
```

Worker loop concept:

```sql
SELECT id
FROM jobs
WHERE status = 'QUEUED'
  AND available_at <= CURRENT_TIMESTAMP
ORDER BY created_at
LIMIT ?;
```

The worker should claim jobs transactionally.

When moving to PostgreSQL, `FOR UPDATE SKIP LOCKED` can be used.

---

## 21. Portable Manifest

Even though the database stores all state, each completed backup should also produce a portable manifest.

Example:

```json
{
  "format": "backupd-v1",
  "backup": {
    "id": "01K7Q1XSCXMR32ZJ4KJ82QDGK2",
    "name": "production-db",
    "created_at": "2026-10-09T02:00:00Z",
    "original_size": 8291038123,
    "plaintext_hash": "blake3:..."
  },
  "artifacts": [
    {
      "filename": "postgres.dump",
      "size": 8291038123,
      "chunks": [
        {
          "sequence": 0,
          "blob_id": "blake3:abc...",
          "plaintext_size": 67108864,
          "stored_size": 21399304
        },
        {
          "sequence": 1,
          "blob_id": "blake3:def...",
          "plaintext_size": 67108864,
          "stored_size": 20719322
        }
      ]
    }
  ]
}
```

The manifest should not embed encryption secrets.

Store it in authoritative storage.

Example:

```text
s3://backup-control-plane/manifests/<backup_id>.json
```

This allows recovery even if the primary catalog database is lost.

---

## 22. API Design

### Create backup from URL

```http
POST /v1/backups
```

Response:

```json
{
  "id": "01K7Q1XSCXMR32ZJ4KJ82QDGK2",
  "status": "queued"
}
```

### Upload file

```http
POST /v1/backups/upload
```

### Get backup

```http
GET /v1/backups/{id}
```

### List backups

```http
GET /v1/backups
```

Filters:

```text
GET /v1/backups?name=prod-db
GET /v1/backups?status=complete
GET /v1/backups?from=2026-10-01
GET /v1/backups?tag.environment=production
```

### Download restored bytes

```http
GET /v1/backups/{id}/download
```

### Verify backup

```http
POST /v1/backups/{id}/verify
```

### Delete logical backup

```http
DELETE /v1/backups/{id}
```

Deletion should first remove references, then perform garbage collection on unreferenced blobs.

---

## 23. Example Backup Record

```json
{
  "id": "01K7Q1XSCXMR32ZJ4KJ82QDGK2",
  "name": "prod-db",
  "status": "complete",
  "created_at": "2026-10-09T02:00:00Z",
  "completed_at": "2026-10-09T02:12:48Z",
  "source": {
    "type": "url"
  },
  "stats": {
    "original_bytes": 8291038123,
    "stored_bytes": 2848189012,
    "chunks": 124,
    "deduplicated_chunks": 91
  },
  "transports": [
    "secondary"
  ],
  "tags": {
    "environment": "production",
    "type": "database"
  }
}
```

---

## 24. Restore Flow

```text
GET /backups/{id}/download
           |
           v
        Catalog
           |
           v
       Manifest
           |
           v
    Resolve each blob
           |
           v
 Concurrent prefetch
           |
           v
 Ordered reconstruction
           |
           v
 Verify stored hash
           |
           v
 Decode envelope
           |
           v
 Decrypt/authenticate
           |
           v
 Decompress
           |
           v
 Verify plaintext hash
           |
           v
 HTTP output stream
```

The original backup is streamed to the caller.

Example:

```bash
curl \
  http://backupd:8080/v1/backups/01K7Q1.../download \
  > postgres.dump
```

The caller does not need to know which transport holds each chunk.

---

## 25. Parallel Restore with Ordered Output

Download chunks concurrently, but write them in sequence.

```text
Fetch:

0 ----+
1 ----+--> parallel fetch/decode
2 ----+
3 ----+

Output:

0 -> 1 -> 2 -> 3
```

Implementation can maintain a small reorder buffer.

Example configuration:

```yaml
restore:
  prefetch: 6
```

This increases remote throughput without buffering the entire backup in RAM.

---

## 26. Restore State Machine

For explicit restore jobs:

```text
REQUESTED
   |
   v
FETCH_MANIFEST
   |
   v
DOWNLOADING
   |
   v
VERIFYING
   |
   v
DECRYPTING
   |
   v
REASSEMBLING
   |
   v
VALIDATING
   |
   v
COMPLETE
```

Failure states must remain observable.

A partially reconstructed output must never be reported as a successful restore.

---

## 27. Verification and Restore Testing

Successful upload is not sufficient.

Recommended states:

```text
uploaded
verified
restore-tested
```

Suggested policy:

```text
Daily:
  create backup
  verify all newly uploaded chunks

Weekly:
  restore one complete backup
  verify output checksum
```

Optionally restore into an isolated environment and perform service-level validation.

---

## 28. Garbage Collection

Because content-addressed blobs may be referenced by multiple backups, deleting a backup must not immediately delete all associated remote objects.

Flow:

```text
Delete backup
   |
   v
Remove artifact references
   |
   v
Find blobs with reference_count = 0
   |
   v
Mark garbage
   |
   v
GC worker deletes physical locations
```

Maintain a grace period before physical deletion.

Example:

```yaml
gc:
  grace_period: 7d
```

---

## 29. Scheduling

The first version should not embed backup creation schedules into `backupd`.

Use external schedulers:

- cron.
- systemd timers.
- Kubernetes CronJobs.
- CI/CD scheduler.

Example:

```bash
pg_dump ... > /backup/prod.dump

curl \
  -F file=@/backup/prod.dump \
  -F name=prod-db \
  -F policy=daily \
  http://backupd:8080/v1/backups/upload
```

Or:

```bash
pg_dump ... | \
backupctl upload - \
  --name prod-db \
  --policy daily
```

This keeps `backupd` focused on backup storage and restore, not backup generation.

---

## 30. Backup Policies

Example configuration:

```yaml
policies:
  weekly:
    transport_priority:
      - s3
    replication:
      min_copies: 1

  daily:
    transport_priority:
      - secondary
      - s3
    replication:
      min_copies: 1
```

A more conservative daily policy may replicate important backups to both destinations.

---

## 31. Configuration

Example `backupd.yaml`:

```yaml
server:
  bind: 0.0.0.0:8080

runtime:
  worker_count: 4

database:
  url: sqlite:///data/backup.db

pipeline:
  chunking:
    mode: fixed
    chunk_size: 64MiB

  compression:
    algorithm: zstd
    level: 6

  encryption:
    algorithm: xchacha20-poly1305
    key_provider: env
    key_env: BACKUP_MASTER_KEY

upload:
  concurrency: 6
  retries: 5
  retry_base_ms: 1000
  retry_max_ms: 30000

restore:
  prefetch: 6

transports:
  s3:
    type: s3
    bucket: my-backups
    region: ap-southeast-1

  secondary:
    type: secondary

policies:
  weekly:
    transports:
      - s3

  daily:
    transports:
      - secondary

verification:
  verify_after_upload: true

gc:
  grace_period: 7d
```

---

## 32. Rust Project Structure

Use a Cargo workspace.

```text
backupd/
+-- Cargo.toml
+-- crates/
    +-- backupd/
    |   +-- src/
    |       +-- main.rs
    |
    +-- api/
    |   +-- src/
    |       +-- routes.rs
    |       +-- request.rs
    |       +-- response.rs
    |
    +-- core/
    |   +-- src/
    |       +-- backup.rs
    |       +-- artifact.rs
    |       +-- blob.rs
    |       +-- manifest.rs
    |       +-- restore.rs
    |
    +-- pipeline/
    |   +-- src/
    |       +-- chunk.rs
    |       +-- compress.rs
    |       +-- encrypt.rs
    |       +-- hash.rs
    |       +-- envelope.rs
    |
    +-- source/
    |   +-- src/
    |       +-- file.rs
    |       +-- http.rs
    |       +-- stream.rs
    |
    +-- transport/
    |   +-- src/
    |       +-- mod.rs
    |       +-- manager.rs
    |       +-- s3.rs
    |       +-- filesystem.rs
    |       +-- secondary.rs
    |
    +-- catalog/
    |   +-- src/
    |       +-- repository.rs
    |       +-- sqlite.rs
    |       +-- postgres.rs
    |
    +-- worker/
    |   +-- src/
    |       +-- jobs.rs
    |       +-- executor.rs
    |
    +-- backupctl/
        +-- src/
            +-- main.rs
```

A single binary can initially embed API + workers.

The workspace separation keeps the design modular as the project grows.

---

## 33. Suggested Rust Libraries

| Concern | Library |
|---|---|
| Async runtime | `tokio` |
| HTTP server | `axum` |
| HTTP client | `reqwest` |
| Database | `sqlx` |
| Serialization | `serde`, `serde_json` |
| IDs | `ulid` |
| Logging | `tracing`, `tracing-subscriber` |
| Errors | `thiserror` |
| Compression | `zstd` |
| Fast hash | `blake3` |
| SHA-256 | `sha2` |
| Encryption | `chacha20poly1305` |
| S3 | AWS Rust SDK |
| CLI | `clap` |
| Metrics | `metrics`, Prometheus exporter |
| Multipart | Axum multipart support |
| MIME | `mime` / `mime_guess` |

Avoid introducing a large application framework.

---

## 34. Runtime Model

Default mode:

```bash
backupd
```

runs:

```text
HTTP API
+
worker pool
+
job scheduler/poller
```

Optional commands:

```bash
backupd serve
backupd worker
backupd migrate
backupd verify <backup_id>
```

For larger deployments, API and worker processes can be separated later.

---

## 35. Deployment

### Single-host deployment

```text
+--------------------------------+
|       backupd container        |
|                                |
| Axum :8080                     |
| Tokio workers                  |
| SQLite catalog                 |
|                                |
+---------------+----------------+
                |
           outbound only
                |
        +-------+--------+
        v                v
       S3           Secondary
```

### Docker Compose

```yaml
services:
  backupd:
    image: backupd:latest
    ports:
      - "8080:8080"
    volumes:
      - ./data:/data
    environment:
      DATABASE_URL: sqlite:///data/backup.db
      BACKUP_MASTER_KEY: ${BACKUP_MASTER_KEY}
```

---

## 36. Observability

Expose structured logs and metrics.

Important metrics:

```text
backup_jobs_total
backup_jobs_failed_total
backup_bytes_ingested_total
backup_bytes_stored_total
backup_dedup_bytes_saved_total
transport_upload_latency_seconds
transport_download_latency_seconds
transport_errors_total
restore_jobs_total
restore_failures_total
blob_verification_failures_total
```

Useful per-job logs:

```text
backup_id
job_id
artifact_id
blob_id
transport
attempt
bytes
latency
```

Do not log encryption keys or authentication material.

---

## 37. Security Model

### Authentication

Initial deployment options:

- Static API token.
- Reverse proxy authentication.
- mTLS for internal environments.

Later:

- JWT/OIDC.

### SSRF protection

Remote URL ingestion creates SSRF risk.

The URL source resolver should:

- Reject localhost by default.
- Reject private IP ranges unless explicitly configured.
- Resolve DNS and validate the resolved address.
- Re-check redirects.
- Limit redirect count.
- Enforce HTTPS where possible.
- Limit total input size.
- Apply connection/read timeouts.

### Filesystem safety

If local path input is supported:

- Restrict readable roots.
- Canonicalize paths.
- Reject traversal.

### Encryption

- Never store master keys in SQLite.
- Never include keys in manifests.
- Prefer environment secret or external KMS.

---

## 38. Failure Handling

### Source failure

```text
URL timeout -> retry source fetch
404 -> permanent failure
input stream aborted -> fail backup
```

### Upload failure

```text
retry transient failures
backoff
persist attempt count
leave completed chunks untouched
```

### Verification failure

```text
mark location CORRUPT
retry upload
if another transport exists, retain it
```

### Process crash

```text
jobs persisted in DB
chunk states persisted
stale UPLOADING states requeued
```

### Catalog loss

Restore catalog from:

```text
portable manifests
+
trusted manifest storage
```

---

## 39. Data Retention

Define policies independently from physical transports.

Example:

```yaml
retention:
  weekly_full:
    keep: 8

  daily:
    keep: 14
```

Retention deletes backup records first.

Physical blob deletion occurs only when no remaining backup references a blob.

---

## 40. Deduplication

Basic algorithm:

```text
chunk
  -> plaintext BLAKE3
  -> SELECT blob WHERE plaintext_hash = ?
```

If blob exists and has an acceptable physical location:

```text
reuse existing blob
skip compression/encryption/upload if compatible
```

If encryption configuration differs, deduplication policy must decide whether reuse is allowed.

Recommended first version:

- Deduplicate only within the same encryption profile.
- Encode encryption profile into blob metadata.

---

## 41. Content-Addressed Daily Backups

This design can replace traditional incremental backups.

Example:

```text
Monday backup:
[A, B, C, D, E]

Tuesday backup:
[A, B, C, X, E]
```

Only `X` is new.

Tuesday's manifest references the complete logical state without requiring Monday's manifest during restore.

This provides incremental transfer efficiency without creating a fragile incremental chain.

---

## 42. Reusable Lessons from the JS Reference Implementation

The reference implementation demonstrates several useful concepts.

### Reusable concept 1: reversible payload wrapper

The JS project embeds arbitrary bytes into another file container and later extracts them.

Generalized form:

```text
bytes
  -> Envelope::encode
  -> remote transport
  -> Envelope::decode
  -> original bytes
```

This concept belongs in the transport/envelope layer.

### Reusable concept 2: remote locator manifest

The JS project records uploaded remote URLs in a JSON mapping.

Generalized form:

```text
logical blob ID
  -> one or more remote locators
```

This should become durable `blob_locations` records plus a portable manifest.

### Reusable concept 3: independent segment retrieval

Each remote object can be retrieved independently.

This maps directly to backup chunks.

### Reusable concept 4: client-side reconstruction

The browser service worker performs:

```text
fetch remote object
-> decode wrapper
-> reconstruct original segment
```

The Rust restore engine performs the same generalized pattern:

```text
Transport::get
-> Envelope::decode
-> verify
-> decrypt
-> decompress
-> reconstruct ordered stream
```

### Elements intentionally not carried over

The backup service does not need:

- FFmpeg.
- HLS.
- `.ts` segmentation.
- `.m3u8` playlists.
- Video.js.
- Service workers.
- Key-server HTTP endpoints.
- Browser-based restore logic.
- Sequential upload behavior.
- Ephemeral local JSON state.

---

## 43. Optimizations Relative to the Reference Implementation

### Remove Base64 where possible

Avoid:

```text
binary
-> Base64 (+~33%)
-> compression
-> container
```

Prefer:

```text
encrypted binary
-> provider envelope
```

If a specific provider requires textual representation, keep that overhead isolated inside the provider envelope.

### Bounded parallel uploads

Replace sequential upload loops with Tokio bounded concurrency.

### Persistent chunk state

Persist every chunk's upload status to support restart and resume.

### End-to-end hashes

Verify both remote object bytes and recovered plaintext.

### Content-addressed deduplication

Avoid re-uploading chunks already present.

### Parallel restore prefetch

Fetch chunks concurrently while emitting them in correct order.

### Transport fallback

Allow each logical blob to have multiple physical locations.

---

## 44. MVP Scope

### Phase 1 — Core service

Implement:

- Rust binary.
- Axum API.
- SQLite catalog.
- URL input.
- Multipart file input.
- Fixed-size chunking.
- BLAKE3 hashing.
- zstd compression.
- XChaCha20-Poly1305 encryption.
- Filesystem transport.
- S3 transport.
- Durable job queue.
- Portable manifest.
- Download/restore endpoint.

### Phase 2 — Secondary transport

Implement:

- Secondary transport abstraction.
- Provider envelope abstraction.
- Per-transport chunk size.
- Upload verification.
- Retry/backoff.
- Fallback retrieval.

### Phase 3 — Incremental efficiency

Implement:

- Deduplication.
- FastCDC content-defined chunking.
- Reference counting.
- Garbage collection.

### Phase 4 — Operational hardening

Implement:

- Metrics.
- Health endpoints.
- Restore tests.
- Retention policies.
- KMS support.
- PostgreSQL catalog support.
- Multi-worker deployment.

---

## 45. Suggested API Health Endpoints

```http
GET /health/live
GET /health/ready
GET /metrics
```

Readiness should validate:

- Database connectivity.
- Worker startup.
- Required encryption configuration.

Do not make readiness depend on all remote transports being online, otherwise an optional secondary provider outage can make the service unavailable.

---

## 46. Backup Lifecycle States

Recommended backup statuses:

```text
QUEUED
FETCHING
PROCESSING
UPLOADING
VERIFYING
COMPLETE
FAILED
DELETING
DELETED
```

Chunk/location statuses:

```text
PENDING
UPLOADING
VERIFYING
COMPLETE
FAILED
CORRUPT
DELETED
```

---

## 47. Example End-to-End Flow

Caller:

```bash
backupctl upload /backups/prod.dump \
  --name prod-db \
  --policy daily
```

`backupd`:

```text
1. Create backup ID.
2. Open source stream.
3. Read 64 MiB chunk.
4. Compute plaintext BLAKE3.
5. Check dedup catalog.
6. zstd compress if new.
7. Encrypt.
8. Encode transport envelope.
9. Compute stored hash.
10. Upload through Transport::put().
11. Verify remote object.
12. Persist Blob + BlobLocation.
13. Repeat.
14. Persist Artifact chunk ordering.
15. Generate manifest.
16. Store manifest in trusted storage.
17. Mark Backup COMPLETE.
18. Return backup record.
```

Later:

```bash
backupctl download 01K7Q1XSCXMR32ZJ4KJ82QDGK2 \
  --output prod-restored.dump
```

Restore:

```text
1. Read backup manifest.
2. Resolve chunk locations.
3. Fetch chunks concurrently.
4. Verify stored hash.
5. Decode envelope.
6. Decrypt and authenticate.
7. Decompress.
8. Verify plaintext hash.
9. Emit chunks in sequence.
10. Verify full artifact checksum.
```

---

## 48. Design Invariants

The implementation should preserve these invariants.

### Invariant 1

A `backup_id` is stable and independent of storage provider.

### Invariant 2

A remote locator is never the authoritative logical identity of data.

### Invariant 3

A backup is not `COMPLETE` until every required chunk has at least one verified location.

### Invariant 4

Encryption occurs before data leaves the service when using an untrusted transport.

### Invariant 5

Restore always verifies integrity before emitting recovered bytes.

### Invariant 6

Provider-specific behavior never leaks into the backup manifest format.

### Invariant 7

Transport failure must not corrupt the backup catalog.

### Invariant 8

Deleting one backup must not delete blobs referenced by another backup.

### Invariant 9

A restart must not require starting a backup from zero.

### Invariant 10

The authoritative manifest must be stored independently of opportunistic data storage.

---

## 49. Recommended First Production Topology

```text
                    Backup Producers
                          |
                          v
                      backupd
                          |
          +---------------+---------------+
          |                               |
      Daily chunks                    Weekly full
          |                               |
          v                               v
   Secondary transport                  S3
          |                               |
          +---------------+---------------+
                          |
                          v
                    Backup Catalog
                       SQLite
                          |
                          v
                 S3 Manifest Bucket
```

For a more conservative system:

```text
Daily backup chunks
   -> secondary transport
   -> optional S3 replication for critical datasets
```

---

## 50. Final Recommended Boundary

`backupd` should own:

- Input acquisition.
- Chunking.
- Compression.
- Encryption.
- Hashing.
- Deduplication.
- Upload orchestration.
- Transport abstraction.
- Backup metadata.
- Portable manifests.
- Verification.
- Restore.

External systems should own:

- Running `pg_dump`.
- Creating filesystem archives.
- Choosing backup schedules.
- Deciding which application data should be backed up.

Remote transports should own only:

- Durable or opportunistic storage of opaque blobs.
- Returning a remote locator.
- Serving the exact stored bytes back on request.

That boundary keeps the service portable, testable, and independent from any single storage provider.

---

## 51. Summary

The target system is best understood as a small Rust backup gateway with Restic/Kopia-like internal principles and pluggable physical transports.

The core design is:

```text
INPUT

file / URL / stream
       |
       v
    backupd
       |
       +-- chunk
       +-- hash
       +-- dedupe
       +-- compress
       +-- encrypt
       +-- envelope
       +-- upload
       +-- verify
       +-- catalog
       |
       v
OUTPUT

backup_id
+
queryable DB record
+
portable manifest
```

Later:

```text
backup_id
   |
   v
 catalog
   |
   v
 blob locations
   |
   v
 fetch + verify + decode + decrypt + decompress
   |
   v
 original backup stream
```

The decisive architectural choice is that storage providers are replaceable implementation details. The durable contract is the `backupd` manifest, blob identity model, and restore pipeline.
