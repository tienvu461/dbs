# Operations and maintenance

## Configuration reference

The current binary reads environment variables. `backupd.yaml` is an illustrative config sample; it is not loaded by the service.

| Variable | Default | Notes |
|---|---|---|
| `BACKUPD_BIND` | `127.0.0.1:8080` | Use `0.0.0.0:8080` in the container. |
| `BACKUPD_API_TOKEN` | unset | Random bearer token, at least 32 characters; required for non-loopback binds. |
| `DATABASE_URL` | `sqlite://data/backup.db` | SQLite URL. Directory is created at startup. |
| `BACKUPD_TRANSPORT` | `filesystem` | Set `s3` for S3-compatible backend. |
| `BACKUPD_STORAGE_ROOT` | `data/blobs` | Filesystem backend root. |
| `BACKUPD_STAGING_ROOT` | `data/staging` | Persist this directory across restarts. |
| `BACKUPD_CHUNK_SIZE` | `67108864` | Bytes per chunk. |
| `BACKUPD_MAX_UPLOAD_BYTES` | unlimited | Set a deliberate deployment limit. |
| `BACKUP_MASTER_KEY` | unset | 32-byte XChaCha key, encoded as 64 hex chars. Needed for TikTok. |
| `S3_ENDPOINT_URL` | required for S3 | AWS endpoint or custom R2/Garage endpoint. |
| `S3_BUCKET` | required for S3 | Bucket name. Provision it before starting backupd. |
| `S3_PREFIX` | `backupd` | Object key prefix. |
| `AWS_REGION` | `us-east-1` | R2 uses `auto`; Garage should match server configuration. |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | SDK credential chain | Static access credentials; omit together to use the AWS SDK chain. |
| `TIKTOK_ORG_ID` / `TIKTOK_COOKIE` | unset | Enables experimental TikTok publishing when both are set. |
| `TIKTOK_CSRF_TOKEN` | unset | Optional session CSRF token. |
| `TIKTOK_CDN_ORIGIN` | `https://p16-ad-sg.tiktokcdn.com/origin/` | Used when TikTok response has a relative `web_uri`. |

## Container deployment

Mount a durable writable volume at `/data`, expose port 8080, and provide the desired storage settings. SQLite and staged uploads must survive restarts. Run a single service instance for the SQLite catalog; SQLite queue claiming is not currently designed for multiple replicas sharing an arbitrary network filesystem.

Example:

```sh
docker run --rm \
  -p 8080:8080 \
  -v backupd-data:/data \
  -e BACKUPD_BIND=0.0.0.0:8080 \
  -e BACKUPD_API_TOKEN \
  -e BACKUPD_TRANSPORT=s3 \
  -e S3_ENDPOINT_URL=https://<account>.r2.cloudflarestorage.com \
  -e S3_BUCKET=backups \
  -e AWS_REGION=auto \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  ghcr.io/<owner>/<repo>:latest
```

GHCR packages may default to private. Set package visibility explicitly in GitHub Packages if unauthenticated pulls are required.

## Upload and restore smoke check

```sh
curl -fsS http://localhost:8080/health/ready
curl -sS -H "Authorization: Bearer $BACKUPD_API_TOKEN" -F 'name=smoke' -F 'file=@./small-backup.bin' \
  http://localhost:8080/v1/backups/upload
```

The upload response includes `id` and `job_id`. Poll until the job status is `complete`, then restore and compare bytes:

```sh
curl -fsS -H "Authorization: Bearer $BACKUPD_API_TOKEN" http://localhost:8080/v1/jobs/<job_id>
curl -fsS -H "Authorization: Bearer $BACKUPD_API_TOKEN" http://localhost:8080/v1/backups/<backup_id>/download -o restored.bin
cmp ./small-backup.bin restored.bin
```

`GET /v1/backups/<backup_id>/tiktok` returns the CDN index. The index includes the public image URLs, chunk sequence, compression/encryption names, and CDN nonce. The regular `/download` endpoint currently uses the catalog's authoritative blob transport; it does not fall back to TikTok. A successful local codec unit test is not proof that TikTok accepted the PNG, preserved its `iTXt`, or serves anonymous GETs. Run live upload + anonymous GET checks with a disposable test backup and an authorized test Business Center before relying on TikTok data. If upload fails after some images were published, the current worker retry may upload duplicate TikTok image materials; clean those up in the Business Center UI where supported.

## Queue recovery and stuck work

- `queued`: waiting to be claimed or waiting for retry delay.
- `running`: claimed by a worker; `locked_at` and `locked_by` show ownership.
- `complete`: finished; the staged file should be removed.
- `failed`: retry budget exhausted; inspect the job error and backup error.

Workers automatically requeue running jobs whose lock is older than ten minutes; recovery checks run at startup and once per minute. Confirm the staged upload still exists before manually resetting a job. Reprocessing is intended to be idempotent because object keys are derived from content hashes. Do not clear `jobs`, `artifact_blobs`, or `blob_locations` manually unless doing a planned recovery with a copy of the database.

Garbage collection is queued after logical backup deletion. GC failures are retried after 30 seconds. Check `gc_queue` and provider-side object existence when storage credentials or connectivity are broken. Retrying deletion is safe; deleting a referenced blob is not.

## Database backup and recovery

Back up the SQLite DB with a SQLite-consistent backup method (e.g. `sqlite3 ... '.backup ...'`) rather than copying only the main DB file while WAL mode is active. Also back up the staging directory while jobs are pending and retain blob data independently. A restore requires a mutually consistent catalog plus object set; there is not yet an automated manifest-based catalog rebuild command.

## Dependency and schema changes

- Update `Cargo.lock` alongside dependency changes; the Docker build uses `--locked`.
- Add schema changes idempotently in `Catalog::migrate`; support upgrading existing DBs, not only fresh installs.
- Add tests for serialization compatibility when changing manifests; older optional fields should default safely where feasible.
- Validate S3-compatible API changes against the supported subset (Put/Get/Head/Delete). R2 has a provider-specific compatibility matrix; do not assume every AWS S3 feature is implemented.
- Change the image workflow/Dockerfile together if binary name, target platforms, runtime libraries, or package naming changes.

## Secrets and incident handling

- Keep AWS credentials, `BACKUP_MASTER_KEY`, and TikTok browser cookies in a secrets manager or protected environment.
- The TikTok cookie is equivalent to a logged-in Business Center session. Rotate/revoke it if exposed; never paste it into issues or logs.
- Losing `BACKUP_MASTER_KEY` makes encrypted chunks unrecoverable. Store it separately with a tested recovery procedure.
- Avoid logging full request headers, query strings containing credentials, signed URLs, or sensitive upload metadata.
