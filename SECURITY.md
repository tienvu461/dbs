# Security and public-repository maintenance

## Reporting

Report vulnerabilities using GitHub private vulnerability reporting for `tienvu461/backupd` when enabled. Otherwise contact the maintainer privately before publishing exploit details. Never include credentials, browser cookies, encryption keys, backup contents, or unredacted provider responses in public issues.

## Secret scanning before commits

Install Python `pre-commit` and a current Go toolchain. The pinned upstream hook builds Gitleaks v8.30.0 in its own environment:

```sh
python -m pip install pre-commit
pre-commit install
pre-commit run --all-files
```

Run `pre-commit install` after the checkout has Git metadata. The hook scans staged changes (including when invoked with `--all-files`; use the directory command below to scan unstaged files). `.github/workflows/secret-scan.yml` additionally scans Git history on pushes and pull requests using Gitleaks in Docker, without third-party API credentials. The image publication workflow also scans history before building/publishing. Make the secret scan job a required branch check. Local hooks can be bypassed, so CI is essential.

Scan working files without Git metadata:

```sh
docker run --rm -v "$PWD:/repo:ro" zricethezav/gitleaks:v8.30.0 \
  dir /repo --config /repo/.gitleaks.toml --redact --no-banner
```

Scan all reachable local Git history before first public publication:

```sh
docker run --rm -v "$PWD:/repo:ro" zricethezav/gitleaks:v8.30.0 \
  git /repo --config /repo/.gitleaks.toml --redact --no-banner
```

Rules extend the upstream detector set and detect assigned backupd/TikTok secrets. Generated `/data` and `/target` paths are excluded from scanning and must never be committed; both CI workflows reject occurrences of these paths in reachable history. Documentation placeholders beginning with `<...>` are allowed for the custom rule; real credentials are not. Scanner success is not proof that all secret formats were recognized. Review staged files before every commit, including binary files and archives. `.gitignore` does not remove previously tracked secrets.

If a secret was committed, revoke/rotate it immediately, then remove it from reachable history following GitHub's sensitive-data removal procedure. Deleting the current file alone is insufficient; forks, caches, images, and prior commits may retain it.

## Runtime access

`BACKUPD_API_TOKEN` enables bearer-token authentication on all `/v1/` routes, including restore and deletion. It must be at least 32 characters; generate it randomly and supply it through a protected environment or secret manager. Non-loopback listeners require the token, including the Docker image's `0.0.0.0:8080` default. Local loopback development can omit the token. Health endpoints remain unauthenticated and reveal only fixed status messages.

Use TLS at a reverse proxy for remote access: bearer tokens and restored plaintext must not travel over public unencrypted HTTP. This is single-tenant authentication, not per-backup authorization or multi-tenant isolation. Application errors are sanitized for server failures. TikTok provider response bodies and raw S3 SDK errors are not stored as job errors or echoed through HTTP.

## Review status and unresolved findings

The source/configuration file scan found no detected credentials. Live provider verification and a Git history scan were not performed during this review because provider credentials and local Git metadata were unavailable. Current unit tests cover only a subset of the service; compilation and a green scan are insufficient to call this production-hardened.

The following findings are release blockers for reliable unattended backup storage:

| Priority | Finding | Location / next work |
|---|---|---|
| High | Queued backups are persisted as `queued`, but deserialization only accepts `processing`, `complete`, and `failed`. Workers cannot deserialize freshly queued records. | `crates/core` status enum and `Catalog` row conversion must include queued state, with an end-to-end job test. |
| High | Worker chunk collection keeps an extra `Arc` alive when `Arc::try_unwrap` runs; successful chunk processing can fail completion. | Drop the extra chunk sink owner before unwrapping or redesign callback results. |
| High | S3 `raw_locator` returns a bare key but `get` expects `s3://bucket/key`. Restore and TikTok primary-copy reads fail. | Unify locator contracts and add a local S3-compatible integration test. |
| High | Filesystem chunk writes do not create the `chunks/` parent directory and create a second hard link under a logical ID. GC only deletes the recorded path, potentially retaining data via the alias. | Create parent directories and make one canonical object path. |
| High | Stale ingestion locks have no heartbeats/fencing. Work lasting more than ten minutes can be reclaimed while still executing. Completion is not fully idempotent. | Add lease renewal, ownership-conditioned writes and crash/retry tests. |
| High | GC has no lease recovery, reference recheck immediately before deletion, bounded task pool, or grace period. Backup deletion and GC enqueue are separate transactions. | Make delete/GC scheduling atomic, fence collection against new references, recover abandoned GC tasks. |
| High | TikTok publishing reads catalog blob locations before this job records them. CDN upload is neither anonymously read back nor hash-verified; URL joining omits a separator for relative URIs. | Persist verified locations before publishing; fix joining; verify anonymous payload/decryption before completion. |
| Medium | Restore buffers the complete artifact. Chunk, PNG and zstd processing still permits memory amplification; size metadata isn't a complete decompression bound. | Bound decompression, total restore memory, metadata sizes and concurrent requests. CDN HTTP reads and PNG text inflation now have size limits. |
| Medium | CDN GET permits only HTTPS TikTok CDN hostnames and disables redirects, but DNS answers are not pinned/validated against private ranges. | Add resolver validation if hostile catalog/manifest input is supported. |
| Medium | Interrupted uploads leave plaintext staging files. Files are flushed but not fsynced; incomplete job creation can orphan them. | Atomic enqueue + backup creation, restrictive staging permissions, fsync policy and orphan cleanup. |
| Medium | SQLite upgrade logic suppresses ALTER errors and does not migrate all old columns (e.g. `error`). | Use versioned migrations tested against earlier schemas. |
| Medium | Readiness only checks SQLite, even if workers stopped. Malformed job JSON can escape retry handling. | Track worker health and handle malformed jobs explicitly. |
| Medium | Key rotation/profiles are absent. Random nonces prevent cross-backup ciphertext deduplication; old data needs its original key. | Version encryption profiles and retain recoverable key metadata without storing secrets. |

The intended invariants in `AGENTS.md` and `ARCHITECTURE.md` describe maintenance goals. The findings above explicitly identify places where the current implementation does not yet meet them. Do not expose secrets or rely on this service as the sole backup copy while these data-integrity issues remain unresolved.

## Build publication

`.dockerignore` admits only Cargo manifests, lockfile and Rust source as build inputs. Local databases, staging files, keys and environment files are excluded from the image context. GHCR uses the workflow-provided `GITHUB_TOKEN`; provider credentials are runtime-only and are not needed to build images. Inspect the image and scan dependencies as part of release work; the current workflows do not run a dependency vulnerability audit.
