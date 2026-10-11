# Repository guide for coding agents

## Project snapshot

`backupd` is a Rust workspace for a standalone backup ingestion and restore service. It uses Axum, Tokio, SQLite, a chunk/compress/encrypt pipeline, and pluggable blob transports. Read [`ARCHITECTURE.md`](ARCHITECTURE.md) for the current implementation boundaries and known gaps; the older [`backupd-architecture-design.md`](backupd-architecture-design.md) is a target design document and is not a complete description of the code today.

## Workspace map

- `crates/backupd`: HTTP API, environment-driven service construction, health endpoints.
- `crates/core`: backup, chunk, artifact, and manifest data types.
- `crates/catalog`: SQLite schema, backup/job records, blob references/locations, and GC queue.
- `crates/pipeline`: fixed-size chunking, zstd, BLAKE3, XChaCha20-Poly1305 and integrity checks.
- `crates/source`: local-file and stream source abstractions.
- `crates/transport`: `BlobTransport`, filesystem and S3-compatible implementations, experimental TikTok Business image uploader and PNG payload codec.
- `crates/worker`: persisted ingestion worker and GC retry loop.
- `.github/workflows/build-image.yml`: publishes multi-platform images to GHCR on `main` pushes.

## Development commands

Use a current stable Rust toolchain (the container build uses Rust 1.99):

```sh
cargo fmt --all -- --check
cargo check --workspace --locked
cargo test --workspace --locked
cargo run -p backupd
```

`cargo run -p backupd` uses the filesystem transport and creates `data/` by default. To validate the container locally:

```sh
docker build --platform linux/amd64 -t backupd:dev .
```

## Implementation conventions

- Keep transport-specific locator/authentication behavior behind `BlobTransport`; manifests should hold logical chunk hashes plus locator metadata, not provider SDK objects.
- Store chunk bytes before catalog completion. Only mark a backup complete once the object locations have been verified and its complete manifest and chunk references are persisted.
- Keep jobs durable in SQLite. A process restart must be able to reclaim stale work. Make job processing idempotent: uploads to the same content-addressed key must validate identical content and repeated completion must not duplicate artifact references.
- Preserve the distinction between the plaintext digest (`ChunkManifest::plaintext_hash`) and the stored-byte digest (`stored_hash`). Verify stored bytes before decrypt/decompress and verify plaintext before restore output.
- Never emit keys, TikTok cookies, AWS credentials, or full sensitive request headers in logs or API errors.
- New config should be documented in `README.md` and `backupd.yaml` if the file is intended as an operator example. The current service reads environment variables; `backupd.yaml` is illustrative and is not parsed at runtime.
- Keep normal tests hermetic. Live AWS/R2/Garage/TikTok checks require credentials and should be opt-in, never a default unit test.

## Security / operational constraints

- Do not add a default public bind; preserve the loopback default unless deployment config explicitly overrides it.
- Do not publish unencrypted backup content to opportunistic/public transports. TikTok publishing must fail closed unless encryption is configured.
- Do not log or persist TikTok session secrets in the catalog. Use environment or a secret manager.
- Filesystem locators and object keys are untrusted input; reject traversal and absolute paths.
- SQLite, staging files, and local blobs need persistent storage when running in a container. Mount a durable volume at `/data`.
- TikTok integration depends on undocumented TikTok Business web endpoints and may break when TikTok changes its UI/backend behavior. Do not represent it as a supported official API.

## Change verification

Before finishing code changes, run formatting, workspace check, and tests. For changes touching upload, jobs, transports, or manifests, also exercise the relevant HTTP flow or add a focused test. Do not claim live provider verification without actually running against that provider.
