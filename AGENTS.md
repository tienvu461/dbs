# Repository guide for coding agents

## Project snapshot

`backupd` is primarily a stateless Rust upload/restore library, one-shot CLI and Cloudflare Rust Queue consumer. Read [`ARCHITECTURE.md`](ARCHITECTURE.md). The original SQLite/Axum service is retained for migration, with known defects documented in `SECURITY.md`; do not route the new library through its catalog/job code. The older design document describes the historical stateful target.

## Workspace map

- `crates/engine`: `backupd-lib`, portable authenticated v2 manifests and bounded encrypted chunk processing; no Tokio/SQLite/native SDK dependencies.
- `crates/backupctl`: stateless native file/stdin/HTTPS adapters and filesystem/S3/TikTok upload/restore command.
- `crates/cloudflare`: workers-rs R2/Queue wrapper and R2 metadata output; check the WASM target explicitly.
- The entries below describe the retained legacy service and native transport helpers.

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
cargo check -p backupd-cloudflare --target wasm32-unknown-unknown --locked
cargo run -p backupctl -- --help
```

`cargo run -p backupd` explicitly runs the legacy server. The default Docker image runs `backupctl`. To validate the container locally:

```sh
docker build --platform linux/amd64 -t backupd:dev .
```

## Implementation conventions

- Keep transport-specific locator/authentication behavior behind `BlobTransport`; manifests should hold logical chunk hashes plus locator metadata, not provider SDK objects.
- Store chunk bytes before catalog completion. Only mark a backup complete once the object locations have been verified and its complete manifest and chunk references are persisted.
- New engine APIs must remain stateless: return portable metadata to callers; no SQL catalog or process-local completion registry. Runtime wrappers own retry/delivery policy. Authenticate manifests before resolving their locators, and read back every uploaded chunk before returning success.
- Keep native SDKs, filesystem and Tokio out of the portable engine/WASM dependency graph. Workers read private input through bindings and use separate input/output buckets.
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
