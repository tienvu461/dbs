# Architecture: stateless library and runtime adapters

## Current primary path

`backupd-lib` accepts a `futures::io::AsyncRead`, a logical filename, a configured encryption key and a `BlobStore`. It returns a `Manifest` only after every uploaded chunk has been retrieved and its stored bytes verified. It owns no SQL database, persistent queue, filesystem staging area, network listener or notification service.

```text
File/stdin/HTTPS reader        R2 object binding reader
          |                              |
          +---------- backupd-lib -------+
                      |
         chunk → hash → zlib → XChaCha20-Poly1305
                      |
         BlobStore.put → BlobStore.get → verify
                      |
             authenticated JSON manifest
                      |
            caller-controlled delivery/storage
```

## Modules and boundaries

| Component | Responsibilities |
|---|---|
| `crates/engine` / `backupd-lib` | Portable v2 manifest, chunk pipeline, authenticated restore, PNG envelope; pure Rust and futures I/O |
| `crates/backupctl` | Native file/stdin/URL source adapters; filesystem/S3/TikTok destinations; atomic local metadata/restore publication |
| `crates/cloudflare` | R2 event validation, queue acknowledgment/retry, R2/TikTok adapters, R2 manifest delivery |
| `crates/transport` | Native S3-compatible SDK and TikTok web uploader reused by CLI |
| Original `backupd/core/catalog/pipeline/source/worker` crates | Legacy SQLite service; not called by the new engine or Cloudflare wrapper |

The portable engine deliberately has no Tokio, SQLx, AWS SDK, OS filesystem or native C compression dependency. Adapters may use non-`Send` futures so the same interface works on the Workers event loop. Rust builds to `wasm32-unknown-unknown` with `workers-rs`.

## Durable contract: `backupd-stateless-v2`

The manifest records filename, nonsecret key ID, algorithm names, whole-file hash/size, and ordered chunks with plaintext/stored hashes, sizes and locators. Chunk identity is a stored-byte hash; provider locators are separate. Metadata is JSON, not an HLS playlist: arbitrary backup chunks are not media segments.

- Compression: **zlib**, using pure-Rust flate2. This new format is intentionally distinct from legacy `backupd-v1` with zstd.
- Encryption: XChaCha20-Poly1305. Each stored chunk contains a 24-byte random nonce followed by authenticated ciphertext and tag.
- AEAD associated data binds the format, chunk sequence, plaintext size and plaintext hash.
- Manifest authentication: keyed BLAKE3 over compact serde serialization with the authentication field empty, with a separate key derived using the context `backupd-stateless-v2 manifest authentication`.
- No master key, upload cookie, access credentials or source signed URL is serialized.

Restore authenticates the manifest **before fetching its locators**, checks stored hash/size before decryption, bounds decompression by declared chunk size, verifies plaintext hash/size and checks the final whole-file hash. The streaming API can write a valid prefix before a later failure; callers must discard partial output on error. The CLI handles that by publishing a temporary file only on success.

Default CLI chunks are 1 MiB; the library permits at most 8 MiB per chunk, 100,000 chunks, bounded stored objects and bounded manifest parsing. Payload memory is chunk-bounded, while metadata memory scales with the number of chunks. Network adapters must also enforce response limits. Public URL ingestion rejects redirects, validates all DNS results and pins them; private/HTTP sources require explicit opt-in.

## Cloudflare execution

R2 object-create notifications deliver bucket/key/size/ETag/event time through Queues. The consumer filters the configured input bucket/prefix, reads the object conditionally on its ETag and passes a stream to the engine. The logical source is private `r2://bucket/key`; no public upload URL is necessary.

Destination options are an R2 binding or the experimental TikTok PNG uploader. The engine always encrypts and read-back verifies either destination. Output is a signed manifest in a separate configured R2 bucket. Source objects are never automatically deleted.

An output key is derived from the source event and nonsecret key ID. Existing authenticated outputs permit duplicate delivery to be acknowledged without rerunning. Publication uses a conditional R2 write; an acknowledgment follows manifest read-back verification. Concurrent duplicate processing can still upload extra objects with fresh nonces, but cannot overwrite the accepted manifest. Queues provides retries and dead-letter handling; the library itself remains stateless. There is no SQL catalog.

## Ownership and remaining limits

The caller owns manifests, keys, input retention, destination credentials and uploaded-object cleanup. Lost metadata is not recoverable through a backup ID/catalog lookup. Random nonces mean encrypted uploads are not cross-backup deduplicated. Failures before manifest publication may leave orphan objects; manual/provider lifecycle policy must account for them.

Workers process a bounded whole input per queue invocation, not a resumable multi-invocation workflow. The example limits input to 16 MiB and uses one-message batches; large dumps need a future range/chunk orchestration layer or the native CLI/Kubernetes Jobs. Native and WASM compilation does not establish production CPU budgets or live provider compatibility.

The legacy service and its old deployment configuration remain for explicit migration work. Its known queue/GC defects listed in `SECURITY.md` are not inherited by the new engine, which never invokes those crates.
