# Stateless API and migration

## Embed the library

Add `backupd-lib` from `crates/engine`. Implement `BlobStore` for the runtime/provider, then call:

```rust,ignore
let uploader = backupd_lib::Uploader::new(key, "key-2026".into(), 1024 * 1024, max_bytes)?;
let manifest = uploader.upload(input_reader, "database.dump".into(), &destination).await?;
let metadata_file = manifest.to_json()?;
// Persist metadata_file somewhere the caller controls.
```

Inputs are asynchronous byte readers. Native wrappers open files/stdin or HTTPS responses; the Cloudflare wrapper turns an R2 object body stream into the same reader. `BlobStore.get` must return the exact original encrypted bytes: a PNG/CDN adapter extracts its envelope before returning. Errors should not contain URLs with query credentials, provider bodies or headers.

Native applications can enable the library's `native` feature and call `backupd_lib::native::upload_source(&uploader, file_or_url, filename, &store, false)`. This opens a file/stdin/HTTPS link and returns the same portable manifest. Leave that feature disabled for Cloudflare; it adds Tokio and reqwest only on native targets.

Any upload result is a complete, read-back verified manifest; errors do not return partial manifests as successful output. There is no server-generated backup ID needed for restore. Save the returned manifest and the encryption key independently. `key_id` labels the key without revealing it.

For restore, deserialize the manifest and pass it to `Uploader.restore`. The engine authenticates metadata first and verifies every chunk before writing it. Treat the output as unsuccessful until the future resolves successfully. Do not publish/rename an output file early.

## Format compatibility

Stateless v2 uses pure-Rust zlib and mandatory XChaCha20-Poly1305 plus an authenticated manifest. Legacy v1 uses zstd and SQLite metadata, with different locator semantics. They are not interchangeable. Restore old backups using the original service/tooling and re-upload their recovered plaintext using the new CLI; keep original copies until the new manifest's restore has been verified.

The portable engine's data types live in `crates/engine`; legacy `backupd-core` types are not the v2 contract. Consumers should reject unknown versions rather than guessing which decompressor/encryption path to use.

## Lifecycle and retries

The library is stateless, not storage-free. Providers hold chunks and callers hold manifests. It has no automatic retention or GC because it cannot know which manifests users still retain. Failed/repeated attempts may leave orphan objects, especially when fresh random encryption nonces change object identities. Never delete objects based only on one manifest without checking all retained manifests.

Metadata delivery happens outside the engine. Today the native CLI writes JSON atomically and the Cloudflare consumer writes it to R2. Future email/Slack handlers should deliver a manifest reference after durable publication, never the key or session credentials. The R2 output object is the completion record even if a future notification channel is down.
