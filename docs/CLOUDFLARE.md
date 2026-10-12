# Cloudflare Rust Worker: R2-triggered stateless backups

## Topology

```text
backupd-input / incoming/* → object-create notification → backupd-uploads Queue
                                                        → Rust consumer
                                                        → R2 or TikTok chunks
                                                        → backupd-manifests / manifests/*.json
```

The consumer gets bucket/key/ETag from the event and reads through `INPUT_BUCKET`. It does not require a public R2 URL or an expiring presigned source URL. Conditional reads fail if the source was overwritten before processing. Upload inputs under immutable keys and retain them until metadata appears. Output buckets must be separate from the input bucket to prevent notification loops.

## Setup

Install Rust, the WASM target, npm/Wrangler and authenticate your Cloudflare account. From `crates/cloudflare`:

```sh
rustup target add wasm32-unknown-unknown
npx wrangler login
npx wrangler r2 bucket create backupd-input
npx wrangler r2 bucket create backupd-blobs
npx wrangler r2 bucket create backupd-manifests
npx wrangler queues create backupd-uploads
npx wrangler queues create backupd-failed
npx wrangler secret put BACKUP_MASTER_KEY
npx wrangler deploy
npx wrangler r2 bucket notification create backupd-input \
  --event-type object-create --queue backupd-uploads --prefix incoming/
```

Review `wrangler.toml` before deploying: binding names, bucket-name variables and actual bucket names must agree. The build command pins `worker-build` 0.8.7 to the workers-rs dependency. The Queue handler runs only on events; it does not start a TCP listener or persistent poller. `workers_dev` is disabled and no HTTP upload handler is exposed.

The wrapper writes the manifest to R2. That is the initial designated delivery channel; email/Slack notification destinations are deferred. Retrieve and retain the JSON through your own R2 access tooling. Do not delete it after receiving a notification.

To restore an R2-destination manifest locally, set `S3_ENDPOINT_URL` to your account's R2 S3 endpoint, `S3_BUCKET` to `backupd-blobs`, `AWS_REGION=auto` and bucket read credentials. Then run `backupctl --store s3 restore saved-manifest.json --output restored.bin`. The native adapter resolves the authenticated `r2://bucket/key` locator through that API without modifying the manifest. For TikTok destinations, use `--store tiktok` instead. Use the same `--key-id` as the Worker's `KEY_ID` and the original key.

## TikTok destination

Set `DESTINATION = "tiktok"`, then configure secrets with Wrangler:

```sh
npx wrangler secret put TIKTOK_ORG_ID
npx wrangler secret put TIKTOK_COOKIE
npx wrangler secret put TIKTOK_CSRF_TOKEN
npx wrangler deploy
```

CSRF is optional in code but may be required by your authenticated Business Center session. Requests use undocumented image upload/registration endpoints. The sink wraps encrypted chunks as PNG iTXt payloads and verifies them using anonymous CDN GET before manifest publication. Credentials never appear in the output JSON; restore through `backupctl --store tiktok` needs the key but no cookie. Provider image-size limits, cookie lifetime and payload preservation still need live testing with your account.

## Limits and retry behavior

- Example: 1 MiB chunks, 16 MiB maximum input and batch size one. Payload buffers are bounded; no whole-file buffering is used for source reads.
- CPU limit is configured to 300,000 ms; check your plan's Queue CPU/wall-time/subrequest limits. Compilation alone does not validate runtime budgets. Read-back verification adds one GET per chunk, plus provider-specific upload requests.
- Each invocation processes the entire input. Large dumps should use the native CLI/on-demand container until range-based multi-invocation work is implemented.
- Queue failures retry up to the configured budget, then reach `backupd-failed`. Source objects remain intact. Inspect failures without logging provider secrets or complete message/request bodies.
- Repeated deliveries use an event-derived manifest key and authenticated existing output. Concurrent duplicates/failed attempts can leave extra chunks. There is no SQL job state or progress checkpoint.
- Manifest writes are conditional and read back before acknowledgment. Do not rotate the key or `KEY_ID` while work is queued without retaining the original key.

## Verification

```sh
cargo check -p backupd-cloudflare --target wasm32-unknown-unknown --locked
npx wrangler deploy --dry-run
```

A live smoke test should upload a small immutable input under `incoming/`, wait for its manifest in the output bucket, restore it and compare bytes. Test duplicate events, overwritten inputs, failed output writes and failed provider uploads. Native tests cover manifest integrity, corruption rejection, PNG payload round-trip and event filtering; they do not replace live Cloudflare/provider tests.
