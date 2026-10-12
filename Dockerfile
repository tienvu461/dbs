FROM rust:1.99-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --locked -p backupctl

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home backupd

WORKDIR /app
COPY --from=builder /build/target/release/backupctl /usr/local/bin/backupctl
RUN mkdir -p /data/blobs && chown -R 10001:10001 /data
ENV BACKUPD_STORAGE_ROOT=/data/blobs
USER 10001:10001
ENTRYPOINT ["backupctl"]
