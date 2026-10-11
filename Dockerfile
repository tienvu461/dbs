FROM rust:1.99-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --locked -p backupd

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home backupd

WORKDIR /app
COPY --from=builder /build/target/release/backupd /usr/local/bin/backupd
EXPOSE 8080
ENV BACKUPD_BIND=0.0.0.0:8080 \
    DATABASE_URL=sqlite:///data/backup.db \
    BACKUPD_STORAGE_ROOT=/data/blobs \
    BACKUPD_STAGING_ROOT=/data/staging
VOLUME ["/data"]
RUN mkdir -p /data/blobs /data/staging && chown -R 10001:10001 /data
USER 10001:10001
ENTRYPOINT ["backupd"]
