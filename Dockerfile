# syntax=docker/dockerfile:1.7
FROM rust:1.89-bookworm AS builder

WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release --no-default-features

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

RUN useradd -r -u 10001 -m -d /var/lib/dmt-indexer dmt

COPY --from=builder /src/target/release/dmt-indexer /usr/local/bin/dmt-indexer

USER dmt
WORKDIR /var/lib/dmt-indexer

ENV RUST_LOG=info
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/dmt-indexer"]
CMD ["run", "--config", "/etc/dmt-indexer/config.toml"]
