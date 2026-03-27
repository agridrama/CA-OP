FROM rust:1.86 AS builder

ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

WORKDIR /app

ARG CARGO_FEATURES="--no-default-features --features protocol-caop"

COPY benchmark/ ./benchmark/
COPY lib/ ./lib/
COPY vendor/ ./vendor/

RUN cargo build --manifest-path ./benchmark/Cargo.toml --release --bin server ${CARGO_FEATURES}

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends iproute2 iputils-ping \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/benchmark/target/release/server /usr/local/bin/server

EXPOSE 8000

ENTRYPOINT ["/usr/local/bin/server"]
