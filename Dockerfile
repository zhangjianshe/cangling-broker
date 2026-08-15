# syntax=docker/dockerfile:1

FROM rust:1.88-bookworm AS builder
WORKDIR /app
ARG GIT_HASH
ARG BUILD_TIME
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    GIT_HASH="$GIT_HASH" BUILD_TIME="$BUILD_TIME" \
    cargo build --release --locked --bin cangling-broker \
    && cp /app/target/release/cangling-broker /usr/local/bin/cangling-broker

FROM debian:bookworm-slim
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
RUN useradd --system --uid 10001 --home-dir /data --create-home app \
    && mkdir -p /data/logs \
    && chown -R app:app /data
COPY --from=builder /usr/local/bin/cangling-broker /usr/local/bin/cangling-broker
USER app
WORKDIR /data
ENV CL_BROKER_DATA=/data
EXPOSE 7500 7501
ENTRYPOINT ["cangling-broker"]
