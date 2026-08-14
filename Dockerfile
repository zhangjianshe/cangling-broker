# syntax=docker/dockerfile:1

FROM rust:1.88-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
RUN cargo build --release --locked --bin cangling-message

FROM debian:bookworm-slim
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
RUN useradd --system --uid 10001 --home-dir /data --create-home app
COPY --from=builder /app/target/release/cangling-message /usr/local/bin/cangling-message
USER app
WORKDIR /data
ENV DATABASE_URL=sqlite:///data/queue.db
EXPOSE 7500 7501
ENTRYPOINT ["cangling-message"]
