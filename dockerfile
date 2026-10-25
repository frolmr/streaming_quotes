# syntax=docker/dockerfile:1
# ---- Build stage ----
FROM rust:1.85-slim-bookworm AS builder

WORKDIR /app

COPY . .

RUN cargo build --release --bin server --bin client

# ---- Runtime stage ----
FROM debian:bookworm-slim AS runtime

RUN useradd -r -s /bin/false appuser

WORKDIR /app

COPY --from=builder --chown=appuser:appuser /app/target/release/server /usr/local/bin/server
COPY --from=builder --chown=appuser:appuser /app/target/release/client /usr/local/bin/client
RUN chmod +x /usr/local/bin/server /usr/local/bin/client

USER appuser

ENTRYPOINT ["/usr/local/bin/server"]
