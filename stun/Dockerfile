FROM python:3.12-alpine AS builder

WORKDIR /build

# Copy requirements and install dependencies
COPY pyproject.toml .
RUN apk add --no-cache build-base && \
    pip install --no-cache-dir build && \
    python -m build --wheel

FROM caddy:2-alpine

WORKDIR /app

# Copy the built wheel from the builder stage
COPY --from=builder /build/dist/*.whl .
RUN apk add --no-cache python3 && \
    python3 -m venv /app/venv && \
    . /app/venv/bin/activate && \
    pip install --no-cache-dir *.whl && \
    rm *.whl

# Copy source code
COPY src/ ./src/

# Copy Caddyfile
COPY caddy/Caddyfile /etc/caddy/Caddyfile
# Copy self-signed certificates
COPY caddy/certs/ /etc/caddy/certs/

COPY entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV HOST=0.0.0.0
ENV PORT=8089

# Expose the port the app runs on
EXPOSE 443
EXPOSE 8089

ENTRYPOINT ["/app/entrypoint.sh"]
