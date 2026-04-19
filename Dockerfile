# ---- builder ----
FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy workspace root manifests first for layer caching
COPY pyproject.toml uv.lock ./

# Copy all workspace packages
COPY packages/ packages/

# Install workspace packages (prod only)
RUN uv sync --frozen --no-dev --package alma-atlas

# ---- runtime ----
FROM python:3.12-slim

WORKDIR /app

# Editable workspace installs need the source tree at runtime
COPY --from=builder /app/pyproject.toml /app/uv.lock ./
COPY --from=builder /app/packages/ /app/packages/

# Copy the installed venv from builder
COPY --from=builder /app/.venv /app/.venv

# Put the venv on PATH
ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8080

ENTRYPOINT ["alma-atlas"]
CMD ["--help"]
