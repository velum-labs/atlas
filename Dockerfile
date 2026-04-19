# ---- builder ----
FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy workspace root manifests first for layer caching
COPY pyproject.toml uv.lock ./

# Copy all workspace packages
COPY packages/ packages/

# Install all packages (production only, no dev deps)
RUN uv sync --frozen --no-dev --no-editable

# ---- runtime ----
FROM python:3.12-slim

WORKDIR /app

# Copy the installed venv from builder
COPY --from=builder /app/.venv /app/.venv

# Put the venv on PATH
ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8080

ENTRYPOINT ["alma-atlas"]
CMD ["--help"]
