FROM python:3-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app
COPY pyproject.toml uv.lock /app/
RUN uv sync --frozen --no-install-project --no-dev
COPY mbstats /app/mbstats
COPY README.md /app/
RUN uv sync --frozen --no-dev
