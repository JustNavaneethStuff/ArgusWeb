FROM python:3.12-slim AS base

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml ./
COPY packages/ packages/
COPY services/ services/
COPY alembic/ alembic/
COPY alembic.ini ./

RUN uv sync --all-packages

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app/packages/argus-core/src:/app/packages/argus-events/src:/app/packages/argus-observability/src:/app/services/scheduler/src:/app/services/crawler/src:/app/services/parser/src:/app/services/cleaner/src:/app/services/api/src"

FROM base AS scheduler
CMD ["python", "-m", "scheduler.main"]

FROM base AS crawler
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libdbus-1-3 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
    libxrandr2 libgbm1 libpango-1.0-0 libcairo2 libasound2 \
    && rm -rf /var/lib/apt/lists/*
RUN playwright install chromium --with-deps
CMD ["python", "-m", "crawler.main"]

FROM base AS parser
CMD ["python", "-m", "parser.main"]

FROM base AS cleaner
CMD ["python", "-m", "cleaner.main"]

FROM base AS api
CMD ["python", "-m", "api.main"]
