# Builder stage
FROM python:3.13-slim AS builder

WORKDIR /app

ENV POETRY_VERSION=2.1.1 \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install "poetry==$POETRY_VERSION"

COPY pyproject.toml poetry.lock* README.md ./

RUN poetry install --only main --no-root

COPY simcc_etl ./simcc_etl
RUN poetry install --only main

# Final runner stage
FROM python:3.13-slim AS runner

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 appuser && \
    mkdir -p /app/storage /app/logs && \
    chown -R appuser:appuser /app

COPY --from=builder --chown=appuser:appuser /app/.venv /app/.venv
COPY --chown=appuser:appuser simcc_etl ./simcc_etl
COPY --chown=appuser:appuser migrations ./migrations
COPY --chown=appuser:appuser alembic.ini pyproject.toml README.md ./
COPY --chown=appuser:appuser storage ./storage

USER appuser

EXPOSE 8000

CMD ["uvicorn", "simcc_etl.app:app", "--host", "0.0.0.0", "--port", "8000"]
