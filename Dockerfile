# syntax=docker/dockerfile:1

FROM ghcr.io/astral-sh/uv:0.13.0 AS uv

FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_NO_DEV=1

COPY --from=uv /uv /usr/local/bin/uv

RUN useradd --create-home --uid 1000 canary
WORKDIR /app

# Dependencies first, so code changes do not invalidate this layer.
COPY pyproject.toml uv.lock .python-version ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project

COPY src ./src
COPY dbt ./dbt
COPY deploy ./deploy
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked

ENV PATH="/app/.venv/bin:${PATH}" \
    DAGSTER_HOME=/app/dagster_home \
    CANARY_DBT_PROJECT_DIR=/app/dbt

# Bake the dbt manifest in, so the code location loads without parsing at start-up.
RUN mkdir -p "${DAGSTER_HOME}" /app/data \
    && cp deploy/dagster.yaml "${DAGSTER_HOME}/dagster.yaml" \
    && DBT_TARGET=local dbt parse --project-dir /app/dbt --profiles-dir /app/dbt --target-path /app/dbt/target \
    && chown -R canary:canary /app

USER canary
EXPOSE 3000

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-m", "complaint_canary.definitions"]
