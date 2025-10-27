# syntax=docker/dockerfile:1.7

# ---------------------------------------------------------------------------
# Base layer with system packages and requirements cache
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential \
      gcc \
      curl \
      libpq-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements once so dependency layers can be cached.
# dev container使用
# COPY backend/requirements /tmp/requirements   
# 本地使用
COPY requirements /tmp/requirements

# ---------------------------------------------------------------------------
# Test / CI stage (installs dev.txt -> includes pytest & tooling)
# docker build --target test --build-arg REQUIREMENTS_FILE=dev.txt -t backend-test .
# docker run --rm backend-test -vv backend/tests/orchestration/test_scheduler_tick.py
# ---------------------------------------------------------------------------
FROM base AS test

ARG REQUIREMENTS_FILE=dev.txt
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r /tmp/requirements/${REQUIREMENTS_FILE}

WORKDIR /workspace
COPY backend /workspace/backend

ENV PYTHONPATH=/workspace/backend

ENTRYPOINT ["pytest"]
CMD ["-vv", "backend/tests"]


# ---------------------------------------------------------------------------
# Runtime image (installs prod requirements by default)
# docker build -t backend-runtime .
# docker build --build-arg REQUIREMENTS_FILE=dev.txt -t backend-dev .
# ---------------------------------------------------------------------------
FROM base AS runtime

ARG REQUIREMENTS_FILE=prod.txt
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r /tmp/requirements/${REQUIREMENTS_FILE}

WORKDIR /app
# dev container 用
# COPY backend/alembic.ini /app/alembic.ini
# COPY backend/app /app/app
# COPY backend/entrypoint.sh /app/entrypoint.sh
# RUN chmod +x /app/entrypoint.sh

# 本地用
COPY alembic.ini /app/backend/alembic.ini
COPY app /app/backend/app
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --retries=5 \
  CMD curl -fsS http://localhost:8000/api/v1/health || exit 1

# 默认命令（在 compose 中可改为 uvicorn --reload）
CMD ["/app/entrypoint.sh", "gunicorn", "-c", "app/core/gunicorn_conf.py", "app.main:app"]
