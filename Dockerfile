# One image for every service (api, worker, dispatcher, downstream-mock); compose picks the command.
FROM python:3.12-slim

RUN pip install --no-cache-dir uv==0.12.17
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1

COPY pyproject.toml uv.lock README.md* ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY mock_downstream ./mock_downstream
RUN uv sync --frozen --no-dev

# Containers stop on SIGTERM; services handle it for a graceful shutdown.
STOPSIGNAL SIGTERM
