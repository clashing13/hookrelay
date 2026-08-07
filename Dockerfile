# Patch and Debian-suite tags are deliberate reproducibility pins. Refresh them
# through a reviewed change that rebuilds and tests the image for vulnerabilities.
ARG PYTHON_IMAGE=python:3.12.13-slim-bookworm

FROM ${PYTHON_IMAGE} AS builder

ARG UV_VERSION=0.12.1

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

RUN python -m pip install --no-cache-dir "uv==${UV_VERSION}"

# Install locked third-party dependencies before copying source code so that
# ordinary source edits can reuse this relatively expensive layer.
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
RUN uv sync --frozen --no-dev --no-editable


FROM ${PYTHON_IMAGE} AS runtime

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN groupadd --gid 10001 hookrelay \
    && useradd --uid 10001 --gid hookrelay --no-create-home --no-log-init \
        --home-dir /app --shell /usr/sbin/nologin hookrelay

COPY --from=builder /app/.venv /app/.venv
# Migration code is root-owned and readable by the service account. The API
# process must not be able to rewrite code used by a later operator command.
COPY alembic.ini ./alembic.ini
COPY migrations ./migrations

USER 10001:10001

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health/live', timeout=2).close()"]

CMD ["hookrelay"]
