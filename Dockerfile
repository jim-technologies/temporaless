# Production container for the Temporaless ConnectStore server.
# Builds the example/py/production_server.py wiring; for your own service,
# replace the CMD line with your entrypoint.
#
# Multi-stage: builder installs deps + the editable package, runtime ships
# only the resulting venv + source. Result is ~140 MB on python:3.14-slim.

FROM python:3.14.6-slim@sha256:d3400aa122fa42cf0af0dbe8ec3091b047eac5c8f7e3539f7135e86d855dc015 AS builder
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never
COPY --from=ghcr.io/astral-sh/uv:0.11.28@sha256:0f36cb9361a3346885ca3677e3767016687b5a170c1a6b88465ec14aefec90aa /uv /uvx /usr/local/bin/

# System deps OpenDAL's Python binding needs at runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY core/py /app/core/py
COPY README.md /app/README.md
# The library exposes ASGI and leaves the server optional. The image selects
# the lockfile-backed `server` extra so every runtime dependency is resolved
# during the repository lock update, not during the container build.
RUN cd core/py && uv sync --frozen --no-dev --extra server

FROM python:3.14.6-slim@sha256:d3400aa122fa42cf0af0dbe8ec3091b047eac5c8f7e3539f7135e86d855dc015 AS runtime
ENV PATH="/app/core/py/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app
COPY --from=builder --chown=app:app /app /app
COPY --chown=app:app examples/py /app/examples/py
USER app
WORKDIR /app

EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request, sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/readyz', timeout=2).status == 200 else 1)"

# Replace this CMD with your own server entrypoint. The default points at the
# canonical production_server.py wiring (auth + health + JSON logs + graceful
# shutdown). It deliberately has no credential or storage defaults and fails
# closed until AUTH_TOKEN and TEMPORALESS_STORAGE_SCHEME are configured.
CMD ["python", "examples/py/production_server.py"]
