# Production container for the Temporaless ConnectStore server.
# Builds the example/py/production_server.py wiring; for your own service,
# replace the CMD line with your entrypoint.
#
# Multi-stage: builder installs deps + the editable package, runtime ships
# only the resulting venv + source. Result is ~140 MB on python:3.13-slim.

FROM python:3.13-slim AS builder
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# System deps OpenDAL's Python binding needs at runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY core/py /app/core/py
COPY README.md /app/README.md
# `uvicorn` is a dev-only dep of the library (the library exposes ASGI; the
# server is the user's choice). For the bundled production_server.py example,
# install it as a runtime dep explicitly so the image is self-contained.
RUN cd core/py && uv sync --frozen --no-dev && uv pip install --python .venv/bin/python uvicorn>=0.34.0

FROM python:3.13-slim AS runtime
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

# Replace this CMD with your own server entrypoint. The default points at
# the canonical production_server.py wiring (auth + health + JSON logs +
# graceful shutdown).
CMD ["python", "examples/py/production_server.py"]
