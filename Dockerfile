# Production containers for Temporaless.
#
# The default (target-less) build is the ConnectStore server: it builds the
# example/py/production_server.py wiring; for your own service, replace the CMD
# line with your entrypoint. Multi-stage: builder installs deps + the editable
# package, runtime ships only the resulting venv + source. Result is ~140 MB on
# python:3.14-slim.
#
# `docker build --target console .` builds the optional read-only console
# (cmd/temporaless-console) instead: Node builds its UI, Go embeds it, and the
# distroless runtime carries only the binary, CA certificates, and the
# libffi/libgcc runtime OpenDAL's purego binding loads. At start OpenDAL
# unpacks one native library per storage service into TMPDIR and the console
# removes them once its stores are open, so /tmp must be writable and allow
# execution (an in-memory emptyDir when the root filesystem is read-only;
# Docker's --tmpfs needs the exec option). Mount the
# configuration at /etc/temporaless-console/console.yaml and every credential
# as a file it names; nothing else is written.
# `make image-check` builds this target from HEAD, runs it that way, and
# scans it with Trivy (scripts/image-check).

# Every base image is pinned by tag and digest here, once; update the tag and
# the digest together. IMAGE_VERSION, IMAGE_REVISION and IMAGE_CREATED set
# the OCI version, revision and created labels of both runtime stages.
ARG NODE_IMAGE="node:24.20.0-trixie-slim@sha256:50c3b2f6988dfc307b86e5301d69611af31f4789bdf232863b07d3b02fe55ae0"
ARG GO_IMAGE="golang:1.26.6-trixie@sha256:b75d466dd608587fd66cca705a307ba65b889827d06ad61d6a75f0482b51b7c7"
ARG DEBIAN_IMAGE="debian:trixie-slim@sha256:a99cfc517144bc59b1978475ec53b46ecabec7e43635402ee5b77cc54cd1b20a"
ARG DISTROLESS_IMAGE="gcr.io/distroless/base-debian13@sha256:0ebad3510af52aefe45045cc01b07564570be4feecf8d9f93d3a05d1b5f2f93b"
ARG PYTHON_IMAGE="python:3.14.6-slim@sha256:cea0e6040540fb2b965b6e7fb5ffa00871e632eef63719f0ea54bca189ce14a6"
ARG UV_IMAGE="ghcr.io/astral-sh/uv:0.11.28@sha256:0f36cb9361a3346885ca3677e3767016687b5a170c1a6b88465ec14aefec90aa"

FROM ${UV_IMAGE} AS uv

FROM ${NODE_IMAGE} AS console-ui
# npm installs the SHA-pinned dashboard framework from its public Git host.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src/cmd/temporaless-console/ui
COPY cmd/temporaless-console/ui/package.json cmd/temporaless-console/ui/package-lock.json cmd/temporaless-console/ui/.npmrc ./
RUN npm ci
COPY cmd/temporaless-console/ui/ ./
RUN npm run build

FROM ${GO_IMAGE} AS console-build
WORKDIR /src
COPY go.mod go.sum ./
COPY core/go core/go
COPY adapters/go adapters/go
COPY cmd/temporaless-console cmd/temporaless-console
COPY --from=console-ui /src/cmd/temporaless-console/assets/dist cmd/temporaless-console/assets/dist
RUN --mount=type=cache,target=/root/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/temporaless-console ./cmd/temporaless-console

FROM ${DEBIAN_IMAGE} AS console-runtime-libs
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libffi8 libgcc-s1 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /rootfs/etc/ssl /rootfs/etc/temporaless-console /rootfs/tmp \
    && cp -a /etc/ssl/certs /rootfs/etc/ssl/ \
    && for lib in /usr/lib/*/libffi.so.8* /usr/lib/*/libgcc_s.so.1*; do \
        mkdir -p "/rootfs$(dirname "$lib")"; \
        cp -a "$lib" "/rootfs$lib"; \
      done \
    && chmod 1777 /rootfs/tmp

FROM ${DISTROLESS_IMAGE} AS console
ARG DISTROLESS_IMAGE
ARG IMAGE_CREATED="1970-01-01T00:00:00Z"
ARG IMAGE_REVISION="unknown"
ARG IMAGE_VERSION="unknown"
LABEL org.opencontainers.image.base.name="${DISTROLESS_IMAGE}" \
      org.opencontainers.image.created="${IMAGE_CREATED}" \
      org.opencontainers.image.description="Temporaless read-only console" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.revision="${IMAGE_REVISION}" \
      org.opencontainers.image.source="https://github.com/jim-technologies/temporaless" \
      org.opencontainers.image.title="temporaless-console" \
      org.opencontainers.image.version="${IMAGE_VERSION}"
COPY --from=console-runtime-libs /rootfs/ /
# Root-owned and 0555 whatever the build host's umask: uid 65532 executes the
# binary and cannot modify it.
COPY --chmod=0555 --from=console-build /out/temporaless-console /usr/local/bin/temporaless-console
USER 65532:65532
EXPOSE 8080
# Probes: GET /healthz and GET /readyz.
ENTRYPOINT ["/usr/local/bin/temporaless-console", "-config", "/etc/temporaless-console/console.yaml"]

FROM ${PYTHON_IMAGE} AS builder
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never
COPY --from=uv /uv /uvx /usr/local/bin/

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

FROM ${PYTHON_IMAGE} AS runtime
ARG PYTHON_IMAGE
ARG IMAGE_CREATED="1970-01-01T00:00:00Z"
ARG IMAGE_REVISION="unknown"
ARG IMAGE_VERSION="unknown"
LABEL org.opencontainers.image.base.name="${PYTHON_IMAGE}" \
      org.opencontainers.image.created="${IMAGE_CREATED}" \
      org.opencontainers.image.description="Temporaless ConnectStore example server" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.revision="${IMAGE_REVISION}" \
      org.opencontainers.image.source="https://github.com/jim-technologies/temporaless" \
      org.opencontainers.image.title="temporaless" \
      org.opencontainers.image.version="${IMAGE_VERSION}"
ENV PATH="/app/core/py/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app
# Application code and the virtual environment stay root-owned and read-only
# to the unprivileged runtime user. Writable state belongs on an explicit
# volume/tmpfs or in the configured object-storage backend.
COPY --from=builder /app /app
COPY examples/py /app/examples/py
# Numeric, so a pod's runAsNonRoot admits the image without a runAsUser; the
# useradd above keeps uid 10001 named in /etc/passwd.
USER 10001:10001
WORKDIR /app

EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request, sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/readyz', timeout=2).status == 200 else 1)"

# Replace this CMD with your own server entrypoint. The default points at the
# canonical production_server.py wiring (auth + health + JSON logs + graceful
# shutdown). It deliberately has no credential or storage defaults and fails
# closed until AUTH_TOKEN and TEMPORALESS_STORAGE_SCHEME are configured.
CMD ["python", "examples/py/production_server.py"]
