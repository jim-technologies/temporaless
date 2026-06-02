# temporaless — the storage-first workflow ConnectStore server, as a
# long-running Nomad service on the shared medallion cluster (alongside
# medallionapi / ghdrive / xscraper / timescaledb).
#
#   nomad job run -var-file=vars.hcl deploy/temporaless.nomad.hcl
#
# Declarative: this file is desired state; `nomad job run` reconciles to it.
#
# DELIBERATE driver choice: temporaless ships its OWN Dockerfile (the bundled
# production_server.py wiring: ConnectStore + bearer auth + /healthz + /readyz +
# JSON logs + graceful SIGTERM). We run that image with the `docker` driver —
# the same supply-chain-clean exception the cluster already makes for the
# official timescaledb image — instead of raw_exec/flox.
#
# Storage: the bundled server defaults to an OpenDAL `fs` backend. For a real
# deploy point TEMPORALESS_STORAGE_ROOT at a persistent path backed by the
# `temporaless_data` host volume (declared in agent.hcl), OR rebuild the image
# with an S3/GCS-backed server and drop the volume — the records are durable in
# object storage and processes stay interchangeable.

variable "image" {
  type        = string
  description = "Fully-qualified temporaless image (registry/repo:tag). Build from the repo Dockerfile and push to your registry."
  default     = "ghcr.io/jim-technologies/temporaless:latest"
}

variable "version" {
  type        = string
  description = "Deploy version stamp. Bump (e.g. to the git SHA) to force a rolling redeploy when the image tag is mutable (e.g. :latest)."
  default     = "dev"
}

variable "auth_token" {
  type        = string
  description = "Bearer token the ConnectStore auth interceptor requires (pass via -var or -var-file; do not commit)."
  sensitive   = true
}

variable "node_pool_constraint" {
  type        = string
  description = "Pin to the node that owns the temporaless_data host volume (its hostname). Required only for the fs backend; clear it when using S3/GCS."
  default     = "medallion-0"
}

job "temporaless" {
  # `datacenters` is explicit by convention; "*" = any DC this cluster has.
  datacenters = ["*"]
  type        = "service"

  # The fs storage backend is pinned to one node's host volume. Remove this
  # constraint when the image is rebuilt against S3/GCS (records are then
  # durable in object storage and any node can serve).
  constraint {
    attribute = "${attr.unique.hostname}"
    value     = var.node_pool_constraint
  }

  meta {
    version = var.version
  }

  # Controlled rolling deploy: one alloc at a time, must pass its health
  # checks, and auto-revert if a new version fails.
  update {
    max_parallel      = 1
    health_check      = "checks"
    min_healthy_time  = "10s"
    healthy_deadline  = "3m"
    progress_deadline = "10m"
    auto_revert       = true
    canary            = 0
  }

  group "server" {
    count = 1

    # Local restarts on crash, then reschedule to another node if it keeps
    # failing.
    restart {
      attempts = 3
      interval = "5m"
      delay    = "15s"
      mode     = "delay"
    }
    reschedule {
      delay          = "30s"
      delay_function = "exponential"
      max_delay      = "1h"
      unlimited      = true
    }

    # Persistent storage for the fs OpenDAL backend. Declared in agent.hcl
    # client{}. Drop this (and the volume_mount + constraint) for S3/GCS.
    volume "data" {
      type      = "host"
      source    = "temporaless_data"
      read_only = false
    }

    network {
      port "http" {
        static = 8080 # matches EXPOSE 8080 / PORT in the image
      }
    }

    task "server" {
      driver = "docker"

      config {
        image = var.image
        ports = ["http"]
      }

      volume_mount {
        volume      = "data"
        destination = "/data"
      }

      env {
        PORT                     = "8080"
        AUTH_TOKEN               = var.auth_token
        TEMPORALESS_STORAGE_ROOT = "/data"
        # Stamp the running code version onto every record so replay identity
        # guards (assertWorkflowIdentity / assertActivityIdentity) reject drift.
        TEMPORALESS_CODE_VERSION = var.version
      }

      # Let in-flight requests drain on SIGTERM (the server installs a graceful
      # shutdown handler).
      kill_timeout = "30s"

      service {
        name     = "temporaless"
        port     = "http"
        provider = "nomad" # swap to "consul" for multi-node discovery

        # /readyz returns 200 only after the store + scheduler are wired and
        # 503 during shutdown — the correct readiness signal for a load
        # balancer. (/healthz is liveness-only.)
        check {
          name     = "readyz"
          type     = "http"
          path     = "/readyz"
          interval = "15s"
          timeout  = "3s"
        }
      }

      resources {
        cpu    = 500
        memory = 512
      }
    }
  }
}
