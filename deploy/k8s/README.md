# Kubernetes deployment

A production-shaped baseline for deploying the Temporaless ConnectStore + your workflow service. Treat these manifests as a starting point — you'll likely customize image names, secret sources, and autoscaling for your environment.

## Files

- [`namespace.yaml`](namespace.yaml) — the `temporaless` namespace.
- [`deployment.yaml`](deployment.yaml) — Deployment + PodDisruptionBudget + HorizontalPodAutoscaler for the ConnectStore + workflow service.
- [`service.yaml`](service.yaml) — ClusterIP Service exposing the gRPC port internally.
- [`configmap.yaml`](configmap.yaml) — non-secret config (storage backend, log level).
- [`secret.example.yaml`](secret.example.yaml) — template for the bearer-token secret. **Do not commit a real secret** — generate with `kubectl create secret` or pull from your secret manager (Vault / SSM / GCP Secret Manager).
- [`networkpolicy.yaml`](networkpolicy.yaml) — restrict ingress to in-cluster callers + egress to S3/GCS/Azure Blob.

## Apply

```sh
kubectl apply -f deploy/k8s/namespace.yaml
kubectl create secret generic temporaless-auth \
    --namespace=temporaless \
    --from-literal=auth-token="$(openssl rand -hex 32)"
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/deployment.yaml
kubectl apply -f deploy/k8s/service.yaml
kubectl apply -f deploy/k8s/networkpolicy.yaml
```

## What's enforced

- **Non-root user** (uid 10001, matches the bundled `Dockerfile`).
- **Read-only root filesystem** + writable emptyDir for `/tmp` only.
- **All capabilities dropped**, no privilege escalation.
- **Liveness probe** on `/healthz` (restart on hang).
- **Readiness probe** on `/readyz` (drain during startup + shutdown).
- **`terminationGracePeriodSeconds: 45`** so SIGTERM has time to flush in-flight RPCs (uvicorn's default graceful shutdown is 30s; add 15s margin).
- **`preStop` hook** sleeps 5s after SIGTERM signaling so the load balancer's `/readyz` check flips to 503 *before* the process actually exits — avoids the small race where new traffic hits a dying pod.
- **PodDisruptionBudget** with `minAvailable: 1` so cluster operations never take all replicas down at once.
- **HPA on CPU + custom metric placeholder** (request rate); customize for your service mesh.
- **Resource requests/limits** with sensible defaults — tune on real traffic.

## What's NOT here

- **Service mesh / mTLS config.** If you run Istio / Linkerd / Cilium, configure the mesh-level sidecar separately. The bearer-token interceptor still works behind any mesh.
- **Cert-manager / TLS termination.** Use your cluster's standard ingress + cert-manager — the manifest is internal-only by default.
- **Storage credentials.** S3/GCS/Azure credentials should come from IRSA / Workload Identity / Pod Identity, not a Secret. The example uses `fs:` for development; flip to a cloud scheme + the right credential mount for prod.
- **Observability sidecars.** OTel collector, Datadog agent, etc. layer on top — the application emits structured JSON to stdout already.

## Validation

```sh
kubectl apply --dry-run=client -f deploy/k8s/
```

Run [`docs/production-checklist.md`](../../docs/production-checklist.md) end-to-end before each new environment.
