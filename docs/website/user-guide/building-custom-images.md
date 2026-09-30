# Building Custom Operator Images

The official controller images published to `ghcr.io/kubeflow/spark-operator/controller`
are the recommended way to run Spark Operator. Some organizations, however, need to
build their own image: to use an approved base image, apply internal hardening, embed a
corporate CA bundle, or satisfy private registry conventions.

Every release attaches prebuilt `spark-operator` binaries so you can do this without
installing a Go toolchain or reproducing the project build.

## Release artifacts

Each GitHub release includes:

| Artifact | Contents |
| --- | --- |
| `spark-operator_<version>_linux_amd64.tar.gz` | `spark-operator` binary and `LICENSE` |
| `spark-operator_<version>_linux_arm64.tar.gz` | `spark-operator` binary and `LICENSE` |
| `SHA256SUMS` | SHA-256 checksums for the archives above |

The binaries are statically linked and carry the same version metadata as the official
images, so `spark-operator version` reports the released version, commit and build date.

## Downloading and verifying

```bash
VERSION=v2.5.1
ARCH=amd64

curl -fsSLO "https://github.com/kubeflow/spark-operator/releases/download/${VERSION}/spark-operator_${VERSION}_linux_${ARCH}.tar.gz"
curl -fsSLO "https://github.com/kubeflow/spark-operator/releases/download/${VERSION}/SHA256SUMS"

# Verify before use.
sha256sum --ignore-missing --check SHA256SUMS

tar -xzf "spark-operator_${VERSION}_linux_${ARCH}.tar.gz"
./spark-operator version
```

## Building a custom image

The operator is a self-contained, statically linked Go binary. It submits Spark
applications over the Kubernetes API (the native submitter) or to a REST submitter
service — it no longer shells out to `spark-submit`, so the runtime image needs **no
Spark distribution, no JVM, and no shell**. Spark itself is supplied entirely by the
user's driver/executor image. The official image therefore builds on
`gcr.io/distroless/static-debian12`, and a custom image can do the same.

```dockerfile
FROM gcr.io/distroless/static-debian12:nonroot

COPY spark-operator /usr/bin/spark-operator

ENTRYPOINT ["/usr/bin/spark-operator"]
```

Substitute your own approved base image as needed; any minimal base works, since the
binary has no runtime dependencies. Pinning the base image by digest, as the project's
own `Dockerfile` does, is recommended for reproducible builds.

Save this as `Dockerfile` next to the extracted `spark-operator`, then:

```bash
docker build -t my-registry.example.com/spark-operator:${VERSION} .
```

### Requirements for a custom base image

The binary is built with `CGO_ENABLED=0`, so it has no dynamic library dependencies.
If you replace the base image, the resulting image only needs to provide:

- CA certificates (for TLS to the Kubernetes API server). `distroless/static` and most
  minimal bases already include these.
- The webhook Deployment mounts its serving certificates at
  `/etc/k8s-webhook-server/serving-certs` from a Secret volume at runtime, so no
  directory needs to be created in the image.

The chart runs the container as a non-root user with a read-only root filesystem and all
capabilities dropped; a minimal static base image satisfies this by default.

## Using the image

Point the Helm chart at your registry:

```yaml
image:
  registry: my-registry.example.com
  repository: spark-operator
  tag: v2.5.1
```

```bash
helm upgrade --install spark-operator spark-operator/spark-operator \
    --namespace spark-operator \
    --create-namespace \
    -f values.yaml
```