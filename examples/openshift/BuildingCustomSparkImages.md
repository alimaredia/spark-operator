# Building Custom Spark Images for OpenShift

This guide covers best practices for building custom Spark container images that are compatible with OpenShift's security model.

## Prerequisites

- Docker or Podman installed
- Access to a container registry (Quay.io, Docker Hub, etc.)
- Familiarity with the main [Spark Operator on OpenShift guide](./SparkOperatorOnOpenShift.md)

---

## OpenShift Image Compatibility (Arbitrary UID)

OpenShift enforces running containers with a random, non-root User ID (UID) as part of its `restricted-v2` Security Context Constraints. Your container images must be designed to handle this.

### Key Requirements

The `Dockerfile` must ensure:

- All directories are owned by **Group ID 0 (root)** and made **group-writable** (`chmod -R g=u`)
- This allows the arbitrary non-root UID (who is a member of Group 0) to read/write all necessary paths
- `ENV HOME=/home/spark` is set for Spark temp files
- **No hardcoded UID** - remove `USER 185` and `useradd` commands
- **No `USER` directive at the end** - OpenShift assigns the arbitrary UID at runtime

### Dockerfile Example

```dockerfile
# Set Spark directories to be owned by group 0 and group-writable
RUN chgrp -R 0 /opt/spark && \
    chmod -R g=u /opt/spark && \
    mkdir -p /opt/spark/work-dir /opt/spark/logs && \
    chgrp -R 0 /opt/spark/work-dir /opt/spark/logs && \
    chmod -R 775 /opt/spark/work-dir /opt/spark/logs

# Ensure /tmp is writable
RUN chmod 1777 /tmp

# Set HOME for Spark temp files
ENV HOME=/home/spark

# Create directories for PVC mounts with GID 0 ownership
RUN mkdir -p /app/assets /app/output /app/scripts /home/spark && \
    chgrp -R 0 /app /home/spark && \
    chmod -R g=u /app /home/spark && \
    chmod -R 775 /app/output /home/spark
```

---

## Building Your Custom Image

### Step 1: Build and Push Your Image

```bash
cd examples/openshift

# Build the image for Red Hat AI (Linux AMD64)
docker buildx build --platform linux/amd64 \
  -t quay.io/YOUR_USERNAME/docling-spark:latest \
  --push .
```

> **Note:** The `--platform linux/amd64` flag ensures the image runs on ROSA nodes, even if you're building on Apple Silicon (M1/M2/M3 Mac).

### Step 2: Update the Manifest

Edit `k8s/docling-spark-app.yaml` to use your image:

```yaml
image: quay.io/YOUR_USERNAME/docling-spark:latest # ← Update this line
```

### Step 3: Deploy

Follow the deployment instructions in the main [Spark Operator on OpenShift guide](./SparkOperatorOnOpenShift.md#5-deploying-the-docling-spark-application).

---

## Troubleshooting

### Permission Denied Errors

If you see permission errors in your pods, verify:
1. All directories in your Dockerfile have `chgrp -R 0` applied
2. Directories are group-writable with `chmod -R g=u` or `chmod -R 775`
3. No `USER` directive at the end of your Dockerfile

### Image Pull Errors

Ensure your registry is accessible from the OpenShift cluster and that `imagePullSecrets` are configured if using a private registry.

### Verify Arbitrary UID is Working

To confirm the container is running with a random non-root UID:

```bash
oc exec -n spark-operator <POD_NAME> -- id
```

Expected output should show a high UID number (e.g., `uid=1000840000`) and `gid=0(root)`.

---

## References

- [OpenShift Security Context Constraints](https://docs.openshift.com/container-platform/latest/authentication/managing-security-context-constraints.html)
- [Spark Operator on OpenShift Guide](./SparkOperatorOnOpenShift.md)

