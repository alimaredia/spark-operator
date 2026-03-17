# Updating Kustomize Manifests from Upstream

This document describes the process for updating the Kustomize manifests in `config/` when syncing the odh/spark-operator repository with the upstream Kubeflow Spark Operator.

## Overview

The ODH Spark Operator uses Kustomize manifests for installation on OpenShift, following the OpenDataHub operator pattern. The manifests are organized as:

```text
config/
├── component_metadata.yaml     # Version tracking
├── crd/                        # Custom Resource Definitions (single source of truth)
│   └── bases/                  # CRD YAML files
├── manager/                    # Controller manager deployment
├── webhook/                    # Admission webhook
├── rbac/                       # All RBAC consolidated
├── default/                    # Main entry point (ties everything together)
└── overlays/                   # Environment-specific configurations
    ├── odh/                    # OpenDataHub (namespace: opendatahub)
    └── rhoai/                  # Red Hat OpenShift AI (namespace: redhat-ods-applications)
```

These manifests are derived from the upstream operator with OpenShift-specific modifications (e.g., no `fsGroup` in security contexts to comply with `restricted-v2` SCC).

## When to Update

Update the manifests whenever:

1. **Syncing with a new upstream release** (e.g., v2.4.0 → v2.5.0)
2. **Upstream changes RBAC permissions** (new API resources, verbs)
3. **New command-line arguments** are added to controller/webhook
4. **CRDs are updated** (new fields, API version changes)
5. **New resources** are added (e.g., PodDisruptionBudget)
6. **Security fixes** that modify deployment specs

## Update Process

### Step 1: Review Upstream Changes

First, review what has changed in the upstream release:

```bash
# Clone or update the upstream repository
git clone https://github.com/kubeflow/spark-operator.git /tmp/upstream-spark-operator
cd /tmp/upstream-spark-operator

# Check the release notes and changelog
cat CHANGELOG.md

# Review changes in the charts directory
git diff v2.4.0..v2.5.0 -- charts/spark-operator-chart/
```

### Step 2: Generate Reference Manifests

Generate manifests from the upstream to see the expected output:

```bash
cd /tmp/upstream-spark-operator

# Generate manifests with our configuration
# Note: This requires helm CLI for reference generation only
helm template spark-operator ./charts/spark-operator-chart \
    --namespace spark-operator \
    > /tmp/upstream-manifests.yaml
```

### Step 3: Compare Changes

Review the differences between upstream and existing files:

```bash
# View the generated manifests
less /tmp/upstream-manifests.yaml

# Compare specific resources
# Example: Check controller deployment args
grep -A 50 "kind: Deployment" /tmp/upstream-manifests.yaml | grep -A 30 "controller"
```

Key areas to check:
- **Deployment args**: New CLI flags, changed defaults
- **RBAC rules**: New API groups, resources, or verbs
- **Container image**: Updated tag/repository
- **Probes**: Health check paths or ports
- **Webhook configurations**: New webhooks or changed paths

### Step 4: Update Individual Files

For each changed resource, update the corresponding file in `config/`.

**Important OpenShift-specific changes to preserve:**

1. **DO NOT add `fsGroup`** to any `securityContext`:
   ```yaml
   # Upstream may generate this (DON'T USE on OpenShift):
   securityContext:
     fsGroup: 185
   
   # Keep it as (CORRECT for OpenShift):
   # No securityContext.fsGroup - let OpenShift assign via restricted-v2 SCC
   ```

2. **Keep namespace as `system`** in base files - Kustomize transforms it:
   - Overlays transform `system` → `opendatahub` (ODH)
   - Overlays transform `system` → `redhat-ods-applications` (RHOAI)
   - Default transforms `system` → `spark-operator`

3. **Preserve comments** explaining OpenShift-specific choices

### Step 5: Update CRDs

CRDs are the single source of truth in `config/crd/bases/`. Ensure they're in sync:

```bash
# Navigate to the odh spark-operator repo
cd /path/to/spark-operator

# Regenerate CRDs from Go types
make manifests

# Verify CRDs in config/crd/bases/
ls -la config/crd/bases/
```

### Step 6: Update Image Version and Metadata

1. Edit `config/manager/manager.yaml` (image tag in deployment):

```yaml
containers:
  - name: controller
    image: ghcr.io/kubeflow/spark-operator/controller:X.Y.Z  # Update version
```

2. Edit `config/webhook/deployment.yaml` (same image):

```yaml
containers:
  - name: webhook
    image: ghcr.io/kubeflow/spark-operator/controller:X.Y.Z  # Update version
```

3. Edit `config/default/kustomization.yaml`:

```yaml
images:
  - name: ghcr.io/kubeflow/spark-operator/controller
    newTag: "X.Y.Z"  # Update to new version
```

4. Update `config/component_metadata.yaml`:

```yaml
releases:
  - name: Spark Operator
    version: vX.Y.Z  # Update version
    repoUrl: https://github.com/kubeflow/spark-operator
```

5. Update overlay params if needed:
- `config/overlays/odh/params.env`
- `config/overlays/rhoai/params.env`

### Step 7: Test the Installation

```bash
# Login to OpenShift
oc login

# Build and verify Kustomize output for each entry point
kubectl kustomize config/default/
kubectl kustomize config/overlays/odh/
kubectl kustomize config/overlays/rhoai/

# Count resources
kubectl kustomize config/default/ | grep "^kind:" | sort | uniq -c

# Test on cluster (use --server-side=true for large CRDs)
oc apply -k config/overlays/odh/ --server-side=true

# Verify pods are running
oc get pods -n opendatahub -l app.kubernetes.io/name=spark-operator

# Verify restricted-v2 SCC is applied
oc describe pod -n opendatahub -l app.kubernetes.io/component=controller | grep openshift.io/scc

# Run the test suite
cd examples/openshift/tests
./test-operator-install.sh
./test-spark-pi.sh
```

### Step 8: Update Documentation

If there are significant changes, update:
- `examples/openshift/SparkOperatorOnOpenShift.md`
- `examples/openshift/UnderstandKustomization.md`
- This file (`UPDATING_MANIFESTS.md`)
- Version references in README files

## Checklist

Use this checklist for each sync:

### Metadata
- [ ] `config/component_metadata.yaml` - update version

### CRDs (in `config/crd/bases/`)
- [ ] `sparkoperator.k8s.io_sparkapplications.yaml` - regenerated via `make manifests`
- [ ] `sparkoperator.k8s.io_scheduledsparkapplications.yaml` - regenerated
- [ ] `sparkoperator.k8s.io_sparkconnects.yaml` - regenerated

### Manager (in `config/manager/`)
- [ ] `serviceaccount.yaml` - check for new annotations
- [ ] `manager.yaml` - check for:
  - [ ] New CLI arguments
  - [ ] Changed ports
  - [ ] Updated probes
  - [ ] Resource limits
  - [ ] **NO fsGroup** in securityContext
  - [ ] Updated image tag

### Webhook (in `config/webhook/`)
- [ ] `serviceaccount.yaml` - check for new annotations
- [ ] `clusterrole.yaml` - check for new permissions
- [ ] `clusterrolebinding.yaml` - usually unchanged
- [ ] `role.yaml` - check for new permissions
- [ ] `rolebinding.yaml` - usually unchanged
- [ ] `service.yaml` - check for port changes
- [ ] `deployment.yaml` - check for:
  - [ ] New CLI arguments
  - [ ] Changed ports
  - [ ] Updated probes
  - [ ] **NO fsGroup** in securityContext
  - [ ] Updated image tag
- [ ] `mutatingwebhookconfiguration.yaml` - check for new webhooks or paths (custom, used by kustomize)
- [ ] `validatingwebhookconfiguration.yaml` - check for new webhooks or paths (custom, used by kustomize)
- [ ] `manifests.yaml` - **auto-generated by `make manifests`** (CI only, not used in kustomization)

> **Note:** The `manifests.yaml` file is auto-generated by `controller-gen` and contains combined webhook configurations. It exists only to satisfy CI checks. The actual webhook configs used by kustomize are `mutatingwebhookconfiguration.yaml` and `validatingwebhookconfiguration.yaml` which contain OpenShift-specific customizations.

### RBAC (in `config/rbac/`)
- [ ] `clusterrole.yaml` - check for new API permissions (custom, used by kustomize)
- [ ] `clusterrolebinding.yaml` - usually unchanged
- [ ] `leader-election-role.yaml` - namespace-scoped Role for leader election (custom)
- [ ] `role.yaml` - **auto-generated by `make manifests`** (CI only, not used in kustomization)
- [ ] `rolebinding.yaml` - usually unchanged
- [ ] `spark_serviceaccount.yaml` - usually unchanged
- [ ] `spark_role.yaml` - check for new permissions needed by jobs
- [ ] `spark_rolebinding.yaml` - usually unchanged
- [ ] `*_viewer_role.yaml` - check for new resources
- [ ] `*_editor_role.yaml` - check for new resources

> **Note:** The `role.yaml` file is auto-generated by `controller-gen` during `make manifests`. It's a ClusterRole (despite the filename) and exists only to satisfy CI checks. The actual RBAC used by kustomize is `clusterrole.yaml` (cluster-wide permissions) and `leader-election-role.yaml` (namespace-scoped leader election).

### Kustomization Files
- [ ] `config/crd/kustomization.yaml` - update if new CRDs added
- [ ] `config/manager/kustomization.yaml` - usually unchanged
- [ ] `config/webhook/kustomization.yaml` - usually unchanged
- [ ] `config/rbac/kustomization.yaml` - update if new RBAC files added
- [ ] `config/default/kustomization.yaml` - update image tag
- [ ] `config/overlays/odh/kustomization.yaml` - update if needed
- [ ] `config/overlays/odh/params.env` - update OPERATOR_VERSION
- [ ] `config/overlays/rhoai/kustomization.yaml` - update if needed
- [ ] `config/overlays/rhoai/params.env` - update OPERATOR_VERSION

### Tests
- [ ] All tests pass: `./test-operator-install.sh`
- [ ] Spark Pi test passes: `./test-spark-pi.sh`

### Documentation
- [ ] `SparkOperatorOnOpenShift.md` updated if needed
- [ ] `UnderstandKustomization.md` updated if structure changes
- [ ] Version note updated (Spark version, Java version compatibility)

## File Reference

| File | Source | Notes |
|------|--------|-------|
| `config/component_metadata.yaml` | Custom | ODH version tracking |
| `config/crd/bases/*.yaml` | `make manifests` | Regenerate from Go types |
| `config/crd/kustomization.yaml` | Custom | References CRD bases |
| `config/manager/manager.yaml` | Upstream | Remove fsGroup, use `system` namespace |
| `config/manager/serviceaccount.yaml` | Upstream | Use `system` namespace |
| `config/webhook/manifests.yaml` | `make manifests` | **Auto-generated (CI only, not used by kustomize)** |
| `config/webhook/*.yaml` (others) | Upstream/Custom | Remove fsGroup, use `system` namespace |
| `config/rbac/role.yaml` | `make manifests` | **Auto-generated ClusterRole (CI only, not used by kustomize)** |
| `config/rbac/leader-election-role.yaml` | Custom | Namespace-scoped Role for leader election |
| `config/rbac/clusterrole.yaml` | Custom | ClusterRole with complete permissions |
| `config/rbac/*.yaml` (others) | Upstream | Use `system` namespace |
| `config/default/kustomization.yaml` | Custom | Main entry point, update image tag |
| `config/overlays/odh/*` | Custom | ODH overlay (namespace: opendatahub) |
| `config/overlays/rhoai/*` | Custom | RHOAI overlay (namespace: redhat-ods-applications) |

### Auto-Generated Files (CI Requirements)

The following files are auto-generated by `make manifests` (via `controller-gen`) and **must be committed** to pass CI, but are **not used** in kustomization:

| File | Generated Content | Why Not Used |
|------|-------------------|--------------|
| `config/rbac/role.yaml` | ClusterRole `spark-operator-controller` | Incomplete permissions; use `clusterrole.yaml` instead |
| `config/webhook/manifests.yaml` | Combined webhook configurations | Missing customizations (objectSelector, etc.); use separate files instead |

These files exist solely to satisfy the `make manifests` CI check which verifies that auto-generated manifests are in sync with Go code annotations.

## Directory Structure After Sync

After a successful sync, your config directory should look like:

```text
config/
├── component_metadata.yaml
├── crd/
│   ├── bases/
│   │   ├── sparkoperator.k8s.io_scheduledsparkapplications.yaml
│   │   ├── sparkoperator.k8s.io_sparkapplications.yaml
│   │   └── sparkoperator.k8s.io_sparkconnects.yaml
│   ├── kustomization.yaml
│   └── kustomizeconfig.yaml
├── default/
│   └── kustomization.yaml
├── manager/
│   ├── kustomization.yaml
│   ├── manager.yaml
│   └── serviceaccount.yaml
├── overlays/
│   ├── odh/
│   │   ├── delete-namespace.yaml
│   │   ├── kustomization.yaml
│   │   └── params.env
│   └── rhoai/
│       ├── delete-namespace.yaml
│       ├── kustomization.yaml
│       └── params.env
├── rbac/
│   ├── clusterrole.yaml              # Custom ClusterRole (used by kustomize)
│   ├── clusterrolebinding.yaml
│   ├── kustomization.yaml
│   ├── leader-election-role.yaml     # Namespace-scoped Role for leader election
│   ├── role.yaml                     # Auto-generated (CI only, not used)
│   ├── rolebinding.yaml
│   ├── scheduledsparkapplication_editor_role.yaml
│   ├── scheduledsparkapplication_viewer_role.yaml
│   ├── spark_role.yaml
│   ├── spark_rolebinding.yaml
│   ├── spark_serviceaccount.yaml
│   ├── sparkapplication_editor_role.yaml
│   └── sparkapplication_viewer_role.yaml
└── webhook/
    ├── clusterrole.yaml
    ├── clusterrolebinding.yaml
    ├── deployment.yaml
    ├── kustomization.yaml
    ├── kustomizeconfig.yaml
    ├── manifests.yaml                # Auto-generated (CI only, not used)
    ├── mutatingwebhookconfiguration.yaml   # Custom (used by kustomize)
    ├── role.yaml
    ├── rolebinding.yaml
    ├── service.yaml
    ├── serviceaccount.yaml
    └── validatingwebhookconfiguration.yaml # Custom (used by kustomize)
```

---

**Maintainer Note:** Always test on a real OpenShift cluster to ensure `restricted-v2` SCC compatibility. Kind/Minikube tests won't catch OpenShift-specific issues like SCC enforcement.
