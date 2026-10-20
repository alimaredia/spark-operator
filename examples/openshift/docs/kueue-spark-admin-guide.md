# Kueue + Spark Operator Integration: Administrator Guide

> **Maturity: Alpha** — The Kueue SparkApplication integration is alpha-quality
> (upstream Kueue v0.17+). APIs, behavior, and configuration may change in future
> releases. Do not use in production without understanding the
> [known limitations](#known-limitations).

This guide is for **cluster administrators** responsible for installing,
configuring, and operating the Kueue quota management system alongside the
Kubeflow Spark Operator on Red Hat OpenShift AI (RHOAI).

## Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Installing the Operators](#installing-the-operators)
- [Enabling SparkApplication Integration](#enabling-sparkapplication-integration)
- [Creating Queue Resources](#creating-queue-resources)
- [Configuring Priority Classes and Preemption](#configuring-priority-classes-and-preemption)
- [RBAC Configuration](#rbac-configuration)
- [Namespace Configuration](#namespace-configuration)
- [Verifying the Installation](#verifying-the-installation)
- [Known Limitations](#known-limitations)
- [Troubleshooting](#troubleshooting)
- [Version Reference](#version-reference)

---

## Overview

Kueue is a Kubernetes-native job queueing system that manages when workloads
are admitted to run based on available quota. When integrated with the Spark
Operator, Kueue provides:

- **Quota management** — enforce CPU and memory limits per team or namespace
- **Fair sharing** — distribute cluster resources equitably across tenants
- **Priority-based scheduling** — admit higher-priority jobs first
- **Preemption** — reclaim resources from lower-priority jobs when needed

Kueue manages SparkApplications by intercepting them at creation time via a
mutating webhook, setting `spec.suspend=true`, and creating a corresponding
Workload object. When quota is available, Kueue sets `spec.suspend=false` and
the Spark Operator proceeds with normal submission.

## Prerequisites

- OpenShift 4.14+ cluster with cluster-admin access
- `oc` CLI logged in as cluster-admin
- `operator-sdk` CLI installed (for dev bundle installation)
- Familiarity with Kueue concepts: ResourceFlavor, ClusterQueue, LocalQueue,
  Workload

## Installing the Operators

### Spark Operator

Install the Spark Operator via Kustomize:

```bash
oc apply -k config/default/ --server-side=true --force-conflicts
```

Verify the controller and webhook pods are running:

```bash
oc get pods -n spark-operator | grep spark-operator
```

You should see both `spark-operator-controller` and `spark-operator-webhook`
pods in `Running` state.

### Kueue Operator (RHBoK)

The SparkApplication integration requires RHBoK v1.4.0+ (upstream Kueue
v0.17+). Earlier versions do not include SparkApplication in the supported
frameworks list.

If an older version of RHBoK is installed, uninstall it first:

```bash
oc delete kueue cluster --ignore-not-found
oc delete subscription kueue-operator -n openshift-kueue-operator --ignore-not-found
oc delete csv kueue-operator.v1.3.1 -n openshift-kueue-operator --ignore-not-found
```

Install the v1.4.0 dev bundle:

```bash
oc create namespace openshift-kueue-operator --dry-run=client -o yaml | oc apply -f -

operator-sdk run bundle \
  quay.io/redhat-user-workloads/kueue-operator-tenant/kueue-bundle-dev-main@sha256:eae53646fd24b6c20f9807f8849bb29dd5b6b8b9566866e16e428882693c3b1e \
  --namespace openshift-kueue-operator
```

Wait for the controller manager pods to be ready:

```bash
oc get pods -n openshift-kueue-operator -w
```

You should see `kueue-controller-manager-*` pods in `Running` state.

## Enabling SparkApplication Integration

Create a Kueue CR that includes `SparkApplication` in the `frameworks` list.
Without this, Kueue's webhook will not intercept SparkApplication resources.

```yaml
apiVersion: kueue.openshift.io/v1
kind: Kueue
metadata:
  name: cluster
spec:
  config:
    integrations:
      frameworks:
      - SparkApplication
      - BatchJob
      - Pod
    fairSharing:
      enable: true
      preemptionStrategies:
      - LessThanOrEqualToFinalShare
      - LessThanInitialShare
  managementState: Managed
```

Apply it:

```bash
oc apply -f examples/openshift/kueue/kueue-cr.yaml
```

Verify SparkApplication appears in the config:

```bash
oc get kueues.kueue.openshift.io cluster -o jsonpath='{.spec.config.integrations.frameworks}'
```

Wait ~30 seconds for the Kueue webhook service to become available:

```bash
oc get svc -n openshift-kueue-operator | grep webhook
```

### FairSharing Configuration

The Kueue CR above enables FairSharing with two preemption strategies:

| Strategy | Behavior |
|----------|----------|
| `LessThanOrEqualToFinalShare` | Preempt workloads in a ClusterQueue only if the preemptor's share after admission is less than or equal to the target's final share |
| `LessThanInitialShare` | Preempt only if the preemptor's share is less than the target's initial share |

FairSharing requires ClusterQueues to be grouped into **Cohorts** (see
[Configuring Priority Classes and Preemption](#configuring-priority-classes-and-preemption)).

## Creating Queue Resources

Kueue uses a three-level resource hierarchy:

```
ResourceFlavor → ClusterQueue → LocalQueue
(hardware type)   (quota pool)   (namespace entry point)
```

### ResourceFlavor

A ResourceFlavor represents a type of resource (e.g., default nodes, GPU nodes).
For most Spark workloads, a single empty flavor is sufficient:

```yaml
apiVersion: kueue.x-k8s.io/v1beta2
kind: ResourceFlavor
metadata:
  name: spark-rf
```

### ClusterQueue

A ClusterQueue defines the quota pool. Configure `resourceGroups` to set CPU
and memory limits, and use `namespaceSelector` to restrict which namespaces can
consume from this queue:

```yaml
apiVersion: kueue.x-k8s.io/v1beta2
kind: ClusterQueue
metadata:
  name: spark-cq
spec:
  namespaceSelector:
    matchExpressions:
    - key: kubernetes.io/metadata.name
      operator: In
      values:
      - spark-test
  resourceGroups:
  - coveredResources: ["cpu", "memory"]
    flavors:
    - name: spark-rf
      resources:
      - name: cpu
        nominalQuota: 3
      - name: memory
        nominalQuota: 3Gi
```

**Quota sizing guidance:** Each SparkApplication typically requires resources
for one driver pod + N executor pods. A driver with 1 CPU + 512Mi and one
executor with 1 CPU + 512Mi consumes roughly 2 CPU and 1Gi. Size your quota to
accommodate the expected concurrency level.

### LocalQueue

A LocalQueue is the namespace-scoped entry point that users reference in their
SparkApplication labels. It maps to a ClusterQueue:

```yaml
apiVersion: kueue.x-k8s.io/v1beta2
kind: LocalQueue
metadata:
  name: spark-lq
  namespace: spark-test
spec:
  clusterQueue: spark-cq
```

### Applying Queue Resources

A complete set of resources is provided in the repository:

```bash
oc apply -f examples/openshift/kueue/kueue-resources.yaml
```

Verify:

```bash
oc get clusterqueue spark-cq
oc get localqueue spark-lq -n spark-test
```

Both should show as `Active`.

## Configuring Priority Classes and Preemption

### WorkloadPriorityClasses

Kueue uses `WorkloadPriorityClass` resources (not Kubernetes PriorityClasses)
to determine admission order and preemption eligibility:

```yaml
apiVersion: kueue.x-k8s.io/v1beta2
kind: WorkloadPriorityClass
metadata:
  name: high-priority
value: 1000
description: "High-priority Spark workloads"
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: WorkloadPriorityClass
metadata:
  name: low-priority
value: 100
description: "Low-priority Spark workloads"
```

Users reference these via the `kueue.x-k8s.io/priority-class` label on their
SparkApplication (see the [User Guide](kueue-spark-user-guide.md)).

### Preemption Policies on ClusterQueues

Configure preemption behavior per ClusterQueue:

```yaml
apiVersion: kueue.x-k8s.io/v1beta2
kind: ClusterQueue
metadata:
  name: spark-cq
spec:
  preemption:
    withinClusterQueue: LowerPriority
    reclaimWithinCohort: Any
    borrowWithinCohort:
      policy: LowerPriority
      maxPriorityThreshold: 500
```

| Field | Behavior |
|-------|----------|
| `withinClusterQueue: LowerPriority` | Preempt lower-priority workloads in the same queue to make room |
| `reclaimWithinCohort: Any` | Reclaim resources lent to other queues in the cohort |
| `borrowWithinCohort.policy: LowerPriority` | Only borrow from cohort members running lower-priority workloads |

### Multi-Tenant Cohorts

For multi-tenant clusters, group ClusterQueues into a **Cohort** to enable
resource borrowing and fair sharing between teams:

```yaml
apiVersion: kueue.x-k8s.io/v1beta2
kind: Cohort
metadata:
  name: spark-cohort
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: ClusterQueue
metadata:
  name: team-a-cq
spec:
  cohortName: spark-cohort
  resourceGroups:
  - coveredResources: ["cpu", "memory"]
    flavors:
    - name: spark-rf
      resources:
      - name: cpu
        nominalQuota: 2
      - name: memory
        nominalQuota: 2Gi
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: ClusterQueue
metadata:
  name: team-b-cq
spec:
  cohortName: spark-cohort
  resourceGroups:
  - coveredResources: ["cpu", "memory"]
    flavors:
    - name: spark-rf
      resources:
      - name: cpu
        nominalQuota: 2
      - name: memory
        nominalQuota: 2Gi
```

> **Important:** In Kueue v1beta2, use `spec.cohortName` (not `spec.cohort`)
> to reference a Cohort CRD. Without proper cohort configuration,
> ClusterQueues cannot borrow from each other and FairSharing has no effect.

## RBAC Configuration

### Kueue RBAC for SparkApplication

Kueue needs permission to set `blockOwnerDeletion` on Workload owner references
pointing to SparkApplications. Without this, Kueue cannot create Workload
objects and SparkApplications stay permanently suspended.

```bash
oc apply -f - <<'EOF'
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: kueue-sparkapplication-finalizers
rules:
- apiGroups: ["sparkoperator.k8s.io"]
  resources: ["sparkapplications/finalizers"]
  verbs: ["update"]
- apiGroups: ["sparkoperator.k8s.io"]
  resources: ["sparkapplications"]
  verbs: ["get", "list", "watch", "update", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: kueue-sparkapplication-finalizers
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: kueue-sparkapplication-finalizers
subjects:
- kind: ServiceAccount
  name: kueue-controller-manager
  namespace: openshift-kueue-operator
EOF
```

### Spark Driver RBAC

The Spark driver pod needs a ServiceAccount with permissions to create executor
pods. This RBAC is **not** part of `oc apply -k config/default/` — it belongs in
each workload namespace, not the operator's. It lives in `config/spark-rbac/`;
apply it to every namespace where SparkApplications will run:

```bash
oc apply -k config/spark-rbac -n spark-test
```

That installs:

- A `spark-operator-spark` ServiceAccount
- A Role granting pod, configmap, PVC, and service management
- A RoleBinding connecting the two

Verify the ServiceAccount exists:

```bash
oc get serviceaccount spark-operator-spark -n spark-test
```

### Non-Admin User RBAC (Optional)

If non-admin users will submit SparkApplications, create a dedicated
ServiceAccount and role binding:

```bash
oc apply -f examples/openshift/k8s/kueue/spark-nonadmin-rbac.yaml
```

## Namespace Configuration

Kueue's mutating webhook only intercepts resources in namespaces with the
managed label. Apply this to every namespace where SparkApplications will
be submitted:

```bash
oc label namespace spark-test kueue.openshift.io/managed=true --overwrite
```

Without this label, SparkApplications are submitted directly to the Spark
Operator without Kueue admission control — no suspension, no quota enforcement.

## Verifying the Installation

Run through this checklist to confirm everything is configured correctly:

```bash
# 1. Kueue controller is running
oc get pods -n openshift-kueue-operator | grep kueue-controller-manager

# 2. Spark Operator is running
oc get pods -n spark-operator | grep spark-operator

# 3. SparkApplication is in the frameworks list
oc get kueues.kueue.openshift.io cluster -o jsonpath='{.spec.config.integrations.frameworks}'

# 4. ClusterQueue and LocalQueue are active
oc get clusterqueue
oc get localqueue -n spark-test

# 5. Namespace is labeled
oc get namespace spark-test --show-labels | grep kueue

# 6. Submit a test SparkApplication
oc apply -f examples/openshift/kueue/spark-pi-kueue.yaml

# 7. Verify Kueue intercepted it (should see suspend=true initially)
oc get sparkapplication spark-pi-kueue -n spark-test -o jsonpath='{.spec.suspend}'

# 8. Watch the lifecycle
oc get sparkapplication spark-pi-kueue -n spark-test -w
```

Expected state transitions: `""` → `Suspended` → `Resuming` → `New` →
`Submitted` → `Running` → `Completed`

## Known Limitations

### Alpha Maturity

The Kueue SparkApplication integration is **alpha** (upstream Kueue v0.17+).
This means:

- The API and behavior may change without notice in future releases
- Not all Kueue features have been validated with SparkApplication
- Thorough testing in a non-production environment is strongly recommended

### Dynamic Allocation Not Supported

SparkApplications with `dynamicAllocation.enabled=true` are **not supported**
with Kueue. Kueue requires a fixed resource footprint at admission time to
reserve quota. Dynamic allocation changes the executor count at runtime, which
conflicts with Kueue's quota model.

Attempting to submit a SparkApplication with dynamic allocation enabled will
result in either a webhook rejection or a terminal failure state.

### Suspend/Resume Is Destructive

When Kueue suspends a SparkApplication (due to preemption, quota reclamation,
or fair sharing), the Spark Operator **deletes all driver and executor pods**.
This is not a graceful pause — all in-memory state and intermediate computation
results are lost.

**Implications for administrators:**

- Communicate this behavior clearly to users
- Ensure users design idempotent Spark jobs (see [User Guide](kueue-spark-user-guide.md))
- Consider configuring persistent storage (S3, PVC) for intermediate results
- Size quotas to minimize unnecessary preemption

### Features Out of Scope

The following Kueue features are **not supported** with SparkApplication:

| Feature | Status | Notes |
|---------|--------|-------|
| MultiKueue | Not supported | Multi-cluster admission not validated |
| DRA (Dynamic Resource Allocation) | Not supported | Device-level resource claims not applicable |
| ProvisioningRequest | Not supported | Node auto-provisioning not validated |
| `pod` integration mode | Not applicable | SparkApplication uses its own integration |

## Troubleshooting

### SparkApplications Stuck in `SUSPENDED` with No Workloads Created

**Symptom:** SparkApplications show `spec.suspend=true` but no Workload
objects are created.

**Cause:** Kueue lacks RBAC permissions for SparkApplication finalizers.

**Diagnosis:**

```bash
oc logs -n openshift-kueue-operator deployment/kueue-controller-manager --tail=50 | grep -i error
```

Look for: `cannot set blockOwnerDeletion if an ownerReference refers to a
resource you can't set finalizers on`

**Fix:** Apply the RBAC from [RBAC Configuration](#kueue-rbac-for-sparkapplication).

### SparkApplications Not Intercepted by Kueue

**Symptom:** SparkApplications run immediately without being suspended.

**Cause:** The namespace is missing the `kueue.openshift.io/managed=true` label.

**Fix:**

```bash
oc label namespace spark-test kueue.openshift.io/managed=true --overwrite
```

### `x509: certificate signed by unknown authority` on SparkApplication Create

**Symptom:** Intermittent TLS errors when creating SparkApplications.

**Cause:** Two Spark Operator installations (e.g., one from RHOAI in
`redhat-ods-applications` and one from `oc apply -k` in `spark-operator`)
have competing webhook pods that overwrite each other's CA bundle in the
MutatingWebhookConfiguration.

**Fix:** Remove the conflicting webhook deployment:

```bash
oc delete deployment spark-operator-webhook -n spark-operator
oc delete service spark-operator-webhook-svc -n spark-operator
oc delete mutatingwebhookconfiguration spark-operator-webhook
oc delete validatingwebhookconfiguration spark-operator-webhook
oc rollout restart deployment spark-operator-webhook -n redhat-ods-applications
```

### `service "spark-operator-webhook-svc" not found`

**Cause:** The Spark Operator is not deployed.

**Fix:**

```bash
oc apply -k config/default/ --server-side=true --force-conflicts
```

### `install plan is not available` During Bundle Install

**Cause:** An existing RHBoK subscription conflicts with the dev bundle.

**Fix:** Uninstall the existing version first (see
[Installing the Operators](#kueue-operator-rhbok)).

### `conversion webhook ... service "kueue-webhook-service" not found`

**Cause:** The Kueue controller manager hasn't finished starting.

**Fix:** Wait ~30 seconds and retry.

### Workloads Stuck in `Pending` (Quota Available)

**Symptom:** Workload objects exist but show `Admitted=False` even though
ClusterQueue has available quota.

**Diagnosis:**

```bash
oc get workloads -n spark-test
oc describe workload <workload-name> -n spark-test
oc get clusterqueue spark-cq -o yaml
```

**Common causes:**

- The LocalQueue references a non-existent ClusterQueue
- The ClusterQueue `namespaceSelector` does not match the namespace
- Resource requests exceed the `nominalQuota`

### Image Pull Errors

The default Spark image `ghcr.io/apache/spark-docker/spark:3.5.4` is publicly
available. If you encounter pull rate limits, pre-pull the image on cluster
nodes or use an internal registry.

### `AccessDeniedException: /opt/spark/work-dir/...`

On OpenShift, Spark needs a writable work directory. Ensure both driver and
executor specs include an `emptyDir` volume:

```yaml
spec:
  volumes:
  - name: spark-work-dir
    emptyDir: {}
  driver:
    volumeMounts:
    - name: spark-work-dir
      mountPath: /opt/spark/work-dir
  executor:
    volumeMounts:
    - name: spark-work-dir
      mountPath: /opt/spark/work-dir
```

### Checking Events and Workload Status

For general debugging, use these commands to inspect the Kueue lifecycle:

```bash
# Namespace events (sorted by time)
oc get events -n spark-test --sort-by='.lastTimestamp' | tail -20

# Workload details
oc get workloads -n spark-test
oc describe workload <workload-name> -n spark-test

# Kueue controller logs
oc logs -n openshift-kueue-operator deployment/kueue-controller-manager --tail=100

# ClusterQueue utilization
oc get clusterqueue -o wide
```

## Version Reference

| Component | Version | Upstream Base |
|-----------|---------|---------------|
| RHBoK | 1.4.0 | Kueue v0.17+ |
| RHBoK (previous GA) | 1.3.1 | Kueue v0.16.5 |
| Spark Operator | v2.4.0+ | kubeflow/spark-operator |
| SparkApplication integration | Alpha | Since upstream Kueue v0.17 |
| Spark image | 3.5.4 | ghcr.io/apache/spark-docker/spark |
