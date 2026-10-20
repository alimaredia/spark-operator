# Kueue + SparkApplication Integration Setup on OpenShift

This guide walks you through setting up and testing the Kueue + Kubeflow Spark Operator (KSO) integration on OpenShift.

## Prerequisites

- OpenShift cluster with `oc` CLI logged in as cluster-admin
- `operator-sdk` installed (`brew install operator-sdk`)
- Spark Operator installed on the cluster (via Kustomize: `oc apply -k config/default/ --server-side=true --force-conflicts`)

## Step 1: Install RHBoK v1.4.0 (Dev Bundle)

If an older version of RHBoK is already installed, uninstall it first:

```bash
oc delete kueue cluster --ignore-not-found
oc delete subscription kueue-operator -n openshift-kueue-operator --ignore-not-found
oc delete csv kueue-operator.v1.3.1 -n openshift-kueue-operator --ignore-not-found
```

Install the v1.4.0 dev bundle (upstream Kueue v0.17+, includes SparkApplication integration):

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

## Step 2: Create the Kueue CR

The Kueue CR must include `SparkApplication` in the frameworks list:

```bash
oc apply -f examples/openshift/kueue/kueue-cr.yaml
```

Verify `SparkApplication` appears in the config:

```bash
oc get kueues.kueue.openshift.io cluster -o jsonpath='{.spec.config.integrations.frameworks}'
```

Wait ~30 seconds for the Kueue webhook service to become available:

```bash
oc get svc -n openshift-kueue-operator | grep webhook
```

## Step 3: Create Kueue Queue Resources

Create the ResourceFlavor, ClusterQueue (3 CPU / 3Gi quota), and LocalQueue:

```bash
oc apply -f examples/openshift/kueue/kueue-resources.yaml
```

Verify:

```bash
oc get clusterqueue spark-cq
oc get localqueue spark-lq -n spark-test
```

## Step 4: Grant Kueue RBAC for SparkApplication

Kueue needs permission to set `blockOwnerDeletion` on Workload owner references pointing to SparkApplications. Without this, Kueue cannot create Workload objects and apps stay permanently suspended.

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

## Step 5: Create and Label the Workload Namespace

Spark workloads run in their own namespace, never the operator's — see
[network-policy-pod-inventory.md](../docs/network-policy-pod-inventory.md). This
guide uses `spark-test`; substitute your own, and keep it consistent with the
LocalQueue namespaces in `examples/openshift/kueue/*.yaml`.

Kueue's mutating webhook only intercepts resources in namespaces carrying its
label:

```bash
oc new-project spark-test
oc label namespace spark-test kueue.openshift.io/managed=true --overwrite
```

## Step 6: Enable the Namespace for Spark

`config/default/` installs the operator but deliberately does **not** include the
Spark driver RBAC, which belongs in each workload namespace. Apply it there:

```bash
oc apply -k config/spark-rbac -n spark-test
```

That creates the ServiceAccount, Role, and RoleBinding the driver needs to manage
its executor pods. Verify:

```bash
oc get serviceaccount spark-operator-spark -n spark-test
```

## Step 7: Submit a SparkApplication

```bash
oc apply -f examples/openshift/kueue/spark-pi-kueue.yaml
```

The `kueue.x-k8s.io/queue-name: spark-lq` label tells Kueue to manage this app.

### Monitor the Lifecycle

```bash
# Watch SparkApplication status
oc get sparkapplication spark-pi-kueue -n spark-test -w

# Check Kueue Workload objects
oc get workloads -n spark-test

# Check pods
oc get pods -n spark-test | grep spark-pi-kueue

# Check events
oc get events -n spark-test --sort-by='.lastTimestamp' | tail -20
```

### Expected Lifecycle

1. Kueue webhook sets `spec.suspend=true` on creation
2. SparkApplication enters `Suspended` state
3. Kueue creates a Workload and admits it (quota available), sets `spec.suspend=false`
4. Spark Operator transitions: `Resuming` -> `New` -> `Submitted` -> `Running`
5. Driver and executor pods are created
6. Job completes, Kueue reclaims quota

## Running the Automated E2E Tests

Automated Ginkgo tests are in `examples/openshift/kueue/`. They cover:

| File | Test Suite | Description |
|------|------------|-------------|
| `kueue_test.go` | Basic Admission | AC1-AC4: Admission, quota enforcement, reclamation, resume after suspension |
| `kueue_validation_test.go` | Validation & Lifecycle | AC1-AC6: Dynamic alloc rejection, pod cleanup, events, non-Kueue regression |

### `kueue_test.go` — Basic Admission Tests

| Test | Description |
|------|-------------|
| AC1 | Basic admission lifecycle (submit -> admit -> run -> complete -> quota reclaimed) |
| AC2 | Quota enforcement (excess jobs remain suspended while quota is exhausted) |
| AC3 | Quota reclamation (queued job admitted after completing job frees quota) |
| AC4 | Resume after suspension (suspended job runs to completion when quota freed) |

### `kueue_validation_test.go` — Validation, Lifecycle Cleanup & Event Visibility

| Test | Description |
|------|-------------|
| AC1 | Dynamic allocation validation (`dynamicAllocation.enabled=true` is rejected or reaches terminal state) |
| AC2 | Pod cleanup after successful completion (no orphan executor pods) |
| AC3 | Pod cleanup after failure (driver crash, no orphan executor pods) |
| AC4 | No orphan pods after Kueue suspend/resume lifecycle |
| AC5 | Event visibility (Workload conditions and Kueue-related events are queryable) |
| AC6 | Non-Kueue regression (standard SparkApp without queue label completes cleanly) |

### Prerequisites

Complete steps 1-6 above.

### Run All Kueue Tests

```bash
cd /path/to/spark-operator

KUBECONFIG=$HOME/.kube/config \
go test -v -tags openshift ./examples/openshift/kueue/ \
  -ginkgo.v \
  -ginkgo.focus="Kueue|Priority|Multi-Tenancy|Validation" \
  -timeout 60m
```

### Run Only Basic Admission Tests

```bash
KUBECONFIG=$HOME/.kube/config \
go test -v -tags openshift ./examples/openshift/kueue/ \
  -ginkgo.v -ginkgo.focus="Kueue SparkApplication Integration" -timeout 35m
```

### Run Only Validation & Lifecycle Tests

```bash
KUBECONFIG=$HOME/.kube/config \
go test -v -tags openshift ./examples/openshift/kueue/ \
  -ginkgo.v -ginkgo.focus="Validation" -timeout 35m
```

### Verbose Output

Tests take ~4-8 minutes for basic/validation tests, ~15-17 minutes for priority tests. Add `-ginkgo.v` to see step-by-step progress and SparkApplication state transitions during each test.

### Priority, FairSharing & Preemption Tests

Tests in `examples/openshift/kueue/kueue_priority_test.go` validate advanced Kueue scheduling:

| Test | Description |
|------|-------------|
| FairSharing Policy | Team B admitted before Team A's additional submissions when A over fair share |
| Priority-Based Scheduling | Higher-priority jobs admitted before lower-priority ones (as non-admin user) |
| Preemption Lifecycle | Running low-priority app preempted: pods deleted, state -> Suspended |
| Resume After Preemption | Preempted app restarts from scratch (new submission ID) and completes |

#### Additional Prerequisites

In addition to steps 1-6, apply the priority-specific resources:

```bash
# Apply WorkloadPriorityClasses, cohort ClusterQueues, and team LocalQueues
oc apply -f examples/openshift/kueue/kueue-priority-resources.yaml

# Apply non-admin ServiceAccount RBAC
oc apply -f examples/openshift/kueue/spark-nonadmin-rbac.yaml
```

Verify the resources:

```bash
oc get workloadpriorityclasses
oc get clusterqueue team-a-cq team-b-cq
oc get localqueue team-a-lq team-b-lq -n spark-test
oc get serviceaccount spark-nonadmin -n spark-test
```

The Kueue CR must have FairSharing enabled (`kueue-cr.yaml` includes this by default):

```bash
oc get kueues.kueue.openshift.io cluster -o jsonpath='{.spec.config.fairSharing}'
```

#### Run

```bash
cd /path/to/spark-operator

KUBECONFIG=$HOME/.kube/config \
go test -v -tags openshift ./examples/openshift/kueue/ \
  -ginkgo.v \
  -ginkgo.focus="Priority" \
  -timeout 45m
```

Tests take ~15-17 minutes to complete (preemption lifecycle requires waiting for state transitions).

#### What the Tests Validate

**FairSharing (AC1):**
- Two teams share a Cohort (`spark-cohort`) with equal quotas (2 CPU each)
- Team A submits 2 apps (4 CPU total, borrowing from Team B's quota)
- When Team B submits an app, Kueue reclaims borrowed quota from Team A
- Team A's borrowing app is preempted/suspended, Team B is admitted

**Priority-Based Scheduling (AC2):**
- A blocker app fills a standalone 2 CPU queue
- A non-admin user submits both a low-priority and high-priority app
- After the blocker completes, Kueue admits the high-priority app first
- Verified via Workload admission timestamps

**Preemption & Resume Lifecycle (AC3/AC4):**

```text
Low-priority app:  Running -> Suspending -> Suspended (pods=0) -> Resuming -> New -> Submitted -> Running -> Completed
High-priority app: Suspended -> Running -> Completed
```

Key assertions:
- All driver and executor pods are deleted when the low-priority app is preempted
- The resumed app gets a **new submission ID** (restart from scratch, not resume)
- Pod count goes to zero during suspend, returns on resume

#### Important: Cohort Configuration

The FairSharing test requires ClusterQueues to be in a **Cohort**. In Kueue v1beta2, use `spec.cohortName` (not `spec.cohort`) to reference a Cohort CRD:

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
  cohortName: spark-cohort   # NOT spec.cohort
```

Without proper cohort configuration, ClusterQueues cannot borrow from each other and FairSharing has no effect.

## Cleanup

```bash
# Delete all SparkApplications across all namespaces
oc delete sparkapplication -n spark-test --all
oc delete sparkapplication -n tenant-a --all --ignore-not-found
oc delete sparkapplication -n tenant-b --all --ignore-not-found

# Delete multi-tenancy resources (if applied)
oc delete -f examples/openshift/kueue/kueue-multitenancy-resources.yaml --ignore-not-found

# Delete priority/fairsharing resources (if applied)
oc delete -f examples/openshift/kueue/kueue-priority-resources.yaml --ignore-not-found
oc delete -f examples/openshift/kueue/spark-nonadmin-rbac.yaml --ignore-not-found

# Delete basic Kueue resources
oc delete -f examples/openshift/kueue/kueue-resources.yaml
oc delete clusterrolebinding kueue-sparkapplication-finalizers
oc delete clusterrole kueue-sparkapplication-finalizers
oc delete -f examples/openshift/kueue/kueue-cr.yaml
```

## Troubleshooting

### SparkApplications stuck in `SUSPENDED` with no Workloads created

Check Kueue controller logs for RBAC errors:

```bash
oc logs -n openshift-kueue-operator deployment/kueue-controller-manager --tail=50 | grep -i error
```

If you see `cannot set blockOwnerDeletion if an ownerReference refers to a resource you can't set finalizers on`, apply the RBAC from Step 4.

### SparkApplications not intercepted by Kueue (no `suspend=true`)

The namespace is missing the managed label. Apply it:

```bash
oc label namespace spark-test kueue.openshift.io/managed=true --overwrite
```

### `x509: certificate signed by unknown authority` on SparkApplication create

If there are two Spark Operator installations (e.g., one from RHOAI in `redhat-ods-applications` and one from `oc apply -k` in `spark-operator`), their webhook pods fight over the MutatingWebhookConfiguration CA bundle, causing intermittent TLS failures.

Fix: remove the conflicting webhook deployment and let the RHOAI one own the config:

```bash
oc delete deployment spark-operator-webhook -n spark-operator
oc delete service spark-operator-webhook-svc -n spark-operator
oc delete mutatingwebhookconfiguration spark-operator-webhook
oc delete validatingwebhookconfiguration spark-operator-webhook
oc rollout restart deployment spark-operator-webhook -n redhat-ods-applications
```

### `service "spark-operator-webhook-svc" not found`

The Spark Operator is not deployed. Redeploy it:

```bash
oc apply -k config/default/ --server-side=true --force-conflicts
```

### FairSharing test fails -- ClusterQueues not borrowing from each other

Check that your ClusterQueues use `spec.cohortName` (not `spec.cohort`):

```bash
oc get clusterqueue team-a-cq -o jsonpath='{.spec.cohortName}'
```

If empty, update the ClusterQueue YAML to use `cohortName` and re-apply. Also verify the Cohort CRD exists:

```bash
oc get cohorts.kueue.x-k8s.io
```

### Multi-tenancy tests fail — SparkApplications not created in tenant namespaces

The Spark Operator must be configured to watch `tenant-a` and `tenant-b` namespaces. If using Kustomize deployment, check the operator's `--namespaces` flag or set it to watch all namespaces. Also verify the Kueue RBAC (Step 4) applies cluster-wide, not just to the `spark-test` namespace.

### `install plan is not available` during bundle install

An existing RHBoK subscription conflicts with the dev bundle. Uninstall the existing version first (see Step 1).

### `conversion webhook ... service "kueue-webhook-service" not found`

The Kueue controller manager hasn't finished starting. Wait ~30 seconds and retry.

### Image pull errors

The default image `ghcr.io/apache/spark-docker/spark:3.5.4` is publicly available. If you have pull rate limits, pre-pull the image or use an internal registry.

### `AccessDeniedException: /opt/spark/work-dir/...`

On OpenShift, Spark needs a writable work directory. Add an `emptyDir` volume to both driver and executor specs (already included in `spark-pi-kueue.yaml`):

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

## Version Reference

| Component | Version | Upstream Base |
|-----------|---------|---------------|
| RHBoK | 1.4.0 | Kueue v0.17+ |
| RHBoK (previous GA) | 1.3.1 | Kueue v0.16.5 |
| Spark Operator | v2.4.0+ | kubeflow/spark-operator |
| SparkApplication integration | Alpha | Since upstream Kueue v0.17 |
| Spark image | 3.5.4 | ghcr.io/apache/spark-docker/spark |
