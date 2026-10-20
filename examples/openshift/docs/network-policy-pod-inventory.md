# Pod inventory: pods managed by the Kubeflow Spark Operator at runtime

> **Purpose:** Inventory all pods managed by the Kubeflow Spark Operator at runtime, as the basis for the operand NetworkPolicy design (mapped to the R1–R6 requirement dimensions below).

## Summary

There are **6 pod types** at runtime for KSO:

| # | Pod | Category | NP scope |
|---|-----|----------|----------|
| 1 | Controller | Operator | Static overlay policies (see [Operator pods](#networkpolicy--operator-pods-controller-1--webhook-2)) |
| 2 | Webhook | Operator | Static overlay policies (see [Operator pods](#networkpolicy--operator-pods-controller-1--webhook-2)) |
| 3 | SparkApplication driver | Operand | `OperandNetworkPolicy` gate |
| 4 | SparkApplication executor | Operand | `OperandNetworkPolicy` gate |
| 5 | SparkConnect server (= driver) | Operand | `OperandNetworkPolicy` gate |
| 6 | SparkConnect executor | Operand | `OperandNetworkPolicy` gate |

A `ScheduledSparkApplication` creates `SparkApplication` objects on a cron schedule — it produces driver/executor pods (#3/#4), not a distinct pod. The Spark UI, driver-ingress, and SparkConnect endpoints are **Services**, not pods.

---

## Operator pod

These run in the operator install namespace (`redhat-ods-applications` / `opendatahub`). Their NetworkPolicies are now shipped on `main` — see [NetworkPolicy — Operator pods (controller #1 + webhook #2)](#networkpolicy--operator-pods-controller-1--webhook-2) below.

### 1. Controller
- **Created by:** `Deployment/spark-operator-controller` (`config/manager/manager.yaml`), 1 replica.
- **Namespace:** operator install namespace.
- **Labels:** `app.kubernetes.io/name=spark-operator`, `app.kubernetes.io/component=controller`, `control-plane=controller-manager`.
- **Ports:** `8080` metrics; health probes on `8081`.
- **Network paths:** egress to kube-apiserver (reconcile, leader election) + DNS; ingress on `8080` from monitoring.

### 2. Webhook
- **Created by:** `Deployment/spark-operator-webhook` (`config/webhook/deployment.yaml`), 1 replica.
- **Namespace:** operator install namespace.
- **Labels:** `app.kubernetes.io/name=spark-operator`, `app.kubernetes.io/component=webhook`.
- **Ports:** `9443` admission server (fronted by `spark-operator-webhook-svc` :443), `8080` metrics; probes on `8081`.
- **Network paths:** ingress on `9443` from the **kube-apiserver** (admission; `failurePolicy: Fail`), ingress `8080` from monitoring; egress to kube-apiserver + DNS.

---

## Operand pods

These run in **user / data-science-project namespaces** — wherever the `SparkApplication`/`SparkConnect` CR is created — **not** in the operator namespace. All carry `sparkoperator.k8s.io/launched-by-spark-operator: "true"`.

Common label constants (`pkg/common/spark.go`): `LabelLaunchedBySparkOperator`, `LabelSparkRole` (`spark-role`), `LabelSparkApplicationSelector` (`spark-app-selector` = Spark appID), `LabelSparkAppName` (`sparkoperator.k8s.io/app-name`), `LabelSparkConnectName` (`sparkoperator.k8s.io/connect-name`).

### 3. SparkApplication driver
- **Created by:** `spark-submit` (REST/CLI) from the controller; the Spark distribution creates the pod (`internal/controller/sparkapplication/submission.go`).
- **Namespace:** the CR's namespace (user).
- **Container:** `spark-kubernetes-driver`.
- **Labels:** `launched-by-spark-operator=true`, `app-name=<app>`, `spark-role=driver`, `spark-app-selector=<appID>`, `spark-version`.
- **Ports it listens on:** Spark UI **4040** (`spark.ui.port`, default); driver RPC + block-manager ports = **Spark defaults (random)**, not pinned by the operator.
- **Network needs:** egress → kube-apiserver (creates/watches executors), DNS, external (Maven/PyPI for `--packages`, object store); ingress ← executors (RPC/block-manager), monitoring + router (UI 4040).

### 4. SparkApplication executor
- **Created by:** the driver (via executor conf passed to `spark-submit`).
- **Namespace:** the CR's namespace (user).
- **Container:** `executor`.
- **Labels:** `launched-by-spark-operator=true`, `app-name=<app>`, `spark-role=executor`, `spark-exec-id`, `spark-app-selector=<appID>`.
- **Ports it listens on:** block-manager / shuffle / block-transfer ports = **Spark defaults (random)**.
- **Network needs:** ingress ← driver + peer executors (this is what Spark 4.2's `NetworkPolicyFeatureStep` isolates); egress → driver, DNS, external object store (executors do the bulk of data read/write). Executors do **not** talk to the kube-apiserver or Maven directly.

### 5. SparkConnect server (acts as the driver)
- **Created by:** the operator **directly** (`createOrUpdateServerPod`, `internal/controller/sparkconnect/reconciler.go`). Pod `<conn>-server`.
- **Namespace:** the CR's namespace (user).
- **Container:** `spark-kubernetes-driver`.
- **Labels:** `launched-by-spark-operator=true`, `connect-name=<conn>`, `spark-role=connect-server`, `spark-version`.
- **Ports (pinned):** driver RPC **7078**, block-manager **7079**, UI **4040**, Spark Connect gRPC **15002**. Fronted by Service `<conn>-server`.
- **Network needs:** egress → kube-apiserver (manages executors), DNS, external; ingress ← Connect gRPC clients (workbenches/router on 15002), executors (7078/7079), monitoring + router (UI 4040).

### 6. SparkConnect executor
- **Created by:** the SparkConnect server pod.
- **Namespace:** the CR's namespace (user).
- **Container:** `executor`.
- **Labels:** `launched-by-spark-operator=true`, `connect-name=<conn>`, `spark-role=executor`.
- **Ports it listens on:** block-manager / shuffle = **Spark defaults (random)**.
- **Network needs:** ingress ← Connect server + peer executors; egress → Connect server, DNS, external object store. Does not talk to the kube-apiserver directly.

---

## NetworkPolicy design — operand pods (implemented)

The operator creates operand NetworkPolicies via `internal/controller/networkpolicy`, invoked from both the SparkApplication and SparkConnect reconcilers. It is gated on the **`OperandNetworkPolicy` feature gate** (`pkg/features`), which is **off by default**; the **RHOAI/ODH overlays** turn it on by appending `--feature-gates=OperandNetworkPolicy=true` to the controller args. Upstream/Helm installs leave it disabled. It uses **three** policies rather than one blanket, specifically so it composes cleanly with Spark 4.2's own executor policy (see *Spark 4.2 interaction* below):

| Policy | Scope | Selects | Purpose |
|--------|-------|---------|---------|
| `spark-operator-operand-egress` | per namespace (blanket) | all operands (`launched-by-spark-operator`) | allow-all **egress** |
| `spark-operator-operand-ingress` | per namespace (blanket) | **non-executor** operands (`spark-role in {driver, connect-server}`) | intra-Spark ingress only — from any operand in the namespace, all ports (executors reach the driver/server on Spark's random RPC/block-manager ports) |
| `<workload>-sparkapp-executor` / `<workload>-sparkconnect-executor` | **per workload** | that workload's executors | same-app ingress only — **identical to Spark's `NetworkPolicyFeatureStep`**. The suffix is per-kind so a SparkApplication and a SparkConnect sharing a name in one namespace do not converge the same policy. |

- **Executor ingress is per-app**, keyed on `spark-app-selector=<appID>` for SparkApplications (exactly Spark 4.2's shape) and on `connect-name=<conn>` for SparkConnect (same pod set as the Connect server's `spark-app-selector`). Executors are **not** selected by the blanket ingress policy.
- **Scope:** covers SparkApplications created by a ScheduledSparkApplication too, since those reconcile through the SparkApplication controller.
- **Lifecycle (cleanup):** every SparkApplication / SparkConnect is added as a **non-controller owner reference** on the policies it touches; the Kubernetes garbage collector deletes each once its last owner is gone (the blanket policies when the last workload in the namespace leaves; a per-workload executor policy when that workload is deleted). No manual last-app detection.
- **The operator's own namespace is intentionally exempt — do not run Spark workloads there.** The RHOAI/ODH overlays ship `spark-operator-allow-internal` (`config/overlays/{rhoai,odh}/networkpolicy.yaml`), a static manifest that selects every operand pod in the operator's namespace and admits 4040/7078/7079 from *any* pod in that namespace, plus 15002 from workbench pods in dashboard namespaces. On RHOAI a platform-owned policy in the same namespace (`podSelector: {}`, no port restrictions) widens it further. Neither is owned by this feature, and NetworkPolicy has no deny, so the policies above **cannot** restore isolation there — operand ingress in the operator's namespace is wider than this design describes, and that is accepted rather than worked around. Run every SparkApplication / SparkConnect in its own namespace and treat the operator's namespace as off-limits for workloads.
- **Consumer ingress is not opened by the operator** (least privilege). Selecting the operand pods with the policies above denies all other ingress by default; exposing the Spark UI, Spark Connect gRPC, or metrics to other namespaces is a **user-managed additive policy** (see *Extending the policies*), the same way RBAC is provisioned per workload namespace.

### Requirement → design mapping

| R | Dimension | Direction | How addressed |
|---|-----------|-----------|---------------|
| **R1** | Monitoring | Ingress | **Not opened by the operator.** Driver metrics ride the UI port; `kubectl port-forward` works, but *any* scrape needs a user-managed policy - a scraper pod does not carry `launched-by-spark-operator`, so same-namespace scraping is denied too. |
| **R2** | Gateway/router + clients | Ingress | **Not opened by the operator.** The Spark UI and Connect gRPC are reachable via `port-forward` only; *all* client access is a user-managed additive policy - a same-namespace notebook as much as the ingress router or a workbench→Connect hop. |
| **R3** | API server | Egress | Covered by the egress allow-all policy. |
| **R4** | Intra-component | Ingress + Egress | Driver/server ingress from any operand in the namespace (all ports); executor ingress from same-app pods (the per-app policy). Egress via the allow-all policy. |
| **R5** | DNS | Egress | Covered by the egress allow-all policy. |
| **R6** | External services | Egress | Covered by the egress allow-all policy (Maven/PyPI/S3 can't be FQDN-matched in NetworkPolicy). |

> **Least privilege by default:** the operator ships only what a job needs to run — allow-all egress and intra-Spark ingress. Operand pods are therefore default-deny for all other ingress. Consumer exposure (UI, gRPC, metrics) is opt-in via a user-managed additive policy, which avoids broad all-namespaces exposure and keeps the operator free of platform-specific selectors.

### Spark 4.2 interaction

Spark 4.2's `NetworkPolicyFeatureStep` makes `spark-submit` create a **per-app executor ingress** policy (`from: spark-app-selector=<appID>`), owned by the driver pod. If the operator's bundled Spark is bumped to 4.2+, both Spark's policy and ours would select the executor pods. Because NetworkPolicies are **additive**, a *blanket* executor ingress (from any same-namespace Spark pod) would **widen** — effectively nullify — Spark's same-app isolation.

Keeping our executor ingress **per-app and identical to Spark's own** avoids this: the two policies express the same same-app rule, so their union is still same-app — redundant but not widening. The blanket policies deliberately exclude executors, so they never touch Spark's executor policy. (Spark 4.2's policy is **ingress-only** and does not grant executor egress; our shared egress policy still provides that, which matters under a default-deny-egress posture.)

### Policies

```yaml
# 1. spark-operator-operand-egress — allow-all egress for every operand (per namespace)
spec:
  podSelector: {matchLabels: {sparkoperator.k8s.io/launched-by-spark-operator: "true"}}
  policyTypes: [Egress]
  egress:
    - {}
---
# 2. spark-operator-operand-ingress — driver + Connect server ingress (per namespace)
spec:
  podSelector:
    matchLabels: {sparkoperator.k8s.io/launched-by-spark-operator: "true"}
    matchExpressions:
      - {key: spark-role, operator: In, values: [driver, connect-server]}
  policyTypes: [Ingress]
  ingress:
    # R4 — intra-Spark only: any operand pod in the namespace, all ports.
    # No consumer-port rule — UI/gRPC/metrics are user-managed (see below).
    - from:
        - podSelector: {matchLabels: {sparkoperator.k8s.io/launched-by-spark-operator: "true"}}
---
# 3. <workload>-sparkapp-executor / <workload>-sparkconnect-executor
#    per-app executor ingress (matches Spark 4.2 NetworkPolicyFeatureStep)
#    SparkApplication: name suffix -sparkapp-executor,     groupLabel = spark-app-selector=<appID>
#    SparkConnect:     name suffix -sparkconnect-executor, groupLabel = sparkoperator.k8s.io/connect-name=<conn>
spec:
  podSelector:
    matchLabels: {<groupLabel>: <value>, spark-role: executor}
  policyTypes: [Ingress]
  ingress:
    - from:
        - podSelector: {matchLabels: {<groupLabel>: <value>}}
```

Each policy carries one non-controller `ownerReference` per owning workload for GC.

### Lifecycle, cleanup & idempotency

`EnsureForSparkApplication` / `EnsureForSparkConnect` run on every reconcile (SparkApplication: on the Running-state pass, once `app.Status.SparkApplicationID` is set; SparkConnect: each reconcile) via `controllerutil.CreateOrUpdate`, which **GETs first and only writes when something changed**:

| Situation | Result |
|-----------|--------|
| First workload in the namespace | policies don't exist → one **Create** each |
| A new workload reconciles | shared policy exists but this workload isn't an owner yet → `SetOwnerReference` appends it → one **Update**; the workload's own per-kind executor policy is created |
| Steady state (policies exist, workload already an owner, spec unchanged) | mutate yields identical objects → **no write** (cached GETs only) |

So the policies are **not** re-created on every pass. `SetOwnerReference` is idempotent (never duplicates an owner) and the specs are static, so re-running `Ensure…` for an already-registered workload is a no-op.

**Cleanup is automatic — there is no explicit delete code.** Each SparkApplication / SparkConnect is recorded as a **non-controller owner reference** on the policies it touches, and the Kubernetes garbage collector handles removal:

- When a workload is **deleted**, the GC removes its owner reference from each policy.
- The two **blanket** policies are deleted when the **last** workload in the namespace is gone; a **per-workload executor** policy is deleted as soon as that single workload is gone.

This satisfies "clean up when the last application is shut down" without the operator doing last-app detection. Notes:
- Cleanup triggers on CR **deletion**, not completion — a `Completed` SparkApplication that still exists keeps its owner references, so the policies persist (it may rerun).
- GC runs **independently of the operator**, so cleanup happens even if the operator is down when the last workload is deleted.

**Edge cases (all self-healing):**
- If a blanket policy's last owner is removed at the same moment another workload is still starting (before it has added its own owner reference), GC may briefly delete it; the starting workload's next reconcile recreates it (closed on requeue).
- If a policy is manually deleted while workloads still exist, the next reconcile recreates it.
- GC is asynchronous, so there is a short delay between the last workload's deletion and the policy's removal.

### Packaging

The policies are **created and maintained by the operator at runtime** (not static manifests), via `internal/controller/networkpolicy`, gated on the **`OperandNetworkPolicy` feature gate** (`pkg/features`, off by default). The feature is **scoped to the RHOAI/ODH overlays**: they enable it by appending `--feature-gates=OperandNetworkPolicy=true` to the controller args, and they grant the `networkpolicies` RBAC via their `networkpolicy-rbac-patch.yaml` overlay patch. The shared `config/rbac` base and the Helm chart carry **no** footprint for this feature, so upstream syncs stay clean. Contrast with the **operator-pod** policies below, which are static overlay manifests.

**Related static policy.** `config/overlays/{rhoai,odh}/networkpolicy.yaml` ships `spark-operator-allow-internal`. Unlike the operator-pod policies further down, it selects **operand** pods (`launched-by-spark-operator`), not operator pods. It is not part of this feature and is not deprecated by it; it takes effect only in the operator's own namespace, where workloads are not expected to run (see above).

**Opting out / rollback.** The gate is off unless an overlay enables it, so plain/upstream and Helm installs never create these policies. To disable it where it is enabled, drop the `--feature-gates=OperandNetworkPolicy=true` arg (or set `OperandNetworkPolicy=false`). Like the driver-PDB gate, **disabling is a no-op for policies that already exist** — they carry owner references to live workloads, so they persist until each workload is deleted (then kube GC removes them). To roll back immediately, disable the gate **and** delete the existing policies, e.g.:

```bash
kubectl delete networkpolicy -A -l sparkoperator.k8s.io/created-by-spark-operator=true
```

### Extending the policies (bring-your-own)

The operator intentionally does **not** open consumer ingress (Spark UI, Connect gRPC, metrics scraping). To expose those — or any extra port such as a metrics-exporter, Spark Thrift/JDBC, or custom app port — **do not edit the operator-managed policies** (they are reconciled with `CreateOrUpdate`, so manual edits are reverted on the next pass). Instead, create a **separate** NetworkPolicy in the workload namespace. NetworkPolicies are **additive** (a pod's allowed traffic is the union of all policies selecting it, with no deny), so a sibling policy only *adds* permissiveness and the operator leaves it alone. Apply it any time; the operator does not garbage-collect it (add an `ownerReference` to the SparkApplication/SparkConnect if you want kube GC to remove it with the workload).

**Namespace enablement (same-namespace consumers).** The simplest, least-permissive policy — apply it once per workload namespace **alongside the ServiceAccount/Role/RoleBinding**, before creating any SparkApplication/SparkConnect. It is deliberately **not** shipped in `config/spark-rbac`: that path is shared with upstream installs, where the `OperandNetworkPolicy` gate is off and no operator policy selects the driver, so this policy would become the *only* ingress policy on it and cut off executor→driver traffic. It admits the Spark UI (4040) and Connect gRPC (15002) to the driver/Connect-server pods **from any pod in the same namespace** (e.g. a notebook or Spark Connect client co-located in the project). This is exactly what the e2e UI and SparkConnect tests apply during setup.
```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: spark-operator-allow-consumers
  namespace: <workload-namespace>
spec:
  podSelector:
    matchExpressions:
      - {key: spark-role, operator: In, values: [driver, connect-server]}
  policyTypes: [Ingress]
  ingress:
    - from:
        - podSelector: {}            # any pod in the same namespace
      ports:
        - {protocol: TCP, port: 4040}    # Spark UI (and PrometheusServlet metrics)
        - {protocol: TCP, port: 15002}   # Spark Connect gRPC
```

For **cross-namespace** consumers, add a **separate policy** whose `from` peer is a `namespaceSelector` targeting the consumer tier. These are additive rather than alternatives: the namespace-enablement policy above admits same-namespace pods only, so exposing the Spark UI through a Route needs `spark-ui-from-router` *as well as* that policy, not instead of it.

**Spark UI via the OpenShift router** (admit 4040 from the ingress tier):
```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: spark-ui-from-router
  namespace: <workload-namespace>
spec:
  podSelector:
    matchLabels: {spark-role: driver}          # all SparkApplication drivers in the namespace
  policyTypes: [Ingress]
  ingress:
    - from:
        - namespaceSelector:
            matchLabels: {network.openshift.io/policy-group: ingress}
      ports:
        - {protocol: TCP, port: 4040}          # or your custom spark.ui.port
```

**Workbench → Spark Connect gRPC** (admit 15002 from RHOAI dashboard/workbench namespaces):
```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: spark-connect-from-workbenches
  namespace: <workload-namespace>              # where the SparkConnect server runs
spec:
  podSelector:
    matchLabels: {spark-role: connect-server}
  policyTypes: [Ingress]
  ingress:
    - from:
        - namespaceSelector: {matchLabels: {opendatahub.io/dashboard: "true"}}
          podSelector: {matchLabels: {opendatahub.io/workbenches: "true"}}
      ports:
        - {protocol: TCP, port: 15002}
```

**Monitoring scrape of driver metrics** (admit the UI/metrics port from the monitoring tier):
```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: spark-metrics-from-monitoring
  namespace: <workload-namespace>
spec:
  podSelector:
    matchLabels: {spark-role: driver}
  policyTypes: [Ingress]
  ingress:
    - from:
        - namespaceSelector: {matchLabels: {network.openshift.io/policy-group: monitoring}}
      ports:
        - {protocol: TCP, port: 4040}          # PrometheusServlet rides the UI port
```

Selector options for `podSelector`: `spark-role: driver` / `connect-server` (all of a kind), `sparkoperator.k8s.io/app-name: <app>` / `connect-name: <conn>` (one workload), or `sparkoperator.k8s.io/launched-by-spark-operator: "true"` (all operands).

- Apply it any time (before or after the workload exists); the CNI re-evaluates continuously.
- The operator does not garbage-collect it — manage its lifecycle yourself, or add an `ownerReference` to the SparkApplication/SparkConnect so kube GC removes it with the workload.
- This only **adds** allows; it cannot make the operator's policies more restrictive (NetworkPolicy has no deny). There is no `spec.networkPolicy` CR field today, so a sibling policy is the supported customization point.

---

## NetworkPolicy — Operator pods (controller #1 + webhook #2)

> **Scope / provenance:** The controller and webhook are **operator pods**. These policies ship as static manifests in the RHOAI and ODH overlays (not reconciler-generated), implementing the ODH NetworkPolicy platform contract.

All three policies select **`app.kubernetes.io/name: spark-operator`**, so one set covers **both** the controller and the webhook pods together (and *not* operand pods, which carry `launched-by-spark-operator`). This satisfies the platform contract's rule that every operator workload be selected by at least one `Ingress` policy and one `Egress` policy.

### Requirement → design mapping (operator-pod set)

| R | Dimension | Direction | Needed? | How addressed on main |
|---|-----------|-----------|---------|------------------------|
| **R1** | Monitoring | Ingress | **Yes** | `spark-operator-allow-metrics`: **8080** (http) + **8443** (future secure) from `network.openshift.io/policy-group: monitoring`; **8081** health, port-scoped. |
| **R2** | Gateway/router | Ingress | **No** | Operator pods aren't exposed via a Route. |
| **R3** | API server | Ingress + Egress | **Yes** | **Ingress** `spark-operator-allow-webhook`: **9443**, port-scoped only (apiserver traffic is node-SNAT'd and unselectable). **Egress** via allow-all below. |
| **R4** | Intra-component | — | **No** | Leader election is via the API-server Lease, not pod-to-pod. |
| **R5** | DNS | Egress | **Yes** | Covered by the allow-all egress. |
| **R6** | External services | Egress | **Yes (controller)** | `spark-operator-allow-egress`: **allow-all** — the controller resolves user-specified dependency URLs, which can't be enumerated. |

### Policies (as shipped on main)

```yaml
# config/overlays/{rhoai,odh}/networkpolicy-webhook.yaml  — R3 admission ingress
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: spark-operator-allow-webhook
spec:
  # 9443: admission webhook — API-server traffic is node-sourced and
  # unselectable, so port-scoped only.
  podSelector:
    matchLabels:
      app.kubernetes.io/name: spark-operator
  policyTypes:
    - Ingress
  ingress:
    - ports:
        - port: 9443
          protocol: TCP
---
# config/overlays/{rhoai,odh}/networkpolicy-metrics.yaml  — R1 monitoring + health ingress
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: spark-operator-allow-metrics
spec:
  # 8080: current HTTP metrics; 8443: future secure metrics — monitoring namespaces only.
  # 8081: health probes — port-scoped, no peer selector (kubelet not selectable).
  podSelector:
    matchLabels:
      app.kubernetes.io/name: spark-operator
  policyTypes:
    - Ingress
  ingress:
    - ports:
        - port: 8080
          protocol: TCP
        - port: 8443
          protocol: TCP
      from:
        - namespaceSelector:
            matchLabels:
              network.openshift.io/policy-group: monitoring
    - ports:
        - port: 8081
          protocol: TCP
---
# config/overlays/{rhoai,odh}/networkpolicy-egress.yaml  — R3/R5/R6 egress
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: spark-operator-allow-egress
spec:
  # Allow-all egress: the controller resolves user-specified
  # dependency URLs, so egress destinations cannot be enumerated.
  podSelector:
    matchLabels:
      app.kubernetes.io/name: spark-operator
  policyTypes:
    - Egress
  egress:
    - {}
```

**Notes:**
- All manifests omit `metadata.namespace`; each overlay's `namespace:` directive stamps it (`redhat-ods-applications` / `opendatahub`).
- The `app.kubernetes.io/name: spark-operator` selector covers controller **and** webhook pods together — there is no per-component split.
- Egress is **allow-all** by design: the platform contract permits it where destinations can't be enumerated. A webhook-*only* deployment could tighten egress to DNS + API server, but the shared selector also covers the controller, which needs broad egress.
