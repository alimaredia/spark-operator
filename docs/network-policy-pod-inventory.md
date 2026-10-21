# Pod inventory: pods managed by the Kubeflow Spark Operator at runtime

> **Purpose:** Answers spike task 1 — *"Inventory all pods managed by the Kubeflow Spark Operator operator at runtime"* — as the basis for NetworkPolicy gap analysis (RHOAIENG-90747, R1–R6).
> **Scope note:** "Managed" is split into **operator pods** (the operator's own control plane — out of scope for the operand spike, tracked by **HPSTRAT-278**) and **operand pods** (the Spark workloads the operator produces — in scope).

## Summary

There are **6 pod types** at runtime (not 5 — the SparkConnect *executor* pod is easy to miss):

| # | Pod | Category | NP scope |
|---|-----|----------|----------|
| 1 | Controller | Operator | HPSTRAT-278 (out of scope here) |
| 2 | Webhook | Operator | HPSTRAT-278 (out of scope here) |
| 3 | SparkApplication driver | Operand | **This spike** |
| 4 | SparkApplication executor | Operand | **This spike** |
| 5 | SparkConnect server (= driver) | Operand | **This spike** |
| 6 | SparkConnect executor | Operand | **This spike** |

**No other pod types exist.** A `ScheduledSparkApplication` creates `SparkApplication` objects on a cron schedule — it produces driver/executor pods (#3/#4), not a distinct pod. The Spark UI, driver-ingress, and SparkConnect endpoints are **Services**, not pods.

---

## Operator pods (out of scope — HPSTRAT-278)

These run in the operator install namespace (`redhat-ods-applications` / `opendatahub`). Their NetworkPolicies (HPSTRAT-278) are now shipped on `main` — see [NetworkPolicy — Operator pods (controller #1 + webhook #2)](#networkpolicy--operator-pods-controller-1--webhook-2) below.

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

## Operand pods (in scope — RHOAIENG-90747)

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
- **Network needs:** ingress ← driver + peer executors (this is what Spark 4.2's `NetworkPolicyFeatureStep` / SPARK-55653 isolates); egress → driver, DNS, external object store (executors do the bulk of data read/write). Executors do **not** talk to the kube-apiserver or Maven directly.

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

The operator creates operand NetworkPolicies via `internal/controller/networkpolicy`, invoked from both the SparkApplication and SparkConnect reconcilers, **unconditionally** (no feature flag). It uses **three** policies rather than one blanket, specifically so it composes cleanly with Spark 4.2's own executor policy (see *Spark 4.2 interaction* below):

| Policy | Scope | Selects | Purpose |
|--------|-------|---------|---------|
| `spark-operator-operand-egress` | per namespace (blanket) | all operands (`launched-by-spark-operator`) | allow-all **egress** |
| `spark-operator-operand-ingress` | per namespace (blanket) | **non-executor** operands (`spark-role in {driver, connect-server}`) | intra-Spark ingress + Spark UI/metrics (4040) and Connect gRPC (15002) from any namespace |
| `<workload>-executor` | **per workload** | that workload's executors | same-app ingress only — **identical to Spark's `NetworkPolicyFeatureStep` (SPARK-55653)** |
| `<app>-driver-ui` | **per app**, only when `spark.ui.port` ≠ 4040 | that app's driver | admits the custom Spark UI/metrics port from any namespace (the blanket ingress only admits the default 4040) |

- **Executor ingress is per-app**, keyed on `spark-app-selector=<appID>` for SparkApplications (exactly Spark 4.2's shape) and on `connect-name=<conn>` for SparkConnect (same pod set as the Connect server's `spark-app-selector`). Executors are **not** selected by the blanket ingress policy.
- **Scope:** covers SparkApplications created by a ScheduledSparkApplication too, since those reconcile through the SparkApplication controller.
- **Lifecycle (cleanup):** every SparkApplication / SparkConnect is added as a **non-controller owner reference** on the policies it touches; the Kubernetes garbage collector deletes each once its last owner is gone (the blanket policies when the last workload in the namespace leaves; a `<workload>-executor` policy when that workload is deleted). No manual last-app detection.

### Requirement → design mapping

| R | Dimension | Direction | How addressed |
|---|-----------|-----------|---------------|
| **R1** | Monitoring | Ingress | Driver metrics are served on the UI port (4040, `/metrics/prometheus`), so the 4040 rule in the ingress blanket admits the monitoring stack. If an app sets a non-default `spark.ui.port`, the per-app `<app>-driver-ui` policy admits that port (UI + metrics ride the same port). A dedicated metrics-exporter port is not covered — document "bring your own policy" for that case. |
| **R2** | Gateway/router + clients | Ingress | Spark UI (**4040**) and Spark Connect gRPC (**15002**) from **any namespace** (`namespaceSelector: {}`) on the ingress blanket — so cross-namespace consumers (ingress router, monitoring, Connect clients such as workbenches) are not blocked. |
| **R3** | API server | Egress | Covered by the egress allow-all policy. |
| **R4** | Intra-component | Ingress + Egress | Driver/server ingress from any operand in the namespace (all ports); executor ingress from same-app pods (the per-app policy). Egress via the allow-all policy. |
| **R5** | DNS | Egress | Covered by the egress allow-all policy. |
| **R6** | External services | Egress | Covered by the egress allow-all policy (Maven/PyPI/S3 can't be FQDN-matched in NetworkPolicy). |

> **Portability:** no OpenShift-specific labels, so it works on upstream Kubernetes as well as OpenShift. Cross-namespace consumer access is handled by opening the two consumer ports (4040, 15002) to **all** namespaces rather than a platform-specific tier. All *other* ports (executor shuffle/block-manager, Connect RPC 7078/7079) stay reachable only from same-app / same-namespace Spark pods.

### Spark 4.2 interaction

Spark 4.2's `NetworkPolicyFeatureStep` (SPARK-55653) makes `spark-submit` create a **per-app executor ingress** policy (`from: spark-app-selector=<appID>`), owned by the driver pod. If the operator's bundled Spark is bumped to 4.2+, both Spark's policy and ours would select the executor pods. Because NetworkPolicies are **additive**, a *blanket* executor ingress (from any same-namespace Spark pod) would **widen** — effectively nullify — Spark's same-app isolation.

Keeping our executor ingress **per-app and identical to SPARK-55653** avoids this: the two policies express the same same-app rule, so their union is still same-app — redundant but not widening. The blanket policies deliberately exclude executors, so they never touch Spark's executor policy. (Spark 4.2's policy is **ingress-only** and does not grant executor egress; our shared egress policy still provides that, which matters under a default-deny-egress posture.)

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
    # R4 — intra-Spark: any operand pod in the namespace, all ports
    - from:
        - podSelector: {matchLabels: {sparkoperator.k8s.io/launched-by-spark-operator: "true"}}
    # R1/R2 — Spark UI + metrics (4040) and Connect gRPC (15002) from any namespace
    - from:
        - namespaceSelector: {}
      ports:
        - {protocol: TCP, port: 4040}
        - {protocol: TCP, port: 15002}
---
# 3. <workload>-executor — per-app executor ingress (matches Spark 4.2 SPARK-55653)
#    SparkApplication: groupLabel = spark-app-selector=<appID>
#    SparkConnect:     groupLabel = sparkoperator.k8s.io/connect-name=<conn>
spec:
  podSelector:
    matchLabels: {<groupLabel>: <value>, spark-role: executor}
  policyTypes: [Ingress]
  ingress:
    - from:
        - podSelector: {matchLabels: {<groupLabel>: <value>}}
---
# 4. <app>-driver-ui — created ONLY when spark.ui.port != 4040 (SparkApplication)
#    admits the custom UI/metrics port that the blanket ingress (4040) misses
spec:
  podSelector:
    matchLabels: {sparkoperator.k8s.io/app-name: <app>, spark-role: driver}
  policyTypes: [Ingress]
  ingress:
    - from:
        - namespaceSelector: {}
      ports:
        - {protocol: TCP, port: <spark.ui.port>}
```

Each policy carries one non-controller `ownerReference` per owning workload for GC.

### Lifecycle, cleanup & idempotency

`EnsureForSparkApplication` / `EnsureForSparkConnect` run on every reconcile (SparkApplication: on the Running-state pass, once `app.Status.SparkApplicationID` is set; SparkConnect: each reconcile) via `controllerutil.CreateOrUpdate`, which **GETs first and only writes when something changed**:

| Situation | Result |
|-----------|--------|
| First workload in the namespace | policies don't exist → one **Create** each |
| A new workload reconciles | shared policy exists but this workload isn't an owner yet → `SetOwnerReference` appends it → one **Update**; the workload's own `<workload>-executor` policy is created |
| Steady state (policies exist, workload already an owner, spec unchanged) | mutate yields identical objects → **no write** (cached GETs only) |

So the policies are **not** re-created on every pass. `SetOwnerReference` is idempotent (never duplicates an owner) and the specs are static, so re-running `Ensure…` for an already-registered workload is a no-op.

**Cleanup is automatic — there is no explicit delete code.** Each SparkApplication / SparkConnect is recorded as a **non-controller owner reference** on the policies it touches, and the Kubernetes garbage collector handles removal:

- When a workload is **deleted**, the GC removes its owner reference from each policy.
- The two **blanket** policies are deleted when the **last** workload in the namespace is gone; a **`<workload>-executor`** policy is deleted as soon as that single workload is gone.

This satisfies "clean up when the last application is shut down" (ODH-ADR-Operator-0016) without the operator doing last-app detection. Notes:
- Cleanup triggers on CR **deletion**, not completion — a `Completed` SparkApplication that still exists keeps its owner references, so the policies persist (it may rerun).
- GC runs **independently of the operator**, so cleanup happens even if the operator is down when the last workload is deleted.

**Edge cases (all self-healing):**
- If a blanket policy's last owner is removed at the same moment another workload is still starting (before it has added its own owner reference), GC may briefly delete it; the starting workload's next reconcile recreates it (closed on requeue).
- If a policy is manually deleted while workloads still exist, the next reconcile recreates it.
- GC is asynchronous, so there is a short delay between the last workload's deletion and the policy's removal.

### Packaging

The policies are **created and maintained by the operator at runtime** (not static manifests), via `internal/controller/networkpolicy`, **unconditionally** (no feature flag). The controller RBAC grants `networkpolicies` (get/list/watch/create/update/patch/delete) in both `config/rbac/role.yaml` and the Helm chart. Contrast with the **operator-pod** policies below, which are static overlay manifests.

### Extending the policies (bring-your-own)

To allow additional ingress (e.g. a dedicated metrics-exporter port, a Spark Thrift/JDBC port, or a custom app port, reached from another namespace), **do not edit the operator-managed policies** — they are reconciled with `CreateOrUpdate`, so manual edits are reverted on the next pass. Instead, create a **separate** NetworkPolicy in the workload namespace. NetworkPolicies are **additive** (a pod's allowed traffic is the union of all policies selecting it, with no deny), so a sibling policy can only *add* permissiveness, and the operator leaves it alone.

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: my-spark-driver-extra        # any name not used by the operator
  namespace: <workload-namespace>
spec:
  podSelector:
    matchLabels:
      sparkoperator.k8s.io/app-name: <app>   # or spark-role: driver / connect-server,
      spark-role: driver                      # or launched-by-spark-operator: "true"
  policyTypes: [Ingress]
  ingress:
    - from:
        - namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: my-clients}}
      ports:
        - {protocol: TCP, port: 10000}
```

- Apply it any time (before or after the workload exists); the CNI re-evaluates continuously.
- The operator does not garbage-collect it — manage its lifecycle yourself, or add an `ownerReference` to the SparkApplication/SparkConnect so kube GC removes it with the workload.
- This only **adds** allows; it cannot make the operator's policies more restrictive (NetworkPolicy has no deny). There is no `spec.networkPolicy` CR field today, so a sibling policy is the supported customization point.

---

## NetworkPolicy — Operator pods (controller #1 + webhook #2)

> **Scope / provenance:** The controller and webhook are **operator pods**. These policies were added on `main` (PR #198, plus the pre-existing metrics policy) in the RHOAI and ODH overlays, implementing the ODH NetworkPolicy platform contract (**ODH-ADR-Operator-0016**). They are **static manifests**, not reconciler-generated.

All three policies select **`app.kubernetes.io/name: spark-operator`**, so one set covers **both** the controller and the webhook pods together (and *not* operand pods, which carry `launched-by-spark-operator`). This satisfies ADR-0016's rule that every operator workload be selected by at least one `Ingress` policy and one `Egress` policy.

### Requirement → design mapping (operator-pod set)

| R | Dimension | Direction | Needed? | How addressed on main |
|---|-----------|-----------|---------|------------------------|
| **R1** | Monitoring | Ingress | **Yes** | `spark-operator-allow-metrics`: **8080** (http) + **8443** (future secure) from `network.openshift.io/policy-group: monitoring`; **8081** health, port-scoped. |
| **R2** | Gateway/router | Ingress | **No** | Operator pods aren't exposed via a Route. |
| **R3** | API server | Ingress + Egress | **Yes** | **Ingress** `spark-operator-allow-webhook`: **9443**, port-scoped only (apiserver traffic is node-SNAT'd and unselectable — ADR-0016). **Egress** via allow-all below. |
| **R4** | Intra-component | — | **No** | Leader election is via the API-server Lease, not pod-to-pod. |
| **R5** | DNS | Egress | **Yes** | Covered by the allow-all egress. |
| **R6** | External services | Egress | **Yes (controller)** | `spark-operator-allow-egress`: **allow-all** — the controller resolves user-specified dependency URLs, which can't be enumerated (ADR-0016). |

### Policies (as shipped on main)

```yaml
# config/overlays/{rhoai,odh}/networkpolicy-webhook.yaml  — R3 admission ingress
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: spark-operator-allow-webhook
spec:
  # 9443: admission webhook — API-server traffic is node-sourced and
  # unselectable, so port-scoped only (ADR-0016).
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
  # Allow-all egress (ADR-0016): the controller resolves user-specified
  # dependency URLs, so egress destinations cannot be enumerated.
  podSelector:
    matchLabels:
      app.kubernetes.io/name: spark-operator
  policyTypes:
    - Egress
  egress:
    - {}
```

### Packaging

Three static manifests in each overlay, referenced from `kustomization.yaml`:

```yaml
# config/overlays/{rhoai,odh}/kustomization.yaml
resources:
  - ../../default
  - ../../spark-rbac
  - ../../prometheus
  - networkpolicy.yaml            # spark-operator-allow-internal (operator-namespace ingress allow)
  - networkpolicy-metrics.yaml    # R1 monitoring + health
  - networkpolicy-egress.yaml     # R3/R5/R6 allow-all egress
  - networkpolicy-webhook.yaml    # R3 admission 9443
```

**Notes:**
- All manifests omit `metadata.namespace`; each overlay's `namespace:` directive stamps it (`redhat-ods-applications` / `opendatahub`).
- The `app.kubernetes.io/name: spark-operator` selector covers controller **and** webhook pods together — there is no per-component split.
- Egress is **allow-all** by design: ADR-0016 permits it where destinations can't be enumerated. A webhook-*only* deployment could tighten egress to DNS + API server, but the shared selector also covers the controller, which needs broad egress.
