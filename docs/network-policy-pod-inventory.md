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

The operator creates a **single blanket NetworkPolicy per namespace** — `spark-operator-operands` — covering **all** operand pods (#3 SparkApplication driver, #4 executor, #5 SparkConnect server, #6 SparkConnect executor), rather than one policy per pod type or per application instance. It is built by `internal/controller/networkpolicy` and invoked from both the SparkApplication and SparkConnect reconcilers.

- **Always on:** the operator creates the policy unconditionally — there is no feature flag. (Egress is allow-all and the consumer ports are open to all namespaces, so enabling it does not block previously-working UI/metrics/Connect-client paths.)
- **Selector / scope:** selects every operand pod via `sparkoperator.k8s.io/launched-by-spark-operator=true` in the workload's namespace. This also covers SparkApplications created by a ScheduledSparkApplication, since those reconcile through the SparkApplication controller.
- **Lifecycle (cleanup):** each SparkApplication / SparkConnect with operands in the namespace is added as a **non-controller owner reference** on the shared policy. The Kubernetes garbage collector deletes the policy automatically once the **last** such workload in the namespace is removed — no manual last-app detection.

### Requirement → design mapping

| R | Dimension | Direction | How addressed |
|---|-----------|-----------|---------------|
| **R1** | Monitoring | Ingress | Driver metrics are served on the UI port (4040, `/metrics/prometheus`), so the 4040 rule below also admits the monitoring stack. Executor/custom metric ports are not covered. |
| **R2** | Gateway/router + clients | Ingress | Spark UI (**4040**) and Spark Connect gRPC (**15002**) from **any namespace** (`namespaceSelector: {}`), so cross-namespace consumers — an ingress router, the monitoring stack, Spark Connect clients such as workbenches — are not blocked. |
| **R3** | API server | Egress | Covered by allow-all egress. |
| **R4** | Intra-component | Ingress + Egress | Ingress from any operand pod in the namespace, **all ports** (Spark uses random RPC/block-manager ports). Egress covered by allow-all. |
| **R5** | DNS | Egress | Covered by allow-all egress. |
| **R6** | External services | Egress | Covered by allow-all egress (Maven/PyPI/S3 can't be FQDN-matched in NetworkPolicy). |

> **Portability:** the operand policy uses no OpenShift-specific labels, so it works on upstream Kubernetes as well as OpenShift. Cross-namespace consumer access is handled by opening the two consumer ports (4040, 15002) to **all** namespaces rather than targeting a platform-specific tier — the generic alternative to OpenShift `policy-group` labels or configurable selectors. All *other* operand ports (executor shuffle/block-manager, Connect RPC 7078/7079) remain reachable only from Spark pods in the same namespace.
>
> **Isolation trade-off:** because the policy is per-namespace (not per-app), Spark pods of different applications in the same namespace can reach each other. This is the accepted trade-off of the blanket model (ODH-ADR-Operator-0016 permits a label-based blanket policy).

### Policy (one per namespace)

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: spark-operator-operands
  namespace: <workload-namespace>
  labels:
    sparkoperator.k8s.io/created-by-spark-operator: "true"
  # one non-controller ownerReference per SparkApplication/SparkConnect with
  # operands in the namespace; kube GC deletes the policy when the last is gone
  ownerReferences: [<SparkApplication / SparkConnect>, ...]
spec:
  podSelector:
    matchLabels:
      sparkoperator.k8s.io/launched-by-spark-operator: "true"
  policyTypes: [Ingress, Egress]
  ingress:
    # R4 — intra-Spark: any operand pod in the namespace, all ports
    - from:
        - podSelector:
            matchLabels: {sparkoperator.k8s.io/launched-by-spark-operator: "true"}
    # R1/R2 — Spark UI + driver metrics (4040) and Spark Connect gRPC (15002)
    #          from any namespace (empty namespaceSelector matches all)
    - from:
        - namespaceSelector: {}
      ports:
        - {protocol: TCP, port: 4040}
        - {protocol: TCP, port: 15002}
  egress:
    # R3 (API server) + R5 (DNS) + R6 (external) — allow-all
    - {}
```

### Lifecycle, cleanup & idempotency

`networkpolicy.Ensure` is invoked on every reconcile (SparkApplication: on the Running-state pass; SparkConnect: each reconcile) via `controllerutil.CreateOrUpdate`, which **GETs first and only writes when something changed**:

| Situation | Result |
|-----------|--------|
| First workload in the namespace | policy doesn't exist → one **Create** |
| A new workload reconciles | policy exists but this workload isn't an owner yet → `SetOwnerReference` appends it → one **Update** |
| Steady state (policy exists, workload already an owner, spec unchanged) | mutate yields an identical object → **no write** (one cached GET only) |

So the policy is **not** re-created on every pass. `SetOwnerReference` is idempotent (never duplicates an owner) and the spec is static, so re-running `Ensure` for an already-registered workload is a no-op.

**Cleanup is automatic — there is no explicit delete code.** Each SparkApplication / SparkConnect is recorded as a **non-controller owner reference** on the shared policy, and the Kubernetes garbage collector handles removal:

- When a workload is **deleted**, the GC removes its owner reference from the policy.
- When the **last** owner reference is removed (the last Spark workload in the namespace is gone), the GC **deletes the policy**.

This satisfies "clean up when the last application is shut down" (ODH-ADR-Operator-0016) without the operator doing last-app detection. Notes:
- Cleanup triggers on CR **deletion**, not completion — a `Completed` SparkApplication that still exists keeps its owner reference, so the policy persists (it may rerun).
- GC runs **independently of the operator**, so cleanup happens even if the operator is down when the last workload is deleted.

**Edge cases (all self-healing):**
- If the policy's last owner is removed at the same moment another workload is still starting (before it has added its own owner reference), GC may briefly delete the policy; the starting workload's next reconcile recreates it (closed on requeue).
- If the policy is manually deleted while workloads still exist, the next reconcile recreates it.
- GC is asynchronous, so there is a short delay between the last workload's deletion and the policy's removal.

### Packaging

The policy is **created and maintained by the operator at runtime** (not a static manifest), via `internal/controller/networkpolicy`, **unconditionally** (no feature flag). The controller RBAC grants `networkpolicies` (get/list/watch/create/update/patch/delete) in both `config/rbac/role.yaml` and the Helm chart. Contrast with the **operator-pod** policies below, which are static overlay manifests.

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
  - networkpolicy.yaml            # operand default-deny / intra-spark ingress
  - networkpolicy-metrics.yaml    # R1 monitoring + health
  - networkpolicy-egress.yaml     # R3/R5/R6 allow-all egress
  - networkpolicy-webhook.yaml    # R3 admission 9443
```

**Notes:**
- All manifests omit `metadata.namespace`; each overlay's `namespace:` directive stamps it (`redhat-ods-applications` / `opendatahub`).
- The `app.kubernetes.io/name: spark-operator` selector covers controller **and** webhook pods together — there is no per-component split.
- Egress is **allow-all** by design: ADR-0016 permits it where destinations can't be enumerated. A webhook-*only* deployment could tighten egress to DNS + API server, but the shared selector also covers the controller, which needs broad egress.
