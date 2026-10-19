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

These run in the operator install namespace (`redhat-ods-applications` / `opendatahub`). Listed for completeness; their NetworkPolicies are HPSTRAT-278's deliverable.

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

## NetworkPolicy design — #3 SparkApplication driver pod

Drafted against the RHOAIENG-90747 contract dimensions **R1–R6**. (Exact R-numbering mirrors the dimensions named in the ticket; confirm wording against RHOAIENG-90747.)

Selector for every policy below: `spark-app-selector=<appID>` + `spark-role=driver`, in the **app's namespace**, owner-referenced to the driver pod (GC on driver termination).

> **What is `<appID>`?** It is the Spark application ID (`spark.app.id`, format `spark-<32 hex>`, e.g. `spark-b992db7da52c42298736dcbb3c9142be`). The **Spark distribution generates it** at driver startup and stamps it onto the driver and executor pods as the `spark-app-selector` label — the operator does not compute it. The operator reads it back off the pod label (`util.GetSparkApplicationID` → `pod.Labels["spark-app-selector"]`) and records it in `app.Status.SparkApplicationID` (`controller.go:1077`); the REST submitter also receives it in the submission response (`spark_app_id`).
>
> **Timing implication:** `<appID>` does not exist until the driver pod is created, so a policy keyed on `spark-app-selector` can only be built in the post-driver-pod reconcile pass (which is also when the driver pod is available as the owner reference). If a policy must exist *before* the driver pod, key it on the operator-controlled `sparkoperator.k8s.io/app-name=<app>` label (set at submission) instead — trading an exact Spark-4.2 match for a value known up front.

### Requirement → design mapping

| R | Dimension | Direction | Driver needs it? | How addressed |
|---|-----------|-----------|------------------|--------------------------|
| **R1** | Monitoring | Ingress | **No (default)** | Operator scrapes the controller, not the driver. No rule shipped; opt-in rule documented below for when Spark driver metrics are enabled. |
| **R2** | Gateway/router | Ingress | **Yes** | Allow `openshift-ingress` router → Spark UI **4040**. |
| **R3** | API server | Egress | **Yes (essential)** | Covered by the allow-all egress. |
| **R4** | Intra-component | Ingress + Egress | **Yes** | Ingress from same-app pods (all ports, by label). Egress covered by allow-all. |
| **R5** | DNS | Egress | **Yes** | Covered by allow-all egress |
| **R6** | External services | Egress | **Yes (most jobs)** | Covered by allow-all egress (Maven/PyPI/S3 can't be FQDN-matched in NetworkPolicy). |

### Policy set (recommended starting point: tight ingress, open egress)

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: <app>-driver
  namespace: <app-namespace>
  labels:
    spark-app-selector: <appID>
    sparkoperator.k8s.io/created-by-spark-operator: "true"
  ownerReferences: [<driver-pod>]
spec:
  podSelector:
    matchLabels:
      spark-app-selector: <appID>
      spark-role: driver
  policyTypes: [Ingress, Egress]
  ingress:
    # R4 — intra-component: executors + peers of the same app (ports random -> by label, no ports)
    - from:
        - podSelector:
            matchLabels: {spark-app-selector: <appID>}
    # R2 — gateway/router -> Spark UI
    - from:
        - namespaceSelector:
            matchLabels: {network.openshift.io/policy-group: ingress}
      ports:
        - {protocol: TCP, port: 4040}
  egress:
    # R3 (API server) + R5 (DNS) + R6 (external) — allow-all for now
    - {}
```

> Kubelet liveness/readiness probes need no rule — NetworkPolicy always permits traffic from the pod's local node.

---

## NetworkPolicy design — #5 SparkConnect server pod (acts as the driver)

Drafted against the RHOAIENG-90747 contract dimensions **R1–R6**. (Exact R-numbering mirrors the dimensions named in the ticket; confirm wording against RHOAIENG-90747.)

Selector for the policy below: `sparkoperator.k8s.io/connect-name=<conn>` + `spark-role=connect-server`, in the **SparkConnect's namespace**, owner-referenced to the SparkConnect CR (GC when the `SparkConnect` is deleted).

> **Two differences from the SparkApplication driver (#3):**
> 1. **Ports are pinned** — the operator sets `spark.driver.port=7078`, `spark.driver.blockManager.port=7079`, UI `4040`, and the Spark Connect gRPC port `15002` (`internal/controller/sparkconnect/`). So intra-component and client rules *can* be port-specific (unlike the SparkApplication driver, whose RPC ports are random).
> 2. **The grouping label is known up front, with no timing window.** The operator creates the server pod **directly**, so `connect-name=<conn>` (an operator-controlled label) exists at creation — unlike `spark-app-selector`, which only appears after Spark starts the driver. The policy can be created alongside the pod, and owned by the `SparkConnect` CR.

### Requirement → design mapping

| R | Dimension | Direction | Server needs it? | How addressed |
|---|-----------|-----------|------------------|--------------------------|
| **R1** | Monitoring | Ingress | **No (default)** | Operator scrapes the controller, not the Connect server. No rule shipped; add an opt-in `openshift-monitoring` rule if Connect metrics are exposed. |
| **R2** | Gateway/router + clients | Ingress | **Yes** | Router → Spark UI **4040**; Spark Connect **gRPC 15002** from workbench/notebook namespaces (the Connect clients). |
| **R3** | API server | Egress | **Yes (essential)** | Covered by the allow-all egress. Server manages its executors via the API. |
| **R4** | Intra-component | Ingress + Egress | **Yes** | Ingress from same-Connect-app pods on the pinned RPC ports **7078/7079** (executors ↔ server). Egress covered by allow-all. |
| **R5** | DNS | Egress | **Yes** | Covered by allow-all egress. |
| **R6** | External services | Egress | **Yes (most jobs)** | Covered by allow-all egress (can't FQDN-match in NetworkPolicy). |

### Policy (single combined object: tight ingress, open egress)

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: <conn>-server
  namespace: <conn-namespace>
  labels:
    sparkoperator.k8s.io/connect-name: <conn>
    sparkoperator.k8s.io/created-by-spark-operator: "true"
  ownerReferences: [<SparkConnect CR>]
spec:
  podSelector:
    matchLabels:
      sparkoperator.k8s.io/connect-name: <conn>
      spark-role: connect-server
  policyTypes: [Ingress, Egress]
  ingress:
    # R4 — intra-component: executors + peers of the same Connect app, on the pinned RPC ports
    - from:
        - podSelector:
            matchLabels: {sparkoperator.k8s.io/connect-name: <conn>}
      ports:
        - {protocol: TCP, port: 7078}   # driver RPC
        - {protocol: TCP, port: 7079}   # block manager
    # R2 — gateway/router -> Spark UI
    - from:
        - namespaceSelector:
            matchLabels: {network.openshift.io/policy-group: ingress}
      ports:
        - {protocol: TCP, port: 4040}
    # R2 — Spark Connect gRPC clients (workbenches/notebooks)
    - from:
        - namespaceSelector:
            matchLabels: {opendatahub.io/dashboard: "true"}
          podSelector:
            matchLabels: {opendatahub.io/workbenches: "true"}
      ports:
        - {protocol: TCP, port: 15002}
  egress:
    # R3 (API server) + R5 (DNS) + R6 (external) — allow-all for now
    - {}
```

> Because the ports are pinned, the R4 ingress here is restricted to **7078/7079** rather than the "all ports, by label" form used for the SparkApplication driver. If executors need additional server ports (e.g. the UI), broaden this rule or drop the `ports` to allow all intra-app traffic by label.
>
> The R2 gRPC rule mirrors the consumer path in the existing `spark-operator-allow-internal` policy (workbench namespaces reaching `15002`). Add a `network.openshift.io/policy-group: ingress` peer on `15002` too if the Connect endpoint is exposed externally via a Route.

---

## NetworkPolicy design — #4 SparkApplication executor pod

Drafted against the RHOAIENG-90747 contract dimensions **R1–R6**.

Selector: `spark-app-selector=<appID>` + `spark-role=executor`, in the **app's namespace**, owner-referenced to the driver pod (GC on driver termination). The ingress rule is an **exact port of Spark 4.2's `NetworkPolicyFeatureStep` (SPARK-55653)**: restrict executor ingress to pods of the same application.

### Requirement → design mapping

| R | Dimension | Direction | Executor needs it? | How addressed |
|---|-----------|-----------|--------------------|--------------------------|
| **R1** | Monitoring | Ingress | **No** | Executors aren't scraped by the operator. |
| **R2** | Gateway/router | Ingress | **No** | Executors have no UI or externally-reachable endpoint. |
| **R3** | API server | Egress | **No** | Executors never talk to the kube-apiserver (only the driver does). |
| **R4** | Intra-component | Ingress + Egress | **Yes** | Ingress from same-app pods (driver + peer executors), all ports — the Spark 4.2 match. Egress to the driver covered by allow-all. |
| **R5** | DNS | Egress | **Yes** | Covered by allow-all egress. |
| **R6** | External services | Egress | **Yes (heavily)** | Executors do the **bulk** of object-store read/write. Covered by allow-all egress. |

### Policy (single combined object)

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: <app>-executor          # Spark 4.2 names it "<resourceNamePrefix>-policy"
  namespace: <app-namespace>
  labels:
    spark-app-selector: <appID>
    sparkoperator.k8s.io/created-by-spark-operator: "true"
  ownerReferences: [<driver-pod>]
spec:
  podSelector:
    matchLabels:
      spark-app-selector: <appID>
      spark-role: executor
  policyTypes: [Ingress, Egress]
  ingress:
    # R4 — EXACT Spark 4.2 NetworkPolicyFeatureStep (SPARK-55653):
    #      ingress only from pods of the same application (driver + peer executors), all ports.
    - from:
        - podSelector:
            matchLabels: {spark-app-selector: <appID>}
  egress:
    # R5 (DNS) + R6 (external) + R4 egress to driver — allow-all for now
    - {}
```

> **Faithful-match option:** Spark 4.2's object is **ingress-only** (no `egress`, no `policyTypes: [Egress]`). If you want the executor policy to be byte-for-byte identical to what Spark 4.2 emits, keep the `ingress` block as a standalone object and ship the executor egress as a **separate** allow-all policy. Combining them (as above) is functionally fine but diverges from the upstream object.

---

## NetworkPolicy design — #6 SparkConnect executor pod

Drafted against the RHOAIENG-90747 contract dimensions **R1–R6**.

Selector: `sparkoperator.k8s.io/connect-name=<conn>` + `spark-role=executor`, in the **SparkConnect's namespace**, owner-referenced to the SparkConnect CR. Analogous to #4, but grouped by `connect-name` (known up front) instead of `spark-app-selector`.

### Requirement → design mapping

| R | Dimension | Direction | Executor needs it? | How addressed |
|---|-----------|-----------|--------------------|--------------------------|
| **R1** | Monitoring | Ingress | **No** | Not scraped. |
| **R2** | Gateway/router | Ingress | **No** | No UI or external endpoint. |
| **R3** | API server | Egress | **No** | Only the Connect server talks to the API. |
| **R4** | Intra-component | Ingress + Egress | **Yes** | Ingress from same-Connect-app pods (server + peer executors). Executor block-manager ports are **random**, so select by label (all ports). Egress to the server covered by allow-all. |
| **R5** | DNS | Egress | **Yes** | Covered by allow-all egress. |
| **R6** | External services | Egress | **Yes (heavily)** | Bulk object-store read/write. Covered by allow-all egress. |

### Policy (single combined object)

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: <conn>-executor
  namespace: <conn-namespace>
  labels:
    sparkoperator.k8s.io/connect-name: <conn>
    sparkoperator.k8s.io/created-by-spark-operator: "true"
  ownerReferences: [<SparkConnect CR>]
spec:
  podSelector:
    matchLabels:
      sparkoperator.k8s.io/connect-name: <conn>
      spark-role: executor
  policyTypes: [Ingress, Egress]
  ingress:
    # R4 — intra-component: Connect server + peer executors of the same app.
    #      Executor ports are random -> select by label, no ports.
    - from:
        - podSelector:
            matchLabels: {sparkoperator.k8s.io/connect-name: <conn>}
  egress:
    # R5 (DNS) + R6 (external) + R4 egress to the Connect server — allow-all for now
    - {}
```

> Unlike the Connect *server* (#5), the executor's block-manager/shuffle ports are **not pinned** (Spark defaults), so its R4 ingress must be by label with no `ports:` restriction — same reasoning as the SparkApplication executor.

---

## NetworkPolicy design — Webhook pod

> **Scope:** The webhook is an **operator pod**,  today it has **no NetworkPolicy at all** (the only shipped policy, `spark-operator-allow-internal`, selects `launched-by-spark-operator=true`, which the webhook does not carry).

Drafted against the RHOAIENG-90747 contract dimensions **R1–R6**.

Selector: `app.kubernetes.io/name=spark-operator` + `app.kubernetes.io/component=webhook`, in the **operator install namespace** (`redhat-ods-applications` / `opendatahub`). Unlike the operand policies (#3–#6), which must be created per-app by the operator at runtime, this is a **static manifest** — one per install, in the operator namespace, with no per-app keying and **no owner reference**. It is shipped **declaratively in the RHOAI and ODH Kustomize overlays** (see *Packaging* below).

> **Why the webhook differs from operand pods:**
> - It **is** scraped — the `spark-operator-podmonitor` selects `app.kubernetes.io/name=spark-operator`, which the webhook carries, scraping its metrics on **8080**. So R1 genuinely applies (it did not for operand pods).
> - The **kube-apiserver calls it inbound** on **9443** (via `spark-operator-webhook-svc` 443→9443) for admission — `failurePolicy: Fail`, so a dropped path blocks pod/SparkApplication admission.
> - It talks only to DNS and the API server, so its **egress can be genuinely tight** (DNS + API server) — no allow-all needed, unlike Spark workloads that reach Maven/S3.

### Requirement → design mapping

| R | Dimension | Direction | Webhook needs it? | How addressed |
|---|-----------|-----------|-------------------|--------------------------|
| **R1** | Monitoring | Ingress | **Yes** | Allow `openshift-monitoring` / `openshift-user-workload-monitoring` → metrics **8080**. |
| **R2** | Gateway/router | Ingress | **No** | Not exposed via a Route; reached only by the API server. |
| **R3** | API server | Ingress + Egress | **Yes (essential)** | **Ingress** on **9443** (admission from the kube-apiserver). **Egress** to the API server (manager client, leader-election Lease, reads cert Secret, patches webhook configs). |
| **R4** | Intra-component | — | **No** | Leader election is via the API-server Lease, not pod-to-pod; no inter-replica traffic. |
| **R5** | DNS | Egress | **Yes** | Resolve the API server and service names. |
| **R6** | External services | Egress | **No** | The webhook calls no external services. |

### Policy (single combined object)

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: spark-operator-webhook
  namespace: <operator-namespace>     # redhat-ods-applications / opendatahub
  labels:
    app.kubernetes.io/name: spark-operator
    app.kubernetes.io/component: webhook
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: spark-operator
      app.kubernetes.io/component: webhook
  policyTypes: [Ingress, Egress]
  ingress:
    # R3 — admission requests from the kube-apiserver on 9443.
    #      The apiserver source is host-network (control-plane nodes), which a
    #      podSelector/namespaceSelector can't match, so this allows the port
    #      from any source. Tighten with the OCP host-network policy group if required.
    - ports:
        - {protocol: TCP, port: 9443}
    # R1 — metrics scrape
    - from:
        - namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: openshift-monitoring}}
        - namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: openshift-user-workload-monitoring}}
      ports:
        - {protocol: TCP, port: 8080}
  egress:
    # R5 — DNS
    - to:
        - namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: openshift-dns}}
      ports:
        - {protocol: UDP, port: 53}
        - {protocol: TCP, port: 53}
    # R3 — API server (manager client + leader-election Lease)
    - ports:
        - {protocol: TCP, port: 6443}
        - {protocol: TCP, port: 443}
```

> **R3 ingress caveat (9443):** admission traffic originates from the kube-apiserver on the control-plane nodes (host network), which standard selectors can't target — hence the port-only `ingress` rule (allow from any source on 9443). To tighten, use the OpenShift host-network policy group (`network.openshift.io/policy-group: host-network`) as the `from` peer, where available.
>
> **R3 egress caveat (API server):** selecting the kube-apiserver as an egress peer is the same OCP wrinkle noted for the driver — the service VIP DNATs to node IPs on 6443, so a plain `namespaceSelector` won't match. The port-only egress rule above works; tighten with the host-network pattern if required.
>
> **Note:** the same policy shape applies to the **controller pod (#1)** — swap the selector to `app.kubernetes.io/component=controller` and drop the **9443** admission-ingress rule (the controller runs no webhook server). Both are HPSTRAT-278's deliverable.

### Packaging — static manifest in the RHOAI and ODH overlays

This policy is static and operator-namespace-scoped, so it ships as a plain manifest in the **RHOAI and ODH Kustomize overlays** — alongside the existing `spark-operator-allow-internal` policy — and is **not** reconciler-generated. This matches where operand-namespace policies already live and keeps both overlays byte-identical.

The manifest is checked in at:
- `config/overlays/rhoai/webhook-networkpolicy.yaml`
- `config/overlays/odh/webhook-networkpolicy.yaml`

and referenced from each overlay's `kustomization.yaml` `resources:` list:

```yaml
# config/overlays/{rhoai,odh}/kustomization.yaml
resources:
  - ../../default
  - ../../spark-rbac
  - ../../prometheus
  - networkpolicy.yaml
  - webhook-networkpolicy.yaml   # NetworkPolicy for the webhook pod (HPSTRAT-278)
```

**Notes:**
- The manifest omits `metadata.namespace`; each overlay's existing `namespace:` directive stamps it (`redhat-ods-applications` for RHOAI, `opendatahub` for ODH) — verified via `kustomize build`.
- The `podSelector` (`app.kubernetes.io/name=spark-operator` + `app.kubernetes.io/component=webhook`) matches the rendered webhook pod template labels; kustomize's label transformers leave it intact.
- The controller policy (#1) would follow the same pattern: a second manifest in both overlays with the selector swapped to `app.kubernetes.io/component=controller` and the `9443` admission-ingress rule dropped.
