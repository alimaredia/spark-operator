# Approaches to Eliminating Build-Time Spark Dependency — Running Comparison

Living document comparing ways to reach the project goal: **ship only non-Spark code** (Go
operator + Helm/CRDs) from the company build, with Spark provided entirely at runtime by the
customer's Spark image. Primary motivation is supply-chain / CVE reduction.

Companion to `k8s-spark-submitter-summary.md` and `spark-submit-flow-summary.md`.

## Evaluation axes

Each approach is scored on:

1. **Goal A — Company build is Spark-free.** Nothing Spark/JVM/PySpark built or vendored by the
   company. (The core CVE-reduction goal.)
2. **Goal B — Spark stays authoritative & customer-owned.** Spark-specific behavior (pod-spec
   generation, driver command, feature steps) comes from the *customer's* Spark image/version,
   patched on the customer's schedule — not baked into company-shipped code.
3. **Maintenance burden.** Ongoing cost the company owns, especially tracking Spark behavior
   across Spark versions.
4. **Operational complexity.** Extra deployments, network hops, security surface (mTLS, RBAC)
   the customer must run.
5. **Correctness / version-compat risk.** Likelihood of silent breakage as Spark evolves.

Legend: ✅ strong / ⚠️ partial or caveated / ❌ weak.

---

## Option 1 — Reimplement all pod-creation logic in pure Go, inside the operator

**Sketch:** Port everything the submitter service + `spark-submit` do for K8s cluster mode
(arg handling, `KubernetesDriverBuilder` feature steps, ConfigMap build, driver pod + owned
resources creation) into Go, and run it directly in the operator. No separate service; the
operator makes the K8s API calls itself.

**Where pod-spec logic lives:** the operator (company-owned Go code).
**Who owns Spark-version correctness:** the company.

| Axis | Score | Notes |
|------|-------|-------|
| A — Spark-free build | ✅ | Fully achieved. No Spark, JVM, or PySpark anywhere in what the company ships. |
| B — Spark authoritative | ❌ | Broken for pod-spec generation. The operator's Go code now *defines* how driver pods are built; Spark no longer authoritative. |
| Maintenance burden | ⚠️ | Company owns a Go reimplementation of the *reproducible* feature steps, but the surface is **bounded, driver-only, and CI-observable** (see "Why the compat burden is bounded"). Not zero, but not the open-ended "Go fork of Spark" it first appears. |
| Operational complexity | ✅ | Lowest — no extra service, no network hop, no extra mTLS/RBAC surface. Operator already talks to the K8s API in Go. |
| Version-compat risk | ⚠️ | Drift is real but **detectable before ship**: a differential/oracle CI suite runs real Spark of each supported version and diffs its driver pod spec against the Go output. Silent runtime breakage becomes a red CI job. |

**Operator vs. separate service (for this option):** put it **in the operator**. The separate-
service architecture only earns its keep when the submitter must be co-located with Spark's jars
(the Java approach). A pure-Go rewrite has no Spark dependency, so a separate Go service would add
a deployment, a network hop, and security surface for **no isolation benefit**.

### Why the compat burden is bounded (rescoring rationale: ❌ → ⚠️)

The initial ❌/❌ scoring treated "reimplement Spark's pod-spec logic in Go" as an unbounded,
bug-for-bug fork of Spark. Two observations shrink it to a bounded, testable problem:

**1. The surface is driver-only and input-bounded.** The operator's *only* input is the
`SparkApplication` CRD, which it turns into `spark-submit` args + pod templates. Executors are
created later by the running driver from the customer's Spark image — **out of scope entirely** for
the Go reimplementation. So the Go code must match Spark on exactly one artifact: the **driver pod
spec (+ its owned resources)** for a bounded set of CRD-derived inputs. Match defaults with no user
input, then confirm CRD-supplied values propagate — that's the whole contract.

**2. Real Spark is a CI oracle (differential / golden-file testing).** The company doesn't *ship*
Spark, but it can *run* Spark in CI. For each supported Spark version, a CI job runs stock
`spark-submit ... --deploy-mode cluster` (or `KubernetesDriverBuilder.buildFromFeatures`) with
`?dryRun=All` / `--dry-run`, captures the driver pod spec it *would* create, and diffs it against
the Go implementation's output for the same inputs. Divergence is a **red build, not a customer
incident.** The oracle is self-updating: bump the Spark version in the matrix and the golden output
regenerates from Spark itself. Drift becomes **bounded, visible, and caught pre-release** instead of
silent and open-ended. This is what moves both axes off ❌.

**The residual burden that keeps it at ⚠️ (not ✅):** the oracle proves *when* Go and Spark diverge;
it doesn't *fix* the divergence — an engineer still has to chase each behavior change, and new Spark
features may add inputs the Go code doesn't handle. And crucially, a whole class of submit-time
behaviors **can't be reproduced in Go at all** — they run arbitrary JVM code, do network/credential
I/O, or read the submit host's local filesystem. Those must be **detected and rejected** (or
delegated to a real JVM), which is the deny-list below.

### The deny-list: submit-time behaviors a Go port cannot reproduce

Audited against `spark/` (master, `5.0.0-SNAPSHOT`) along the full driver path
(`SparkSubmit.prepareSubmitEnvironment` → `KubernetesClientApplication.run` → `Client.run` →
`KubernetesDriverBuilder.buildFromFeatures`). Classification: **(a)** runs arbitrary user JVM code ·
**(b)** external I/O / network / credential fetch · **(c)** local-filesystem read on the submit host ·
**(d)** JVM/fabric8-only resolution. Anything in this table must be **validate-and-rejected** by the
operator's admission webhook (or delegated to a real JVM submitter, i.e. fall back to Option 3 for
that job) — a Go port silently producing a *wrong* pod spec for these is the failure mode to prevent.

| Behavior | Class | Trigger config key(s) / condition | Source |
|---|---|---|---|
| Custom driver feature steps (arbitrary class loaded + run) | (a) | `spark.kubernetes.driver.pod.featureSteps` non-empty | `KubernetesDriverBuilder.scala:52-74` |
| Kerberos delegation-token minting (contacts HDFS/HMS/HBase/KMS) | (b) | Hadoop security on, no keytab & no `...tokenSecret.name` | `KerberosConfDriverFeatureStep.scala:90-112` |
| krb5.conf local read → ConfigMap | (c) | `spark.kubernetes.kerberos.krb5.path` | `KerberosConfDriverFeatureStep.scala:231-241` |
| keytab local read → Secret | (c) | `spark.kerberos.keytab` (submission-local path) | `KerberosConfDriverFeatureStep.scala:244-252` |
| `HADOOP_CONF_DIR` listed + files read → ConfigMap | (c) | env `HADOOP_CONF_DIR` set | `HadoopConfDriverFeatureStep.scala:38-54,115-131` |
| Driver k8s credential files read → Secret | (c) | `spark.kubernetes.authenticate.driver.{caCertFile,clientKeyFile,clientCertFile}` | `DriverKubernetesCredentialsFeatureStep.scala:173-181` |
| Local jars/files/archives/pyFiles upload to remote FS | (b) | any local `spark.{jars,files,archives,submit.pyFiles}` **with** `spark.kubernetes.file.upload.path` | `BasicDriverFeatureStep.scala:165-195`, `KubernetesUtils.scala:311-336` |
| PySpark primary `.py` upload | (b) | PythonMainAppResource + local file + `...file.upload.path` | `DriverCommandFeatureStep.scala:78-100` |
| Executor pod-template download + read → ConfigMap | (b)+(c) | `spark.kubernetes.executor.podTemplateFile` | `PodTemplateConfigMapStep.scala:77-94` |
| Driver pod-template download + **fabric8 parse/default + layered merge** | (b)+(d) | `spark.kubernetes.driver.podTemplateFile` (+ `...podTemplateContainerName`) | `KubernetesUtils.scala:97-141` |
| Maven/Ivy `--packages` resolution (downloads jars) | (b) | `spark.jars.packages` (+ `repositories`/`ivy`/`ivySettings`/`excludes`) | `SparkSubmit.scala:338-369` |
| Glob path expansion via Hadoop FS | (b) | glob chars in `spark.{jars,files,pyFiles,archives}` | `SparkSubmit.scala:399-402` |
| Proxy-user `doAs` / UGI | (a)/(b) | `--proxy-user` (+ `...allowCustomClasspathInClusterMode`) | `SparkSubmit.scala:169-201,860-862` |
| Main-class read from jar manifest | (c) | `mainClass` unset + jar primary resource | `SparkSubmit.scala:537-544` |
| Submitter-env reads (`getCurrentUserName`, `sys.env`) | (d) | always (Basic/DriverCommand-python/LocalDirs) | `BasicDriverFeatureStep.scala:124` |

**Reproducible-in-Go (the actual port target):** `BasicDriverFeatureStep.configurePod` (image, cpu/mem,
ports, labels, nodeSelector, pull secrets, security context — *minus* its upload path),
`DriverServiceFeatureStep`, `NetworkPolicyFeatureStep`, `MountSecretsFeatureStep`,
`EnvSecretsFeatureStep`, `MountVolumesFeatureStep`, `LocalDirsFeatureStep`, and
`DriverCommandFeatureStep`'s Java/R command building. This set is what the oracle suite pins.

**One subtlety the audit surfaced:** the driver **ConfigMap** built in `Client.run`
(`KubernetesClientApplication.scala:106-110`) is derived from the *resolved* system properties
accumulated by **every** step's `getAdditionalPodSystemProperties`. So the deny-list isn't only about
pod fields — a denied step that would have contributed a system property also changes the ConfigMap.
Rejecting the trigger keys (rather than partially honoring them) keeps that consistent.

**Summary:** Optimizes hard for Goal A at some expense of Goal B. Simplest to operate. The compat
burden is **not** the open-ended "Go fork of Spark" it first appears — it's a bounded, driver-only
port whose correctness is pinned by a real-Spark CI oracle, *provided* the non-reproducible submit-time
behaviors above are deny-listed (or delegated). That reframing is what moves maintenance and
version-compat from ❌ to ⚠️; they stay at ⚠️ (not ✅) because detection isn't the same as a fix, and
the deny-list narrows the feature set the operator can accept versus real `spark-submit`.

---

## Option 2 — Upstream the submitter into Spark (Track 1 + Track 2)  ★ chosen endgame

**Sketch (per `AGENTS.md`):** Move the submission server *into Apache Spark itself* so it ships in
`$SPARK_HOME/jars/*` and runs entirely from the customer's Spark image — nothing Spark-linked is
built by the company. Delivered in two tracks:

- **Track 1 (do first; small; no SPIP):** make in-JVM submission safe & stable so a long-lived
  server can call Spark's submission code many times. Three changes: (1) per-submission driver
  ConfigMap name derived from the app ID (fixes the once-per-JVM `configMapNameDriver` immutable-
  ConfigMap collision at `KubernetesClientUtils.scala:61,149`); (2) relax the JVM-global
  `synchronized` (`KubernetesClientUtils.scala:83,94`); (3) expose a **stable, non-exiting,
  non-blocking** submission entry (parsed args + `KubernetesClient` → driver pod name/uid; no
  watcher loop, no `System.exit`) so external callers don't need `private[spark]` injection.
  Valuable regardless of endgame: lets the current `k8s-spark-submitter` PoC shed its hacks;
  prerequisite for Track 2.
- **Track 2 (SPIP; larger; builds on Track 1):** add a new Spark main class
  `org.apache.spark.deploy.k8s.submit.KubernetesRestSubmissionServer` — a Jetty server (following
  the `HistoryServer` long-lived-server pattern) that calls the Track 1 primitive per request.
  Ships in Spark's jars, launched via `bin/spark-class` + an `sbin/start-...` wrapper. Needs a
  SPIP + dev@ consensus; counter the "third-party?" pushback with the supply-chain/CVE argument
  and the operator's existing alpha REST-submitter (demonstrated demand).

**Where pod-spec logic lives:** Spark's own code (upstream, in the customer image).
**Who owns Spark-version correctness:** Spark / the customer.

| Axis | Score | Notes |
|------|-------|-------|
| A — Spark-free build | ✅ | The server *is* Spark, shipped in the customer image. Company builds only the Go operator + Helm/CRD. No `provided`, no jar to compile. |
| B — Spark authoritative | ✅ | Native Spark feature steps in the customer image, versioned with Spark. |
| Maintenance burden | ✅ | Company owns only the Go REST client. The server is upstream, community-maintained; no package hack, no `Client` fork. |
| Operational complexity | ⚠️ | Still a long-lived service + mTLS/RBAC to run — but declarative once CR-driven (below). |
| Version-compat risk | ✅ | Public, stable Spark entry point instead of `private[spark]` injection. |

**Only option that wins both Goal A and Goal B.** The trade vs. Option 1: no bug-for-bug Spark
compat burden (Spark stays authoritative); the cost is upstream acceptance (SPIP) and a release-
timeline gap bridged by the current `k8s-spark-submitter` PoC (the interim stopgap + reference
implementation — it can't be the endgame itself because it must be compiled against Spark, and even
Maven `provided` scope violates the zero-Spark-in-the-build constraint).

### End outcome of Track 2: a `SparkRESTSubmitter` CRD in the operator

Once Track 2 lands (Spark ships `KubernetesRestSubmissionServer`), the natural operator-side shape
is a new CRD that makes the submission server **first-class and declarative** instead of a
hand-deployed external service wired by a static flag. This is the concrete "custom resource for a
Spark-native REST submission server" from the project goal — the end state of Track 2 landing
upstream.

**Precedent in the operator:** `SparkConnect` (`api/v1alpha1/sparkconnect_types.go`) already models
a long-lived Spark server as a CRD whose controller reconciles a Pod + Service
(`internal/controller/sparkconnect/reconciler.go:302` Pod, `:518` Service). `SparkRESTSubmitter`
follows the same pattern (operator is Kubebuilder v4 / controller-runtime).

**`SparkRESTSubmitterSpec`** (mirrors `SparkApplication`/`SparkConnect`):
- `Image` — the user's Spark image, same convention as `SparkApplication.Spec.Image`
  (`sparkapplication_types.go:56`) with per-role override via embedded `SparkPodSpec.Image` (`:459`)
- `SparkConf` / server config, `SparkVersion`
- pod-shape fields: resources, `serviceAccount`, nodeSelector, tolerations, `replicas`
- `TLS` — cert/key/CA secret refs (matches the REST client's existing mTLS support)
- service exposure (port, type)

**Controller reconciles:**
1. A **Deployment** running `bin/spark-class org.apache.spark.deploy.k8s.submit.KubernetesRestSubmissionServer`
   from `Spec.Image`. Prefer a Deployment over SparkConnect's single Pod — the submitter is
   *stateless*, so replicas + rolling updates + self-healing are a clean win.
2. A **Service** fronting it → the endpoint (published on `.status`).
3. **RBAC**: a ServiceAccount + Role/RoleBinding granting pod-create in target namespaces — the
   *server* creates driver pods, so it needs its own permissions, distinct from the operator's.
4. Optional TLS secret wiring.

**Integration models (the key design decision):** today the submitter URL is a single global CLI
flag (`--submitter-service-url`, `start.go:293`) and the operator assumes the service already exists
(`WaitForConnection`, `rest_submission.go:266`).
- **Model A (minimal):** the CR just provisions the Deployment+Service; admin still points the
  static flag at the resulting Service DNS. Keeps the one-global-submitter limitation.
- **Model B (recommended):** `SparkApplication` gets a `SubmitterRef` (or namespace default); the
  submission path resolves the Service URL from the CR status at submit time. **Unlocks per-app,
  per-Spark-version submitters** — route a job to a submitter built from a matching Spark image
  (the submitter's Spark version must be compatible with the job it submits). Turns "deploy the
  submitter yourself and pass me a URL" into "declare a `SparkRESTSubmitter` and I'll run it."

**Caveats:** net-new CRD + controller + RBAC provisioning (and, for Model B, a submission-path
change from static flag to CR-resolved endpoint); depends on Track 2 shipping the server main class
(until then, point `Image` at the current `k8s-spark-submitter` PoC image — so the CR can be
prototyped now and swapped to stock Spark later); operator stays pure Go throughout (it only reconciles Deployment/Service/RBAC
— no Spark, no JVM).

---

## Option 3 — Ephemeral `spark-submit` pod per submission, on the user's image  (= `AGENTS.md` Option B)

**Sketch:** Remove `spark-submit`/Spark/PySpark from the operator image entirely (operator becomes
pure Go). On each `SparkApplication` submission, the operator creates a **short-lived submitter pod**
from the user-provided Spark image that runs `spark-submit ... --deploy-mode cluster` exactly the way
the operator pod does today. That pod creates the driver pod, then exits. No in-process reuse, no
REST service.

**Where pod-spec logic lives:** Spark's own code (real `spark-submit` in the user image).
**Who owns Spark-version correctness:** Spark / the customer.

| Axis | Score | Notes |
|------|-------|-------|
| A — Spark-free build | ✅ | Operator image drops Spark/PySpark; it only creates a K8s Pod. Nothing Spark-linked built or vendored. |
| B — Spark authoritative | ✅ | Runs stock `spark-submit` from the customer image — the strongest form of "Spark authoritative": no reuse hack, no reimplementation, no upstream change. |
| Maintenance burden | ✅ | Lowest of all options. No package hack, no `Client` fork, no Go reimplementation, no SPIP. The operator just templates a pod. |
| Operational complexity | ⚠️ | No long-lived service / mTLS / new CRD, but spawns an extra ephemeral pod per submit and needs RBAC (a ServiceAccount with pod-create) so that pod can create the driver. |
| Version-compat risk | ✅ | Uses real `spark-submit` — no compatibility risk whatsoever. |

**Cost (the whole point): latency / no warm path.** Every submission pays a **JVM cold start + extra
pod scheduling** before the driver is even created, with no warm service to amortize it. This is the
"simple but slower" trade — functionally correct but the slowest submit path.

**Its edge: available now, zero upstream dependency.** Unlike Option 2, it needs no SPIP, no Spark
changes, and no new CRD — it works against any existing Spark image today. It wins both Goal A and
Goal B; the only things it sacrifices are submission latency and warmth.

**Relation to others:** this is `AGENTS.md` Option B. `AGENTS.md` also records a *rejected* variant —
a REST server that merely *execs* `spark-submit` — which is strictly worse (network hop + cold start)
than both this and Option 2.

---

## Option 4 — Hybrid: Go fast path in the operator, delegate the hard cases to real Spark

**Sketch:** Combine Option 1 and Option 3 and route between them **per submission**. The operator
carries the pure-Go pod-creation path (Option 1) for the **reproducible subset** of feature steps.
On each `SparkApplication`, it classifies the request against the **deny-list** (Option 1's audited
table): if the job trips *no* non-reproducible trigger, the operator builds the driver pod itself in
Go — zero extra infrastructure. If it trips *any* trigger (Kerberos, `--packages`, file uploads, pod
templates, custom feature steps, proxy-user, …), the operator **delegates** that job to real Spark —
an ephemeral `spark-submit` pod on the customer's image (Option 3), or a submitter service. The
deny-list stops being *validate-and-reject* and becomes *validate-and-route*.

**Where pod-spec logic lives:** **split by job.** Operator Go for the common/reproducible case;
Spark's own code (in the customer image) for the delegated hard case.
**Who owns Spark-version correctness:** the company for the Go fast path (pinned by the oracle CI
suite); Spark/the customer for everything delegated.

| Axis | Score | Notes |
|------|-------|-------|
| A — Spark-free build | ✅ | Still ships only the Go operator + Helm/CRD. The fallback runs the customer's Spark image (ephemeral `spark-submit` pod) — nothing Spark-linked built or vendored by the company. (True only if the fallback is the ephemeral pod or an upstreamed server — *not* the current PoC, which is compiled against Spark.) |
| B — Spark authoritative | ⚠️ | **Per-job.** Spark is authoritative for every delegated job; for the Go fast path the operator still defines the pod spec. Partial by construction — but the customer picks which side any given job lands on via the confs they set. |
| Maintenance burden | ⚠️ | Highest-surface of the ⚠️ options: you own Option 1's Go reimplementation **and** the classifier/router **and** the fallback path. But the deny-list means you never have to reproduce the hard behaviors, and the router is a thin, testable mapping over the audited trigger keys. |
| Operational complexity | ⚠️ | Zero extra infra for fast-path jobs (like Option 1); you pay Option 3's extra-pod + RBAC cost **only on delegated jobs**. Complexity is *conditional on usage*, not paid up front. |
| Version-compat risk | ⚠️ | **Lower than Option 1.** The deny-list is a safety net: anything the Go path doesn't (or can't correctly) reproduce is gated behind a trigger key and delegated to real Spark, so an unhandled behavior degrades to "delegated," not "silently wrong pod." The fast-path subset stays oracle-pinned. |

**Why this is the interesting one: it makes Goal A vs Goal B a per-job dial, not a global vote.**
Every other option picks one point on the A↔B axis for *all* jobs. Option 4 lets simple jobs take
the Spark-free, zero-infra, low-latency Go path while Kerberos/`--packages`/pod-template jobs
transparently get real Spark. Users lose no functionality (the deny-list stops rejecting and starts
routing), and the company only pays the fallback's operational cost on the fraction of jobs that
need it.

**The router is the deny-list.** No new analysis is required — the audited trigger-key table under
Option 1 *is* the routing predicate. That table is the single source of truth for both "what to
reject" (pure Option 1) and "what to delegate" (Option 4); the only difference is the action taken
when a key matches.

**Costs / caveats:**
- **Two code paths to keep honest.** The Go fast path and the delegated path must produce equivalent
  pods for jobs near the boundary, or behavior shifts as a user toggles a triggering conf. The oracle
  suite covers the fast path; the delegated path is real Spark (self-correct).
- **Latency is bimodal.** Fast-path jobs are quick; delegated jobs pay Option 3's cold start. Fine if
  the hard cases are the minority, surprising if users can't predict which path they're on — worth
  surfacing the chosen path on `.status`.
- **Conservative routing is mandatory.** The router must **fail safe**: any conf it doesn't recognize
  routes to Spark, never to the Go path. A Go path that silently mishandles an unrecognized key is the
  exact failure mode the hybrid exists to avoid.
- **Fallback choice matters for Goal A.** Use the ephemeral `spark-submit` pod (Option 3) as the
  fallback to stay strictly Spark-free-to-build. A submitter-service fallback re-introduces Option 2's
  service/upstream question for the delegated slice.

**Summary:** The pragmatic superset of Options 1 and 3 — Option 1's zero-infra speed where it's safe,
Option 3's real-Spark fidelity where it's needed, chosen automatically per job by the deny-list. It
trades a second code path and bimodal latency for never having to reject a feature or globally
sacrifice Goal B. Strongest near-term answer if fast-path jobs dominate but the hard cases must still
work.

### If the fast path dominates ("Option 1 with a rare safety valve") — current expectation

Working assumption: the overwhelming majority of real `SparkApplication`s stay in the reproducible
subset, and deny-list jobs (Kerberos, `--packages`, pod templates, custom steps) are rare. Under that
assumption:

- **Build order is incremental.** Ship **pure Option 1 first** with the deny-list as *validate-and-reject*
  — a complete milestone covering ~all jobs. Then add the ephemeral-pod fallback to flip the handful of
  `reject` verdicts to `route`. The valve is a strict addition, not a rewrite; value lands before it exists.
- **The valve is nearly free.** The fallback is just Option 3 ("`spark-submit` in a pod"), and the
  operator already templates pods. Small marginal cost over the Go work you're doing anyway — it buys
  "never tell a user no."
- **The dominant risk flips from latency to rot.** Bimodal latency stops mattering (rare cold starts are
  fine). A rarely-exercised path is the one that silently breaks by the time someone hits it — so keep a
  **standing CI job that runs ≥1 delegated job per supported Spark version** on every release, proving the
  valve green even when no customer has used it recently.
- **Architecture simplifies: no submitter service, no `SparkRESTSubmitter` CRD.** A rare valve should be
  the ephemeral `spark-submit` pod, not a long-lived service. For this product that drops the entire REST-
  submitter code path and Option 2's SPIP/CRD apparatus — the "real Spark" surface shrinks to a pod the
  operator spawns on the rare hard job.

---

## Options to come

- (placeholder) Other approaches the user wants to explore — will be added and scored as we go.
  (`AGENTS.md` Options B and C are now covered: B = Option 3, C ≈ Option 1; Option 4 is the
  Option 1 + Option 3 hybrid.)

---

## Running takeaways

- **Option 2 (upstream into Spark) is the chosen endgame** — wins both goals *and* is warm/low-latency,
  but requires upstream acceptance (SPIP) + a CRD/service. **Option 3 (ephemeral `spark-submit` pod)
  also wins both goals with the least effort and no upstream dependency — its only cost is
  per-submission latency**, making it the natural near-term / no-wait choice (or a permanent one if
  latency isn't critical). Option 1 (pure Go) is the fallback that sacrifices Goal B and takes on
  permanent Spark-compat burden. The current `k8s-spark-submitter` is the interim stopgap + reference
  impl (not scored — it can't be an endgame since it's compiled against Spark).
- **Option 4 (hybrid) is the one that refuses to pick a single point on the A↔B axis.** It runs
  Option 1's Go fast path for the reproducible subset and delegates deny-list jobs to real Spark
  (ephemeral `spark-submit` pod, Option 3), routing **per job** off the same audited trigger-key
  table. Users lose no functionality and the company pays the fallback's cost only on the jobs that
  need it. Strongest near-term answer when simple jobs dominate but Kerberos/`--packages`/pod-template
  jobs must still work. Cost: two code paths + bimodal latency; the router must fail safe (unknown
  conf ⇒ delegate, never Go).
- **The real axis separating the two both-goals-winners (Option 2 vs Option 3) is latency/warmth vs.
  upstream effort:** Option 3 = slow-but-now-and-simple; Option 2 = fast-but-needs-upstream.
- **A submitter service is only overhead once you've reimplemented pod-creation in Go (Option 1).**
  Its value — Spark stays authoritative (Goal B), full-fidelity execution of the deny-list behaviors,
  and native multi-Spark-version support — is exactly what Option 1 discards, which is why it *looks*
  like pure overhead from inside Option 1. The Go operator path is a functional **subset** of real
  `spark-submit` (it must reject the deny-list); the submitter is the full set. Option 4 is the way to
  keep the subset's simplicity without losing the full set's coverage.
- The single most decisive axis is **Goal A vs. Goal B**: any approach that reimplements pod-spec
  generation outside Spark (pure Go) maximizes A but sacrifices B. Approaches that keep Spark
  authoritative preserve B but must solve *where that Spark code runs* without dragging Spark into
  the company build — upstreaming into Spark (Option 2) is how.
- The hard constraint that rules out any company-built Spark artifact (including the current
  `k8s-spark-submitter` PoC as an endgame): **zero Spark in the company build, including Maven
  `provided` scope** (still resolved at build time, shows up in SBOMs).
- "Operator vs. separate service" is not an independent choice — it falls out of whether the
  pod-creation code depends on Spark's Java internals. Java dependency ⇒ separate service in the
  Spark image. No Spark dependency (pure Go) ⇒ fold into the operator.
- The **`SparkRESTSubmitter` CRD is the end outcome of Track 2 landing** (inside Option 2), not a
  separate approach — it makes the upstreamed server declarative/first-class in the operator.
  `SparkConnect` is the existing precedent (CRD → long-lived Pod + Service). Recommended Model B
  (per-app `SubmitterRef`) enables matching each job to a submitter of the right Spark version.
