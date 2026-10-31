# Spark REST Submitter — Project Guide

## Goal

Eliminate **all build-time dependency on Spark/PySpark** from a company's Kubeflow Spark
Operator product, so that the company's build pipelines ship **only non-Spark code** (Go
operator + Helm/CRDs), while Spark is provided **entirely at runtime by the user-supplied
Spark container image** — exactly the way driver and executor pods already get their Spark.

Concretely, the endgame is:

- **Operator**: pure Go (Kubeflow Spark Operator in REST-submitter mode). No Spark, no JVM,
  no PySpark in its image. It only makes HTTP calls. to a custom resource for a Spark-native REST submission server for Kubernetes cluster. 
- **Company build footprint**: Go operator + Helm chart / CRD only. Nothing Spark-linked is
  built or vendored.
- **Spark (and its CVEs)**: 100% the customer's image, patched on the customer's schedule —
  same trust model already used for driver/executor images.

The core motivation is **supply-chain / CVE reduction**: today the Kubeflow Spark Operator
image bundles Spark/PySpark (to shell out to `spark-submit`), which drags in a large CVE
surface. Moving submission out of the operator — and making the submitter part of Spark that
runs in the user's image — removes that surface from everything the company builds.

## Repositories in this workspace

| Directory | What it is | Role in this effort |
|-----------|-----------|---------------------|
| `spark/` | Apache Spark source (checkout is `master`, `5.0.0-SNAPSHOT`) | Where the upstream changes go (the two tracks below) |
| `spark-operator/` | Kubeflow Spark Operator | The Go operator; recently added an alpha REST-submitter option that POSTs `spark-submit` args to a submitter service |
| `k8s-spark-submitter/` | Existing standalone REST submitter service (Scala/Java 17, targets Spark 4.0.1) | Proof-of-concept / reference implementation of the submitter; the thing the operator's alpha REST-submitter talks to |

## Key background and constraints (established, do not relitigate)

1. **Zero Spark in the company's build is a hard constraint.** Even compiling against Spark
   with Maven `provided` scope is disallowed — `provided` deps are still resolved at build
   time and show up in SBOMs/scanners. So the company **cannot build** any Spark-linked
   artifact itself, including a "bytecode-only" jar.

2. **`k8s-spark-submitter` cannot be the final answer as-is**, because it is Spark-linked
   code that must be compiled against Spark. Options that were considered:
   - **B. Drop the in-process design; have the operator launch a `spark-submit` pod on the
     user's image** — pure-Go build, but ~status-quo latency (JVM cold start per submit) and
     no warm service. A REST server that merely execs `spark-submit` is strictly worse
     (network hop + cold start) and was rejected.
   - **C. Reimplement Spark's K8s submission backend in pure Go** — Spark-free build and
     fast, but owns bug-for-bug Spark compatibility forever.
   - **upstream the submitter into Spark** so it ships in the user's
     image (this document's Goal). Dominates A/B/C for the stated constraint.

3. **`k8s-spark-submitter` is a thin wrapper over Spark, not a reimplementation.** It reuses
   Spark's internal classes (`SparkSubmitArguments`, `SparkSubmit.prepareSubmitEnvironment`,
   `ClientArguments`, `KubernetesDriverConf`, `KubernetesDriverBuilder`,
   `KubernetesClientUtils`) and only diverges from Spark's own `Client` in one meaningful
   way: it generates a **per-submission ConfigMap name** to avoid a collision. It reaches
   Spark's package-private classes by declaring its own classes inside
   `package org.apache.spark.deploy.k8s.submit` — brittle across Spark versions.

## The upstream plan — two tracks

### Track 1 (do first; small, high odds, no SPIP needed): make in-JVM submission safe & stable

Today it is **unsafe to submit repeatedly in one long-lived JVM**:

- `KubernetesClientUtils.configMapNameDriver` is an `object val` computed **once per JVM**
  (`spark/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/submit/KubernetesClientUtils.scala:61`),
  read by `Client.run()`
  (`.../submit/KubernetesClientApplication.scala:106`), and the ConfigMap is built
  `.withImmutable(true)` (`KubernetesClientUtils.scala:149`). A second submission in the same
  JVM reuses the same immutable ConfigMap name → either a create/update failure or a driver
  silently mounting the *previous* job's config; shared owner references also let jobs GC each
  other's ConfigMap. This is why `k8s-spark-submitter` bypasses `Client` entirely.
- Secondary single-shot smells: `buildSparkConfDirFilesMap`/`...Java` are `synchronized` on
  the object (`KubernetesClientUtils.scala:83,94`); `Client.run()` has a blocking
  `WAIT_FOR_APP_COMPLETION` watcher loop (`KubernetesClientApplication.scala:185-205`); the
  normal `SparkSubmit.main` entry calls `System.exit`.
- Access problem: `Client`, `Client.run()`, `ClientArguments`, `KubernetesDriverConf` are all
  `private[spark]`. Note `KubernetesClientUtils` is already `@DeveloperApi @Stable @Since`
  and its doc says it is used by the "Spark K8s operator" — a useful wedge.

**Track 1 changes:**
1. Derive the driver ConfigMap name **per submission** from the (already-unique) app ID,
   deprecating `configMapNameDriver`. Pure correctness fix.
2. Relax the JVM-global `synchronized` so concurrent submissions don't serialize.
3. Expose a **stable, non-exiting, non-blocking** submission entry (takes parsed args + a
   `KubernetesClient`, returns created driver pod name/uid; no watcher loop, no `System.exit`)
   so external callers don't need `private[spark]` package injection.

Track 1 is valuable regardless of the endgame: it de-fragilizes `k8s-spark-submitter` (delete
its custom `K8sSparkClient`, stop injecting into `private[spark]`) and is the prerequisite for
any long-lived submission server.

### Track 2 (SPIP; larger; builds on track 1): a Kubernetes REST submission server in Spark

Add a new server main class (e.g. `org.apache.spark.deploy.k8s.submit.KubernetesRestSubmissionServer`)
that starts a Jetty `RestSubmissionServer` and, per request, calls the track-1 submission
primitive. Follow the existing long-lived-server pattern in Spark:

- Template: `HistoryServer` (`spark/core/src/main/scala/org/apache/spark/deploy/history/HistoryServer.scala:299,304`),
  launched by `sbin/start-history-server.sh` → `spark-daemon.sh start <CLASS>`.
- Existing REST framework to reuse/generalize: `spark/core/src/main/scala/org/apache/spark/deploy/rest/`
  (`RestSubmissionServer`, `StandaloneRestServer`, `RestSubmissionClient`) — currently
  **Standalone-only** and `spark://`-locked; it is a design precedent, not reusable as-is for k8s.
- Add an `sbin/start-...` convenience wrapper.
- Because the class ships in `$SPARK_HOME/jars/*`, it runs entirely from the user's Spark
  image via `bin/spark-class` — no Spark-linked code built by the company.

Track 2 needs a **SPIP + dev@ consensus**. Anticipated pushback (per Spark's `CONTRIBUTING.md`
"can this be a third-party project?"): counter with the supply-chain/CVE argument and the fact
that the Kubeflow operator's alpha REST-submitter already demonstrates community demand.

</content>
</invoke>
