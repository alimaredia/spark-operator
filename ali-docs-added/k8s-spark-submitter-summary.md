# How the k8s-spark-submitter Works

A code-grounded walkthrough of the standalone REST submitter service — a Scala 2.13 / Java 17
application that accepts a REST request and launches a Spark driver on Kubernetes. Line
references point at the source under `k8s-spark-submitter/`.

## Build & stack (context first)

- **Build tool:** Maven (`pom.xml`, `mvnw` wrapper). Not sbt.
- **Language/runtime:** Scala 2.13.16 on Java 17.
- **Key dependencies, all `provided` (supplied at runtime by the Spark 4.0.1 base image):**
  `spark-core` + `spark-kubernetes` 4.0.1, `io.fabric8:kubernetes-client` 7.1.0, Jackson 2.18.2.
  The only bundled compile dependency is `micrometer-registry-prometheus`.
- **Web server:** embedded **Jetty** (Spark's shaded copy) using raw Jakarta Servlets — no
  Spring/Play/Akka.

### The central design idea

The submitter does **not** shell out to `spark-submit`, and does **not** call
`SparkSubmit.main` / `KubernetesClientApplication`. Instead it **reimplements Spark's
in-cluster submission in-process** by placing two of its own classes inside Spark's package
namespaces so they can reach Spark's `private[spark]` internals:

- `src/main/scala/org/apache/spark/deploy/k8s/submit/K8sSparkClient.scala`
  (package `org.apache.spark.deploy.k8s.submit`) — reaches `KubernetesDriverBuilder`,
  `KubernetesDriverConf`, `KubernetesClientUtils`, feature steps.
- `src/main/scala/org/apache/spark/deploy/K8sSparkSubmitArgsParser.scala`
  (package `org.apache.spark.deploy`) — reaches `SparkSubmitArguments`,
  `SparkSubmit.prepareSubmitEnvironment`, `ClientArguments`.

These files are **the submitter's own code** (compiled into its jar), not Spark code — they
just wear Spark package names so the JVM grants them insider access. Spark itself arrives at
runtime via the `provided` jars in the base image. This pattern is what lets the Spark-
dependent submission logic live in a service inside the customer's Spark image, keeping the
Go operator Spark-free.

## The request flow, step by step

### 1. Server bootstrap — `SparkSubmitServer.scala:33` (`main`), `createServer()` at `:89`
Starts Jetty on two ports:
- **API port 8080:** `POST /api/v1/spark-submit`
- **Probe port 8081:** `GET /healthz`, `GET /metrics`

Constructs the singletons `SparkSubmitMetrics` and
`SparkSubmitter(new KubernetesClientProvider())` (`:92-93`). The API context runs a filter
chain (`:159-176`): optional `CertReloadFilter` (TLS cert hot-reload) → `SparkSubmitMetricsFilter`
→ `SparkSubmitServlet`. TLS/mTLS is optional; the probe port is always plain HTTP.

### 2. Metrics filter — `SparkSubmitMetricsFilter.scala:17`
For submit requests (POST + path ending `/spark-submit`), starts a Micrometer timer and
`recordRequestStart()`, runs the chain, then in `finally` records duration and response status.

### 3. Servlet handling — `SparkSubmitServlet.doPost` at `:24`
1. Validate `Content-Type` starts with `application/json` → else 415 (`:28-31`).
2. Read the `?dryRun=true` query param (`:33`).
3. Deserialize the body with Jackson (`FAIL_ON_UNKNOWN_PROPERTIES=false`) (`:34`).
4. Set MDC `submissionId` for structured logging (`:36-37`).
5. Delegate to `sparkSubmitter.submitJob(request, dryRun)` (`:42`).
6. Choose status: **200 OK** for dry-run/duplicate, **201 Created** for a new submit (`:47-48`).
7. Error mapping (`:49-60`): `SparkSubmitException` → status from its error code;
   `IOException` → 400; anything else → 500. `doGet` returns 405.

### 4. Request model — `SparkSubmitRequest.scala:11-28` (snake_case JSON)
- `submission_id` — defaults to `G-<UUID>`.
- `spark_submit_args` — the list of spark-submit CLI tokens.
- `driver_pod_template` / `executor_pod_template` — inline pod templates (JSON accepted).

### 5. Orchestration — `SparkSubmitter.submitJob` at `:39`
1. Parse args → `SparkConf` (`:40-41`).
2. **Tag the driver pod** with label `spark-submitter/submission-id = submissionId` (`:48`) —
   this powers duplicate detection.
3. Materialize any inline pod templates to temp files and point the confs at them (`:50`, `:96-108`).
4. Call `K8sSparkClient.submit(...)` (`:52`).
5. **409 handling** (`:55`, `:68-89`): list pods by the submission-id label; if one exists it's a
   **duplicate** (return existing pod, `duplicate_submission=true`); otherwise throw
   `DRIVER_POD_ALREADY_EXISTS`.
6. Map other exceptions to clean error codes (`:118-158`).
7. `finally`: delete the temp template dir (`:59`).

### 5a. Arg parsing — `K8sSparkSubmitArgsParser.parseArgs` at `:32` (in `org.apache.spark.deploy`)
1. `new SparkSubmitArguments(args)` — reuses Spark's own CLI parser (`--master`, `--class`,
   `--conf`, …).
2. `validateClusterMode` — **only `--deploy-mode cluster` is allowed** (`:49-55`).
3. `new SparkSubmit().prepareSubmitEnvironment(...)` — turns CLI options into a real `SparkConf`
   plus K8s child args.
4. `ClientArguments.fromCommandLineArgs(...)` — extracts main app resource, main class, driver
   args, proxy user. Any failure → `INVALID_SPARK_SUBMIT_ARGS`.

### 6. The actual K8s submission — `K8sSparkClient.submit` at `:52` (in `org.apache.spark.deploy.k8s.submit`)
Reimplements Spark's in-cluster `Client` (rationale at `:16-34`: Spark's real client uses a
ConfigMap-name singleton that collides across submissions in one long-lived JVM — i.e. a server).
1. Generate `spark.app.id = spark-<UUID>` if missing (`:56-57`).
2. Build `KubernetesDriverConf` (`:60-66`).
3. `KubernetesDriverBuilder().buildFromFeatures(...)` — runs Spark's **feature steps** to produce
   the driver spec: pod, services, secrets, volumes (`:69`).
4. `buildConfigMap` — creates a ConfigMap with a **unique-per-submission name** holding
   `spark.properties` (`:72`, `:92-97`).
5. `buildDriverPod` — mounts the ConfigMap and sets `SPARK_CONF_DIR` (`:75`, `:99-121`).
6. `createResources` via the **fabric8 client** (`:83`, `:176-219`): apply pre-resources
   (secrets/service accounts) → **create the driver Pod** → patch pre and post resources with the
   pod as **owner reference** for garbage collection.
7. On error: best-effort delete the pod + pre-resources (`safeCleanup`, `:206-223`), then rethrow
   raw for the service layer to translate.

**Objects created:** the driver **Pod**, a **ConfigMap**, the driver **Service**, plus any
**Secrets / ServiceAccounts** the feature steps emit. **Executors are NOT created here** — the
running driver spawns them itself. The fabric8 client is a pooled singleton
(`KubernetesClientProvider.scala:14`). `dryRun` threads down to fabric8's `.dryRun(true)` /
server-side `?dryRun=All`, validating RBAC/schema/webhooks/quota without persisting.

### 7. Response — fully synchronous, `SparkSubmitResponse.scala:11-21`
Returns once the pod and companion resources exist in Kubernetes. Body (snake_case, non-null):
`submission_id`, `app_name`, `message`, `submitted_at`, `spark_app_id`, `driver_pod_name`,
`driver_pod_uid`, `namespace`, `duplicate_submission`. Status **201** on new submit, **200** on
dry-run/duplicate. **There is no job-status / polling endpoint** — after submission the caller
watches the driver pod via Kubernetes.

### 8. State & lifecycle — essentially stateless
No DB, no job registry. Lifecycle is pushed to Kubernetes:
- **Owner references** — deleting the driver pod garbage-collects all companions.
- **Submission-id label** — enables idempotent duplicate detection on retries.
- **Best-effort rollback** of partially created resources on failure.
- **Temp-file lifecycle** — per-submission template dirs, deleted in `finally`.
- Only in-process state: the pooled fabric8 client and Micrometer counters.

## The generalizable boundary: `createResources`

Step 6.6 above — `createResources` (`K8sSparkClient.scala:176-219`) — is worth calling out
because it is **a clean, Spark-agnostic boundary**. Its signature takes only generic fabric8
types:

```scala
private[submit] def createResources(client: KubernetesClient, ns: String, pod: Pod,
                                    pre: List[HasMetadata], post: List[HasMetadata],
                                    isDryRun: Boolean = false): Option[String]
```

`HasMetadata` is fabric8's base interface for *any* Kubernetes object, and the body only does
generic operations (`serverSideApply()`, `resource(pod).create()`, `delete()`). It never touches
`SparkConf`, `KubernetesDriverConf`, or feature steps.

**The fabric8 apply logic is a clean, generalizable boundary.** If you wanted to reuse this
"apply a driver pod + owned resources" machinery for a non-Spark submitter, `createResources`
would transfer almost as-is — everything Spark-specific sits in *producing* the `Pod` /
`HasMetadata` lists (steps 6.2–6.5), not in applying them. Because this step is pure
"apply these Kubernetes objects, with the pod as GC owner, with dry-run and rollback," **it
could be made fully generic in another language such as Go** (using a Go Kubernetes client), with
no dependency on Spark at all.

Two caveats before calling it 100% Spark-free today:
1. `addOwnerReference` (`:153-159`) calls `org.apache.spark.deploy.k8s.KubernetesUtils.addOwnerReference`,
   a Spark util — trivial (just sets `ownerReferences` metadata) and easily reimplemented, but it
   means the apply path isn't strictly Spark-free right now.
2. It assumes a **single primary Pod owns everything else** — a fine pattern for driver-style
   workloads, but an assumption rather than full generality.

So the Spark dependency is concentrated entirely in building the resource objects (the feature-step
pipeline); the act of applying them is a generic primitive that a Go implementation could own.

## Key files

| File | Role |
|------|------|
| `.../SparkSubmitServer.scala` | Jetty bootstrap, routes, filters, TLS |
| `.../api/servlet/SparkSubmitServlet.scala` | HTTP handler for `POST /api/v1/spark-submit` |
| `.../api/ApiConstants.scala` | Paths, HTTP status, error codes, messages |
| `.../api/ServletSupport.scala` | Jackson JSON (de)serialization + error mapping |
| `.../model/SparkSubmitRequest.scala` / `SparkSubmitResponse.scala` | Request/response DTOs |
| `.../service/SparkSubmitter.scala` | Orchestration, duplicate handling, error mapping |
| `.../service/KubernetesClientProvider.scala` | Pooled fabric8 client |
| `org/apache/spark/deploy/K8sSparkSubmitArgsParser.scala` | Reuses Spark's arg parser |
| `org/apache/spark/deploy/k8s/submit/K8sSparkClient.scala` | In-process driver-pod/ConfigMap creation (the real "spark submit") |
| `.../metrics/SparkSubmitMetricsFilter.scala` | Request timing/count |
| `pom.xml` | Maven build, dependency versions |
