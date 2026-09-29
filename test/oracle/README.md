# Spark driver-spec oracle

Differential ("oracle") tests that pin the pure-Go
[`sparkdrivercreator.SparkDriverCreator`](../../pkg/sparkdrivercreator) against
**real Spark**. For each supported Spark version, stock `spark-submit`
(`--deploy-mode cluster`, `--master k8s://…`) is run and the driver **pod**,
**ConfigMap**, and **Service** it would create are captured as golden files. The
Go builder must reproduce them byte-for-byte (after normalization).

This is the strategy from `approaches-comparison.md` (Option 1, "Why the compat
burden is bounded"): the company doesn't *ship* Spark, but it *runs* Spark in CI
as an oracle, so drift becomes a red build instead of a customer incident.

## Why this works without a cluster (or a dry-run)

Spark's k8s submit path has no dry-run: `Client.run` just `POST`s the driver pod
and server-side-applies the owned resources over HTTP. So we point `spark-submit`
at [`recorder/`](recorder) — a tiny fake API server that answers fabric8's
discovery probes and **records every request body**. Those bodies are exactly
what Spark intends to create. No kube-apiserver, no defaulting noise: we compare
*client intent*, which is what the Go code must match.

Spark itself comes from `pip install pyspark==<version>` — the wheel bundles a
full, self-contained Spark (`bin/spark-submit`, the Hadoop client jars,
`spark-kubernetes_*.jar`, and the fabric8 client), so no tarball or external
Hadoop is needed, and each version is one pip pin.

## Layout

```
recorder/                 fake API server that records mutating requests
cases/<case>/
  application.yaml        source-of-truth SparkApplication (the Go builder's input)
  submit-args.txt         equivalent spark-submit args (fed to stock spark-submit)
golden/<version>/<case>/
  driver-pod.json         normalized captured objects — COMMITTED
  configmap.json
  service.json
  podspec-configmap.json  executor pod-template ConfigMap — only for pod-template cases
  persistentvolumeclaim.json  on-demand driver PVC — only for driver-volume cases
  raw/, vars.json         intermediate capture artifacts — gitignored
internal/oraclenorm/      shared normalization (golden + Go output use the same)
cmd/oraclegen/            raw captures -> normalized golden files
setup-spark.sh            pip install pyspark==<ver> into a per-version venv
regen.sh                  run spark-submit against the recorder, write golden
oracle_test.go            pure-Go test: no JVM; reads committed golden files
```

## Cases

Each `cases/<case>/` is one submission scenario, exercised across every version
in the matrix:

- **`sparkpi-minimal`** — a bare cluster-mode Scala submit; the floor of what
  `Build` must reproduce.
- **`sparkpi-overrides`** — user-set driver cores/memory/labels/env/etc., pinning
  the conditional structured-field translations.
- **`sparkpi-operator`** — models how the **Spark Operator** submits: it sets
  `spark.kubernetes.driver.pod.name` to a deterministic `<app>-driver` and injects
  the operator tracking labels (`sparkoperator.k8s.io/app-name`,
  `.../launched-by-spark-operator`, `.../mutated-by-spark-operator`,
  `.../submission-id`). This is the scenario the native (spark-submit-free)
  submitter will drive, so the pod name diverges from the random resource prefix
  (which the Service and ConfigMap still carry). The `submission-id` uses a fixed
  sentinel UUID so both sides are deterministic without extra normalization.
- **`sparkpi-podtemplate`** — the classic operator path always synthesizes a
  driver *and* executor pod-template file, so this case pins Spark's
  `PodTemplateConfigMapStep`: a **4th object**, the immutable executor
  pod-template ConfigMap (`<prefix>-driver-podspec-conf-map`, golden
  `podspec-configmap.json`), plus the driver `pod-template-volume` mount and the
  `spark.kubernetes.executor.podTemplateFile` conf rewritten to its in-pod mount
  path. The submit-time `driver.podTemplateFile` / `driver.podTemplateContainerName`
  confs are host-specific artifacts the native path drops, so normalization strips
  them from the golden. The builder consumes the same executor template object via
  `BuildOptions.ExecutorPodTemplate`; cases without a template produce no 4th
  object.
- **`sparkpi-pvc`** — a driver **on-demand PersistentVolumeClaim**, pinning Spark's
  `MountVolumesFeatureStep`. When a driver volume sets all of
  `options.claimName=OnDemand`, `options.storageClass`, and `options.sizeLimit`,
  spark-submit substitutes `OnDemand` → `<prefix>-driver-pvc-<i>`, mounts the volume
  on the driver (first in the volume list, before the local-dir and conf volumes),
  and creates a **5th object**: the PVC (golden `persistentvolumeclaim.json`), owned
  by the driver pod. The typed CRD volume source can only carry `claimName`+`readOnly`,
  so on-demand PVCs are expressible only via `spec.sparkConf` passthrough — which this
  case exercises. fabric8 serializes the driver-volume mount/source's zero-valued
  `readOnly`/`subPath`/`subPathExpr` explicitly (Go's typed structs omit them), so
  normalization strips those zero values to keep the two sides comparable.
- **`sparkpi-volumes`** — the non-PVC driver volume types (`emptyDir`, `hostPath`,
  `nfs`) that `MountVolumesFeatureStep` supports. Unlike an on-demand PVC, none of
  these create an additional object — each is purely a driver-pod mutation (a pod
  volume + a container mount). One volume of each type is exercised in a single case
  (names `cache`/`host`/`shared`). Spark derives its driver volumes from an
  **unordered Scala Set**, so the pod body's volume/mount order is a hash order the
  pure-Go builder cannot reproduce (it emits a deterministic name-sorted order);
  normalization sorts `spec.volumes` and each container's `volumeMounts` by name on
  both sides, so the comparison is over the *set* of volumes/mounts (order carries no
  meaning to Kubernetes). As with `sparkpi-pvc`, fabric8 emits the mounts'
  zero-valued `readOnly`/`subPath`/`subPathExpr` explicitly and normalization strips
  them. Effect is entirely in `driver-pod.json`.
- **`sparkpi-secrets`** — referenced secrets, pinning `MountSecretsFeatureStep` and
  `EnvSecretsFeatureStep`. Neither creates an API object (the Secret must pre-exist);
  both only mutate the driver pod. `spec.driver.secrets` → a pod volume
  (`<secretName>-volume`, source: the Secret) + a driver mount at the given path
  (`spark.kubernetes.driver.secrets.<name>=<path>`); `spec.driver.envSecretKeyRefs`
  → a driver env var sourced via `valueFrom.secretKeyRef`
  (`spark.kubernetes.driver.secretKeyRef.<env>=<name>:<key>`). The builder folds the
  typed CRD secret fields into the resolved conf (as the operator does with `--conf`),
  so the feature steps and the `spark.properties` passthrough share one source of
  truth. No new golden file — the effect is entirely in `driver-pod.json` (secret
  volume/mount ordered before the local-dir/conf ones; the secretKeyRef env var right
  after the base env) and `configmap.json` (the two secret confs).

- **`sparkpi-secret-creds`** — typed driver secret **credential env**, the extra the
  operator injects for well-known secret types on top of a normal secret mount
  (`driverSecretOption`). A `GCPServiceAccount` secret adds
  `GOOGLE_APPLICATION_CREDENTIALS=<path>/key.json`; a `HadoopDelegationToken` secret
  adds `HADOOP_TOKEN_FILE_LOCATION=<path>/hadoop.token`. Each is a plain
  `spark.kubernetes.driverEnv.*` conf, so `BasicDriverFeatureStep` surfaces it as a
  driver env var (after the base env, before `SPARK_DRIVER_BIND_ADDRESS`) and it also
  appears in `spark.properties`. The secrets are still mounted like any referenced
  secret — no API object. No new golden file; the effect is in `driver-pod.json` (the
  two credential env vars + the two secret volumes/mounts) and `configmap.json`.

## Running the tests

Pure Go, no JVM required — reads the committed golden files:

```sh
go test ./test/oracle/
```

- `TestGoldenWellFormed` guards the golden files (valid, fully normalized).
- `TestDriverSpecMatchesOracle` is the differential test; each captured object
  (Pod, ConfigMap, Service, and — for pod-template cases — the executor
  pod-template ConfigMap) is its own subtest that compares `Build`'s output to the
  golden, or **skips** if `Build` does not produce that object for the case (e.g.
  the pod-template ConfigMap on cases with no template). All pass today.

## Regenerating golden files (needs a JVM)

Refreshing the goldens for a version **already** in the matrix — e.g. after a
change to the normalization or the cases:

```sh
test/oracle/setup-spark.sh 4.0.4        # one-time per version (skip if venv exists)
SPARK_VERSION=4.0.4 test/oracle/regen.sh
RAW_ONLY=1 test/oracle/regen.sh         # capture raw bodies only (debugging)
```

`SPARK_VERSION` defaults to `4.0.4` if unset. This does **not** change which
versions the test runs — that comes from `sparkVersions` in `oracle_test.go`.

## Adding a Spark version

Same as regenerating above, plus one edit to register the version so the test
actually runs it (versions are listed explicitly in `oracle_test.go`; a golden
dir alone is ignored):

1. Do everything in "Regenerating golden files" for the new version, e.g.
   `test/oracle/setup-spark.sh 4.0.5 && SPARK_VERSION=4.0.5 test/oracle/regen.sh`.
2. Add `"4.0.5"` to `sparkVersions` in `oracle_test.go`.

The oracle is self-updating: the golden regenerates from Spark itself, so a new
version is a few commands plus one line, not a hand-written expectation.

## Normalization

Per-submission values Spark randomizes/timestamps are replaced with stable
placeholders by `internal/oraclenorm`, applied identically to the golden and the
Go output (see that package for the catalogue: app id, resource prefix,
ConfigMap id, local dir, submitTime, properties date, master URL, SPARK_USER,
and owner-reference uid — the last two are host/post-create values the pure-Go
builder cannot reproduce). The golden object files are byte-identical across
independent `spark-submit` runs.

Two structural normalizations also run on driver pods: fabric8's explicit
zero-valued volume-mount fields (`readOnly:false`/`subPath:""`/`subPathExpr:""`) and
PVC-source `readOnly:false` are stripped to match Go's `omitempty`; and
`spec.volumes` and each container's `volumeMounts` are sorted by name, because Spark
builds its driver volumes from an unordered Scala Set (a hash order the pure-Go
builder emits name-sorted instead). Sorting is applied identically to both sides, so
volume/mount comparison is over the set of entries.

The per-submission **resource prefix** (`<appName>-<16hex>`) is extracted from the
driver **Service** name (`<prefix>-driver-svc`), not the pod name: when a case
overrides `spark.kubernetes.driver.pod.name` (e.g. `sparkpi-operator`), the pod
name no longer contains the prefix, but the Service and ConfigMap always do.
