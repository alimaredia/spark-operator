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

The per-submission **resource prefix** (`<appName>-<16hex>`) is extracted from the
driver **Service** name (`<prefix>-driver-svc`), not the pod name: when a case
overrides `spark.kubernetes.driver.pod.name` (e.g. `sparkpi-operator`), the pod
name no longer contains the prefix, but the Service and ConfigMap always do.
