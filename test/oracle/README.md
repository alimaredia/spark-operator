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
  raw/, vars.json         intermediate capture artifacts — gitignored
internal/oraclenorm/      shared normalization (golden + Go output use the same)
cmd/oraclegen/            raw captures -> normalized golden files
setup-spark.sh            pip install pyspark==<ver> into a per-version venv
regen.sh                  run spark-submit against the recorder, write golden
oracle_test.go            pure-Go test: no JVM; reads committed golden files
```

## Running the tests

Pure Go, no JVM required — reads the committed golden files:

```sh
go test ./test/oracle/
```

- `TestGoldenWellFormed` guards the golden files (valid, fully normalized).
- `TestDriverSpecMatchesOracle` is the differential test; each captured object
  (Pod, ConfigMap, Service) is its own subtest that compares `Build`'s output to
  the golden, or **skips** if `Build` does not produce that object yet. All three
  (Pod, ConfigMap, Service) pass today.

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
