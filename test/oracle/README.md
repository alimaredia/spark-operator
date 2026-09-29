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
- `TestDriverSpecMatchesOracle` is the differential test; it **skips** until
  `SparkDriverCreator.Build` is implemented, then compares its output to golden.

## Regenerating golden files (needs a JVM)

```sh
test/oracle/setup-spark.sh 4.0.4        # one-time per version
test/oracle/regen.sh                    # SPARK_VERSION defaults to 4.0.4
SPARK_VERSION=4.0.4 test/oracle/regen.sh
RAW_ONLY=1 test/oracle/regen.sh         # capture raw bodies only (debugging)
```

## Adding a Spark version

1. `test/oracle/setup-spark.sh 4.0.5`
2. `SPARK_VERSION=4.0.5 test/oracle/regen.sh`
3. Add `"4.0.5"` to `sparkVersions` in `oracle_test.go`.

The oracle is self-updating: the golden regenerates from Spark itself, so a new
version is a few commands, not a hand-written expectation.

## Normalization

Per-submission values Spark randomizes/timestamps are replaced with stable
placeholders by `internal/oraclenorm`, applied identically to the golden and the
Go output (see that package for the catalogue: app id, resource prefix,
ConfigMap id, local dir, submitTime, properties date, master URL). The golden
object files are byte-identical across independent `spark-submit` runs.
