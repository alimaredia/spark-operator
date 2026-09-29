#!/usr/bin/env bash
#
# regen.sh — regenerate the Spark oracle golden files for one Spark version.
#
# For each case under test/oracle/cases/, this:
#   1. starts the fake API server (recorder) on an ephemeral port,
#   2. runs stock spark-submit (--deploy-mode cluster, master k8s://<recorder>),
#   3. captures the driver pod / ConfigMap / Service request bodies Spark sends,
#   4. normalizes non-deterministic values and writes them under
#      test/oracle/golden/<version>/<case>/.
#
# Requirements: a JVM on PATH and a Spark install for the target version. The
# easiest way to get one (and to manage many versions) is pyspark:
#   test/oracle/setup-spark.sh 4.0.4     # pip install pyspark==4.0.4 into a venv
# regen.sh then auto-discovers that venv's bundled Spark. Alternatively set
# SPARK_HOME=... to point at any Spark binary distribution.
#
# Usage:
#   test/oracle/regen.sh                 # default version (4.0.4)
#   SPARK_VERSION=4.0.5 test/oracle/regen.sh
#   RAW_ONLY=1 test/oracle/regen.sh      # capture raw bodies only (skip normalize)
set -euo pipefail

SPARK_VERSION="${SPARK_VERSION:-4.0.4}"
SCALA_BINARY="${SCALA_BINARY:-2.13}"
IMAGE="${IMAGE:-spark:${SPARK_VERSION}}"
EXAMPLES_JAR="${EXAMPLES_JAR:-local:///opt/spark/examples/jars/spark-examples_${SCALA_BINARY}-${SPARK_VERSION}.jar}"
SUBMIT_TIMEOUT="${SUBMIT_TIMEOUT:-120}"

ORACLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_ROOT="$(cd "$ORACLE_DIR/../.." && pwd)"
CASES_DIR="$ORACLE_DIR/cases"
GOLDEN_DIR="$ORACLE_DIR/golden/$SPARK_VERSION"

# Resolve SPARK_HOME: an explicit env var wins; otherwise auto-discover the
# pyspark venv that setup-spark.sh created for this version.
if [[ -z "${SPARK_HOME:-}" ]]; then
  for candidate in "$HOME/.cache/spark-oracle/venv-${SPARK_VERSION}"/lib/python*/site-packages/pyspark; do
    if [[ -x "$candidate/bin/spark-submit" ]]; then SPARK_HOME="$candidate"; break; fi
  done
fi

if [[ -z "${SPARK_HOME:-}" || ! -x "$SPARK_HOME/bin/spark-submit" ]]; then
  echo "ERROR: no Spark $SPARK_VERSION found." >&2
  echo "       Run: test/oracle/setup-spark.sh $SPARK_VERSION" >&2
  echo "       (or set SPARK_HOME to a Spark $SPARK_VERSION install)" >&2
  exit 1
fi
echo ">> SPARK_HOME=$SPARK_HOME"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo ">> building recorder"
( cd "$MODULE_ROOT" && go build -o "$WORK/recorder" ./test/oracle/recorder )

# run_with_timeout <seconds> <cmd...>  — portable timeout (macOS has no timeout).
run_with_timeout() {
  local secs="$1"; shift
  "$@" &
  local pid=$!
  ( sleep "$secs"; kill -TERM "$pid" 2>/dev/null || true ) &
  local watcher=$!
  local rc=0
  wait "$pid" 2>/dev/null || rc=$?
  kill -TERM "$watcher" 2>/dev/null || true
  return $rc
}

# build_argv <submit-args.txt> <recorder host:port> <case dir>  — echoes argv, one
# per line. __CASE_DIR__ resolves to the case directory so pod-template cases can
# point spark-submit at their sibling *-pod-template.yaml files.
build_argv() {
  local file="$1" recorder="$2" case_dir="$3"
  case_dir="${case_dir%/}"
  while IFS= read -r line; do
    [[ -z "$line" || "$line" == \#* ]] && continue
    line="${line//__RECORDER__/$recorder}"
    line="${line//__IMAGE__/$IMAGE}"
    line="${line//__EXAMPLES_JAR__/$EXAMPLES_JAR}"
    line="${line//__CASE_DIR__/$case_dir}"
    printf '%s\n' "$line"
  done < "$file"
}

for case_dir in "$CASES_DIR"/*/; do
  case_name="$(basename "$case_dir")"
  args_file="$case_dir/submit-args.txt"
  [[ -f "$args_file" ]] || { echo "skip $case_name (no submit-args.txt)"; continue; }

  echo ">> case: $case_name"
  out="$WORK/$case_name"
  mkdir -p "$out"

  # 1. start the recorder and wait for it to publish its port.
  "$WORK/recorder" -addr 127.0.0.1:0 -out "$out" -port-file "$out/port" \
    > "$out/recorder.log" 2>&1 &
  rec_pid=$!
  for _ in $(seq 1 50); do [[ -s "$out/port" ]] && break; sleep 0.1; done
  if [[ ! -s "$out/port" ]]; then
    echo "ERROR: recorder did not start; log:" >&2; cat "$out/recorder.log" >&2
    kill "$rec_pid" 2>/dev/null || true; exit 1
  fi
  recorder_addr="$(cat "$out/port")"
  echo "   recorder at $recorder_addr"

  # 2. run spark-submit against the recorder.
  argv=()
  while IFS= read -r a; do argv+=("$a"); done < <(build_argv "$args_file" "$recorder_addr" "$case_dir")
  echo "   spark-submit ${argv[*]}"
  set +e
  SPARK_HOME="$SPARK_HOME" run_with_timeout "$SUBMIT_TIMEOUT" \
    "$SPARK_HOME/bin/spark-submit" "${argv[@]}" > "$out/spark-submit.log" 2>&1
  submit_rc=$?
  set -e
  kill "$rec_pid" 2>/dev/null || true
  wait "$rec_pid" 2>/dev/null || true

  captured="$(ls "$out/raw" 2>/dev/null | wc -l | tr -d ' ')"
  echo "   spark-submit exit=$submit_rc, captured $captured request bodies"
  if [[ "$captured" -eq 0 ]]; then
    echo "ERROR: nothing captured; spark-submit log tail:" >&2
    tail -30 "$out/spark-submit.log" >&2
    exit 1
  fi

  # 3. stage raw + normalize into golden.
  dest="$GOLDEN_DIR/$case_name"
  mkdir -p "$dest/raw"
  cp "$out/raw/"* "$dest/raw/"
  if [[ "${RAW_ONLY:-0}" == "1" ]]; then
    echo "   RAW_ONLY: wrote raw bodies to $dest/raw"
    continue
  fi
  ( cd "$MODULE_ROOT" && go run ./test/oracle/cmd/oraclegen \
      -raw "$dest/raw" -out "$dest" )
  echo "   wrote golden to $dest"
done

echo ">> done: $GOLDEN_DIR"
