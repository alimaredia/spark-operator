#!/usr/bin/env bash
#
# setup-spark.sh <version> — install a self-contained Spark of the given version
# via pyspark, so regen.sh can generate the oracle golden files for it.
#
# The pyspark PyPI package bundles a full Spark distribution (bin/spark-submit,
# the Hadoop client jars, spark-kubernetes_*.jar, and the fabric8 kubernetes
# client) — everything spark-submit needs for k8s cluster-mode submission, with
# no external Hadoop dependency. Each version gets its own venv, so multiple
# Spark versions coexist for the per-version oracle matrix.
#
# Usage:
#   test/oracle/setup-spark.sh 4.0.4
#   test/oracle/setup-spark.sh 4.0.5
set -euo pipefail

VERSION="${1:-}"
if [[ -z "$VERSION" ]]; then
  echo "usage: $0 <spark-version>   e.g. $0 4.0.4" >&2
  exit 1
fi

CACHE="${SPARK_ORACLE_CACHE:-$HOME/.cache/spark-oracle}"
VENV="$CACHE/venv-$VERSION"
PYTHON="${PYTHON:-python3}"

mkdir -p "$CACHE"
if [[ ! -x "$VENV/bin/pip3" ]]; then
  echo ">> creating venv $VENV"
  "$PYTHON" -m venv "$VENV"
fi

echo ">> installing pyspark==$VERSION (bundles Spark $VERSION)"
"$VENV/bin/pip3" install --no-input "pyspark==$VERSION"

SPARK_HOME=""
for candidate in "$VENV"/lib/python*/site-packages/pyspark; do
  [[ -x "$candidate/bin/spark-submit" ]] && SPARK_HOME="$candidate" && break
done
if [[ -z "$SPARK_HOME" ]]; then
  echo "ERROR: pyspark installed but bin/spark-submit not found under $VENV" >&2
  exit 1
fi

echo ">> ready. SPARK_HOME=$SPARK_HOME"
echo "   regen with: SPARK_VERSION=$VERSION test/oracle/regen.sh"
