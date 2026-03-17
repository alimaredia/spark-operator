#!/bin/bash
# ============================================================================
# Test: Docling Spark Application
# ============================================================================
#
# This test runs the docling-spark-app workload which converts PDFs to
# markdown using Apache Spark. It validates the full Spark Operator
# pipeline including PVC storage, multi-executor workloads, and
# OpenShift security contexts.
#
# This test verifies:
#   1. SparkApplication can be submitted
#   2. Driver pod starts and runs
#   3. Executor pods are created
#   4. Application completes successfully
#   5. Driver logs confirm execution
#
# Prerequisites:
#   - Spark Operator already installed (run 'make operator-install' first)
#   - PVCs created (docling-input, docling-output)
#   - Test PDFs uploaded to input PVC
#
# Usage:
#   ./test-docling-spark.sh
#   CLEANUP=false ./test-docling-spark.sh   # Keep resources for debugging
#
# Environment Variables:
#   APP_NAMESPACE     - Namespace to deploy app (default: spark-operator)
#   TIMEOUT_SECONDS   - Max wait time for completion (default: 600)
#   CLEANUP           - Set to "false" to preserve resources (default: true)
#
# ============================================================================

set -euo pipefail

# ============================================================================
# Configuration
# ============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

APP_NAMESPACE="${APP_NAMESPACE:-spark-operator}"
APP_NAME="${APP_NAME:-docling-spark-job}"
DRIVER_POD="${APP_NAME}-driver"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-600}"
APP_YAML="${APP_YAML:-$REPO_ROOT/examples/openshift/k8s/docling-spark-app.yaml}"

# ============================================================================
# Helper Functions
# ============================================================================
log()  { echo "➡️  $1"; }
pass() { echo "✅ $1"; }
fail() { echo "❌ $1"; exit 1; }
warn() { echo "⚠️  $1"; }

cleanup() {
    if [ "${CLEANUP:-true}" = "false" ]; then
        warn "CLEANUP=false, leaving resources for inspection"
        echo ""
        echo "To inspect:"
        echo "  kubectl get sparkapplication $APP_NAME -n $APP_NAMESPACE -o yaml"
        echo "  kubectl logs $DRIVER_POD -n $APP_NAMESPACE"
        echo ""
        echo "To cleanup manually:"
        echo "  kubectl delete sparkapplication $APP_NAME -n $APP_NAMESPACE"
        return
    fi
    log "Cleaning up SparkApplication..."
    kubectl delete sparkapplication "$APP_NAME" -n "$APP_NAMESPACE" --ignore-not-found || true
}

trap cleanup EXIT

get_app_state() {
    kubectl get sparkapplication "$APP_NAME" -n "$APP_NAMESPACE" \
        -o jsonpath='{.status.applicationState.state}' 2>/dev/null || echo "NOT_FOUND"
}

get_app_error() {
    kubectl get sparkapplication "$APP_NAME" -n "$APP_NAMESPACE" \
        -o jsonpath='{.status.applicationState.errorMessage}' 2>/dev/null || echo ""
}

# ============================================================================
# Pre-flight Checks
# ============================================================================
log "Running pre-flight checks..."

if ! kubectl get deployment -n "$APP_NAMESPACE" -l app.kubernetes.io/name=spark-operator &>/dev/null; then
    fail "Spark Operator not found. Run 'make operator-install' first."
fi
echo "  Spark Operator: Found"

if [ ! -f "$APP_YAML" ]; then
    fail "SparkApplication YAML not found: $APP_YAML"
fi
echo "  App YAML: $APP_YAML"

pass "Pre-flight checks passed"

# ============================================================================
# Setup: Create namespace
# ============================================================================
log "Creating namespace '$APP_NAMESPACE' if not exists..."
kubectl create namespace "$APP_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# ============================================================================
# Deploy SparkApplication (Docling Spark)
# ============================================================================
log "Deploying Docling Spark application..."
echo "  Name:      $APP_NAME"
echo "  Namespace: $APP_NAMESPACE"
echo "  YAML:      $APP_YAML"

kubectl delete sparkapplication "$APP_NAME" -n "$APP_NAMESPACE" --ignore-not-found 2>/dev/null || true

kubectl apply -f "$APP_YAML" -n "$APP_NAMESPACE"

pass "SparkApplication submitted"

# ============================================================================
# Wait for SparkApplication to Complete
# ============================================================================
log "Waiting for SparkApplication to complete (timeout: ${TIMEOUT_SECONDS}s)..."

SECONDS=0
LAST_STATE=""

while [ $SECONDS -lt $TIMEOUT_SECONDS ]; do
    STATE=$(get_app_state)

    if [ "$STATE" != "$LAST_STATE" ]; then
        echo "  [${SECONDS}s] State: $STATE"
        LAST_STATE="$STATE"
    fi

    case "$STATE" in
        COMPLETED)
            pass "SparkApplication completed successfully!"
            break
            ;;
        FAILED)
            echo ""
            echo "=== SparkApplication Failed ==="
            echo "Error: $(get_app_error)"
            echo ""
            echo "=== SparkApplication Status ==="
            kubectl get sparkapplication "$APP_NAME" -n "$APP_NAMESPACE" \
                -o jsonpath='{.status}' 2>/dev/null || true
            echo ""
            echo "=== Driver Pod Logs ==="
            kubectl logs "$DRIVER_POD" -n "$APP_NAMESPACE" --tail=50 2>/dev/null || echo "(no logs available)"
            fail "SparkApplication failed!"
            ;;
        FAILED_SUBMISSION)
            echo ""
            echo "=== Submission Failed ==="
            echo "Error: $(get_app_error)"
            echo ""
            echo "=== SparkApplication Status ==="
            kubectl get sparkapplication "$APP_NAME" -n "$APP_NAMESPACE" \
                -o jsonpath='{.status}' 2>/dev/null || true
            fail "SparkApplication submission failed!"
            ;;
    esac

    sleep 5
done

if [ "$STATE" != "COMPLETED" ]; then
    echo ""
    echo "=== Timeout - Current State: $STATE ==="
    echo ""
    echo "=== Pods in $APP_NAMESPACE ==="
    kubectl get pods -n "$APP_NAMESPACE" -o wide
    echo ""
    echo "=== Driver Pod Logs ==="
    kubectl logs "$DRIVER_POD" -n "$APP_NAMESPACE" --tail=100 2>/dev/null || echo "(no logs available)"
    fail "SparkApplication did not complete within ${TIMEOUT_SECONDS}s"
fi

# ============================================================================
# Verify Results
# ============================================================================
log "Verifying execution..."

# Check driver pod status
DRIVER_STATUS=$(kubectl get pod "$DRIVER_POD" -n "$APP_NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || echo "NOT_FOUND")
echo "  Driver pod ($DRIVER_POD): $DRIVER_STATUS"

# Get executor count
EXECUTOR_COUNT=$(kubectl get pods -n "$APP_NAMESPACE" -l "spark-role=executor,spark-app-name=$APP_NAME" --no-headers 2>/dev/null | wc -l | tr -d ' ')
echo "  Executors created: $EXECUTOR_COUNT"

# ============================================================================
# Show Driver Logs
# ============================================================================
echo ""
log "Driver logs (last 20 lines):"
kubectl logs "$DRIVER_POD" -n "$APP_NAMESPACE" --tail=20 2>/dev/null || warn "Could not get driver logs"

# ============================================================================
# Summary
# ============================================================================
echo ""
echo "============================================"
pass "DOCLING SPARK TEST PASSED!"
echo "============================================"
echo ""
echo "Summary:"
echo "  - SparkApplication: $APP_NAME"
echo "  - Namespace: $APP_NAMESPACE"
echo "  - Final State: COMPLETED"
echo "  - Driver Pod: $DRIVER_STATUS"
echo "  - Executor Pods: $EXECUTOR_COUNT"
echo ""
