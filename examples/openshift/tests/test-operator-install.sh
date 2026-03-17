#!/bin/bash
# This test verifies:
#   1. Spark Operator installs successfully from Kustomize manifests
#   2. No operator pods have fsGroup=185
#   3. Container runs with non-root UID
#
# Usage:
#   ./test-operator-install.sh           # Install, test, and cleanup
#   CLEANUP=false ./test-operator-install.sh  # Keep operator for subsequent tests
#
# Prerequisites:
#   - kubectl (or oc if OPENSHIFT=true) configured with cluster access
#   - Cluster Admin privileges
#   - Git repository cloned
#
# ============================================================================

set -exuo pipefail

# ============================================================================
# Configuration
# ============================================================================
# Use oc instead of kubectl when running on OpenShift
if [ "${OPENSHIFT:-false}" = "true" ]; then
    CLI="oc"
else
    CLI="kubectl"
fi

RELEASE_NAMESPACE="${RELEASE_NAMESPACE:-spark-operator}"
TIMEOUT="${TIMEOUT:-5m}"

# Get the repository root (relative to this script's location)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# ============================================================================
# Helper Functions
# ============================================================================
log()  { echo "➡️  $1"; }
pass() { echo "✅ $1"; }
fail() { echo "❌ $1"; exit 1; }
warn() { echo "⚠️  $1"; }

cleanup() {
    # By default, CLEANUP the operator after tests
    # Set CLEANUP=false to keep operator for subsequent tests
    if [ "${CLEANUP:-true}" = "true" ]; then
        log "Cleaning up..."
        $CLI delete -k "$REPO_ROOT/config/default/" 2>/dev/null || true
    else
        log "Keeping operator installed (CLEANUP=false)"
        log "To cleanup manually: $CLI delete -k $REPO_ROOT/config/default/"
    fi
}

# Cleanup on exit (if CLEANUP=true)
trap cleanup EXIT

# ============================================================================
# Setup: Install Spark Operator
# ============================================================================
log "Installing Spark Operator using Kustomize manifests..."
log "  Namespace: $RELEASE_NAMESPACE"
log "  Manifests: $REPO_ROOT/config/default/"

# Note: --server-side=true is required because the CRDs are large and exceed
# Kubernetes annotation size limits for client-side apply.
$CLI apply -k "$REPO_ROOT/config/default/" --server-side=true

pass "Spark Operator manifests applied successfully"

# ============================================================================
# Wait for pods to be ready
# ============================================================================
log "Waiting for operator pods to be ready..."

# Wait for deployments to be available
$CLI wait --for=condition=Available deployment \
    -l app.kubernetes.io/name=spark-operator \
    -n "$RELEASE_NAMESPACE" \
    --timeout="$TIMEOUT"

pass "All operator deployments are available"

# ============================================================================
# Test 1: Verify Installation
# ============================================================================
log "TEST 1: Verifying operator pods are running..."

# Check that controller and webhook pods exist and are running
PODS=$($CLI get pods -n "$RELEASE_NAMESPACE" -l app.kubernetes.io/name=spark-operator -o name 2>/dev/null)

if [ -z "$PODS" ]; then
    fail "No operator pods found with label app.kubernetes.io/name=spark-operator"
fi

# Wait for all pods to be ready
$CLI wait --for=condition=Ready pod \
    -l app.kubernetes.io/name=spark-operator \
    -n "$RELEASE_NAMESPACE" \
    --timeout=120s

# Verify expected pods are running
CONTROLLER_POD=$($CLI get pods -n "$RELEASE_NAMESPACE" \
    -l app.kubernetes.io/component=controller \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

WEBHOOK_POD=$($CLI get pods -n "$RELEASE_NAMESPACE" \
    -l app.kubernetes.io/component=webhook \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

if [ -z "$CONTROLLER_POD" ]; then
    fail "Controller pod not found"
fi

echo "  Controller: $CONTROLLER_POD"
echo "  Webhook:    ${WEBHOOK_POD:-not found}"

pass "TEST 1 PASSED: Operator pods are running"

# ============================================================================
# Test 2: Verify fsGroup is NOT 185
# ============================================================================
log "TEST 2: Checking fsGroup on operator pods..."

# Check controller pod
FSGROUP=$($CLI get pod "$CONTROLLER_POD" -n "$RELEASE_NAMESPACE" \
    -o jsonpath='{.spec.securityContext.fsGroup}' 2>/dev/null || echo "")
if [ "$FSGROUP" = "185" ]; then
    fail "Pod $CONTROLLER_POD has fsGroup=185 (not allowed for OpenShift)"
elif [ -z "$FSGROUP" ] || [ "$FSGROUP" = "null" ]; then
    echo "  $CONTROLLER_POD: fsGroup not set (OK for OpenShift)"
else
    echo "  $CONTROLLER_POD: fsGroup=$FSGROUP (OK)"
fi

# Check webhook pod (if exists)
if [ -n "$WEBHOOK_POD" ]; then
  FSGROUP=$($CLI get pod "$WEBHOOK_POD" -n "$RELEASE_NAMESPACE" \
      -o jsonpath='{.spec.securityContext.fsGroup}' 2>/dev/null || echo "")
  if [ "$FSGROUP" = "185" ]; then
      fail "Pod $WEBHOOK_POD has fsGroup=185 (not allowed for OpenShift)"
  elif [ -z "$FSGROUP" ] || [ "$FSGROUP" = "null" ]; then
      echo "  $WEBHOOK_POD: fsGroup not set (OK for OpenShift)"
  else
      echo "  $WEBHOOK_POD: fsGroup=$FSGROUP (OK)"
  fi
fi

pass "TEST 2 PASSED: No operator pods have fsGroup=185"

# ============================================================================
# Test 3: Verify container runs with non-root UID
# ============================================================================
log "TEST 3: Verifying container runs with non-root UID..."

# Get the UID from the controller pod
UID_OUTPUT=$($CLI exec -n "$RELEASE_NAMESPACE" "$CONTROLLER_POD" -- id 2>/dev/null || echo "")

if [ -z "$UID_OUTPUT" ]; then
    fail "Could not execute 'id' command in controller pod"
fi

echo "  Container identity: $UID_OUTPUT"

# Extract the UID number
CONTAINER_UID=$(echo "$UID_OUTPUT" | grep -o 'uid=[0-9]*' | cut -d= -f2)

if [ -z "$CONTAINER_UID" ]; then
    fail "Could not parse UID from output"
fi

if [ "$CONTAINER_UID" = "0" ]; then
    fail "Container is running as root (uid=0)"
fi

echo "  Container UID: $CONTAINER_UID (non-root, OK)"

pass "TEST 3 PASSED: Container runs with non-root UID"

# ============================================================================
# Summary
# ============================================================================
echo ""
echo "============================================"
pass "ALL OPERATOR INSTALL TESTS PASSED!"
echo "============================================"
echo ""
echo "This creates:"
echo "  - Operator namespace with controller and webhook deployments"
echo "  - 3 CRDs (SparkApplication, ScheduledSparkApplication, SparkConnect)"
echo "  - Comprehensive RBAC configuration"
echo "  - Spark job ServiceAccount for driver pods"
echo ""
echo "Operator will be cleaned up on exit (default behavior)."
echo ""
echo "To keep operator for subsequent tests, run with:"
echo "  CLEANUP=false ./test-operator-install.sh"
echo ""
