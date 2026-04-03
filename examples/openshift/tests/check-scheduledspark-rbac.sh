#!/usr/bin/env bash
set -euo pipefail

# RBAC preflight for ScheduledSparkApplication
# Env (override as needed):
#   NAMESPACE (default: redhat-ods-applications)
#   CONTROLLER_SA (default: spark-operator-controller)

NAMESPACE="${NAMESPACE:-redhat-ods-applications}"
CONTROLLER_SA="${CONTROLLER_SA:-spark-operator-controller}"

fail() { echo "ERROR: $*" >&2; exit 1; }

check() {
  local verb="$1" res="$2" sub="${3:-}"
  local cmd=(oc auth can-i "$verb" "$res" --as="system:serviceaccount:${NAMESPACE}:${CONTROLLER_SA}" -n "$NAMESPACE")
  [[ -n "$sub" ]] && cmd+=(--subresource="$sub")
  local out; out="$("${cmd[@]}")"
  echo "can-i $verb $res${sub:+/$sub} => $out"
  [[ "$out" == "yes" ]] || fail "Missing RBAC: $verb $res${sub:+/$sub}"
}

echo "RBAC preflight for $NAMESPACE/$CONTROLLER_SA"
check update scheduledsparkapplications finalizers
check patch  scheduledsparkapplications finalizers
check create sparkapplications
check update sparkapplications status
echo "RBAC OK"

