#!/usr/bin/env bash
set -euo pipefail

# Creates the required Kubernetes ConfigMaps for the Spark job using paths
# resolved relative to the repository root. This avoids hardcoding a user
# home directory in kubectl --from-file arguments.
#
# Usage:
#   ./scripts/create_spark_configmaps.sh [--namespace NAMESPACE]
#
# By default this creates two configmaps in the 'spark-jobs' namespace:
#  - on-prem-etl-script
#  - consolidate-data-script

NS="spark-jobs"
PROJECT_ROOT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -n|--namespace)
      NS="$2"; shift 2;;
    --root)
      PROJECT_ROOT="$2"; shift 2;;
    -h|--help)
      sed -n '1,200p' "$0"; exit 0;;
    *)
      echo "Unknown arg: $1"; exit 1;;
  esac
done

# If project root not supplied, derive it from this script's location (assumes script is in scripts/)
if [[ -z "$PROJECT_ROOT" ]]; then
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
fi

SRC_DIR="$PROJECT_ROOT/3-spark-app/src"

REQUIRED_FILES=(main.py consolidate_data.py generate_sample_data.py)

# Check kubectl
if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl not found in PATH. Please install kubectl and try again." >&2
  exit 2
fi

# Verify source files exist
for f in "${REQUIRED_FILES[@]}"; do
  if [[ ! -f "$SRC_DIR/$f" ]]; then
    echo "Expected source file not found: $SRC_DIR/$f" >&2
    exit 3
  fi
done

# Helper: create or update a configmap idempotently using apply
create_or_update_configmap() {
  local name="$1"
  shift
  local tmpfile
  tmpfile=$(mktemp)
  kubectl create configmap "$name" -n "$NS" "$@" --dry-run=client -o yaml > "$tmpfile"
  kubectl apply -f "$tmpfile"
  rm -f "$tmpfile"
}

# Build --from-file args
FROM_ARGS=()
for f in "${REQUIRED_FILES[@]}"; do
  FROM_ARGS+=("--from-file=$SRC_DIR/$f")
done

echo "Using project root: $PROJECT_ROOT"
echo "Creating configmaps in namespace: $NS"

create_or_update_configmap "on-prem-etl-script" "${FROM_ARGS[@]}"
create_or_update_configmap "consolidate-data-script" "${FROM_ARGS[@]}"

echo "ConfigMaps created/updated: on-prem-etl-script, consolidate-data-script (namespace: $NS)"

exit 0
