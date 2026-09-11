#!/usr/bin/env bash
# Validate, deploy and run one target end to end.
#   ./scripts/deploy.sh dev            # deploy + run the medallion job
#   ./scripts/deploy.sh dev --setup    # also seed the dimension and generate data
set -euo pipefail

TARGET="${1:?usage: deploy.sh <dev|test|prod> [--setup]}"
shift || true

echo "==> validate  ($TARGET)"
databricks bundle validate -t "$TARGET"

echo "==> deploy    ($TARGET)"
databricks bundle deploy -t "$TARGET"

if [[ "${1:-}" == "--setup" ]]; then
  echo "==> setup_job ($TARGET)   migrations + dimension seed + ping generation"
  databricks bundle run setup_job -t "$TARGET"
fi

echo "==> medallion_job ($TARGET)   bronze -> silver -> gold"
databricks bundle run medallion_job -t "$TARGET"

echo "==> done ($TARGET)"
