#!/usr/bin/env bash
# Upload rule files in ./rules/ to a Unity Catalog Volume.
#
# Usage:
#   bash scripts/upload_rules.sh                       # defaults: cep_demo / network
#   bash scripts/upload_rules.sh my_catalog my_schema  # override
#   PROFILE=field-eng-east bash scripts/upload_rules.sh
#
# Requires: databricks CLI v0.205+ (`databricks --version`)
set -euo pipefail

CATALOG="${1:-cep_demo}"
SCHEMA="${2:-network}"
PROFILE="${PROFILE:-DEFAULT}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RULES_DIR="${SCRIPT_DIR}/../rules"
VOLUME_BASE="dbfs:/Volumes/${CATALOG}/${SCHEMA}/rules"
VOLUME_BASE_APPS="dbfs:/Volumes/${CATALOG}/${SCHEMA}/rules_apps"

if [[ ! -d "${RULES_DIR}" ]]; then
  echo "rules/ directory not found at ${RULES_DIR}" >&2
  exit 1
fi

echo ">> Uploading $(ls "${RULES_DIR}"/*.json | wc -l | tr -d ' ') rule file(s) to ${VOLUME_BASE}"
echo ">> Profile: ${PROFILE}"

for f in "${RULES_DIR}"/*.json; do
  name="$(basename "$f")"
  echo "   - ${name}"
  databricks --profile "${PROFILE}" fs cp --overwrite "$f" "${VOLUME_BASE}/${name}"
done

echo ""
echo ">> Also seeding ${VOLUME_BASE_APPS} so the rule editor app has files to load"
for f in "${RULES_DIR}"/*.json; do
  name="$(basename "$f")"
  databricks --profile "${PROFILE}" fs cp --overwrite "$f" "${VOLUME_BASE_APPS}/${name}"
done

echo ""
echo "Done. Verify:"
echo "  databricks --profile ${PROFILE} fs ls ${VOLUME_BASE}"
