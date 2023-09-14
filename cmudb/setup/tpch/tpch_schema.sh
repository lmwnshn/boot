#!/usr/bin/env bash

set -euxo pipefail

if [ ! -d "${TPCH_SCHEMA_ROOT}" ]; then
  mkdir -p "${TPCH_SCHEMA_ROOT}"
  cp "${ROOT_DIR}/cmudb/setup/tpch/tpch_schema.sql" "${TPCH_SCHEMA_ROOT}/tpch_schema.sql"
  cp "${ROOT_DIR}/cmudb/setup/tpch/tpch_constraints.sql" "${TPCH_SCHEMA_ROOT}/tpch_constraints.sql"
fi
