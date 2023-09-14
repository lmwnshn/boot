#!/usr/bin/env bash

set -euxo pipefail

if [ ! -d "${DSB_SCHEMA_ROOT}" ]; then
  mkdir -p "${DSB_SCHEMA_ROOT}"
  cp "${DSB_REPO_ROOT}/scripts/create_tables.sql" "${DSB_SCHEMA_ROOT}/create_tables.sql"
  cp "${DSB_REPO_ROOT}/scripts/dsb_index_pg.sql" "${DSB_SCHEMA_ROOT}/dsb_index_pg.sql"
fi
