#!/usr/bin/env bash

set -euxo pipefail

if [ ! -d "${TPCH_DATA_ROOT}/sf_${TPCH_SF}/" ]; then
  cd "${TPCH_REPO_ROOT}/dbgen"
  ./dbgen -vf -s "${TPCH_SF}"
  mkdir -p "${TPCH_DATA_ROOT}/sf_${TPCH_SF}/"
  mv ./*.tbl "${TPCH_DATA_ROOT}/sf_${TPCH_SF}/"
  cd "${ROOT_DIR}"
fi
