#!/usr/bin/env bash

set -euxo pipefail

if [ ! -d "${DSB_DATA_ROOT}/sf_${DSB_SF}/" ]; then
  cd "${DSB_REPO_ROOT}/code/tools"
  rm -rf ./tbl_data
  mkdir ./tbl_data
  ./dsdgen -dir tbl_data -terminate n -scale "${DSB_SF}"
  mkdir -p "${DSB_DATA_ROOT}/sf_${DSB_SF}/"
  mv ./tbl_data/* "${DSB_DATA_ROOT}/sf_${DSB_SF}/"
  rm -rf ./tbl_data
  cd "${ROOT_DIR}"
fi
