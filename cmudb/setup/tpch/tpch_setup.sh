#!/usr/bin/env bash

set -euxo pipefail

if [ ! -d "${TPCH_REPO_ROOT}" ]; then
  mkdir -p "${TPCH_REPO_ROOT}/.."
  cd "${TPCH_REPO_ROOT}/.."
  git clone git@github.com:lmwnshn/tpch-kit.git --single-branch --branch master --depth 1
  cd ./tpch-kit/dbgen
  make MACHINE=LINUX DATABASE=POSTGRESQL
  cd "${ROOT_DIR}"
fi
