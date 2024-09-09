#!/usr/bin/env bash

set -euxo pipefail

if [ ! -d "${DSB_REPO_ROOT}" ]; then
  mkdir -p "${DSB_REPO_ROOT}/.."
  cd "${DSB_REPO_ROOT}/.."
  git clone git@github.com:lmwnshn/dsb.git --single-branch --branch fix_ubuntu --depth 1
  cd ./dsb/code/tools
  make
  cd "${ROOT_DIR}"
fi
