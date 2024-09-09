#!/usr/bin/env bash

set -euxo pipefail

if [ ! -d "${JOB_REPO_ROOT}" ]; then
  mkdir -p "${JOB_REPO_ROOT}/.."
  cd "${JOB_REPO_ROOT}/.."
  git clone git@github.com:lmwnshn/join-order-benchmark-copy.git --single-branch --branch main --depth 1 job

  cd ./job/raw_data/
  cat imdb.tgz.part-* > imdb.tgz
  tar xvzf imdb.tgz
  cd "${ROOT_DIR}"
fi
