#!/usr/bin/env bash

set -euxo pipefail

function generate_queries() {
  num_streams=$1
  seed=$2

  output_dir="${DSB_QUERY_ROOT}"/default/"${seed}"
  need_distcomp="1"

  cd "${DSB_REPO_ROOT}/code/tools"
  set +x
  for template_path in ../../query_templates_pg/*/query*.tpl ; do
    folder=$(dirname "${template_path}")
    template=$(basename "${template_path}")

    if [ -d "${output_dir}" ]; then
      cd "${output_dir}"
      local count=$(find "${output_dir}" -type f -name "*.sql" | wc -l)
      local expected=$(( 52*${num_streams} ))
      if [ "${count}" = "${expected}" ]; then
        continue
      fi
    fi

    cd "${DSB_REPO_ROOT}/code/tools"
    mkdir -p "${output_dir}"

    if [ "${need_distcomp}" = "1" ]; then
      ./distcomp -i ./tpcds.dst -o ./tpcds.idx -rngseed "${seed}" -param_dist default # -param_dist normal -param_sigma 2 -param_center 0
      need_distcomp="0"
    fi

    rm -rf ./tmp
    mkdir -p ./tmp
    ./dsqgen -streams "${num_streams}" -output_dir ./tmp -dialect postgres -directory "${folder}" -template "${template}"

    cd ./tmp/
    for f in * ; do
      name=$(echo "${template}_${f}" | sed "s/.tpl_query_/-/")
      mv "${f}" "${name}"
    done
    cd ../

    mv ./tmp/* "${output_dir}"
    rm -rf ./tmp
  done
  cd "${ROOT_DIR}"

  set -x
}

generate_queries "${DSB_QUERY_TRAIN_NUM}" "${DSB_QUERY_TRAIN_SEED}"
generate_queries "${DSB_QUERY_TEST_NUM}" "${DSB_QUERY_TEST_SEED}"
