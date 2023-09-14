#!/usr/bin/env bash

export ROOT_DIR=$(pwd)
POSTGRES_RUN_CONFIGURE="false"
POSTGRES_RUN_CLEAN="false"
POSTGRES_RUN_CLEAR_DATA="false"
POSTGRES_BUILD_TYPE="release"

export HOSTNAME=$(hostname)

export ARTIFACT_ROOT="${ROOT_DIR}/artifact"

if [ "${HOSTNAME}" = "dev8" ]; then
  export POSTGRES_BUILD_DIR="${ROOT_DIR}/build/postgres/"
  export POSTGRES_BIN_DIR="${ROOT_DIR}/build/postgres/bin/"
  export POSTGRES_DATA_DIR="/mnt/nvme1n1/postgres/data/"
elif [ "${HOSTNAME}" = "dev9" ]; then
  export POSTGRES_BUILD_DIR="${ROOT_DIR}/build/postgres/"
  export POSTGRES_BIN_DIR="${ROOT_DIR}/build/postgres/bin/"
  export POSTGRES_DATA_DIR="/mnt/nvme0n1/postgres/data/"
else
  export POSTGRES_BUILD_DIR="${ROOT_DIR}/build/postgres/"
  export POSTGRES_BIN_DIR="${ROOT_DIR}/build/postgres/bin/"
  export POSTGRES_DATA_DIR="${ROOT_DIR}/data/"
fi

export POSTGRES_USER="noisepage_user"
export POSTGRES_PASS="noisepage_pass"
export POSTGRES_DB="noisepage_db"
export POSTGRES_HOST="localhost"
export POSTGRES_PORT=15799
export POSTGRES_PID=-1

if [ "${HOSTNAME}" = "dev8" ]; then
  export TPCH_REPO_ROOT="${ROOT_DIR}/build/tpch-kit"
  export TPCH_DATA_ROOT="/mnt/nvme1n1/kapi/tpch/tpch-data/"
  export TPCH_SCHEMA_ROOT="/mnt/nvme1n1/kapi/tpch/tpch-schema/"
  export TPCH_QUERY_ROOT="/mnt/nvme1n1/kapi/tpch/tpch-queries/"
  export TPCH_QUERY_START=15721
  export TPCH_QUERY_STOP=16720
elif [ "${HOSTNAME}" = "dev9" ]; then
  export DSB_REPO_ROOT="${ROOT_DIR}/build/dsb"
  export DSB_DATA_ROOT="/mnt/nvme0n1/kapi/dsb/dsb-data/"
  export DSB_SCHEMA_ROOT="/mnt/nvme0n1/kapi/dsb/dsb-schema/"
  export DSB_QUERY_ROOT="/mnt/nvme0n1/kapi/dsb/dsb-queries/"
  export DSB_QUERY_TRAIN_SEED=15721
  export DSB_QUERY_TRAIN_NUM=100
  export DSB_QUERY_TEST_SEED=15722
  export DSB_QUERY_TEST_NUM=100
else
  export TPCH_REPO_ROOT="${ROOT_DIR}/build/tpch-kit"
  export TPCH_DATA_ROOT="${ROOT_DIR}/build/tpch-data/"
  export TPCH_SCHEMA_ROOT="${ROOT_DIR}/build/tpch-schema/"
  export TPCH_QUERY_ROOT="${ROOT_DIR}/build/tpch-queries/"
  export TPCH_QUERY_START=15721
  export TPCH_QUERY_STOP=15730

  export DSB_REPO_ROOT="${ROOT_DIR}/build/dsb"
  export DSB_DATA_ROOT="${ROOT_DIR}/build/dsb-data/"
  export DSB_SCHEMA_ROOT="${ROOT_DIR}/build/dsb-schema/"
  export DSB_QUERY_ROOT="${ROOT_DIR}/build/dsb-queries/"
  export DSB_QUERY_TRAIN_SEED=15721
  export DSB_QUERY_TRAIN_NUM=20
  export DSB_QUERY_TEST_SEED=15722
  export DSB_QUERY_TEST_NUM=2
fi

main() {
  kill "$(lsof -t -i:15799)"
  set -euxo pipefail

  if [ "${HOSTNAME}" = "dev8" ]; then
    setup_tpch
    setup_postgres

    load_tpch_sf 1
    run_tpch_sf 1
    load_tpch_sf 10
    run_tpch_sf 10
    load_tpch_sf 100
    run_tpch_sf 100
    export OPENBLAS_NUM_THREADS=1
    MODEL_BENCHMARK=tpch MODEL_SF=1 python3 ./cmudb/runner/model.py
    MODEL_BENCHMARK=tpch MODEL_SF=10 python3 ./cmudb/runner/model.py
    MODEL_BENCHMARK=tpch MODEL_SF=100 python3 ./cmudb/runner/model.py

    tar cvzf tpch.tgz artifact

    rm -rf ./artifact/cache
    rm -rf ./artifact/model
    rm -rf ./artifact/plot
    for i in {15821..16720}; do rm -rf ./artifact/experiment/*/tpch/*/$i/; done

    export TPCH_QUERY_START=15721
    export TPCH_QUERY_STOP=15820
    load_tpch_sf 10
    TPCH_MU=1 run_tpch_sf 10
    export OPENBLAS_NUM_THREADS=1
    MODEL_BENCHMARK=tpch MODEL_SF=10 python3 ./cmudb/runner/model.py
    mv artifact artifact_muhyp
    export TPCH_QUERY_START=15721
    export TPCH_QUERY_STOP=16720

    tar cvzf tpch_muhyp.tgz artifact_muhyp

    rm -rf ./artifact/

  elif [ "${HOSTNAME}" = "dev9" ]; then
    setup_dsb
    setup_postgres

    load_dsb_sf 1
    run_dsb_sf 1
    load_dsb_sf 10
    run_dsb_sf 10

    export OPENBLAS_NUM_THREADS=1
    MODEL_BENCHMARK=dsb MODEL_SF=1 python3 ./cmudb/runner/model.py
    MODEL_BENCHMARK=dsb MODEL_SF=10 python3 ./cmudb/runner/model.py
    tar cvzf dsb.tgz artifact
  else
    setup_tpch
    setup_dsb
    setup_postgres

    load_tpch_sf 1
    run_tpch_sf 1
    export OPENBLAS_NUM_THREADS=1
    MODEL_BENCHMARK=tpch MODEL_SF=1 python3 ./cmudb/runner/model.py

    load_dsb_sf 1
    run_dsb_sf 1
    export OPENBLAS_NUM_THREADS=1
    MODEL_BENCHMARK=dsb MODEL_SF=1 python3 ./cmudb/runner/model.py
  fi

  kill ${POSTGRES_PID}
}

sql_create_database() {
  set +e
  "${POSTGRES_BIN_DIR}/psql" -c "create database ${POSTGRES_DB} with owner = '${POSTGRES_USER}'" postgres -p "${POSTGRES_PORT}"
  set -e
}

sql_database_exists() {
  local EXISTS=$("${POSTGRES_BIN_DIR}/psql" -d template1 -p "${POSTGRES_PORT}" --tuples-only --no-align -c "SELECT 1 FROM pg_database WHERE datname='${POSTGRES_DB}'")
  if [ "${EXISTS}" = "1" ]; then
    return 0
  else
    return 1
  fi
}

setup_postgres() {
  cd "${ROOT_DIR}"
  mkdir -p "${POSTGRES_BUILD_DIR}"
  if [ ! -f "${ROOT_DIR}/config.log" ] || [ "${POSTGRES_RUN_CONFIGURE}" = "true" ]; then
    set +e
    make clean
    set -e
    ./cmudb/build/configure.sh "${POSTGRES_BUILD_TYPE}" "${POSTGRES_BUILD_DIR}"
  fi
  if [ "${POSTGRES_RUN_CLEAN}" = "true" ]; then
    make clean
  fi
  make install-world-bin -j

  if [ "${POSTGRES_RUN_CLEAR_DATA}" = "true" ]; then
    rm -rf "${POSTGRES_DATA_DIR}/pgdata"
  fi

  local RAN_INITDB="false"
  if [ ! -d "${POSTGRES_DATA_DIR}/pgdata" ]; then
    "${POSTGRES_BIN_DIR}/initdb" -D "${POSTGRES_DATA_DIR}/pgdata"
    RAN_INITDB="true"
  fi
  if [ "${HOSTNAME}" = "dev8" ]; then
    cp ./cmudb/env/dev8.pgtune.auto.conf "${POSTGRES_DATA_DIR}/pgdata/postgresql.auto.conf"
  elif [ "${HOSTNAME}" = "dev9" ]; then
    cp ./cmudb/env/dev9.pgtune.auto.conf "${POSTGRES_DATA_DIR}/pgdata/postgresql.auto.conf"
  else
    cp ./cmudb/env/kapi.pgtune.auto.conf "${POSTGRES_DATA_DIR}/pgdata/postgresql.auto.conf"
  fi

  cd ./cmudb/extension/bytejack_rs/
  cargo build --release
  cbindgen . -o target/bytejack_rs.h --lang c
  cd "${ROOT_DIR}"

  cd ./cmudb/extension/bytejack/
  make clean
  make install -j
  cd "${ROOT_DIR}"

  export RUST_BACKTRACE=1
  "${POSTGRES_BIN_DIR}/postgres" -D "${POSTGRES_DATA_DIR}/pgdata" -p "${POSTGRES_PORT}" &
  POSTGRES_PID=$!

  until "${POSTGRES_BIN_DIR}/pg_isready" -p ${POSTGRES_PORT} &> /dev/null
  do
    sleep 1
  done

  if [ "${RAN_INITDB}" = "true" ]; then
    "${POSTGRES_BIN_DIR}/psql" -c "create user ${POSTGRES_USER} with login superuser password '${POSTGRES_PASS}'" postgres -p "${POSTGRES_PORT}"
    sql_create_database
  fi

  echo "Started Postgres, PID: ${POSTGRES_PID}"
}

setup_tpch() {
  ./cmudb/setup/tpch/tpch_setup.sh
  ./cmudb/setup/tpch/tpch_schema.sh
  ./cmudb/setup/tpch/tpch_queries.sh
}

load_tpch_sf() {
  local SF=$1
  local OLD_POSTGRES_DB=${POSTGRES_DB}

  POSTGRES_DB="tpch_sf_${SF}"
  if ! sql_database_exists; then
    TPCH_SF="${SF}" ./cmudb/setup/tpch/tpch_data.sh
    sql_create_database
    TPCH_SF="${SF}" python3 ./cmudb/runner/tpch_load.py
    POSTGRES_DB="${OLD_POSTGRES_DB}"
  fi
}

run_tpch_sf() {
  local SF=$1
  local OLD_POSTGRES_DB=${POSTGRES_DB}

  POSTGRES_DB="tpch_sf_${SF}"
  TPCH_SF="${SF}" python3 ./cmudb/runner/tpch_run.py
  POSTGRES_DB="${OLD_POSTGRES_DB}"
}

setup_dsb() {
  ./cmudb/setup/dsb/dsb_setup.sh
  ./cmudb/setup/dsb/dsb_schema.sh
  ./cmudb/setup/dsb/dsb_queries.sh
}

load_dsb_sf() {
  local SF=$1
  local OLD_POSTGRES_DB=${POSTGRES_DB}

  POSTGRES_DB="dsb_sf_${SF}"
  if ! sql_database_exists; then
    DSB_SF="${SF}" ./cmudb/setup/dsb/dsb_data.sh
    sql_create_database
    DSB_SF="${SF}" python3 ./cmudb/runner/dsb_load.py
    POSTGRES_DB="${OLD_POSTGRES_DB}"
  fi
}

run_dsb_sf() {
  local SF=$1
  local OLD_POSTGRES_DB=${POSTGRES_DB}

  POSTGRES_DB="dsb_sf_${SF}"
  DSB_SF="${SF}" python3 ./cmudb/runner/dsb_run.py
  POSTGRES_DB="${OLD_POSTGRES_DB}"

  if [ "${SF}" = "1" ]; then
    cd "${ROOT_DIR}"
    ls artifact/experiment/default/dsb/sf_1/default/"${DSB_QUERY_TRAIN_SEED}"/query*.timeout > cmudb/env/"${HOSTNAME}_dsb_sf1_timeout_${DSB_QUERY_TRAIN_SEED}.txt"
    grep -oE "'Execution Time.*" artifact/experiment/default/dsb/sf_1/default/"${DSB_QUERY_TRAIN_SEED}"/query*.res | cut -d'}' -f1 > cmudb/env/"${HOSTNAME}_dsb_sf1_runtime_${DSB_QUERY_TRAIN_SEED}.txt"
    ls artifact/experiment/default/dsb/sf_1/default/"${DSB_QUERY_TEST_SEED}"/query*.timeout > cmudb/env/"${HOSTNAME}_dsb_sf1_timeout_${DSB_QUERY_TEST_SEED}.txt"
    grep -oE "'Execution Time.*" artifact/experiment/default/dsb/sf_1/default/"${DSB_QUERY_TEST_SEED}"/query*.res | cut -d'}' -f1 > cmudb/env/"${HOSTNAME}_dsb_sf1_runtime_${DSB_QUERY_TEST_SEED}.txt"
  fi
}

main "$@"
