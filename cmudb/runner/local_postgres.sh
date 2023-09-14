#!/bin/bash

set -euxo pipefail

BUILD_DIR="$(pwd)/build/postgres/"
BIN_DIR="$(pwd)/build/postgres/bin/"
POSTGRES_USER="noisepage_user"
POSTGRES_PASSWORD="noisepage_pass"
POSTGRES_DB="noisepage_db"
POSTGRES_PORT=15799

ROOT_DIR=$(pwd)

mkdir -p ${BUILD_DIR}
./cmudb/build/configure.sh release ${BUILD_DIR}
make clean
make install-world-bin -j
rm -rf ${BIN_DIR}/pgdata
${BIN_DIR}/initdb -D ${BIN_DIR}/pgdata
cp ./cmudb/env/kapi.pgtune.auto.conf ${BIN_DIR}/pgdata/postgresql.auto.conf

cd ./cmudb/extension/bytejack_rs/
cargo build --release
cbindgen . -o target/bytejack_rs.h --lang c
cd ${ROOT_DIR}

cd ./cmudb/extension/bytejack/
make clean
make install -j
cd ${ROOT_DIR}

RUST_BACKTRACE=1 ${BIN_DIR}/postgres -D ${BIN_DIR}/pgdata -p ${POSTGRES_PORT}
