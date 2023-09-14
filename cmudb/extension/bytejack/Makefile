EXTENSION = bytejack
MODULE_big = bytejack
DATA = bytejack--0.0.1.sql
OBJS = src/bytejack.o src/bytejack_executor.o src/bytejack_explain.o src/bytejack_instrument.o src/bytejack_Seqscan.o src/bytejack_util.o
SHLIB_LINK = ../bytejack_rs/target/release/libbytejack_rs.a
# If PG_CONFIG is not set, try the default build folder.
PG_CONFIG ?= ../../../build/postgres/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)