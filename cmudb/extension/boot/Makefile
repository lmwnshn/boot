EXTENSION = boot
MODULE_big = boot
DATA = boot--0.0.1.sql
OBJS = src/boot.o src/boot_executor.o src/boot_explain.o src/boot_instrument.o src/boot_Seqscan.o src/boot_util.o
SHLIB_LINK = ../boot_rs/target/release/libboot_rs.a
# If PG_CONFIG is not set, try the default build folder.
PG_CONFIG ?= ../../../build/postgres/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
