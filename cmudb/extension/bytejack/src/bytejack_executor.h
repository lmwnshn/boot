#pragma once

// clang-format off
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/fmgr.h"
// clang-format on

#include "../../../../src/include/access/sdir.h"
#include "../../../../src/include/executor/execdesc.h"
#include "../../../../src/include/executor/executor.h"
#include "../../bytejack_rs/target/bytejack_rs.h"

extern RuntimeState *runtime_state;
extern ExecutorStart_hook_type bytejack_prev_ExecutorStart_hook;
extern ExecutorRun_hook_type bytejack_prev_ExecutorRun_hook;
extern ExecutorFinish_hook_type bytejack_prev_ExecutorFinish_hook;
extern ExecutorEnd_hook_type bytejack_prev_ExecutorEnd_hook;

void bytejack_ExecutorStart(QueryDesc *queryDesc, int eflags);
void bytejack_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once);
void bytejack_ExecutorFinish(QueryDesc *queryDesc);
void bytejack_ExecutorEnd(QueryDesc *queryDesc);
