#pragma once

// clang-format off
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/fmgr.h"
// clang-format on

#include "../../../../src/include/executor/instrument.h"
#include "../../../../src/include/nodes/execnodes.h"

extern InstrAddTupleBatchTimes_hook_type bytejack_prev_InstrAddTupleBatchTimes_hook;

bool bytejack_InstrAddTupleBatchTimes(struct PlanState *node, double n_tuples, double accumulated_us);
