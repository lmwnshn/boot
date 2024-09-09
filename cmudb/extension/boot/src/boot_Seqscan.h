#pragma once

// clang-format off
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/fmgr.h"
// clang-format on

#include "../../../../src/include/executor/nodeSeqscan.h"

extern SeqNext_hook_type boot_prev_SeqNext_hook;

extern TupleTableSlot *boot_SeqNext(SeqScanState *node);
