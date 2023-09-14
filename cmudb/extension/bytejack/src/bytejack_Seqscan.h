#pragma once

// clang-format off
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/fmgr.h"
// clang-format on

#include "../../../../src/include/executor/nodeSeqscan.h"

extern SeqNext_hook_type bytejack_prev_SeqNext_hook;

extern TupleTableSlot *bytejack_SeqNext(SeqScanState *node);
