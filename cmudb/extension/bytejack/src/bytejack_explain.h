#pragma once

// clang-format off
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/fmgr.h"
// clang-format on

#include "../../../../src/include/commands/explain.h"

extern ExplainIntercept_hook_type bytejack_prev_ExplainIntercept_hook;

void bytejack_ExplainIntercept(JumbleState *jstate, const char *queryString, ParamListInfo params, ParseState *pstate,
                               ExplainState *es, List *rewritten);