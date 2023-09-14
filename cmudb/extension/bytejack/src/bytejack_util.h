#pragma once

// clang-format off
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/fmgr.h"
// clang-format on

#include "../../../../src/include/utils/queryjumble.h"

char *generate_normalized_query(JumbleState *jstate, const char *query, int query_loc, int *query_len_p);
