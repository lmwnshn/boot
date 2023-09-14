#include "bytejack_explain.h"

#include "../../../../src/include/lib/stringinfo.h"
#include "../../../../src/include/nodes/parsenodes.h"
#include "../../../../src/include/nodes/pg_list.h"
#include "../../../../src/include/parser/parse_node.h"
#include "../../bytejack_rs/target/bytejack_rs.h"
#include "bytejack.h"
#include "bytejack_util.h"

ExplainIntercept_hook_type bytejack_prev_ExplainIntercept_hook = NULL;

void bytejack_ExplainIntercept(JumbleState *jstate, const char *queryString, ParamListInfo params, ParseState *pstate,
                               ExplainState *es, List *rewritten) {
  const char *queryStringNormalized;
  int queryLen;
  CacheResult *cacheResult;

  if (bytejack_prev_ExplainIntercept_hook) {
    (*bytejack_prev_ExplainIntercept_hook)(jstate, queryString, params, pstate, es, rewritten);
  }

  if (!bytejack_enable) {
    ExplainBeginOutput(es);
    if (rewritten == NIL) {
      if (es->format == EXPLAIN_FORMAT_TEXT) {
        appendStringInfoString(es->str, "Query rewrites to nothing\n");
      }
    } else {
      ListCell *l;
      foreach (l, rewritten) {
        ExplainOneQuery(lfirst_node(Query, l), CURSOR_OPT_PARALLEL_OK, NULL, es, pstate->p_sourcetext, params,
                        pstate->p_queryEnv);
        if (lnext(rewritten, l) != NULL) {
          ExplainSeparatePlans(es);
        }
      }
    }
    ExplainEndOutput(es);
    Assert(es->indent == 0);
    return;
  }

  queryLen = strlen(queryString);
  queryStringNormalized = generate_normalized_query(jstate, queryString, 0, &queryLen);

  cacheResult = NULL;
  if (bytejack_intercept_explain_analyze && es->analyze) {
    cacheResult = cache_get_explain(redis_con, bytejack_intelligent_cache, queryStringNormalized);
  }

  if (cacheResult) {
    appendStringInfoString(es->str, (const char *)cache_result_bytes(cacheResult));
  } else {
    ExplainBeginOutput(es);
    if (rewritten == NIL) {
      if (es->format == EXPLAIN_FORMAT_TEXT) {
        appendStringInfoString(es->str, "Query rewrites to nothing\n");
      }
    } else {
      ListCell *l;
      foreach (l, rewritten) {
        ExplainOneQuery(lfirst_node(Query, l), CURSOR_OPT_PARALLEL_OK, NULL, es, pstate->p_sourcetext, params,
                        pstate->p_queryEnv);
        if (lnext(rewritten, l) != NULL) {
          ExplainSeparatePlans(es);
        }
      }
    }
    ExplainEndOutput(es);
    Assert(es->indent == 0);

    if (bytejack_intercept_explain_analyze && es->analyze) {
      cache_append_explain(redis_con, bytejack_intelligent_cache, queryStringNormalized, es->str->data);
    }
  }
}
