#include "boot_explain.h"

#include <unistd.h>

#include "../../../../src/include/access/xact.h"
#include "../../../../src/include/commands/createas.h"
#include "../../../../src/include/lib/stringinfo.h"
#include "../../../../src/include/nodes/nodeFuncs.h"
#include "../../../../src/include/nodes/parsenodes.h"
#include "../../../../src/include/nodes/pg_list.h"
#include "../../../../src/include/parser/parse_node.h"
#include "../../../../src/include/storage/bufmgr.h"
#include "../../../../src/include/tcop/tcopprot.h"
#include "../../../../src/include/utils/snapmgr.h"
#include "../../boot_rs/target/boot_rs.h"
#include "boot.h"
#include "boot_util.h"

// From explain.c.
static double elapsed_time(instr_time *starttime);
static void show_buffer_usage(ExplainState *es, const BufferUsage *usage, bool planning);
static void ExplainIndentText(ExplainState *es);

void boot_ExplainDefault(JumbleState *jstate, const char *queryString, ParamListInfo params, ParseState *pstate,
                         ExplainState *es, List *rewritten);
void boot_ExplainTemplate(JumbleState *jstate, const char *queryString, ParamListInfo params, ParseState *pstate,
                          ExplainState *es, List *rewritten);
void boot_ExplainOnePlan(PlannedStmt *plannedstmt, IntoClause *into, ExplainState *es, const char *queryString,
                         ParamListInfo params, QueryEnvironment *queryEnv, const instr_time *planduration,
                         const BufferUsage *bufusage, const char *queryTemplate);
void boot_ExplainOneQuery(Query *query, int cursorOptions, IntoClause *into, ExplainState *es, const char *queryString,
                          ParamListInfo params, QueryEnvironment *queryEnv, const char *queryTemplate);
void boot_ExplainPlan(JumbleState *jstate, const char *queryString, ParamListInfo params, ParseState *pstate,
                      ExplainState *es, List *rewritten);

ExplainIntercept_hook_type boot_prev_ExplainIntercept_hook = NULL;
MacroState *macro_state = NULL;

static bool boot_init_macro_state(PlanState *planstate) {
  int plan_type = planstate->plan->type;
  int plan_node_id = planstate->plan->plan_node_id;
  double optimizer_startup_cost = planstate->plan->startup_cost;
  double optimizer_total_cost = planstate->plan->total_cost;
  double optimizer_plan_rows = planstate->plan->plan_rows;
  double optimizer_plan_width = planstate->plan->plan_width;
  macro_init_operator(macro_state, plan_node_id, -1, plan_type, optimizer_startup_cost, optimizer_total_cost,
                      optimizer_plan_rows, optimizer_plan_width);
  return planstate_tree_walker(planstate, boot_init_macro_state, NULL);
}

void boot_ExplainDefault(JumbleState *jstate, const char *queryString, ParamListInfo params, ParseState *pstate,
                         ExplainState *es, List *rewritten) {
  ListCell *l;
  ExplainBeginOutput(es);
  if (rewritten == NIL) {
    if (es->format == EXPLAIN_FORMAT_TEXT) {
      appendStringInfoString(es->str, "Query rewrites to nothing\n");
    }
  } else {
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
}

void boot_ExplainTemplate(JumbleState *jstate, const char *queryString, ParamListInfo params, ParseState *pstate,
                          ExplainState *es, List *rewritten) {
  const char *queryStringNormalized;
  int queryLen;
  CacheResult *cacheResult;
  ListCell *l;

  // Cache key computation.
  queryLen = strlen(queryString);
  queryStringNormalized = generate_normalized_query(jstate, queryString, 0, &queryLen);

  // Cache key lookup.
  cacheResult = NULL;
  if (boot_intercept_explain_analyze && es->analyze) {
    cacheResult = cache_get_explain(redis_con, boot_intelligent_cache, queryStringNormalized, 0);
  }

  // Append to es. Actual returning is handled after hook.
  if (cacheResult) {
    appendStringInfoString(es->str, (const char *)cache_result_bytes(cacheResult));
  } else {
    // Cache result computation.
    ExplainBeginOutput(es);
    if (rewritten == NIL) {
      if (es->format == EXPLAIN_FORMAT_TEXT) {
        appendStringInfoString(es->str, "Query rewrites to nothing\n");
      }
    } else {
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
    // Cache result save.
    if (boot_intercept_explain_analyze && es->analyze) {
      cache_append_explain(redis_con, boot_intelligent_cache, queryStringNormalized, es->str->data);
    }
  }
  cache_result_free(cacheResult);
}

void boot_ExplainOnePlan(PlannedStmt *plannedstmt, IntoClause *into, ExplainState *es, const char *queryString,
                         ParamListInfo params, QueryEnvironment *queryEnv, const instr_time *planduration,
                         const BufferUsage *bufusage, const char *queryTemplate) {
  DestReceiver *dest;
  QueryDesc *queryDesc;
  instr_time starttime;
  double totaltime = 0;
  int eflags;
  int instrument_option = 0;
  CacheResult *cacheResult = NULL;
  const char *cacheKey = NULL;
  ScanDirection dir;
  double plantime;

  Assert(plannedstmt->commandType != CMD_UTILITY);
  if (es->analyze && es->timing) {
    instrument_option |= INSTRUMENT_TIMER;
  } else if (es->analyze) {
    instrument_option |= INSTRUMENT_ROWS;
  }
  if (es->buffers) {
    instrument_option |= INSTRUMENT_BUFFERS;
  }
  if (es->wal) {
    instrument_option |= INSTRUMENT_WAL;
  }
  INSTR_TIME_SET_CURRENT(starttime);
  PushCopiedSnapshot(GetActiveSnapshot());
  UpdateActiveSnapshotCommandId();
  if (into) {
    dest = CreateIntoRelDestReceiver(into);
  } else {
    dest = None_Receiver;
  }
  queryDesc = CreateQueryDesc(plannedstmt, queryString, GetActiveSnapshot(), InvalidSnapshot, dest, params, queryEnv,
                              instrument_option);
  if (es->analyze) {
    eflags = 0;
  } else {
    eflags = EXEC_FLAG_EXPLAIN_ONLY;
  }
  if (into) {
    eflags |= GetIntoRelEFlags(into);
  }
  ExecutorStart(queryDesc, eflags);
  totaltime += elapsed_time(&starttime);

  if (boot_intercept_explain_analyze) {
    macro_state = macro_state_new();
    boot_init_macro_state(queryDesc->planstate);

    // Cache key strategy.
    cacheKey = NULL;
    if (boot_macro_mode == 1) {
      cacheKey = (const char *)macro_operator_hash(queryTemplate, macro_state);
    }

    // Test for match.
    cacheResult = NULL;
    if (es->analyze) {
      //      elog(LOG, "TEST key %s", cacheKey);
      plantime = INSTR_TIME_GET_DOUBLE(*planduration);
      cacheResult = cache_get_explain(redis_con, boot_intelligent_cache, cacheKey, 1000.0 * plantime);
    }

    // If cache entry exists, use it.
    if (cacheResult) {
      //      elog(LOG, "LOAD key %s data %s", cacheKey, (const char *)cache_result_bytes(cacheResult));
      appendStringInfoString(es->str, (const char *)cache_result_bytes(cacheResult));
      ExecutorEnd(queryDesc);
      FreeQueryDesc(queryDesc);
      PopActiveSnapshot();
      macro_state_free(macro_state);
      cache_result_free(cacheResult);
      if (es->analyze) {
        CommandCounterIncrement();
      }
      return;
    }
    //    elog(LOG, "RUN key %s", cacheKey);
  }

  if (es->analyze) {
    INSTR_TIME_SET_CURRENT(starttime);
    if (into && into->skipData) {
      dir = NoMovementScanDirection;
    } else {
      dir = ForwardScanDirection;
    }
    ExecutorRun(queryDesc, dir, 0L, true);
    ExecutorFinish(queryDesc);
    totaltime += elapsed_time(&starttime);
  }
  ExplainBeginOutput(es);
  ExplainOpenGroup("Query", NULL, true, es);
  ExplainPrintPlan(es, queryDesc);
  if (es->verbose && plannedstmt->queryId != UINT64CONST(0) && compute_query_id != COMPUTE_QUERY_ID_REGRESS) {
    ExplainPropertyInteger("Query Identifier", NULL, (int64)plannedstmt->queryId, es);
  }
  if (bufusage) {
    ExplainOpenGroup("Planning", "Planning", true, es);
    show_buffer_usage(es, bufusage, true);
    ExplainCloseGroup("Planning", "Planning", true, es);
  }
  if (es->summary && planduration) {
    double plantime = INSTR_TIME_GET_DOUBLE(*planduration);
    ExplainPropertyFloat("Planning Time", "ms", 1000.0 * plantime, 3, es);
  }
  if (es->analyze) {
    ExplainPrintTriggers(es, queryDesc);
  }
  if (es->costs) {
    ExplainPrintJITSummary(es, queryDesc);
  }
  INSTR_TIME_SET_CURRENT(starttime);
  ExecutorEnd(queryDesc);
  FreeQueryDesc(queryDesc);
  PopActiveSnapshot();
  if (es->analyze) {
    CommandCounterIncrement();
  }
  totaltime += elapsed_time(&starttime);
  if (es->summary && es->analyze) {
    ExplainPropertyFloat("Execution Time", "ms", 1000.0 * totaltime, 3, es);
  }
  ExplainCloseGroup("Query", NULL, true, es);
  ExplainEndOutput(es);

  if (boot_intercept_explain_analyze && es->analyze) {
    //    elog(LOG, "SAVE key %s data %s", cacheKey, es->str->data);
    cache_append_explain(redis_con, boot_intelligent_cache, cacheKey, es->str->data);
  }
  macro_state_free(macro_state);
  cache_result_free(cacheResult);
}

void boot_ExplainOneQuery(Query *query, int cursorOptions, IntoClause *into, ExplainState *es, const char *queryString,
                          ParamListInfo params, QueryEnvironment *queryEnv, const char *queryTemplate) {
  PlannedStmt *plan;
  instr_time planstart, planduration;
  BufferUsage bufusage_start, bufusage;

  if (query->commandType == CMD_UTILITY) {
    ExplainBeginOutput(es);
    ExplainOneUtility(query->utilityStmt, into, es, queryString, params, queryEnv);
    ExplainEndOutput(es);
    return;
  }

  if (es->buffers) {
    bufusage_start = pgBufferUsage;
  }
  INSTR_TIME_SET_CURRENT(planstart);

  plan = pg_plan_query(query, queryString, cursorOptions, params);

  INSTR_TIME_SET_CURRENT(planduration);
  INSTR_TIME_SUBTRACT(planduration, planstart);

  if (es->buffers) {
    memset(&bufusage, 0, sizeof(BufferUsage));
    BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start);
  }

  boot_ExplainOnePlan(plan, into, es, queryString, params, queryEnv, &planduration, (es->buffers ? &bufusage : NULL),
                      queryTemplate);
}

void boot_ExplainPlan(JumbleState *jstate, const char *queryString, ParamListInfo params, ParseState *pstate,
                      ExplainState *es, List *rewritten) {
  const char *queryStringNormalized;
  ListCell *l;
  int queryLen;

  queryLen = strlen(queryString);
  queryStringNormalized = generate_normalized_query(jstate, queryString, 0, &queryLen);

  // Cache result computation.
  if (rewritten == NIL) {
    if (es->format == EXPLAIN_FORMAT_TEXT) {
      appendStringInfoString(es->str, "Query rewrites to nothing\n");
    }
  } else {
    foreach (l, rewritten) {
      boot_ExplainOneQuery(lfirst_node(Query, l), CURSOR_OPT_PARALLEL_OK, NULL, es, pstate->p_sourcetext, params,
                           pstate->p_queryEnv, queryStringNormalized);
      if (lnext(rewritten, l) != NULL) {
        elog(ERROR, "BROKEN ASSUMPTION: %s", queryString);
      }
    }
  }
  Assert(es->indent == 0);
}

void boot_ExplainIntercept(JumbleState *jstate, const char *queryString, ParamListInfo params, ParseState *pstate,
                           ExplainState *es, List *rewritten) {
  if (boot_prev_ExplainIntercept_hook) {
    (*boot_prev_ExplainIntercept_hook)(jstate, queryString, params, pstate, es, rewritten);
  }

  if (!boot_enable) {
    boot_ExplainDefault(jstate, queryString, params, pstate, es, rewritten);
    return;
  }

  //  elog(LOG, "boot.macro_mode %d", boot_macro_mode);

  // By only using the query template, we don't need to invoke the planner.
  if (boot_macro_mode == 0) {
    boot_ExplainTemplate(jstate, queryString, params, pstate, es, rewritten);
    return;
  }
  // Otherwise, welp. Copy pasta to avoid planning twice.
  if (boot_macro_mode == 1) {
    boot_ExplainPlan(jstate, queryString, params, pstate, es, rewritten);
    return;
  }
  elog(ERROR, "HECC boot_macro_mode %d", boot_macro_mode);
}

static double elapsed_time(instr_time *starttime) {
  instr_time endtime;

  INSTR_TIME_SET_CURRENT(endtime);
  INSTR_TIME_SUBTRACT(endtime, *starttime);
  return INSTR_TIME_GET_DOUBLE(endtime);
}

static void show_buffer_usage(ExplainState *es, const BufferUsage *usage, bool planning) {
  if (es->format == EXPLAIN_FORMAT_TEXT) {
    bool has_shared = (usage->shared_blks_hit > 0 || usage->shared_blks_read > 0 || usage->shared_blks_dirtied > 0 ||
                       usage->shared_blks_written > 0);
    bool has_local = (usage->local_blks_hit > 0 || usage->local_blks_read > 0 || usage->local_blks_dirtied > 0 ||
                      usage->local_blks_written > 0);
    bool has_temp = (usage->temp_blks_read > 0 || usage->temp_blks_written > 0);
    bool has_timing = (!INSTR_TIME_IS_ZERO(usage->blk_read_time) || !INSTR_TIME_IS_ZERO(usage->blk_write_time));
    bool has_temp_timing =
        (!INSTR_TIME_IS_ZERO(usage->temp_blk_read_time) || !INSTR_TIME_IS_ZERO(usage->temp_blk_write_time));
    bool show_planning = (planning && (has_shared || has_local || has_temp || has_timing || has_temp_timing));

    if (show_planning) {
      ExplainIndentText(es);
      appendStringInfoString(es->str, "Planning:\n");
      es->indent++;
    }

    /* Show only positive counter values. */
    if (has_shared || has_local || has_temp) {
      ExplainIndentText(es);
      appendStringInfoString(es->str, "Buffers:");

      if (has_shared) {
        appendStringInfoString(es->str, " shared");
        if (usage->shared_blks_hit > 0) appendStringInfo(es->str, " hit=%lld", (long long)usage->shared_blks_hit);
        if (usage->shared_blks_read > 0) appendStringInfo(es->str, " read=%lld", (long long)usage->shared_blks_read);
        if (usage->shared_blks_dirtied > 0)
          appendStringInfo(es->str, " dirtied=%lld", (long long)usage->shared_blks_dirtied);
        if (usage->shared_blks_written > 0)
          appendStringInfo(es->str, " written=%lld", (long long)usage->shared_blks_written);
        if (has_local || has_temp) appendStringInfoChar(es->str, ',');
      }
      if (has_local) {
        appendStringInfoString(es->str, " local");
        if (usage->local_blks_hit > 0) appendStringInfo(es->str, " hit=%lld", (long long)usage->local_blks_hit);
        if (usage->local_blks_read > 0) appendStringInfo(es->str, " read=%lld", (long long)usage->local_blks_read);
        if (usage->local_blks_dirtied > 0)
          appendStringInfo(es->str, " dirtied=%lld", (long long)usage->local_blks_dirtied);
        if (usage->local_blks_written > 0)
          appendStringInfo(es->str, " written=%lld", (long long)usage->local_blks_written);
        if (has_temp) appendStringInfoChar(es->str, ',');
      }
      if (has_temp) {
        appendStringInfoString(es->str, " temp");
        if (usage->temp_blks_read > 0) appendStringInfo(es->str, " read=%lld", (long long)usage->temp_blks_read);
        if (usage->temp_blks_written > 0)
          appendStringInfo(es->str, " written=%lld", (long long)usage->temp_blks_written);
      }
      appendStringInfoChar(es->str, '\n');
    }

    /* As above, show only positive counter values. */
    if (has_timing || has_temp_timing) {
      ExplainIndentText(es);
      appendStringInfoString(es->str, "I/O Timings:");

      if (has_timing) {
        appendStringInfoString(es->str, " shared/local");
        if (!INSTR_TIME_IS_ZERO(usage->blk_read_time))
          appendStringInfo(es->str, " read=%0.3f", INSTR_TIME_GET_MILLISEC(usage->blk_read_time));
        if (!INSTR_TIME_IS_ZERO(usage->blk_write_time))
          appendStringInfo(es->str, " write=%0.3f", INSTR_TIME_GET_MILLISEC(usage->blk_write_time));
        if (has_temp_timing) appendStringInfoChar(es->str, ',');
      }
      if (has_temp_timing) {
        appendStringInfoString(es->str, " temp");
        if (!INSTR_TIME_IS_ZERO(usage->temp_blk_read_time))
          appendStringInfo(es->str, " read=%0.3f", INSTR_TIME_GET_MILLISEC(usage->temp_blk_read_time));
        if (!INSTR_TIME_IS_ZERO(usage->temp_blk_write_time))
          appendStringInfo(es->str, " write=%0.3f", INSTR_TIME_GET_MILLISEC(usage->temp_blk_write_time));
      }
      appendStringInfoChar(es->str, '\n');
    }

    if (show_planning) es->indent--;
  } else {
    ExplainPropertyInteger("Shared Hit Blocks", NULL, usage->shared_blks_hit, es);
    ExplainPropertyInteger("Shared Read Blocks", NULL, usage->shared_blks_read, es);
    ExplainPropertyInteger("Shared Dirtied Blocks", NULL, usage->shared_blks_dirtied, es);
    ExplainPropertyInteger("Shared Written Blocks", NULL, usage->shared_blks_written, es);
    ExplainPropertyInteger("Local Hit Blocks", NULL, usage->local_blks_hit, es);
    ExplainPropertyInteger("Local Read Blocks", NULL, usage->local_blks_read, es);
    ExplainPropertyInteger("Local Dirtied Blocks", NULL, usage->local_blks_dirtied, es);
    ExplainPropertyInteger("Local Written Blocks", NULL, usage->local_blks_written, es);
    ExplainPropertyInteger("Temp Read Blocks", NULL, usage->temp_blks_read, es);
    ExplainPropertyInteger("Temp Written Blocks", NULL, usage->temp_blks_written, es);
    if (track_io_timing) {
      ExplainPropertyFloat("I/O Read Time", "ms", INSTR_TIME_GET_MILLISEC(usage->blk_read_time), 3, es);
      ExplainPropertyFloat("I/O Write Time", "ms", INSTR_TIME_GET_MILLISEC(usage->blk_write_time), 3, es);
      ExplainPropertyFloat("Temp I/O Read Time", "ms", INSTR_TIME_GET_MILLISEC(usage->temp_blk_read_time), 3, es);
      ExplainPropertyFloat("Temp I/O Write Time", "ms", INSTR_TIME_GET_MILLISEC(usage->temp_blk_write_time), 3, es);
    }
  }
}

static void ExplainIndentText(ExplainState *es) {
  Assert(es->format == EXPLAIN_FORMAT_TEXT);
  if (es->str->len == 0 || es->str->data[es->str->len - 1] == '\n') appendStringInfoSpaces(es->str, es->indent * 2);
}
