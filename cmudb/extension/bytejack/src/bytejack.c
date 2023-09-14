// clang-format off
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/fmgr.h"
#include "../../../../src/include/utils/guc.h"
// clang-format on

#include "bytejack.h"

#include "../../../../src/include/storage/ipc.h"
#include "../../../../src/include/utils/builtins.h"
#include "../../../../src/include/utils/queryjumble.h"
#include "bytejack_Seqscan.h"
#include "bytejack_executor.h"
#include "bytejack_explain.h"
#include "bytejack_instrument.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(bytejack_cache_clear);
PG_FUNCTION_INFO_V1(bytejack_connect);
PG_FUNCTION_INFO_V1(bytejack_disconnect);
PG_FUNCTION_INFO_V1(bytejack_save);

Datum bytejack_cache_clear(PG_FUNCTION_ARGS);
Datum bytejack_connect(PG_FUNCTION_ARGS);
Datum bytejack_disconnect(PG_FUNCTION_ARGS);
Datum bytejack_save(PG_FUNCTION_ARGS);

void _PG_init(void);  // NOLINT
static void bytejack_cleanup(int code, Datum arg);

ConnectionLifetime_hook_type bytejack_prev_ConnectionLifetime_hook = NULL;
RedisCon *redis_con = NULL;
bool bytejack_enable = false;
bool bytejack_intercept_explain_analyze = false;
bool bytejack_early_stop = false;
bool bytejack_intelligent_cache = false;
bool bytejack_seq_sample = false;
double bytejack_seq_sample_pct = 100;
int bytejack_seq_sample_seed = 15721;

double bytejack_mu_hyp_opt = 0.10;
double bytejack_mu_hyp_time = 1e6;
double bytejack_mu_hyp_stdev = 2.0;

Datum bytejack_cache_clear(PG_FUNCTION_ARGS) {
  cache_clear(redis_con);
  PG_RETURN_BOOL(true);
}

Datum bytejack_connect(PG_FUNCTION_ARGS) {
  redis_con = redis_connect();
  on_proc_exit(bytejack_cleanup, 0);
  PG_RETURN_BOOL(true);
}

Datum bytejack_disconnect(PG_FUNCTION_ARGS) {
  bytejack_cleanup(0, 0);
  PG_RETURN_BOOL(true);
}

Datum bytejack_save(PG_FUNCTION_ARGS) {
  char *dbname = TextDatumGetCString(PG_GETARG_DATUM(0));
  cache_save(redis_con, dbname);
  PG_RETURN_BOOL(true);
}

void bytejack_ConnectionLifetime(void) {
  if (bytejack_prev_ConnectionLifetime_hook) {
    bytejack_prev_ConnectionLifetime_hook();
  }
}

void _PG_init(void) {
  DefineCustomBoolVariable("bytejack.enable", "Enable bytejack.", NULL, &bytejack_enable, false, PGC_SUSET, 0, NULL,
                           NULL, NULL);

  DefineCustomBoolVariable("bytejack.intercept_explain_analyze", "Intercept EXPLAIN ANALYZE queries.", NULL,
                           &bytejack_intercept_explain_analyze, false, PGC_SUSET, 0, NULL, NULL, NULL);
  DefineCustomBoolVariable("bytejack.intelligent_cache", "Intelligent cache.", NULL,
                           &bytejack_intelligent_cache, false, PGC_SUSET, 0, NULL, NULL, NULL);

  DefineCustomBoolVariable("bytejack.early_stop", "Early stop operators.", NULL, &bytejack_early_stop, false, PGC_SUSET,
                           0, NULL, NULL, NULL);

  DefineCustomBoolVariable("bytejack.seq_sample", "Whether to sample seq scans.", NULL, &bytejack_seq_sample, false,
                           PGC_USERSET, 0, NULL, NULL, NULL);
  DefineCustomRealVariable("bytejack.seq_sample_pct", "The percentage of rows to sample.", NULL,
                           &bytejack_seq_sample_pct, 100, 0, 100, PGC_USERSET, 0, NULL, NULL, NULL);
  DefineCustomIntVariable("bytejack.seq_sample_seed", "The seed for sampling.", NULL, &bytejack_seq_sample_seed, 15721,
                          0, 100000, PGC_USERSET, 0, NULL, NULL, NULL);

  DefineCustomRealVariable("bytejack.mu_hyp_opt", "muacc min opt pct.", NULL,
                           &bytejack_mu_hyp_opt, 0.10, 0, 1, PGC_USERSET, 0, NULL, NULL, NULL);
  DefineCustomRealVariable("bytejack.mu_hyp_time", "muacc min time (us).", NULL,
                           &bytejack_mu_hyp_time, 1000000, 0, 1000000000, PGC_USERSET, 0, NULL, NULL, NULL);
  DefineCustomRealVariable("bytejack.mu_hyp_stdev", "muacc stdev.", NULL,
                           &bytejack_mu_hyp_stdev, 2.0, 0, 10, PGC_USERSET, 0, NULL, NULL, NULL);

  MarkGUCPrefixReserved("bytejack");

  EnableQueryId();

  bytejack_prev_ConnectionLifetime_hook = ConnectionLifetime_hook;
  ConnectionLifetime_hook = bytejack_ConnectionLifetime;

  bytejack_prev_ExplainIntercept_hook = ExplainIntercept_hook;
  ExplainIntercept_hook = bytejack_ExplainIntercept;

  bytejack_prev_ExecutorStart_hook = ExecutorStart_hook;
  ExecutorStart_hook = bytejack_ExecutorStart;
  bytejack_prev_ExecutorRun_hook = ExecutorRun_hook;
  ExecutorRun_hook = bytejack_ExecutorRun;
  bytejack_prev_ExecutorFinish_hook = ExecutorFinish_hook;
  ExecutorFinish_hook = bytejack_ExecutorFinish;
  bytejack_prev_ExecutorEnd_hook = ExecutorEnd_hook;
  ExecutorEnd_hook = bytejack_ExecutorEnd;

  bytejack_prev_SeqNext_hook = SeqNext_hook;
  SeqNext_hook = bytejack_SeqNext;

  bytejack_prev_InstrAddTupleBatchTimes_hook = InstrAddTupleBatchTimes_hook;
  InstrAddTupleBatchTimes_hook = bytejack_InstrAddTupleBatchTimes;
}

static void bytejack_cleanup(int code, Datum arg) {
  (void)code;
  (void)arg;
  redis_free(redis_con);
  redis_con = NULL;
}