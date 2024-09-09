// clang-format off
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/fmgr.h"
#include "../../../../src/include/utils/guc.h"
// clang-format on

#include "boot.h"

#include "../../../../src/include/storage/ipc.h"
#include "../../../../src/include/utils/builtins.h"
#include "../../../../src/include/utils/queryjumble.h"
#include "boot_Seqscan.h"
#include "boot_executor.h"
#include "boot_explain.h"
#include "boot_instrument.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(boot_cache_clear);
PG_FUNCTION_INFO_V1(boot_connect);
PG_FUNCTION_INFO_V1(boot_disconnect);
PG_FUNCTION_INFO_V1(boot_save);

Datum boot_cache_clear(PG_FUNCTION_ARGS);
Datum boot_connect(PG_FUNCTION_ARGS);
Datum boot_disconnect(PG_FUNCTION_ARGS);
Datum boot_save(PG_FUNCTION_ARGS);

void _PG_init(void);  // NOLINT
static void boot_cleanup(int code, Datum arg);

ConnectionLifetime_hook_type boot_prev_ConnectionLifetime_hook = NULL;
RedisCon *redis_con = NULL;
bool boot_enable = false;
bool boot_intercept_explain_analyze = false;
bool boot_early_stop = false;
bool boot_intelligent_cache = false;
bool boot_seq_sample = false;
double boot_seq_sample_pct = 100;
int boot_seq_sample_seed = 15721;
int boot_macro_mode = 0;

double boot_mu_hyp_opt = 0.10;
double boot_mu_hyp_time = 1e6;
double boot_mu_hyp_stdev = 2.0;

Datum boot_cache_clear(PG_FUNCTION_ARGS) {
  cache_clear(redis_con);
  PG_RETURN_BOOL(true);
}

Datum boot_connect(PG_FUNCTION_ARGS) {
  redis_con = redis_connect();
  on_proc_exit(boot_cleanup, 0);
  PG_RETURN_BOOL(true);
}

Datum boot_disconnect(PG_FUNCTION_ARGS) {
  boot_cleanup(0, 0);
  PG_RETURN_BOOL(true);
}

Datum boot_save(PG_FUNCTION_ARGS) {
  char *dbname = TextDatumGetCString(PG_GETARG_DATUM(0));
  cache_save(redis_con, dbname);
  PG_RETURN_BOOL(true);
}

void boot_ConnectionLifetime(void) {
  if (boot_prev_ConnectionLifetime_hook) {
    boot_prev_ConnectionLifetime_hook();
  }
}

void _PG_init(void) {
  DefineCustomBoolVariable("boot.enable", "Enable Boot.", NULL, &boot_enable, false, PGC_SUSET, 0, NULL, NULL, NULL);

  DefineCustomBoolVariable("boot.intercept_explain_analyze", "Intercept EXPLAIN ANALYZE queries.", NULL,
                           &boot_intercept_explain_analyze, false, PGC_SUSET, 0, NULL, NULL, NULL);
  DefineCustomBoolVariable("boot.intelligent_cache", "Enable intelligent cache.", NULL, &boot_intelligent_cache, false,
                           PGC_SUSET, 0, NULL, NULL, NULL);
  DefineCustomIntVariable("boot.macro_mode", "Cache mode: 0 = template, 1 = QP.", NULL, &boot_macro_mode, 0, 0, 100000,
                          PGC_USERSET, 0, NULL, NULL, NULL);

  DefineCustomBoolVariable("boot.early_stop", "Early stop operators.", NULL, &boot_early_stop, false, PGC_SUSET, 0,
                           NULL, NULL, NULL);

  DefineCustomBoolVariable("boot.seq_sample", "Whether to sample seq scans.", NULL, &boot_seq_sample, false,
                           PGC_USERSET, 0, NULL, NULL, NULL);
  DefineCustomRealVariable("boot.seq_sample_pct", "The percentage of rows to sample.", NULL, &boot_seq_sample_pct, 100,
                           0, 100, PGC_USERSET, 0, NULL, NULL, NULL);
  DefineCustomIntVariable("boot.seq_sample_seed", "The seed for sampling.", NULL, &boot_seq_sample_seed, 15721, 0,
                          100000, PGC_USERSET, 0, NULL, NULL, NULL);

  DefineCustomRealVariable("boot.mu_hyp_opt", "muacc min opt pct.", NULL, &boot_mu_hyp_opt, 0.10, 0, 1, PGC_USERSET, 0,
                           NULL, NULL, NULL);
  DefineCustomRealVariable("boot.mu_hyp_time", "muacc min time (us).", NULL, &boot_mu_hyp_time, 1000000, 0, 1000000000,
                           PGC_USERSET, 0, NULL, NULL, NULL);
  DefineCustomRealVariable("boot.mu_hyp_stdev", "muacc stdev.", NULL, &boot_mu_hyp_stdev, 2.0, 0, 10, PGC_USERSET, 0,
                           NULL, NULL, NULL);

  MarkGUCPrefixReserved("boot");

  EnableQueryId();

  boot_prev_ConnectionLifetime_hook = ConnectionLifetime_hook;
  ConnectionLifetime_hook = boot_ConnectionLifetime;

  boot_prev_ExplainIntercept_hook = ExplainIntercept_hook;
  ExplainIntercept_hook = boot_ExplainIntercept;

  boot_prev_ExecutorStart_hook = ExecutorStart_hook;
  ExecutorStart_hook = boot_ExecutorStart;
  boot_prev_ExecutorRun_hook = ExecutorRun_hook;
  ExecutorRun_hook = boot_ExecutorRun;
  boot_prev_ExecutorFinish_hook = ExecutorFinish_hook;
  ExecutorFinish_hook = boot_ExecutorFinish;
  boot_prev_ExecutorEnd_hook = ExecutorEnd_hook;
  ExecutorEnd_hook = boot_ExecutorEnd;

  boot_prev_SeqNext_hook = SeqNext_hook;
  SeqNext_hook = boot_SeqNext;

  boot_prev_InstrAddTupleBatchTimes_hook = InstrAddTupleBatchTimes_hook;
  InstrAddTupleBatchTimes_hook = boot_InstrAddTupleBatchTimes;
}

static void boot_cleanup(int code, Datum arg) {
  (void)code;
  (void)arg;
  redis_free(redis_con);
  redis_con = NULL;
}
