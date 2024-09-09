#include "boot_executor.h"

#include "../../../../src/include/access/parallel.h"
#include "../../../../src/include/nodes/execnodes.h"
#include "../../../../src/include/nodes/nodeFuncs.h"
#include "boot.h"

RuntimeState *runtime_state = NULL;
ExecutorStart_hook_type boot_prev_ExecutorStart_hook = NULL;
ExecutorRun_hook_type boot_prev_ExecutorRun_hook = NULL;
ExecutorFinish_hook_type boot_prev_ExecutorFinish_hook = NULL;
ExecutorEnd_hook_type boot_prev_ExecutorEnd_hook = NULL;

static int boot_nesting_level = 0;

static void boot_cleanup(void);
static bool boot_init_runtime_state(PlanState *planstate);

void boot_ExecutorStart(QueryDesc *queryDesc, int eflags) {
  if (boot_prev_ExecutorStart_hook) {
    boot_prev_ExecutorStart_hook(queryDesc, eflags);
  } else {
    standard_ExecutorStart(queryDesc, eflags);
  }
  if (boot_nesting_level == 0 && boot_enable) {
    runtime_state = runtime_state_new();
    boot_init_runtime_state(queryDesc->planstate);
  }
}

void boot_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once) {
  boot_nesting_level++;
  PG_TRY();
  {
    if (boot_prev_ExecutorRun_hook) {
      boot_prev_ExecutorRun_hook(queryDesc, direction, count, execute_once);
    } else {
      standard_ExecutorRun(queryDesc, direction, count, execute_once);
    }
    boot_nesting_level--;
  }
  PG_CATCH();
  {
    boot_nesting_level--;
    boot_cleanup();
    PG_RE_THROW();
  }
  PG_END_TRY();
}

void boot_ExecutorFinish(QueryDesc *queryDesc) {
  boot_nesting_level++;
  PG_TRY();
  {
    if (boot_prev_ExecutorFinish_hook) {
      boot_prev_ExecutorFinish_hook(queryDesc);
    } else {
      standard_ExecutorFinish(queryDesc);
    }
  }
  PG_FINALLY();
  { boot_nesting_level--; }
  PG_END_TRY();
}

void boot_ExecutorEnd(QueryDesc *queryDesc) {
  if (boot_prev_ExecutorEnd_hook) {
    boot_prev_ExecutorEnd_hook(queryDesc);
  } else {
    standard_ExecutorEnd(queryDesc);
  }

  if (boot_nesting_level == 0 && boot_enable) {
    boot_cleanup();
  }
}

static void boot_cleanup(void) {
  if (boot_enable) {
    runtime_state_free(runtime_state);
    runtime_state = NULL;
  }
}

static bool boot_init_runtime_state(PlanState *planstate) {
  int plan_type = planstate->plan->type;
  int plan_node_id = planstate->plan->plan_node_id;
  double optimizer_startup_cost = planstate->plan->startup_cost;
  double optimizer_total_cost = planstate->plan->total_cost;
  double optimizer_plan_rows = planstate->plan->plan_rows;
  double optimizer_plan_width = planstate->plan->plan_width;
  runtime_init_operator(runtime_state, plan_node_id, ParallelWorkerNumber, plan_type, optimizer_startup_cost,
                        optimizer_total_cost, optimizer_plan_rows, optimizer_plan_width, boot_mu_hyp_opt,
                        boot_mu_hyp_time, boot_mu_hyp_stdev);
  return planstate_tree_walker(planstate, boot_init_runtime_state, NULL);
}
