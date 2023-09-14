#include "bytejack_executor.h"

#include "../../../../src/include/access/parallel.h"
#include "../../../../src/include/nodes/execnodes.h"
#include "../../../../src/include/nodes/nodeFuncs.h"
#include "bytejack.h"

RuntimeState *runtime_state = NULL;
ExecutorStart_hook_type bytejack_prev_ExecutorStart_hook = NULL;
ExecutorRun_hook_type bytejack_prev_ExecutorRun_hook = NULL;
ExecutorFinish_hook_type bytejack_prev_ExecutorFinish_hook = NULL;
ExecutorEnd_hook_type bytejack_prev_ExecutorEnd_hook = NULL;

static int bytejack_nesting_level = 0;

static void bytejack_cleanup(void);
static bool bytejack_init_runtime_state(PlanState *planstate);

void bytejack_ExecutorStart(QueryDesc *queryDesc, int eflags) {
  if (bytejack_prev_ExecutorStart_hook) {
    bytejack_prev_ExecutorStart_hook(queryDesc, eflags);
  } else {
    standard_ExecutorStart(queryDesc, eflags);
  }
  if (bytejack_nesting_level == 0 && bytejack_enable) {
    runtime_state = runtime_state_new();
    bytejack_init_runtime_state(queryDesc->planstate);
  }
}

void bytejack_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once) {
  bytejack_nesting_level++;
  PG_TRY();
  {
    if (bytejack_prev_ExecutorRun_hook) {
      bytejack_prev_ExecutorRun_hook(queryDesc, direction, count, execute_once);
    } else {
      standard_ExecutorRun(queryDesc, direction, count, execute_once);
    }
    bytejack_nesting_level--;
  }
  PG_CATCH();
  {
    bytejack_nesting_level--;
    bytejack_cleanup();
    PG_RE_THROW();
  }
  PG_END_TRY();
}

void bytejack_ExecutorFinish(QueryDesc *queryDesc) {
  bytejack_nesting_level++;
  PG_TRY();
  {
    if (bytejack_prev_ExecutorFinish_hook) {
      bytejack_prev_ExecutorFinish_hook(queryDesc);
    } else {
      standard_ExecutorFinish(queryDesc);
    }
  }
  PG_FINALLY();
  { bytejack_nesting_level--; }
  PG_END_TRY();
}

void bytejack_ExecutorEnd(QueryDesc *queryDesc) {
  if (bytejack_prev_ExecutorEnd_hook) {
    bytejack_prev_ExecutorEnd_hook(queryDesc);
  } else {
    standard_ExecutorEnd(queryDesc);
  }

  if (bytejack_nesting_level == 0 && bytejack_enable) {
    bytejack_cleanup();
  }
}

static void bytejack_cleanup(void) {
  if (bytejack_enable) {
    runtime_state_free(runtime_state);
    runtime_state = NULL;
  }
}

static bool bytejack_init_runtime_state(PlanState *planstate) {
  int plan_type = planstate->plan->type;
  int plan_node_id = planstate->plan->plan_node_id;
  double optimizer_startup_cost = planstate->plan->startup_cost;
  double optimizer_total_cost = planstate->plan->total_cost;
  double optimizer_plan_rows = planstate->plan->plan_rows;
  double optimizer_plan_width = planstate->plan->plan_width;
  runtime_init_operator(runtime_state, plan_node_id, ParallelWorkerNumber, plan_type, optimizer_startup_cost,
                        optimizer_total_cost, optimizer_plan_rows, optimizer_plan_width,
                        bytejack_mu_hyp_opt, bytejack_mu_hyp_time, bytejack_mu_hyp_stdev);
  return planstate_tree_walker(planstate, bytejack_init_runtime_state, NULL);
}