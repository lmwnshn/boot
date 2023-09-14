#include "bytejack_instrument.h"

#include "../../../../src/include/access/parallel.h"
#include "../../bytejack_rs/target/bytejack_rs.h"
#include "bytejack.h"
#include "bytejack_executor.h"

InstrAddTupleBatchTimes_hook_type bytejack_prev_InstrAddTupleBatchTimes_hook = NULL;

bool bytejack_InstrAddTupleBatchTimes(struct PlanState *node, double n_tuples, double accumulated_us) {
  int ret;

  if (bytejack_prev_InstrAddTupleBatchTimes_hook) {
    bytejack_prev_InstrAddTupleBatchTimes_hook(node, n_tuples, accumulated_us);
  }

  if (!bytejack_enable) {
    return false;
  }

  //  let ret_stop = 1;
  //  let ret_newbatch = 2;
  //  let ret_samebatch = 3;
  ret =
      runtime_add_tuple_batch(runtime_state, node->plan->plan_node_id, ParallelWorkerNumber, n_tuples, accumulated_us);
  if (ret == 1) {
    if (bytejack_early_stop) {
      node->bytejack_stop = true;
    }
    return true;
  } else if (ret == 2) {
    return true;
  } else if (ret == 3) {
    return false;
  }
  // Hopefully, unreachable.
  return false;
}
