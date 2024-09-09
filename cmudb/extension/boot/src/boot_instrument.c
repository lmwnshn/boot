#include "boot_instrument.h"

#include "../../../../src/include/access/parallel.h"
#include "../../boot_rs/target/boot_rs.h"
#include "boot.h"
#include "boot_executor.h"

InstrAddTupleBatchTimes_hook_type boot_prev_InstrAddTupleBatchTimes_hook = NULL;

bool boot_InstrAddTupleBatchTimes(struct PlanState *node, double n_tuples, double accumulated_us) {
  int ret;

  if (boot_prev_InstrAddTupleBatchTimes_hook) {
    boot_prev_InstrAddTupleBatchTimes_hook(node, n_tuples, accumulated_us);
  }

  if (!boot_enable) {
    return false;
  }

  //  let ret_stop = 1;
  //  let ret_newbatch = 2;
  //  let ret_samebatch = 3;
  ret =
      runtime_add_tuple_batch(runtime_state, node->plan->plan_node_id, ParallelWorkerNumber, n_tuples, accumulated_us);
  if (ret == 1) {
    if (boot_early_stop) {
      node->boot_stop = true;
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
