#include "bytejack_Seqscan.h"

#include <math.h>

#include "../../../../src/include/access/relscan.h"
#include "../../../../src/include/access/tableam.h"
#include "../../../../src/include/common/hashfn.h"
#include "bytejack.h"

SeqNext_hook_type bytejack_prev_SeqNext_hook = NULL;

TupleTableSlot *bytejack_SeqNext(SeqScanState *node) {
  TableScanDesc scandesc;
  EState *estate;
  ScanDirection direction;
  TupleTableSlot *slot;
  bool slot_ok;
  bool can_sample;

  uint32 hashinput[3];
  uint32 hash;
  uint32 block_number;
  uint16 offset_number;
  uint64 cutoff;
  float8 seed;

  scandesc = node->ss.ss_currentScanDesc;
  estate = node->ss.ps.state;
  direction = estate->es_direction;
  slot = node->ss.ss_ScanTupleSlot;

  if (scandesc == NULL) {
    scandesc = table_beginscan(node->ss.ss_currentRelation, estate->es_snapshot, 0, NULL);
    node->ss.ss_currentScanDesc = scandesc;
  }

  while (true) {
    slot_ok = table_scan_getnextslot(scandesc, direction, slot);

    if (!slot_ok) {
      return NULL;
    }

    can_sample = bytejack_enable && bytejack_seq_sample && node->ss.ss_currentRelation->rd_id >= FirstNormalObjectId;
    if (!can_sample) {
      return slot;
    }

    // bernoulli.c
    block_number = BlockIdGetBlockNumber(&slot->tts_tid.ip_blkid);
    offset_number = slot->tts_tid.ip_posid;
    hashinput[0] = block_number;
    hashinput[1] = offset_number;
    seed = bytejack_seq_sample_seed;
    hashinput[2] = hash_any((unsigned char *)&seed, sizeof(seed));

    hash = DatumGetUInt32(hash_any((const unsigned char *)hashinput, (int)sizeof(hashinput)));
    cutoff = (uint64)((double)rint(((double)PG_UINT32_MAX + 1) * bytejack_seq_sample_pct / 100));
    if (hash < cutoff) {
      return slot;
    }
  }
}
