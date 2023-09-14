#pragma once

#include "../../bytejack_rs/target/bytejack_rs.h"

extern ConnectionLifetime_hook_type bytejack_prev_ConnectionLifetime_hook;

extern bool bytejack_enable;
extern bool bytejack_intercept_explain_analyze;
extern bool bytejack_intelligent_cache;
extern bool bytejack_early_stop;
extern bool bytejack_seq_sample;
extern double bytejack_seq_sample_pct;
extern int bytejack_seq_sample_seed;
extern RedisCon *redis_con;
extern double bytejack_mu_hyp_opt;
extern double bytejack_mu_hyp_time;
extern double bytejack_mu_hyp_stdev;

void bytejack_ConnectionLifetime(void);
