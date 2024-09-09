#pragma once

#include "../../boot_rs/target/boot_rs.h"

extern ConnectionLifetime_hook_type boot_prev_ConnectionLifetime_hook;

extern bool boot_enable;
extern bool boot_intercept_explain_analyze;
extern bool boot_intelligent_cache;
extern bool boot_early_stop;
extern bool boot_seq_sample;
extern double boot_seq_sample_pct;
extern int boot_seq_sample_seed;
extern RedisCon *redis_con;
extern double boot_mu_hyp_opt;
extern double boot_mu_hyp_time;
extern double boot_mu_hyp_stdev;
extern int boot_macro_mode;

void boot_ConnectionLifetime(void);
