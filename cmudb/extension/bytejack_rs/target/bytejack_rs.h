#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct CacheResult CacheResult;

typedef struct RedisCon RedisCon;

typedef struct RuntimeState RuntimeState;

struct RedisCon *redis_connect(void);

void redis_free(struct RedisCon *ptr);

struct RuntimeState *runtime_state_new(void);

void runtime_state_free(struct RuntimeState *ptr);

void runtime_init_operator(struct RuntimeState *runtime_state,
                           int32_t operator_id,
                           int32_t parallel_worker_number,
                           int32_t plan_type,
                           double optimizer_startup_cost,
                           double optimizer_total_cost,
                           double optimizer_plan_rows,
                           double optimizer_plan_width,
                           double mu_hyp_opt,
                           double mu_hyp_time,
                           double mu_hyp_stdev);

int32_t runtime_add_tuple_batch(struct RuntimeState *runtime_state,
                                int32_t operator_id,
                                int32_t parallel_worker_number,
                                double n_tuples,
                                double accumulated_us);

int32_t cache_result_len(struct CacheResult *ptr);

const uint8_t *cache_result_bytes(struct CacheResult *ptr);

void cache_result_free(struct CacheResult *ptr);

struct CacheResult *cache_get_explain(struct RedisCon *redis_con,
                                      bool intelligent,
                                      const char *query_text);

void cache_append_explain(struct RedisCon *redis_con,
                          bool intelligent,
                          const char *query_text,
                          const char *explain_text);

void cache_clear(struct RedisCon *redis_con);

void cache_save(struct RedisCon *redis_con, const char *dbname);
