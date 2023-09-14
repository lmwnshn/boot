extern crate redis;

use welford::Welford;
use std::ffi::{c_char, CStr};
use std::{ptr};
use std::collections::HashMap;

pub struct RedisCacheEntry {
    fetches: i32,
    backoff_counter: i32,
    times: Welford<f64>,
    num_times: i32,
}

impl RedisCacheEntry {
    pub fn new() -> Self {
        RedisCacheEntry {
            fetches: 0,
            backoff_counter: 1,
            times: Welford::new(),
            num_times: 0,
        }
    }

    pub fn should_rerun(&mut self) -> bool {
        self.fetches += 1;
        if self.fetches <= self.backoff_counter {
            false
        } else {
            self.fetches = 0;
            self.backoff_counter = std::cmp::min(self.backoff_counter * 2, 100);
            true
        }
    }

    pub fn add_latest(&mut self, time_us: f64) {
        if self.num_times > 1 {
            let mean_old = self.times.mean().unwrap();
            let stdev_old = self.times.var().unwrap().sqrt();
            let similar = (mean_old - 2.0 * stdev_old <= time_us) && (time_us <= mean_old + 2.0 * stdev_old);

            if !similar {
                self.fetches = 0;
                self.backoff_counter = 1;
                self.times = Welford::new();
                self.num_times = 0;
            }
        }
        self.times.push(time_us);
        self.num_times += 1
    }
}

pub struct RedisCon {
    _client: redis::Client,
    con: redis::Connection,
    stats: HashMap<String, RedisCacheEntry>,
}

#[no_mangle]
pub extern "C" fn redis_connect() -> *mut RedisCon {
    let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
    let con = client.get_connection().unwrap();
    Box::into_raw(Box::new(RedisCon { _client: client, con, stats: HashMap::new() }))
}

#[no_mangle]
pub extern "C" fn redis_free(ptr: *mut RedisCon) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let _ = Box::from_raw(ptr);
    }
}

#[derive(Debug)]
pub struct OperatorState {
    plan_type: i32,
    optimizer_startup_cost: f64,
    optimizer_total_cost: f64,
    optimizer_plan_rows: f64,
    optimizer_plan_width: f64,
    time_us: Vec<f64>,
    batch_sizes: Vec<f64>,
    current_n_tuples: f64,
    mu_hyp_opt: f64,
    mu_hyp_time: f64,
    mu_hyp_stdev: f64,
}

// 20 Seq Scan
// 49 Gather

impl OperatorState {
    fn new() -> Self {
        OperatorState {
            plan_type: -1,
            optimizer_startup_cost: 0.0,
            optimizer_total_cost: 0.0,
            optimizer_plan_rows: 0.0,
            optimizer_plan_width: 0.0,
            time_us: vec![],
            batch_sizes: vec![],
            current_n_tuples: 0.0,
            mu_hyp_opt: 0.0,
            mu_hyp_time: 0.0,
            mu_hyp_stdev: 0.0,
        }
    }

    fn try_stop(&mut self) -> bool {
        if self.batch_sizes.len() < 5 {
            return false;
        }

        let batch_window = (self.batch_sizes.len() as f64 * 0.2) as usize;
        let last_batch_idx = self.batch_sizes.len() - 1;
        let end_batch_idx = last_batch_idx - 1;
        let start_batch_idx = end_batch_idx - batch_window;

        let mut wold = Welford::<f64>::with_weights();
        for idx in start_batch_idx..=end_batch_idx {
            wold.push_weighted(self.time_us[idx], self.batch_sizes[idx] as usize);
        }
        let mean_old = wold.mean().unwrap();
        let stdev_old = wold.var().unwrap().sqrt();

        let mut wnew = Welford::<f64>::with_weights();
        wnew.push_weighted(self.time_us[last_batch_idx], self.batch_sizes[last_batch_idx] as usize);
        let mean_new = wnew.mean().unwrap();

        (mean_old - self.mu_hyp_stdev * stdev_old <= mean_new) && (mean_new <= mean_old + self.mu_hyp_stdev * stdev_old)
    }

    fn add_tuple_batch(&mut self, n_tuples: f64, accumulated_us: f64) -> i32 {
        let ret_stop = 1;
        let ret_newbatch = 2;
        let ret_samebatch = 3;

        match self.plan_type {
            _ => {
                self.current_n_tuples += n_tuples;
                let start_new_batch = (self.current_n_tuples >= self.mu_hyp_opt * self.optimizer_plan_rows) && (accumulated_us >= self.mu_hyp_time);
                if start_new_batch {
                    self.time_us.push(accumulated_us);
                    self.batch_sizes.push(self.current_n_tuples);
                    self.current_n_tuples = 0.0;
                    if self.try_stop() {
                        ret_stop
                    } else {
                        ret_newbatch
                    }
                } else {
                    ret_samebatch
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct RuntimeState {
    operators: HashMap<(i32, i32), OperatorState>,
}

#[no_mangle]
pub extern "C" fn runtime_state_new() -> *mut RuntimeState {
    Box::into_raw(Box::new(RuntimeState { operators: HashMap::new() }))
}

#[no_mangle]
pub extern "C" fn runtime_state_free(ptr: *mut RuntimeState) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let _ = Box::from_raw(ptr);
    }
}

#[no_mangle]
pub extern "C" fn runtime_init_operator(
    runtime_state: *mut RuntimeState,
    operator_id: i32,
    parallel_worker_number: i32,
    plan_type: i32,
    optimizer_startup_cost: f64,
    optimizer_total_cost: f64,
    optimizer_plan_rows: f64,
    optimizer_plan_width: f64,
    mu_hyp_opt: f64,
    mu_hyp_time: f64,
    mu_hyp_stdev: f64,
) {
    let state = unsafe { &mut *runtime_state };
    state.operators.insert((operator_id, parallel_worker_number), OperatorState::new());
    let op = state.operators.get_mut(&(operator_id, parallel_worker_number)).unwrap();
    op.plan_type = plan_type;
    op.optimizer_startup_cost = optimizer_startup_cost;
    op.optimizer_total_cost = optimizer_total_cost;
    op.optimizer_plan_rows = optimizer_plan_rows;
    op.optimizer_plan_width = optimizer_plan_width;
    op.mu_hyp_opt = mu_hyp_opt;
    op.mu_hyp_time = mu_hyp_time;
    op.mu_hyp_stdev = mu_hyp_stdev;
}

#[no_mangle]
pub extern "C" fn runtime_add_tuple_batch(
    runtime_state: *mut RuntimeState,
    operator_id: i32, parallel_worker_number: i32,
    n_tuples: f64, accumulated_us: f64,
) -> i32 {
    let state = unsafe { &mut *runtime_state };
    let op = state.operators.get_mut(&(operator_id, parallel_worker_number)).unwrap();
    op.add_tuple_batch(n_tuples, accumulated_us)
}


pub struct CacheResult {
    len: i32,
    bytes: Vec<u8>,
}

impl CacheResult {
    fn new(len: i32, bytes: Vec<u8>) -> CacheResult {
        CacheResult {
            len,
            bytes,
        }
    }
}

#[no_mangle]
pub extern "C" fn cache_result_len(ptr: *mut CacheResult) -> i32 {
    unsafe { (*ptr).len }
}

#[no_mangle]
pub extern "C" fn cache_result_bytes(ptr: *mut CacheResult) -> *const u8 {
    unsafe { (*ptr).bytes.as_ptr() }
}

#[no_mangle]
pub extern "C" fn cache_result_free(ptr: *mut CacheResult) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let _ = Box::from_raw(ptr);
    }
}

#[no_mangle]
pub extern "C" fn cache_get_explain(redis_con: *mut RedisCon, intelligent: bool, query_text: *const c_char) -> *mut CacheResult {
    let now = std::time::Instant::now();
    let query = unsafe { CStr::from_ptr(query_text) }.to_str().unwrap();
    let redis_con = unsafe { &mut *redis_con };
    let con = &mut redis_con.con;

    if intelligent {
        let should_rerun = match redis_con.stats.get_mut(query) {
            None => { return ptr::null_mut(); }
            Some(entry) => {
                entry.should_rerun()
            }
        };
        if should_rerun {
            return ptr::null_mut();
        }
    }

    let val: Option<String> = redis::cmd("GET").arg(query).query(con).unwrap();
    match val {
        None => { return ptr::null_mut(); }
        Some(s) => {
            let mut v: serde_json::Value = serde_json::from_str(&s).unwrap();
            if let serde_json::Value::Array(ref mut veccy) = v {
                if let serde_json::Value::Object(ref mut map) = veccy[0] {
                    map.insert("Bytejack".into(), serde_json::Value::String("true".into()));
                    map.insert("Execution Time".into(), serde_json::Value::Number((now.elapsed().as_millis() as u64).into()));
                    map.insert("Planning Time".into(), serde_json::Value::Number(0.into()));
                }
            }
            let mut s = serde_json::to_vec(&v).unwrap();
            s.push(0);
            Box::into_raw(Box::new(CacheResult::new(s.len() as i32, s)))
        }
    }
}

#[no_mangle]
pub extern "C" fn cache_append_explain(redis_con: *mut RedisCon, intelligent: bool, query_text: *const c_char, explain_text: *const c_char) {
    let query = unsafe { CStr::from_ptr(query_text) }.to_str().unwrap();
    let explain = unsafe { CStr::from_ptr(explain_text) }.to_str().unwrap();
    let redis_con = unsafe { &mut (*redis_con) };
    let con = &mut redis_con.con;
    redis::cmd("SET").arg(query).arg(explain).execute(con);

    if intelligent {
        let mut v: serde_json::Value = serde_json::from_str(explain).unwrap();
        if let serde_json::Value::Array(ref mut veccy) = v {
            if let serde_json::Value::Object(ref mut map) = veccy[0] {
                let time_plan = map.get("Planning Time".into()).unwrap().as_f64().unwrap();
                let time_exec = map.get("Execution Time".into()).unwrap().as_f64().unwrap();
                if !redis_con.stats.contains_key(query) {
                    redis_con.stats.insert(query.into(), RedisCacheEntry::new());
                }
                redis_con.stats.get_mut(query).unwrap().add_latest(time_plan + time_exec);
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn cache_clear(redis_con: *mut RedisCon) {
    if redis_con.is_null() {
        return;
    }
    let con = unsafe { &mut (*redis_con).con };
    redis::cmd("FLUSHALL").execute(con);
}

#[no_mangle]
pub extern "C" fn cache_save(redis_con: *mut RedisCon, dbname: *const c_char) {
    if redis_con.is_null() {
        return;
    }
    let con = unsafe { &mut (*redis_con).con };
    redis::cmd("SAVE").execute(con);
    let dbname = unsafe { CStr::from_ptr(dbname) }.to_str().unwrap();
    std::fs::rename("/var/lib/redis/dump.rdb", format!("/var/lib/redis/{}.rdb", dbname)).unwrap();
}
