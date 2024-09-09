# Technical Details

We made a few modifications to core PostgreSQL to make it easier to run on other machines.
However, these changes (which are less than 100 LOC) make it harder to install the extension by itself.

- The best way to obtain "Operator Time" is to use [TScout](https://dl.acm.org/doi/abs/10.1145/3514221.3517845).
  However, because this requires eBPF, we provide a simple (and higher overhead) userspace version in PostgreSQL.
- To avoid large diffs of copy-paste PostgreSQL source, we introduce a few new hooks.
- To avoid complicating the C-Rust FFI further, we embed a "stop" boolean flag into the `PlanState` struct directly
  instead of adding more hook logic.