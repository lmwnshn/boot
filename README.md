# VLDB 2024: Boot

2024-09-09: This repository is a proof-of-concept extracted from the [Database Gym](https://github.com/cmu-db/dbgym) project.
If you're looking to use Boot, especially for PostgreSQL, check that out instead.

---

Source code for the Boot framework in "Hit the Gym: Accelerating Query Execution to Efficiently Bootstrap
Behavior Models for Self-Driving Database Management Systems", appearing in VLDB 2024.

## Setup

__Read the setup script before running it! It may break your system!__

We assume Ubuntu 22.04.

1. Run `./cmudb/setup/dependencies.sh`. This sets up:
    - Python3.10 venv
    - Rust
    - Redis
2. Perform standard system setup to reduce noise.
   - e.g., [sled experiment checklist](https://sled.rs/perf.html#experiment-checklist)

## Gather Data

1. Use [PGTune](https://pgtune.leopard.in.ua/) to generate a reasonable PostgreSQL configuration for your system 
   and replace the contents of `./cmudb/env/default.pgtune.auto.conf`.
   Otherwise, it defaults to a laptop configuration.
2. Use the virtual environment that was set up above: `source ./venv/bin/activate`.
3. Invoke `./cmudb/runner/run.sh` to run a smoke-test version of all the experiments in the paper.
   - Example changes: SF 1 instead of SF 100, 10 TPC-H seeds instead of 1000 TPC-H seeds, ...
   - Artifacts for each workload (e.g., raw data, models) are stored in the corresponding `./artifact_workload` folder.
   - For our example configuration `./cmudb/env/dev8.pgtune.auto.conf`, the smoke-test takes around a day to generate
     data.
4. If you want to run the full set of experiments, run `FULL_RUN=true ./cmudb/runner/run.sh`.
   - This may take many months on standard server hardware (see `./cmudb/env/dev8.pgtune.auto.conf`).
     That's why this research paper exists!
   - If you have multiple machines with identical specifications, you may modify the script to distribute the work
     across them. Make sure that for a given Boot configuration and workload, the queries are executed uninterrupted
     and on the same machine. Otherwise, the Macro-Accelerator may not have the right state.

## Plot Results

1. Invoking `./cmudb/runner/run.sh` produces an `artifact.tgz`. Copy and extract this to some folder `foo`.
2. In the first cell of `./cmudb/runner/analysis.ipynb`, set the path `ARTIFACTS_TGZ_ROOT` to `foo`.
3. If you ran the full set of experiments, also set `SMOKE_TEST = False` in the same cell. 
   Otherwise, the plots will be different from the paper as you did not generate the necessary data.
   For example, the plot scripts will substitute both SF100 and SF10 with SF1.
4. Run the notebook to generate all plots. The default save location is `foo/plot_camera_ready`.
