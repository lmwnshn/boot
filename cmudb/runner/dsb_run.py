import os
import time
import traceback
from pathlib import Path
from typing import Optional

import psycopg.errors
import sqlalchemy.exc
from dsb_rewriter import *
from sqlalchemy import Connection, Engine, NullPool, create_engine
from tqdm import tqdm
from util import (conn_execute, connstr, prewarm_all, sql_file_queries,
                  vacuum_analyze_all)


class Config:
    def __init__(
        self,
        expt_name: str,
        timeout_s: int = 60 * 5,
        rewriter: Optional[Rewriter] = None,
        before_sql: Optional[list[str]] = None,
        after_sql: Optional[list[str]] = None,
    ):
        self.expt_name = expt_name
        self.timeout_s = timeout_s
        self.rewriter = rewriter if rewriter is not None else EARewriter()
        self.before_sql = before_sql if before_sql is not None else []
        self.after_sql = after_sql if after_sql is not None else []


def dsb(engine: Engine, conn: Connection, config: Config, verbose=False):
    hostname = os.getenv("HOSTNAME")
    artifact_root = Path(os.getenv("ARTIFACT_ROOT"))
    query_root = Path(os.getenv("DSB_QUERY_ROOT"))
    train_seed = int(os.getenv("DSB_QUERY_TRAIN_SEED"))
    test_seed = int(os.getenv("DSB_QUERY_TEST_SEED"))
    dsb_sf = int(os.getenv("DSB_SF"))

    seed_timeouts = {}
    if dsb_sf > 1:
        for seed in [train_seed, test_seed]:
            seed_timeouts[seed] = set()
            with open(
                Path(os.getenv("ROOT_DIR"))
                / "cmudb"
                / "env"
                / f"{hostname}_dsb_sf1_timeout_{seed}.txt"
            ) as f:
                for line in f:
                    seed_timeouts[seed].add(str(Path(line).stem))
            with open(
                Path(os.getenv("ROOT_DIR"))
                / "cmudb"
                / "env"
                / f"{hostname}_dsb_sf1_runtime_{seed}.txt"
            ) as f:
                for line in f:
                    filename, _, runtime_ms = line.split(":")
                    # Assume linear scaling.
                    scaled_runtime_s = (float(runtime_ms) / 1000) * dsb_sf
                    if scaled_runtime_s >= config.timeout_s:
                        seed_timeouts[seed].add(str(Path(filename).stem))

    readied = False
    executed_once = False

    seeds = [train_seed]
    if config.expt_name == "default":
        seeds.append(test_seed)

    for seed in tqdm(seeds, desc=f"{config.expt_name} {dsb_sf} DSB seeds.", leave=None):
        outdir = (
            artifact_root
            / "experiment"
            / config.expt_name
            / "dsb"
            / f"sf_{dsb_sf}"
            / "default"
            / str(seed)
        )
        outdir.mkdir(parents=True, exist_ok=True)

        for query_path in tqdm(
            sorted(
                (query_root / "default" / str(seed)).glob("*.sql"),
                key=lambda s: str(s).split("-")[0],
            ),
            desc=f"{config.expt_name} {dsb_sf} DSB query.",
            leave=None,
        ):
            query_id = str(query_path.stem)
            for query_subnum, query in enumerate(
                sql_file_queries(query_path.absolute()), 1
            ):
                outpath_ok = outdir / f"{query_path.stem}-{query_subnum}.ok"
                outpath_res = outdir / f"{query_path.stem}-{query_subnum}.res"
                outpath_err = outdir / f"{query_path.stem}-{query_subnum}.err"
                outpath_timeout = outdir / f"{query_path.stem}-{query_subnum}.timeout"

                if outpath_err.exists():
                    continue

                if outpath_ok.exists():
                    continue

                if (
                    seed in seed_timeouts
                    and f"{query_path.stem}-{query_subnum}" in seed_timeouts[seed]
                ):
                    if config.expt_name == "default":
                        outpath_timeout.touch(exist_ok=True)
                        outpath_ok.touch(exist_ok=True)
                        continue

                try:
                    if not readied:
                        conn_execute(
                            conn, f"SET statement_timeout = '0s'", verbose=verbose
                        )
                        prewarm_all(engine, conn, verbose=verbose)
                        vacuum_analyze_all(engine, conn, verbose=verbose)
                        for sql in config.before_sql:
                            conn_execute(conn, sql, verbose=verbose)
                        conn_execute(
                            conn,
                            f"SET statement_timeout = '{config.timeout_s}s'",
                            verbose=verbose,
                        )
                        readied = True
                        executed_once = True

                    with open(outpath_res, "w") as output_file:
                        query, is_ea = config.rewriter.rewrite(
                            query_id, query_subnum, query
                        )
                        result = conn_execute(conn, query, verbose=False)
                        if is_ea:
                            ea_result = str(result.fetchone()[0][0])
                            print(ea_result, file=output_file)
                        outpath_ok.touch(exist_ok=True)

                except sqlalchemy.exc.OperationalError as e:
                    if isinstance(e.orig, psycopg.errors.QueryCanceled):
                        # Since DSB's data distribution varies, timeouts may not be shared.
                        outpath_timeout.touch(exist_ok=True)
                        outpath_ok.touch(exist_ok=True)
                    else:
                        raise e

                except sqlalchemy.exc.DataError as e:
                    if isinstance(e.orig, psycopg.errors.DivisionByZero):
                        with open(outpath_err, "w") as error_file:
                            traceback.print_exception(e, file=error_file)
                        outpath_ok.touch(exist_ok=True)
                    else:
                        raise e

    if executed_once:
        for sql in config.after_sql:
            conn_execute(conn, sql, verbose=verbose)


def main():
    engine: Engine = create_engine(
        connstr(),
        poolclass=NullPool,
        execution_options={"isolation_level": "AUTOCOMMIT"},
    )

    def make_bytejack_config(
        enable=False,
        intercept_explain_analyze=False,
        intelligent_cache=False,
        early_stop=False,
        seq_sample=False,
        seq_sample_pct=100,
        seq_sample_seed=15721,
    ):
        name = "_".join(
            [
                "bytejack",
                f"e{int(enable)}",
                f"iea{int(intercept_explain_analyze)}",
                f"ic{int(intelligent_cache)}",
                f"es{int(early_stop)}",
                f"ss{int(seq_sample)}",
                f"ssp{int(seq_sample_pct)}",
                f"sss{int(seq_sample_seed)}",
            ]
        )
        return Config(
            expt_name=name,
            before_sql=[
                "DROP EXTENSION IF EXISTS bytejack",
                "CREATE EXTENSION IF NOT EXISTS bytejack",
                "SELECT bytejack_connect()",
                "SELECT bytejack_cache_clear()",
                f"SET bytejack.enable={enable}",
                f"SET bytejack.intercept_explain_analyze={intercept_explain_analyze}",
                f"SET bytejack.intelligent_cache={intelligent_cache}",
                f"SET bytejack.early_stop={early_stop}",
                f"SET bytejack.seq_sample={seq_sample}",
                f"SET bytejack.seq_sample_pct={seq_sample_pct}",
                f"SET bytejack.seq_sample_seed={seq_sample_seed}",
            ],
            after_sql=[
                f"SELECT bytejack_save('{name}')",
                "SELECT bytejack_disconnect()",
            ],
        )

    configs = [
        Config(expt_name="default"),
        # fmt: off
        # Kitchen sink.
        make_bytejack_config(enable=True, intercept_explain_analyze=True, intelligent_cache=True, early_stop=True,
                             seq_sample=True, seq_sample_pct=10, seq_sample_seed=15721),
        # IC + ES.
        make_bytejack_config(enable=True, intercept_explain_analyze=True, intelligent_cache=True, early_stop=True),
        # IC + SS.
        make_bytejack_config(enable=True, intercept_explain_analyze=True, intelligent_cache=True,
                             seq_sample=True, seq_sample_pct=10, seq_sample_seed=15721),
        # C.
        make_bytejack_config(enable=True, intercept_explain_analyze=True),
        # IC.
        make_bytejack_config(enable=True, intercept_explain_analyze=True, intelligent_cache=True),
        # ES.
        make_bytejack_config(enable=True, early_stop=True),
        # SS.
        make_bytejack_config(enable=True, seq_sample=True, seq_sample_pct=10, seq_sample_seed=15721),
        # fmt: on
    ]

    dsb_sf = int(os.getenv("DSB_SF"))
    if dsb_sf == 10:
        for ssseed in [15721, 15722, 15723]:
            for pct in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
                configs.append(
                    make_bytejack_config(enable=True, intercept_explain_analyze=True, intelligent_cache=True,
                                         early_stop=True, seq_sample=True, seq_sample_pct=pct, seq_sample_seed=ssseed),
                )

    pbar = tqdm(range(len(configs)), desc="Configs.", leave=None)
    for config in configs:
        time.time()
        pbar.set_description(f"Config: {config.expt_name} {connstr()}")
        try:
            with engine.connect() as conn:
                conn_execute(conn, "DROP EXTENSION IF EXISTS bytejack", verbose=False)
                conn_execute(
                    conn, "CREATE EXTENSION IF NOT EXISTS bytejack", verbose=False
                )
                conn_execute(conn, "SET bytejack.enable=false", verbose=False)
                conn_execute(conn, "DROP EXTENSION IF EXISTS bytejack", verbose=False)
                dsb(engine, conn, config)
        except Exception:
            traceback.print_exc()
            print(f"ERROR FOR CONFIG: {config.expt_name}")
            pass
        pbar.update()
    pbar.close()


if __name__ == "__main__":
    main()
