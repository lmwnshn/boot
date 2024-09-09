import json
import os
import time
import traceback
from pathlib import Path
from typing import Optional

import psycopg.errors
import sqlalchemy.exc
from sqlalchemy import Connection, Engine, NullPool, create_engine
from tpch_rewriter import *
from tqdm import tqdm, trange
from util import (
    conn_execute,
    connstr,
    prewarm_all,
    sql_file_queries,
    vacuum_analyze_all,
)


class Skipper:
    def __init__(self):
        self.num_skips = 0
        self.next_num_skips = 1

    def should_skip(self):
        return self.num_skips > 0

    def record_skip(self):
        if self.num_skips == 0:
            self.num_skips = self.next_num_skips
            self.next_num_skips *= 2
            return
        self.num_skips -= 1

    def reset(self):
        self.num_skips = 0
        self.next_num_skips = 1


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


def tpch(engine: Engine, conn: Connection, config: Config, verbose=False):
    artifact_root = Path(os.getenv("ARTIFACT_ROOT"))
    query_root = Path(os.getenv("TPCH_QUERY_ROOT"))
    query_start = int(os.getenv("TPCH_QUERY_START"))
    query_stop = int(os.getenv("TPCH_QUERY_STOP"))
    tpch_sf = int(os.getenv("TPCH_SF"))

    readied = False
    executed_once = False
    timeout_skippers = {}

    for seed in trange(
        query_start, query_stop + 1, desc=f"{config.expt_name} TPCH seed.", leave=None
    ):
        outdir = (
            artifact_root
            / "experiment"
            / config.expt_name
            / "tpch"
            / f"sf_{tpch_sf}"
            / str(seed)
        )
        outdir.mkdir(parents=True, exist_ok=True)

        for query_path in tqdm(
            [(query_root / str(seed) / f"{i}.sql") for i in range(1, 22 + 1)],
            desc=f"{config.expt_name} TPCH query.",
            leave=None,
        ):
            query_num = int(query_path.stem)
            for query_subnum, query in enumerate(
                sql_file_queries(query_path.absolute()), 1
            ):
                outpath_ok = outdir / f"{query_path.stem}-{query_subnum}.ok"
                outpath_res = outdir / f"{query_path.stem}-{query_subnum}.res"
                outpath_timeout = outdir / f"{query_path.stem}-{query_subnum}.timeout"

                timeout_key = (query_num, query_subnum)

                if outpath_timeout.exists():
                    # Simulate having skipped a query.
                    if timeout_key not in timeout_skippers:
                        timeout_skippers[timeout_key] = Skipper()
                    timeout_skippers[timeout_key].record_skip()

                if outpath_ok.exists():
                    continue

                if config.expt_name != "default":
                    if timeout_key in timeout_skippers:
                        if timeout_skippers[timeout_key].should_skip():
                            outpath_timeout.touch(exist_ok=True)
                            outpath_ok.touch(exist_ok=True)
                            timeout_skippers[timeout_key].record_skip()
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
                        if (query_num, query_subnum) == (15, 1):
                            conn_execute(
                                conn, "DROP VIEW IF EXISTS revenue0", verbose=False
                            )

                        query, is_ea = config.rewriter.rewrite(
                            query_num, query_subnum, query
                        )

                        result = conn_execute(conn, query, verbose=False)
                        if is_ea:
                            ea_result = str(result.fetchone()[0][0])
                            print(ea_result, file=output_file)
                        outpath_ok.touch(exist_ok=True)

                except sqlalchemy.exc.OperationalError as e:
                    if isinstance(e.orig, psycopg.errors.QueryCanceled):
                        if timeout_key not in timeout_skippers:
                            timeout_skippers[timeout_key] = Skipper()
                        outpath_timeout.touch(exist_ok=True)
                        outpath_ok.touch(exist_ok=True)
                        timeout_skippers[timeout_key].record_skip()
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

    def make_boot_config(
        enable=False,
        intercept_explain_analyze=False,
        intelligent_cache=False,
        early_stop=False,
        seq_sample=False,
        seq_sample_pct=100,
        seq_sample_seed=15721,
        macro_mode=0,
    ):
        name = "_".join(
            [
                "boot",
                f"e{int(enable)}",
                f"iea{int(intercept_explain_analyze)}",
                f"ic{int(intelligent_cache)}",
                f"es{int(early_stop)}",
                f"ss{int(seq_sample)}",
                f"ssp{int(seq_sample_pct)}",
                f"sss{int(seq_sample_seed)}",
            ]
        )
        if macro_mode != 0:
            name = f"{name}_mm{int(macro_mode)}"
        return Config(
            expt_name=name,
            before_sql=[
                "DROP EXTENSION IF EXISTS boot",
                "CREATE EXTENSION IF NOT EXISTS boot",
                "SELECT boot_connect()",
                "SELECT boot_cache_clear()",
                f"SET boot.enable={enable}",
                f"SET boot.intercept_explain_analyze={intercept_explain_analyze}",
                f"SET boot.intelligent_cache={intelligent_cache}",
                f"SET boot.early_stop={early_stop}",
                f"SET boot.seq_sample={seq_sample}",
                f"SET boot.seq_sample_pct={seq_sample_pct}",
                f"SET boot.seq_sample_seed={seq_sample_seed}",
                f"SET boot.macro_mode={macro_mode}",
            ],
            after_sql=[
                f"SELECT boot_save('{name}')",
                "SELECT boot_disconnect()",
            ],
        )

    configs = [
        Config(expt_name="default"),
        Config(
            expt_name="nts_b10_r15721",
            rewriter=NTSRewriter("BERNOULLI (10)", "REPEATABLE (15721)"),
        ),
        Config(
            expt_name="sts_b10_r15721",
            rewriter=STSRewriter("BERNOULLI (10)", "REPEATABLE (15721)"),
        ),
        # fmt: off
        # Kitchen sink.
        make_boot_config(enable=True, intercept_explain_analyze=True, intelligent_cache=True, early_stop=True,
                         seq_sample=True, seq_sample_pct=10, seq_sample_seed=15721),
        # IC + ES.
        make_boot_config(enable=True, intercept_explain_analyze=True, intelligent_cache=True, early_stop=True),
        # IC + SS.
        make_boot_config(enable=True, intercept_explain_analyze=True, intelligent_cache=True,
                         seq_sample=True, seq_sample_pct=10, seq_sample_seed=15721),
        make_boot_config(enable=True, intercept_explain_analyze=True, intelligent_cache=True,
                         seq_sample=True, seq_sample_pct=50, seq_sample_seed=15721),
        # C.
        make_boot_config(enable=True, intercept_explain_analyze=True),
        # IC.
        make_boot_config(enable=True, intercept_explain_analyze=True, intelligent_cache=True),
        # IC-P.
        make_boot_config(enable=True, intercept_explain_analyze=True, intelligent_cache=True, macro_mode=1),
        # ES.
        make_boot_config(enable=True, early_stop=True),
        # SS.
        make_boot_config(enable=True, seq_sample=True, seq_sample_pct=10, seq_sample_seed=15721),
        make_boot_config(enable=True, seq_sample=True, seq_sample_pct=50, seq_sample_seed=15721),
        # fmt: on
    ]

    tpch_sf = int(os.getenv("TPCH_SF"))
    for ssseed in [15721, 15722, 15723]:
        for pct in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
            configs.append(
                make_boot_config(
                    enable=True,
                    intercept_explain_analyze=True,
                    intelligent_cache=True,
                    early_stop=True,
                    seq_sample=True,
                    seq_sample_pct=pct,
                    seq_sample_seed=ssseed,
                ),
            )

    mu_hyp = os.getenv("TPCH_MU")
    if mu_hyp is not None and int(mu_hyp) == 1:
        for mu_hyp_opt in [0.01, 0.05, 0.1]:
            for mu_hyp_time in [1e5, 5e5, 1e6]:
                for mu_hyp_stdev in [1.0, 2.0, 3.0]:
                    config = make_boot_config(
                        enable=True,
                        early_stop=True,
                    )
                    config.expt_name += "_" + "_".join(
                        [
                            f"muhypopt_{mu_hyp_opt}",
                            f"muhyptime_{mu_hyp_time}",
                            f"muhypstdev_{mu_hyp_stdev}",
                        ]
                    )
                    config.before_sql += [
                        f"SET boot.mu_hyp_opt={mu_hyp_opt}",
                        f"SET boot.mu_hyp_time={mu_hyp_time}",
                        f"SET boot.mu_hyp_stdev={mu_hyp_stdev}",
                    ]
                    configs.append(config)

    pbar = tqdm(range(len(configs)), desc="Configs.", leave=None)
    for config in configs:
        time.time()
        pbar.set_description(f"Config: {config.expt_name} {connstr()}")
        try:
            with engine.connect() as conn:
                conn_execute(conn, "DROP EXTENSION IF EXISTS boot", verbose=False)
                conn_execute(conn, "CREATE EXTENSION IF NOT EXISTS boot", verbose=False)
                conn_execute(conn, "SET boot.enable=false", verbose=False)
                conn_execute(conn, "DROP EXTENSION IF EXISTS boot", verbose=False)
                tpch(engine, conn, config)
        except Exception:
            traceback.print_exc()
            print(f"ERROR FOR CONFIG: {config.expt_name}")
            pass
        pbar.update()
    pbar.close()


if __name__ == "__main__":
    main()
