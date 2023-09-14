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


def tpch(engine: Engine, conn: Connection, config: Config, verbose=False):
    artifact_root = Path(os.getenv("ARTIFACT_ROOT"))
    query_root = Path(os.getenv("TPCH_QUERY_ROOT"))
    query_start = int(os.getenv("TPCH_QUERY_START"))
    query_stop = int(os.getenv("TPCH_QUERY_STOP"))
    tpch_sf = int(os.getenv("TPCH_SF"))

    readied = False
    executed_once = False
    timeout_queries = []

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

                if outpath_timeout.exists():
                    timeout_queries.append((query_num, query_subnum))

                if outpath_ok.exists():
                    continue

                if (query_num, query_subnum) in timeout_queries:
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
                        timeout_queries.append((query_num, query_subnum))
                        outpath_timeout.touch(exist_ok=True)
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

    tpch_sf = int(os.getenv("TPCH_SF"))
    if tpch_sf == 100:
        for ssseed in [15721, 15722, 15723]:
            for pct in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
                configs.append(
                    make_bytejack_config(enable=True, intercept_explain_analyze=True, intelligent_cache=True,
                                         early_stop=True, seq_sample=True, seq_sample_pct=pct, seq_sample_seed=ssseed),
                )


    mu_hyp = os.getenv("TPCH_MU")
    if mu_hyp is not None and int(mu_hyp) == 1:
        for mu_hyp_opt in [0.01, 0.05, 0.1]:
            for mu_hyp_time in [1e5, 5e5, 1e6]:
                for mu_hyp_stdev in [1.0, 2.0, 3.0]:
                    config = make_bytejack_config(
                        enable=True, early_stop=True,
                    )
                    config.expt_name += ("_" + "_".join([
                        f"muhypopt_{mu_hyp_opt}",
                        f"muhyptime_{mu_hyp_time}",
                        f"muhypstdev_{mu_hyp_stdev}",
                    ]))
                    config.before_sql += [
                        f"SET bytejack.mu_hyp_opt={mu_hyp_opt}",
                        f"SET bytejack.mu_hyp_time={mu_hyp_time}",
                        f"SET bytejack.mu_hyp_stdev={mu_hyp_stdev}",
                    ]
                    configs.append(config)

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
                tpch(engine, conn, config)
        except Exception:
            traceback.print_exc()
            print(f"ERROR FOR CONFIG: {config.expt_name}")
            pass
        pbar.update()
    pbar.close()


if __name__ == "__main__":
    main()
