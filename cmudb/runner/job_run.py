import json
import os
import time
import traceback
from pathlib import Path
from typing import Optional

import psycopg.errors
import sqlalchemy.exc
from job_rewriter import *
from sqlalchemy import Connection, Engine, NullPool, create_engine
from tqdm import tqdm, trange
from util import (
    conn_execute,
    connstr,
    prewarm_all,
    sql_file_queries,
    vacuum_analyze_all,
)


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


def job(engine: Engine, conn: Connection, config: Config, verbose=False):
    artifact_root = Path(os.getenv("ARTIFACT_ROOT"))
    query_root = Path(os.getenv("JOB_QUERY_ROOT"))

    job_query_list = [
        "1a.sql",
        "1b.sql",
        "1c.sql",
        "1d.sql",
        "2a.sql",
        "2b.sql",
        "2c.sql",
        "2d.sql",
        "3a.sql",
        "3b.sql",
        "3c.sql",
        "4a.sql",
        "4b.sql",
        "4c.sql",
        "5a.sql",
        "5b.sql",
        "5c.sql",
        "6a.sql",
        "6b.sql",
        "6c.sql",
        "6d.sql",
        "6e.sql",
        "6f.sql",
        "7a.sql",
        "7b.sql",
        "7c.sql",
        "8a.sql",
        "8b.sql",
        "8c.sql",
        "8d.sql",
        "9a.sql",
        "9b.sql",
        "9c.sql",
        "9d.sql",
        "10a.sql",
        "10b.sql",
        "10c.sql",
        "11a.sql",
        "11b.sql",
        "11c.sql",
        "11d.sql",
        "12a.sql",
        "12b.sql",
        "12c.sql",
        "13a.sql",
        "13b.sql",
        "13c.sql",
        "13d.sql",
        "14a.sql",
        "14b.sql",
        "14c.sql",
        "15a.sql",
        "15b.sql",
        "15c.sql",
        "15d.sql",
        "16a.sql",
        "16b.sql",
        "16c.sql",
        "16d.sql",
        "17a.sql",
        "17b.sql",
        "17c.sql",
        "17d.sql",
        "17e.sql",
        "17f.sql",
        "18a.sql",
        "18b.sql",
        "18c.sql",
        "19a.sql",
        "19b.sql",
        "19c.sql",
        "19d.sql",
        "20a.sql",
        "20b.sql",
        "20c.sql",
        "21a.sql",
        "21b.sql",
        "21c.sql",
        "22a.sql",
        "22b.sql",
        "22c.sql",
        "22d.sql",
        "23a.sql",
        "23b.sql",
        "23c.sql",
        "24a.sql",
        "24b.sql",
        "25a.sql",
        "25b.sql",
        "25c.sql",
        "26a.sql",
        "26b.sql",
        "26c.sql",
        "27a.sql",
        "27b.sql",
        "27c.sql",
        "28a.sql",
        "28b.sql",
        "28c.sql",
        "29a.sql",
        "29b.sql",
        "29c.sql",
        "30a.sql",
        "30b.sql",
        "30c.sql",
        "31a.sql",
        "31b.sql",
        "31c.sql",
        "32a.sql",
        "32b.sql",
        "33a.sql",
        "33b.sql",
        "33c.sql",
    ]

    readied = False
    executed_once = False
    timeout_queries = []

    outdir = (
        artifact_root
        / "experiment"
        / config.expt_name
        / "job"
        / "sf_none"
        / "seed_none"
    )
    outdir.mkdir(parents=True, exist_ok=True)

    for query_path in tqdm(
        [(query_root / job_query) for job_query in job_query_list],
        desc=f"{config.expt_name} JOB query.",
        leave=None,
    ):
        query_name = query_path.stem
        for query_subnum, query in enumerate(
            sql_file_queries(query_path.absolute()), 1
        ):
            outpath_ok = outdir / f"{query_path.stem}-{query_subnum}.ok"
            outpath_res = outdir / f"{query_path.stem}-{query_subnum}.res"
            outpath_timeout = outdir / f"{query_path.stem}-{query_subnum}.timeout"

            if outpath_timeout.exists():
                timeout_queries.append((query_name, query_subnum))

            if outpath_ok.exists():
                continue

            if (query_name, query_subnum) in timeout_queries:
                outpath_timeout.touch(exist_ok=True)
                outpath_ok.touch(exist_ok=True)
                continue

            try:
                if not readied:
                    conn_execute(conn, f"SET statement_timeout = '0s'", verbose=verbose)
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
                        query_name, query_subnum, query
                    )

                    result = conn_execute(conn, query, verbose=False)
                    if is_ea:
                        ea_result = str(result.fetchone()[0][0])
                        print(ea_result, file=output_file)
                    outpath_ok.touch(exist_ok=True)

            except sqlalchemy.exc.OperationalError as e:
                if isinstance(e.orig, psycopg.errors.QueryCanceled):
                    timeout_queries.append((query_name, query_subnum))
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

    for ssseed in [
        15721,
        15722,
        15723,
        15724,
        15725,
        15726,
        15727,
        15728,
        15729,
        15730,
    ]:
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
                job(engine, conn, config)
        except Exception:
            traceback.print_exc()
            print(f"ERROR FOR CONFIG: {config.expt_name}")
            pass
        pbar.update()
    pbar.close()


if __name__ == "__main__":
    main()
