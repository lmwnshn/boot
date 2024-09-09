import os
from pathlib import Path

from sqlalchemy import Connection, Engine, create_engine
from util import conn_execute, connstr, sql_file_execute, vacuum_full_analyze_all


def loaded(conn: Connection):
    res = conn_execute(
        conn, "SELECT * FROM pg_indexes WHERE indexname = 'l_sk_pk'"
    ).fetchall()
    return len(res) > 0


def load(conn: Connection):
    schema_root = Path(os.getenv("TPCH_SCHEMA_ROOT"))
    data_root = Path(os.getenv("TPCH_DATA_ROOT"))
    tpch_sf = int(os.getenv("TPCH_SF"))

    tables = [
        "region",
        "nation",
        "part",
        "supplier",
        "partsupp",
        "customer",
        "orders",
        "lineitem",
    ]

    sql_file_execute(conn, schema_root / "tpch_schema.sql")
    for table in tables:
        conn_execute(conn, f"TRUNCATE {table} CASCADE")
    for table in tables:
        table_path = data_root / f"sf_{tpch_sf}" / f"{table}.tbl"
        conn_execute(conn, f"COPY {table} FROM '{str(table_path)}' CSV DELIMITER '|'")
    sql_file_execute(conn, schema_root / "tpch_constraints.sql")


def main():
    engine: Engine = create_engine(
        connstr(), execution_options={"isolation_level": "AUTOCOMMIT"}
    )
    with engine.connect() as conn:
        if not loaded(conn):
            load(conn)
            vacuum_full_analyze_all(engine, conn)


if __name__ == "__main__":
    main()
