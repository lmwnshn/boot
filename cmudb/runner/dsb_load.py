import os
from pathlib import Path

from sqlalchemy import Connection, Engine, create_engine
from util import conn_execute, connstr, sql_file_execute, vacuum_full_analyze_all


def loaded(conn: Connection):
    res = conn_execute(
        conn,
        "SELECT * FROM pg_indexes WHERE indexname = '_dta_index_customer_5_949578421__k13_k5'",
    ).fetchall()
    return len(res) > 0


def load(conn: Connection):
    schema_root = Path(os.getenv("DSB_SCHEMA_ROOT"))
    data_root = Path(os.getenv("DSB_DATA_ROOT"))
    dsb_sf = int(os.getenv("DSB_SF"))

    tables = [
        "dbgen_version",
        "customer_address",
        "customer_demographics",
        "date_dim",
        "warehouse",
        "ship_mode",
        "time_dim",
        "reason",
        "income_band",
        "item",
        "store",
        "call_center",
        "customer",
        "web_site",
        "store_returns",
        "household_demographics",
        "web_page",
        "promotion",
        "catalog_page",
        "inventory",
        "catalog_returns",
        "web_returns",
        "web_sales",
        "catalog_sales",
        "store_sales",
    ]

    sql_file_execute(conn, schema_root / "create_tables.sql")
    for table in tables:
        conn_execute(conn, f"TRUNCATE {table} CASCADE")
    for table in tables:
        table_path = data_root / f"sf_{dsb_sf}" / f"{table}.dat"
        conn_execute(conn, f"COPY {table} FROM '{str(table_path)}' CSV DELIMITER '|'")
    sql_file_execute(conn, schema_root / "dsb_index_pg.sql")


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
