import os
from pathlib import Path

from sqlalchemy import Connection, Engine, create_engine
from util import conn_execute, connstr, sql_file_execute, vacuum_full_analyze_all


def loaded(conn: Connection):
    res = conn_execute(
        conn, "SELECT * FROM pg_indexes WHERE indexname = 'role_id_cast_info'"
    ).fetchall()
    return len(res) > 0


def load(conn: Connection):
    schema_root = Path(os.getenv("JOB_SCHEMA_ROOT"))
    data_root = Path(os.getenv("JOB_DATA_ROOT"))

    tables = [
        "aka_name",
        "aka_title",
        "cast_info",
        "char_name",
        "comp_cast_type",
        "company_name",
        "company_type",
        "complete_cast",
        "info_type",
        "keyword",
        "kind_type",
        "link_type",
        "movie_companies",
        "movie_info",
        "movie_info_idx",
        "movie_keyword",
        "movie_link",
        "name",
        "person_info",
        "role_type",
        "title",
    ]

    sql_file_execute(conn, schema_root / "schema.sql")
    for table in tables:
        conn_execute(conn, f"TRUNCATE {table} CASCADE")
    for table in tables:
        table_path = data_root / f"{table}.csv"
        conn_execute(
            conn,
            f"COPY {table} FROM '{str(table_path)}' CSV DELIMITER ',' QUOTE '\"' ESCAPE '\\'",
        )
    sql_file_execute(conn, schema_root / "fkindexes.sql")


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
