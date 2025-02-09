"""Utility functions to simplify DuckDB interactions."""

import duckdb
from os import getenv
from fsspec import filesystem


def init_duckdb_gcs() -> duckdb.connect:
    con = duckdb.connect(":memory:")
    con.register_filesystem(filesystem("gcs"))
    return con
