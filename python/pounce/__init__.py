"""pounce — Arrow-native SQL Server driver for Python.

Zero-copy Arrow transport over TDS. No ODBC. No driver manager.

    import pounce

    # DB-API
    conn = pounce.connect("Server=localhost,1433;UID=sa;PWD=secret")

    # Pandas
    df = pounce.read_sql("SELECT * FROM t", conn=conn)
    pounce.to_sql(df, "t2", conn=conn, if_exists="replace")

    # Polars
    df = pounce.read_polars("SELECT * FROM t", conn=conn)
    pounce.polars_to_sql(df, "t2", conn=conn)

    # Ingest
    pounce.ingest_csv("data.csv", "t", conn=conn)
    pounce.export_parquet("SELECT * FROM t", "out.parquet", conn=conn)

Part of the CopyCat ecosystem: https://github.com/copycatdb
"""

from pounce.dbapi import connect, Connection, Cursor
from pounce.pandas_support import read_sql, to_sql
from pounce.polars_support import read_polars, polars_to_sql
from pounce.ingest import ingest_csv, ingest_parquet, export_csv, export_parquet

__version__ = "0.1.0"
__all__ = [
    "connect", "Connection", "Cursor",
    "read_sql", "to_sql",
    "read_polars", "polars_to_sql",
    "ingest_csv", "ingest_parquet", "export_csv", "export_parquet",
]
