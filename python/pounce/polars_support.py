"""Polars integration for pounce — read/write Polars DataFrames via Arrow.

Polars has native Arrow support, so ``pl.from_arrow()`` is genuinely zero-copy.

Examples::

    import pounce

    df = pounce.read_polars("SELECT * FROM sales", conn_str="...")
    pounce.polars_to_sql(df, "target_table", conn_str="...")
"""

from __future__ import annotations

import pyarrow as pa

from pounce.dbapi import connect, Connection


def _get_connection(conn=None, conn_str=None) -> tuple[Connection, bool]:
    if conn is not None:
        return conn, False
    if conn_str is not None:
        return connect(conn_str), True
    raise ValueError("Must provide either conn or conn_str")


def read_polars(
    query: str,
    conn: Connection | None = None,
    conn_str: str | None = None,
    params=None,
) -> "polars.DataFrame":
    """Execute a SQL query and return results as a Polars DataFrame.

    Uses Arrow as the intermediate format — genuinely zero-copy since
    Polars natively understands Arrow memory.

    Parameters
    ----------
    query : str
        SQL query to execute.
    conn : Connection, optional
        An existing pounce connection.
    conn_str : str, optional
        Connection string.
    params : sequence, optional
        Query parameters.

    Returns
    -------
    polars.DataFrame
    """
    import polars as pl

    c, should_close = _get_connection(conn, conn_str)
    try:
        cur = c.cursor()
        cur.execute(query, params)
        table = cur.fetch_arrow_table()
        return pl.from_arrow(table)
    finally:
        if should_close:
            c.close()


def polars_to_sql(
    df: "polars.DataFrame",
    table_name: str,
    conn: Connection | None = None,
    conn_str: str | None = None,
    if_exists: str = "fail",
    chunksize: int | None = None,
) -> int:
    """Write a Polars DataFrame to SQL Server via Arrow bulk load.

    Parameters
    ----------
    df : polars.DataFrame
        Data to write.
    table_name : str
        Target table name.
    conn : Connection, optional
        An existing pounce connection.
    conn_str : str, optional
        Connection string.
    if_exists : str
        ``"fail"``, ``"replace"``, or ``"append"``.
    chunksize : int, optional
        Write in batches of this size.

    Returns
    -------
    int
        Total rows written.
    """
    mode_map = {"fail": "create", "replace": "replace", "append": "append"}
    if if_exists not in mode_map:
        raise ValueError(f"if_exists must be one of {list(mode_map.keys())}")
    mode = mode_map[if_exists]

    c, should_close = _get_connection(conn, conn_str)
    try:
        arrow_table = df.to_arrow()
        total = 0
        cur = c.cursor()

        if chunksize and chunksize < arrow_table.num_rows:
            for start in range(0, arrow_table.num_rows, chunksize):
                chunk = arrow_table.slice(start, chunksize)
                chunk_mode = mode if start == 0 else "append"
                cur.adbc_ingest(table_name, chunk, mode=chunk_mode)
                total += cur.rowcount
        else:
            cur.adbc_ingest(table_name, arrow_table, mode=mode)
            total = cur.rowcount

        cur.close()
        return total
    finally:
        if should_close:
            c.close()
