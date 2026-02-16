"""Pandas integration for pounce — read/write DataFrames via Arrow.

Examples::

    import pounce

    df = pounce.read_sql("SELECT * FROM sales", conn_str="Server=localhost,1433;UID=sa;PWD=secret")
    pounce.to_sql(df, "target_table", conn_str="Server=localhost,1433;UID=sa;PWD=secret", if_exists="replace")
"""

from __future__ import annotations

import pyarrow as pa

from pounce.dbapi import connect, Connection


def _get_connection(conn=None, conn_str=None) -> tuple[Connection, bool]:
    """Return (connection, should_close) from either an existing conn or a conn_str."""
    if conn is not None:
        return conn, False
    if conn_str is not None:
        return connect(conn_str), True
    raise ValueError("Must provide either conn (Connection) or conn_str (str)")


def read_sql(
    query: str,
    conn: Connection | None = None,
    conn_str: str | None = None,
    params=None,
) -> "pandas.DataFrame":
    """Execute a SQL query and return results as a Pandas DataFrame.

    Uses Arrow as the intermediate format for near-zero-copy transfer.

    Parameters
    ----------
    query : str
        SQL query to execute.
    conn : Connection, optional
        An existing pounce connection.
    conn_str : str, optional
        Connection string (creates a temporary connection).
    params : sequence, optional
        Query parameters for ``?`` placeholders.

    Returns
    -------
    pandas.DataFrame
    """
    c, should_close = _get_connection(conn, conn_str)
    try:
        cur = c.cursor()
        cur.execute(query, params)
        table = cur.fetch_arrow_table()
        return table.to_pandas()
    finally:
        if should_close:
            c.close()


def to_sql(
    df: "pandas.DataFrame",
    table_name: str,
    conn: Connection | None = None,
    conn_str: str | None = None,
    if_exists: str = "fail",
    chunksize: int | None = None,
    dtype: dict | None = None,
) -> int:
    """Write a Pandas DataFrame to a SQL Server table via Arrow bulk load.

    Parameters
    ----------
    df : pandas.DataFrame
        Data to write.
    table_name : str
        Target table name.
    conn : Connection, optional
        An existing pounce connection.
    conn_str : str, optional
        Connection string (creates a temporary connection).
    if_exists : str
        ``"fail"`` — error if table exists.
        ``"replace"`` — drop and recreate.
        ``"append"`` — insert into existing table.
    chunksize : int, optional
        If set, write in batches of this size.
    dtype : dict, optional
        Column name → pyarrow type overrides for the Arrow conversion.

    Returns
    -------
    int
        Total number of rows written.
    """
    mode_map = {
        "fail": "create",
        "replace": "replace",
        "append": "append",
    }
    if if_exists not in mode_map:
        raise ValueError(f"if_exists must be one of {list(mode_map.keys())}, got {if_exists!r}")
    mode = mode_map[if_exists]

    c, should_close = _get_connection(conn, conn_str)
    try:
        # Convert DataFrame → Arrow Table
        if dtype:
            schema_overrides = []
            for col in df.columns:
                if col in dtype:
                    schema_overrides.append(pa.field(col, dtype[col]))
                else:
                    schema_overrides.append(pa.field(col, pa.Array.from_pandas(df[col]).type))
            arrow_schema = pa.schema(schema_overrides)
            arrow_table = pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False)
        else:
            arrow_table = pa.Table.from_pandas(df, preserve_index=False)

        total = 0
        cur = c.cursor()

        if chunksize and chunksize < len(df):
            for start in range(0, arrow_table.num_rows, chunksize):
                chunk = arrow_table.slice(start, chunksize)
                # First chunk uses the requested mode, subsequent chunks append
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
