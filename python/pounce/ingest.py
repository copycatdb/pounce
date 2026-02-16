"""CSV/Parquet ingest and export helpers for pounce.

Examples::

    import pounce

    pounce.ingest_csv("data.csv", "target_table", conn_str="...")
    pounce.ingest_parquet("data.parquet", "target_table", conn_str="...")
    pounce.export_parquet("SELECT * FROM big_table", "output.parquet", conn_str="...")
    pounce.export_csv("SELECT * FROM big_table", "output.csv", conn_str="...")
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from pounce.dbapi import connect, Connection


def _get_connection(conn=None, conn_str=None) -> tuple[Connection, bool]:
    if conn is not None:
        return conn, False
    if conn_str is not None:
        return connect(conn_str), True
    raise ValueError("Must provide either conn or conn_str")


def _try_tqdm(iterable, **kwargs):
    """Wrap with tqdm if available, otherwise return as-is."""
    try:
        from tqdm import tqdm
        return tqdm(iterable, **kwargs)
    except ImportError:
        return iterable


def ingest_csv(
    path: str,
    table_name: str,
    conn: Connection | None = None,
    conn_str: str | None = None,
    if_exists: str = "fail",
    chunksize: int | None = None,
    read_options=None,
    parse_options=None,
    convert_options=None,
) -> int:
    """Load a CSV file into SQL Server via Arrow.

    Parameters
    ----------
    path : str
        Path to the CSV file.
    table_name : str
        Target table name.
    conn / conn_str
        Connection or connection string.
    if_exists : str
        ``"fail"``, ``"replace"``, or ``"append"``.
    chunksize : int, optional
        If set, read and ingest in batches.
    read_options, parse_options, convert_options
        PyArrow CSV reader options.

    Returns
    -------
    int
        Total rows ingested.
    """
    mode_map = {"fail": "create", "replace": "replace", "append": "append"}
    if if_exists not in mode_map:
        raise ValueError(f"if_exists must be one of {list(mode_map.keys())}")
    mode = mode_map[if_exists]

    c, should_close = _get_connection(conn, conn_str)
    try:
        ro = read_options or pcsv.ReadOptions()
        po = parse_options or pcsv.ParseOptions()
        co = convert_options or pcsv.ConvertOptions()

        if chunksize:
            ro = pcsv.ReadOptions(block_size=chunksize * 1024)  # approximate
            reader = pcsv.open_csv(path, read_options=ro, parse_options=po, convert_options=co)
            total = 0
            first = True
            for batch in reader:
                chunk_table = pa.Table.from_batches([batch])
                chunk_mode = mode if first else "append"
                cur = c.cursor()
                cur.adbc_ingest(table_name, chunk_table, mode=chunk_mode)
                total += cur.rowcount
                cur.close()
                first = False
            return total
        else:
            table = pcsv.read_csv(path, read_options=ro, parse_options=po, convert_options=co)
            cur = c.cursor()
            cur.adbc_ingest(table_name, table, mode=mode)
            total = cur.rowcount
            cur.close()
            return total
    finally:
        if should_close:
            c.close()


def ingest_parquet(
    path: str,
    table_name: str,
    conn: Connection | None = None,
    conn_str: str | None = None,
    if_exists: str = "fail",
    columns: list[str] | None = None,
) -> int:
    """Load a Parquet file into SQL Server via Arrow.

    Parameters
    ----------
    path : str
        Path to the Parquet file.
    table_name : str
        Target table name.
    conn / conn_str
        Connection or connection string.
    if_exists : str
        ``"fail"``, ``"replace"``, or ``"append"``.
    columns : list of str, optional
        Only load these columns.

    Returns
    -------
    int
        Total rows ingested.
    """
    mode_map = {"fail": "create", "replace": "replace", "append": "append"}
    if if_exists not in mode_map:
        raise ValueError(f"if_exists must be one of {list(mode_map.keys())}")
    mode = mode_map[if_exists]

    c, should_close = _get_connection(conn, conn_str)
    try:
        table = pq.read_table(path, columns=columns)
        cur = c.cursor()
        cur.adbc_ingest(table_name, table, mode=mode)
        total = cur.rowcount
        cur.close()
        return total
    finally:
        if should_close:
            c.close()


def export_parquet(
    query: str,
    path: str,
    conn: Connection | None = None,
    conn_str: str | None = None,
    params=None,
    compression: str = "snappy",
) -> int:
    """Export SQL query results to a Parquet file.

    Parameters
    ----------
    query : str
        SQL query.
    path : str
        Output Parquet file path.
    conn / conn_str
        Connection or connection string.
    params : sequence, optional
        Query parameters.
    compression : str
        Parquet compression codec (default ``"snappy"``).

    Returns
    -------
    int
        Number of rows exported.
    """
    c, should_close = _get_connection(conn, conn_str)
    try:
        cur = c.cursor()
        cur.execute(query, params)
        table = cur.fetch_arrow_table()
        pq.write_table(table, path, compression=compression)
        return table.num_rows
    finally:
        if should_close:
            c.close()


def export_csv(
    query: str,
    path: str,
    conn: Connection | None = None,
    conn_str: str | None = None,
    params=None,
) -> int:
    """Export SQL query results to a CSV file.

    Parameters
    ----------
    query : str
        SQL query.
    path : str
        Output CSV file path.
    conn / conn_str
        Connection or connection string.
    params : sequence, optional
        Query parameters.

    Returns
    -------
    int
        Number of rows exported.
    """
    c, should_close = _get_connection(conn, conn_str)
    try:
        cur = c.cursor()
        cur.execute(query, params)
        table = cur.fetch_arrow_table()
        pcsv.write_csv(table, path)
        return table.num_rows
    finally:
        if should_close:
            c.close()
