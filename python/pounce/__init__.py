"""pounce — Arrow-native SQL Server driver for Python.

Zero-copy Arrow transport over TDS. No ODBC. No driver manager.

    from pounce import dbapi

    conn = dbapi.connect("Server=localhost,1433;UID=sa;PWD=secret")
    cur = conn.cursor()
    cur.execute("SELECT * FROM my_table")

    table = cur.fetch_arrow_table()    # zero-copy Arrow
    rows = cur.fetchall()              # classic DB-API tuples

Part of the CopyCat ecosystem: https://github.com/copycatdb
"""

from pounce.dbapi import connect, Connection, Cursor

__version__ = "0.1.0"
__all__ = ["connect", "Connection", "Cursor"]
