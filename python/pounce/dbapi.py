"""DB-API 2.0 interface for SQL Server, backed by Apache Arrow.

pounce speaks TDS natively (via tabby) and returns data as Arrow record
batches through zero-copy FFI. The DB-API layer on top lets you drop
pounce into any codebase that already uses PEP 249 — fetchone/fetchall
work as expected — but the real power is ``fetch_arrow_table()`` which
hands you a PyArrow Table without creating a single Python row object.

Quick start::

    from pounce import dbapi

    conn = dbapi.connect(
        "Server=localhost,1433;UID=sa;PWD=secret;TrustServerCertificate=yes"
    )

    # Arrow path — zero-copy, columnar, fast
    cur = conn.cursor()
    cur.execute("SELECT * FROM sales")
    table = cur.fetch_arrow_table()
    df = table.to_pandas()              # or polars.from_arrow(table)

    # DB-API path — classic tuples
    cur.execute("SELECT name FROM users WHERE id = ?", [42])
    row = cur.fetchone()

Connection string keys (case-insensitive)::

    Server=host,port    — host and optional port (default 1433)
    UID=username        — SQL login username
    PWD=password        — SQL login password
    Database=name       — initial database (default master)
    TrustServerCertificate=yes — skip TLS certificate validation
"""

import pyarrow as pa
from pounce._native import NativeConnection

# ---------------------------------------------------------------------------
# DB-API 2.0 module-level attributes
# ---------------------------------------------------------------------------

#: PEP 249 API level.
apilevel = "2.0"

#: Thread safety: connections may not be shared across threads.
threadsafety = 1

#: Parameter marker style (``?`` positional placeholders).
paramstyle = "qmark"


# ---------------------------------------------------------------------------
# DB-API 2.0 exception hierarchy
# ---------------------------------------------------------------------------

class Error(Exception):
    """Base class for all pounce errors."""

class DatabaseError(Error):
    """Error reported by SQL Server (syntax, constraint, permission, …)."""

class OperationalError(DatabaseError):
    """Connection lost, timeout, server unavailable."""

class IntegrityError(DatabaseError):
    """Constraint violation (PK, FK, unique, check)."""

class ProgrammingError(DatabaseError):
    """Bad SQL, wrong parameter count, cursor misuse."""

class InterfaceError(Error):
    """Misuse of the driver API (e.g. operating on a closed cursor)."""


# ---------------------------------------------------------------------------
# connect()
# ---------------------------------------------------------------------------

def connect(connection_string: str, **kwargs) -> "Connection":
    """Open a connection to SQL Server.

    Parameters
    ----------
    connection_string : str
        ADO-style connection string, e.g.
        ``"Server=host,port;UID=user;PWD=pass;TrustServerCertificate=yes"``

    Returns
    -------
    Connection
        A new DB-API 2.0 connection backed by a TDS session.

    Raises
    ------
    OperationalError
        If the connection cannot be established.
    """
    return Connection(connection_string, **kwargs)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

class Connection:
    """DB-API 2.0 connection wrapping a single TDS session.

    Supports context-manager usage::

        with dbapi.connect(...) as conn:
            cur = conn.cursor()
            ...
    """

    def __init__(self, connection_string: str, autocommit: bool = False):
        self._native = NativeConnection(connection_string)
        self._native.set_autocommit(autocommit)
        self._closed = False

    # -- Properties --------------------------------------------------------

    @property
    def autocommit(self) -> bool:
        """Whether each statement auto-commits. Default ``False``."""
        return self._native.get_autocommit()

    @autocommit.setter
    def autocommit(self, value: bool):
        self._native.set_autocommit(value)

    # -- DB-API methods ----------------------------------------------------

    def cursor(self) -> "Cursor":
        """Create a new cursor on this connection."""
        if self._closed:
            raise InterfaceError("Connection is closed")
        return Cursor(self)

    def commit(self):
        """Commit the current transaction."""
        self._native.commit()

    def rollback(self):
        """Roll back the current transaction."""
        self._native.rollback()

    def close(self):
        """Close the connection and release resources."""
        if not self._closed:
            self._native.close()
            self._closed = True

    # -- Context manager ---------------------------------------------------

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ---------------------------------------------------------------------------
# Cursor
# ---------------------------------------------------------------------------

class Cursor:
    """DB-API 2.0 cursor with Arrow-native extensions.

    Every ``execute()`` call runs the query through Rust (tabby) and captures
    results directly as Arrow record batches.  From there you can:

    * ``fetch_arrow_table()`` — zero-copy PyArrow Table (fastest)
    * ``fetchone() / fetchall()`` — classic DB-API tuples (lazily materialised)
    * ``fetch_record_batch()`` — streaming RecordBatchReader

    Supports context-manager usage::

        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            ...
    """

    def __init__(self, connection: Connection):
        self._conn = connection
        self._result = None       # PyArrow Table when query has results
        self._rows = None         # lazily materialised list[tuple]
        self._row_index = 0       # current position for fetchone/fetchmany
        self._description = None  # DB-API column metadata
        self._rowcount = -1       # rows affected / rows returned
        self._closed = False
        self._batch_size = 65536  # default Arrow batch size hint

    # -- DB-API descriptor properties --------------------------------------

    @property
    def description(self):
        """Column metadata after ``execute()``, or ``None``.

        Each entry is a 7-tuple:
        ``(name, type_code, display_size, internal_size, precision, scale, nullable)``
        """
        return self._description

    @property
    def rowcount(self) -> int:
        """Number of rows affected (DML) or returned (SELECT), else ``-1``."""
        return self._rowcount

    @property
    def arraysize(self) -> int:
        """Default batch size for ``fetchmany()``. Default 65 536."""
        return self._batch_size

    @arraysize.setter
    def arraysize(self, value: int):
        self._batch_size = value

    # -- Lifecycle ---------------------------------------------------------

    def close(self):
        """Close the cursor and release results."""
        self._closed = True
        self._result = None
        self._rows = None

    def _check_closed(self):
        if self._closed:
            raise InterfaceError("Cursor is closed")

    # -- Parameter binding -------------------------------------------------

    def _substitute_params(self, sql: str, parameters=None) -> str:
        """Replace ``?`` placeholders with T-SQL literals.

        This is client-side substitution — not true parameterised queries
        (those require RPC, which is on the roadmap). Safe for trusted input;
        values are escaped but callers should still validate untrusted data.
        """
        if not parameters:
            return sql
        result = []
        param_idx = 0
        in_string = False
        for ch in sql:
            if in_string:
                result.append(ch)
                if ch == "'":
                    in_string = False
            elif ch == "'":
                result.append(ch)
                in_string = True
            elif ch == "?" and param_idx < len(parameters):
                result.append(self._param_to_literal(parameters[param_idx]))
                param_idx += 1
            else:
                result.append(ch)
        return "".join(result)

    @staticmethod
    def _param_to_literal(val) -> str:
        """Convert a Python value to a T-SQL literal string."""
        if val is None:
            return "NULL"
        if isinstance(val, bool):
            return "1" if val else "0"
        if isinstance(val, int):
            return str(val)
        if isinstance(val, float):
            return str(val)
        if isinstance(val, bytes):
            return "0x" + val.hex()
        if isinstance(val, str):
            return "N'" + val.replace("'", "''") + "'"
        # datetime, date, time, etc. — stringify and quote
        return "N'" + str(val).replace("'", "''") + "'"

    # -- Execution ---------------------------------------------------------

    def execute(self, operation: str, parameters=None):
        """Execute a SQL statement.

        Parameters
        ----------
        operation : str
            SQL query with optional ``?`` parameter placeholders.
        parameters : sequence, optional
            Values to bind to ``?`` placeholders.

        Results are available via ``fetch*`` or ``fetch_arrow_table()``.
        For DML (INSERT/UPDATE/DELETE), check ``rowcount`` instead.
        """
        self._check_closed()
        sql = self._substitute_params(operation, parameters)
        result = self._conn._native.execute_arrow(sql, self._batch_size)

        if isinstance(result, int):
            # DML — no result set, just affected row count
            self._result = None
            self._rowcount = result
            self._description = None
            self._rows = None
        else:
            # SELECT — result is a PyArrow Table
            self._result = result
            self._rowcount = result.num_rows
            self._description = self._build_description(result.schema)
            self._rows = None
            self._row_index = 0

    def executemany(self, operation: str, seq_of_parameters):
        """Execute a SQL statement once for each parameter set.

        Useful for batch INSERT. Not yet optimised — executes sequentially.
        """
        self._check_closed()
        total = 0
        for params in seq_of_parameters:
            self.execute(operation, params)
            if self._rowcount > 0:
                total += self._rowcount
        self._rowcount = total

    # -- Description builder -----------------------------------------------

    def _build_description(self, schema):
        """Build DB-API description tuples from an Arrow schema."""
        desc = []
        for field in schema:
            desc.append((
                field.name,      # name
                field.type,      # type_code (Arrow DataType)
                None,            # display_size
                None,            # internal_size
                None,            # precision
                None,            # scale
                field.nullable,  # null_ok
            ))
        return desc

    # -- Row materialisation -----------------------------------------------

    def _ensure_rows(self):
        """Lazily convert Arrow table → list of tuples for DB-API fetch."""
        if self._rows is None and self._result is not None:
            columns = [self._result.column(i) for i in range(self._result.num_columns)]
            num_rows = self._result.num_rows
            self._rows = [
                tuple(col[i].as_py() for col in columns)
                for i in range(num_rows)
            ]

    # -- DB-API fetch methods ----------------------------------------------

    def fetchone(self):
        """Fetch the next row, or ``None`` if exhausted."""
        self._check_closed()
        if self._result is None:
            raise ProgrammingError("No result set")
        self._ensure_rows()
        if self._row_index >= len(self._rows):
            return None
        row = self._rows[self._row_index]
        self._row_index += 1
        return row

    def fetchmany(self, size=None):
        """Fetch up to *size* rows (default ``arraysize``)."""
        self._check_closed()
        if self._result is None:
            raise ProgrammingError("No result set")
        self._ensure_rows()
        if size is None:
            size = self._batch_size
        end = _min(self._row_index + size, len(self._rows))
        rows = self._rows[self._row_index:end]
        self._row_index = end
        return rows

    def fetchall(self):
        """Fetch all remaining rows."""
        self._check_closed()
        if self._result is None:
            raise ProgrammingError("No result set")
        self._ensure_rows()
        rows = self._rows[self._row_index:]
        self._row_index = len(self._rows)
        return rows

    # -- Arrow-native fetch ------------------------------------------------

    def fetch_arrow_table(self) -> pa.Table:
        """Return results as a zero-copy PyArrow Table.

        This is the fast path — no Python row objects are created.
        The Table is backed by Arrow arrays built directly from TDS wire
        data in Rust, then handed to Python via FFI.

        Returns
        -------
        pyarrow.Table

        Raises
        ------
        ProgrammingError
            If there is no result set (e.g. after DML).
        """
        self._check_closed()
        if self._result is None:
            raise ProgrammingError("No result set")
        return self._result

    def fetch_record_batch(self):
        """Return a ``RecordBatchReader`` for streaming large results.

        Each batch is a zero-copy Arrow RecordBatch. Useful when you want
        to process results incrementally without loading everything into
        memory at once.
        """
        self._check_closed()
        if self._result is None:
            raise ProgrammingError("No result set")
        return self._result.to_reader()

    # -- Ingest (Arrow → SQL Server) ---------------------------------------

    def adbc_ingest(self, table_name: str, data, mode: str = "create", **kwargs):
        """Bulk-load a PyArrow Table into SQL Server.

        Parameters
        ----------
        table_name : str
            Target table name.
        data : pyarrow.Table
            Data to load.
        mode : str
            ``"create"`` — CREATE TABLE + INSERT (fails if exists).
            ``"append"`` — INSERT into existing table.
            ``"replace"`` — DROP + CREATE + INSERT.
            ``"create_append"`` — CREATE if not exists, then INSERT.

        Returns
        -------
        None
            Row count available via ``cursor.rowcount``.
        """
        self._check_closed()
        if not isinstance(data, pa.Table):
            raise TypeError(f"Expected pyarrow.Table, got {type(data)}")
        self._rowcount = self._conn._native.ingest(table_name, data, mode)

    # -- DB-API no-ops -----------------------------------------------------

    def setinputsizes(self, sizes):
        """No-op (required by DB-API 2.0)."""

    def setoutputsize(self, size, column=None):
        """No-op (required by DB-API 2.0)."""

    # -- Iteration ---------------------------------------------------------

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _min(a, b):
    """Built-in min is shadowed by the module namespace — use this instead."""
    return a if a < b else b
