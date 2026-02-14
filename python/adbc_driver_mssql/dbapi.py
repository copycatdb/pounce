"""DB-API 2.0 compatible interface for SQL Server via Arrow-native transport.

Usage:
    import adbc_driver_mssql.dbapi as mssql
    conn = mssql.connect("Server=localhost,1433;UID=sa;PWD=pass;TrustServerCertificate=yes")
    cur = conn.cursor()
    cur.execute("SELECT * FROM my_table")
    table = cur.fetch_arrow_table()  # zero-copy Arrow
    rows = cur.fetchall()            # DB-API tuples
"""
import pyarrow as pa
from adbc_driver_mssql._native import NativeConnection

# DB-API 2.0 globals
apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"


class Error(Exception):
    pass

class DatabaseError(Error):
    pass

class OperationalError(DatabaseError):
    pass

class IntegrityError(DatabaseError):
    pass

class ProgrammingError(DatabaseError):
    pass

class InterfaceError(Error):
    pass


def connect(connection_string: str, **kwargs) -> "Connection":
    """Open a connection to SQL Server."""
    return Connection(connection_string, **kwargs)


class Connection:
    def __init__(self, connection_string: str, autocommit: bool = False):
        self._native = NativeConnection(connection_string)
        self._native.set_autocommit(autocommit)
        self._closed = False

    @property
    def autocommit(self) -> bool:
        return self._native.get_autocommit()

    @autocommit.setter
    def autocommit(self, value: bool):
        self._native.set_autocommit(value)

    def cursor(self) -> "Cursor":
        if self._closed:
            raise InterfaceError("Connection is closed")
        return Cursor(self)

    def commit(self):
        self._native.commit()

    def rollback(self):
        self._native.rollback()

    def close(self):
        if not self._closed:
            self._native.close()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class Cursor:
    def __init__(self, connection: Connection):
        self._conn = connection
        self._result = None  # PyArrow Table or rowcount int
        self._rows = None    # materialized rows for DB-API
        self._row_index = 0
        self._description = None
        self._rowcount = -1
        self._closed = False
        self._batch_size = 65536

    @property
    def description(self):
        return self._description

    @property
    def rowcount(self) -> int:
        return self._rowcount

    @property
    def arraysize(self) -> int:
        return self._batch_size

    @arraysize.setter
    def arraysize(self, value: int):
        self._batch_size = value

    def close(self):
        self._closed = True
        self._result = None
        self._rows = None

    def _check_closed(self):
        if self._closed:
            raise InterfaceError("Cursor is closed")

    def _substitute_params(self, sql: str, parameters=None) -> str:
        """Replace ? placeholders with literal values."""
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
        # datetime, date, etc
        return "N'" + str(val).replace("'", "''") + "'"

    def execute(self, operation: str, parameters=None):
        self._check_closed()
        sql = self._substitute_params(operation, parameters)
        result = self._conn._native.execute_arrow(sql, self._batch_size)

        if isinstance(result, int):
            # DML
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
        self._check_closed()
        total = 0
        for params in seq_of_parameters:
            self.execute(operation, params)
            if self._rowcount > 0:
                total += self._rowcount
        self._rowcount = total

    def _build_description(self, schema):
        desc = []
        for field in schema:
            desc.append((
                field.name,
                field.type,
                None,  # display_size
                None,  # internal_size
                None,  # precision
                None,  # scale
                field.nullable,
            ))
        return desc

    def _ensure_rows(self):
        """Lazily materialize rows from Arrow table for DB-API fetch methods."""
        if self._rows is None and self._result is not None:
            # Convert Arrow table to list of tuples
            columns = [self._result.column(i) for i in range(self._result.num_columns)]
            num_rows = self._result.num_rows
            self._rows = []
            for i in range(num_rows):
                row = tuple(col[i].as_py() for col in columns)
                self._rows.append(row)

    def fetchone(self):
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
        self._check_closed()
        if self._result is None:
            raise ProgrammingError("No result set")
        self._ensure_rows()
        if size is None:
            size = self._batch_size
        end = min(self._row_index + size, len(self._rows))
        rows = self._rows[self._row_index:end]
        self._row_index = end
        return rows

    def fetchall(self):
        self._check_closed()
        if self._result is None:
            raise ProgrammingError("No result set")
        self._ensure_rows()
        rows = self._rows[self._row_index:]
        self._row_index = len(self._rows)
        return rows

    def fetch_arrow_table(self) -> pa.Table:
        """Return results as a PyArrow Table (zero-copy from Rust)."""
        self._check_closed()
        if self._result is None:
            raise ProgrammingError("No result set")
        return self._result

    def fetch_record_batch(self):
        """Return a RecordBatchReader for streaming large results."""
        self._check_closed()
        if self._result is None:
            raise ProgrammingError("No result set")
        return self._result.to_reader()

    def adbc_ingest(self, table_name: str, data, mode: str = "create"):
        """Bulk ingest a PyArrow Table into SQL Server.

        Args:
            table_name: Target table name
            data: PyArrow Table
            mode: "create", "append", "replace", or "create_append"
        """
        self._check_closed()
        if not isinstance(data, pa.Table):
            raise TypeError(f"Expected pyarrow.Table, got {type(data)}")
        self._rowcount = self._conn._native.ingest(table_name, data, mode)

    def setinputsizes(self, sizes):
        pass  # no-op per DB-API spec

    def setoutputsize(self, size, column=None):
        pass  # no-op per DB-API spec

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


def min(a, b):
    return a if a < b else b
