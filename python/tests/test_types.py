"""Tests for SQL Server type mapping across all interfaces."""

import pytest
import math
from decimal import Decimal
from conftest import CONN_STR, drop_table, execute_sql


class TestIntegerTypes:
    @pytest.mark.parametrize("sql_type,value,expected", [
        ("TINYINT", 255, 255),
        ("TINYINT", 0, 0),
        ("SMALLINT", -32768, -32768),
        ("SMALLINT", 32767, 32767),
        ("INT", -2147483648, -2147483648),
        ("INT", 2147483647, 2147483647),
        ("BIGINT", -9223372036854775808, -9223372036854775808),
        ("BIGINT", 9223372036854775807, 9223372036854775807),
    ])
    def test_integer_boundaries(self, conn, sql_type, value, expected):
        cur = conn.cursor()
        cur.execute(f"SELECT CAST({value} AS {sql_type}) AS v")
        row = cur.fetchone()
        assert row[0] == expected


class TestFloatTypes:
    def test_float(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT CAST(3.14159265358979 AS FLOAT) AS v")
        assert abs(cur.fetchone()[0] - 3.14159265358979) < 1e-10

    def test_real(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT CAST(3.14 AS REAL) AS v")
        assert abs(cur.fetchone()[0] - 3.14) < 0.01


class TestDecimalTypes:
    def test_decimal_basic(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT CAST(123.45 AS DECIMAL(10,2)) AS v")
        val = cur.fetchone()[0]
        assert abs(float(val) - 123.45) < 0.001

    def test_decimal_high_precision(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT CAST(1234567890.123456789 AS DECIMAL(28,9)) AS v")
        val = cur.fetchone()[0]
        assert val is not None

    def test_decimal_zero_scale(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT CAST(42 AS DECIMAL(10,0)) AS v")
        val = cur.fetchone()[0]
        assert int(val) == 42


class TestStringTypes:
    def test_varchar(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT CAST('hello' AS VARCHAR(50)) AS v")
        assert cur.fetchone()[0] == "hello"

    def test_nvarchar(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT CAST(N'hello' AS NVARCHAR(50)) AS v")
        assert cur.fetchone()[0] == "hello"

    def test_unicode_chinese(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT N'你好世界' AS v")
        assert cur.fetchone()[0] == "你好世界"

    def test_unicode_emoji(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT N'🎉🚀💯' AS v")
        result = cur.fetchone()[0]
        # SQL Server may not support all emoji in nvarchar, but should not crash
        assert result is not None

    def test_unicode_arabic(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT N'مرحبا' AS v")
        assert cur.fetchone()[0] == "مرحبا"

    def test_empty_string(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT '' AS v")
        assert cur.fetchone()[0] == ""

    def test_long_string(self, conn):
        """Test nvarchar(max) with a long string."""
        drop_table(conn, "test_long_str")
        execute_sql(conn, "CREATE TABLE test_long_str (v NVARCHAR(MAX))")
        long_str = "x" * 10000
        cur = conn.cursor()
        cur.execute(f"INSERT INTO test_long_str VALUES (N'{long_str}')")
        cur.execute("SELECT v FROM test_long_str")
        result = cur.fetchone()[0]
        assert len(result) == 10000
        drop_table(conn, "test_long_str")


class TestDateTimeTypes:
    def test_date(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT CAST('2024-01-15' AS DATE) AS v")
        val = cur.fetchone()[0]
        assert val is not None

    def test_datetime2(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT CAST('2024-06-15 14:30:00.123456' AS DATETIME2) AS v")
        val = cur.fetchone()[0]
        assert val is not None

    def test_time(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT CAST('14:30:00' AS TIME) AS v")
        val = cur.fetchone()[0]
        assert val is not None

    def test_datetimeoffset(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT CAST('2024-06-15 14:30:00 +05:30' AS DATETIMEOFFSET) AS v")
        val = cur.fetchone()[0]
        assert val is not None


class TestBitType:
    def test_bit_true(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT CAST(1 AS BIT) AS v")
        assert cur.fetchone()[0] in (True, 1)

    def test_bit_false(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT CAST(0 AS BIT) AS v")
        assert cur.fetchone()[0] in (False, 0)


class TestUniqueIdentifier:
    def test_guid(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT CAST('A0E1F2D3-B4C5-6789-0ABC-DEF012345678' AS UNIQUEIDENTIFIER) AS v")
        val = cur.fetchone()[0]
        assert val is not None
        assert len(str(val)) == 36  # GUID format


class TestBinaryTypes:
    def test_varbinary(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT CAST(0xDEADBEEF AS VARBINARY(10)) AS v")
        val = cur.fetchone()[0]
        assert val is not None

    def test_binary_roundtrip(self, conn):
        drop_table(conn, "test_bin_rt")
        execute_sql(conn, "CREATE TABLE test_bin_rt (data VARBINARY(100))")
        execute_sql(conn, "INSERT INTO test_bin_rt VALUES (0x48656C6C6F)")
        cur = conn.cursor()
        cur.execute("SELECT data FROM test_bin_rt")
        val = cur.fetchone()[0]
        assert val is not None
        drop_table(conn, "test_bin_rt")


class TestXmlType:
    def test_xml(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT CAST('<root><item>hello</item></root>' AS XML) AS v")
        val = cur.fetchone()[0]
        assert "hello" in str(val)


class TestSingleRowSingleCol:
    def test_single_value(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT 42 AS v")
        assert cur.fetchone() == (42,)
        assert cur.fetchone() is None

    def test_single_null(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT CAST(NULL AS INT) AS v")
        assert cur.fetchone() == (None,)
