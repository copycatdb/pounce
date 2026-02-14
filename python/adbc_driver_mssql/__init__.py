"""ADBC Driver for Microsoft SQL Server - DB-API 2.0 compatible interface."""
from adbc_driver_mssql.dbapi import connect, Connection, Cursor

__all__ = ["connect", "Connection", "Cursor"]
