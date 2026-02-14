//! TDS protocol implementation for Microsoft SQL Server (TDS 7.4).

#[macro_use]
mod macros;

mod client;
mod from_sql;
mod query;
mod sql_read_bytes;
mod to_sql;

pub mod error;
mod result;
mod row;
mod tds;

mod sql_browser;

pub use client::{AuthMethod, Client, Config};
pub(crate) use error::Error;
pub use from_sql::{FromSql, FromSqlOwned};
pub use query::Query;
pub use result::*;
pub use row::{Column, ColumnType, Row};
pub use sql_browser::SqlBrowser;
pub use tds::{
    codec::{BulkLoadRequest, ColumnData, ColumnFlag, FixedLenType, IntoRow, TokenInfo, TokenRow, TypeInfo, TypeLength, VarLenType},
    numeric,
    stream::QueryStream,
    time, xml, EncryptionLevel,
};
pub use to_sql::{IntoSql, ToSql};
pub use uuid::Uuid;

use sql_read_bytes::*;
use tds::codec::*;

/// An alias for a result that holds this module's error type as the error.
pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn get_driver_version() -> u64 {
    env!("CARGO_PKG_VERSION")
        .splitn(6, '.')
        .enumerate()
        .fold(0u64, |acc, part| match part.1.parse::<u64>() {
            Ok(num) => acc | num << (part.0 * 8),
            _ => acc | 0 << (part.0 * 8),
        })
}
