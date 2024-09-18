use thiserror::Error;

/// Any error that might occur during builder DB connection pool access.
#[cfg(feature = "pool")]
#[derive(Debug, Error)]
pub enum AcquireThenError<E> {
    /// Failed to acquire a DB connection.
    #[error("failed to acquire a DB connection: {0}")]
    Acquire(#[from] tokio::sync::AcquireError),
    /// The tokio spawn blocking task failed to join.
    #[error("failed to join task: {0}")]
    Join(#[from] tokio::task::JoinError),
    /// The error returned by the `acquire_then` function result.
    #[error("{0}")]
    Inner(E),
}

/// An `acquire_then` error whose function returns a result with a rusqlite error.
#[cfg(feature = "pool")]
pub type AcquireThenRusqliteError = AcquireThenError<rusqlite::Error>;

/// An `acquire_then` error whose function returns a result with a query error.
#[cfg(feature = "pool")]
pub type AcquireThenQueryError = AcquireThenError<crate::error::QueryError>;

/// Any error that might occur during decoding of a type returned by the DB.
#[derive(Debug, Error)]
#[error("decoding failed due to postcard deserialization error: {0}")]
pub struct DecodeError(#[from] pub postcard::Error);

/// A database or decoding error returned by a query.
#[derive(Debug, Error)]
pub enum QueryError {
    /// A DB error occurred.
    #[error("a DB error occurred: {0}")]
    Rusqlite(#[from] rusqlite::Error),
    /// A decoding error occurred.
    #[error("failed to decode: {0}")]
    Decode(#[from] DecodeError),
}
