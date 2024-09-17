//! Combines the `essential-builder-db` with `rusqlite-pool`'s `AsyncConnectionPool` to provide
//! easy async access to the builder's DB.

use essential_builder_db::{self as builder_db, SolutionFailure};
use essential_types::{solution::Solution, ContentAddress};
use rusqlite::Transaction;
use rusqlite_pool::tokio::{AsyncConnectionHandle, AsyncConnectionPool};
use std::{ops::Range, path::PathBuf, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::sync::{AcquireError, TryAcquireError};

/// Access to the builder's DB connection pool and DB-access-related methods.
///
/// The handle is safe to clone and share between threads.
#[derive(Clone)]
pub struct ConnectionPool(AsyncConnectionPool);

/// A temporary connection handle to a builder's [`ConnectionPool`].
///
/// Provides `Deref`, `DerefMut` impls for the inner [`rusqlite::Connection`].
pub struct ConnectionHandle(AsyncConnectionHandle);

/// Builder configuration related to the database.
#[derive(Clone, Debug)]
pub struct Config {
    /// The number of simultaneous connections to the database to maintain.
    pub conn_limit: usize,
    /// How to source the builder's database.
    pub source: Source,
}

/// The source of the builder's database.
#[derive(Clone, Debug)]
pub enum Source {
    /// Use an in-memory database using the given string as a unique ID.
    Memory(String),
    /// Use the database at the given path.
    Path(PathBuf),
}

/// Any error that might occur during builder DB connection pool access.
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
pub type AcquireThenRusqliteError = AcquireThenError<rusqlite::Error>;

/// An `acquire_then` error whose function returns a result with a query error.
pub type AcquireThenQueryError = AcquireThenError<builder_db::error::QueryError>;

impl ConnectionPool {
    /// Create the connection pool from the given configuration.
    pub fn new(conf: &Config) -> rusqlite::Result<Self> {
        Ok(Self(new_conn_pool(conf)?))
    }

    /// Acquire a temporary database [`ConnectionHandle`] from the inner pool.
    ///
    /// In the case that all connections are busy, waits for the first available
    /// connection.
    pub async fn acquire(&self) -> Result<ConnectionHandle, AcquireError> {
        self.0.acquire().await.map(ConnectionHandle)
    }

    /// Attempt to synchronously acquire a temporary database [`ConnectionHandle`]
    /// from the inner pool.
    ///
    /// Returns `Err` in the case that all database connections are busy or if
    /// the builder has been closed.
    pub fn try_acquire(&self) -> Result<ConnectionHandle, TryAcquireError> {
        self.0.try_acquire().map(ConnectionHandle)
    }
}

/// Short-hand methods for async DB access.
impl ConnectionPool {
    /// Asynchronous access to the builder's DB via the given function.
    ///
    /// Requests and awaits a connection from the connection pool, then spawns a
    /// blocking task for the given function providing access to the connection handle.
    pub async fn acquire_then<F, T, E>(&self, f: F) -> Result<T, AcquireThenError<E>>
    where
        F: 'static + Send + FnOnce(&mut ConnectionHandle) -> Result<T, E>,
        T: 'static + Send,
        E: 'static + Send,
    {
        // Acquire a handle.
        let mut handle = self.acquire().await?;

        // Spawn the given DB connection access function on a task.
        tokio::task::spawn_blocking(move || f(&mut handle))
            .await?
            .map_err(AcquireThenError::Inner)
    }

    /// Acquire a connection and call [`builder_db::create_tables`].
    pub async fn create_tables(&self) -> Result<(), AcquireThenRusqliteError> {
        self.acquire_then(|h| with_tx(h, |tx| builder_db::create_tables(tx)))
            .await
    }

    /// Acquire a connection and call [`builder_db::insert_solution_submission`].
    pub async fn insert_solution_submission(
        &self,
        solution: Arc<Solution>,
        timestamp: Duration,
    ) -> Result<(), AcquireThenRusqliteError> {
        self.acquire_then(move |h| {
            with_tx(h, |tx| {
                builder_db::insert_solution_submission(tx, &solution, timestamp)
            })
        })
        .await
    }

    /// Acquire a connection and call [`builder_db::insert_solution_failure`].
    pub async fn insert_solution_failure(
        &self,
        solution_ca: ContentAddress,
        failure: SolutionFailure<'static>,
    ) -> Result<(), AcquireThenRusqliteError> {
        self.acquire_then(move |h| builder_db::insert_solution_failure(h, &solution_ca, failure))
            .await
    }

    /// Acquire a connection and call [`builder_db::get_solution`].
    pub async fn get_solution(
        &self,
        ca: ContentAddress,
    ) -> Result<Option<Solution>, AcquireThenQueryError> {
        self.acquire_then(move |h| builder_db::get_solution(h, &ca))
            .await
    }

    /// Acquire a connection and call [`builder_db::list_solutions`].
    pub async fn list_solutions(
        &self,
        time_range: Range<Duration>,
        limit: i64,
    ) -> Result<Vec<(ContentAddress, Solution, Duration)>, AcquireThenQueryError> {
        self.acquire_then(move |h| builder_db::list_solutions(h, time_range, limit))
            .await
    }

    /// Acquire a connection and call [`builder_db::list_submissions`].
    pub async fn list_submissions(
        &self,
        time_range: Range<Duration>,
        limit: i64,
    ) -> Result<Vec<(ContentAddress, Duration)>, AcquireThenRusqliteError> {
        self.acquire_then(move |h| builder_db::list_submissions(h, time_range, limit))
            .await
    }

    /// Acquire a connection and call [`builder_db::latest_solution_failures`].
    pub async fn latest_solution_failures(
        &self,
        solution_ca: ContentAddress,
        limit: u32,
    ) -> Result<Vec<SolutionFailure<'static>>, AcquireThenRusqliteError> {
        self.acquire_then(move |h| builder_db::latest_solution_failures(h, &solution_ca, limit))
            .await
    }

    /// Acquire a connection and call [`builder_db::delete_solution`].
    pub async fn delete_solution(
        &self,
        ca: ContentAddress,
    ) -> Result<(), AcquireThenRusqliteError> {
        self.acquire_then(move |h| builder_db::delete_solution(h, &ca))
            .await
    }

    /// Delete the given set of solutions in a single transaction.
    pub async fn delete_solutions(
        &self,
        cas: impl 'static + IntoIterator<Item = ContentAddress> + Send,
    ) -> Result<(), AcquireThenRusqliteError> {
        self.acquire_then(|h| {
            with_tx(h, |tx| {
                for ca in cas {
                    builder_db::delete_solution(tx, &ca)?;
                }
                Ok(())
            })
        })
        .await
    }

    /// Delete all solutions that only have submissions older than the given timestamp.
    pub async fn delete_solutions_older_than(
        &self,
        timestamp: Duration,
    ) -> Result<(), AcquireThenRusqliteError> {
        self.acquire_then(move |h| builder_db::delete_solutions_older_than(h, timestamp))
            .await
    }

    /// Acquire a connection and call [`builder_db::delete_oldest_failures`].
    pub async fn delete_oldest_solution_failures(
        &self,
        keep_limit: u32,
    ) -> Result<(), AcquireThenRusqliteError> {
        self.acquire_then(move |h| builder_db::delete_oldest_solution_failures(h, keep_limit))
            .await
    }
}

impl Config {
    /// The default connection limit.
    ///
    /// This default uses the number of available CPUs as a heuristic for a
    /// default connection limit. Specifically, it multiplies the number of
    /// available CPUs by 4.
    pub fn default_conn_limit() -> usize {
        // TODO: Unsure if wasm-compatible? May want a feature for this?
        num_cpus::get().saturating_mul(4)
    }
}

impl Source {
    /// A temporary, in-memory DB with a default ID.
    pub fn default_memory() -> Self {
        // Default ID cannot be an empty string.
        Self::Memory("__default-id".to_string())
    }
}

impl AsRef<rusqlite::Connection> for ConnectionHandle {
    fn as_ref(&self) -> &rusqlite::Connection {
        self
    }
}

impl core::ops::Deref for ConnectionHandle {
    type Target = AsyncConnectionHandle;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl core::ops::DerefMut for ConnectionHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Default for Source {
    fn default() -> Self {
        Self::default_memory()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            conn_limit: Self::default_conn_limit(),
            source: Source::default(),
        }
    }
}

/// Short-hand for constructing a transaction, providing it as an argument to
/// the given function, then committing the transaction before returning.
pub(crate) fn with_tx<T, E>(
    conn: &mut rusqlite::Connection,
    f: impl FnOnce(&mut Transaction) -> Result<T, E>,
) -> Result<T, E>
where
    E: From<rusqlite::Error>,
{
    let mut tx = conn.transaction()?;
    let out = f(&mut tx)?;
    tx.commit()?;
    Ok(out)
}

/// Initialise the connection pool from the given configuration.
fn new_conn_pool(conf: &Config) -> rusqlite::Result<AsyncConnectionPool> {
    AsyncConnectionPool::new(conf.conn_limit, || new_conn(&conf.source))
}

/// Create a new connection given a DB source.
pub(crate) fn new_conn(source: &Source) -> rusqlite::Result<rusqlite::Connection> {
    match source {
        Source::Memory(id) => new_mem_conn(id),
        Source::Path(p) => rusqlite::Connection::open(p),
    }
}

/// Create an in-memory connection with the given ID
fn new_mem_conn(id: &str) -> rusqlite::Result<rusqlite::Connection> {
    let conn_str = format!("file:/{id}");
    rusqlite::Connection::open_with_flags_and_vfs(conn_str, Default::default(), "memdb")
}
