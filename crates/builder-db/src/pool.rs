//! Provides an async-friendly [`ConnectionPool`] implementation.

use crate::{
    error::{
        AcquireThenError, AcquireThenQueryError, AcquireThenRusqliteError, ConnectionCloseErrors,
    },
    with_tx,
};
use essential_builder_types::SolutionSetFailure;
use essential_types::{solution::SolutionSet, ContentAddress};
use rusqlite_pool::tokio::{AsyncConnectionHandle, AsyncConnectionPool};
use std::{ops::Range, path::PathBuf, sync::Arc, time::Duration};
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

impl ConnectionPool {
    /// Create the connection pool from the given configuration.
    pub fn new(conf: &Config) -> rusqlite::Result<Self> {
        let conn_pool = Self(new_conn_pool(conf)?);
        if let Source::Path(_) = conf.source {
            let conn = conn_pool
                .try_acquire()
                .expect("pool must have at least one connection");
            conn.pragma_update(None, "journal_mode", "wal")?;
        }
        Ok(conn_pool)
    }

    /// Create the connection pool from the given configuration and ensure the DB tables have been
    /// created if they do not already exist before returning.
    pub fn with_tables(conf: &Config) -> rusqlite::Result<Self> {
        let conn_pool = Self::new(conf)?;
        let mut conn = conn_pool.try_acquire().unwrap();
        with_tx(&mut conn, |tx| crate::create_tables(tx))?;
        Ok(conn_pool)
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

    /// Close a connection pool, returning a `ConnectionCloseErrors` in the case of any errors.
    pub fn close(&self) -> Result<(), ConnectionCloseErrors> {
        let res = self.0.close();
        let errs: Vec<_> = res.into_iter().filter_map(Result::err).collect();
        if !errs.is_empty() {
            return Err(ConnectionCloseErrors(errs));
        }
        Ok(())
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

    /// Acquire a connection and call [`crate::create_tables`].
    pub async fn create_tables(&self) -> Result<(), AcquireThenRusqliteError> {
        self.acquire_then(|h| with_tx(h, |tx| crate::create_tables(tx)))
            .await
    }

    /// Acquire a connection and call [`crate::insert_solution_set_submission`].
    pub async fn insert_solution_set_submission(
        &self,
        solution_set: Arc<SolutionSet>,
        timestamp: Duration,
    ) -> Result<ContentAddress, AcquireThenRusqliteError> {
        self.acquire_then(move |h| {
            with_tx(h, |tx| {
                crate::insert_solution_set_submission(tx, &solution_set, timestamp)
            })
        })
        .await
    }

    /// Acquire a connection and call [`crate::insert_solution_set_failure`].
    pub async fn insert_solution_set_failure(
        &self,
        solution_set_ca: ContentAddress,
        failure: SolutionSetFailure<'static>,
    ) -> Result<(), AcquireThenRusqliteError> {
        self.acquire_then(move |h| crate::insert_solution_set_failure(h, &solution_set_ca, failure))
            .await
    }

    /// Acquire a connection and call [`crate::get_solution_set`].
    pub async fn get_solution_set(
        &self,
        ca: ContentAddress,
    ) -> Result<Option<SolutionSet>, AcquireThenQueryError> {
        self.acquire_then(move |h| crate::get_solution_set(h, &ca))
            .await
    }

    /// Acquire a connection and call [`crate::list_solution_sets`].
    pub async fn list_solution_sets(
        &self,
        time_range: Range<Duration>,
        limit: i64,
    ) -> Result<Vec<(ContentAddress, SolutionSet, Duration)>, AcquireThenQueryError> {
        self.acquire_then(move |h| crate::list_solution_sets(h, time_range, limit))
            .await
    }

    /// Acquire a connection and call [`crate::list_submissions`].
    pub async fn list_submissions(
        &self,
        time_range: Range<Duration>,
        limit: i64,
    ) -> Result<Vec<(ContentAddress, Duration)>, AcquireThenRusqliteError> {
        self.acquire_then(move |h| crate::list_submissions(h, time_range, limit))
            .await
    }

    /// Acquire a connection and call [`crate::latest_solution_set_failures`].
    pub async fn latest_solution_set_failures(
        &self,
        solution_set_ca: ContentAddress,
        limit: u32,
    ) -> Result<Vec<SolutionSetFailure<'static>>, AcquireThenRusqliteError> {
        self.acquire_then(move |h| crate::latest_solution_set_failures(h, &solution_set_ca, limit))
            .await
    }

    /// Acquire a connection and call [`crate::list_solution_set_failures`].
    pub async fn list_solution_set_failures(
        &self,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<SolutionSetFailure<'static>>, AcquireThenRusqliteError> {
        self.acquire_then(move |h| crate::list_solution_set_failures(h, offset, limit))
            .await
    }

    /// Acquire a connection and call [`crate::delete_solution_set`].
    pub async fn delete_solution_set(
        &self,
        ca: ContentAddress,
    ) -> Result<(), AcquireThenRusqliteError> {
        self.acquire_then(move |h| crate::delete_solution_set(h, &ca))
            .await
    }

    /// Delete the given set of solution sets in a single transaction.
    pub async fn delete_solution_sets(
        &self,
        cas: impl 'static + IntoIterator<Item = ContentAddress> + Send,
    ) -> Result<(), AcquireThenRusqliteError> {
        self.acquire_then(|h| with_tx(h, |tx| crate::delete_solution_sets(tx, cas)))
            .await
    }

    /// Acquire a connection and call [`crate::delete_oldest_solution_set_failures`].
    pub async fn delete_oldest_solution_set_failures(
        &self,
        keep_limit: u32,
    ) -> Result<(), AcquireThenRusqliteError> {
        self.acquire_then(move |h| crate::delete_oldest_solution_set_failures(h, keep_limit))
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

/// Initialise the connection pool from the given configuration.
fn new_conn_pool(conf: &Config) -> rusqlite::Result<AsyncConnectionPool> {
    AsyncConnectionPool::new(conf.conn_limit, || new_conn(&conf.source))
}

/// Create a new connection given a DB source.
fn new_conn(source: &Source) -> rusqlite::Result<rusqlite::Connection> {
    let conn = match source {
        Source::Memory(id) => new_mem_conn(id),
        Source::Path(p) => {
            if let Some(dir) = p.parent() {
                let _ = std::fs::create_dir_all(dir);
            }
            let conn = rusqlite::Connection::open(p)?;
            conn.pragma_update(None, "trusted_schema", false)?;
            conn.pragma_update(None, "synchronous", 1)?;
            Ok(conn)
        }
    }?;
    conn.pragma_update(None, "foreign_keys", true)?;
    Ok(conn)
}

/// Create an in-memory connection with the given ID
fn new_mem_conn(id: &str) -> rusqlite::Result<rusqlite::Connection> {
    let conn_str = format!("file:/{id}");
    rusqlite::Connection::open_with_flags_and_vfs(conn_str, Default::default(), "memdb")
}
