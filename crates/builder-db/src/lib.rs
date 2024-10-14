//! The database API for the block builder's solution pool and related storage.
//!
//! The `essential-builder-db` crate provides a simple database API for managing the block
//! builder's solution pool and related storage, using SQLite as the underlying database. It allows
//! you to store, query, and delete solutions, as well as manage solution submissions with
//! timestamps.
//!
//! ## Overview
//!
//! - [`create_tables`]: Creates all required tables in the database.
//! - [`insert_solution_submission`]: Inserts a solution and its associated submission timestamp.
//! - [`insert_solution_failure`]: Records a failure to apply a solution to a block.
//! - [`get_solution`]: Retrieves a solution by its content address.
//! - [`list_solutions`]: Lists all solutions that were submitted within a given time range.
//! - [`list_submissions`]: Lists submissions based on timestamp.
//! - [`latest_solution_failures`]: Queries the latest failures for a given solution.
//! - [`delete_solution`]: Deletes a solution and its submissions given the solution's address.
//! - [`delete_oldest_solution_failures`]: Deletes the oldest solution failures until the
//!   stored number is within a given limit.

use error::{DecodeError, QueryError};
use essential_hash::content_addr;
use essential_types::{solution::Solution, ContentAddress, Hash};
#[cfg(feature = "pool")]
pub use pool::ConnectionPool;
use rusqlite::{named_params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use std::{ops::Range, time::Duration};

pub mod error;
#[cfg(feature = "pool")]
pub mod pool;
pub mod sql;

/// A failed attempt at applying a solution at a particular location within a particular block.
///
/// The builder stores these in order to provide solution submitters feedback in the case that a
/// solution could not be applied trivially and did not make it into a block.
#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct SolutionFailure<'a> {
    /// The number of the block in which the builder attempted to apply the solution.
    pub attempt_block_num: i64,
    /// The address of the block in which the builder attempted to apply the solution.
    pub attempt_block_addr: ContentAddress,
    /// The solution index within the block at which the builder attempted to apply the solution.
    pub attempt_solution_ix: u32,
    /// An error message describing why the builder failed to apply the solution.
    pub err_msg: std::borrow::Cow<'a, str>,
}

/// Encodes the given value into a blob.
///
/// This serializes the value using postcard.
pub fn encode<T>(value: &T) -> Vec<u8>
where
    T: Serialize,
{
    postcard::to_allocvec(value).expect("postcard serialization cannot fail")
}

/// Decodes the given blob into a value of type `T`.
///
/// This deserializes the bytes into a value of `T` with `postcard`.
pub fn decode<T>(value: &[u8]) -> Result<T, DecodeError>
where
    T: for<'de> Deserialize<'de>,
{
    Ok(postcard::from_bytes(value)?)
}

/// Create all tables.
pub fn create_tables(tx: &Transaction) -> rusqlite::Result<()> {
    for table in sql::table::ALL {
        tx.execute(table.create, ())?;
    }
    Ok(())
}

/// Insert a submitted solution and the time it was received into the table.
///
/// This first inserts the solution and its CA into the solution table if it doesn't
/// already exist, then inserts associated timestamp into the submission table.
pub fn insert_solution_submission(
    tx: &Transaction,
    solution: &Solution,
    timestamp: Duration,
) -> rusqlite::Result<ContentAddress> {
    // Insert the solution (or ignore if exists).
    let ca = content_addr(solution);
    let ca_blob = &ca.0;
    let solution_blob = encode(solution);
    tx.execute(
        sql::insert::SOLUTION,
        named_params! {
            ":content_addr": &ca_blob,
            ":solution": solution_blob,
        },
    )?;

    // Insert the submission timestamp.
    let secs = timestamp.as_secs();
    let nanos = timestamp.subsec_nanos();
    tx.execute(
        sql::insert::SUBMISSION,
        named_params! {
            ":solution_addr": ca_blob,
            ":timestamp_secs": secs,
            ":timestamp_nanos": nanos,
        },
    )?;

    Ok(ca)
}

/// Record a failure to include a solution in a block.
///
/// We only record that a failure occurred, the block number, solution index at which the failure
/// occurred, and a basic error message that can be returned to the submitter. If the user or
/// application requires more detailed information about the failure, the block number and solution
/// index should be enough to reconstruct the failure with a synced node.
pub fn insert_solution_failure(
    conn: &Connection,
    solution_ca: &ContentAddress,
    solution_failure: SolutionFailure,
) -> rusqlite::Result<()> {
    conn.execute(
        sql::insert::SOLUTION_FAILURE,
        named_params! {
            ":solution_addr": &solution_ca.0,
            ":attempt_block_num": solution_failure.attempt_block_num,
            ":attempt_block_addr": &solution_failure.attempt_block_addr.0,
            ":attempt_solution_ix": solution_failure.attempt_solution_ix,
            ":err_msg": solution_failure.err_msg.as_bytes(),
        },
    )?;
    Ok(())
}

/// Fetches a solution by its content address.
pub fn get_solution(
    conn: &Connection,
    ca: &ContentAddress,
) -> Result<Option<Solution>, QueryError> {
    let ca_blob = &ca.0;
    let mut stmt = conn.prepare(sql::query::GET_SOLUTION)?;
    let solution_blob: Option<Vec<u8>> = stmt
        .query_row([ca_blob], |row| row.get("solution"))
        .optional()?;
    Ok(solution_blob.as_deref().map(decode).transpose()?)
}

/// List all solutions that were submitted within the given time range.
///
/// The number of results will be limited to the given `limit`.
///
/// Note that if the same solution was submitted multiple times within the given time range, they
/// will appear multiple times in the result.
pub fn list_solutions(
    conn: &Connection,
    time_range: Range<Duration>,
    limit: i64,
) -> Result<Vec<(ContentAddress, Solution, Duration)>, QueryError> {
    let mut stmt = conn.prepare(sql::query::LIST_SOLUTIONS)?;
    let start_secs = time_range.start.as_secs();
    let start_nanos = time_range.start.subsec_nanos();
    let end_secs = time_range.end.as_secs();
    let end_nanos = time_range.end.subsec_nanos();
    let rows = stmt.query_map(
        named_params! {
            ":start_secs": start_secs,
            ":start_nanos": start_nanos,
            ":end_secs": end_secs,
            ":end_nanos": end_nanos,
            ":limit": limit,
        },
        |row| {
            let solution_addr_blob: Hash = row.get("content_addr")?;
            let solution_blob: Vec<u8> = row.get("solution")?;
            let secs: u64 = row.get("timestamp_secs")?;
            let nanos: u32 = row.get("timestamp_nanos")?;
            let timestamp = Duration::new(secs, nanos);
            Ok((ContentAddress(solution_addr_blob), solution_blob, timestamp))
        },
    )?;
    rows.into_iter()
        .map(|res| {
            let (ca, blob, ts) = res?;
            let solution = decode(&blob)?;
            Ok((ca, solution, ts))
        })
        .collect()
}

/// List all submissions that were made within the given time range.
///
/// The number of results will be limited to the given `limit`.
pub fn list_submissions(
    conn: &Connection,
    time_range: Range<Duration>,
    limit: i64,
) -> rusqlite::Result<Vec<(ContentAddress, Duration)>> {
    let mut stmt = conn.prepare(sql::query::LIST_SUBMISSIONS)?;
    let start_secs = time_range.start.as_secs();
    let start_nanos = time_range.start.subsec_nanos();
    let end_secs = time_range.end.as_secs();
    let end_nanos = time_range.end.subsec_nanos();
    let rows = stmt.query_map(
        named_params! {
            ":start_secs": start_secs,
            ":start_nanos": start_nanos,
            ":end_secs": end_secs,
            ":end_nanos": end_nanos,
            ":limit": limit,
        },
        |row| {
            let solution_addr_blob: Hash = row.get("content_addr")?;
            let secs: u64 = row.get("timestamp_secs")?;
            let nanos: u32 = row.get("timestamp_nanos")?;
            let timestamp = Duration::new(secs, nanos);
            Ok((ContentAddress(solution_addr_blob), timestamp))
        },
    )?;
    rows.collect()
}

/// Query the latest solution failures for a given solution content address.
///
/// Results are ordered by block number and solution index in descending order.
///
/// Returns at most `limit` failures.
pub fn latest_solution_failures(
    conn: &Connection,
    ca: &ContentAddress,
    limit: u32,
) -> rusqlite::Result<Vec<SolutionFailure<'static>>> {
    let ca_blob = &ca.0;
    let mut stmt = conn.prepare(sql::query::LATEST_SOLUTION_FAILURES)?;
    let rows = stmt.query_map(
        named_params! {
            ":solution_addr": ca_blob,
            ":limit": limit,
        },
        |row| {
            let attempt_block_num: i64 = row.get("attempt_block_num")?;
            let attempt_block_addr: Hash = row.get("attempt_block_addr")?;
            let attempt_solution_ix: u32 = row.get("attempt_solution_ix")?;
            let err_msg_blob: Vec<u8> = row.get("err_msg")?;
            let err_msg = String::from_utf8_lossy(&err_msg_blob).into_owned();
            Ok(SolutionFailure {
                attempt_block_num,
                attempt_block_addr: ContentAddress(attempt_block_addr),
                attempt_solution_ix,
                err_msg: err_msg.into(),
            })
        },
    )?;
    rows.collect()
}

/// Delete the solution with the given CA from the database if it exists.
///
/// This also deletes all submissions associated with the specified solution.
pub fn delete_solution(conn: &Connection, ca: &ContentAddress) -> rusqlite::Result<()> {
    let ca_blob = &ca.0;
    conn.execute(
        sql::delete::SOLUTION,
        named_params! {
            ":content_addr": ca_blob,
        },
    )?;
    Ok(())
}

/// Delete the solutions with the given CAs from the database if they exist.
///
/// This also deletes all submissions associated with the specified solutions.
pub fn delete_solutions(
    tx: &Transaction,
    cas: impl IntoIterator<Item = ContentAddress>,
) -> rusqlite::Result<()> {
    for ca in cas {
        crate::delete_solution(tx, &ca)?;
    }
    Ok(())
}

/// Delete the oldest solution failures until the number of stored failures
/// is less than or equal to `keep_limit`.
pub fn delete_oldest_solution_failures(conn: &Connection, keep_limit: u32) -> rusqlite::Result<()> {
    conn.execute(
        sql::delete::OLDEST_SOLUTION_FAILURES,
        named_params! {
            ":keep_limit": keep_limit,
        },
    )?;
    Ok(())
}

/// Short-hand for constructing a transaction, providing it as an argument to
/// the given function, then committing the transaction before returning.
pub fn with_tx<T, E>(
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
