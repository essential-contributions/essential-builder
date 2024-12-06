//! The database API for the block builder's solution set pool and related storage.
//!
//! The `essential-builder-db` crate provides a simple database API for managing the block
//! builder's solution set pool and related storage, using SQLite as the underlying database. It allows
//! you to store, query, and delete solution sets, as well as manage solution set submissions with
//! timestamps.
//!
//! ## Overview
//!
//! - [`create_tables`]: Creates all required tables in the database.
//! - [`insert_solution_set_submission`]: Inserts a solution set and its associated submission timestamp.
//! - [`insert_solution_set_failure`]: Records a failure to apply a solution set to a block.
//! - [`get_solution_set`]: Retrieves a solution set by its content address.
//! - [`list_solution_sets`]: Lists all solution sets that were submitted within a given time range.
//! - [`list_submissions`]: Lists submissions based on timestamp.
//! - [`latest_solution_set_failures`]: Queries the latest failures for a given solution set.
//! - [`delete_solution_set`]: Deletes a solution set and its submissions given the solution set's address.
//! - [`delete_oldest_solution_set_failures`]: Deletes the oldest solution set failures until the
//!   stored number is within a given limit.

use error::{DecodeError, QueryError};
use essential_builder_types::SolutionSetFailure;
use essential_hash::content_addr;
use essential_types::{solution::SolutionSet, ContentAddress, Hash};
#[cfg(feature = "pool")]
pub use pool::ConnectionPool;
use rusqlite::{named_params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use std::{ops::Range, time::Duration};

pub mod error;
#[cfg(feature = "pool")]
pub mod pool;
pub mod sql;

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

/// Insert a submitted solution set and the time it was received into the table.
///
/// This first inserts the solution set and its CA into the solution set table if it doesn't
/// already exist, then inserts associated timestamp into the submission table.
pub fn insert_solution_set_submission(
    tx: &Transaction,
    solution_set: &SolutionSet,
    timestamp: Duration,
) -> rusqlite::Result<ContentAddress> {
    // Insert the solution set (or ignore if exists).
    let ca = content_addr(solution_set);
    let ca_blob = &ca.0;
    let solution_set_blob = encode(solution_set);
    tx.execute(
        sql::insert::SOLUTION_SET,
        named_params! {
            ":content_addr": &ca_blob,
            ":solution_set": solution_set_blob,
        },
    )?;

    // Insert the submission timestamp.
    let secs = timestamp.as_secs();
    let nanos = timestamp.subsec_nanos();
    tx.execute(
        sql::insert::SUBMISSION,
        named_params! {
            ":solution_set_addr": ca_blob,
            ":timestamp_secs": secs,
            ":timestamp_nanos": nanos,
        },
    )?;

    Ok(ca)
}

/// Record a failure to include a solution set in a block.
///
/// We only record that a failure occurred, the block number, solution set index at which the failure
/// occurred, and a basic error message that can be returned to the submitter. If the user or
/// application requires more detailed information about the failure, the block number and solution
/// set index should be enough to reconstruct the failure with a synced node.
pub fn insert_solution_set_failure(
    conn: &Connection,
    solution_set_ca: &ContentAddress,
    solution_set_failure: SolutionSetFailure,
) -> rusqlite::Result<()> {
    conn.execute(
        sql::insert::SOLUTION_SET_FAILURE,
        named_params! {
            ":solution_set_addr": &solution_set_ca.0,
            ":attempt_block_num": solution_set_failure.attempt_block_num,
            ":attempt_block_addr": &solution_set_failure.attempt_block_addr.0,
            ":attempt_solution_set_ix": solution_set_failure.attempt_solution_set_ix,
            ":err_msg": solution_set_failure.err_msg.as_bytes(),
        },
    )?;
    Ok(())
}

/// Fetches a solution set by its content address.
pub fn get_solution_set(
    conn: &Connection,
    ca: &ContentAddress,
) -> Result<Option<SolutionSet>, QueryError> {
    let ca_blob = &ca.0;
    let mut stmt = conn.prepare(sql::query::GET_SOLUTION_SET)?;
    let solution_set_blob: Option<Vec<u8>> = stmt
        .query_row([ca_blob], |row| row.get("solution_set"))
        .optional()?;
    Ok(solution_set_blob.as_deref().map(decode).transpose()?)
}

/// List all solution sets that were submitted within the given time range.
///
/// The number of results will be limited to the given `limit`.
///
/// Note that if the same solution set was submitted multiple times within the given time range, they
/// will appear multiple times in the result.
pub fn list_solution_sets(
    conn: &Connection,
    time_range: Range<Duration>,
    limit: i64,
) -> Result<Vec<(ContentAddress, SolutionSet, Duration)>, QueryError> {
    let mut stmt = conn.prepare(sql::query::LIST_SOLUTION_SETS)?;
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
            let solution_set_addr_blob: Hash = row.get("content_addr")?;
            let solution_set_blob: Vec<u8> = row.get("solution_set")?;
            let secs: u64 = row.get("timestamp_secs")?;
            let nanos: u32 = row.get("timestamp_nanos")?;
            let timestamp = Duration::new(secs, nanos);
            Ok((
                ContentAddress(solution_set_addr_blob),
                solution_set_blob,
                timestamp,
            ))
        },
    )?;
    rows.into_iter()
        .map(|res| {
            let (ca, blob, ts) = res?;
            let solution_set = decode(&blob)?;
            Ok((ca, solution_set, ts))
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
            let solution_set_addr_blob: Hash = row.get("content_addr")?;
            let secs: u64 = row.get("timestamp_secs")?;
            let nanos: u32 = row.get("timestamp_nanos")?;
            let timestamp = Duration::new(secs, nanos);
            Ok((ContentAddress(solution_set_addr_blob), timestamp))
        },
    )?;
    rows.collect()
}

/// Query the latest solution set failures for a given solution set content address.
///
/// Results are ordered by block number and solution set index in descending order.
///
/// Returns at most `limit` failures.
pub fn latest_solution_set_failures(
    conn: &Connection,
    ca: &ContentAddress,
    limit: u32,
) -> rusqlite::Result<Vec<SolutionSetFailure<'static>>> {
    let ca_blob = &ca.0;
    let mut stmt = conn.prepare(sql::query::LATEST_SOLUTION_SET_FAILURES)?;
    let rows = stmt.query_map(
        named_params! {
            ":solution_set_addr": ca_blob,
            ":limit": limit,
        },
        |row| {
            let attempt_block_num: i64 = row.get("attempt_block_num")?;
            let attempt_block_addr: Hash = row.get("attempt_block_addr")?;
            let attempt_solution_set_ix: u32 = row.get("attempt_solution_set_ix")?;
            let err_msg_blob: Vec<u8> = row.get("err_msg")?;
            let err_msg = String::from_utf8_lossy(&err_msg_blob).into_owned();
            Ok(SolutionSetFailure {
                attempt_block_num,
                attempt_block_addr: ContentAddress(attempt_block_addr),
                attempt_solution_set_ix,
                err_msg: err_msg.into(),
            })
        },
    )?;
    rows.collect()
}

/// List the latest solution set failures.
///
/// Results are ordered by block number and solution set index in descending order.
///
/// Returns at most `limit` failures and starts at `offset`.
pub fn list_solution_set_failures(
    conn: &Connection,
    offset: u32,
    limit: u32,
) -> rusqlite::Result<Vec<SolutionSetFailure<'static>>> {
    let mut stmt = conn.prepare(sql::query::LIST_SOLUTION_SET_FAILURES)?;
    let rows = stmt.query_map(
        named_params! {
            ":offset": offset,
            ":limit": limit,
        },
        |row| {
            let attempt_block_num: i64 = row.get("attempt_block_num")?;
            let attempt_block_addr: Hash = row.get("attempt_block_addr")?;
            let attempt_solution_set_ix: u32 = row.get("attempt_solution_set_ix")?;
            let err_msg_blob: Vec<u8> = row.get("err_msg")?;
            let err_msg = String::from_utf8_lossy(&err_msg_blob).into_owned();
            Ok(SolutionSetFailure {
                attempt_block_num,
                attempt_block_addr: ContentAddress(attempt_block_addr),
                attempt_solution_set_ix,
                err_msg: err_msg.into(),
            })
        },
    )?;
    rows.collect()
}

/// Delete the solution set with the given CA from the database if it exists.
///
/// This also deletes all submissions associated with the specified solution set.
pub fn delete_solution_set(conn: &Connection, ca: &ContentAddress) -> rusqlite::Result<()> {
    let ca_blob = &ca.0;
    conn.execute(
        sql::delete::SOLUTION_SET,
        named_params! {
            ":content_addr": ca_blob,
        },
    )?;
    Ok(())
}

/// Delete the solution sets with the given CAs from the database if they exist.
///
/// This also deletes all submissions associated with the specified solution sets.
pub fn delete_solution_sets(
    tx: &Transaction,
    cas: impl IntoIterator<Item = ContentAddress>,
) -> rusqlite::Result<()> {
    for ca in cas {
        crate::delete_solution_set(tx, &ca)?;
    }
    Ok(())
}

/// Delete the oldest solution set failures until the number of stored failures
/// is less than or equal to `keep_limit`.
pub fn delete_oldest_solution_set_failures(
    conn: &Connection,
    keep_limit: u32,
) -> rusqlite::Result<()> {
    conn.execute(
        sql::delete::OLDEST_SOLUTION_SET_FAILURES,
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
