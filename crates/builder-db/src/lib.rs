//! The database API for the block builder's solution pool and related storage.

use error::{DecodeError, QueryError};
use essential_hash::content_addr;
use essential_types::{solution::Solution, ContentAddress, Hash};
use rusqlite::{named_params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use std::{ops::Range, time::Duration};

pub mod error;
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

/// Insert a submitted solution and the time it was received into the table.
///
/// This inserts the solution into the solution table if it doesn't already
/// exist, and inserts an associated timestamp into the submission table.
pub fn insert_solution_submission(
    tx: &Transaction,
    solution: &Solution,
    timestamp: Duration,
) -> rusqlite::Result<()> {
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
