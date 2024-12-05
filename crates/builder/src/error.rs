//! Error type declarations for block building.

use essential_builder_db as builder_db;
use essential_check::solution::PredicatesError;
use essential_node as node;
use essential_node_db as node_db;
use essential_types::{predicate::PredicateDecodeError, ContentAddress, Key};
use thiserror::Error;

/// Any errors that might occur within [`crate::build_block_fifo`].
#[derive(Debug, Error)]
pub enum BuildBlockError {
    /// A builder DB query error occurred.
    #[error("A builder DB query error occurred: {0}")]
    BuilderQuery(#[from] builder_db::error::AcquireThenQueryError),
    /// A builder DB rusqlite error occurred.
    #[error("A builder DB rusqlite error occurred: {0}")]
    BuilderRusqlite(#[from] builder_db::error::AcquireThenRusqliteError),
    /// A node DB rusqlite error occurred.
    #[error("A node DB rusqlite error occurred: {0}")]
    NodeRusqlite(#[from] node::db::pool::AcquireThenRusqliteError),
    /// Failed to check and apply a sequence of solution sets.
    #[error("Failed to check and apply solution sets: {0}")]
    CheckSets(#[from] CheckSetsError),
    /// System time produced a non-monotonic timestamp.
    #[error("System time produced non-monotonic timestamp")]
    TimestampNotMonotonic,
    /// System time is out of range of `Word`.
    #[error("System timestamp is out of range of `Word`")]
    TimestampOutOfRange,
    /// Failed to retrieve the last block header.
    #[error("Failed to retrieve the last block header")]
    LastBlockHeader(#[from] node::db::pool::AcquireThenError<LastBlockHeaderError>),
    /// The next block number would be out of `u64` range.
    #[error("The next block number would be out of `u64` range")]
    BlockNumberOutOfRange,
}

/// Errors that can occur while retrieving the last block header.
#[derive(Debug, Error)]
pub enum LastBlockHeaderError {
    /// A rusqlite error occurred.
    #[error("A rusqlite error occurred: {0}")]
    Rusqlite(#[from] rusqlite::Error),
    /// A node DB query error occurred.
    #[error("A node DB query error occurred: {0}")]
    Query(#[from] node_db::QueryError),
    /// The node DB contained no number for the last finalized block.
    #[error("The node DB contained no number for the last finalized block")]
    NoNumberForLastFinalizedBlock,
    /// The node DB contained no timestamp for the last finalized block.
    #[error("The node DB contained no timestamp for the last finalized block")]
    NoTimestampForLastFinalizedBlock,
}

/// Any errors that might occur within `check_sets`.
#[derive(Debug, Error)]
pub enum CheckSetsError {
    /// An error occurred while checking a solution set.
    #[error("an error occurred while attempting to apply a set: {0}")]
    CheckSolution(#[from] CheckSetError),
}

/// Any errors that might occur within `crate::check_set`.
#[derive(Debug, Error)]
pub enum CheckSetError {
    /// A rusqlite error occurred.
    #[error("a rusqlite error occurred: {0}")]
    Rusqlite(#[from] rusqlite::Error),
    /// A node DB query failed.
    #[error("a node DB query failed: {0}")]
    NodeQuery(#[from] node::db::pool::AcquireThenQueryError),
}

/// An error occurred while fetching a solution set's predicates.
#[derive(Debug, Error)]
pub enum SetPredicatesError {
    /// An error occurred while querying the node DB.
    #[error("an error occurred while querying for a predicate from the node DB: {0}")]
    QueryPredicate(#[from] QueryPredicateError),
    /// The node DB is missing a required predicate.
    #[error("the node DB is missing a required predicate ({0})")]
    PredicateDoesNotExist(ContentAddress),
}

/// An error occurred while fetching a predicate's programs.
#[derive(Debug, Error)]
pub enum PredicateProgramsError {
    /// An error occurred while querying the node DB.
    #[error("an error occurred while querying for a program from the node DB: {0}")]
    QueryProgram(#[from] QueryProgramError),
    /// The node DB is missing a required predicate.
    #[error("the node DB is missing a required program ({0})")]
    ProgramDoesNotExist(ContentAddress),
}

/// Represents the reason why a [`SolutionSet`][essential_types::solution::SolutionSet] is invalid.
#[derive(Debug, Error)]
pub enum InvalidSet {
    /// Solution set specified a predicate to solve that does not exist.
    #[error("Solution set specified a predicate to solve that does not exist")]
    PredicateDoesNotExist(ContentAddress),
    /// Solution set contains a predicate that specified a program that does not exist.
    #[error("Solution set contains a predicate that specified a program that does not exist")]
    ProgramDoesNotExist(ContentAddress),
    /// Solution set specified a predicate that exists, but was invalid when reading from contract
    /// registry state.
    #[error(
        "Solution set specified a predicate that was invalid when reading from contract registry state"
    )]
    PredicateInvalid,
    /// Solution set contains a predicate that specified a program that exists,
    /// but was invalid when reading from program registry state.
    #[error(
        "Solution set contains a predicate that specified a program that was invalid when reading from program registry state"
    )]
    ProgramInvalid,
    /// Validation of the solution set predicates failed.
    #[error("Validation of the solution set predicates failed: {0}")]
    Predicates(PredicatesError<StateReadError>),
}

/// Any errors that might occur in the [`View`][crate::state::View]'s
/// [`StateRead`][essential_check::state_read_vm::StateRead] implementation.
#[derive(Debug, Error)]
pub enum StateReadError {
    /// A state query to the underlying DB connection pool failed.
    #[error("a state query failed: {0}")]
    Query(#[from] node::db::pool::AcquireThenQueryError),
    /// No entry exists for the given key.
    #[error("No entry exists for the given key {0:?}")]
    NoEntry(Key),
    /// Key out of range.
    #[error("A key would be out of range: `key` {key:?}, `num_values` {num_values}")]
    OutOfRange { key: Key, num_values: usize },
}

/// Any errors that might occur while querying for predicates.
#[derive(Debug, Error)]
pub enum QueryPredicateError {
    /// A DB query failure occurred.
    #[error("failed to query the node DB: {0}")]
    ConnPoolQuery(#[from] node::db::pool::AcquireThenQueryError),
    /// The queried predicate is missing the word that encodes its length.
    #[error("the queried predicate is missing the word that encodes its length")]
    MissingLenBytes,
    /// The queried predicate length was invalid.
    #[error("the queried predicate length was invalid")]
    InvalidLenBytes,
    /// Failed to decode the queried predicate.
    #[error("failed to decode the queried predicate: {0}")]
    Decode(#[from] PredicateDecodeError),
}

/// Any errors that might occur while querying for programs.
#[derive(Debug, Error)]
pub enum QueryProgramError {
    /// A DB query failure occurred.
    #[error("failed to query the node DB: {0}")]
    ConnPoolQuery(#[from] node::db::pool::AcquireThenQueryError),
    /// The queried program is missing the word that encodes its length.
    #[error("the queried program is missing the word that encodes its length")]
    MissingLenBytes,
    /// The queried program length was invalid.
    #[error("the queried program length was invalid")]
    InvalidLenBytes,
}
