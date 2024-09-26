//! Error type declarations for block building.

use essential_builder_db as builder_db;
use essential_check::solution::PredicatesError;
use essential_node as node;
use essential_node_db as node_db;
use essential_types::{ContentAddress, Key};
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
    NodeRusqlite(#[from] node::db::AcquireThenRusqliteError),
    /// Failed to check and apply a sequence of solutions.
    #[error("Failed to check and apply solutions: {0}")]
    CheckSolutions(#[from] CheckSolutionsError),
    /// System time produced a non-monotonic timestamp.
    #[error("System time produced non-monotonic timestamp")]
    TimestampNotMonotonic,
    /// Failed to retrieve the last block header.
    #[error("Failed to retrieve the last block header")]
    LastBlockHeader(#[from] node::db::AcquireThenError<LastBlockHeaderError>),
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
    #[error("A node DB query error occurred")]
    Query(#[from] node_db::QueryError),
    /// The node DB contained no number for the last finalized block.
    #[error("The node DB contained no number for the last finalized block")]
    NoNumberForLastFinalizedBlock,
    /// The node DB contained no timestamp for the last finalized block.
    #[error("The node DB contained no timestamp for the last finalized block")]
    NoTimestampForLastFinalizedBlock,
}

/// Any errors that might occur within `check_solutions`.
#[derive(Debug, Error)]
pub enum CheckSolutionsError {
    #[error("an error occurred while attempting to apply a solution: {0}")]
    CheckSolution(#[from] CheckSolutionError),
}

/// Any errors that might occur within `crate::check_solution`.
#[derive(Debug, Error)]
pub enum CheckSolutionError {
    #[error("a rusqlite error occurred: {0}")]
    Rusqlite(#[from] rusqlite::Error),
    #[error("a node DB query failed: {0}")]
    NodeQuery(#[from] node::db::AcquireThenQueryError),
}

#[derive(Debug, Error)]
pub enum SolutionPredicatesError {
    #[error("an error occurred while querying the node DB: {0}")]
    Query(#[from] node::db::AcquireThenQueryError),
    #[error("the node DB is missing a required predicate ({0})")]
    PredicateDoesNotExist(ContentAddress),
}

/// Represents the reason why a [`Solution`][essential_types::solution::Solution] is invalid.
#[derive(Debug, Error)]
pub enum InvalidSolution {
    /// Solution specified a predicate to solve that does not exist.
    #[error("Solution specified a predicate to solve that does not exist")]
    PredicateDoesNotExist(ContentAddress),
    /// Validation of the solution predicates failed.
    #[error("Validation of the solution predicates failed: {0}")]
    Predicates(PredicatesError<StateReadError>),
}

/// Any errors that might occur in the [`Transaction`][crate::state::Transaction]'s
/// [`StateRead`][essential_check::state_read_vm::StateRead] implementation.
#[derive(Debug, Error)]
pub enum StateReadError {
    /// A state query to the underlying DB connection pool failed.
    #[error("a state query failed: {0}")]
    Query(#[from] node::db::AcquireThenQueryError),
    /// No entry exists for the given key.
    #[error("No entry exists for the given key {0:?}")]
    NoEntry(Key),
    /// Key out of range.
    #[error("A key would be out of range: `key` {key:?}, `num_values` {num_values}")]
    OutOfRange { key: Key, num_values: usize },
}
