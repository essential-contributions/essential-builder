use essential_builder_db as builder_db;
use essential_check::solution::PredicatesError;
use essential_node as node;
use essential_node_db as node_db;
use essential_types::ContentAddress;
use crate::state::StateReadError;
use thiserror::Error;

/// Any errors that might occur within [`build_block_fifo`].
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
    ApplySolutions(#[from] ApplySolutionsError),
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

/// Any errors that might occur within [`check_and_apply_solutions`].
#[derive(Debug, Error)]
pub enum ApplySolutionsError {
    #[error("an error occurred while attempting to apply a solution: {0}")]
    ApplySolution(#[from] ApplySolutionError),
}

/// Any errors that might occur within [`check_and_apply_solution`].
#[derive(Debug, Error)]
pub enum ApplySolutionError {
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

/// Represents the reason why a [`Solution`] is invalid.
#[derive(Debug, Error)]
pub enum InvalidSolution {
    /// Solution specified a predicate to solve that does not exist.
    #[error("Solution specified a predicate to solve that does not exist")]
    PredicateDoesNotExist(ContentAddress),
    /// Validation of the solution predicates failed.
    #[error("Validation of the solution predicates failed: {0}")]
    Predicates(PredicatesError<StateReadError>),
}
