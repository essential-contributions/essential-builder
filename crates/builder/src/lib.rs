//! A block builder implementation for the Essential protocol.
//!
//! The primary entrypoint to this crate is the [`build_block_fifo`] function.

use error::{
    ApplySolutionError, ApplySolutionsError, BuildBlockError, InvalidSolution,
    LastBlockHeaderError, SolutionPredicatesError,
};
use essential_builder_db::{self as builder_db, SolutionFailure};
use essential_check::{self as check, solution::CheckPredicateConfig, state_read_vm::Gas};
use essential_node as node;
use essential_node_db as node_db;
use essential_types::{
    predicate::Predicate,
    solution::{Solution, SolutionData},
    Block, ContentAddress, PredicateAddress,
};
use std::{collections::HashMap, ops::Range, sync::Arc, time::Duration};

pub mod error;
pub mod state;

/// Block building configuration.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Config {
    /// The maximum number of solution failures to keep in the DB, used to provide feedback to the
    /// submitters.
    pub solution_failures_to_keep: u32,
    /// Configuration required by [`check::solution::check_predicates`].
    ///
    /// Wrapped in an `Arc` as this is shared between tasks.
    pub check: Arc<CheckPredicateConfig>,
}

/// A summary of building a block, returned by [`build_block_fifo`].
#[derive(Debug)]
pub struct SolutionsSummary {
    /// The addresses of all successful solutions.
    pub succeeded: Vec<(ContentAddress, Gas)>,
    /// The addresses of all failed solutions.
    pub failed: Vec<(ContentAddress, SolutionIndex, InvalidSolution)>,
}

/// The index of a solution within a block.
pub type SolutionIndex = u32;

impl Config {
    /// The default number of solution failures that the builder will retain in its DB.
    pub const DEFAULT_SOLUTION_FAILURE_KEEP_LIMIT: u32 = 10_000;
}

impl Default for Config {
    fn default() -> Self {
        Self {
            solution_failures_to_keep: Self::DEFAULT_SOLUTION_FAILURE_KEEP_LIMIT,
            check: Default::default(),
        }
    }
}

/// Naiively build a block in FIFO order.
///
/// Attempts to build a block from the available solutions in the pool in the order in which they
/// were received. No attempt is made at MEV, and solutions that don't succeed in the immediate
/// order provided are considered failed.
///
/// All solutions that are attempted (both those that succeed and those that fail) are deleted from
/// the builder's solution pool. Failed solutions are recorded to the builder's `solution_failure`
/// table, while the successful solutions can be found in the block.
///
/// An in-memory [`SolutionsSummary`] is returned that describes which solutions succeeded and
/// which ones failed for convenience.
///
/// # Example
///
/// ```no_run
/// # async fn f() -> Result<(), essential_builder::error::BuildBlockError> {
/// # let builder_conn_pool: essential_builder_db::ConnectionPool = todo!();
/// # let node_conn_pool: essential_node::db::ConnectionPool = todo!();
/// use essential_builder::{build_block_fifo, Config};
///
/// let config = Config::default();
///
/// // Build blocks in a loop.
/// loop {
///     build_block_fifo(&builder_conn_pool, &node_conn_pool, &config).await?;
///
///     // Wait some time or for an event before building next block if necessary.
/// }
/// # }
/// ```
pub async fn build_block_fifo(
    builder_conn_pool: &builder_db::ConnectionPool,
    node_conn_pool: &node::db::ConnectionPool,
    conf: &Config,
) -> Result<SolutionsSummary, BuildBlockError> {
    // Retrieve the last block header.
    let last_block_header_opt = node_conn_pool
        .acquire_then(|h| last_block_header(h))
        .await?;

    // Current timestamp as a `Duration` since `UNIX_EPOCH`.
    let block_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| BuildBlockError::TimestampNotMonotonic)?;

    // Determine the block number for this block.
    let block_number = match last_block_header_opt {
        None => 0,
        Some((last_block_num, last_block_ts)) => {
            let block_num = last_block_num
                .checked_add(1)
                .ok_or(BuildBlockError::BlockNumberOutOfRange)?;
            if block_timestamp < last_block_ts {
                return Err(BuildBlockError::TimestampNotMonotonic);
            }
            block_num
        }
    };

    // Prepare the node DB transaction for state accumulation.
    let node_tx = state::Transaction::new(node_conn_pool.clone());

    // TODO: Apply any "special" block-builder specific solutions here
    // (e.g. updating block number and timestamp in the block contract).

    // Read out the oldest solutions.
    const MAX_TIMESTAMP_RANGE: Range<Duration> =
        Duration::from_secs(0)..Duration::from_secs(i64::MAX as _);
    const LIMIT: i64 = 10_000; // TODO: Make this configurable?
    let solutions = builder_conn_pool
        .list_solutions(MAX_TIMESTAMP_RANGE, LIMIT)
        .await?;

    let solutions = solutions
        .into_iter()
        .map(|(ca, solution, _ts)| (ca, Arc::new(solution)));

    // Check and apply all solutions.
    let output = check_and_apply_solutions(node_tx, solutions, &conf.check).await?;
    let (node_tx, solutions, summary) = output;

    // Construct the block.
    let block = Block {
        number: block_number,
        timestamp: block_timestamp,
        solutions: solutions.into_iter().map(Arc::unwrap_or_clone).collect(),
    };

    // Commit the state changes, along with the constructed block.
    let (_output, _conn_pool) = node_tx
        .commit(move |tx| {
            let block_ca = essential_hash::content_addr(&block);
            node_db::insert_block(tx, &block)?;
            // FIXME: Don't immediately finalize when integrating with the L1.
            node_db::finalize_block(tx, &block_ca)?;
            Ok(())
        })
        .await?;

    // Record solution failures to the DB for submitter feedback.
    record_solution_failures(
        builder_conn_pool,
        block_number
            .try_into()
            .expect("block_number should be `i64`"),
        &summary.failed,
        conf.solution_failures_to_keep,
    )
    .await?;

    // Delete all attempted solutions, both those that succeeded and those that failed.
    let attempted: Vec<_> = summary
        .succeeded
        .iter()
        .map(|(ca, _gas)| ca.clone())
        .chain(summary.failed.iter().map(|(ca, _ix, _err)| ca.clone()))
        .collect();
    builder_conn_pool.delete_solutions(attempted).await?;

    Ok(summary)
}

/// Retrieve the last block number and its timestamp.
fn last_block_header(
    conn: &rusqlite::Connection,
) -> Result<Option<(u64, Duration)>, LastBlockHeaderError> {
    // Retrieve the last block CA.
    let block_ca = match node_db::get_latest_finalized_block_address(conn)? {
        Some(ca) => ca,
        None => return Ok(None),
    };

    // Retrieve the block's number and timestamp.
    let number = node_db::get_block_number(conn, &block_ca)?
        .ok_or(LastBlockHeaderError::NoNumberForLastFinalizedBlock)?;

    // FIXME: Change `get_block_number` to `get_block_header` and include timestamp to avoid
    // needing to list blocks like this.
    let timestamp = {
        let range = number..i64::MAX as u64;
        let blocks = node_db::list_blocks(conn, range)?;
        // We expect 1 block, but check for the error case where we get less than or more than 1.
        let mut blocks: Vec<_> = blocks.into_iter().take(2).collect();
        match blocks.len() {
            1 => blocks.pop().expect("len is 1").timestamp,
            _ => return Err(LastBlockHeaderError::NoTimestampForLastFinalizedBlock),
        }
    };

    Ok(Some((number, timestamp)))
}

/// Check and apply all solutions from the given sequence of proposed solutions.
///
/// Upon completion, the given transaction will have applied all state mutations proposed by the
/// solutions included in the returned block.
pub async fn check_and_apply_solutions(
    mut node_tx: state::Transaction,
    proposed_solutions: impl IntoIterator<Item = (ContentAddress, Arc<Solution>)>,
    check_conf: &Arc<check::solution::CheckPredicateConfig>,
) -> Result<(state::Transaction, Vec<Arc<Solution>>, SolutionsSummary), ApplySolutionsError> {
    let mut solutions = vec![];
    let mut succeeded = vec![];
    let mut failed = vec![];
    for (ix, (solution_ca, solution)) in proposed_solutions.into_iter().enumerate() {
        match check_and_apply_solution(node_tx.clone(), &solution, check_conf).await? {
            Err(invalid) => {
                let solution_ix: u32 = ix.try_into().expect("`u32::MAX` below solution limit");
                failed.push((solution_ca, solution_ix, invalid));
            }
            Ok((new_node_tx, gas)) => {
                succeeded.push((solution_ca, gas));
                solutions.push(solution);
                node_tx = new_node_tx;
            }
        }
    }
    let summary = SolutionsSummary { succeeded, failed };
    Ok((node_tx, solutions, summary))
}

/// Validate and attempt to apply the given solution based on the current state provided by
/// `pre_state` transaction.
///
/// If the solution is valid, returns the `post_state` transaction along with the total gas spent.
async fn check_and_apply_solution(
    pre_state: state::Transaction,
    solution: &Arc<Solution>,
    check_conf: &Arc<CheckPredicateConfig>,
) -> Result<Result<(state::Transaction, Gas), InvalidSolution>, ApplySolutionError> {
    // Retrieve the predicates that the solution attempts to solve.
    let predicates = match get_solution_predicates(&pre_state, &solution.data).await {
        Ok(predicates) => predicates,
        Err(SolutionPredicatesError::PredicateDoesNotExist(ca)) => {
            return Ok(Err(InvalidSolution::PredicateDoesNotExist(ca)));
        }
        Err(SolutionPredicatesError::Query(err)) => return Err(ApplySolutionError::NodeQuery(err)),
    };

    // Create the post-state and check the solution's predicates.
    let mut post_state = pre_state.clone();
    post_state.apply_mutations(&solution.data);
    match check::solution::check_predicates(
        &pre_state,
        &post_state,
        solution.clone(),
        |addr: &PredicateAddress| predicates[&addr.predicate].clone(),
        check_conf.clone(),
    )
    .await
    {
        Err(err) => Ok(Err(InvalidSolution::Predicates(err))),
        Ok((_util, gas)) => Ok(Ok((post_state, gas))),
    }
}

/// Read and return all predicates required the given solution data.
async fn get_solution_predicates(
    node_tx: &state::Transaction,
    solution_data: &[SolutionData],
) -> Result<HashMap<ContentAddress, Arc<Predicate>>, SolutionPredicatesError> {
    // Spawn concurrent queries for each predicate.
    let queries = solution_data
        .iter()
        .map(|data| data.predicate_to_solve.predicate.clone())
        .map(move |pred_ca| node_tx.clone().get_predicate(pred_ca));

    // Collect the results into a map.
    let mut map = HashMap::new();
    let results = futures::future::join_all(queries).await;
    for (data, res) in solution_data.iter().zip(results) {
        let ca = data.predicate_to_solve.predicate.clone();
        let predicate =
            res?.ok_or_else(|| SolutionPredicatesError::PredicateDoesNotExist(ca.clone()))?;
        map.insert(ca, Arc::new(predicate));
    }

    Ok(map)
}

/// Record solution failures to the DB for submitter feedback.
async fn record_solution_failures(
    builder_conn_pool: &builder_db::ConnectionPool,
    attempt_block_num: i64,
    failed: &[(ContentAddress, SolutionIndex, InvalidSolution)],
    failures_to_keep: u32,
) -> Result<(), builder_db::error::AcquireThenRusqliteError> {
    // Nothing to do if no failures.
    if failed.is_empty() {
        return Ok(());
    }

    // Construct the solution failures.
    let failures: Vec<_> = failed
        .iter()
        .map(|(ca, sol_ix, invalid)| {
            let failure = SolutionFailure {
                attempt_block_num,
                attempt_solution_ix: *sol_ix,
                err_msg: format!("{invalid}").into(),
            };
            (ca.clone(), failure)
        })
        .collect();

    // Acquire a connection, record failures and delete old failures in one transaction.
    builder_conn_pool
        .acquire_then(move |h| {
            builder_db::with_tx(h, |tx| {
                for (ca, failure) in failures {
                    builder_db::insert_solution_failure(tx, &ca, failure)?;
                }
                builder_db::delete_oldest_solution_failures(tx, failures_to_keep)
            })?;
            Ok::<_, rusqlite::Error>(())
        })
        .await?;
    Ok(())
}
