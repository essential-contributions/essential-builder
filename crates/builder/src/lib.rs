//! A block builder implementation for the Essential protocol.
//!
//! The primary entrypoint to this crate is the [`build_block_fifo`] function.

use error::{
    BuildBlockError, CheckSolutionError, CheckSolutionsError, InvalidSolution,
    LastBlockHeaderError, QueryPredicateError, SolutionPredicatesError,
};
use essential_builder_db::{self as builder_db};
use essential_builder_types::SolutionFailure;
use essential_check::{self as check, solution::CheckPredicateConfig, state_read_vm::Gas};
pub use essential_node as node;
use essential_node_db as node_db;
use essential_node_types::{block_state_solution, BigBang};
use essential_types::{
    predicate::Predicate,
    solution::{Solution, SolutionData},
    Block, ContentAddress, PredicateAddress, Word,
};
use std::{collections::HashMap, num::NonZero, ops::Range, sync::Arc, time::Duration};

pub mod error;
pub mod state;

/// Block building configuration.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Config {
    /// The maximum number of solution failures to keep in the DB, used to provide feedback to the
    /// submitters.
    ///
    /// Defaults to [`Config::DEFAULT_SOLUTION_FAILURE_KEEP_LIMIT`].
    pub solution_failures_to_keep: u32,
    /// The maximum number of solutions to attempt to check and include in a block.
    ///
    /// Defaults to [`Config::DEFAULT_SOLUTION_ATTEMPTS_PER_BLOCK`].
    pub solution_attempts_per_block: NonZero<u32>,
    /// The number of sequential solutions to attempt to check in parallel at a time.
    ///
    /// If greater than `solution_attempts_per_block`, the `solution_attempts_per_block`
    /// is used instead.
    ///
    /// If unspecified, uses `num_cpus::get()`.
    pub parallel_chunk_size: NonZero<usize>,
    /// Configuration required by [`check::solution::check_predicates`].
    ///
    /// Wrapped in an `Arc` as this is shared between tasks.
    pub check: Arc<CheckPredicateConfig>,
    /// The address of the big bang contract registry contract and its predicate.
    pub contract_registry: PredicateAddress,
    /// The address of the big bang block state contract and its predicate.
    pub block_state: PredicateAddress,
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

type BlockNum = i64;

impl Config {
    /// The default number of solution failures that the builder will retain in its DB.
    pub const DEFAULT_SOLUTION_FAILURE_KEEP_LIMIT: u32 = 10_000;
    /// The default max number of solutions to attempt to check and include in a block.
    pub const DEFAULT_SOLUTION_ATTEMPTS_PER_BLOCK: u32 = 10_000;

    /// The default number of sequential solutions to attempt to check in parallel.
    pub fn default_parallel_chunk_size() -> NonZero<usize> {
        num_cpus::get()
            .try_into()
            .expect("`num_cpus::get()` must be non-zero")
    }
}

impl Default for Config {
    fn default() -> Self {
        let big_bang = BigBang::default();
        Self {
            solution_failures_to_keep: Self::DEFAULT_SOLUTION_FAILURE_KEEP_LIMIT,
            solution_attempts_per_block: Self::DEFAULT_SOLUTION_ATTEMPTS_PER_BLOCK
                .try_into()
                .expect("declared const must be non-zero"),
            parallel_chunk_size: Self::default_parallel_chunk_size(),
            contract_registry: big_bang.contract_registry,
            block_state: big_bang.block_state,
            check: Default::default(),
        }
    }
}

/// Naively build a block in FIFO order.
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
#[cfg_attr(feature = "tracing", tracing::instrument("build", skip_all))]
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
            if block_timestamp <= last_block_ts {
                return Err(BuildBlockError::TimestampNotMonotonic);
            }
            block_num
        }
    };

    #[cfg(feature = "tracing")]
    tracing::debug!("Building block {}", block_number);

    // TODO: Produce any "special" block-builder specific solutions here
    // (e.g. updating block number and timestamp in the block contract).
    let block_secs: Word = block_timestamp
        .as_secs()
        .try_into()
        .map_err(|_| BuildBlockError::TimestampOutOfRange)?;
    let sol_data = block_state_solution(conf.block_state.clone(), block_number, block_secs);
    let solution = Solution {
        data: vec![sol_data],
    };
    let ca = essential_hash::content_addr(&solution);
    let mut solutions = vec![(ca, Arc::new(solution))];

    // Read out the oldest solutions.
    const MAX_TIMESTAMP_RANGE: Range<Duration> =
        Duration::from_secs(0)..Duration::from_secs(i64::MAX as _);
    let limit = i64::from(u32::from(conf.solution_attempts_per_block));
    solutions.extend(
        builder_conn_pool
            .list_solutions(MAX_TIMESTAMP_RANGE, limit)
            .await?
            .into_iter()
            .map(|(ca, solution, _ts)| (ca, Arc::new(solution))),
    );

    // Check all solutions.
    let (solutions, summary) =
        check_solutions(node_conn_pool.clone(), block_number, &solutions, conf).await?;

    // Construct the block.
    let block = Block {
        number: block_number,
        timestamp: block_timestamp,
        solutions: solutions.into_iter().map(Arc::unwrap_or_clone).collect(),
    };
    let block_addr = essential_hash::content_addr(&block);
    #[cfg(feature = "tracing")]
    tracing::debug!(
        "Built block {} with {} solutions at {:?}",
        block_addr,
        block.solutions.len(),
        block.timestamp
    );

    // If the block is empty, notify that we're skipping the block.
    // FIXME: This uses `<= 1` because the first solution is the block state solution.
    // This should be refactored.
    if block.solutions.len() <= 1 {
        #[cfg(feature = "tracing")]
        tracing::debug!("Skipping empty block {}", block_addr);

    // Only insert the block if
    } else {
        // Commit the new block to the node DB.
        // FIXME: Don't immediately insert and finalize when integrating with the L1.
        node_conn_pool
            .acquire_then(|conn| {
                builder_db::with_tx(conn, move |tx| {
                    let block_ca = essential_hash::content_addr(&block);
                    node_db::insert_block(tx, &block)?;
                    node_db::finalize_block(tx, &block_ca)
                })
            })
            .await?;
        #[cfg(feature = "tracing")]
        tracing::debug!("Committed and finalized block {}", block_addr);
    }

    // Record solution failures to the DB for submitter feedback.
    let failures: Vec<_> = summary
        .failed
        .iter()
        .map(|(ca, sol_ix, invalid)| {
            let failure = SolutionFailure {
                attempt_block_num: block_number,
                attempt_block_addr: block_addr.clone(),
                attempt_solution_ix: *sol_ix,
                err_msg: format!("{invalid}").into(),
            };
            (ca.clone(), failure)
        })
        .collect();
    let failures_to_keep = conf.solution_failures_to_keep;

    // Delete all attempted solutions, both those that succeeded and those that failed.
    let attempted: Vec<_> = summary
        .succeeded
        .iter()
        .map(|(ca, _gas)| ca.clone())
        .chain(summary.failed.iter().map(|(ca, _ix, _err)| ca.clone()))
        .collect();

    builder_conn_pool
        .acquire_then(move |conn| {
            builder_db::with_tx(conn, |tx| {
                record_solution_failures(tx, failures, failures_to_keep)?;
                builder_db::delete_solutions(tx, attempted)
            })
        })
        .await?;

    Ok(summary)
}

/// Retrieve the last block number and its timestamp.
fn last_block_header(
    conn: &rusqlite::Connection,
) -> Result<Option<(Word, Duration)>, LastBlockHeaderError> {
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
        // We expect 1 block, but check for the error case where we get less than or more than 1.
        let range = number..number.saturating_add(2);
        let blocks = node_db::list_blocks(conn, range)?;
        let mut blocks: Vec<_> = blocks.into_iter().take(2).collect();
        match blocks.len() {
            1 => blocks.pop().expect("len is 1").timestamp,
            _ => return Err(LastBlockHeaderError::NoTimestampForLastFinalizedBlock),
        }
    };

    Ok(Some((number, timestamp)))
}

/// Check the given sequence of proposed solutions.
///
/// We optimistically check `conf.parallel_chunk_size` solutions in parallel at a time.
/// This gives us the benefit of parallel checking, while capping the number of following solutions
/// that must be re-checked in the case that one of the solutions earlier in the chunk fails to
/// validate.
async fn check_solutions(
    node_conn_pool: node::db::ConnectionPool,
    block_num: BlockNum,
    proposed_solutions: &[(ContentAddress, Arc<Solution>)],
    conf: &Config,
) -> Result<(Vec<Arc<Solution>>, SolutionsSummary), CheckSolutionsError> {
    let chunk_size = conf.parallel_chunk_size.into();
    let mut solutions = vec![];
    let mut succeeded = vec![];
    let mut failed = vec![];
    let mut mutations = state::Mutations::default();

    // On each pass we process a chunk at a time.
    // If there's a failure, the next chunk starts after the first failure in this chunk.
    let mut chunk_start = 0;
    while chunk_start < proposed_solutions.len() {
        // The range of the chunk of solutions to check on this pass.
        let chunk_end = chunk_start
            .saturating_add(chunk_size)
            .min(proposed_solutions.len());
        let range = chunk_start..chunk_end;
        let chunk = &proposed_solutions[range.clone()];

        // Apply the mutations from this chunk of solutions.
        mutations.extend(range.clone().zip(chunk.iter().map(|(_, s)| &**s)));
        // Temporarily move mutations behind a share-able `Arc`.
        let mutations_arc = Arc::new(std::mem::take(&mut mutations));

        // Check the chunk in parallel.
        let results = check_solution_chunk(
            &node_conn_pool,
            block_num,
            &mutations_arc,
            range.clone().zip(chunk.iter().map(|(_, s)| s.clone())),
            &conf.contract_registry.contract,
            &conf.check,
        )
        .await?;

        // Re-take ownership of the mutations.
        // We know this is unique as `check_solution_chunk` has joined.
        debug_assert_eq!(
            Arc::strong_count(&mutations_arc),
            1,
            "`Arc<Mutations>` not unique"
        );
        mutations = Arc::unwrap_or_clone(mutations_arc);

        // Process the results.
        for (sol_ix, (res, (sol_ca, sol))) in range.zip(results.into_iter().zip(chunk)) {
            chunk_start += 1;
            match res {
                Ok(gas) => {
                    succeeded.push((sol_ca.clone(), gas));
                    solutions.push(sol.clone());
                    #[cfg(feature = "tracing")]
                    tracing::trace!("Solution check success {}", sol_ca);
                }
                // If a solution was invalid, remove its mutations.
                Err(invalid) => {
                    mutations.remove_solution(sol_ix);
                    let sol_ix: u32 = sol_ix.try_into().expect("`u32::MAX` below solution limit");
                    failed.push((sol_ca.clone(), sol_ix, invalid));
                    #[cfg(feature = "tracing")]
                    tracing::trace!("Solution check failure {}", sol_ca);
                    break;
                }
            }
        }
    }
    let summary = SolutionsSummary { succeeded, failed };
    Ok((solutions, summary))
}

/// Check a sequential chunk of solutions in parallel.
async fn check_solution_chunk(
    node_conn_pool: &node::db::ConnectionPool,
    block_num: BlockNum,
    proposed_mutations: &Arc<state::Mutations>,
    chunk: impl IntoIterator<Item = (usize, Arc<Solution>)>,
    contract_registry: &ContentAddress,
    check_conf: &Arc<check::solution::CheckPredicateConfig>,
) -> Result<Vec<Result<Gas, InvalidSolution>>, CheckSolutionsError> {
    // Spawn concurrent checks for each solution.
    let checks: tokio::task::JoinSet<_> = chunk
        .into_iter()
        .map(move |(sol_ix, solution)| {
            let mutations = proposed_mutations.clone();
            let conn_pool = node_conn_pool.clone();
            let check_conf = check_conf.clone();
            let registry = contract_registry.clone();
            let (pre, post) = state::pre_and_post_view(conn_pool, mutations, block_num, sol_ix);
            async move {
                let res = check_solution(solution.clone(), pre, post, &registry, check_conf).await;
                (sol_ix, res)
            }
        })
        .collect();

    // Await the results.
    let mut results = checks.join_all().await;
    results.sort_by_key(|&(ix, _)| ix);
    results
        .into_iter()
        .map(|(_ix, res)| res.map_err(CheckSolutionsError::CheckSolution))
        .collect()
}

/// Validate the given solution.
///
/// If the solution is valid, returns the total gas spent.
async fn check_solution(
    solution: Arc<Solution>,
    pre_state: state::View,
    post_state: state::View,
    contract_registry: &ContentAddress,
    check_conf: Arc<CheckPredicateConfig>,
) -> Result<Result<Gas, InvalidSolution>, CheckSolutionError> {
    // Retrieve the predicates that the solution attempts to solve from the post-state. This
    // ensures that the solution has access to contracts submitted as a part of the solution.
    let predicates =
        match get_solution_predicates(contract_registry, &post_state, &solution.data).await {
            Ok(predicates) => predicates,
            Err(SolutionPredicatesError::PredicateDoesNotExist(ca)) => {
                return Ok(Err(InvalidSolution::PredicateDoesNotExist(ca)));
            }
            Err(SolutionPredicatesError::QueryPredicate(err)) => match err {
                QueryPredicateError::Decode(_)
                | QueryPredicateError::MissingLenBytes
                | QueryPredicateError::InvalidLenBytes => {
                    return Ok(Err(InvalidSolution::PredicateInvalid));
                }
                QueryPredicateError::ConnPoolQuery(err) => {
                    return Err(CheckSolutionError::NodeQuery(err))
                }
            },
        };

    // Create the post-state and check the solution's predicates.
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
        Ok(gas) => Ok(Ok(gas)),
    }
}

/// Read and return all predicates required the given solution data.
async fn get_solution_predicates(
    contract_registry: &ContentAddress,
    view: &state::View,
    solution_data: &[SolutionData],
) -> Result<HashMap<ContentAddress, Arc<Predicate>>, SolutionPredicatesError> {
    // Spawn concurrent queries for each predicate.
    let queries: tokio::task::JoinSet<_> = solution_data
        .iter()
        .map(|data| data.predicate_to_solve.clone())
        .enumerate()
        .map(move |(ix, pred_addr)| {
            let view = view.clone();
            let registry = contract_registry.clone();
            async move {
                let pred = view.get_predicate(registry, &pred_addr).await;
                (ix, pred)
            }
        })
        .collect();

    // Collect the results into a map.
    let mut map = HashMap::new();
    let mut results = queries.join_all().await;
    results.sort_by_key(|(ix, _)| *ix);
    for (data, (_ix, res)) in solution_data.iter().zip(results) {
        let ca = data.predicate_to_solve.predicate.clone();
        let predicate =
            res?.ok_or_else(|| SolutionPredicatesError::PredicateDoesNotExist(ca.clone()))?;
        map.insert(ca, Arc::new(predicate));
    }

    Ok(map)
}

/// Record solution failures to the DB for submitter feedback.
fn record_solution_failures(
    builder_tx: &mut rusqlite::Transaction,
    failures: Vec<(ContentAddress, SolutionFailure)>,
    failures_to_keep: u32,
) -> rusqlite::Result<()> {
    // Nothing to do if no failures.
    if failures.is_empty() {
        return Ok(());
    }
    // Acquire a connection, record failures and delete old failures in one transaction.
    for (ca, failure) in failures {
        builder_db::insert_solution_failure(builder_tx, &ca, failure)?;
    }
    builder_db::delete_oldest_solution_failures(builder_tx, failures_to_keep)
}
