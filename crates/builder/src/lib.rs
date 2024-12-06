//! A block builder implementation for the Essential protocol.
//!
//! The primary entrypoint to this crate is the [`build_block_fifo`] function.

use error::{
    BuildBlockError, CheckSetError, CheckSetsError, InvalidSet, LastBlockHeaderError,
    PredicateProgramsError, QueryPredicateError, QueryProgramError, SetPredicatesError,
};
use essential_builder_db::{self as builder_db};
use essential_builder_types::SolutionSetFailure;
use essential_check::{self as check, solution::CheckPredicateConfig, vm::Gas};
pub use essential_node as node;
use essential_node_db as node_db;
use essential_node_types::{block_state_solution, BigBang, Block, BlockHeader};
use essential_types::{
    predicate::Predicate,
    solution::{Solution, SolutionSet},
    ContentAddress, PredicateAddress, Program, Word,
};
use std::{collections::HashMap, num::NonZero, ops::Range, sync::Arc, time::Duration};

pub mod error;
pub mod state;

/// Block building configuration.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Config {
    /// The maximum number of solution set failures to keep in the DB, used to provide feedback to the
    /// submitters.
    ///
    /// Defaults to [`Config::DEFAULT_SOLUTION_SET_FAILURE_KEEP_LIMIT`].
    pub solution_set_failures_to_keep: u32,
    /// The maximum number of solution sets to attempt to check and include in a block.
    ///
    /// Defaults to [`Config::DEFAULT_SOLUTION_SET_ATTEMPTS_PER_BLOCK`].
    pub solution_set_attempts_per_block: NonZero<u32>,
    /// The number of sequential solution sets to attempt to check in parallel at a time.
    ///
    /// If greater than `solution_set_attempts_per_block`, the `solution_set_attempts_per_block`
    /// is used instead.
    ///
    /// If unspecified, uses `num_cpus::get()`.
    pub parallel_chunk_size: NonZero<usize>,
    /// Configuration required by [`check::solution::check_set_predicates`].
    ///
    /// Wrapped in an `Arc` as this is shared between tasks.
    pub check: Arc<CheckPredicateConfig>,
    /// The address of the big bang contract registry contract and its predicate.
    pub contract_registry: PredicateAddress,
    /// The address of the big bang program registry contract and its predicate.
    pub program_registry: PredicateAddress,
    /// The address of the big bang block state contract and its predicate.
    pub block_state: PredicateAddress,
}

/// A summary of building a block, returned by [`build_block_fifo`].
#[derive(Debug)]
pub struct SolutionSetsSummary {
    /// The addresses of all successful solution sets.
    pub succeeded: Vec<(ContentAddress, Gas)>,
    /// The addresses of all failed solution sets.
    pub failed: Vec<(ContentAddress, SolutionSetIndex, InvalidSet)>,
}

/// The index of a solution set within a block.
pub type SolutionSetIndex = u32;

type BlockNum = i64;

impl Config {
    /// The default number of solution set failures that the builder will retain in its DB.
    pub const DEFAULT_SOLUTION_SET_FAILURE_KEEP_LIMIT: u32 = 10_000;
    /// The default max number of solution sets to attempt to check and include in a block.
    pub const DEFAULT_SOLUTION_SET_ATTEMPTS_PER_BLOCK: u32 = 10_000;

    /// The default number of sequential solution sets to attempt to check in parallel.
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
            solution_set_failures_to_keep: Self::DEFAULT_SOLUTION_SET_FAILURE_KEEP_LIMIT,
            solution_set_attempts_per_block: Self::DEFAULT_SOLUTION_SET_ATTEMPTS_PER_BLOCK
                .try_into()
                .expect("declared const must be non-zero"),
            parallel_chunk_size: Self::default_parallel_chunk_size(),
            contract_registry: big_bang.contract_registry,
            program_registry: big_bang.program_registry,
            block_state: big_bang.block_state,
            check: Default::default(),
        }
    }
}

/// Naively build a block in FIFO order.
///
/// Attempts to build a block from the available solution sets in the pool in the order in which they
/// were received. No attempt is made at MEV, and solution sets that don't succeed in the immediate
/// order provided are considered failed.
///
/// All solution sets that are attempted (both those that succeed and those that fail) are deleted from
/// the builder's solution set pool. Failed solution sets are recorded to the builder's `solution_set_failure`
/// table, while the successful solution sets can be found in the block.
///
/// Returns the address of the block if one was successfully created alongside an in-memory
/// [`SetsSummary`] that describes which solution sets succeeded and which ones failed for
/// convenience.
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
) -> Result<(Option<ContentAddress>, SolutionSetsSummary), BuildBlockError> {
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
        Some(BlockHeader {
            number: last_block_num,
            timestamp: last_block_ts,
        }) => {
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
    let solution = block_state_solution(conf.block_state.clone(), block_number, block_secs);
    let solution_set = SolutionSet {
        solutions: vec![solution],
    };
    let ca = essential_hash::content_addr(&solution_set);
    let mut solution_sets = vec![(ca, Arc::new(solution_set))];

    // Read out the oldest solution sets.
    const MAX_TIMESTAMP_RANGE: Range<Duration> =
        Duration::from_secs(0)..Duration::from_secs(i64::MAX as _);
    let limit = i64::from(u32::from(conf.solution_set_attempts_per_block));
    solution_sets.extend(
        builder_conn_pool
            .list_solution_sets(MAX_TIMESTAMP_RANGE, limit)
            .await?
            .into_iter()
            .map(|(ca, solution_set, _ts)| (ca, Arc::new(solution_set))),
    );

    // Check all solution sets.
    let (solution_sets, summary) =
        check_solution_sets(node_conn_pool.clone(), block_number, &solution_sets, conf).await?;

    // Construct the block.
    let block = Block {
        header: BlockHeader {
            number: block_number,
            timestamp: block_timestamp,
        },
        solution_sets: solution_sets
            .into_iter()
            .map(Arc::unwrap_or_clone)
            .collect(),
    };
    let block_addr = essential_hash::content_addr(&block);
    #[cfg(feature = "tracing")]
    tracing::debug!(
        "Built block {} with {} solution sets at {:?}",
        block_addr,
        block.solution_sets.len(),
        block.header.timestamp
    );

    // If the block is empty, notify that we're skipping the block.
    // FIXME: This uses `<= 1` because the first solution set is the block state solution set.
    // This should be refactored.
    let skip_block = block.solution_sets.len() <= 1;
    if skip_block {
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

    // Record solution set failures to the DB for submitter feedback.
    let failures: Vec<_> = summary
        .failed
        .iter()
        .map(|(ca, set_ix, invalid)| {
            let failure = SolutionSetFailure {
                attempt_block_num: block_number,
                attempt_block_addr: block_addr.clone(),
                attempt_solution_set_ix: *set_ix,
                err_msg: format!("{invalid}").into(),
            };
            (ca.clone(), failure)
        })
        .collect();
    let failures_to_keep = conf.solution_set_failures_to_keep;

    // Delete all attempted solution sets, both those that succeeded and those that failed.
    let attempted: Vec<_> = summary
        .succeeded
        .iter()
        .map(|(ca, _gas)| ca.clone())
        .chain(summary.failed.iter().map(|(ca, _ix, _err)| ca.clone()))
        .collect();

    builder_conn_pool
        .acquire_then(move |conn| {
            builder_db::with_tx(conn, |tx| {
                record_solution_set_failures(tx, failures, failures_to_keep)?;
                builder_db::delete_solution_sets(tx, attempted)
            })
        })
        .await?;

    let block_addr = if skip_block { None } else { Some(block_addr) };
    Ok((block_addr, summary))
}

/// Retrieve the header for the last block.
///
/// Returns the block number and block timestamp in that order.
fn last_block_header(
    conn: &rusqlite::Connection,
) -> Result<Option<BlockHeader>, LastBlockHeaderError> {
    // Retrieve the last block CA.
    let block_ca = match node_db::get_latest_finalized_block_address(conn)? {
        Some(ca) => ca,
        None => return Ok(None),
    };

    // Retrieve the block's number and timestamp.
    let header = node_db::get_block_header(conn, &block_ca)?
        .ok_or(LastBlockHeaderError::NoNumberForLastFinalizedBlock)?;

    Ok(Some(header))
}

/// Check the given sequence of proposed solution sets.
///
/// We optimistically check `conf.parallel_chunk_size` solution sets in parallel at a time.
/// This gives us the benefit of parallel checking, while capping the number of following solution
/// sets that must be re-checked in the case that one of the solution sets earlier in the chunk fails to
/// validate.
async fn check_solution_sets(
    node_conn_pool: node::db::ConnectionPool,
    block_num: BlockNum,
    proposed_solution_sets: &[(ContentAddress, Arc<SolutionSet>)],
    conf: &Config,
) -> Result<(Vec<Arc<SolutionSet>>, SolutionSetsSummary), CheckSetsError> {
    let chunk_size = conf.parallel_chunk_size.into();
    let mut solution_sets = vec![];
    let mut succeeded = vec![];
    let mut failed = vec![];
    let mut mutations = state::Mutations::default();

    // On each pass we process a chunk at a time.
    // If there's a failure, the next chunk starts after the first failure in this chunk.
    let mut chunk_start = 0;
    while chunk_start < proposed_solution_sets.len() {
        // The range of the chunk of solution sets to check on this pass.
        let chunk_end = chunk_start
            .saturating_add(chunk_size)
            .min(proposed_solution_sets.len());
        let range = chunk_start..chunk_end;
        let chunk = &proposed_solution_sets[range.clone()];

        // Apply the mutations from this chunk of solution sets.
        mutations.extend(range.clone().zip(chunk.iter().map(|(_, s)| &**s)));
        // Temporarily move mutations behind a share-able `Arc`.
        let mutations_arc = Arc::new(std::mem::take(&mut mutations));

        // Check the chunk in parallel.
        let results = check_solution_set_chunk(
            &node_conn_pool,
            block_num,
            &mutations_arc,
            range.clone().zip(chunk.iter().map(|(_, s)| s.clone())),
            &conf.contract_registry.contract,
            &conf.program_registry.contract,
            &conf.check,
        )
        .await?;

        // Re-take ownership of the mutations.
        // We know this is unique as `check_solution_set_chunk` has joined.
        debug_assert_eq!(
            Arc::strong_count(&mutations_arc),
            1,
            "`Arc<Mutations>` not unique"
        );
        mutations = Arc::unwrap_or_clone(mutations_arc);

        // Process the results.
        for (set_ix, (res, (set_ca, set))) in range.zip(results.into_iter().zip(chunk)) {
            chunk_start += 1;
            match res {
                Ok(gas) => {
                    succeeded.push((set_ca.clone(), gas));
                    solution_sets.push(set.clone());
                    #[cfg(feature = "tracing")]
                    tracing::trace!("Solution set check success {}", set_ca);
                }
                // If a solution set was invalid, remove its mutations.
                Err(invalid) => {
                    mutations.remove_solution_set(set_ix);
                    let set_ix: u32 = set_ix
                        .try_into()
                        .expect("`u32::MAX` below solution set limit");
                    failed.push((set_ca.clone(), set_ix, invalid));
                    #[cfg(feature = "tracing")]
                    tracing::trace!("Solution set check failure {}", set_ca);
                    break;
                }
            }
        }
    }
    let summary = SolutionSetsSummary { succeeded, failed };
    Ok((solution_sets, summary))
}

/// Check a sequential chunk of solution sets in parallel.
async fn check_solution_set_chunk(
    node_conn_pool: &node::db::ConnectionPool,
    block_num: BlockNum,
    proposed_mutations: &Arc<state::Mutations>,
    chunk: impl IntoIterator<Item = (usize, Arc<SolutionSet>)>,
    contract_registry: &ContentAddress,
    program_registry: &ContentAddress,
    check_conf: &Arc<check::solution::CheckPredicateConfig>,
) -> Result<Vec<Result<Gas, InvalidSet>>, CheckSetsError> {
    // Spawn concurrent checks for each solution set.
    let checks: tokio::task::JoinSet<_> = chunk
        .into_iter()
        .map(move |(set_ix, set)| {
            let mutations = proposed_mutations.clone();
            let conn_pool = node_conn_pool.clone();
            let check_conf = check_conf.clone();
            let contract_registry = contract_registry.clone();
            let program_registry = program_registry.clone();
            let (pre, post) = state::pre_and_post_view(conn_pool, mutations, block_num, set_ix);
            async move {
                let res = check_set(
                    set.clone(),
                    pre,
                    post,
                    &contract_registry,
                    &program_registry,
                    check_conf,
                )
                .await;
                (set_ix, res)
            }
        })
        .collect();

    // Await the results.
    let mut results = checks.join_all().await;
    results.sort_by_key(|&(ix, _)| ix);
    results
        .into_iter()
        .map(|(_ix, res)| res.map_err(CheckSetsError::CheckSolution))
        .collect()
}

/// Validate the given solution set.
///
/// If the solution set is valid, returns the total gas spent.
async fn check_set(
    solution_set: Arc<SolutionSet>,
    pre_state: state::View,
    post_state: state::View,
    contract_registry: &ContentAddress,
    program_registry: &ContentAddress,
    check_conf: Arc<CheckPredicateConfig>,
) -> Result<Result<Gas, InvalidSet>, CheckSetError> {
    // Retrieve the predicates that the solution set attempts to solve from the post-state. This
    // ensures that the solution set has access to contracts submitted as a part of the solution
    // set.
    let predicates =
        match get_solution_set_predicates(contract_registry, &post_state, &solution_set.solutions)
            .await
        {
            Ok(predicates) => predicates,
            Err(SetPredicatesError::PredicateDoesNotExist(ca)) => {
                return Ok(Err(InvalidSet::PredicateDoesNotExist(ca)));
            }
            Err(SetPredicatesError::QueryPredicate(err)) => match err {
                QueryPredicateError::Decode(_)
                | QueryPredicateError::MissingLenBytes
                | QueryPredicateError::InvalidLenBytes => {
                    return Ok(Err(InvalidSet::PredicateInvalid));
                }
                QueryPredicateError::ConnPoolQuery(err) => {
                    return Err(CheckSetError::NodeQuery(err))
                }
            },
        };

    // Retrieve the programs that the predicates specify from the post-state.
    let programs = match get_predicates_programs(program_registry, &post_state, &predicates).await {
        Ok(programs) => programs,
        Err(PredicateProgramsError::ProgramDoesNotExist(ca)) => {
            return Ok(Err(InvalidSet::ProgramDoesNotExist(ca)));
        }
        Err(PredicateProgramsError::QueryProgram(err)) => match err {
            QueryProgramError::MissingLenBytes | QueryProgramError::InvalidLenBytes => {
                return Ok(Err(InvalidSet::ProgramInvalid));
            }
            QueryProgramError::ConnPoolQuery(err) => return Err(CheckSetError::NodeQuery(err)),
        },
    };

    let get_predicate = move |addr: &PredicateAddress| {
        predicates
            .get(&addr.predicate)
            .cloned()
            .expect("predicate must have been fetched in the previous step")
    };

    let get_program = move |addr: &ContentAddress| {
        programs
            .get(addr)
            .cloned()
            .expect("program must have been fetched in the previous step")
    };

    // Create the post-state and check the solution set's predicates.
    match check::solution::check_set_predicates(
        &pre_state,
        &post_state,
        solution_set.clone(),
        get_predicate,
        get_program,
        check_conf.clone(),
    )
    .await
    {
        Err(err) => Ok(Err(InvalidSet::Predicates(err))),
        Ok(gas) => Ok(Ok(gas)),
    }
}

/// Read and return all predicates required by the given solutions.
async fn get_solution_set_predicates(
    contract_registry: &ContentAddress,
    view: &state::View,
    solutions: &[Solution],
) -> Result<HashMap<ContentAddress, Arc<Predicate>>, SetPredicatesError> {
    // Spawn concurrent queries for each predicate.
    let queries: tokio::task::JoinSet<_> = solutions
        .iter()
        .map(|solution| solution.predicate_to_solve.clone())
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
    for (sol, (_ix, res)) in solutions.iter().zip(results) {
        let ca = sol.predicate_to_solve.predicate.clone();
        let predicate =
            res?.ok_or_else(|| SetPredicatesError::PredicateDoesNotExist(ca.clone()))?;
        map.insert(ca, Arc::new(predicate));
    }

    Ok(map)
}

/// Read and return all programs required by the given predicates.
async fn get_predicates_programs(
    program_registry: &ContentAddress,
    view: &state::View,
    predicates: &HashMap<ContentAddress, Arc<Predicate>>,
) -> Result<HashMap<ContentAddress, Arc<Program>>, PredicateProgramsError> {
    // Spawn concurrent queries for each program.
    let queries: tokio::task::JoinSet<_> = predicates
        .iter()
        .flat_map(|(_, pred)| {
            pred.nodes
                .iter()
                .map(|node| node.program_address.clone())
                .enumerate()
                .map(move |(ix, prog_addr)| {
                    let view = view.clone();
                    let registry = program_registry.clone();
                    async move {
                        let prog = view.get_program(registry, &prog_addr).await;
                        (ix, prog)
                    }
                })
        })
        .collect();

    // Collect the results into a map.
    let mut map = HashMap::new();
    let mut results = queries.join_all().await;
    results.sort_by_key(|(ix, _)| *ix);

    for (node, (_ix, res)) in predicates
        .iter()
        .flat_map(|(_, pred)| pred.nodes.iter())
        .zip(results)
    {
        let ca = node.program_address.clone();
        let program =
            res?.ok_or_else(|| PredicateProgramsError::ProgramDoesNotExist(ca.clone()))?;
        map.insert(ca, Arc::new(program));
    }

    Ok(map)
}

/// Record solution set failures to the DB for submitter feedback.
fn record_solution_set_failures(
    builder_tx: &mut rusqlite::Transaction,
    failures: Vec<(ContentAddress, SolutionSetFailure)>,
    failures_to_keep: u32,
) -> rusqlite::Result<()> {
    // Nothing to do if no failures.
    if failures.is_empty() {
        return Ok(());
    }
    // Acquire a connection, record failures and delete old failures in one transaction.
    for (ca, failure) in failures {
        builder_db::insert_solution_set_failure(builder_tx, &ca, failure)?;
    }
    builder_db::delete_oldest_solution_set_failures(builder_tx, failures_to_keep)
}
