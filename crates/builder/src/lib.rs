//! A block builder implementation for the Essential protocol.

use essential_check::{
    self as check,
    solution::{CheckPredicateConfig, PredicatesError, Utility},
    state_read_vm::Gas,
};
use essential_node as node;
use essential_types::{
    predicate::Predicate,
    solution::{Solution, SolutionData},
    ContentAddress, PredicateAddress,
};
use state::StateReadError;
use std::{collections::HashMap, sync::Arc, time::Duration};
use thiserror::Error;

pub mod db;
mod state;

/// A summary of building a block, returned by [`build_block_fifo`].
pub struct SolutionsSummary {
    /// The addresses of all successful solutions.
    pub succeeded: Vec<(ContentAddress, Utility, Gas)>,
    /// The addresses of all failed solutions.
    pub failed: Vec<(ContentAddress, InvalidSolution)>,
}

/// Any errors that might occur within [`build_block_fifo`].
#[derive(Debug, Error)]
pub enum BuildBlockError {
    /// A builder DB rusqlite error occurred.
    #[error("A builder DB rusqlite error occurred: {0}")]
    BuilderQuery(#[from] db::AcquireThenQueryError),
    /// A node DB rusqlite error occurred.
    #[error("A node DB rusqlite error occurred: {0}")]
    NodeRusqlite(#[from] node::db::AcquireThenRusqliteError),
    /// Failed to check and apply a sequence of solutions.
    #[error("Failed to check and apply solutions: {0}")]
    ApplySolutions(#[from] ApplySolutionsError),
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

pub async fn run(
    builder_conn_pool: db::ConnectionPool,
    node_conn_pool: node::db::ConnectionPool,
    check_conf: Arc<check::solution::CheckPredicateConfig>,
) -> Result<(), BuildBlockError> {
    loop {
        build_block_fifo(
            &builder_conn_pool,
            node_conn_pool.clone(),
            check_conf.clone(),
        )
        .await?;
    }
}

/// Naiively build a block in FIFO order.
///
/// Attempts to build a block from the given proposed solutions in the order in which they're
/// received. No attempt is made at MEV, and solutions that don't succeed in the immediate order
/// provided are considered failed.
pub async fn build_block_fifo(
    builder_conn_pool: &db::ConnectionPool,
    node_conn_pool: node::db::ConnectionPool,
    check_conf: Arc<check::solution::CheckPredicateConfig>,
) -> Result<(), BuildBlockError> {
    // Read out the oldest solutions.
    let min = Duration::from_secs(0);
    let max = Duration::from_secs(i64::MAX as _);
    const LIMIT: i64 = 10_000; // TODO: Make this configurable?
    let solutions = builder_conn_pool.list_solutions(min..max, LIMIT).await?;

    let solutions = solutions
        .into_iter()
        .map(|(ca, solution, _ts)| (ca, Arc::new(solution)));

    // Prepare the node DB transaction for state accumulation.
    let node_tx = state::Transaction::new(node_conn_pool);

    // Check and apply all solutions.
    let output = check_and_apply_solutions(node_tx, solutions, &check_conf).await?;
    let (node_tx, solutions, summary) = output;

    // TODO:
    // Remove solved solutions.
    // Remove failed solutions older than

    // Commit the state changes, along with anything else we need to commit.
    let (_output, _conn_pool) = node_tx.commit(|tx| Ok(())).await?;

    Ok(())
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
    for (solution_ca, solution) in proposed_solutions {
        match check_and_apply_solution(node_tx.clone(), &solution, check_conf).await? {
            Err(invalid) => failed.push((solution_ca, invalid)),
            Ok((new_node_tx, util, gas)) => {
                succeeded.push((solution_ca, util, gas));
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
/// If the solution is valid, returns the `post_state` transaction along with the total utility and
/// gas spent.
async fn check_and_apply_solution(
    pre_state: state::Transaction,
    solution: &Arc<Solution>,
    check_conf: &Arc<CheckPredicateConfig>,
) -> Result<Result<(state::Transaction, Utility, Gas), InvalidSolution>, ApplySolutionError> {
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
        Ok((util, gas)) => Ok(Ok((post_state, util, gas))),
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
