//! A block builder implementation for the Essential protocol.

use essential_check::{
    self as check,
    solution::Utility,
    state_read_vm::{Gas, StateRead},
};
use essential_node as node;
use essential_node_db as node_db;
use essential_types::{
    predicate::Predicate,
    solution::{Solution, SolutionData},
    Block, ContentAddress, Key, PredicateAddress, Value, Word,
};
use rusqlite::{Connection, Savepoint, Transaction};
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;

pub mod db;
mod state;

/// A summary of building a block, returned by [`build_block_fifo`].
pub struct BuildSummary {
    /// The addresses of all successful solutions.
    pub successful: Vec<(ContentAddress, Utility, Gas)>,
    /// The addresses of all failed solutions.
    pub failed: Vec<(ContentAddress, InvalidSolution)>,
}

/// Any errors that might occur within [`build_block_fifo`].
#[derive(Debug, Error)]
pub enum BuildBlockError {
    #[error("a rusqlite error occurred: {0}")]
    Rusqlite(#[from] rusqlite::Error),
    #[error("an error occurred while attempting to apply a solution: {0}")]
    ApplySolution(#[from] ApplySolutionError),
}

/// Any errors that might occur within [`build_block_fifo`].
#[derive(Debug, Error)]
pub enum ApplySolutionError {
    #[error("a rusqlite error occurred: {0}")]
    Rusqlite(#[from] rusqlite::Error),
    #[error("a node DB query failed: {0}")]
    NodeQuery(#[from] node_db::QueryError),
}

#[derive(Debug, Error)]
pub enum SolutionPredicatesError {
    #[error("an error occurred while querying the node DB: {0}")]
    Query(#[from] node_db::QueryError),
    #[error("the node DB is missing a required predicate ({0})")]
    InvalidPredicate(ContentAddress),
}

/// Represents the reason why a [`Solution`] is invalid.
#[derive(Debug, Error)]
pub enum InvalidSolution {
    /// Solution specified a predicate to solve that does not exist.
    #[error("Solution specified a predicate to solve that does not exist")]
    InvalidPredicate(ContentAddress),
}

/// Naiively build a block of solutions from the given sequence of proposed solutions.
///
/// Attempts to build a block from the given proposed solutions in the order in which they're
/// received. No attempt is made at MEV, and solutions that don't succeed in the immediate order
/// provided are considered failed.
///
/// Upon completion, the given transaction will have applied all state mutations proposed by the
/// solutions included in the returned block.
pub async fn build_block_fifo(
    node_tx: &mut Transaction<'_>,
    proposed_solutions: impl IntoIterator<Item = (ContentAddress, Arc<Solution>)>,
    check_conf: &check::solution::CheckPredicateConfig,
) -> Result<(Vec<Arc<Solution>>, BuildSummary), BuildBlockError> {
    let mut solutions = vec![];
    let mut successful = vec![];
    let mut failed = vec![];
    for (solution_ca, solution) in proposed_solutions {
        let savepoint = node_tx.savepoint()?;
        match check_and_apply_solution(&savepoint, &solution, check_conf)? {
            Err(invalid) => failed.push((solution_ca, invalid)),
            Ok((util, gas)) => {
                successful.push((solution_ca, util, gas));
                solutions.push(solution);
                savepoint.commit()?;
            }
        }
    }

    let summary = BuildSummary { successful, failed };
    Ok((solutions, summary))
}

/// Validate and attempt to apply the given solution based on the current state provided by
/// `node_tx` transaction savepoint.
fn check_and_apply_solution(
    pre_state: &Savepoint<'_>,
    solution: &Arc<Solution>,
    check_conf: &check::solution::CheckPredicateConfig,
) -> Result<Result<(Utility, Gas), InvalidSolution>, ApplySolutionError> {
    // Retrieve the predicates that the solution attempts to solve.
    let predicates = match get_solution_predicates(pre_state, &solution.data) {
        Ok(predicates) => predicates,
        Err(SolutionPredicatesError::InvalidPredicate(ca)) => {
            return Ok(Err(InvalidSolution::InvalidPredicate(ca)));
        }
        Err(SolutionPredicatesError::Query(err)) => return Err(ApplySolutionError::NodeQuery(err)),
    };

    let get_predicate = |addr: &PredicateAddress| predicates[&addr.predicate].clone();

    todo!();

    // Create the post state for constraint checking.
    let post_state: PostState = todo!();

    // // We only need read-only access to pre and post state during validation.
    // let (util, gas) =
    //     check::solution::check_predicates(&pre, &post, solution.clone(), get_predicate, config)
    //         .await?;

    todo!("commit post state");
    // apply_state_mutations(

    // Ok(Ok((util, gas)))
}

struct PreState<'conn>(Savepoint<'conn>);

struct PostState<'pre, 'conn> {
    pre_state: &'pre Savepoint<'conn>,
    mutations: HashMap<(ContentAddress, Key), Value>,
}

// #[derive(Debug, Error)]
// enum StateReadError {
//     /// No entry exists for the given key.
//     #[error("No entry exists for the given key {0:?}")]
//     NoEntry(Key),
//     /// Key out of range.
//     #[error("A key would be out of range: `key` {key:?}, `num_values` {num_values}")]
//     OutOfRange { key: Key, num_values: usize },
//     #[error("A DB query error occurred: {0}")]
//     Query(#[from] node_db::QueryError),
// }
//
// impl<'conn> StateRead for PreState<'conn> {
//     type Error = StateReadError;
//     // TODO: We can't do this async
//     type Future = std::future::Ready<Result<Vec<Value>, Self::Error>>;
//     fn key_range(&self, contract: ContentAddress, mut key: Key, num_values: usize) -> Self::Future {
//         use std::future::ready;
//         let mut values = vec![];
//         for _ in 0..num_values {
//             match node_db::query_state(&self.0, &contract, &key) {
//                 Err(err) => return ready(Err(StateReadError::Query(err))),
//                 Ok(opt) => match opt {
//                     None => return ready(Err(StateReadError::NoEntry(key))),
//                     Some(value) => values.push(value),
//                 },
//             }
//             key = match next_key(key.clone()) {
//                 None => return ready(Err(StateReadError::OutOfRange { key, num_values })),
//                 Some(key) => key,
//             };
//         }
//         ready(Ok(values))
//     }
// }
//
// impl<'pre, 'conn> StateRead for PostState<'pre, 'conn> {
//     type Error = StateReadError;
//     type Future = std::future::Ready<Result<Vec<Value>, Self::Error>>;
//     fn key_range(&self, contract: ContentAddress, mut key: Key, num_values: usize) -> Self::Future {
//         todo!()
//     }
// }

/// Apply the mutations proposed by the `solution` to the given node DB transaction.
///
/// Assumes the given `solution_data` has been checked with [`check::solution::check_data`].
fn apply_state_mutations(sp: &Savepoint, solution_data: &[SolutionData]) -> rusqlite::Result<()> {
    for data in solution_data {
        let contract = &data.predicate_to_solve.contract;
        for mutation in &data.state_mutations {
            node_db::update_state(sp, contract, &mutation.key, &mutation.value)?;
        }
    }
    Ok(())
}

/// Read and return all predicates required the given solution data.
///
/// The returned `Vec` matches the order of the given solution data.
/// Missing predicates are represented with `None` values.
fn get_solution_predicates(
    conn: &Connection,
    solution_data: &[SolutionData],
) -> Result<HashMap<ContentAddress, Arc<Predicate>>, SolutionPredicatesError> {
    let mut map = HashMap::new();
    for data in solution_data {
        let pred_ca = &data.predicate_to_solve.predicate;
        let opt = node_db::get_predicate(conn, pred_ca)?;
        let predicate =
            opt.ok_or_else(|| SolutionPredicatesError::InvalidPredicate(pred_ca.clone()))?;
        map.insert(pred_ca.clone(), Arc::new(predicate));
    }
    Ok(map)
}
