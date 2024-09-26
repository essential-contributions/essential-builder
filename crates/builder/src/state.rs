//! A parallel-friendly, queryable view into state mutations for a set of proposed solutions.

use crate::error::StateReadError;
use essential_check::state_read_vm::StateRead;
use essential_node as node;
use essential_types::{predicate::Predicate, solution::Solution, ContentAddress, Key, Value, Word};
use futures::FutureExt;
use std::{cmp::Ordering, collections::HashMap, future::Future, pin::Pin, sync::Arc};

/// A map from each state key to their associated mutations within a chunk of solutions.
///
/// This enables shared, fast access to the latest value for any given key at any point within a
/// chunk of solutions, with the goal of enabling parallel checking of a proposed solution chunk.
#[derive(Clone, Default)]
pub(crate) struct Mutations(MutationsMap);
type MutationsMap = HashMap<(ContentAddress, Key), Vec<(SolutionIx, Value)>>;
type SolutionIx = usize;

/// A view into the latest state prior to the solution at the given index.
#[derive(Clone)]
pub(crate) struct View {
    conn_pool: node::db::ConnectionPool,
    proposed_mutations: Arc<Mutations>,
    solution_ix: SolutionIx,
}

impl Mutations {
    /// Query the latest state for the given key where the solution index matches the given
    /// condition.
    fn query(
        &self,
        contract: ContentAddress,
        key: Key,
        cmp: impl Fn(SolutionIx) -> Ordering,
    ) -> Option<&(SolutionIx, Value)> {
        let muts = self.0.get(&(contract, key))?;
        match muts.binary_search_by(|(ix, _)| cmp(*ix)) {
            Ok(ix) => return muts.get(ix),
            Err(insert_ix) => muts[0..insert_ix].iter().rev().next(),
        }
    }

    /// Remove mutations associated with the given solution.
    pub(crate) fn remove_solution(&mut self, sol_ix: usize) {
        self.0
            .iter_mut()
            .for_each(|(_, vals)| vals.retain(|(ix, _)| *ix != sol_ix));
    }
}

impl View {
    /// Query the state at the given contract and key.
    /// First queries the `proposed_mutations`, then falls back to the connection pool.
    async fn query(
        &self,
        contract: ContentAddress,
        key: Key,
    ) -> Result<Option<Value>, node::db::AcquireThenQueryError> {
        let cmp = |ix| cmp_excl(ix, self.solution_ix);
        if let Some((_ix, v)) = self
            .proposed_mutations
            .query(contract.clone(), key.clone(), cmp)
        {
            return Ok(Some(v.clone()));
        }
        self.conn_pool.query_state(contract, key).await
    }

    /// Query a range of keys and return the resulting state.
    async fn query_range(
        &self,
        contract_ca: ContentAddress,
        mut key: Key,
        mut num_values: usize,
    ) -> Result<Vec<Value>, StateReadError> {
        let mut values = vec![];
        while num_values > 0 {
            match self.query(contract_ca.clone(), key.clone()).await? {
                None => return Err(StateReadError::NoEntry(key)),
                Some(value) => values.push(value),
            }
            key = next_key(key).map_err(|key| StateReadError::OutOfRange { key, num_values })?;
            num_values -= 1;
        }
        Ok(values)
    }

    /// Get the predicate at the given content address.
    pub(crate) async fn get_predicate(
        self,
        predicate_ca: ContentAddress,
    ) -> Result<Option<Predicate>, node::db::AcquireThenQueryError> {
        // FIXME:
        // Update this to use `self.query` once contract registry is working.
        // This is because the required predicate may be provided by this solution
        // or another solution earlier in this block.
        self.conn_pool.get_predicate(predicate_ca).await
    }
}

impl<'a> Extend<(SolutionIx, &'a Solution)> for Mutations {
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = (SolutionIx, &'a Solution)>,
    {
        for (sol_ix, solution) in iter.into_iter() {
            for data in &solution.data {
                let contract = data.predicate_to_solve.contract.clone();
                for mutation in &data.state_mutations {
                    self.0
                        .entry((contract.clone(), mutation.key.clone()))
                        .or_default()
                        .push((sol_ix, mutation.value.clone()));
                }
            }
        }
    }
}

impl StateRead for View {
    type Error = StateReadError;
    type Future = Pin<Box<dyn Future<Output = Result<Vec<Value>, Self::Error>> + Send>>;
    fn key_range(&self, contract: ContentAddress, key: Key, num_values: usize) -> Self::Future {
        let tx = self.clone();
        async move { tx.query_range(contract, key, num_values).await }.boxed()
    }
}

/// Create the pre and post state [`View`] for the solution at the given index.
pub(crate) fn pre_and_post_view(
    conn_pool: node::db::ConnectionPool,
    proposed_mutations: Arc<Mutations>,
    solution_ix: SolutionIx,
) -> (View, View) {
    let pre = View {
        conn_pool: conn_pool.clone(),
        proposed_mutations: proposed_mutations.clone(),
        solution_ix,
    };
    let post = View {
        conn_pool,
        proposed_mutations,
        solution_ix: solution_ix
            .checked_add(1)
            .expect("solution max out of range"),
    };
    (pre, post)
}

/// Comparison fn for exclusive solution query.
fn cmp_excl(probe: SolutionIx, sol_ix: SolutionIx) -> Ordering {
    sol_ix
        .checked_sub(1)
        .map(|prev_sol_ix| probe.cmp(&prev_sol_ix))
        .unwrap_or(Ordering::Less)
}

/// Calculate the next key.
fn next_key(mut key: Key) -> Result<Key, Key> {
    for w in key.iter_mut().rev() {
        match *w {
            Word::MAX => *w = Word::MIN,
            _ => {
                *w += 1;
                return Ok(key);
            }
        }
    }
    Err(key)
}
