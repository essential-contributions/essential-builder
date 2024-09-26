use super::SolutionIx;
use essential_types::{solution::Solution, ContentAddress, Key, Value};
use std::{cmp::Ordering, collections::HashMap};

/// A map from each state key to their associated mutations within a chunk of solutions.
///
/// This enables shared, fast access to the latest value for any given key at any point within a
/// chunk of solutions, with the goal of enabling parallel checking of a proposed solution chunk.
#[derive(Clone, Default)]
pub(crate) struct Mutations(HashMap<(ContentAddress, Key), Vec<(SolutionIx, Value)>>);

impl Mutations {
    /// Query the latest state for the given key where the solution index is selected using the
    /// given comparison function.
    pub(super) fn query(
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
