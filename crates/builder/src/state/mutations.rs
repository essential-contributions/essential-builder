use super::SolutionIx;
use essential_types::{solution::Solution, ContentAddress, Key, Value};
use std::collections::{BTreeMap, HashMap};

/// A map from each state key to their associated mutations within a chunk of solutions.
///
/// This enables shared, fast access to the latest value for any given key at any point within a
/// chunk of solutions, with the goal of enabling parallel checking of a proposed solution chunk.
#[derive(Clone, Default)]
pub(crate) struct Mutations(HashMap<(ContentAddress, Key), BTreeMap<SolutionIx, Value>>);

impl Mutations {
    /// Query the latest mutation for the given key up to (but excluding) the given solution index.
    pub(super) fn query_excl(
        &self,
        contract: ContentAddress,
        key: Key,
        solution_ix: usize,
    ) -> Option<(&SolutionIx, &Value)> {
        let muts = self.0.get(&(contract, key))?;
        muts.range(0..solution_ix).next_back()
    }

    /// Remove mutations associated with the given solution.
    pub(crate) fn remove_solution(&mut self, sol_ix: usize) {
        self.0.values_mut().for_each(|muts| {
            muts.remove(&sol_ix);
        });
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
                        .insert(sol_ix, mutation.value.clone());
                }
            }
        }
    }
}
