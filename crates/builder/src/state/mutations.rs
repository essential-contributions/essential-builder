use super::SolutionSetIx;
use essential_types::{solution::SolutionSet, ContentAddress, Key, Value};
use std::collections::{BTreeMap, HashMap};

/// A map from each state key to their associated mutations within a chunk of solution sets.
///
/// This enables shared, fast access to the latest value for any given key at any point within a
/// chunk of solution sets, with the goal of enabling parallel checking of a proposed solution set chunk.
#[derive(Clone, Default)]
pub(crate) struct Mutations(HashMap<(ContentAddress, Key), BTreeMap<SolutionSetIx, Value>>);

impl Mutations {
    /// Query the latest mutation for the given key up to (but excluding) the given solution set index.
    pub(super) fn query_excl(
        &self,
        contract: ContentAddress,
        key: Key,
        solution_set_ix: usize,
    ) -> Option<(&SolutionSetIx, &Value)> {
        let muts = self.0.get(&(contract, key))?;
        muts.range(0..solution_set_ix).next_back()
    }

    /// Remove mutations associated with the given solution set.
    pub(crate) fn remove_solution_set(&mut self, set_ix: usize) {
        self.0.values_mut().for_each(|muts| {
            muts.remove(&set_ix);
        });
    }
}

impl<'a> Extend<(SolutionSetIx, &'a SolutionSet)> for Mutations {
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = (SolutionSetIx, &'a SolutionSet)>,
    {
        for (set_ix, set) in iter.into_iter() {
            for sol in &set.solutions {
                let contract = sol.predicate_to_solve.contract.clone();
                for mutation in &sol.state_mutations {
                    self.0
                        .entry((contract.clone(), mutation.key.clone()))
                        .or_default()
                        .insert(set_ix, mutation.value.clone());
                }
            }
        }
    }
}
