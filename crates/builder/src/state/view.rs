use super::{Mutations, SolutionIx};
use crate::{error::StateReadError, BlockNum};
use essential_check::state_read_vm::StateRead;
use essential_node as node;
use essential_types::{predicate::Predicate, ContentAddress, Key, Value, Word};
use futures::FutureExt;
use std::{future::Future, pin::Pin, sync::Arc};

/// A view into the latest state prior to the solution at the given index.
///
/// Provides a [`StateRead`] implementation for use with [`essential_check`].
#[derive(Clone)]
pub(crate) struct View {
    conn_pool: node::db::ConnectionPool,
    proposed_mutations: Arc<Mutations>,
    block_num: BlockNum,
    solution_ix: SolutionIx,
}

impl View {
    /// Query the state at the given contract and key.
    /// First queries the `proposed_mutations`, then falls back to the connection pool.
    async fn query(
        &self,
        contract: ContentAddress,
        key: Key,
    ) -> Result<Option<Value>, node::db::AcquireThenQueryError> {
        if let Some((_ix, v)) =
            self.proposed_mutations
                .query_excl(contract.clone(), key.clone(), self.solution_ix)
        {
            return Ok(Some(v.clone()));
        }
        let block_num = self.block_num;
        self.conn_pool
            .acquire_then(move |conn| {
                use essential_node_db::finalized::query_state_exclusive_block;
                let value = query_state_exclusive_block(conn, &contract, &key, block_num)?;
                Ok(value)
            })
            .await
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
    block_num: BlockNum,
    solution_ix: SolutionIx,
) -> (View, View) {
    let pre = View {
        conn_pool: conn_pool.clone(),
        proposed_mutations: proposed_mutations.clone(),
        block_num,
        solution_ix,
    };
    let post = View {
        conn_pool,
        proposed_mutations,
        block_num,
        solution_ix: solution_ix
            .checked_add(1)
            .expect("solution max out of range"),
    };
    (pre, post)
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
