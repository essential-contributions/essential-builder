use super::{Mutations, SolutionSetIx};
use crate::{
    error::{QueryPredicateError, QueryProgramError, StateReadError},
    BlockNum,
};
use essential_check::vm::StateRead;
use essential_node as node;
use essential_node_types::{contract_registry, program_registry};
use essential_types::{
    convert::bytes_from_word, predicate::Predicate, ContentAddress, Key, PredicateAddress, Program,
    Value, Word,
};
use futures::FutureExt;
use std::{future::Future, pin::Pin, sync::Arc};

/// A view into the latest state prior to the solution set at the given index.
///
/// Provides a [`StateRead`] implementation for use with [`essential_check`].
#[derive(Clone)]
pub(crate) struct View {
    conn_pool: node::db::ConnectionPool,
    proposed_mutations: Arc<Mutations>,
    block_num: BlockNum,
    solution_set_ix: SolutionSetIx,
}

impl View {
    /// Query the state at the given contract and key.
    /// First queries the `proposed_mutations`, then falls back to the connection pool.
    async fn query(
        &self,
        contract: ContentAddress,
        key: Key,
    ) -> Result<Option<Value>, node::db::pool::AcquireThenQueryError> {
        if let Some((_ix, v)) =
            self.proposed_mutations
                .query_excl(contract.clone(), key.clone(), self.solution_set_ix)
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
            let value = self
                .query(contract_ca.clone(), key.clone())
                .await?
                .unwrap_or(vec![]);
            values.push(value);
            key = next_key(key).map_err(|key| StateReadError::OutOfRange { key, num_values })?;
            num_values -= 1;
        }
        Ok(values)
    }

    /// Get the predicate at the given content address.
    pub(crate) async fn get_predicate(
        self,
        contract_registry: ContentAddress,
        pred_addr: &PredicateAddress,
    ) -> Result<Option<Predicate>, QueryPredicateError> {
        // Check that the predicate is a part of the contract.
        let contract_predicate_key = contract_registry::contract_predicate_key(pred_addr);
        if self
            .query(contract_registry.clone(), contract_predicate_key)
            .await?
            .is_none()
        {
            return Ok(None);
        }

        // Read the full predicate out of the contract registry storage.
        let predicate_key = contract_registry::predicate_key(&pred_addr.predicate);
        let Some(pred_words) = self.query(contract_registry, predicate_key).await? else {
            return Ok(None);
        };

        // Read the length from the front.
        let Some(&pred_len_bytes) = pred_words.first() else {
            return Err(QueryPredicateError::MissingLenBytes);
        };
        let pred_len_bytes: usize = pred_len_bytes
            .try_into()
            .map_err(|_| QueryPredicateError::InvalidLenBytes)?;
        let pred_words = &pred_words[1..];
        let pred_bytes: Vec<u8> = pred_words
            .iter()
            .copied()
            .flat_map(bytes_from_word)
            .take(pred_len_bytes)
            .collect();

        let predicate = Predicate::decode(&pred_bytes)?;
        Ok(Some(predicate))
    }

    /// Get the program at the given content address.
    pub(crate) async fn get_program(
        self,
        program_registry: ContentAddress,
        prog_addr: &ContentAddress,
    ) -> Result<Option<Program>, QueryProgramError> {
        let program_key = program_registry::program_key(prog_addr);
        let Some(prog_words) = self.query(program_registry, program_key).await? else {
            return Ok(None);
        };

        // Read the length from the front.
        let Some(&prog_len_bytes) = prog_words.first() else {
            return Err(QueryProgramError::MissingLenBytes);
        };
        let prog_len_bytes: usize = prog_len_bytes
            .try_into()
            .map_err(|_| QueryProgramError::InvalidLenBytes)?;
        let prog_words = &prog_words[1..];
        let prog_bytes: Vec<u8> = prog_words
            .iter()
            .copied()
            .flat_map(bytes_from_word)
            .take(prog_len_bytes)
            .collect();

        let program = Program(prog_bytes);
        Ok(Some(program))
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

/// Create the pre and post state [`View`] for the solution set at the given index.
pub(crate) fn pre_and_post_view(
    conn_pool: node::db::ConnectionPool,
    proposed_mutations: Arc<Mutations>,
    block_num: BlockNum,
    solution_set_ix: SolutionSetIx,
) -> (View, View) {
    let pre = View {
        conn_pool: conn_pool.clone(),
        proposed_mutations: proposed_mutations.clone(),
        block_num,
        solution_set_ix,
    };
    let post = View {
        conn_pool,
        proposed_mutations,
        block_num,
        solution_set_ix: solution_set_ix
            .checked_add(1)
            .expect("solution set max out of range"),
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
