//! An implementation of the node state transaction required during block building.
//!
//! Using a `rusqlite::Transaction<'conn>` would require purely sync DB interaction throughout
//! block building, as it is bound by the lifetime of its connection.
//!
//! To avoid this restriction, we create a dedicated, thread-safe, in-memory transaction around a
//! [`node::db::ConnectionPool`]. This allows for progressively building a state transaction for
//! the block, while still enabling asynchronous queries for predicate and state via the connection
//! pool.

use essential_builder_db as builder_db;
use essential_check::state_read_vm::StateRead;
use essential_node as node;
use essential_node_db as node_db;
use essential_types::{
    predicate::Predicate, solution::SolutionData, ContentAddress, Key, Value, Word,
};
use futures::FutureExt;
use std::{future::Future, pin::Pin};
use thiserror::Error;

/// A simple node DB transaction around a connection pool.
///
/// This transaction is specifically designed for the block building process.
///
/// Cloning the transaction is equivalent to taking a "snapshot" of the transaction.
///
/// The transaction is not committed on drop. [`Transaction::commit`] must be called to
/// commit the transaction to to the DB via the connection pool.
#[derive(Clone)]
pub struct Transaction {
    conn_pool: node::db::ConnectionPool,
    mutations: imbl::HashMap<(ContentAddress, Key), Value>,
}

/// Any errors that might occur in the [`Transaction`][crate::state::Transaction]'s  [`StateRead`]
/// implementation.
#[derive(Debug, Error)]
pub enum StateReadError {
    /// A state query to the underlying DB connection pool failed.
    #[error("a state query failed: {0}")]
    Query(#[from] node::db::AcquireThenQueryError),
    /// No entry exists for the given key.
    #[error("No entry exists for the given key {0:?}")]
    NoEntry(Key),
    /// Key out of range.
    #[error("A key would be out of range: `key` {key:?}, `num_values` {num_values}")]
    OutOfRange { key: Key, num_values: usize },
}

impl Transaction {
    /// Construct a transaction around the given node DB connection pool.
    pub fn new(conn_pool: node::db::ConnectionPool) -> Self {
        let mutations = Default::default();
        Self {
            conn_pool,
            mutations,
        }
    }

    /// Consume the transaction, commit the accumulated mutations to the inner connection pool,
    /// providing access to the connection transaction used in case anything else needs to be
    /// committed at the same time.
    pub async fn commit<F, O>(
        self,
        f: F,
    ) -> Result<(O, node::db::ConnectionPool), node::db::AcquireThenRusqliteError>
    where
        F: 'static + Send + FnOnce(&mut rusqlite::Transaction) -> rusqlite::Result<O>,
        O: 'static + Send,
    {
        let Self {
            conn_pool,
            mutations,
        } = self;
        let output = conn_pool
            .acquire_then(|conn| {
                builder_db::with_tx(conn, move |tx| {
                    for ((contract_ca, key), value) in mutations {
                        node_db::update_state(tx, &contract_ca, &key, &value)?;
                    }
                    let output = f(tx)?;
                    Ok(output)
                })
            })
            .await?;
        Ok((output, conn_pool))
    }

    /// Get the predicate at the given content address.
    pub(crate) async fn get_predicate(
        self,
        predicate_ca: ContentAddress,
    ) -> Result<Option<Predicate>, node::db::AcquireThenQueryError> {
        // FIXME:
        // Update this to use `self.query_state` once contract registry is working.
        // This is because the required predicate may be provided by a prior solution
        // in this block.
        self.conn_pool.get_predicate(predicate_ca).await
    }

    /// Apply the mutations described by the given solution data.
    pub(crate) fn apply_mutations(&mut self, solution_data: &[SolutionData]) {
        for data in solution_data {
            let contract = &data.predicate_to_solve.contract;
            for mutation in &data.state_mutations {
                let entry_key = (contract.clone(), mutation.key.clone());
                self.mutations.insert(entry_key, mutation.value.clone());
            }
        }
    }

    /// Query a single key for its state value if it exists.
    async fn query_state(
        &self,
        contract_ca: ContentAddress,
        key: Key,
    ) -> Result<Option<Value>, node::db::AcquireThenQueryError> {
        match self.mutations.get(&(contract_ca.clone(), key.clone())) {
            Some(value) => Ok(Some(value.clone())),
            None => self.conn_pool.query_state(contract_ca, key).await,
        }
    }

    /// Query a contiguous range of keys and return the resulting state.
    async fn query_state_range(
        &self,
        contract_ca: ContentAddress,
        mut key: Key,
        mut num_values: usize,
    ) -> Result<Vec<Value>, StateReadError> {
        let mut values = vec![];
        while num_values > 0 {
            match self.query_state(contract_ca.clone(), key.clone()).await? {
                None => return Err(StateReadError::NoEntry(key)),
                Some(value) => values.push(value),
            }
            key = next_key(key).map_err(|key| StateReadError::OutOfRange { key, num_values })?;
            num_values -= 1;
        }
        Ok(values)
    }
}

impl StateRead for Transaction {
    type Error = StateReadError;
    type Future = Pin<Box<dyn Future<Output = Result<Vec<Value>, Self::Error>> + Send>>;
    fn key_range(&self, contract: ContentAddress, key: Key, num_values: usize) -> Self::Future {
        let tx = self.clone();
        async move { tx.query_state_range(contract, key, num_values).await }.boxed()
    }
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
