//! An implementation of the node state transaction required during block building.
//!
//! Using a `rusqlite::Transaction<'conn>` would require purely sync DB interaction throughout
//! block building, as it is bound by the lifetime of its connection. Instead, we create a
//! thread-safe, in-memory transaction around a `ConnectionPool`.

use essential_check::state_read_vm::StateRead;
use essential_node as node;
use essential_node_db as node_db;
use essential_types::{ContentAddress, Key, Value, Word};
use futures::FutureExt;
use std::{future::Future, pin::Pin};
use thiserror::Error;

/// Similar to [`StateRead`], but for asynchronusly querying a single key.
pub(crate) trait QueryState {
    type Error: core::fmt::Debug + core::fmt::Display;
    type Future: Future<Output = Result<Option<Value>, Self::Error>>;
    fn query_state(&self, contract_ca: ContentAddress, key: Key) -> Self::Future;
}

/// A node state transaction around a connection pool, required during block building.
///
/// Cloning the transaction is equivalent to creating a new nested transaction.
#[derive(Clone)]
pub(crate) struct Transaction<T> {
    inner: T,
    mutations: imbl::HashMap<(ContentAddress, Key), Value>,
}

#[derive(Debug, Error)]
pub(crate) enum QueryStateRangeError<E> {
    #[error("a state query failed: {0}")]
    QueryState(E),
    /// No entry exists for the given key.
    #[error("No entry exists for the given key {0:?}")]
    NoEntry(Key),
    /// Key out of range.
    #[error("A key would be out of range: `key` {key:?}, `num_values` {num_values}")]
    OutOfRange { key: Key, num_values: usize },
}

impl<T> Transaction<T> {

}

impl<T> StateRead for Transaction<T>
where
    T: Clone + QueryState + Send + Sync + 'static,
    T::Future: Send,
{
    type Error = QueryStateRangeError<T::Error>;
    type Future = Pin<Box<dyn Future<Output = Result<Vec<Value>, Self::Error>> + Send>>;
    fn key_range(&self, contract: ContentAddress, key: Key, num_values: usize) -> Self::Future {
        let tx = self.clone();
        async move { query_state_range(&tx, contract, key, num_values).await }.boxed()
    }
}

impl<T> QueryState for Transaction<T>
where
    T: Clone + QueryState + Send + Sync + 'static,
    T::Future: Send,
{
    type Error = T::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Option<Value>, Self::Error>> + Send>>;
    fn query_state(&self, contract: ContentAddress, key: Key) -> Self::Future {
        let tx = self.clone();
        async move { tx_query_state(&tx, contract, key).await }.boxed()
    }
}

impl QueryState for node::db::ConnectionPool {
    type Error = node::db::AcquireThenQueryError;
    type Future = Pin<Box<dyn Future<Output = Result<Option<Value>, Self::Error>> + Send>>;
    fn query_state(&self, contract: ContentAddress, key: Key) -> Self::Future {
        let pool = self.clone();
        async move { pool.query_state(contract, key).await }.boxed()
    }
}

/// Query a key using the given transaction.
async fn tx_query_state<T>(
    tx: &Transaction<T>,
    contract: ContentAddress,
    key: Key,
) -> Result<Option<Value>, T::Error>
where
    T: QueryState,
{
    match tx.mutations.get(&(contract.clone(), key.clone())) {
        Some(value) => Ok(Some(value.clone())),
        None => tx.inner.query_state(contract, key).await,
    }
}

/// Asynchronously read the given range of keys.
async fn query_state_range<T>(
    query_state: &T,
    contract_ca: ContentAddress,
    mut key: Key,
    mut num_values: usize,
) -> Result<Vec<Value>, QueryStateRangeError<T::Error>>
where
    T: QueryState,
{
    let mut values = vec![];
    while num_values > 0 {
        let res = query_state
            .query_state(contract_ca.clone(), key.clone())
            .await;
        let opt = res.map_err(QueryStateRangeError::QueryState)?;
        match opt {
            None => return Err(QueryStateRangeError::NoEntry(key)),
            Some(value) => values.push(value),
        }
        key = next_key(key).map_err(|key| QueryStateRangeError::OutOfRange { key, num_values })?;
        num_values -= 1;
    }
    Ok(values)
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
