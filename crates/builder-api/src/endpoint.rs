//! Provides a small module for each endpoint with associated `PATH` and `handler`.

use axum::{
    extract::{Path, Query, State},
    response::{
        sse::{self, Sse},
        IntoResponse,
    },
    Json,
};
use essential_builder_db as db;
use essential_types::{ContentAddress, Solution};
use serde::Deserialize;
use std::sync::Arc;
use thiserror::Error;

/// Any endpoint error that might occur.
#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to decode from hex string: {0}")]
    HexDecode(#[from] hex::FromHexError),
    #[error("DB query failed: {0}")]
    ConnPoolRusqlite(#[from] db::error::AcquireThenRusqliteError),
}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        use axum::http::StatusCode;
        match self {
            Error::ConnPoolRusqlite(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
            Error::HexDecode(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        }
    }
}

/// The return a health check response.
pub mod health_check {
    pub const PATH: &str = "/";
    pub async fn handler() {}
}

/// The `get-predicate` get endpoint.
///
/// Takes a contract content address (encoded as hex) as a path parameter.
pub mod submit_solution {
    use super::*;
    pub const PATH: &str = "/submit-solution";
    pub async fn handler(
        State(state): State<crate::State>,
        Json(solution): Json<Solution>,
    ) -> Result<Json<ContentAddress>, Error> {
        let solution = Arc::new(solution);
        let timestamp = now_timestamp();
        let solution_ca = state
            .conn_pool
            .insert_solution_submission(solution, timestamp)
            .await?;
        Ok(Json(solution_ca))
    }
}

/// Get the current moment in time as a `Duration` since the unix epoch.
fn now_timestamp() -> std::time::Duration {
    todo!()
}
