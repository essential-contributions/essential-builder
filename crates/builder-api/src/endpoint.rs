//! Provides a small module for each endpoint with associated `PATH` and `handler`.

use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};
use essential_builder_db as db;
use essential_types::{solution::Solution, ContentAddress};
use std::sync::Arc;
use thiserror::Error;

/// Any endpoint error that might occur.
#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to decode from hex string: {0}")]
    HexDecode(#[from] hex::FromHexError),
    #[error("DB query failed: {0}")]
    ConnPoolRusqlite(#[from] db::error::AcquireThenRusqliteError),
    #[error("{0}")]
    SystemTime(#[from] SystemTimeError),
}

/// The system time returned a timestamp that preceded `UNIX_EPOCH`.
#[derive(Debug, Error)]
#[error("system time preceded `UNIX_EPOCH`")]
pub struct SystemTimeError;

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        use axum::http::StatusCode;
        match self {
            Error::ConnPoolRusqlite(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
            Error::HexDecode(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
            Error::SystemTime(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    }
}

/// The return a health check response.
pub mod health_check {
    pub const PATH: &str = "/";
    pub async fn handler() {}
}

/// The `/latest_solution_failures` get endpoint.
///
/// Takes a solution content address (encoded as hex) and a `limit` as a path parameter and returns
/// at most `limit` (or [`latest_solution_failures::MAX_LIMIT`] - whatever's lowest) of
/// the latest failures for the associated solution.
pub mod latest_solution_failures {
    use essential_builder_types::SolutionFailure;
    pub const MAX_LIMIT: u32 = 10;
    use super::*;
    pub const PATH: &str = "/latest-solution-failures/:solution_ca/:limit";
    pub async fn handler(
        State(state): State<crate::State>,
        Path((solution_ca, limit)): Path<(String, u32)>,
    ) -> Result<Json<Vec<SolutionFailure<'static>>>, Error> {
        let solution_ca: ContentAddress = solution_ca.parse()?;
        let limit = limit.min(MAX_LIMIT);
        let failures = state
            .conn_pool
            .latest_solution_failures(solution_ca, limit)
            .await?;
        Ok(Json(failures))
    }
}

/// The `/submit-solution` get endpoint.
///
/// Takes a JSON-serialized [`Solution`], and responds with its [`ContentAddress`] upon
/// successfully adding the solution to the solution pool.
pub mod submit_solution {
    use super::*;
    pub const PATH: &str = "/submit-solution";
    pub async fn handler(
        State(state): State<crate::State>,
        Json(solution): Json<Solution>,
    ) -> Result<Json<ContentAddress>, Error> {
        let solution = Arc::new(solution);
        let timestamp = now_timestamp()?;
        let solution_ca = state
            .conn_pool
            .insert_solution_submission(solution, timestamp)
            .await?;
        Ok(Json(solution_ca))
    }
}

/// Get the current moment in time as a `Duration` since `UNIX_EPOCH`.
fn now_timestamp() -> Result<std::time::Duration, SystemTimeError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| SystemTimeError)
}
