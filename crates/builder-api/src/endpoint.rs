//! Provides a small module for each endpoint with associated `PATH` and `handler`.

use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};
use essential_builder_db as db;
use essential_types::{solution::SolutionSet, ContentAddress};
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

/// The `/latest_solution_set_failures` get endpoint.
///
/// Takes a solution set content address (encoded as hex) and a `limit` as a path parameter and returns
/// at most `limit` (or [`latest_solution_set_failures::MAX_LIMIT`] - whatever's lowest) of
/// the latest failures for the associated solution set.
pub mod latest_solution_set_failures {
    use essential_builder_types::SolutionSetFailure;
    pub const MAX_LIMIT: u32 = 10;
    use super::*;
    pub const PATH: &str = "/latest-solution-set-failures/:solution_set_ca/:limit";
    pub async fn handler(
        State(state): State<crate::State>,
        Path((solution_set_ca, limit)): Path<(String, u32)>,
    ) -> Result<Json<Vec<SolutionSetFailure<'static>>>, Error> {
        let solution_set_ca: ContentAddress = solution_set_ca.parse()?;
        let limit = limit.min(MAX_LIMIT);
        let failures = state
            .conn_pool
            .latest_solution_set_failures(solution_set_ca, limit)
            .await?;
        Ok(Json(failures))
    }
}

/// The `/list_solution_set_failures` get endpoint.
///
/// Takes a `start` and a `limit` as a path parameter and returns
/// at most `limit` of the latest failures for all solution sets.
/// Note start counts down from the latest failure.
/// So the latest failure is at `0`.
pub mod list_solution_set_failures {
    use super::*;
    use essential_builder_types::SolutionSetFailure;
    pub const PATH: &str = "/list-solution-set-failures/:start/:limit";
    pub async fn handler(
        State(state): State<crate::State>,
        Path((offset, limit)): Path<(u32, u32)>,
    ) -> Result<Json<Vec<SolutionSetFailure<'static>>>, Error> {
        let failures = state
            .conn_pool
            .list_solution_set_failures(offset, limit)
            .await?;
        Ok(Json(failures))
    }
}

/// The `/submit-solution-set` get endpoint.
///
/// Takes a JSON-serialized [`SolutionSet`], and responds with its [`ContentAddress`] upon
/// successfully adding the solution set to the solution set pool.
pub mod submit_solution_set {
    use super::*;
    pub const PATH: &str = "/submit-solution-set";
    pub async fn handler(
        State(state): State<crate::State>,
        Json(solution_set): Json<SolutionSet>,
    ) -> Result<Json<ContentAddress>, Error> {
        let solution_set = Arc::new(solution_set);
        let timestamp = now_timestamp()?;
        let solution_set_ca = state
            .conn_pool
            .insert_solution_set_submission(solution_set, timestamp)
            .await?;
        Ok(Json(solution_set_ca))
    }
}

/// Get the current moment in time as a `Duration` since `UNIX_EPOCH`.
fn now_timestamp() -> Result<std::time::Duration, SystemTimeError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| SystemTimeError)
}
