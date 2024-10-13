//! The API for the Essential block builder.
//!
//! Find the available endpoints under the [`endpoint`] module.
//!
//! To serve the builder API, construct a [`router`], a [`TcpListener`] and call [`serve`].

use axum::{
    routing::{get, post},
    Router,
};
use essential_builder_db as db;
use std::{io, net::SocketAddr};
use thiserror::Error;
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinSet,
};
use tower_http::cors::CorsLayer;

pub mod endpoint;

/// State provided to the endpoints when serving connections.
#[derive(Clone)]
pub struct State {
    /// A builder DB connection pool.
    pub conn_pool: db::ConnectionPool,
}

/// An error occurred while attempting to serve a new connection.
#[derive(Debug, Error)]
pub enum ServeNextConnError {
    /// Failed to acquire the next connection.
    #[error("failed to acquire next connection: {0}")]
    Next(#[from] io::Error),
    /// Failed to serve the connection.
    #[error("{0}")]
    Serve(#[from] ServeConnError),
}

/// An error occurred while attempting to serve a connection.
#[derive(Debug, Error)]
#[error("Serve connection error: {0}")]
pub struct ServeConnError(#[from] Box<dyn std::error::Error + Send + Sync>);

/// The default value used by `essential-builder-cli` for the maximum number of
/// TCP stream connections to maintain at once.
pub const DEFAULT_CONNECTION_LIMIT: usize = 2_000;

/// Continuously serve the Builder API using the given `router` and TCP `listener`.
///
/// The number of simultaneous TCP stream connections will be capped at the given
/// `conn_limit`.
///
/// This constructs a new `JoinSet` to use for limiting connections and then
/// calls [`serve_next_conn`] in a loop. Any outstanding connections will not be
/// counted toward the connection limit.
pub async fn serve(router: &Router, listener: &TcpListener, conn_limit: usize) {
    let mut conn_set = JoinSet::new();
    loop {
        serve_next_conn(router, listener, conn_limit, &mut conn_set).await;
    }
}

/// Accept and serve the next connection.
///
/// The number of simultaneous TCP stream connections will be capped at the given
/// `conn_limit`.
///
/// If we're at the connection limit, this first awaits for a connection task to
/// become available.
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() {
/// # use essential_builder_api as builder_api;
/// # use essential_builder_db as builder_db;
/// let conf = builder_db::pool::Config::default();
/// let conn_pool = builder_db::ConnectionPool::with_tables(&conf).unwrap();
/// let state = builder_api::State { conn_pool };
/// let router = builder_api::router(state);
/// let listener = tokio::net::TcpListener::bind("127.0.0.1:3553").await.unwrap();
/// let conn_limit = builder_api::DEFAULT_CONNECTION_LIMIT;
/// let mut conn_set = tokio::task::JoinSet::new();
/// // Accept and serve connections.
/// loop {
///     builder_api::serve_next_conn(&router, &listener, conn_limit, &mut conn_set).await;
/// }
/// # }
/// ```
#[tracing::instrument(skip_all)]
pub async fn serve_next_conn(
    router: &Router,
    listener: &TcpListener,
    conn_limit: usize,
    conn_set: &mut JoinSet<()>,
) {
    // Await the next connection.
    let stream = match next_conn(listener, conn_limit, conn_set).await {
        Ok((stream, _remote_addr)) => {
            #[cfg(feature = "tracing")]
            tracing::trace!("Accepted new connection from: {_remote_addr}");
            stream
        }
        Err(_err) => {
            #[cfg(feature = "tracing")]
            tracing::trace!("Failed to accept connection {_err}");
            return;
        }
    };

    // Serve the acquired connection.
    let router = router.clone();
    conn_set.spawn(async move {
        if let Err(_err) = serve_conn(&router, stream).await {
            #[cfg(feature = "tracing")]
            tracing::trace!("Serve connection error: {_err}");
        }
    });
}

/// Accept and return the next TCP stream connection.
///
/// If we're at the connection limit, this first awaits for a connection task to
/// become available.
#[tracing::instrument(skip_all, err)]
pub async fn next_conn(
    listener: &TcpListener,
    conn_limit: usize,
    conn_set: &mut JoinSet<()>,
) -> io::Result<(TcpStream, SocketAddr)> {
    // If the `conn_set` size currently exceeds the limit, wait for the next to join.
    if conn_set.len() >= conn_limit {
        #[cfg(feature = "tracing")]
        tracing::info!("Connection limit reached: {conn_limit}");
        conn_set.join_next().await.expect("set cannot be empty")?;
    }
    // Await another connection.
    tracing::trace!("Awaiting new connection at {}", listener.local_addr()?);
    listener.accept().await
}

/// Serve a newly accepted TCP stream.
#[tracing::instrument(skip_all, err)]
pub async fn serve_conn(router: &Router, stream: TcpStream) -> Result<(), ServeConnError> {
    // Hyper has its own `AsyncRead` and `AsyncWrite` traits and doesn't use
    // tokio. `TokioIo` converts between them.
    let stream = hyper_util::rt::TokioIo::new(stream);

    // Hyper also has its own `Service` trait and doesn't use tower. We can use
    // `hyper::service::service_fn` to create a hyper `Service` that calls our
    // app through `tower::Service::call`.
    let hyper_service = hyper::service::service_fn(
        move |request: axum::extract::Request<hyper::body::Incoming>| {
            tower::Service::call(&mut router.clone(), request)
        },
    );

    // `TokioExecutor` tells hyper to use `tokio::spawn` to spawn tasks.
    let executor = hyper_util::rt::TokioExecutor::new();
    let conn = hyper_util::server::conn::auto::Builder::new(executor).http2_only();
    conn.serve_connection(stream, hyper_service)
        .await
        .map_err(ServeConnError)
}

/// Construct the endpoint router with the builder [`endpoint`]s, CORS layer and DB
/// connection pool as state.
pub fn router(state: State) -> Router {
    with_endpoints(Router::new())
        .layer(cors_layer())
        .with_state(state)
}

/// Add the builder API [`endpoint`]s to the given `router`.
pub fn with_endpoints(router: Router<State>) -> Router<State> {
    use endpoint::*;
    router
        .route(health_check::PATH, get(health_check::handler))
        .route(
            latest_solution_failures::PATH,
            get(latest_solution_failures::handler),
        )
        .route(submit_solution::PATH, post(submit_solution::handler))
}

/// The default CORS layer.
pub fn cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods([http::Method::GET, http::Method::OPTIONS, http::Method::POST])
        .allow_headers([http::header::CONTENT_TYPE])
}
