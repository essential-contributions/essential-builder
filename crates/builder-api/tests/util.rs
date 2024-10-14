use essential_builder_api as builder_api;
use essential_builder_db as db;
use std::future::Future;

const LOCALHOST: &str = "127.0.0.1";

#[cfg(feature = "tracing")]
pub fn init_tracing_subscriber() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .try_init();
}

pub fn test_conn_pool() -> db::ConnectionPool {
    let conf = db::pool::Config {
        source: db::pool::Source::Memory(uuid::Uuid::new_v4().into()),
        ..Default::default()
    };
    let pool = db::ConnectionPool::new(&conf).unwrap();
    let mut conn = pool.try_acquire().unwrap();
    db::with_tx(&mut conn, |tx| db::create_tables(tx)).unwrap();
    pool
}

pub fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .http2_prior_knowledge() // Enforce HTTP/2
        .build()
        .unwrap()
}

async fn test_listener() -> tokio::net::TcpListener {
    tokio::net::TcpListener::bind(format!("{LOCALHOST}:0"))
        .await
        .unwrap()
}

/// Spawns a test server, then calls the given asynchronous function. Upon
/// completion, closes the server, panicking if any errors occurred.
pub async fn with_test_server<Fut>(
    state: builder_api::State,
    f: impl FnOnce(u16) -> Fut,
) -> Fut::Output
where
    Fut: Future,
{
    let router = builder_api::router(state);
    let listener = test_listener().await;
    let port = listener.local_addr().unwrap().port();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let api_jh = tokio::spawn(async move {
        tokio::select! {
            _ = builder_api::serve(&router, &listener, builder_api::DEFAULT_CONNECTION_LIMIT) => {},
            _ = shutdown_rx => {},
        }
    });
    let output = f(port).await;
    shutdown_tx.send(()).unwrap();
    api_jh.await.unwrap();
    output
}

pub fn get_url(port: u16, endpoint_path: &str) -> String {
    format!("http://{LOCALHOST}:{port}{endpoint_path}")
}

/// Shorthand for making a get request to the server instance at the given port for the given endpoint path.
pub async fn reqwest_get(port: u16, endpoint_path: &str) -> reqwest::Response {
    client()
        .get(get_url(port, endpoint_path))
        .send()
        .await
        .unwrap()
}

/// State that only has a DB connection pool and no new block TX (for non-subscription tests).
pub fn state(conn_pool: db::ConnectionPool) -> builder_api::State {
    builder_api::State { conn_pool }
}
