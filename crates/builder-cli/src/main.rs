use anyhow::Context;
use clap::{Parser, ValueEnum};
use essential_builder::{self as builder, build_block_fifo};
use essential_builder_api as builder_api;
use essential_builder_db as builder_db;
use essential_node as node;
use essential_node_api as node_api;
use std::{
    net::{SocketAddr, SocketAddrV4},
    path::PathBuf,
    time::{Duration, Instant},
};

/// The Essential Builder CLI.
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Disable the tracing subscriber.
    #[arg(long, default_value_t = false)]
    disable_tracing: bool,
    /// Specify the interval at which the builder will attempt to build new blocks.
    ///
    /// This is a temporary parameter, mainly named at emulating the experience of waiting for the
    /// L1 block time.
    #[arg(long, default_value_t = DEFAULT_BLOCK_INTERVAL_MS)]
    block_interval_ms: u32,

    // ----- builder API -----
    /// The address to bind to for the builder API's TCP listener.
    #[arg(long, default_value_t = SocketAddrV4::new([0; 4].into(), 0).into())]
    builder_api_bind_address: SocketAddr,
    /// The maximum number of builder API TCP streams to be served.
    #[arg(long, default_value_t = builder_api::DEFAULT_CONNECTION_LIMIT)]
    builder_api_tcp_conn_limit: usize,

    // ----- node API -----
    /// The address to bind to for the node API's TCP listener.
    #[arg(long, default_value_t = SocketAddrV4::new([0; 4].into(), 0).into())]
    node_api_bind_address: SocketAddr,
    /// The maximum number of node API TCP streams to be served.
    #[arg(long, default_value_t = node_api::DEFAULT_CONNECTION_LIMIT)]
    node_api_tcp_conn_limit: usize,

    // ----- builder DB -----
    /// The type of builder DB storage to use.
    ///
    /// In the case that "persistent" is specified, assumes the default path.
    #[arg(long, default_value_t = Db::Memory, value_enum)]
    builder_db: Db,
    /// The path to the builder's sqlite database.
    ///
    /// Specifying this overrides the `builder_db` type as `persistent`.
    ///
    /// By default, this path will be within the user's data directory.
    #[arg(long)]
    builder_db_path: Option<PathBuf>,
    /// The number of simultaneous sqlite DB connections to maintain for serving the API.
    ///
    /// By default, this is the number of available CPUs multiplied by 4.
    #[arg(long, default_value_t = builder_db::pool::Config::default_conn_limit())]
    builder_db_conn_limit: usize,

    // ----- node DB -----
    /// The type of node DB storage to use.
    ///
    /// In the case that "persistent" is specified, assumes the default path.
    #[arg(long, default_value_t = Db::Memory, value_enum)]
    node_db: Db,
    /// The path to the node's sqlite database.
    ///
    /// Specifying this overrides the `node_db` type as `persistent`.
    ///
    /// By default, this path will be within the user's data directory.
    #[arg(long)]
    node_db_path: Option<PathBuf>,
    /// The number of simultaneous sqlite DB connections to maintain for serving the API.
    ///
    /// By default, this is the number of available CPUs multiplied by 4.
    #[arg(long, default_value_t = node::db::Config::default_conn_limit())]
    node_db_conn_limit: usize,
}

const DEFAULT_BLOCK_INTERVAL_MS: u32 = 5_000;

#[derive(ValueEnum, Clone, Copy, Debug)]
enum Db {
    /// Temporary, in-memory storage that lasts for the duration of the process.
    Memory,
    /// Persistent storage on the local HDD or SSD.
    ///
    /// The DB path may be specified with `--db-path`.
    Persistent,
}

// The default path to the builder's DB.
fn default_builder_db_path() -> Option<PathBuf> {
    dirs::data_dir().map(|mut path| {
        path.extend(["essential", "builder", "db.sqlite"]);
        path
    })
}

// The default path to the node's DB.
fn default_node_db_path() -> Option<PathBuf> {
    dirs::data_dir().map(|mut path| {
        path.extend(["essential", "node", "db.sqlite"]);
        path
    })
}

/// Construct the builder's DB config from the parsed args.
fn builder_db_conf_from_args(args: &Args) -> anyhow::Result<builder_db::pool::Config> {
    let source = match (&args.builder_db, &args.builder_db_path) {
        (Db::Memory, None) => builder_db::pool::Source::default_memory(),
        (_, Some(path)) => builder_db::pool::Source::Path(path.clone()),
        (Db::Persistent, None) => {
            let Some(path) = default_builder_db_path() else {
                anyhow::bail!("unable to detect user's data directory for default DB path")
            };
            builder_db::pool::Source::Path(path)
        }
    };
    let conn_limit = args.builder_db_conn_limit;
    let config = builder_db::pool::Config { source, conn_limit };
    Ok(config)
}

/// Construct the node's DB config from the parsed args.
fn node_db_conf_from_args(args: &Args) -> anyhow::Result<node::db::Config> {
    let source = match (&args.node_db, &args.node_db_path) {
        (Db::Memory, None) => node::db::Source::default_memory(),
        (_, Some(path)) => node::db::Source::Path(path.clone()),
        (Db::Persistent, None) => {
            let Some(path) = default_node_db_path() else {
                anyhow::bail!("unable to detect user's data directory for default DB path")
            };
            node::db::Source::Path(path)
        }
    };
    let conn_limit = args.node_db_conn_limit;
    let config = node::db::Config { source, conn_limit };
    Ok(config)
}

/// Construct the builder's block-building config from the parsed args.
fn builder_conf_from_args(args: &Args) -> anyhow::Result<builder::Config> {
    todo!()
}

#[cfg(feature = "tracing")]
fn init_tracing_subscriber() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .try_init();
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = Args::parse();
    if let Err(_err) = run(args).await {
        #[cfg(feature = "tracing")]
        tracing::error!("{_err}");
    }
}

/// Run the essential builder.
async fn run(args: Args) -> anyhow::Result<()> {
    // Initialise tracing.
    if !args.disable_tracing {
        #[cfg(feature = "tracing")]
        init_tracing_subscriber()
    }

    // Initialize the node DB.
    let node_db_conf = node_db_conf_from_args(&args)?;
    #[cfg(feature = "tracing")]
    {
        tracing::debug!("Node DB config:\n{:#?}", node_db_conf);
        tracing::info!("Initializing node DB");
    }
    let node_conf = node::Config { db: node_db_conf };
    let node = node::Node::new(&node_conf)?;
    let node_db = node.db();

    // Run the node API.
    #[cfg(feature = "tracing")]
    let block_tx = node::BlockTx::new();
    let block_rx = block_tx.new_listener();
    let api_state = node_api::State {
        new_block: Some(block_rx),
        conn_pool: node_db.clone(),
    };
    let router = node_api::router(api_state);
    let listener = tokio::net::TcpListener::bind(args.bind_address).await?;
    #[cfg(feature = "tracing")]
    tracing::info!("Starting node API server at {}", listener.local_addr()?);
    let api = node_api::serve(&router, &listener, args.node_api_tcp_conn_limit);

    // Initialize the builder DB.
    let builder_db_conf = builder_db_conf_from_args(&args)?;
    #[cfg(feature = "tracing")]
    {
        tracing::debug!("Builder DB config:\n{:#?}", builder_db_conf);
        tracing::info!("Initializing builder DB");
    }
    let builder_db = builder_db::ConnectionPool::with_tables(&builder_db_conf)?;

    // Run the builder API.
    let api_state = builder_api::State {
        conn_pool: builder_db.clone(),
    };
    let router = builder_api::router(api_state);
    let listener = tokio::net::TcpListener::bind(args.bind_address).await?;
    #[cfg(feature = "tracing")]
    tracing::info!("Starting builder API server at {}", listener.local_addr()?);
    let api = builder_api::serve(&router, &listener, args.builder_api_tcp_conn_limit);

    // Run the block builder.
    let builder_conf = builder_conf_from_args(&args)?;
    let block_interval = Duration::from_millis(args.block_interval_ms.into());
    let builder = run_builder(
        builder_db.clone(),
        node_db.clone(),
        block_tx,
        builder_conf,
        block_interval,
    );

    // Select the first future to complete to close.
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::select! {
        _ = api => {},
        _ = ctrl_c => {},
        res = builder => res.context("Critical error during block building")?,
    }

    builder_db.close().map_err(|e| anyhow::anyhow!("{e}"))?;
    node.close().map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

/// Run the block building loop forever, or until we encounter a critical error.
async fn run_builder(
    builder_conn_pool: builder_db::ConnectionPool,
    node_conn_pool: node::db::ConnectionPool,
    block_tx: node::BlockTx,
    conf: builder::Config,
    block_interval: Duration,
) -> anyhow::Result<()> {
    let mut interval = tokio::time::interval(block_interval);
    loop {
        interval.tick().await;
        let summary = build_block_fifo(&builder_conn_pool, &node_conn_pool, &conf).await?;
        block_tx.notify();
    }
}
