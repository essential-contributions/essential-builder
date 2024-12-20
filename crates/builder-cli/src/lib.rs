use anyhow::Context;
use clap::{Parser, ValueEnum};
use essential_builder::{self as builder, build_block_fifo};
use essential_builder_api as builder_api;
use essential_builder_db as builder_db;
use essential_check::solution::CheckPredicateConfig;
use essential_node as node;
use essential_node_api as node_api;
use essential_node_types::{block_notify::BlockTx, BigBang};
use std::{
    net::{SocketAddr, SocketAddrV4},
    num::NonZero,
    path::{Path, PathBuf},
    time::Duration,
};

#[cfg(test)]
mod tests;

/// The Essential Builder CLI.
#[derive(Parser, Clone)]
#[command(version, about)]
pub struct Args {
    /// Disable the tracing subscriber.
    #[arg(long, default_value_t = false)]
    disable_tracing: bool,
    /// Specify the interval at which the builder will attempt to build new blocks.
    ///
    /// This is a temporary parameter, mainly named at emulating the experience of waiting for the
    /// L1 block time.
    #[arg(long, default_value_t = DEFAULT_BLOCK_INTERVAL_MS)]
    block_interval_ms: u32,
    /// The maximum number of solution set failures to keep in the DB, used to provide feedback to the
    /// submitters.
    #[arg(long, default_value_t = builder::Config::DEFAULT_SOLUTION_SET_FAILURE_KEEP_LIMIT)]
    solution_set_failures_to_keep: u32,
    /// The maximum number of solution sets to attempt to check and include in a block.
    #[arg(long, default_value_t = NonZero::new(builder::Config::DEFAULT_SOLUTION_SET_ATTEMPTS_PER_BLOCK).expect("declared const must be non-zero"))]
    solution_set_attempts_per_block: NonZero<u32>,
    /// The number of sequential solution sets to attempt to check in parallel at a time.
    ///
    /// If greater than `solution-set-attempts-per-block`, the `solution-set-attempts-per-block`
    /// is used instead.
    ///
    /// If unspecified, uses `num_cpus::get()`.
    #[arg(long, default_value_t = builder::Config::default_parallel_chunk_size())]
    parallel_chunk_size: NonZero<usize>,
    /// Whether or not to wait and collect all failures during solution set checking after a single
    /// state read or constraint fails.
    ///
    /// Potentially useful for debugging or testing tools.
    #[arg(long)]
    solution_set_check_collects_all_failures: bool,
    /// Specify a path to the `big-bang.yml` configuration.
    ///
    /// This specifies the genesis configuration, which includes items like the contract registry
    /// address, block state address and associated big-bang state.
    ///
    /// If no configuration is specified, defaults to the `BigBang::default()` implementation.
    ///
    /// To learn more, see the API docs for the `essential_node_types::BigBang` type.
    #[arg(long)]
    big_bang: Option<std::path::PathBuf>,

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
    #[arg(long, default_value_t = node::db::pool::Config::default_conn_limit())]
    node_db_conn_limit: usize,

    // ----- run node -----
    /// The endpoint of the node that will act as the layer-1 for the relayer.
    ///
    /// If this is `Some`, then the relayer stream will run.
    #[arg(long)]
    relayer_source_endpoint: Option<String>,
    /// Run the validation stream of the node.
    #[arg(long)]
    validation: bool,
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
        path.extend(["essential", "builder", "db.sqlite3"]);
        path
    })
}

// The default path to the node's DB.
fn default_node_db_path() -> Option<PathBuf> {
    dirs::data_dir().map(|mut path| {
        path.extend(["essential", "node", "db.sqlite3"]);
        path
    })
}

/// Construct the builder's DB config from the parsed args.
fn builder_db_conf_from_args(args: &Args) -> anyhow::Result<builder_db::pool::Config> {
    let source = match (&args.builder_db, &args.builder_db_path) {
        (Db::Memory, None) => {
            let id = format!("__essential-builder-db-{}", uuid::Uuid::new_v4());
            builder_db::pool::Source::Memory(id)
        }
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
fn node_db_conf_from_args(args: &Args) -> anyhow::Result<node::db::pool::Config> {
    let source = match (&args.node_db, &args.node_db_path) {
        (Db::Memory, None) => {
            let id = format!("__essential-node-db-{}", uuid::Uuid::new_v4());
            node::db::pool::Source::Memory(id)
        }
        (_, Some(path)) => node::db::pool::Source::Path(path.clone()),
        (Db::Persistent, None) => {
            let Some(path) = default_node_db_path() else {
                anyhow::bail!("unable to detect user's data directory for default DB path")
            };
            node::db::pool::Source::Path(path)
        }
    };
    let conn_limit = args.node_db_conn_limit;
    let config = node::db::pool::Config { source, conn_limit };
    Ok(config)
}

/// Construct the builder's block-building config from the parsed args.
fn builder_conf_from_args(args: &Args, big_bang: &BigBang) -> builder::Config {
    builder::Config {
        solution_set_failures_to_keep: args.solution_set_failures_to_keep,
        solution_set_attempts_per_block: args.solution_set_attempts_per_block,
        parallel_chunk_size: args.parallel_chunk_size,
        check: std::sync::Arc::new(CheckPredicateConfig {
            collect_all_failures: args.solution_set_check_collects_all_failures,
        }),
        contract_registry: big_bang.contract_registry.clone(),
        program_registry: big_bang.program_registry.clone(),
        block_state: big_bang.block_state.clone(),
    }
}

/// Construct the node's run config from the parsed args.
fn node_run_conf_from_args(args: &Args) -> node::RunConfig {
    node::RunConfig {
        relayer_source_endpoint: args.relayer_source_endpoint.clone(),
        run_validation: args.validation,
    }
}

/// Load the big bang configuration from the yml file at the given path, or produce the default if
/// no path is given.
fn load_big_bang_or_default(path: Option<&Path>) -> anyhow::Result<BigBang> {
    match path {
        None => Ok(BigBang::default()),
        Some(path) => {
            let big_bang_str = std::fs::read_to_string(path)
                .context("failed to read big bang configuration from path")?;
            serde_yaml::from_str(&big_bang_str)
                .context("failed to deserialize big bang configuration from YAML string")
        }
    }
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

/// Run the essential builder.
pub async fn run(args: Args) -> anyhow::Result<()> {
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
    let node_db = node::db::ConnectionPool::with_tables(&node_db_conf)?;

    // Load the big bang configuration, and ensure the big bang block exists.
    let big_bang = load_big_bang_or_default(args.big_bang.as_deref())?;
    node::ensure_big_bang_block(&node_db, &big_bang)
        .await
        .context("failed to ensure big bang block")?;

    // Run the node API.
    let block_tx = BlockTx::new();
    let block_rx = block_tx.new_listener();
    let api_state = node_api::State {
        new_block: Some(block_rx),
        conn_pool: node_db.clone(),
    };
    let router = node_api::router(api_state);
    let listener = tokio::net::TcpListener::bind(args.node_api_bind_address).await?;
    #[cfg(feature = "tracing")]
    tracing::info!("Starting node API server at {}", listener.local_addr()?);
    let node_api = node_api::serve(&router, &listener, args.node_api_tcp_conn_limit);

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
    let listener = tokio::net::TcpListener::bind(args.builder_api_bind_address).await?;
    #[cfg(feature = "tracing")]
    tracing::info!("Starting builder API server at {}", listener.local_addr()?);
    let builder_api = builder_api::serve(&router, &listener, args.builder_api_tcp_conn_limit);

    // Run the block builder.
    let builder_conf = builder_conf_from_args(&args, &big_bang);
    let block_interval = Duration::from_millis(args.block_interval_ms.into());
    let builder = run_builder(
        builder_db.clone(),
        node_db.clone(),
        block_tx.clone(),
        builder_conf,
        block_interval,
    );

    let node_run_conf = node_run_conf_from_args(&args);
    let node_run = {
        let node_db = node_db.clone();
        async move {
            if node_run_conf.relayer_source_endpoint.is_none() && !node_run_conf.run_validation {
                std::future::pending().await
            } else {
                node::run(
                    node_db.clone(),
                    node_run_conf,
                    big_bang.contract_registry.contract,
                    big_bang.program_registry.contract,
                    block_tx,
                )?
                .join()
                .await?;
                Ok::<_, anyhow::Error>(())
            }
        }
    };

    // Select the first future to complete to close.
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::select! {
        _ = builder_api => {},
        _ = node_api => (),
        _ = node_run => (),
        _ = ctrl_c => {},
        res = builder => res.context("Critical error during block building")?,
    }

    builder_db.close().map_err(|e| anyhow::anyhow!("{e}"))?;
    node_db.close().map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

/// Run the block building loop forever, or until we encounter a critical error.
async fn run_builder(
    builder_conn_pool: builder_db::ConnectionPool,
    node_conn_pool: node::db::ConnectionPool,
    block_tx: BlockTx,
    conf: builder::Config,
    block_interval: Duration,
) -> anyhow::Result<()> {
    #[cfg(feature = "tracing")]
    tracing::info!("Running the block builder");
    #[cfg(feature = "tracing")]
    tracing::debug!("Builder config:\n{:#?}", conf);
    let mut interval = tokio::time::interval(block_interval);
    loop {
        interval.tick().await;
        let (built_block_addr, _summary) =
            build_block_fifo(&builder_conn_pool, &node_conn_pool, &conf).await?;
        if built_block_addr.is_some() {
            block_tx.notify();
        }
    }
}
