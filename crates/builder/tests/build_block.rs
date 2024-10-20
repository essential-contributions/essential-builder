use essential_builder::{build_block_fifo, Config};
use essential_builder_db as builder_db;
use essential_node::{self as node, test_utils as util};
use essential_node_types::{register_contract_solution, BigBang};
use essential_types::solution::Solution;
use std::sync::Arc;
use std::time::Duration;

async fn test_builder_conn_pool() -> builder_db::ConnectionPool {
    let config = builder_db::pool::Config {
        conn_limit: 4,
        source: builder_db::pool::Source::Memory(uuid::Uuid::new_v4().into()),
    };
    let conn_pool = builder_db::ConnectionPool::new(&config).unwrap();
    conn_pool.create_tables().await.unwrap();
    conn_pool
}

async fn test_node_conn_pool() -> node::db::ConnectionPool {
    let node_conf = node::db::Config {
        conn_limit: 4,
        source: node::db::Source::Memory(uuid::Uuid::new_v4().into()),
    };
    let db = node::db(&node_conf).unwrap();
    let bb = BigBang::default();
    node::ensure_big_bang_block(&db, &bb).await.unwrap();
    db
}

#[tokio::test]
async fn build_block_all_solutions_succeed() {
    // Setup test configuration
    let builder_config = Config::default();

    // Create in-memory connection pools for the builder and node DBs
    let builder_conn_pool = test_builder_conn_pool().await;
    let node_conn_pool = test_node_conn_pool().await;

    // Generate and insert test solutions
    let blocks = util::test_blocks_with_contracts(1, 101);
    let solutions = blocks
        .into_iter()
        .flat_map(|block| block.solutions)
        .map(Arc::new)
        .collect::<Vec<_>>();

    // Insert solutions into the builder DB
    for solution in &solutions {
        let submission_timestamp = Duration::from_secs(0);
        builder_conn_pool
            .insert_solution_submission(solution.clone(), submission_timestamp)
            .await
            .unwrap();
    }

    // Build the block.
    let (_, summary) = build_block_fifo(&builder_conn_pool, &node_conn_pool, &builder_config)
        .await
        .unwrap();

    // Check that all solutions succeeded
    assert_eq!(summary.succeeded.len(), solutions.len() + 1);
    assert_eq!(summary.failed.len(), 0);

    // Check that the solutions were deleted after being used
    let remaining_solutions = builder_conn_pool
        .list_solutions(Duration::ZERO..Duration::from_secs(i64::MAX as _), i64::MAX)
        .await
        .unwrap();
    assert!(remaining_solutions.is_empty());
}

#[tokio::test]
async fn build_block_all_solutions_fail() {
    // Setup test configuration
    let builder_config = Config::default();

    // Create in-memory connection pools for the builder and node DBs
    let builder_conn_pool = test_builder_conn_pool().await;
    let node_conn_pool = test_node_conn_pool().await;

    // Generate and insert test solutions that will fail (mock failing conditions)
    let (blocks, _contracts) = util::test_blocks(100);
    let solutions = blocks
        .into_iter()
        .flat_map(|block| block.solutions)
        .map(Arc::new)
        .collect::<Vec<_>>();

    // Insert solutions into the builder DB
    for solution in &solutions {
        let timestamp = Duration::from_secs(0); // Use a fixed timestamp for simplicity
        builder_conn_pool
            .insert_solution_submission(solution.clone(), timestamp)
            .await
            .unwrap();
    }

    // Build the block.
    // We haven't inserted any contracts, so all solutions should fail.
    let (_, summary) = build_block_fifo(&builder_conn_pool, &node_conn_pool, &builder_config)
        .await
        .unwrap();

    // Check that all solutions failed
    assert_eq!(summary.failed.len(), solutions.len());
    assert_eq!(summary.succeeded.len(), 1); // Only the block state solution succeeds.

    // Check that solution failures are recorded
    for (ca, solution_ix, _invalid_solution) in summary.failed {
        let failures = builder_conn_pool
            .latest_solution_failures(ca.clone(), 1)
            .await
            .unwrap();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].attempt_solution_ix, solution_ix);
    }

    // Check that the solutions were deleted after being attempted
    let remaining_solutions = builder_conn_pool
        .list_solutions(Duration::ZERO..Duration::from_secs(i64::MAX as _), i64::MAX)
        .await
        .unwrap();
    assert!(remaining_solutions.is_empty());
}

#[tokio::test]
async fn build_block_no_solutions() {
    // Setup test configuration
    let builder_config = Config::default();

    // Create in-memory connection pools for the builder and node DBs
    let builder_conn_pool = test_builder_conn_pool().await;
    let node_conn_pool = test_node_conn_pool().await;

    // No solutions are inserted into the builder DB

    // Build the block.
    let (_, summary) = build_block_fifo(&builder_conn_pool, &node_conn_pool, &builder_config)
        .await
        .unwrap();

    // Check that there are no succeeded or failed solutions besides the block state solution.
    assert_eq!(summary.succeeded.len(), 1);
    assert_eq!(summary.failed.len(), 0);
}

#[tokio::test]
async fn build_block_mixed_solutions() {
    // Setup test configuration
    let builder_config = Config::default();

    // Create in-memory connection pools for the builder and node DBs
    let builder_conn_pool = test_builder_conn_pool().await;
    let node_conn_pool = test_node_conn_pool().await;
    let registry = BigBang::default().contract_registry;

    // Generate and insert test solutions, some of which will fail and others will succeed
    let (blocks, contracts) = util::test_blocks(10);
    let solutions = blocks
        .into_iter()
        .enumerate()
        .flat_map(|(i, mut block)| {
            // Only for the 3rd solution include its contract so that one solution succeeds.
            let ix = 2;
            if i == ix {
                let sol = register_contract_solution(registry.clone(), &contracts[ix]).unwrap();
                let solution = Solution { data: vec![sol] };
                block.solutions.insert(0, solution);
            }
            block.solutions
        })
        .map(Arc::new)
        .collect::<Vec<_>>();

    // Insert solutions into the builder DB
    for solution in &solutions {
        let timestamp = Duration::from_secs(0); // Use a fixed timestamp for simplicity
        builder_conn_pool
            .insert_solution_submission(solution.clone(), timestamp)
            .await
            .unwrap();
    }

    // Build the block.
    let (_, summary) = build_block_fifo(&builder_conn_pool, &node_conn_pool, &builder_config)
        .await
        .unwrap();

    // Check that some solutions succeeded and some failed
    assert!(!summary.succeeded.is_empty());
    assert!(!summary.failed.is_empty());

    // Check that solution failures are recorded for the failed ones
    for (ca, solution_ix, _invalid_solution) in summary.failed {
        let failures = builder_conn_pool
            .latest_solution_failures(ca, 1)
            .await
            .unwrap();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].attempt_solution_ix, solution_ix);
    }

    // Check that the solutions were deleted after being attempted
    let remaining_solutions = builder_conn_pool
        .list_solutions(Duration::ZERO..Duration::from_secs(i64::MAX as _), i64::MAX)
        .await
        .unwrap();
    assert!(remaining_solutions.is_empty());
}
