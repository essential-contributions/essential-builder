use essential_builder::{build_block_fifo, Config};
use essential_builder_db as builder_db;
use essential_node::{self as node, test_utils as util};
use essential_node_types::{register_contract_solution, BigBang};
use essential_types::solution::SolutionSet;
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
    let node_conf = node::db::pool::Config {
        conn_limit: 4,
        source: node::db::pool::Source::Memory(uuid::Uuid::new_v4().into()),
    };
    let db = node::db::ConnectionPool::with_tables(&node_conf).unwrap();
    let bb = BigBang::default();
    node::ensure_big_bang_block(&db, &bb).await.unwrap();
    db
}

#[tokio::test]
async fn build_block_all_solution_sets_succeed() {
    // Setup test configuration
    let builder_config = Config::default();

    // Create in-memory connection pools for the builder and node DBs
    let builder_conn_pool = test_builder_conn_pool().await;
    let node_conn_pool = test_node_conn_pool().await;

    // Generate and insert test solution sets
    let blocks = util::test_blocks_with_contracts(1, 101);
    let solution_sets = blocks
        .into_iter()
        .flat_map(|block| block.solution_sets)
        .map(Arc::new)
        .collect::<Vec<_>>();

    // Insert solution sets into the builder DB
    for solution_set in &solution_sets {
        let submission_timestamp = Duration::from_secs(0);
        builder_conn_pool
            .insert_solution_set_submission(solution_set.clone(), submission_timestamp)
            .await
            .unwrap();
    }

    // Build the block.
    let (_, summary) = build_block_fifo(&builder_conn_pool, &node_conn_pool, &builder_config)
        .await
        .unwrap();

    // Check that all solution sets succeeded
    assert_eq!(summary.succeeded.len(), solution_sets.len() + 1);
    assert_eq!(summary.failed.len(), 0);

    // Check that the solution sets were deleted after being used
    let remaining_solution_sets = builder_conn_pool
        .list_solution_sets(Duration::ZERO..Duration::from_secs(i64::MAX as _), i64::MAX)
        .await
        .unwrap();
    assert!(remaining_solution_sets.is_empty());
}

#[tokio::test]
async fn build_block_all_solution_sets_fail() {
    // Setup test configuration
    let builder_config = Config::default();

    // Create in-memory connection pools for the builder and node DBs
    let builder_conn_pool = test_builder_conn_pool().await;
    let node_conn_pool = test_node_conn_pool().await;

    // Generate and insert test solution sets that will fail (mock failing conditions)
    let (blocks, _contracts, _programs) = util::test_blocks(100);
    let solution_sets = blocks
        .into_iter()
        .flat_map(|block| block.solution_sets)
        .map(Arc::new)
        .collect::<Vec<_>>();

    // Insert solution sets into the builder DB
    for solution_set in &solution_sets {
        let timestamp = Duration::from_secs(0); // Use a fixed timestamp for simplicity
        builder_conn_pool
            .insert_solution_set_submission(solution_set.clone(), timestamp)
            .await
            .unwrap();
    }

    // Build the block.
    // We haven't inserted any contracts, so all solution sets should fail.
    let (_, summary) = build_block_fifo(&builder_conn_pool, &node_conn_pool, &builder_config)
        .await
        .unwrap();

    // Check that all solution sets failed
    assert_eq!(summary.failed.len(), solution_sets.len());
    assert_eq!(summary.succeeded.len(), 1); // Only the block state solution set succeeds.

    // Check that solution set failures are recorded
    for (ca, solution_set_ix, _invalid_solution_set) in summary.failed {
        let failures = builder_conn_pool
            .latest_solution_set_failures(ca.clone(), 1)
            .await
            .unwrap();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].attempt_solution_set_ix, solution_set_ix);
    }

    // Check that the solution sets were deleted after being attempted
    let remaining_solution_sets = builder_conn_pool
        .list_solution_sets(Duration::ZERO..Duration::from_secs(i64::MAX as _), i64::MAX)
        .await
        .unwrap();
    assert!(remaining_solution_sets.is_empty());
}

#[tokio::test]
async fn build_block_no_solution_sets() {
    // Setup test configuration
    let builder_config = Config::default();

    // Create in-memory connection pools for the builder and node DBs
    let builder_conn_pool = test_builder_conn_pool().await;
    let node_conn_pool = test_node_conn_pool().await;

    // No solution sets are inserted into the builder DB

    // Build the block.
    let (_, summary) = build_block_fifo(&builder_conn_pool, &node_conn_pool, &builder_config)
        .await
        .unwrap();

    // Check that there are no succeeded or failed solution sets besides the block state solution
    // set.
    assert_eq!(summary.succeeded.len(), 1);
    assert_eq!(summary.failed.len(), 0);
}

#[tokio::test]
async fn build_block_mixed_solution_sets() {
    // Setup test configuration
    let builder_config = Config::default();

    // Create in-memory connection pools for the builder and node DBs
    let builder_conn_pool = test_builder_conn_pool().await;
    let node_conn_pool = test_node_conn_pool().await;
    let registry = BigBang::default().contract_registry;

    // Generate and insert test solution sets, some of which will fail and others will succeed
    let (blocks, contracts, _programs) = util::test_blocks(10);
    let solution_sets = blocks
        .into_iter()
        .enumerate()
        .flat_map(|(i, mut block)| {
            // Only for the 3rd solution set include its contract so that one solution set succeeds.
            let ix = 2;
            if i == ix {
                let sol = register_contract_solution(registry.clone(), &contracts[ix]).unwrap();
                let solution_set = SolutionSet {
                    solutions: vec![sol],
                };
                block.solution_sets.insert(0, solution_set);
            }
            block.solution_sets
        })
        .map(Arc::new)
        .collect::<Vec<_>>();

    // Insert solution sets into the builder DB
    for solution_set in &solution_sets {
        let timestamp = Duration::from_secs(0); // Use a fixed timestamp for simplicity
        builder_conn_pool
            .insert_solution_set_submission(solution_set.clone(), timestamp)
            .await
            .unwrap();
    }

    // Build the block.
    let (_, summary) = build_block_fifo(&builder_conn_pool, &node_conn_pool, &builder_config)
        .await
        .unwrap();

    // Check that some solution sets succeeded and some failed
    assert!(!summary.succeeded.is_empty());
    assert!(!summary.failed.is_empty());

    // Check that solution set failures are recorded for the failed ones
    for (ca, solution_set_ix, _invalid_solution_set) in summary.failed {
        let failures = builder_conn_pool
            .latest_solution_set_failures(ca, 1)
            .await
            .unwrap();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].attempt_solution_set_ix, solution_set_ix);
    }

    // Check that the solution sets were deleted after being attempted
    let remaining_solution_sets = builder_conn_pool
        .list_solution_sets(Duration::ZERO..Duration::from_secs(i64::MAX as _), i64::MAX)
        .await
        .unwrap();
    assert!(remaining_solution_sets.is_empty());
}
