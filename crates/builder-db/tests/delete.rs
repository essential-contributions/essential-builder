use essential_builder_db::{self as builder_db};
use essential_builder_types::SolutionSetFailure;
use rusqlite::Connection;
use std::time::Duration;

mod util;

#[test]
fn delete_oldest_failures() {
    // Generate a test solution set and its content address.
    let block = util::test_block(0, Duration::from_secs(0));
    let block_ca = essential_hash::content_addr(&block);
    let solution_set = block.solution_sets[0].clone();
    let ca = essential_hash::content_addr(&solution_set);

    // Create an in-memory SQLite database.
    let mut conn = Connection::open_in_memory().unwrap();

    // Create the necessary tables.
    let tx = conn.transaction().unwrap();
    builder_db::create_tables(&tx).unwrap();
    builder_db::insert_solution_set_submission(&tx, &solution_set, block.timestamp).unwrap();
    tx.commit().unwrap();

    // Insert multiple solution set failures.
    let failures = vec![
        SolutionSetFailure {
            attempt_block_num: 1,
            attempt_block_addr: block_ca.clone(),
            attempt_solution_set_ix: 0,
            err_msg: "Failure 1".into(),
        },
        SolutionSetFailure {
            attempt_block_num: 2,
            attempt_block_addr: block_ca.clone(),
            attempt_solution_set_ix: 1,
            err_msg: "Failure 2".into(),
        },
        SolutionSetFailure {
            attempt_block_num: 3,
            attempt_block_addr: block_ca,
            attempt_solution_set_ix: 2,
            err_msg: "Failure 3".into(),
        },
    ];
    for failure in &failures {
        builder_db::insert_solution_set_failure(&conn, &ca, failure.clone()).unwrap();
    }

    // Ensure all failures are inserted.
    let all_failures = builder_db::latest_solution_set_failures(&conn, &ca, 10).unwrap();
    assert_eq!(all_failures.len(), 3);

    // Delete the oldest failure to maintain a keep limit of 2.
    builder_db::delete_oldest_solution_set_failures(&conn, 2).unwrap();

    // Query remaining failures and check that the oldest one is deleted.
    let remaining_failures = builder_db::latest_solution_set_failures(&conn, &ca, 10).unwrap();
    assert_eq!(remaining_failures.len(), 2);
    assert_eq!(remaining_failures[0], failures[2]); // Most recent failure
    assert_eq!(remaining_failures[1], failures[1]); // Second most recent failure
}
