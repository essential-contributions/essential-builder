use essential_builder_db as builder_db;
use essential_builder_types::SolutionSetFailure;
use rusqlite::Connection;

mod util;

#[test]
fn insert_solution_set_submission() {
    // Generate some test solution sets with unique timestamps, some overlapping.
    let solution_sets: Vec<_> = util::test_blocks(100)
        .into_iter()
        .flat_map(|b| b.solution_sets.into_iter().map(move |s| (s, b.timestamp)))
        .collect();

    // Create an in-memory SQLite database.
    let mut conn = Connection::open_in_memory().unwrap();

    // Create the tables.
    let tx = conn.transaction().unwrap();
    builder_db::create_tables(&tx).unwrap();
    tx.commit().unwrap();

    // Write to solution sets.
    let tx = conn.transaction().unwrap();
    for (solution_set, timestamp) in &solution_sets {
        builder_db::insert_solution_set_submission(&tx, solution_set, *timestamp).unwrap();
    }
    tx.commit().unwrap();
}

#[test]
fn insert_solution_set_failure() {
    // Generate a test solution set and its content address.
    let blocks = util::test_blocks(1);
    let solution_set = blocks[0].solution_sets[0].clone();
    let ca = essential_hash::content_addr(&solution_set);
    let block_ca = essential_hash::content_addr(&blocks[0]);

    // Create an in-memory SQLite database.
    let mut conn = Connection::open_in_memory().unwrap();

    // Create the necessary tables.
    let tx = conn.transaction().unwrap();
    builder_db::create_tables(&tx).unwrap();
    tx.commit().unwrap();

    // Insert a solution set failure.
    let failure = SolutionSetFailure {
        attempt_block_num: 1,
        attempt_block_addr: block_ca,
        attempt_solution_set_ix: 0,
        err_msg: "Failed to include solution set ".into(),
    };
    builder_db::insert_solution_set_failure(&conn, &ca, failure.clone()).unwrap();

    // Query the latest failures and check that the inserted failure is retrieved.
    let failures = builder_db::latest_solution_set_failures(&conn, &ca, 1).unwrap();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0], failure);
}
