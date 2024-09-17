use essential_builder_db as builder_db;
use rusqlite::Connection;

mod util;

#[test]
fn insert_solution_submission() {
    // Generate some test solutions with unique timestamps, some overlapping.
    let solutions: Vec<_> = util::test_blocks(100)
        .into_iter()
        .flat_map(|b| b.solutions.into_iter().map(move |s| (s, b.timestamp)))
        .collect();

    // Create an in-memory SQLite database.
    let mut conn = Connection::open_in_memory().unwrap();

    // Create the tables.
    let tx = conn.transaction().unwrap();
    builder_db::create_tables(&tx).unwrap();
    tx.commit().unwrap();

    // Write to solutions.
    let tx = conn.transaction().unwrap();
    for (solution, timestamp) in &solutions {
        builder_db::insert_solution_submission(&tx, solution, *timestamp).unwrap();
    }
    tx.commit().unwrap();
}

#[test]
fn insert_solution_failure() {
    // Generate a test solution and its content address.
    let block = util::test_blocks(1);
    let solution = block[0].solutions[0].clone();
    let ca = essential_hash::content_addr(&solution);

    // Create an in-memory SQLite database.
    let mut conn = Connection::open_in_memory().unwrap();

    // Create the necessary tables.
    let tx = conn.transaction().unwrap();
    builder_db::create_tables(&tx).unwrap();
    tx.commit().unwrap();

    // Insert a solution failure.
    let failure = builder_db::SolutionFailure {
        attempt_block_num: 1,
        attempt_solution_ix: 0,
        err_msg: "Failed to include solution".into(),
    };
    builder_db::insert_solution_failure(&conn, &ca, failure.clone()).unwrap();

    // Query the latest failures and check that the inserted failure is retrieved.
    let failures = builder_db::latest_solution_failures(&conn, &ca, 1).unwrap();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0], failure);
}
