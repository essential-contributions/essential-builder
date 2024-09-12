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
