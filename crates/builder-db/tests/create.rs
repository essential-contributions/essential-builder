use essential_builder_db as builder_db;
use rusqlite::Connection;

#[test]
fn create_tables() {
    // Create an in-memory SQLite database.
    let mut conn = Connection::open_in_memory().unwrap();

    // Create the tables.
    let tx = conn.transaction().unwrap();
    builder_db::create_tables(&tx).unwrap();
    tx.commit().unwrap();
}
