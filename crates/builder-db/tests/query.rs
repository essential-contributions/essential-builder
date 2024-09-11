use essential_builder_db as builder_db;
use rusqlite::Connection;
use std::time::Duration;

mod util;

#[test]
fn get_solution() {
    // Generate some test solutions with unique timestamps, some overlapping.
    let solutions: Vec<_> = util::test_blocks(10)
        .into_iter()
        .flat_map(|b| b.solutions.into_iter().map(move |s| (s, b.timestamp)))
        .collect();

    // Create an in-memory SQLite database.
    let mut conn = Connection::open_in_memory().unwrap();

    // Create the necessary tables and write the solutions.
    let tx = conn.transaction().unwrap();
    builder_db::create_tables(&tx).unwrap();
    for (solution, timestamp) in &solutions {
        builder_db::insert_solution_submission(&tx, solution, *timestamp).unwrap();
    }
    tx.commit().unwrap();

    // Check we can retrieve all solutions.
    for (expected_solution, _ts) in &solutions {
        let ca = essential_hash::content_addr(expected_solution);
        let solution = builder_db::get_solution(&conn, &ca).unwrap().unwrap();
        assert_eq!(expected_solution, &solution);
    }
}

#[test]
fn list_solutions() {
    // Generate some test solutions with unique timestamps and corresponding blobs.
    let solutions: Vec<_> = util::test_blocks(10)
        .into_iter()
        .flat_map(|b| b.solutions.into_iter().map(move |s| (s, b.timestamp)))
        .collect();

    // Create an in-memory SQLite database.
    let mut conn = Connection::open_in_memory().unwrap();

    // Create the necessary tables and write the solutions.
    let tx = conn.transaction().unwrap();
    builder_db::create_tables(&tx).unwrap();
    for (solution, timestamp) in &solutions {
        builder_db::insert_solution_submission(&tx, solution, *timestamp).unwrap();
    }
    tx.commit().unwrap();

    // List all solutions.
    let min = Duration::ZERO;
    let max = Duration::from_secs(i64::MAX as u64);
    let range = min..max;
    let limit = i64::MAX;
    let fetched_solutions = builder_db::list_solutions(&conn, range.clone(), limit).unwrap();

    // Check they match.
    for (expected, fetched) in solutions.iter().zip(&fetched_solutions) {
        let (expected_solution, expected_ts) = expected;
        let (ca, solution, ts) = fetched;
        assert_eq!(expected_solution, solution);
        assert_eq!(expected_ts, ts);
        assert_eq!(&essential_hash::content_addr(solution), ca);
    }

    // Check that duplicates are still yielded for each submission.
    // Let's write some duplicates of the first solution into the DB.
    let pre_occurrences = fetched_solutions
        .iter()
        .filter(|(ca, _, _)| essential_hash::content_addr(&solutions[0].0) == *ca)
        .count();

    // Write the duplicates.
    const NUM_ADDED: usize = 4;
    let tx = conn.transaction().unwrap();
    for (solution, timestamp) in (0..NUM_ADDED).map(|_| &solutions[0]) {
        builder_db::insert_solution_submission(&tx, solution, *timestamp).unwrap();
    }
    tx.commit().unwrap();

    // We added the first solution again 3 times, so there should be 4 occurrences.
    let fetched_solutions = builder_db::list_solutions(&conn, range, limit).unwrap();
    let post_occurrences = fetched_solutions
        .iter()
        .filter(|(ca, _, _)| essential_hash::content_addr(&solutions[0].0) == *ca)
        .count();
    assert_eq!(pre_occurrences + NUM_ADDED, post_occurrences);
}

#[test]
fn list_submissions() {
    // Generate some test solutions with unique timestamps, some overlapping.
    let solutions: Vec<_> = util::test_blocks(10)
        .into_iter()
        .flat_map(|b| b.solutions.into_iter().map(move |s| (s, b.timestamp)))
        .collect();

    // Create an in-memory SQLite database.
    let mut conn = Connection::open_in_memory().unwrap();

    // Create the necessary tables and write the solutions.
    let tx = conn.transaction().unwrap();
    builder_db::create_tables(&tx).unwrap();
    for (solution, timestamp) in &solutions {
        builder_db::insert_solution_submission(&tx, solution, *timestamp).unwrap();
    }
    tx.commit().unwrap();

    // Query all submissions.
    let min = Duration::ZERO;
    let max = Duration::from_secs(i64::MAX as u64);
    let range_all = min..max;
    let limit = i64::MAX;
    let submissions = builder_db::list_submissions(&conn, range_all.clone(), limit).unwrap();
    let expected_submissions: Vec<_> = solutions
        .iter()
        .map(|(s, ts)| (essential_hash::content_addr(s), *ts))
        .collect();
    assert_eq!(&expected_submissions, &submissions);

    // Query the solutions for each submission.
    let queried_solutions: Vec<_> = submissions
        .iter()
        .map(|(ca, ts)| {
            let solution = builder_db::get_solution(&conn, ca).unwrap().unwrap();
            (solution, *ts)
        })
        .collect();
    assert_eq!(&solutions, &queried_solutions);

    // Query the solutions between 1 and 4 seconds.
    let min = Duration::from_secs(1);
    let max = Duration::from_secs(4);
    let range = min..max;
    let submissions = builder_db::list_submissions(&conn, range.clone(), limit).unwrap();
    for (_ca, timestamp) in &submissions {
        assert!(range.contains(timestamp));
    }

    // Query all solutions, but limit to only the first 3.
    let limit = 3;
    let submissions = builder_db::list_submissions(&conn, range_all, limit).unwrap();
    assert_eq!(submissions.len(), limit as usize);
}
