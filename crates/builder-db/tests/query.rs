use essential_builder_db::{self as builder_db};
use essential_builder_types::SolutionSetFailure;
use rusqlite::Connection;
use std::time::Duration;

mod util;

#[test]
fn get_solution_set() {
    // Generate some test solution sets with unique timestamps, some overlapping.
    let solution_sets: Vec<_> = util::test_blocks(10)
        .into_iter()
        .flat_map(|b| b.solution_sets.into_iter().map(move |s| (s, b.timestamp)))
        .collect();

    // Create an in-memory SQLite database.
    let mut conn = Connection::open_in_memory().unwrap();

    // Create the necessary tables and write the solution sets.
    let tx = conn.transaction().unwrap();
    builder_db::create_tables(&tx).unwrap();
    for (solution_set, timestamp) in &solution_sets {
        builder_db::insert_solution_set_submission(&tx, solution_set, *timestamp).unwrap();
    }
    tx.commit().unwrap();

    // Check we can retrieve all solution sets.
    for (expected_solution_set, _ts) in &solution_sets {
        let ca = essential_hash::content_addr(expected_solution_set);
        let solution_set = builder_db::get_solution_set(&conn, &ca).unwrap().unwrap();
        assert_eq!(expected_solution_set, &solution_set);
    }
}

#[test]
fn list_solution_sets() {
    // Generate some test solution sets with unique timestamps and corresponding blobs.
    let solution_sets: Vec<_> = util::test_blocks(10)
        .into_iter()
        .flat_map(|b| b.solution_sets.into_iter().map(move |s| (s, b.timestamp)))
        .collect();

    // Create an in-memory SQLite database.
    let mut conn = Connection::open_in_memory().unwrap();

    // Create the necessary tables and write the solution sets.
    let tx = conn.transaction().unwrap();
    builder_db::create_tables(&tx).unwrap();
    for (solution_set, timestamp) in &solution_sets {
        builder_db::insert_solution_set_submission(&tx, solution_set, *timestamp).unwrap();
    }
    tx.commit().unwrap();

    // List all solution sets.
    let min = Duration::ZERO;
    let max = Duration::from_secs(i64::MAX as u64);
    let range = min..max;
    let limit = i64::MAX;
    let fetched_solution_sets =
        builder_db::list_solution_sets(&conn, range.clone(), limit).unwrap();

    // Check they match.
    for (expected, fetched) in solution_sets.iter().zip(&fetched_solution_sets) {
        let (expected_solution_set, expected_ts) = expected;
        let (ca, solution_set, ts) = fetched;
        assert_eq!(expected_solution_set, solution_set);
        assert_eq!(expected_ts, ts);
        assert_eq!(&essential_hash::content_addr(solution_set), ca);
    }

    // Check that duplicates are still yielded for each submission.
    // Let's write some duplicates of the first solution set into the DB.
    let pre_occurrences = fetched_solution_sets
        .iter()
        .filter(|(ca, _, _)| essential_hash::content_addr(&solution_sets[0].0) == *ca)
        .count();

    // Write the duplicates.
    const NUM_ADDED: usize = 4;
    let tx = conn.transaction().unwrap();
    for (solution_set, timestamp) in (0..NUM_ADDED).map(|_| &solution_sets[0]) {
        builder_db::insert_solution_set_submission(&tx, solution_set, *timestamp).unwrap();
    }
    tx.commit().unwrap();

    // We added the first solution set again 3 times, so there should be 4 occurrences.
    let fetched_solution_sets = builder_db::list_solution_sets(&conn, range, limit).unwrap();
    let post_occurrences = fetched_solution_sets
        .iter()
        .filter(|(ca, _, _)| essential_hash::content_addr(&solution_sets[0].0) == *ca)
        .count();
    assert_eq!(pre_occurrences + NUM_ADDED, post_occurrences);
}

#[test]
fn list_submissions() {
    // Generate some test solution sets with unique timestamps, some overlapping.
    let solution_sets: Vec<_> = util::test_blocks(10)
        .into_iter()
        .flat_map(|b| b.solution_sets.into_iter().map(move |s| (s, b.timestamp)))
        .collect();

    // Create an in-memory SQLite database.
    let mut conn = Connection::open_in_memory().unwrap();

    // Create the necessary tables and write the solution sets.
    let tx = conn.transaction().unwrap();
    builder_db::create_tables(&tx).unwrap();
    for (solution_set, timestamp) in &solution_sets {
        builder_db::insert_solution_set_submission(&tx, solution_set, *timestamp).unwrap();
    }
    tx.commit().unwrap();

    // Query all submissions.
    let min = Duration::ZERO;
    let max = Duration::from_secs(i64::MAX as u64);
    let range_all = min..max;
    let limit = i64::MAX;
    let submissions = builder_db::list_submissions(&conn, range_all.clone(), limit).unwrap();
    let expected_submissions: Vec<_> = solution_sets
        .iter()
        .map(|(s, ts)| (essential_hash::content_addr(s), *ts))
        .collect();
    assert_eq!(&expected_submissions, &submissions);

    // Query the solution sets for each submission.
    let queried_solution_sets: Vec<_> = submissions
        .iter()
        .map(|(ca, ts)| {
            let solution_set = builder_db::get_solution_set(&conn, ca).unwrap().unwrap();
            (solution_set, *ts)
        })
        .collect();
    assert_eq!(&solution_sets, &queried_solution_sets);

    // Query the solution sets between 1 and 4 seconds.
    let min = Duration::from_secs(1);
    let max = Duration::from_secs(4);
    let range = min..max;
    let submissions = builder_db::list_submissions(&conn, range.clone(), limit).unwrap();
    for (_ca, timestamp) in &submissions {
        assert!(range.contains(timestamp));
    }

    // Query all solution sets, but limit to only the first 3.
    let limit = 3;
    let submissions = builder_db::list_submissions(&conn, range_all, limit).unwrap();
    assert_eq!(submissions.len(), limit as usize);

    // Delete all solution sets, then check that our query is empty.
    let tx = conn.transaction().unwrap();
    for (solution_set, _) in &solution_sets {
        let ca = essential_hash::content_addr(solution_set);
        builder_db::delete_solution_set(&tx, &ca).unwrap();
    }
    tx.commit().unwrap();

    // Query all submissions.
    let min = Duration::ZERO;
    let max = Duration::from_secs(i64::MAX as u64);
    let range_all = min..max;
    let limit = i64::MAX;
    let submissions = builder_db::list_submissions(&conn, range_all.clone(), limit).unwrap();
    assert!(submissions.is_empty());
}

#[test]
fn latest_solution_set_failures() {
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

    // Query the last 2 failures and check ordering by block number and solution set index.
    let latest_failures = builder_db::latest_solution_set_failures(&conn, &ca, 2).unwrap();
    assert_eq!(latest_failures.len(), 2);
    assert_eq!(latest_failures[0], failures[2]); // Most recent failure
    assert_eq!(latest_failures[1], failures[1]); // Second most recent failure
}

#[test]
fn list_solution_set_failures() {
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

    // Query the last 2 failures and check ordering by block number and solution set index.
    let latest_failures = builder_db::list_solution_set_failures(&conn, 0, 2).unwrap();
    assert_eq!(latest_failures.len(), 2);
    assert_eq!(latest_failures[0], failures[2]); // Most recent failure
    assert_eq!(latest_failures[1], failures[1]); // Second most recent failure
}
