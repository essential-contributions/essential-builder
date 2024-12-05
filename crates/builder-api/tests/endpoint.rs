use essential_builder_api as builder_api;
use essential_builder_types::SolutionSetFailure;
use essential_node::test_utils as test_util;
use essential_types::ContentAddress;
use std::{sync::Arc, time::Duration};
use util::{
    client, get_url, init_tracing_subscriber, reqwest_get, state, test_conn_pool, with_test_server,
};

mod util;

#[tokio::test]
async fn test_health_check() {
    #[cfg(feature = "tracing")]
    init_tracing_subscriber();

    let db = test_conn_pool();
    with_test_server(state(db), |port| async move {
        let response = reqwest_get(port, builder_api::endpoint::health_check::PATH).await;
        assert!(response.status().is_success());
    })
    .await;
}

#[tokio::test]
async fn test_submit_solution_set() {
    #[cfg(feature = "tracing")]
    init_tracing_subscriber();

    let db = test_conn_pool();

    // Generate and insert test solution sets
    let (blocks, _contracts, _programs) = test_util::test_blocks(100);
    let solution_sets = blocks
        .into_iter()
        .flat_map(|block| block.solution_sets)
        .map(Arc::new)
        .collect::<Vec<_>>();

    // Submit all of the solution sets via the API.
    let solution_sets2 = solution_sets.clone();
    with_test_server(state(db.clone()), |port| async move {
        // Submit all of the solution sets.
        for solution_set in solution_sets2 {
            let solution_set_ca = essential_hash::content_addr(&*solution_set);
            let response = client()
                .post(get_url(
                    port,
                    builder_api::endpoint::submit_solution_set::PATH,
                ))
                .json(&*solution_set)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(
                solution_set_ca,
                response.json::<ContentAddress>().await.unwrap()
            );
        }
    })
    .await;

    // List all the solution sets from the DB to check they're there.
    let min = Duration::ZERO;
    let max = Duration::from_secs(i64::MAX as u64);
    let range = min..max;
    let limit = i64::MAX;
    let fetched_solution_sets = db.list_solution_sets(range, limit).await.unwrap();
    for (set, (_ca, fetched_set, _ts)) in solution_sets.into_iter().zip(fetched_solution_sets) {
        assert_eq!(*set, fetched_set);
    }
}

#[tokio::test]
async fn test_latest_solution_set_failures() {
    #[cfg(feature = "tracing")]
    init_tracing_subscriber();

    let db = test_conn_pool();

    // Fake some solution set failures.
    const N_FAILURES: i64 = 3;
    let solution_set_cas: Vec<_> = (0..N_FAILURES)
        .map(|i| ContentAddress([i as u8; 32]))
        .collect();
    let failures: Vec<_> = (0..N_FAILURES)
        .map(|i| SolutionSetFailure {
            attempt_block_num: i,
            attempt_block_addr: ContentAddress([i as u8; 32]),
            attempt_solution_set_ix: 0,
            err_msg: format!("failure {i}").into(),
        })
        .collect();

    // Insert them into the DB so that they're queryable.
    for (solution_set_ca, failure) in solution_set_cas.iter().zip(&failures) {
        db.insert_solution_set_failure(solution_set_ca.clone(), failure.clone())
            .await
            .unwrap();
    }

    // Submit all of the solution sets via the API.
    let fetched_failures = with_test_server(state(db.clone()), |port| async move {
        let mut failures = vec![];
        for ca in solution_set_cas {
            let limit = 1;
            let response =
                reqwest_get(port, &format!("/latest-solution-set-failures/{ca}/{limit}")).await;
            assert_eq!(response.status(), 200);
            let failure = response
                .json::<Vec<SolutionSetFailure<'static>>>()
                .await
                .unwrap();
            failures.extend(failure);
        }
        failures
    })
    .await;

    // Check the fetched failures match those we inserted.
    for (failure, fetched_failure) in failures.into_iter().zip(fetched_failures) {
        assert_eq!(failure, fetched_failure);
    }
}

#[tokio::test]
async fn test_list_solution_set_failures() {
    #[cfg(feature = "tracing")]
    init_tracing_subscriber();

    let db = test_conn_pool();

    // Fake some solution set failures.
    const N_FAILURES: i64 = 3;
    let solution_set_cas: Vec<_> = (0..N_FAILURES)
        .map(|i| ContentAddress([i as u8; 32]))
        .collect();
    let failures: Vec<_> = (0..N_FAILURES)
        .map(|i| SolutionSetFailure {
            attempt_block_num: i,
            attempt_block_addr: ContentAddress([i as u8; 32]),
            attempt_solution_set_ix: 0,
            err_msg: format!("failure {i}").into(),
        })
        .collect();

    // Insert them into the DB so that they're queryable.
    for (solution_set_ca, failure) in solution_set_cas.iter().zip(&failures) {
        db.insert_solution_set_failure(solution_set_ca.clone(), failure.clone())
            .await
            .unwrap();
    }

    // Submit all of the solution sets via the API.
    let fetched_failures = with_test_server(state(db.clone()), |port| async move {
        let mut failures = vec![];
        for (start, _) in solution_set_cas.iter().enumerate() {
            let limit = 1;
            let response = reqwest_get(
                port,
                &format!("/list-solution-set-failures/{start}/{limit}"),
            )
            .await;
            assert_eq!(response.status(), 200);
            let failure = response
                .json::<Vec<SolutionSetFailure<'static>>>()
                .await
                .unwrap();
            failures.extend(failure);
        }
        failures
    })
    .await;

    // Check the fetched failures match those we inserted.
    for (failure, fetched_failure) in failures.into_iter().rev().zip(fetched_failures) {
        assert_eq!(failure, fetched_failure);
    }
}
