use super::*;

#[tokio::test]
async fn test_args() {
    let args = Args::parse_from(Some(""));

    let r = tokio::time::timeout(Duration::from_millis(100), run(args.clone())).await;
    // Error means that the timeout was reached which means that the run
    // ran successfully
    assert!(r.is_err());

    // Block interval 1
    let mut a = args.clone();
    a.block_interval_ms = 1;

    let r = tokio::time::timeout(Duration::from_millis(100), run(a)).await;
    // Error means that the timeout was reached which means that the run
    // ran successfully
    assert!(r.is_err());

    // Keep zero failures
    let mut a = args.clone();
    a.solution_failures_to_keep = 0;

    let r = tokio::time::timeout(Duration::from_millis(100), run(a)).await;
    // Error means that the timeout was reached which means that the run
    // ran successfully
    assert!(r.is_err());

    // validation
    let mut a = args.clone();
    a.validation = true;

    let r = tokio::time::timeout(Duration::from_millis(100), run(a)).await;
    // Error means that the timeout was reached which means that the run
    // ran successfully
    assert!(r.is_err());
}
