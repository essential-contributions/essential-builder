INSERT INTO submission (solution_id, timestamp_secs, timestamp_nanos)
VALUES (
    (
        SELECT id
        FROM solution
        WHERE content_addr = :solution_addr
        LIMIT 1
    ),
    :timestamp_secs,
    :timestamp_nanos
)
