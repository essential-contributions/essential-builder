INSERT INTO submission (solution_set_id, timestamp_secs, timestamp_nanos)
VALUES (
    (
        SELECT id
        FROM solution_set
        WHERE content_addr = :solution_set_addr
        LIMIT 1
    ),
    :timestamp_secs,
    :timestamp_nanos
)
