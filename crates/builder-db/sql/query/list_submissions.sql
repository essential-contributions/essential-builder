SELECT
    sol_set.content_addr,
    sub.timestamp_secs,
    sub.timestamp_nanos
FROM
    solution_set sol_set
JOIN
    submission sub
ON
    sol_set.id = sub.solution_set_id
WHERE
    (sub.timestamp_secs > :start_secs OR
    (sub.timestamp_secs = :start_secs AND sub.timestamp_nanos >= :start_nanos))
AND
    (sub.timestamp_secs < :end_secs OR
    (sub.timestamp_secs = :end_secs AND sub.timestamp_nanos < :end_nanos))
LIMIT :limit
