DELETE FROM solution_failure
WHERE id IN (
    SELECT id
    FROM solution_failure
    ORDER BY attempt_block_num ASC, attempt_solution_ix ASC
    LIMIT CASE
        WHEN (SELECT COUNT(*) FROM solution_failure) > :keep_limit
        THEN (SELECT COUNT(*) FROM solution_failure) - :keep_limit
        ELSE 0
    END
)
