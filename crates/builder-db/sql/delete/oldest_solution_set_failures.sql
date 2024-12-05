DELETE FROM solution_set_failure
WHERE id IN (
    SELECT id
    FROM solution_set_failure
    ORDER BY attempt_block_num ASC, attempt_solution_set_ix ASC
    LIMIT CASE
        WHEN (SELECT COUNT(*) FROM solution_set_failure) > :keep_limit
        THEN (SELECT COUNT(*) FROM solution_set_failure) - :keep_limit
        ELSE 0
    END
)
