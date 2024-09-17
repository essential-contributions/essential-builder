DELETE FROM solution_failure
WHERE id IN (
    SELECT id
    FROM solution_failure
    ORDER BY attempt_block_num ASC, attempt_solution_ix ASC
    LIMIT (SELECT COUNT(*) FROM solution_failure) - :keep_limit
)
