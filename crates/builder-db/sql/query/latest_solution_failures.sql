SELECT
    solution_addr, attempt_block_num, attempt_solution_ix, err_msg
FROM
    solution_failure
WHERE
    solution_addr = :solution_addr
ORDER BY
    attempt_block_num DESC, attempt_solution_ix DESC
LIMIT :limit
