SELECT
    solution_set_addr, attempt_block_num, attempt_block_addr, attempt_solution_set_ix, err_msg
FROM
    solution_set_failure
WHERE
    solution_set_addr = :solution_set_addr
ORDER BY
    attempt_block_num DESC, attempt_solution_set_ix DESC
LIMIT :limit
