SELECT
    solution_addr, attempt_block_num, attempt_block_addr, attempt_solution_ix, err_msg
FROM
    solution_failure
ORDER BY
    attempt_block_num DESC, attempt_solution_ix DESC
LIMIT :offset, :limit
