SELECT
    solution_set_addr, attempt_block_num, attempt_block_addr, attempt_solution_set_ix, err_msg
FROM
    solution_set_failure
ORDER BY
    attempt_block_num DESC, attempt_solution_set_ix DESC
LIMIT :offset, :limit
