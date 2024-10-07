INSERT INTO solution_failure (
    solution_addr, attempt_block_num, attempt_block_addr, attempt_solution_ix, err_msg
)
VALUES (
    :solution_addr, :attempt_block_num, :attempt_block_addr, :attempt_solution_ix, :err_msg
)
