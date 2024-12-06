INSERT INTO solution_set_failure (
    solution_set_addr, attempt_block_num, attempt_block_addr, attempt_solution_set_ix, err_msg
)
VALUES (
    :solution_set_addr, :attempt_block_num, :attempt_block_addr, :attempt_solution_set_ix, :err_msg
)
