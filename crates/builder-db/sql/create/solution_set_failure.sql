CREATE TABLE IF NOT EXISTS solution_set_failure (
    id INTEGER PRIMARY KEY,
    solution_set_addr BLOB NOT NULL,
    attempt_block_num INTEGER NOT NULL,
    attempt_block_addr BLOB NOT NULL,
    attempt_solution_set_ix INTEGER NOT NULL,
    err_msg BLOB NOT NULL
)
