CREATE TABLE IF NOT EXISTS solution_failure (
    id INTEGER PRIMARY KEY,
    solution_addr BLOB NOT NULL,
    attempt_block_num INTEGER NOT NULL,
    attempt_solution_ix INTEGER NOT NULL,
    err_msg BLOB NOT NULL
)
