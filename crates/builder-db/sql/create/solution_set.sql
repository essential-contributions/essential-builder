CREATE TABLE IF NOT EXISTS solution_set (
    id INTEGER PRIMARY KEY,
    content_addr BLOB NOT NULL UNIQUE,
    solution_set BLOB NOT NULL
)
