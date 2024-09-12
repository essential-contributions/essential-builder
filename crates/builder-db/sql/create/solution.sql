CREATE TABLE IF NOT EXISTS solution (
    id INTEGER PRIMARY KEY,
    content_addr BLOB NOT NULL UNIQUE,
    solution BLOB NOT NULL
)
