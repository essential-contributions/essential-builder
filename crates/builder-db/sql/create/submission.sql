CREATE TABLE IF NOT EXISTS submission (
    id INTEGER PRIMARY KEY,
    solution_set_id INTEGER NOT NULL,
    timestamp_secs INTEGER NOT NULL,
    timestamp_nanos INTEGER NOT NULL,
    FOREIGN KEY (solution_set_id) REFERENCES solution_set(id) ON DELETE CASCADE
)
