CREATE TABLE IF NOT EXISTS submission (
    id INTEGER PRIMARY KEY,
    solution_id INTEGER NOT NULL,
    timestamp_secs INTEGER NOT NULL,
    timestamp_nanos INTEGER NOT NULL,
    FOREIGN KEY (solution_id) REFERENCES solution(id) ON DELETE CASCADE
)
