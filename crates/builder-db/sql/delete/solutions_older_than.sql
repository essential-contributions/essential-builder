-- Delete all submissions older than the given timestamp.
DELETE FROM submission
WHERE (timestamp_secs < :secs)
   OR (timestamp_secs = :secs AND timestamp_nanos < :nanos);

-- Delete all solutions that no longer have any associated submissions.
DELETE FROM solution
WHERE NOT EXISTS (
    SELECT 1 FROM submission
    WHERE submission.solution_id = solution.id
);
