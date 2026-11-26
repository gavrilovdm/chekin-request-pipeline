-- Operator replay (T11): attempt stays monotonic because it is the fencing token; the retry budget counts
-- attempt - attempt_offset, and a replay sets attempt_offset = attempt.
ALTER TABLE requests ADD COLUMN attempt_offset integer NOT NULL DEFAULT 0;
