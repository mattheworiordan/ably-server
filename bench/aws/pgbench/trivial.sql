-- Latency floor: one autocommit INSERT is one round trip and one WAL flush.
-- Variables: none. Run with -M prepared.
INSERT INTO commit_probe (payload) VALUES (repeat('x', 200));
