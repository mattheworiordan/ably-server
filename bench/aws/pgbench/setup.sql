-- Run 0a setup (after schema.sql): the commit-latency probe table and the
-- pre-seeded channels table. psql variable: channels (how many channels).
--
--   psql -v channels=200000 -f setup.sql

CREATE TABLE IF NOT EXISTS commit_probe (
  id      bigserial PRIMARY KEY,
  payload text NOT NULL
);

-- One row per channel, seeded the way advance_channel_serial would seed a
-- new channel, so a publish is an UPDATE of an existing row (the common case
-- on a busy channel) and not an INSERT.
INSERT INTO channels (name, channel_serial, initial_channel_serial)
SELECT 'ch-' || g::text, seed, seed
FROM generate_series(1, :channels) AS g,
     LATERAL (SELECT format_channel_serial((extract(epoch FROM clock_timestamp()) * 1000)::bigint, 0, 'pgbench') AS seed) s
ON CONFLICT (name) DO NOTHING;

ANALYZE;
