-- A batch of :batch publishes to :batch different channels in one
-- transaction and one round trip. The channels are a contiguous window
-- 'ch-<base>'..'ch-<base+batch-1>' and the rows are locked in name order
-- (advance_channel_serial is called over a sorted subquery), so concurrent
-- batches cannot deadlock, as in the write path's sorted lock order. No
-- pg_notify. Needs -M prepared or extended.
-- Variables: channels, payload_bytes, batch (run with -D batch=10, 30 or 100).
\set base random(1, :channels - :batch + 1)
\set bid random(1, 9000000000000000000)
\startpipeline
BEGIN;
WITH chs AS MATERIALIZED (
  SELECT 'ch-' || g::text AS name, g AS n FROM generate_series(:base::int, :base::int + :batch::int - 1) AS g
), adv AS MATERIALIZED (
  SELECT c.name, c.n, advance_channel_serial(c.name, 'pgbench') AS cs FROM (SELECT name, n FROM chs ORDER BY name) c
), ins AS (
  INSERT INTO channel_messages (channel, channel_serial, idx, id, kind, payload, message_serial)
  SELECT name, cs, 0, :bid::text || ':' || n::text, 'message', decode(repeat('61', :payload_bytes), 'hex'), cs || ':000' FROM adv
  RETURNING 1
), proj AS (
  INSERT INTO messages (channel, message_serial, payload, deleted)
  SELECT name, cs || ':000', decode(repeat('61', :payload_bytes), 'hex'), FALSE FROM adv
  RETURNING 1
)
SELECT count(*) FROM adv;
COMMIT;
\endpipeline
