-- As publish_pipelined.sql, with pg_notify inside the transaction. Shows what
-- the notify lock costs once the round trips are gone.
-- Needs -M prepared or extended. Variables: channels, payload_bytes.
\set cid random(1, :channels)
\set bid random(1, 9000000000000000000)
\startpipeline
BEGIN;
WITH adv AS (
  SELECT 'ch-' || :cid::text AS name, advance_channel_serial('ch-' || :cid::text, 'pgbench') AS cs
), ins AS (
  INSERT INTO channel_messages (channel, channel_serial, idx, id, kind, payload, message_serial)
  SELECT name, cs, 0, :bid::text || ':0', 'message', decode(repeat('61', :payload_bytes), 'hex'), cs || ':000' FROM adv
  RETURNING 1
), proj AS (
  INSERT INTO messages (channel, message_serial, payload, deleted)
  SELECT name, cs || ':000', decode(repeat('61', :payload_bytes), 'hex'), FALSE FROM adv
  RETURNING 1
)
SELECT cs, pg_notify('ably_channel', json_build_object('channel', name, 'serial', cs)::text) FROM adv;
COMMIT;
\endpipeline
