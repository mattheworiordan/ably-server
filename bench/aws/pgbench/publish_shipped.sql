-- The shipped publish transaction (internal/storage/postgres/postgres.go,
-- channelStore.Store at main 90dfa15), one message per publish. Seven round
-- trips: BEGIN, idempotency lookup, advance_channel_serial, two INSERTs,
-- pg_notify, COMMIT. Needs -M prepared (pgbench turns :name into a parameter).
-- Variables: channels, payload_bytes.
--
-- Differences from the Go code: the batch id is a random 63-bit integer and
-- the channel is picked uniformly from 'ch-1'..'ch-<channels>'; the payload is
-- payload_bytes of a constant byte where the server stores msgpack.
\set cid random(1, :channels)
\set bid random(1, 9000000000000000000)
BEGIN;
SELECT channel_serial FROM channel_messages WHERE channel = 'ch-' || :cid::text AND id = ANY(ARRAY[:bid::text || ':0']) LIMIT 1;
SELECT advance_channel_serial('ch-' || :cid::text, 'pgbench') AS cs \gset
INSERT INTO channel_messages (channel, channel_serial, idx, id, kind, payload, message_serial)
 VALUES ('ch-' || :cid::text, :cs::text, 0, :bid::text || ':0', 'message', decode(repeat('61', :payload_bytes), 'hex'), :cs::text || ':000');
INSERT INTO messages (channel, message_serial, payload, deleted)
 VALUES ('ch-' || :cid::text, :cs::text || ':000', decode(repeat('61', :payload_bytes), 'hex'), FALSE);
SELECT pg_notify('ably_channel', json_build_object('channel', 'ch-' || :cid::text, 'serial', :cs::text)::text);
COMMIT;
