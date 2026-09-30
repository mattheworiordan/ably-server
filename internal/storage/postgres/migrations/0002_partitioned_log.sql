-- Time-partitioned message log with per-channel retention classes
-- (DESIGN.md §6.3 "Retention").
--
-- channel_messages (the log) and messages (the latest-version
-- projection) are the two tables that grow with every publish. Both
-- become two-level partitioned tables:
--
--   level 1: LIST (persisted). FALSE is the "live" class: channels
--            whose namespace is not persisted keep only the continuity
--            window (default 2 minutes). TRUE is the "persisted" class:
--            channels in a persisted namespace keep the configured
--            history retention (default 24 hours).
--   level 2: RANGE on the serial (channel_serial for the log,
--            message_serial for the projection). A serial starts with
--            its 14-digit zero-padded mint time in ms (§8), so a range
--            of serials is a range of time.
--
-- The leaf partitions are created ahead of time and dropped whole by
-- the retention sweep in retention.go, never DELETEd row by row. This
-- migration creates only the structure; Open creates the first leaves
-- before it returns.
--
-- A partitioned table cannot carry a UNIQUE index that omits the
-- partition key, so the old UNIQUE (channel, id) idempotency index
-- becomes a plain index. Uniqueness of a client id per channel is
-- enforced by the channels-row lock: every publish locks its channel's
-- row before it checks the id (§6.3).
--
-- Existing rows are kept: a non-empty old table is attached as one leaf
-- of the live class covering everything up to a minute past now, and
-- ages out on the continuity window like any other leaf. An empty old
-- table (a fresh install) is dropped. Attaching scans the old table and
-- builds the new primary key on it, so the migration takes time in
-- proportion to the existing log.
--
-- Because the migration cannot know which old rows belong to persisted
-- namespaces, all of them land in the live class. retention_state
-- records the bound of that legacy leaf, so a resume on a persisted
-- channel from before it is refused rather than wrongly claimed
-- continuous once the leaf has aged out (DESIGN.md §4.3).
--
-- The migration locks both tables up front, in one order, so a node
-- still on the old version cannot deadlock against it half way.

LOCK TABLE channel_messages, messages IN ACCESS EXCLUSIVE MODE;

CREATE TABLE retention_state (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

ALTER TABLE channel_messages RENAME TO channel_messages_legacy;
ALTER INDEX channel_messages_pkey RENAME TO channel_messages_legacy_pkey;
ALTER INDEX channel_messages_idempotency_idx RENAME TO channel_messages_legacy_idempotency_idx;
ALTER INDEX channel_messages_versions_idx RENAME TO channel_messages_legacy_versions_idx;
ALTER TABLE channel_messages_legacy ADD COLUMN persisted BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE channel_messages (
  channel        TEXT    NOT NULL,
  channel_serial TEXT    NOT NULL,
  idx            INT     NOT NULL,
  id             TEXT,
  kind           TEXT    NOT NULL DEFAULT 'message',
  payload        BYTEA   NOT NULL,
  message_serial TEXT,
  is_append      BOOLEAN NOT NULL DEFAULT FALSE,
  summary        BYTEA,
  persisted      BOOLEAN NOT NULL DEFAULT FALSE,
  PRIMARY KEY (channel, channel_serial, idx, persisted)
) PARTITION BY LIST (persisted);

CREATE TABLE channel_messages_live PARTITION OF channel_messages
  FOR VALUES IN (FALSE) PARTITION BY RANGE (channel_serial);
CREATE TABLE channel_messages_persisted PARTITION OF channel_messages
  FOR VALUES IN (TRUE) PARTITION BY RANGE (channel_serial);

CREATE INDEX channel_messages_id_idx
  ON channel_messages (channel, id)
  WHERE id IS NOT NULL;

CREATE INDEX channel_messages_versions_idx
  ON channel_messages (channel, message_serial, channel_serial, idx)
  WHERE message_serial IS NOT NULL;

ALTER TABLE messages RENAME TO messages_legacy;
ALTER INDEX messages_pkey RENAME TO messages_legacy_pkey;
ALTER TABLE messages_legacy ADD COLUMN persisted BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE messages (
  channel        TEXT    NOT NULL,
  message_serial TEXT    NOT NULL,
  payload        BYTEA   NOT NULL,
  deleted        BOOLEAN NOT NULL DEFAULT FALSE,
  persisted      BOOLEAN NOT NULL DEFAULT FALSE,
  PRIMARY KEY (channel, message_serial, persisted)
) PARTITION BY LIST (persisted);

CREATE TABLE messages_live PARTITION OF messages
  FOR VALUES IN (FALSE) PARTITION BY RANGE (message_serial);
CREATE TABLE messages_persisted PARTITION OF messages
  FOR VALUES IN (TRUE) PARTITION BY RANGE (message_serial);

DO $$
DECLARE
  bound TEXT := lpad(((extract(epoch from clock_timestamp()) * 1000)::BIGINT + 60000)::TEXT, 14, '0');
BEGIN
  -- A partition's primary key must be the parent's, so the old key is
  -- dropped and the attach builds the (…, persisted) one.
  IF EXISTS (SELECT 1 FROM channel_messages_legacy) THEN
    ALTER TABLE channel_messages_legacy DROP CONSTRAINT channel_messages_legacy_pkey;
    EXECUTE format(
      'ALTER TABLE channel_messages_live ATTACH PARTITION channel_messages_legacy FOR VALUES FROM (MINVALUE) TO (%L)',
      bound);
    DROP INDEX channel_messages_legacy_idempotency_idx;
    INSERT INTO retention_state (key, value) VALUES ('legacy_live_bound', bound);
  ELSE
    DROP TABLE channel_messages_legacy;
  END IF;

  IF EXISTS (SELECT 1 FROM messages_legacy) THEN
    ALTER TABLE messages_legacy DROP CONSTRAINT messages_legacy_pkey;
    EXECUTE format(
      'ALTER TABLE messages_live ATTACH PARTITION messages_legacy FOR VALUES FROM (MINVALUE) TO (%L)',
      bound);
  ELSE
    DROP TABLE messages_legacy;
  END IF;
END
$$;
