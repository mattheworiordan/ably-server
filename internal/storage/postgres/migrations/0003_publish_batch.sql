-- Batched publish: the first half of a batch transaction (DESIGN.md
-- §6.3 "Publish batching").
--
-- publish_batch_lock locks the channels rows of every channel in a batch
-- of publishes (cms) in sorted name order, then mints one channelSerial
-- per cm in batch order, all in one round trip. The caller then inserts
-- the rows with the serials it was given and commits.
--
-- A channel whose row is locked by another transaction (a hot channel
-- being written from another node) is skipped rather than waited for
-- (FOR UPDATE SKIP LOCKED), and its cms come back 'deferred' so the
-- caller retries them in its next batch and cold channels' publishes are
-- not held up. A cm flagged in p_wait (already deferred too often) waits
-- for the lock instead, so a hot channel cannot be starved. The waited-
-- for rows are locked first, in one statement in sorted name order, and
-- the skipped-if-busy rows after, in a statement that never waits, so two
-- batches cannot deadlock on channels rows.
--
-- Idempotency: a cm with client-supplied ids (listed in p_ids, with the
-- cm's 1-based position in p_id_cm) is looked up after its channel is
-- locked, at or after its retention floor (p_floors); every stored row of
-- a locked channel sorts below the serials minted here. A hit comes back
-- 'duplicate' with the original serial and mints nothing. Duplicates between cms of the same batch are the
-- caller's to detect, since this batch's rows are not inserted yet.
--
-- Returns one row per cm, in batch order: status is 'ok' (serial is the
-- minted channelSerial and prev the one it follows on its channel,
-- counting earlier cms of the batch, which is the predecessor a chaining
-- bus announces), 'deferred', or 'duplicate' (dup_serial is the
-- original cm's channelSerial).

CREATE FUNCTION publish_batch_lock(
  p_series   TEXT,
  p_channels TEXT[],
  p_wait     BOOLEAN[],
  p_floors   TEXT[],
  p_id_cm    INT[],
  p_ids      TEXT[]
) RETURNS TABLE (ord INT, status TEXT, serial TEXT, prev TEXT, dup_serial TEXT)
LANGUAGE plpgsql AS $$
DECLARE
  v_wait_names TEXT[];
  v_skip_names TEXT[];
  v_names      TEXT[];  -- locked channels
  v_cur        TEXT[];  -- their serial, advanced as cms are minted
  v_start      TEXT[];  -- their serial when locked
  v_missing    TEXT[];
  v_dup_cm     INT[];
  v_dup_serial TEXT[];
  v_pos        INT;
  v_dpos       INT;
  v_serial     TEXT;
  v_seed       TEXT;
  v_hi         TEXT;
  i            INT;
BEGIN
  SELECT coalesce(array_agg(c) FILTER (WHERE w), ARRAY[]::TEXT[]),
         coalesce(array_agg(c) FILTER (WHERE NOT w), ARRAY[]::TEXT[])
    INTO v_wait_names, v_skip_names
    FROM (SELECT c, bool_or(w) AS w FROM unnest(p_channels, p_wait) AS t(c, w) GROUP BY c) g;

  -- Waited-for rows first (sorted), then the rest without waiting.
  SELECT coalesce(array_agg(name ORDER BY name), ARRAY[]::TEXT[]),
         coalesce(array_agg(channel_serial ORDER BY name), ARRAY[]::TEXT[])
    INTO v_names, v_cur
    FROM (SELECT name, channel_serial FROM channels
          WHERE name = ANY(v_wait_names) ORDER BY name FOR UPDATE) l;
  SELECT v_names || coalesce(array_agg(name ORDER BY name), ARRAY[]::TEXT[]),
         v_cur || coalesce(array_agg(channel_serial ORDER BY name), ARRAY[]::TEXT[])
    INTO v_names, v_cur
    FROM (SELECT name, channel_serial FROM channels
          WHERE name = ANY(v_skip_names) ORDER BY name FOR UPDATE SKIP LOCKED) l;

  -- Channels with no row yet (Storage.Channel or the batching path
  -- normally creates the row before a publish is queued, so this is a
  -- fallback) are created as ensure_channel would and then locked
  -- without waiting, in sorted order: a row another transaction is
  -- creating or holding defers its cms like any busy channel, so this
  -- step never waits while holding locks. ON CONFLICT DO NOTHING does not
  -- wait on a committed row; on an uncommitted concurrent insert it
  -- would, which is why the fallback is only reached for names no bind
  -- or earlier publish has created.
  SELECT array_agg(c ORDER BY c) INTO v_missing
    FROM unnest(v_wait_names || v_skip_names) AS c
    WHERE NOT c = ANY(v_names) AND NOT EXISTS (SELECT 1 FROM channels WHERE name = c);
  IF v_missing IS NOT NULL THEN
    v_seed := format_channel_serial((extract(epoch from clock_timestamp()) * 1000)::BIGINT, 0, p_series);
    INSERT INTO channels (name, channel_serial, initial_channel_serial)
      SELECT c, v_seed, v_seed FROM unnest(v_missing) AS c
      ORDER BY c
      ON CONFLICT (name) DO NOTHING;
    SELECT v_names || coalesce(array_agg(name ORDER BY name), ARRAY[]::TEXT[]),
           v_cur || coalesce(array_agg(channel_serial ORDER BY name), ARRAY[]::TEXT[])
      INTO v_names, v_cur
      FROM (SELECT name, channel_serial FROM channels
            WHERE name = ANY(v_missing) ORDER BY name FOR UPDATE SKIP LOCKED) l;
  END IF;
  v_start := v_cur;
  -- Every stored row of a locked channel sorts at or below its current
  -- serial, so the largest of those bounds the id lookup from above and
  -- lets run-time pruning skip the leaves created ahead of now.
  SELECT max(x) INTO v_hi FROM unnest(v_start) AS x;

  -- One pass over every client id of a locked channel.
  SELECT array_agg(x.cm), array_agg(x.cs) INTO v_dup_cm, v_dup_serial
    FROM (
      SELECT DISTINCT ON (t.cm) t.cm, m.channel_serial AS cs
      FROM unnest(p_id_cm, p_ids) AS t(cm, id)
      JOIN channel_messages m
        ON m.channel = p_channels[t.cm] AND m.id = t.id
       AND m.channel_serial >= p_floors[t.cm] AND m.channel_serial <= v_hi
      WHERE p_channels[t.cm] = ANY(v_names)
      ORDER BY t.cm, m.channel_serial
    ) x;

  FOR i IN 1 .. coalesce(array_length(p_channels, 1), 0) LOOP
    ord := i; serial := NULL; prev := NULL; dup_serial := NULL;
    v_pos := array_position(v_names, p_channels[i]);
    IF v_pos IS NULL THEN
      status := 'deferred';
      RETURN NEXT;
      CONTINUE;
    END IF;
    v_dpos := array_position(v_dup_cm, i);
    IF v_dpos IS NOT NULL THEN
      status := 'duplicate'; dup_serial := v_dup_serial[v_dpos];
      RETURN NEXT;
      CONTINUE;
    END IF;
    v_serial := next_channel_serial(v_cur[v_pos], p_series);
    status := 'ok'; serial := v_serial; prev := v_cur[v_pos];
    v_cur[v_pos] := v_serial;
    RETURN NEXT;
  END LOOP;

  UPDATE channels c SET channel_serial = u.cur
    FROM unnest(v_names, v_cur, v_start) AS u(name, cur, start)
    WHERE c.name = u.name AND u.cur <> u.start;
END;
$$;
