-- Per-node presence liveness (DESIGN.md §12.5, --presence-lease-mode).
--
-- presence_nodes holds one liveness lease per node process. In the
-- default node lease mode a live node refreshes its own row on the bump
-- cadence (one row, not one per member), member rows carry an
-- 'infinity' expires_at, and the reaper deletes the members of every
-- node that has no unexpired row here, then the node's row. The member
-- lease mode ignores this table for its own rows (it bumps expires_at
-- on every member row, as before) and reads it only to reap rows a
-- node-mode node left behind.
CREATE TABLE presence_nodes (
  node_id    TEXT        PRIMARY KEY,
  expires_at TIMESTAMPTZ NOT NULL
);

-- The reaper finds the node ids that own members (a skip scan over this
-- index) and deletes a dead node's members by node id; the member-mode
-- lease bump selects a node's rows by it too. Without it each of those
-- is a sequential scan of the whole presence table. Building it takes a
-- SHARE lock on presence for the build, which blocks presence writes for
-- about a second per million rows during the upgrade that applies this.
CREATE INDEX presence_node_idx ON presence (node_id);
