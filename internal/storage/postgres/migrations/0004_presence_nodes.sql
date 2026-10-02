-- Per-node presence liveness (DESIGN.md §12.5).
--
-- presence_nodes holds one liveness lease per node process. A live node
-- refreshes its own row on the bump cadence (one row, not one per
-- member), member rows carry an 'infinity' expires_at, and the reaper
-- deletes the members of every node that has no unexpired row here,
-- then the node's row. (When this migration was written a member lease
-- mode, since retired, could still bump expires_at on every member row
-- instead; DESIGN.md §9 "Removed settings".)
CREATE TABLE presence_nodes (
  node_id    TEXT        PRIMARY KEY,
  expires_at TIMESTAMPTZ NOT NULL
);

-- The reaper finds the node ids that own members (a skip scan over this
-- index) and deletes a dead node's members by node id. Without it each
-- of those is a sequential scan of the whole presence table. Building it takes a
-- SHARE lock on presence for the build, which blocks presence writes for
-- about a second per million rows during the upgrade that applies this.
CREATE INDEX presence_node_idx ON presence (node_id);
