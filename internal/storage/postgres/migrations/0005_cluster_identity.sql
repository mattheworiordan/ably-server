-- Cluster identity (DESIGN.md §11).
--
-- One row per schema, recording what every node of the cluster that
-- runs on this schema must agree on: the bus (§7.2), the retention
-- settings and the persisted namespaces (§6.3), and a random deployment
-- id that scopes the nats bus's subjects and envelopes so two clusters
-- sharing a NATS cluster never hear each other. The migration only
-- creates the table: the first node to open the schema writes the row
-- (Open, under the shard-identity advisory lock), and every later open
-- compares and refuses a mismatch. In a shard list (§6.4) every shard's
-- schema has its own row, all with shard 0's deployment id.
--
-- To change a recorded setting deliberately: stop every node, UPDATE the
-- row (in every shard's schema), then start the nodes with the new
-- setting. The refusal prints the statement.
CREATE TABLE cluster_identity (
  one                  BOOLEAN     PRIMARY KEY DEFAULT TRUE CHECK (one),
  deployment_id        TEXT        NOT NULL,
  bus                  TEXT        NOT NULL,
  message_retention    INTERVAL    NOT NULL,
  persisted_retention  INTERVAL    NOT NULL,
  persisted_namespaces TEXT[]      NOT NULL,
  min_server_version   TEXT        NOT NULL,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
