-- magic_events: several streams on one outbox. Version 2.
--
-- An event names the stream it goes to (`#[event(stream = "live")]`): a
-- JetStream stream of its own, with its own subject prefix and retention,
-- declared in the relay's settings by that key. The main stream — where an
-- event that names none goes — is the empty key, so what version 1 wrote is
-- the main stream's: its rows, its relay row, its counts. The relay keeps
-- each stream apart: its own passes, backoff, failures and counts.

-- The stream an event goes to: its key, empty for the main one. A writer of
-- a build before 0.2, which names no stream, writes to the main one.
ALTER TABLE magic_events.outbox ADD COLUMN stream TEXT NOT NULL DEFAULT '';

-- The relay's read is one stream's pending rows, in order; the status's,
-- one stream's parked rows.
DROP INDEX magic_events.outbox_pending;
CREATE INDEX outbox_pending ON magic_events.outbox (stream, id) WHERE parked_at IS NULL;
DROP INDEX magic_events.outbox_parked;
CREATE INDEX outbox_parked ON magic_events.outbox (stream, id) WHERE parked_at IS NOT NULL;

-- A row per stream: how its relay is doing, and the largest body `emit`
-- takes for it, which `Events::start` writes for every stream it declares —
-- `emit` refuses an event of a stream with no row. Version 1's row is the
-- main stream's.
ALTER TABLE magic_events.relay ADD COLUMN stream TEXT NOT NULL DEFAULT '';
ALTER TABLE magic_events.relay ALTER COLUMN stream DROP DEFAULT;
ALTER TABLE magic_events.relay DROP CONSTRAINT relay_pkey;
ALTER TABLE magic_events.relay ADD PRIMARY KEY (stream);

-- `id` stays 1 on the main stream's row, and is NULL on every other: a
-- writer of a build before 0.2, still running while this step runs, reads
-- the main stream's `max_body` by it.
ALTER TABLE magic_events.relay ALTER COLUMN id DROP NOT NULL, ALTER COLUMN id DROP DEFAULT;
ALTER TABLE magic_events.relay ADD UNIQUE (id);

-- Messages published per stream, day (UTC) and type.
ALTER TABLE magic_events.published ADD COLUMN stream TEXT NOT NULL DEFAULT '';
ALTER TABLE magic_events.published ALTER COLUMN stream DROP DEFAULT;
ALTER TABLE magic_events.published DROP CONSTRAINT published_pkey;
ALTER TABLE magic_events.published ADD PRIMARY KEY (stream, day, type);
