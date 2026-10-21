-- magic_events: the outbox, in its own schema. Version 1.
--
-- `emit` writes an event here in the writing transaction, as one more
-- statement: it is published if and only if that transaction commits. The
-- relay — one at a time across every process, on an advisory lock — reads
-- what committed in id order, publishes it to JetStream, and deletes it once
-- the stream acknowledged it.

CREATE SCHEMA magic_events;

-- The version of this schema: the relay refuses to start against another.
-- `migration` appends the statement that writes it here; a later version is
-- a step of its own, run after this one.
CREATE TABLE magic_events.version (
    id      INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    version INTEGER NOT NULL
);

-- What committed and is not published yet: a row a message, deleted once the
-- stream holds it.
CREATE TABLE magic_events.outbox (
    id         BIGSERIAL   PRIMARY KEY,
    -- The CloudEvents id (`ce-id`), and the broker's deduplication id
    -- (`Nats-Msg-Id`): a message sent again is the same message.
    event_id   UUID        NOT NULL UNIQUE,
    -- The subject after the settings' prefix, as the event rendered it.
    subject    TEXT        NOT NULL,
    -- The CloudEvents type, with its version: `order.shipped.v1`.
    type       TEXT        NOT NULL,
    -- The headers the event rendered (`ce-type`, `ce-subject`), as sent.
    headers    JSONB       NOT NULL,
    body       BYTEA       NOT NULL,
    -- `ce-time`: when it was emitted, in the writing transaction.
    emitted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    -- Times the broker refused it; set aside (parked) after three.
    attempts   INTEGER     NOT NULL DEFAULT 0,
    last_error TEXT,
    parked_at  TIMESTAMPTZ
);

-- The relay's read: what is pending, in order, however many parked rows
-- lead it.
CREATE INDEX outbox_pending ON magic_events.outbox (id) WHERE parked_at IS NULL;

-- The status's: what was set aside, however many rows are pending.
CREATE INDEX outbox_parked ON magic_events.outbox (id) WHERE parked_at IS NOT NULL;

-- A queue: rows are inserted and deleted all day, and the relay reads its
-- head every pass. Vacuumed every thousand dead rows, whatever the table's
-- size, and without the default's pauses, so the head it reads stays short.
ALTER TABLE magic_events.outbox SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_vacuum_cost_delay = 0
);

COMMENT ON TABLE magic_events.outbox IS
    'Events committed and not published yet, in emit order. A parked row is one the broker '
    'refused three times: it waits for `Events::unpark`, and holds back nothing else.';

-- A truncate would take events away that committed: they would never be
-- published, and nothing would say so.
CREATE FUNCTION magic_events.refuse_truncate() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'magic_events.outbox holds committed events: delete the rows you mean to drop';
END;
$$;

CREATE TRIGGER refuse_truncate BEFORE TRUNCATE ON magic_events.outbox
    FOR EACH STATEMENT EXECUTE FUNCTION magic_events.refuse_truncate();

-- One row: how the relay is doing, for the status, and the largest body
-- `emit` takes, which the relay writes as it starts.
CREATE TABLE magic_events.relay (
    id                INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    max_body          INTEGER,
    last_published_at TIMESTAMPTZ,
    failures          INTEGER NOT NULL DEFAULT 0,
    last_error        TEXT,
    retry_after       TIMESTAMPTZ
);

INSERT INTO magic_events.relay (id) VALUES (1);

-- Messages published, per day (UTC) and type: the outbox forgets what it
-- published, and these count it.
CREATE TABLE magic_events.published (
    day   DATE   NOT NULL,
    type  TEXT   NOT NULL,
    count BIGINT NOT NULL,
    PRIMARY KEY (day, type)
);
