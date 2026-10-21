-- The shop's own tables. The outbox is magic_events', in its own schema.
CREATE SCHEMA IF NOT EXISTS shop;

CREATE TABLE IF NOT EXISTS shop.orders (
    id         BIGSERIAL   PRIMARY KEY,
    store      TEXT        NOT NULL,
    total      BIGINT      NOT NULL,
    placed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    shipped_at TIMESTAMPTZ
);

-- Billing's: one invoice per order, and the events it acted on, so an event
-- delivered twice is billed once.
CREATE TABLE IF NOT EXISTS shop.invoices (
    order_id   BIGINT      PRIMARY KEY REFERENCES shop.orders (id),
    total      BIGINT      NOT NULL,
    issued_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shop.billed_events (
    event_id   TEXT        PRIMARY KEY
);
