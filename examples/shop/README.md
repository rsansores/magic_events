# The shop

An online shop that places orders and ships them, and two systems that read
what it says happened: billing, which invoices each order once, and
shipping, which packs and ships. One PostgreSQL, one NATS server with
JetStream.

```sh
docker compose up -d          # PostgreSQL on 5434, NATS on 4223
cargo run --bin shop          # places an order a second; the relay runs inside
cargo run --bin billing       # in another terminal
cargo run --bin shipping      # and another
```

- `src/lib.rs` — the events: an enum, one variant per event, its bodies thin
  (a store and order ids).
- `src/bin/shop.rs` — the shop: each order written with its event in one
  transaction; every third step one `order.shipped` for all the store's
  orders; one step in five rolled back, its event never published. It runs
  the relay (`Events::start`), and serves `Events::status` and
  `Events::recent` at http://localhost:3000/events/status and
  `/events/recent`, as the dashboard package reads them.
- `src/bin/billing.rs` — a durable consumer of `shop_events.orders.*.placed`:
  it reads each order where the shop keeps it, and records each event it
  acted on, so an event delivered twice is billed once.
- `src/bin/shipping.rs` — a durable consumer of every order event.

Stop billing for a while and start it again: it takes up where it stopped.
Stop NATS (`docker compose stop nats`): the shop keeps taking orders, its
outbox grows, and once NATS is back every event goes out, once.

`DATABASE_URL` and `NATS_URL` point the programs elsewhere. The example
reaches the crate in this repository by path; a copy outside it leaves
`path` out of `Cargo.toml`.
