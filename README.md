# magic_events

Events that leave PostgreSQL only if the transaction that raised them
committed — and always leave if it did.

```rust
use magic_events::{emit, Event, Events, Settings};

// An event is a serde type that derives Event: its type, its version, and
// the subject it goes to, each {field} checked at compile time.
#[derive(serde::Serialize, Event)]
#[event(
    type = "order.shipped",
    version = 1,
    subject = "orders.{store}.shipped"
)]
pub struct OrderShipped {
    pub store: StoreId,
    pub ids: Vec<OrderId>,
}

// Once, in main: checks the schema and the stream, and runs the relay.
let settings = Settings::new("SHOP_EVENTS", "//shop.example.com");
let events = Events::start(pool.clone(), jetstream, settings).await?;

// Wherever you write: one more statement of the transaction.
let mut tx = pool.begin().await?;
sqlx::query("UPDATE orders SET shipped_at = now() WHERE id = ANY($1)")
    .bind(&ids)
    .execute(&mut *tx)
    .await?;
emit(&mut tx, &OrderShipped { store, ids }).await?;
tx.commit().await?; // Committed: it is published. Rolled back: it never was.

// At shutdown.
events.stop().await;
```

The message lands on `shop_events.orders.<store>.shipped` in the JetStream
stream `SHOP_EVENTS`, as a CloudEvent: `ce-type: order.shipped.v1`, a
`ce-id` of its own, and `{"store":7,"ids":[41,42]}` as its body. Any system
that cares reads it with a durable consumer of its own.

You write `emit`. You do not write the outbox table, the relay, its lock,
its retries or its deduplication.

## Why

Your data is in PostgreSQL, and other systems need to know when it changes.
There are three ways to tell them:

| | What goes wrong |
|---|---|
| Publish inside the transaction | The transaction holds its locks while the broker answers, and a transaction that rolls back afterwards has published what never happened. |
| Publish after commit | A crash, a deploy or a broker away between the commit and the publish loses the event, and nothing says so. |
| **A transactional outbox** (this crate) | The event is a row written in the same transaction as the data: it exists if and only if the data does. A relay publishes what committed, and deletes it once the stream acknowledged it. |

The outbox is the one that cannot lie, and it is the most work to build
well: a table, a relay that runs in one process at a time and takes over
when that process dies, fencing so a relay that lost its lock cannot write,
ordered and pipelined publishing, deduplication of what is sent twice,
backoff, and a way to set aside what the broker refuses. This crate is
that, behind one function.

## What it guarantees, exactly

- **No event from a rolled-back transaction.** `emit` is an `INSERT` in
  your transaction; a rollback takes it away with your data.
- **Every committed event reaches the stream, at least once.** A row leaves
  the outbox only after the stream acknowledged its message. Each message
  carries its event's id as `Nats-Msg-Id`, and the stream drops a message
  it already holds within its duplicate window (2 minutes by default). A
  duplicate is possible only past that window: the relay died after the
  stream took a message and before it deleted the row, and no relay ran
  again for longer than the window.
- **The events of one transaction go out in the order they were emitted.**
  Events of *concurrent* transactions go out in the order of their rows'
  ids, which is not commit order: a transaction that commits later with a
  lower id is published late — but never lost, since rows are deleted, not
  passed by a cursor. With more than one message in flight (`in_flight`,
  64 by default), a message the broker loses in the middle of a pipeline,
  while one after it is stored, lands after it when it is sent again;
  `in_flight(1)` gives strict order at half the throughput.
- **When the broker is down, nothing is dropped.** The outbox grows, the
  relay backs off (500 ms doubling to a minute), and everything goes out
  once the broker is back. `status()` shows the outbox's depth and age, so
  you can alert on it.
- **A message the broker refuses for good holds back nothing.** Refused
  three times (too large for the stream, say), it is set aside — parked —
  with the broker's reason, and the rows behind it go on. `unpark` sends it
  again after a fix.

What it does **not** guarantee:

- **Exactly-once delivery** to a consumer. A consumer acknowledges what it
  handled; one that crashes before acknowledging gets the event again, with
  the same `ce-id`. Deduplicate by `ce-id` where acting twice would hurt.
- **Order across transactions.** Do not build a consumer on it. Thin events
  and a re-read make that safe: a consumer reads the current state, not the
  event's.
- **Delivery to a consumer that does not exist yet.** The stream keeps
  messages for its retention (7 days by default); a durable consumer created
  later starts where you tell it to.

## Where it fits, and where it does not

It fits a service that writes PostgreSQL and must tell other systems — in
any language — that something happened: an order shipped, a price changed,
a device went offline. You say what happened, in your own words; the events
are a contract of their own, not a copy of your tables.

- **Change data capture** (Debezium, logical replication) streams rows:
  every change of every column, in the database's words, with a connector to
  run beside it. This crate streams meaning — "these 40 orders shipped" — and
  runs inside your process.
- **Publishing directly** from your code is the dual write above.
- **A job queue** hands work to one worker. A stream is read by every system
  that subscribes, each at its own pace, and replayed within its retention.

It is not a stream processor and not an event store: the events are
notifications, and your tables stay the truth. PostgreSQL 14 or later, and
NATS with JetStream (tested with 2.15).

## Events

A struct with named fields is one event. An enum is one event per variant,
each with its own type, version and subject:

```rust
#[derive(serde::Serialize, serde::Deserialize, magic_events::Event)]
pub enum Catalog {
    #[event(
        type = "product.updated",
        version = 1,
        subject = "catalog.{store}.updated"
    )]
    ProductUpdated { store: StoreId, ids: Vec<i64> },
    #[event(
        type = "product.archived",
        version = 1,
        subject = "catalog.{store}.archived"
    )]
    ProductArchived { store: StoreId, ids: Vec<i64> },
    // A body that changes for its readers is a new version, a variant
    // of its own: `ce-type` says which, `product.archived.v2`.
    #[event(
        type = "product.archived",
        version = 2,
        subject = "catalog.{store}.archived"
    )]
    ProductArchivedV2 {
        store: StoreId,
        ids: Vec<i64>,
        reason: String,
    },
}
```

- **The subject** is your prefix (the stream's name in lowercase,
  `shop_events`), then the event's: tokens joined by dots, each text,
  `{field}`s or both. A field is any `Display` value. A `{field}` that is no
  field, a field that is not `Display`, a token with whitespace, `*` or `>`
  — each is a compile error; a value that renders as one is refused by
  `emit`, before anything is written.
- **Keep the body thin**: ids, not the rows' fields. A consumer reads the
  rest through your API, with its own credentials, so your API stays the one
  contract for data, and an event never carries what its reader may not see.
  One write of many rows is one event with many ids.
- **The body** is the event's JSON, as serde writes it — for an enum, the
  variant's own fields, since the variant is in `ce-type`. 64 KiB at most by
  default (`Settings::max_body`).
- `#[event(ce_subject = "{id}")]` sets the CloudEvents `subject` attribute.

`emit` does no network I/O: it is one `INSERT`, about 16 µs over the write
it goes with (below). Call it where you write, in the same transaction —
there is no other way to emit.

## Setting up

The crate keeps its tables in a schema of its own, `magic_events`, in your
database:

```rust
// A host with no migrations of its own: once, at start, before anything emits.
magic_events::Events::install(&pool).await?;
```

```rust
// A host with migrations of its own: one migration per step, each written once.
for step in 1..=magic_events::SCHEMA_VERSION {
    let sql = magic_events::migration(step).expect("a step of this build");
    std::fs::write(format!("migrations/magic_events_{step}.sql"), sql).unwrap();
}
```

`Events::start` refuses a schema of another version. A release that changes
the tables adds a step, says so in the changelog, and never edits one.

Grants are yours: a role that emits needs `USAGE` on the schema, `INSERT`
on `magic_events.outbox`, `USAGE` on `magic_events.outbox_id_seq` and
`SELECT` on `magic_events.relay`; the relay's role needs `SELECT`, `UPDATE`
and `DELETE` on the outbox, `SELECT` and `UPDATE` on `magic_events.relay`,
`SELECT`, `INSERT` and `UPDATE` on `magic_events.published`, and `SELECT` on
`magic_events.version`.

Every setting has a default and a setter:

```rust
Settings::new("SHOP_EVENTS", "//shop.example.com")
    .subject_prefix("shop.events") // the lowercase stream name by default
    .retention(Duration::from_secs(30 * 24 * 60 * 60)) // 7 days by default
    .replicas(3) // 1 by default
    .storage(Storage::File) // the default
```

| Setting | Default | |
|---|---|---|
| `retention` | 7 days | the stream's `max_age` |
| `duplicate_window` | 2 min | how long the stream drops a message it holds |
| `storage`, `replicas` | file, 1 | the stream's, set when it is created |
| `manage_stream` | `true` | `false`: the stream is the operator's; it must exist and take `<prefix>.>` |
| `idle` | 200 ms | how long the relay waits when the outbox is empty |
| `batch` | 256 | rows read per pass |
| `in_flight` | 64 | messages sent and not acknowledged yet |
| `send_timeout` | 10 s | how long a message may wait for its acknowledgement |
| `backoff` | 500 ms to 60 s | how long the relay waits after a failed pass, doubling |
| `standby` | 30 s | how often a process that is not the relay asks for its lock |
| `max_body` | 64 KiB | the largest body `emit` takes; held to the broker's `max_payload` at start |

The relay runs inside your process, in a task of its own: no service to
deploy. Every process that calls `Events::start` competes for one advisory
lock; one publishes, the others stand by, and one of them takes over within
`standby` when it stops or dies.

## The messages

Each message is a CloudEvent in the [NATS protocol binding]'s *binary
content mode*: every attribute a header, the body the event's JSON.

| Header | |
|---|---|
| `ce-specversion` | `1.0` |
| `ce-id` | the event's id, a UUIDv7, made by `emit` |
| `ce-source` | `Settings::new`'s second argument |
| `ce-type` | the type and its version: `order.shipped.v1` |
| `ce-time` | when it was emitted, RFC 3339: `2027-01-15T08:00:00.123456Z` |
| `ce-datacontenttype` | `application/json` |
| `ce-subject` | with `#[event(ce_subject = …)]` |
| `Nats-Msg-Id` | the `ce-id` again: what JetStream deduplicates by |

Values are percent-encoded as the binding says (a space is `%20`). The
binding's last release, 1.0.2, predates NATS headers and knows only the
structured mode; binary mode — and `ce-datacontenttype` as a header of its
own rather than `Content-Type` — is the binding as it stands since, and what
this follows. A structured-mode reader (a body with the attributes in it)
is not served.

[NATS protocol binding]: https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/nats-protocol-binding.md

## Consuming

Each consuming system has a durable consumer of its own, filtered to what
it needs; the stream tracks how far each has read.

With the `nats` CLI:

```sh
nats consumer add SHOP_EVENTS billing --pull --deliver all --ack explicit \
  --filter 'shop_events.orders.*.placed' --defaults
nats consumer next SHOP_EVENTS billing --count 10
```

In Rust, with the `consume` feature, the events arrive decoded into your
own type, acknowledged when your handler answers `Ok` and delivered again
after a backoff when it fails:

```rust
consume(
    &jetstream,
    "SHOP_EVENTS",
    "billing",
    "shop_events.catalog.>",
    |delivery: Delivery<Catalog>| async move {
        // delivery.id is the ce-id: the same however often it comes.
        if let Catalog::ProductUpdated { store, ids } = delivery.event {
            println!("invoice the products {ids:?} of store {store}");
        }
        Ok(())
    },
)
.await?;
```

In TypeScript, with [`@nats-io/jetstream`](https://www.npmjs.com/package/@nats-io/jetstream):

```ts
import { connect } from '@nats-io/transport-node'
import { AckPolicy, jetstream, jetstreamManager } from '@nats-io/jetstream'

const nc = await connect({ servers: 'localhost:4222' })
await (await jetstreamManager(nc)).consumers.add('SHOP_EVENTS', {
  durable_name: 'billing',
  filter_subject: 'shop_events.orders.*.placed',
  ack_policy: AckPolicy.Explicit,
})
const consumer = await jetstream(nc).consumers.get('SHOP_EVENTS', 'billing')
for await (const m of await consumer.consume()) {
  const id = m.headers?.get('ce-id') // deduplicate by it
  const type = m.headers?.get('ce-type') // 'order.placed.v1'
  const event = m.json<{ store: number; ids: number[] }>()
  // … read what you need through the API, act, then:
  m.ack()
}
```

Any language with a JetStream client does the same: read `ce-type` to know
the event, `ce-id` to deduplicate, and the body as JSON.

## Monitoring

`Events::status` answers what waits in the outbox and since when, what was
set aside and why, whether a relay runs, what the stream holds per subject,
how far behind each consumer is, and what was published per day and type.
`Events::recent` answers the last messages, headers and bodies:

```rust
// Behind your own authorization: the operator's view, or the subjects
// this caller may see.
let status = events.status(&Filter::all()).await?;
let one_store = events
    .status(&Filter::subject("shop_events.orders.7.>"))
    .await?;
let last_ten = events.recent(&Filter::all(), 10).await?;
```

Every read takes a subject filter, so a view can be narrowed to what one
caller may see. A consumer's lag is the whole stream's, and the published
counts are kept per type: both are answered only for `Filter::all()` — show
them on an operator's view. A broker that does not answer leaves its part
out and says why; the status is what you look at when things fail.

The answers serialize as JSON for
[`magic_events_dashboard`](dashboard/README.md), a Vue 3 package: a
ready-made `<EventsDashboard>`, or a headless core for your own design
system. It says in one line whether events are flowing, lagging, stuck or
down, and why.

```rust
// After fixing what the broker refused: send it again with the next pass.
events.unpark(id).await?;
events.unpark_all().await?;
```

## Recipes

**Multi-tenant subjects.** Put the tenant first in the subject, and every
filter can scope by it — a consumer of one tenant, a status of one tenant:

```rust
#[derive(serde::Serialize, magic_events::Event)]
#[event(
    type = "order.placed",
    version = 1,
    subject = "{tenant}.orders.{store}.placed"
)]
pub struct OrderPlaced {
    pub tenant: TenantId,
    pub store: StoreId,
    pub ids: Vec<OrderId>,
}
```

A consumer of every tenant filters `shop_events.*.orders.>`; one tenant's
view is `Filter::subject("shop_events.acme.>")`; NATS permissions can give
each consuming system only the subjects it may read.

**An event with no write.** Record the fact first. "A store went offline"
is not a moment anyone writes; make it one — a periodic update — and emit
in its transaction:

```rust
// Every minute: the fact first, then its event, in one transaction.
let mut tx = pool.begin().await?;
let ids: Vec<i64> = sqlx::query_scalar(
    "UPDATE stores SET offline_at = now() \
      WHERE offline_at IS NULL AND last_seen_at < now() - interval '5 minutes' \
      RETURNING id",
)
.fetch_all(&mut *tx)
.await?;
if !ids.is_empty() {
    magic_events::emit(&mut tx, &StoresOffline { ids }).await?;
}
tx.commit().await?;
```

**Under a pooler.** `emit` is a statement of your transaction: any pooler
mode serves it. The relay holds a session-level advisory lock on one
connection for as long as it runs, so the pool you give `Events::start`
must reach the database directly, or through a pooler in session mode. In
transaction mode the relay's transactions run where its lock is not: it
publishes nothing, and logs an error that names the pooler.

## Performance

On a 13th-gen Intel i9-13900 (32 threads), PostgreSQL 18 and NATS 2.15 on
the same machine, 2026-10-21:

- `emit` costs about **16 µs** over the write it goes with (a one-row
  `INSERT` alone: about 10 µs), apart from the commit, whose wait for the
  disk is the same either way and much longer.
- The relay publishes about **5,400 events a second** with one message in
  flight, **8,800** with 16, and **11,000** with 64 (the default), to a
  stream on file, in batches of 256.

`cargo test -p magic_events --release bench_ -- --ignored --nocapture --test-threads 1`
measures them on yours.

## Learn more

- **[How it works](HOW_IT_WORKS.md)** — the schema, the relay's loop, the
  lock and its fencing, and every failure and what happens.
- **[The shop example](examples/shop)** — a shop that writes, a relay, and
  two consumers, with `docker compose`.
- **[The dashboard](dashboard/README.md)** — `magic_events_dashboard`.

## FAQ

**Why poll the outbox rather than `LISTEN`/`NOTIFY`?** An indexed read of an
empty table every 200 ms costs next to nothing, and `NOTIFY` takes a
database-wide lock at commit, which serializes the commits of every
transaction that notifies — every writer that emits. A wake-up on `NOTIFY`
may become a setting if latency under 200 ms is ever needed.

**Can I emit outside a transaction?** `emit` takes a connection: on one
outside a transaction, its `INSERT` commits on its own. That is an event
with no fact behind it — record the fact, and emit with it.

## Status

`0.1`. The API may change before `1.0`. PostgreSQL 14 or later,
[sqlx] 0.8, [async-nats] 0.50, Rust 1.88 or later.

[sqlx]: https://crates.io/crates/sqlx
[async-nats]: https://crates.io/crates/async-nats

## License

Licensed under either of [Apache License, Version 2.0](https://github.com/rsansores/magic_events/blob/main/LICENSE-APACHE) or
[MIT license](https://github.com/rsansores/magic_events/blob/main/LICENSE-MIT) at your option.
