# Changelog

## 0.2.0 — 2026-11-22

Schema 2: a new step, `magic_events/sql/postgres/2.sql` (`migration(2)`).
Messages unchanged: an event of the main stream is published as before
(`tests/golden/messages.txt` gains a case of another stream). Dashboard
contract 2: fields renamed (below); the dashboard reads only contract 2.

- **Several streams on one outbox.** An event names the stream it goes to,
  `#[event(stream = "live")]`, per struct or per variant; one that names
  none goes to the main stream, as every event did. A stream's settings are
  a `StreamSettings` — its JetStream name, subject prefix, retention,
  duplicate window, storage, replicas, `max_body` and `manage_stream`:
  `Settings::new` takes the main stream's (or only its name), and
  `Settings::stream(key, StreamSettings)` declares another. One relay
  publishes every stream, in one process at a time; each stream has its own
  passes, backoff, failures, parked rows and counts, in id order within it,
  so a stream the broker fails — deleted, full, slow — holds back no other.
  `Events::start` refuses a key declared twice, two keys of one JetStream
  stream, overlapping subject prefixes, and settings that declare a stream
  otherwise than the leading relay does.
- **The leader declares the streams.** A relay that takes the lock records,
  for each stream its settings declare, the JetStream name, the prefix and
  `max_body`, and marks every other stream undeclared; its lanes publish
  its own settings' streams, and a warning says when it moves one
  elsewhere than the relay before it declared. Every process that starts
  records the streams it declares; `emit` refuses with `Error::Misuse`,
  writing nothing, an event of a stream none ever did, and takes any other
  — waiting, when the leader does not publish its stream, as the status
  shows. A relay that cannot declare its streams lets go of the lock and
  waits `standby`.
- **`Events::retire(key, drop_waiting)`** takes away a stream no relay
  declares any more, refusing its waiting events unless told to drop them;
  `emit` refuses its events from then on.
- **Status, recent and unpark per stream.** `Filter::main()` (was
  `Filter::all()`) reads the main stream, `Filter::stream(key)` another,
  `Filter::subject(…)` the stream whose prefix it is under. A status says
  which stream (`key`), and one of every subject lists every stream's
  outbox and failures (`outboxes`, of `StreamOutbox`). `Events::unpark_all`
  takes a `&Filter`. `Events::pass_now` declares the streams and runs a
  pass of each.
- **`consume`** reads any stream, by its JetStream name, as before.
- **The dashboard** lists the streams when there are several (`streams`,
  `streamsOf`, `StreamRow`), and is stuck while events wait for a stream
  the leading relay does not declare (`stream_undeclared`).
- A relay stopped as it lost its lock could panic, rather than stop.

Upgrading from 0.1:

- **The schema**: `Events::install` runs step 2; a host with migrations of
  its own adds one with `magic_events::migration(2)`. The step keeps every
  row: what waits in the outbox, the relay's row and the counts become the
  main stream's. It locks `relay`, `outbox` and `published` first, so
  writes wait for as long as it runs, which on a large outbox includes
  rebuilding the two indexes.
- **A rolling deploy**: a writer of 0.1 still running after step 2 goes on
  writing to the main stream (the outbox's `stream` defaults to it, and the
  main stream's relay row keeps `id = 1`; a later step drops both once no
  0.1 writer can be left). A relay of 0.1 still leading after it fails its
  reads — the step renames the outbox's `emitted_at` to `occurred_at` for
  that — and publishes nothing until it stops and a 0.2 relay takes over.
  A stream added in the deploy takes events from the first process of the
  new settings that starts; they wait while a relay of older settings
  leads, and go out once one of the new does.
- **The grants**: the relay's role needs `INSERT` on `magic_events.relay`
  too, and — to retire a stream — `DELETE` on `magic_events.relay` and
  `magic_events.published`.
- **The API**:
  - `Settings::new(name, source)` stays; the main stream's other settings
    move to `StreamSettings`: `Settings::new("SHOP", src).retention(r)` is
    `Settings::new(StreamSettings::new("SHOP").retention(r), src)`, and so
    for `subject_prefix`, `duplicate_window`, `storage`, `replicas`,
    `manage_stream` and `max_body`.
  - `Filter::all()` is `Filter::main()`; `Events::unpark_all()` is
    `Events::unpark_all(&Filter::main())`.
  - `status::Stream` is `status::JetStream`, and `Status::stream` is
    `Status::jetstream`.
  - Times are `time`'s, not `chrono`'s, which is unmaintained
    (RUSTSEC-2026-0543): every time of the status, and `Delivery::time`, is
    a `time::OffsetDateTime`, and `Published::day` a `time::Date`. Their
    JSON is the same RFC 3339, and the crate no longer depends on `chrono`,
    nor turns on sqlx's `chrono` feature.
  - Added: `StreamSettings`, `Settings::stream`, `Filter::stream`,
    `Events::retire`, `Status::key`, `Status::outboxes`, `StreamOutbox`,
    `Recent::key`, `SCHEMA_VERSION` 2, and the derive's `stream` key.
    `emit` may answer `Error::Misuse`.
- **The dashboard contract** is 2: `stream` is `jetstream`, and `key` and
  `outboxes` are new. Upgrade the dashboard with the crate.

## 0.1.3 — 2026-10-21

Schema 1, unchanged. Messages unchanged. Dashboard contract 1, unchanged.
The crates are unchanged but for their version.

- **`useEventsView`** (dashboard): the headless dashboard over answers the
  host already holds — `Events::status` and, optionally, `Events::recent`,
  read through its own data layer (vue-query, Pinia, a socket) with its own
  cache, polling and errors. It fetches nothing and works out the same state
  `useEventsDashboard` does, the recent events among it (`recent`,
  `recentError`). `useEventsDashboard` is now `useEventsView` over a fetch
  function, and answers as before.

## 0.1.1 — 2026-10-21

Schema 1, unchanged. Messages unchanged. Dashboard contract 1, unchanged.

- **`Settings::role`**: the database role the crate's own work runs as — the
  relay's session, and the transactions of `Events::start`,
  `Events::status` and `Events::unpark` — rather than the role the pool logs
  in as. For a host whose transactions each assume a role of their own,
  under row security say: the relay's grants go to that role alone, and the
  host's pool stays the one pool. `emit` runs as the caller's transaction
  does, and `Events::install` as the login role. API: one method added.
- **`Events::stop` takes `&self`**, so an `Events` shared behind an `Arc` —
  with request handlers serving `Events::status`, say — is stopped where the
  host shuts down. A call written `events.stop().await` compiles as before.
- A test stopped the relay mid-drain only on a slow enough machine; it now
  does on any.

## 0.1.0 — 2026-10-21

The first release. Schema 1. Dashboard contract 1.

- **An event is a type that derives `Event`**: a struct, or an enum of one
  event per variant, each saying its type, its version and its subject
  (`orders.{store}.shipped`), checked at compile time.
- **`emit`** writes an event to the outbox in the caller's transaction, as
  one `INSERT`: published if and only if the transaction commits. A subject
  no subject may hold, or a body over `max_body`, is refused at the call.
- **`Events::start`** runs the relay inside the host's process, one process
  at a time on an advisory lock, fenced so a relay that lost its lock writes
  nothing: pipelined publishing in id order, deduplicated by `Nats-Msg-Id`,
  backoff while the broker is away, and a message the broker refuses three
  times set aside (`Events::unpark`). It creates the stream, or checks the
  operator's.
- **The messages** are CloudEvents in the NATS binding's binary content
  mode: `ce-specversion`, `ce-id`, `ce-source`, `ce-type` (`order.shipped.v1`),
  `ce-time`, `ce-datacontenttype`, `ce-subject`; the body is the event's
  JSON (`tests/golden/messages.txt`).
- **`Events::status` and `Events::recent`**, under a subject filter: the
  outbox, the relay, the stream, the consumers' lag, the counts published;
  serialized as contract 1 for **`magic_events_dashboard`**, a Vue 3
  package, headless or ready-made.
- **`consume`** (feature `consume`): a Rust consumer that decodes each
  event into its type, one at a time, acknowledges once the handler answers
  `Ok`, and waits out a broker that goes away.
- **The shop example**: a shop that writes and relays, two consumers, and
  an events page over the dashboard.
- **The schema** `magic_events`: `Events::install`, or `migration(1)` for a
  host with migrations of its own.
