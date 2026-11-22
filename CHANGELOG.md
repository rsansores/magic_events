# Changelog

## 0.2.0 — unreleased

Schema 2: a new step, `magic_events/sql/postgres/2.sql` (`migration(2)`).
Messages unchanged: an event of the main stream is published as before
(`tests/golden/messages.txt` gains a case of another stream). Dashboard
contract 1: fields added, none removed or changed.

- **Several streams on one outbox.** An event names the stream it goes to,
  `#[event(stream = "live")]`, per struct or per variant; one that names
  none goes to the main stream, as every event did. `Settings::stream(key,
  StreamSettings)` declares another stream — its own JetStream name,
  subject prefix, retention, duplicate window, storage, replicas,
  `max_body` and `manage_stream` — while the relay's settings stay on
  `Settings`, and `Settings`' setters of a stream's settings set the main
  one's, as before. One relay publishes every stream, in one process at a
  time; each stream has its own passes, backoff, failures, parked rows and
  counts, in id order within it, so a stream the broker fails — deleted,
  full, slow — holds back no other. `Events::start` refuses a key declared
  twice, two keys of one JetStream stream, and overlapping subject
  prefixes. A stream declared once and no longer keeps its rows, and the
  status lists it.
- **`emit`** writes the event's stream and holds its body to that stream's
  `max_body`; it refuses with `Error::Misuse`, writing nothing, an event of
  a stream no relay ever declared.
- **Status, recent and unpark per stream.** `Filter::stream(key)` reads
  another stream; `Filter::all()` is the main one's, and
  `Filter::subject(…)` the stream whose prefix it is under. `Status` and
  `Recent` say which stream (`stream_key`), and a status of every subject
  lists every stream's outbox and failures (`streams`, of `StreamOutbox`).
  `Events::unpark_all` takes a `&Filter`: the parked rows of one stream,
  under a subject filter. `Events::pass_now` runs a pass of every stream.
- **`consume`** reads any stream, by its JetStream name, as before.
- **The dashboard** lists the streams when there are several
  (`streams`, `streamsOf`, `StreamRow`), and is stuck while events wait for
  a stream no relay declares any more (`stream_undeclared`).

Upgrading from 0.1:

- **The schema**: `Events::install` runs step 2; a host with migrations of
  its own adds one with `magic_events::migration(2)`. The step keeps every
  row: what waits in the outbox, the relay's row and the counts become the
  main stream's. On a large outbox it rebuilds the two indexes in the
  migration's transaction.
- **The grants**: the relay's role needs `INSERT` on `magic_events.relay`
  too: `Events::start` writes a row for each stream it declares.
- **A rolling deploy**: a writer of 0.1 still running after step 2 goes on
  writing to the main stream. A relay of 0.1 still leading after it cannot
  settle a pass — its counts no longer fit the table — and sends that batch
  again until it stops, which the stream drops as duplicates only within
  its `duplicate_window`; and it would publish an event of another stream
  under the main stream's prefix. So stop the 0.1 relays within the
  duplicate window of the step, and declare other streams only once no 0.1
  process runs.
- **The API**: `Events::unpark_all()` is `Events::unpark_all(&Filter::all())`
  for the main stream. Added: `StreamSettings`, `Settings::stream`,
  `Filter::stream`, `Status::stream_key`, `Status::streams`,
  `StreamOutbox`, `Recent::stream_key`, `SCHEMA_VERSION` 2, and the
  derive's `stream` key. `emit` may answer `Error::Misuse`.

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
