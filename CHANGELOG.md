# Changelog

## 0.1.1 — unreleased

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
