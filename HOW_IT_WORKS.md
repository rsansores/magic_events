# How it works

An event is a row. `emit` writes it to an outbox table in the caller's
transaction; a relay, in one process at a time, reads what committed,
publishes it to JetStream, and deletes it once the stream acknowledged it.
Everything below is how those two halves stay correct when processes die,
connections drop and the broker goes away.

## The tables

The schema `magic_events` (`magic_events/sql/postgres/1.sql`):

| Table | What it holds |
|---|---|
| `version` | one row: the schema's version. `Events::start` refuses another. |
| `outbox` | the events committed and not published yet, one row each, in emit order (`id`, a `bigserial`). A `TRUNCATE` is refused: it would drop committed events with no trace. |
| `relay` | one row: how the relay is doing (`last_published_at`, `failures`, `last_error`, `retry_after`), and `max_body`, the largest body `emit` takes, written by `Events::start`. |
| `published` | messages published, per day (UTC) and type: the outbox forgets what it published, these count it. |

An outbox row holds the event's id (`event_id`, a UUIDv7: its `ce-id` and
its `Nats-Msg-Id`), its subject after the prefix, its type with the version
(`order.shipped.v1`), the headers `emit` rendered (`ce-type`, and
`ce-subject` when there is one), the body, when it was emitted
(`clock_timestamp()`, its `ce-time`), and — for a message the broker
refuses — how often, why, and when it was set aside (`parked_at`). A
partial index on `id` where `parked_at IS NULL` is what the relay reads.

The tables change only by a new step (`sql/postgres/<n>.sql`), never by
editing one that was released: `migration(n)` is that step as a script that
also records its version, and `Events::install` runs the steps a database
lacks, in one transaction, under an advisory lock so two processes
installing at once take turns.

## `emit`

`emit(&mut conn, &event)`:

1. asks the event — the code the derive wrote — for its type, version,
   subject tokens and `ce-subject`;
2. refuses, before anything is written, a subject token that is empty or
   holds a `.`, `*`, `>`, whitespace or anything but printable ASCII, and a
   token or `ce-subject` over 1 KiB;
3. serializes the body: the event's JSON, or for an enum the variant's own
   fields (the variant is in `ce-type`);
4. runs one statement: it reads `relay.max_body` (64 KiB when no relay
   recorded one) and inserts the row only if the body fits, answering which.

No network, no second round trip: the writer pays one `INSERT`. It does not
know the subject prefix or the CloudEvents `source`; the relay adds both,
so a change to either applies to what is still in the outbox.

## The relay

`Events::start` checks the settings, the schema's version, and that
`max_body` with 4 KiB of headers fits the broker's `max_payload`; records
`max_body`; creates the stream or brings it to the settings (or, with
`manage_stream(false)`, checks that it takes every subject under the
prefix); and spawns the relay in a task of its own.

### One at a time

The relay runs where it holds a session advisory lock (`EVNTRLAY`), taken
with `pg_try_advisory_lock` on a connection detached from the pool — one
that closes, and lets go of the lock, if the task holding it is dropped
mid-question rather than going back to the pool still holding it. A process
that does not get the lock stands by and asks again every `standby`. When
the leader's process dies or its connection drops, PostgreSQL releases the
lock, and a process standing by takes it.

Each pass opens its transactions on the lock's own connection, and checks,
inside each, that the server process it runs on holds the lock
(`pg_locks` for `pg_backend_pid()`). That is the fence: a relay that lost
its lock — its connection killed while it was publishing — commits nothing,
so it can never delete rows a new leader is publishing. It also catches a
pooler in transaction mode, which may run each transaction on another
server process: the relay then publishes nothing and says why.

At the top of every loop the leader checks its lock is still there; if not,
it stands by again.

### A pass

1. **Read**, fenced: up to `batch` rows `WHERE parked_at IS NULL ORDER BY id`.
2. **Publish**, with no transaction open: each row's message — the subject
   under the prefix, the CloudEvents headers, `Nats-Msg-Id` — handed to
   JetStream, up to `in_flight` sent and not acknowledged yet, the
   acknowledgements awaited in order, each within `send_timeout`. The first
   failure stops the sending; what was sent before it is still awaited.
3. **Settle**, fenced, in one transaction: the acknowledged prefix deleted
   and counted into `published`, and the relay's row updated — or the
   failure recorded:
   - a **transient** failure (a timeout, the connection lost, no stream
     answering, the stream unable to take anything): `relay.failures + 1`,
     `last_error`, `retry_after`; the rows stay, and the relay waits out its
     backoff (500 ms, doubling, up to a minute);
   - a **permanent** one, the broker refusing that message for what it is
     (larger than the server's `max_payload` or the stream's
     `max_message_size`): `attempts + 1` on that row; at the third, it is
     parked, logged as an error, and the next pass goes on with the rows
     behind it.
4. An empty outbox: wait `idle`, and read again.

A row is deleted only after its message was acknowledged, and its message
always carries the same `Nats-Msg-Id`. A relay that dies between the
acknowledgement and the delete — or loses its lock there, and is fenced out
of the delete — leaves the row, and whoever relays next sends it again; the
stream, which remembers ids for its `duplicate_window`, drops it.

Rows are read in `id` order, and an `id` is taken when a row is inserted,
not when its transaction commits. A transaction that commits after a later
one has its rows published after the later one's — late, not lost: nothing
remembers a position to skip past them; the relay reads what is there.

## What fails, and what happens

| What fails | What happens | What heals it |
|---|---|---|
| The writer's transaction rolls back | Its rows were never there. | — |
| The broker is away, slow, or its stream full | Passes fail, rows stay, the relay backs off; the status shows the failures and the outbox's age. | The broker coming back: everything goes, once. |
| The broker refuses a message for good | Three attempts, then parked; the rows behind it go on; an error log, and the status lists it. | A fix (a larger `max_message_size`, say), then `unpark`. |
| The relay's process dies mid-pass | Its lock is released; another process takes it within `standby`. Rows published and not deleted are sent again and dropped by the stream. | Nothing to do. |
| The relay's connection is killed | Its fenced transactions fail; it finds its lock gone and stands by; another process (or itself) takes the lock. | Nothing to do. |
| The relay is down longer than `duplicate_window`, right after a publish | The rows it published and did not delete are published again, and this time kept: a duplicate, with the same `ce-id`. | Consumers deduplicate by `ce-id`. |
| No process runs the relay | Events wait in the outbox; the status says no relay runs. | Starting one. |
| A consumer is away | The stream keeps its messages for `retention`; the consumer's lag grows in the status. | The consumer coming back within `retention`. |

## The status

`Events::status` reads the database in one `REPEATABLE READ` snapshot — the
outbox under the filter (a NATS subject filter, turned into a regular
expression on each row's full subject), its parked rows, the relay's row,
whether any process holds the lock, the published counts — and asks the
broker for the stream's state, its message count per subject under the
filter, and its durable consumers. The broker's part is best-effort: one that
does not answer within five seconds is left out, and the answer says why.
A consumer's lag and the published counts cannot be narrowed by subject, so
only an answer for every subject carries them.

`Events::recent` reads the last messages through ordered consumers of the
broker's own — ephemeral, left behind by nothing — each starting further
back until it has what it was asked for or reaches the stream's start.

## Consuming

`consume` creates or resumes a durable pull consumer with explicit
acknowledgements. It reads `ce-type`, finds which of `E`'s events that is,
and decodes the body into it — for an enum, by the variant's place, so serde
renames do not matter. A handler's `Ok` acknowledges; an error asks for a
redelivery after 1 s, doubling with each delivery up to a minute; a message
`E` cannot read is terminated — never delivered to that consumer again —
with an error logged.
