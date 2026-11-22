# How it works

An event is a row. `emit` writes it to an outbox table in the caller's
transaction; a relay, in one process at a time, reads what committed,
publishes it to its JetStream stream, and deletes it once the stream
acknowledged it.
Everything below is how those two halves stay correct when processes die,
connections drop and the broker goes away.

## The tables

The schema `magic_events` (`magic_events/sql/postgres/`, one file a step):

| Table | What it holds |
|---|---|
| `version` | one row: the schema's version. `Events::start` refuses another. |
| `outbox` | the events committed and not published yet, one row each, in emit order (`id`, a `bigserial`). A `TRUNCATE` is refused: it would drop committed events with no trace. |
| `relay` | one row per stream (`stream`, its key: empty for the main one): how its relay is doing (`last_published_at`, `failures`, `last_error`, `retry_after`), and `max_body`, the largest body `emit` takes for it. `Events::start` writes a row for every stream it declares. |
| `published` | messages published, per stream, day (UTC) and type: the outbox forgets what it published, these count it. The relay forgets days older than 90; the status reads the last 30. |

An outbox row holds the event's id (`event_id`, a UUIDv7: its `ce-id` and
its `Nats-Msg-Id`), the key of its stream (`stream`), its subject after the
prefix, its type with the version
(`order.shipped.v1`), the headers `emit` rendered (`ce-type`, and
`ce-subject` when there is one), the body, when it was emitted
(`clock_timestamp()`, its `ce-time`), and — for a message the broker
refuses — how often, why, and when it was set aside (`parked_at`). A
partial index on `(stream, id)` where `parked_at IS NULL` is what the relay
reads, a stream at a time, and one where it is not is what the status lists
set-aside rows by. The table is
a queue — rows inserted and deleted all day — so it is vacuumed every
thousand dead rows, whatever its size, and without the default's pauses.

The tables change only by a new step (`sql/postgres/<n>.sql`), never by
editing one that was released: `migration(n)` is that step as a script that
also records its version, and `Events::install` runs the steps a database
lacks, in one transaction, under an advisory lock so two processes
installing at once take turns.

Step 2 made one stream several: it added `stream` to the outbox, the relay
row and the counts, every row there already the main stream's (the empty
key), and kept `id = 1` on the main stream's relay row, which a writer of a
build before it reads — so such a writer, still running while the step
runs, goes on writing to the main stream.

## `emit`

`emit(&mut conn, &event)`:

1. asks the event — the code the derive wrote — for its type, version,
   subject tokens, `ce-subject` and stream key;
2. refuses, before anything is written, a subject token that is empty or
   holds a `.`, `*`, `>`, whitespace or anything but printable ASCII, a
   subject over 1 KiB, and a `ce-subject` over 1 KiB once percent-encoded —
   with the source and the prefix the settings hold to similar limits, a
   message's headers stay within the 4 KiB `start` keeps for them;
3. serializes the body: the event's JSON, or for an enum the variant's own
   fields (the variant is in `ce-type`) — an object, or it is refused;
4. runs one statement: it reads the `max_body` of the event's stream from
   its relay row (64 KiB when no relay recorded one) and inserts the row
   only if the body fits, answering which — and refuses the event, writing
   nothing, when its stream has no relay row: no relay ever declared it, so
   nothing would publish it.

No broker, no second round trip: the writer pays one `INSERT`. It does not
know the subject prefix or the CloudEvents `source`; the relay adds both,
so a change to either applies to what is still in the outbox.

## Streams

An event names its stream by a key (`#[event(stream = "live")]`), or none:
the main stream. `Settings::new` names the main stream's JetStream stream;
`Settings::stream(key, …)` declares another, with its own name, subject
prefix, retention, duplicate window, storage, replicas, `max_body` and
`manage_stream`. What is the relay's — the source, the waits, the batch, the
role — is the same for every stream. `Events::start` refuses a key declared
twice, two keys of one JetStream stream, and two subject prefixes one of
which takes the other's subjects (`shop` and `shop.live`): a stream would
hold another's events, and a subject filter would not say whose it is.

## The relay

`Events::start` checks the settings, the schema's version, and that each
stream's `max_body` with 4 KiB of headers fits the broker's `max_payload`;
creates each stream or brings it to its settings (or, with
`manage_stream(false)`, checks that it takes every subject under its
prefix); writes each stream's relay row with its `max_body` for `emit` —
every process that starts records its own, the last one counts; and spawns
the relay in a task of its own.

### One at a time

The relay runs where it holds a session advisory lock (`EVNTRLAY`), taken
with `pg_try_advisory_lock` on a connection of its own, opened with the
pool's options rather than taken from it: it lives exactly as long as the
lock, and closes — letting go of the lock — when the task holding it is
dropped. A process that does not get the lock stands by and asks again
every `standby`. When the leader's process dies or its connection drops,
PostgreSQL releases the lock, and a process standing by takes it.

The lock's session is set so the server ends it when the relay is gone
without saying so: TCP keepalives after 10 s of silence, every 5 s, three
missed; anything sent and unacknowledged for 20 s; a statement running past
30 s cancelled; and the client looked for every 5 s while one runs. With
`Settings::role`, the session sets that role too (`SET ROLE`), and every
transaction of the crate's own outside it — start's checks, status, unpark —
sets it for itself (`SET LOCAL ROLE`), so the connection goes back to the
pool as it came. So a
leader whose network dies — a partition, a frozen machine — loses its lock
within about 30 seconds, not the hours the defaults take. On its side, every
statement the relay sends is bounded by the same 30 s: a database that
stops answering is taken for away, the lock let go, and the relay stands by.

Each pass opens its transactions on the lock's own connection, and checks,
inside each, that the server process it runs on holds the lock
(`pg_locks` for `pg_backend_pid()`). That is the fence: a relay that lost
its lock — its connection killed while it was publishing — commits nothing,
so it can never delete rows a new leader is publishing. It also catches a
pooler in transaction mode, which may run each transaction on another
server process: `Events::start` takes the lock and opens a fenced
transaction once, as a probe, and refuses to start, naming the pooler, when
the fence fails; a relay that meets it later publishes nothing and says why.

### A lane per stream

The leader runs a lane for each stream it declares: a loop of that stream's
passes, with its own backoff, its own failures in its relay row, and its own
idle wait. The lanes run side by side — futures of the relay's one task —
and share the lock's connection, taking turns on it for their statements;
everything else, the publishing and the waiting for acknowledgements, each
does on its own.

Side by side, not one after the other: a pass waits for its stream's
acknowledgements, up to `send_timeout` (10 s) when the stream's leader does
not answer, and passes taken in turn would make every other stream wait as
long, every backoff, for as long as that stream is away. Side by side, a
stream that fails costs the others only the turns its statements take on
the connection — each bounded by the database's 30 s, as every statement
of the relay's is. And futures of one task, not tasks of their own: the
relay starts, stops and dies as one — a lane cannot outlive the lock, nor
the `Events` that ran it.

At the top of every pass a lane checks the lock is still there; the first
to find it gone ends every lane, and the relay stands by again.
`Events::stop` is seen between passes, never inside one: each lane finishes
the pass under way — what it published deleted — and the lock is let go at
once, so a deploy sends nothing twice and the next relay waits for no
keepalive. Once an hour, beside the lanes, the leader forgets the counts
older than 90 days.

A stream declared once and no longer has no lane: its rows wait, its relay
row stays — so `emit` still takes its events — and the status lists it as
declared by no one. Nothing drops them.

### A pass

1. **Read**, fenced: up to `batch` of the stream's rows `WHERE stream = $1
   AND parked_at IS NULL ORDER BY id`.
2. **Publish**, with no transaction open: each row's message — the subject
   under the stream's prefix, the CloudEvents headers, `Nats-Msg-Id` —
   handed to JetStream, up to `in_flight` sent and not acknowledged yet, the
   acknowledgements awaited in order, each within `send_timeout`. The first
   failure stops the sending; what was sent before it is still awaited.
3. **Settle**, fenced, in one transaction: the acknowledged prefix deleted
   and counted into `published`, and the stream's relay row updated — or the
   failure recorded:
   - a **transient** failure (a timeout, the connection lost, no stream
     answering, the stream unable to take anything): the stream's
     `failures + 1`, `last_error`, `retry_after`; the rows stay, and the
     lane waits out its backoff (500 ms, doubling, up to a minute) — the
     other lanes go on;
   - a **permanent** one, the broker refusing that message for what it is
     (larger than the server's `max_payload` or the stream's
     `max_message_size`): `attempts + 1` on that row; at the third, it is
     parked, logged as an error, and the next pass goes on with the rows
     behind it.
4. Nothing waiting for the stream: wait `idle`, and read again.

A row is deleted only after its message was acknowledged, and its message
always carries the same `Nats-Msg-Id`. A relay that dies between the
acknowledgement and the delete — or loses its lock there, and is fenced out
of the delete — leaves the row, and whoever relays next sends it again; the
stream, which remembers ids for its `duplicate_window`, drops it.

Rows are read in `id` order within a stream, and an `id` is taken when a
row is inserted,
not when its transaction commits. A transaction that commits after a later
one has its rows published after the later one's — late, not lost: nothing
remembers a position to skip past them; the relay reads what is there.

## What fails, and what happens

| What fails | What happens | What heals it |
|---|---|---|
| The writer's transaction rolls back | Its rows were never there. | — |
| The broker is away, slow, or its stream full | Passes fail, rows stay, the relay backs off; the status shows the failures and the outbox's age. | The broker coming back: everything goes, once. |
| One stream is deleted, full, or its leader does not answer | Its lane's passes fail and back off, its rows stay; every other stream's lane publishes as before. The status of that stream shows its failures, and every stream's lists them. | The stream back (`Events::start` makes a managed one again): its rows go, in order, once. |
| The broker refuses a message for good | Three attempts, then parked; the rows behind it go on; an error log, and the status lists it. | A fix (a larger `max_message_size`, say), then `unpark`. |
| The relay's process dies mid-pass | Its lock is released; another process takes it within `standby`. Rows published and not deleted are sent again and dropped by the stream. | Nothing to do. |
| The relay's connection is killed | Its fenced transactions fail; it finds its lock gone and stands by; another process (or itself) takes the lock. | Nothing to do. |
| The relay's network dies, or its database stops answering | Its statements give up within 30 s and it stands by; the server ends its session, and the lock, within about 30 s; another process takes over. | Nothing to do. |
| The stream took a message, and the row was not deleted — the relay died, or the acknowledgement was lost — and it is sent again only after `duplicate_window` | Published again, and this time kept: a duplicate, with the same `ce-id`. | Consumers deduplicate by `ce-id`. |
| A message is parked | The rest of its transaction's events go out before it; it follows them once unparked. | Consumers do not rely on order (a re-read does not need it). |
| No process runs the relay | Events wait in the outbox; the status says no relay runs. | Starting one. |
| A stream is no longer declared | Its rows wait, nothing publishes them; `emit` still takes its events; the status of every subject lists it as declared by no one. | Declaring it again, or deleting its rows. |
| An event names a stream no relay ever declared | `emit` refuses it, and writes nothing. | Declaring the stream, and starting a relay. |
| A consumer is away | The stream keeps its messages for `retention`; the consumer's lag grows in the status. | The consumer coming back within `retention`. |

## The status

`Events::status` answers one stream: the one `Filter::stream` names, the
one whose prefix `Filter::subject` is under, or the main one. It reads the
database in one `REPEATABLE READ` snapshot — the stream's outbox under the
filter (a NATS subject filter, turned into a regular expression on each
row's full subject), its parked rows, its relay row, whether any process
holds the lock, its published counts, and every stream's outbox and relay
row — and asks the broker for the stream's state, its message count per
subject under the filter, and its durable consumers. For every subject, the outbox is counted
whole; a narrower filter is matched row by row. The broker's part is best-effort: one that
does not answer within five seconds is left out, and the answer says why.
A consumer's lag, the published counts and the other streams cannot be
narrowed by subject, so only an answer for every subject carries them.

`Events::recent` reads the last messages — 1,000 at most — through ordered
consumers of the broker's own, ephemeral, leaving nothing behind: each
starts further back until it has what it was asked for or reaches the
stream's start, keeping only the last it read.

## Consuming

`consume` reads any stream — the main one or another — by its JetStream
name: it creates or resumes a durable pull consumer with explicit
acknowledgements — and refuses one of that name that reads another filter,
whose events `E` would not know. It takes one message at a time, so none
waits in a buffer while its acknowledgement's 30 s run out, and tells the
broker every 10 s that a slow handler is still at work. It reads `ce-type`,
finds which of `E`'s events that is, and decodes the body into it — for an
enum, by the variant's place, so serde renames do not matter. A handler's
`Ok` acknowledges; an error asks for a redelivery after 1 s, doubling with
each delivery up to a minute, and the events behind it go first. A
message of a type `E` does not declare comes again every minute, until a
build that declares it reads it; one of a type `E` declares whose body does
not decode is terminated — never delivered to that consumer again — with an
error logged. A broker that goes away is waited for: the client
reconnects and the consumer goes on, and an answer lost on the way means
the event comes again.
