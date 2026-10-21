// Generated from magic_events::status by its test `the_dashboards_types_are_generated_from_these`: UPDATE_CONTRACT=1 rewrites it.

/** The contract these types describe: an answer of another is refused. */
export const CONTRACT = 1

/**
 * What `Events::status` answers: the relay, the outbox and the
 * stream, as one filter sees them.
 */
export type Status = { 
/**
 * The contract this answer serializes as: `CONTRACT`.
 */
contract: number, 
/**
 * The subject filter it covers, in full.
 */
filter: string, 
/**
 * The database's clock as it answered: what the ages are counted from.
 */
at: string, 
/**
 * What waits in the outbox, under the filter.
 */
outbox: Outbox, 
/**
 * The relay, wherever it runs.
 */
relay: Relay, 
/**
 * What the stream holds under the filter; `None` when the broker could
 * not be reached (`broker_error` says why).
 */
stream: Stream | null, 
/**
 * Why the broker's part is missing, when it is.
 */
broker_error: string | null, 
/**
 * Each durable consumer of the stream: only for every subject, since a
 * consumer's lag is the whole stream's.
 */
consumers: Array<Consumer> | null, 
/**
 * Messages published per day (UTC) and type, the last 30 days: only
 * for every subject, since they are counted by type alone.
 */
published: Array<Published> | null, };

/**
 * What waits in the outbox.
 */
export type Outbox = { 
/**
 * Rows waiting to be published, parked ones not counted.
 */
pending: number, 
/**
 * When the oldest of them was emitted.
 */
oldest_pending_at: string | null, 
/**
 * Rows the broker refused for good, waiting for `unpark`.
 */
parked: number, 
/**
 * The first of them, in order, up to ten.
 */
first_parked: Array<Parked>, };

/**
 * A row the broker refused for good.
 */
export type Parked = { 
/**
 * Its id in the outbox: what `Events::unpark` takes.
 */
id: number, 
/**
 * The event's id, its `ce-id`.
 */
event_id: string, 
/**
 * The subject it is published on, in full.
 */
subject: string, 
/**
 * Its `ce-type`, as `order.shipped.v1`.
 */
type: string, 
/**
 * Times the broker refused it.
 */
attempts: number, 
/**
 * Why, the last time.
 */
last_error: string | null, 
/**
 * When it was emitted.
 */
emitted_at: string, 
/**
 * When it was set aside.
 */
parked_at: string, };

/**
 * The relay, wherever it runs.
 */
export type Relay = { 
/**
 * Whether a process holds the relay's lock now.
 */
running: boolean, 
/**
 * When the stream last acknowledged a message.
 */
last_published_at: string | null, 
/**
 * Passes the broker failed in a row; 0 once one goes through.
 */
failures: number, 
/**
 * Why the last of them failed.
 */
last_error: string | null, 
/**
 * When the relay tries again after them.
 */
retry_after: string | null, };

/**
 * What the stream holds under the filter.
 */
export type Stream = { 
/**
 * The stream's name.
 */
name: string, 
/**
 * Messages held under the filter.
 */
messages: number, 
/**
 * Bytes the stream holds: only for every subject.
 */
bytes: number | null, 
/**
 * When its first message was published: only for every subject.
 */
first_at: string | null, 
/**
 * When its last message was published: only for every subject.
 */
last_at: string | null, 
/**
 * Subjects under the filter with messages held.
 */
subjects_total: number, 
/**
 * The hundred of them with the most messages, the most first.
 */
subjects: Array<SubjectCount>, };

/**
 * Messages held on one subject.
 */
export type SubjectCount = { 
/**
 * The subject, in full.
 */
subject: string, 
/**
 * Messages held on it.
 */
messages: number, };

/**
 * A durable consumer of the stream: one system that reads it.
 */
export type Consumer = { 
/**
 * Its durable name.
 */
name: string, 
/**
 * The subject filters it reads; none for every subject.
 */
filters: Array<string>, 
/**
 * Messages it has not been delivered yet: how far behind it is.
 */
pending: number, 
/**
 * Messages delivered and not acknowledged yet.
 */
ack_pending: number, 
/**
 * Messages delivered more than once.
 */
redelivered: number, 
/**
 * When it was last delivered a message.
 */
last_active: string | null, };

/**
 * Messages published on one day, of one type.
 */
export type Published = { 
/**
 * The day, UTC, as `2027-01-15`.
 */
day: string, 
/**
 * The `ce-type`, as `order.shipped.v1`.
 */
type: string, 
/**
 * How many.
 */
count: number, };

/**
 * What `Events::recent` answers: the last messages the stream
 * holds under a filter.
 */
export type Recent = { 
/**
 * The contract this answer serializes as: `CONTRACT`.
 */
contract: number, 
/**
 * The subject filter it covers, in full.
 */
filter: string, 
/**
 * The messages, oldest first.
 */
messages: Array<Message>, };

/**
 * One message as the stream holds it.
 */
export type Message = { 
/**
 * Its sequence in the stream.
 */
sequence: number, 
/**
 * Its subject.
 */
subject: string, 
/**
 * When the stream took it.
 */
time: string, 
/**
 * Its headers, by name: the CloudEvents attributes among them.
 */
headers: { [key in string]: string }, 
/**
 * Its body, as text.
 */
body: string, };
