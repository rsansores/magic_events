//! What the relay answers about itself, for a dashboard or an alarm: what
//! waits in the outbox and since when, what was set aside and why, whether a
//! relay runs, what the stream holds, and how far behind each consumer is.
//!
//! Every read takes a [`Filter`], a subject filter under the settings'
//! prefix, so a host shows each viewer what is theirs and nothing else. What
//! cannot be narrowed by subject — each consumer's lag, the counts of what
//! was published per day — is answered only for [`Filter::all`]: show it on
//! an operator's view.
//!
//! Every answer serializes as the JSON of contract [`CONTRACT`], and carries
//! that number: the dashboard package reads it.

use crate::relay::LOCK_KEY;
use crate::settings::{is_token, Settings};
use crate::stream::covers;
use crate::Error;
use async_nats::jetstream::consumer::{pull::OrderedConfig, DeliverPolicy};
use async_nats::jetstream::Context;
use chrono::{DateTime, NaiveDate, Utc};
use futures_util::{StreamExt, TryStreamExt};
use serde::Serialize;
use sqlx::PgPool;
use std::collections::BTreeMap;
use std::time::Duration;
use uuid::Uuid;

/// The version of the JSON the answers serialize as. A field added does not
/// change it; a field removed or renamed, or a meaning changed, does, and a
/// dashboard refuses an answer of a contract it does not read.
pub const CONTRACT: u32 = 1;

/// Parked rows an answer lists, at most: the first, in order.
const PARKED_LISTED: i64 = 10;

/// Subjects an answer counts messages of, at most: those with the most.
const SUBJECTS_LISTED: usize = 100;

/// Messages `recent` answers, at most.
const RECENT_MOST: u32 = 1_000;

/// Days of published counts an answer carries.
const PUBLISHED_DAYS: i32 = 30;

/// How long the broker may take to answer a read before the answer says it
/// could not be reached.
const BROKER_WAIT: Duration = Duration::from_secs(5);

/// Which subjects a read covers: every one under the settings' prefix, or
/// those a subject filter under it takes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Filter {
    subject: Option<String>,
}

impl Filter {
    /// Every subject under the prefix: the operator's view, the only one
    /// that answers the consumers and the published counts.
    #[must_use]
    pub fn all() -> Self {
        Filter { subject: None }
    }

    /// The subjects `filter` takes — tokens joined by dots, `*` for any one
    /// token, `>` last for the rest — written in full, prefix included:
    /// `"shop.orders.north.>"`.
    #[must_use]
    pub fn subject(filter: impl Into<String>) -> Self {
        Filter {
            subject: Some(filter.into()),
        }
    }

    /// The filter in full, checked against `settings`' prefix.
    pub(crate) fn resolve(&self, settings: &Settings) -> Result<String, Error> {
        let all = crate::stream::subjects(settings);
        let Some(subject) = &self.subject else {
            return Ok(all);
        };
        let tokens: Vec<&str> = subject.split('.').collect();
        let valid = tokens
            .iter()
            .enumerate()
            .all(|(i, t)| is_token(t) || *t == "*" || (*t == ">" && i == tokens.len() - 1));
        if !valid || !covers(&all, subject) {
            return Err(Error::Misuse(format!(
                "`{subject}` is no subject filter under `{all}`: tokens joined by dots, `*` for \
                 any one, `>` last for the rest"
            )));
        }
        Ok(subject.clone())
    }
}

/// The filter `filter` as a regular expression PostgreSQL matches a full
/// subject against.
fn pattern(filter: &str) -> String {
    let tokens: Vec<String> = filter
        .split('.')
        .map(|t| match t {
            "*" => "[^.]+".to_owned(),
            ">" => ".+".to_owned(),
            literal => literal
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() {
                        c.to_string()
                    } else {
                        format!("\\{c}")
                    }
                })
                .collect(),
        })
        .collect();
    format!("^{}$", tokens.join("\\."))
}

/// What `Events::status` answers: the relay, the outbox and the
/// stream, as one filter sees them.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct Status {
    /// The contract this answer serializes as: `CONTRACT`.
    pub contract: u32,
    /// The subject filter it covers, in full.
    pub filter: String,
    /// The database's clock as it answered: what the ages are counted from.
    pub at: DateTime<Utc>,
    /// What waits in the outbox, under the filter.
    pub outbox: Outbox,
    /// The relay, wherever it runs.
    pub relay: Relay,
    /// What the stream holds under the filter; `None` when the broker could
    /// not be reached (`broker_error` says why).
    pub stream: Option<Stream>,
    /// Why the broker's part is missing, when it is.
    pub broker_error: Option<String>,
    /// Each durable consumer of the stream: only for every subject, since a
    /// consumer's lag is the whole stream's.
    pub consumers: Option<Vec<Consumer>>,
    /// Messages published per day (UTC) and type, the last 30 days: only
    /// for every subject, since they are counted by type alone.
    pub published: Option<Vec<Published>>,
}

/// What waits in the outbox.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct Outbox {
    /// Rows waiting to be published, parked ones not counted.
    pub pending: u64,
    /// When the oldest of them was emitted.
    pub oldest_pending_at: Option<DateTime<Utc>>,
    /// Rows the broker refused for good, waiting for `unpark`.
    pub parked: u64,
    /// The first of them, in order, up to ten.
    pub first_parked: Vec<Parked>,
}

/// A row the broker refused for good.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct Parked {
    /// Its id in the outbox: what `Events::unpark` takes. A sequence from 1,
    /// far below 2^53, so a JavaScript number holds it exactly.
    pub id: i64,
    /// The event's id, its `ce-id`.
    pub event_id: Uuid,
    /// The subject it is published on, in full.
    pub subject: String,
    /// Its `ce-type`, as `order.shipped.v1`.
    #[serde(rename = "type")]
    pub ty: String,
    /// Times the broker refused it.
    pub attempts: u32,
    /// Why, the last time.
    pub last_error: Option<String>,
    /// When it was emitted.
    pub emitted_at: DateTime<Utc>,
    /// When it was set aside.
    pub parked_at: DateTime<Utc>,
}

/// The relay, wherever it runs.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct Relay {
    /// Whether a process holds the relay's lock now.
    pub running: bool,
    /// When the stream last acknowledged a message.
    pub last_published_at: Option<DateTime<Utc>>,
    /// Passes the broker failed in a row; 0 once one goes through.
    pub failures: u32,
    /// Why the last of them failed.
    pub last_error: Option<String>,
    /// When the relay tries again after them.
    pub retry_after: Option<DateTime<Utc>>,
}

/// What the stream holds under the filter.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct Stream {
    /// The stream's name.
    pub name: String,
    /// Messages held under the filter.
    pub messages: u64,
    /// Bytes the stream holds: only for every subject.
    pub bytes: Option<u64>,
    /// When its first message was published: only for every subject.
    pub first_at: Option<DateTime<Utc>>,
    /// When its last message was published: only for every subject.
    pub last_at: Option<DateTime<Utc>>,
    /// Subjects under the filter with messages held.
    pub subjects_total: u64,
    /// The hundred of them with the most messages, the most first.
    pub subjects: Vec<SubjectCount>,
}

/// Messages held on one subject.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct SubjectCount {
    /// The subject, in full.
    pub subject: String,
    /// Messages held on it.
    pub messages: u64,
}

/// A durable consumer of the stream: one system that reads it.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct Consumer {
    /// Its durable name.
    pub name: String,
    /// The subject filters it reads; none for every subject.
    pub filters: Vec<String>,
    /// Messages it has not been delivered yet: how far behind it is.
    pub pending: u64,
    /// Messages delivered and not acknowledged yet.
    pub ack_pending: u64,
    /// Messages delivered more than once.
    pub redelivered: u64,
    /// When it was last delivered a message.
    pub last_active: Option<DateTime<Utc>>,
}

/// Messages published on one day, of one type.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct Published {
    /// The day, UTC, as `2027-01-15`.
    pub day: NaiveDate,
    /// The `ce-type`, as `order.shipped.v1`.
    #[serde(rename = "type")]
    pub ty: String,
    /// How many.
    pub count: u64,
}

/// What `Events::recent` answers: the last messages the stream
/// holds under a filter.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct Recent {
    /// The contract this answer serializes as: `CONTRACT`.
    pub contract: u32,
    /// The subject filter it covers, in full.
    pub filter: String,
    /// The messages, oldest first.
    pub messages: Vec<Message>,
}

/// One message as the stream holds it.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(test, derive(ts_rs::TS))]
#[non_exhaustive]
pub struct Message {
    /// Its sequence in the stream.
    pub sequence: u64,
    /// Its subject.
    pub subject: String,
    /// When the stream took it.
    pub time: DateTime<Utc>,
    /// Its headers, by name: the CloudEvents attributes among them.
    pub headers: BTreeMap<String, String>,
    /// Its body, as text.
    pub body: String,
}

/// A time as the broker's client answers it: `time`'s by default, `chrono`'s
/// when anything in the build enables its `chrono` feature — either way, a
/// `DateTime<Utc>` here.
trait BrokerTime {
    fn utc(self) -> Option<DateTime<Utc>>;
}

impl BrokerTime for time::OffsetDateTime {
    fn utc(self) -> Option<DateTime<Utc>> {
        i64::try_from(self.unix_timestamp_nanos())
            .ok()
            .map(DateTime::from_timestamp_nanos)
    }
}

impl BrokerTime for DateTime<Utc> {
    fn utc(self) -> Option<DateTime<Utc>> {
        Some(self)
    }
}

fn utc(t: impl BrokerTime) -> Option<DateTime<Utc>> {
    t.utc()
}

/// A parked row, as the database reads it.
#[derive(sqlx::FromRow)]
struct ParkedRow {
    id: i64,
    event_id: Uuid,
    subject: String,
    #[sqlx(rename = "type")]
    ty: String,
    attempts: i32,
    last_error: Option<String>,
    emitted_at: DateTime<Utc>,
    parked_at: DateTime<Utc>,
}

/// The relay's row, and whether a process holds its lock.
#[derive(sqlx::FromRow)]
struct RelayRow {
    running: bool,
    last_published_at: Option<DateTime<Utc>>,
    failures: i32,
    last_error: Option<String>,
    retry_after: Option<DateTime<Utc>>,
}

/// The database's part of a status, in one snapshot.
async fn database(pool: &PgPool, prefix: &str, filter: &str, all: bool) -> Result<Status, Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    // Every subject under the prefix is every row: no pattern to match. A
    // narrower filter starts with the prefix's tokens (`Filter::resolve`
    // holds it to that), and a row's subject is what follows them.
    let pattern = (!all).then(|| {
        let skip = prefix.split('.').count();
        pattern(&filter.split('.').skip(skip).collect::<Vec<_>>().join("."))
    });
    let (at, pending, oldest, parked): (DateTime<Utc>, i64, Option<DateTime<Utc>>, i64) =
        sqlx::query_as(
            "SELECT now(), \
                    count(*) FILTER (WHERE parked_at IS NULL), \
                    min(emitted_at) FILTER (WHERE parked_at IS NULL), \
                    count(*) FILTER (WHERE parked_at IS NOT NULL) \
               FROM magic_events.outbox WHERE $1::text IS NULL OR subject ~ $1",
        )
        .bind(&pattern)
        .fetch_one(&mut *tx)
        .await?;
    let first_parked: Vec<ParkedRow> = sqlx::query_as(
        "SELECT id, event_id, $1 || '.' || subject AS subject, type, attempts, last_error, \
                emitted_at, parked_at \
           FROM magic_events.outbox \
          WHERE parked_at IS NOT NULL AND ($2::text IS NULL OR subject ~ $2) \
          ORDER BY id LIMIT $3",
    )
    .bind(prefix)
    .bind(&pattern)
    .bind(PARKED_LISTED)
    .fetch_all(&mut *tx)
    .await?;
    let relay: RelayRow = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND granted \
                   AND database = (SELECT oid FROM pg_database WHERE datname = current_database()) \
                   AND objsubid = 1 AND classid = ($1::bigint >> 32)::oid \
                   AND objid = ($1::bigint & 4294967295)::oid) AS running, \
                last_published_at, failures, last_error, retry_after \
           FROM magic_events.relay WHERE id = 1",
    )
    .bind(LOCK_KEY)
    .fetch_one(&mut *tx)
    .await?;
    let published = if all {
        let rows: Vec<(NaiveDate, String, i64)> = sqlx::query_as(
            "SELECT day, type, count FROM magic_events.published \
              WHERE day > (now() AT TIME ZONE 'UTC')::date - $1 ORDER BY day DESC, type",
        )
        .bind(PUBLISHED_DAYS)
        .fetch_all(&mut *tx)
        .await?;
        Some(
            rows.into_iter()
                .map(|(day, ty, count)| Published {
                    day,
                    ty,
                    count: count.unsigned_abs(),
                })
                .collect(),
        )
    } else {
        None
    };
    tx.commit().await?;
    Ok(Status {
        contract: CONTRACT,
        filter: filter.to_owned(),
        at,
        outbox: Outbox {
            pending: pending.unsigned_abs(),
            oldest_pending_at: oldest,
            parked: parked.unsigned_abs(),
            first_parked: first_parked
                .into_iter()
                .map(|r| Parked {
                    id: r.id,
                    event_id: r.event_id,
                    subject: r.subject,
                    ty: r.ty,
                    attempts: u32::try_from(r.attempts).unwrap_or(0),
                    last_error: r.last_error,
                    emitted_at: r.emitted_at,
                    parked_at: r.parked_at,
                })
                .collect(),
        },
        relay: Relay {
            running: relay.running,
            last_published_at: relay.last_published_at,
            failures: u32::try_from(relay.failures).unwrap_or(0),
            last_error: relay.last_error,
            retry_after: relay.retry_after,
        },
        stream: None,
        broker_error: None,
        consumers: None,
        published,
    })
}

/// The broker's part of a status: the stream under the filter, and with
/// every subject, its consumers.
async fn broker(
    jetstream: &Context,
    name: &str,
    filter: &str,
    all: bool,
) -> Result<(Stream, Option<Vec<Consumer>>), String> {
    let stream = jetstream
        .get_stream(name)
        .await
        .map_err(|e| format!("the stream `{name}` could not be read: {e}"))?;
    let state = stream.cached_info().state.clone();
    let mut counts: Vec<SubjectCount> = stream
        .info_with_subjects(filter)
        .await
        .map_err(|e| format!("the stream `{name}`'s subjects could not be read: {e}"))?
        .map_ok(|(subject, messages)| SubjectCount {
            subject,
            messages: u64::try_from(messages).unwrap_or(u64::MAX),
        })
        .try_collect()
        .await
        .map_err(|e| format!("the stream `{name}`'s subjects could not be read: {e}"))?;
    counts.sort_by(|a, b| b.messages.cmp(&a.messages).then(a.subject.cmp(&b.subject)));
    let subjects_total = u64::try_from(counts.len()).unwrap_or(u64::MAX);
    let messages = if all {
        state.messages
    } else {
        counts.iter().map(|c| c.messages).sum()
    };
    counts.truncate(SUBJECTS_LISTED);
    let held = state.messages > 0;
    let stream_status = Stream {
        name: name.to_owned(),
        messages,
        bytes: all.then_some(state.bytes),
        first_at: if all && held {
            utc(state.first_timestamp)
        } else {
            None
        },
        last_at: if all && held {
            utc(state.last_timestamp)
        } else {
            None
        },
        subjects_total,
        subjects: counts,
    };
    let consumers = if all {
        let mut consumers: Vec<Consumer> = stream
            .consumers()
            .try_filter(|c| std::future::ready(c.config.durable_name.is_some()))
            .map_ok(|c| {
                let mut filters = c.config.filter_subjects.clone();
                if !c.config.filter_subject.is_empty() {
                    filters.insert(0, c.config.filter_subject.clone());
                }
                Consumer {
                    name: c.name.clone(),
                    filters,
                    pending: c.num_pending,
                    ack_pending: u64::try_from(c.num_ack_pending).unwrap_or(u64::MAX),
                    redelivered: u64::try_from(c.num_redelivered).unwrap_or(u64::MAX),
                    last_active: c.delivered.last_active.and_then(utc),
                }
            })
            .try_collect()
            .await
            .map_err(|e| format!("the stream `{name}`'s consumers could not be read: {e}"))?;
        consumers.sort_by(|a, b| a.name.cmp(&b.name));
        Some(consumers)
    } else {
        None
    };
    Ok((stream_status, consumers))
}

/// A status of the relay, the outbox and the stream under `filter`. The
/// database's failure is an error; the broker's is an answer without its
/// part, saying why.
pub(crate) async fn status(
    pool: &PgPool,
    jetstream: &Context,
    settings: &Settings,
    filter: &Filter,
) -> Result<Status, Error> {
    let subject = filter.resolve(settings)?;
    // `Filter::subject("<prefix>.>")` is every subject, as `Filter::all()` is.
    let all = subject == crate::stream::subjects(settings);
    let mut status = crate::relay::bounded(
        settings.database_timeout,
        database(pool, &settings.subject_prefix, &subject, all),
    )
    .await?;
    match tokio::time::timeout(
        BROKER_WAIT,
        broker(jetstream, &settings.stream, &subject, all),
    )
    .await
    .unwrap_or_else(|_| Err(format!("the broker did not answer within {BROKER_WAIT:?}")))
    {
        Ok((stream, consumers)) => {
            status.stream = Some(stream);
            status.consumers = consumers;
        }
        Err(why) => status.broker_error = Some(why),
    }
    Ok(status)
}

/// The last `limit` messages under `filter`, oldest first, read through
/// ordered consumers of the broker's own, each deleted once read — nothing
/// left behind — and each starting further back until it has them or the
/// stream's start; within `RECENT_WAIT` in all.
pub(crate) async fn recent(
    jetstream: &Context,
    settings: &Settings,
    filter: &Filter,
    limit: u32,
) -> Result<Recent, Error> {
    let subject = filter.resolve(settings)?;
    let limit = usize::try_from(limit.min(RECENT_MOST)).unwrap_or(usize::MAX);
    let read = async {
        let messages = if limit == 0 {
            Vec::new()
        } else {
            last(jetstream, &settings.stream, &subject, limit).await?
        };
        Ok(Recent {
            contract: CONTRACT,
            filter: subject.clone(),
            messages,
        })
    };
    tokio::time::timeout(RECENT_WAIT, read)
        .await
        .unwrap_or_else(|_| {
            Err(unread(
                &settings.stream,
                &format!("no answer within {RECENT_WAIT:?}"),
            ))
        })
}

/// How long `recent` may take, every round of it.
const RECENT_WAIT: Duration = Duration::from_secs(15);

fn unread(stream: &str, why: &dyn std::fmt::Display) -> Error {
    Error::Broker {
        reason: format!("the stream `{stream}` could not be read: {why}"),
        transient: true,
    }
}

/// The last `limit` messages of `name` under `subject`, oldest first.
async fn last(
    jetstream: &Context,
    name: &str,
    subject: &str,
    limit: usize,
) -> Result<Vec<Message>, Error> {
    let stream = jetstream
        .get_stream(name)
        .await
        .map_err(|e| unread(name, &e))?;
    let state = stream.cached_info().state.clone();
    if state.messages == 0 {
        return Ok(Vec::new());
    }
    let mut back = u64::try_from(limit).unwrap_or(u64::MAX).saturating_mul(4);
    loop {
        let start = state
            .last_sequence
            .saturating_sub(back)
            .max(state.first_sequence);
        let consumer = stream
            .create_consumer(OrderedConfig {
                filter_subject: subject.to_owned(),
                deliver_policy: DeliverPolicy::ByStartSequence {
                    start_sequence: start,
                },
                ..Default::default()
            })
            .await
            .map_err(|e| unread(name, &e))?;
        let consumer_name = consumer.cached_info().name.clone();
        let read = round(consumer, name, state.last_sequence, limit).await;
        // Its round over, the consumer goes, whatever the round came to.
        let _ = stream.delete_consumer(&consumer_name).await;
        let (found, matched) = read?;
        if matched >= limit || start == state.first_sequence {
            return Ok(found.into());
        }
        back = back.saturating_mul(4);
    }
}

/// One round of `last`: the last `limit` messages `consumer` delivers up to
/// the stream's `last_sequence`, and how many it delivered in all.
async fn round(
    mut consumer: async_nats::jetstream::consumer::Consumer<OrderedConfig>,
    name: &str,
    last_sequence: u64,
    limit: usize,
) -> Result<(std::collections::VecDeque<Message>, usize), Error> {
    // The last `limit` read so far: what is read before them is let go.
    let mut found = std::collections::VecDeque::with_capacity(limit);
    let mut matched = 0_usize;
    let pending = consumer
        .info()
        .await
        .map_err(|e| unread(name, &e))?
        .num_pending;
    if pending == 0 {
        return Ok((found, matched));
    }
    let mut messages = consumer.messages().await.map_err(|e| unread(name, &e))?;
    while let Some(message) = tokio::time::timeout(BROKER_WAIT, messages.next())
        .await
        .map_err(|_| unread(name, &"no message within the wait"))?
    {
        let message = message.map_err(|e| unread(name, &e))?;
        let info = message.info().map_err(|e| unread(name, &e))?;
        let (sequence, time, left) = (info.stream_sequence, utc(info.published), info.pending);
        if sequence > last_sequence {
            break;
        }
        let headers = message
            .headers
            .iter()
            .flat_map(|headers| headers.iter())
            .filter_map(|(name, values)| {
                values
                    .first()
                    .map(|v| (name.to_string(), v.as_str().to_owned()))
            })
            .collect();
        matched += 1;
        if found.len() == limit {
            found.pop_front();
        }
        found.push_back(Message {
            sequence,
            subject: message.subject.to_string(),
            time: time.unwrap_or_default(),
            headers,
            body: String::from_utf8_lossy(&message.payload).into_owned(),
        });
        if left == 0 || sequence == last_sequence {
            break;
        }
    }
    Ok((found, matched))
}

#[cfg(test)]
#[path = "status_tests.rs"]
pub(crate) mod tests;
