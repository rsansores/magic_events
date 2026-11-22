//! The relay: one process at a time, on an advisory lock, publishes what
//! committed, in id order within each stream, and deletes what the stream
//! acknowledged.
//!
//! Each stream the leader's settings declare has a lane of its own — its
//! passes, its backoff, its failures — and the lanes run side by side: a
//! stream whose broker fails or is slow waits alone. A lane reads through
//! the pool; only what must be fenced — the settling of a pass — takes a
//! turn on the lock's connection.
//!
//! A pass reads a batch of one stream's pending rows, publishes them outside
//! any transaction — up to `in_flight` sent and not acknowledged yet, the
//! acknowledgements awaited in order — then, in one transaction fenced on
//! the lock, deletes the acknowledged prefix and counts it. The pass stops
//! at the first message the broker did not take:
//!
//! - a transient failure (the broker away, slow, or its stream unable to
//!   take anything) keeps the rows, records the failure, and the relay waits
//!   out a backoff;
//! - a permanent one (the broker refuses that message for what it is) counts
//!   an attempt on that row, and after the third sets it aside — parked — so
//!   it holds back nothing behind it.
//!
//! A message keeps its id however often it is sent (`Nats-Msg-Id`), so the
//! stream drops one it already holds within its duplicate window: a relay
//! that died after the stream took a message and before it deleted the row
//! publishes it again, and the stream keeps one.

use crate::envelope::Pending;
use crate::publish::{Ack, Failure, Publish};
use crate::settings::{Declared, Settings};
use crate::Error;
use sqlx::{Connection, PgConnection, PgPool, Postgres, Transaction};
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Mutex};

/// The advisory lock only the running relay holds ("EVNTRLAY").
pub(crate) const LOCK_KEY: i64 = 0x4556_4e54_524c_4159;

/// Times the broker refuses a message before it is parked.
pub(crate) const ATTEMPTS: i32 = 3;

/// Days of published counts kept; the status reads the last 30.
const PUBLISHED_KEPT_DAYS: i32 = 90;

/// How often the leader forgets the published counts past those days.
const PRUNE_EVERY: Duration = Duration::from_secs(60 * 60);

/// Whether the server process the statement runs on holds the advisory lock
/// `$1`: a connection through a pooler in transaction mode may run each
/// transaction on another, which does not.
const HELD_HERE: &str = "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' \
       AND pid = pg_backend_pid() AND granted AND objsubid = 1 \
       AND classid = ($1::bigint >> 32)::oid AND objid = ($1::bigint & 4294967295)::oid)";

/// How the server ends the lock's session — and lets the lock go — when the
/// relay is gone, rather than keep it for the hours its defaults take to
/// notice: keepalive probes after 10 s of silence, every 5 s, three missed;
/// anything sent unacknowledged for 20 s (both ignored over a Unix socket);
/// a statement that runs past the relay's own wait (`$1`, in ms) cancelled;
/// and, while one runs, the client looked for every 5 s.
const SESSION: &str = "SELECT set_config('tcp_keepalives_idle', '10', false), \
       set_config('tcp_keepalives_interval', '5', false), \
       set_config('tcp_keepalives_count', '3', false), \
       set_config('tcp_user_timeout', '20000', false), \
       set_config('statement_timeout', $1, false), \
       set_config('client_connection_check_interval', '5000', false)";

/// The error of a statement the database did not answer within `wait`.
fn unanswered(wait: Duration) -> Error {
    Error::Database(sqlx::Error::Io(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!("the database did not answer within {wait:?}"),
    )))
}

/// A transaction of `pool` as the settings' role, when they name one: for
/// the crate's own work outside the relay's session. `SET LOCAL` — the role
/// is let go at commit or rollback, so the connection goes back to the pool
/// as it came.
pub(crate) async fn begin(
    pool: &PgPool,
    settings: &Settings,
) -> Result<Transaction<'static, Postgres>, Error> {
    let mut tx = pool.begin().await?;
    if let Some(role) = &settings.role {
        sqlx::query("SELECT set_config('role', $1, true)")
            .bind(role)
            .execute(&mut *tx)
            .await?;
    }
    Ok(tx)
}

/// `work`, or an error once `wait` passed without its answer.
pub(crate) async fn bounded<T>(
    wait: Duration,
    work: impl Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    tokio::time::timeout(wait, work)
        .await
        .unwrap_or_else(|_| Err(unanswered(wait)))
}

/// Proof that this process runs the relay: a connection holding the
/// advisory lock. Drop it, or lose the connection, and the lock is free.
pub(crate) struct Lock {
    pub(crate) conn: PgConnection,
    /// How long a statement of the relay's may take before the relay counts
    /// the database as away.
    pub(crate) wait: Duration,
}

impl Lock {
    /// The lock, on a connection of its own — opened with `pool`'s options,
    /// not taken from it: it lives as long as the lock, and closes, letting go
    /// of the lock, when it is dropped — or `None` when another session holds
    /// it.
    /// The session runs as the settings' role, when they name one.
    pub(crate) async fn take(pool: &PgPool, settings: &Settings) -> Result<Option<Lock>, Error> {
        let wait = settings.database_timeout;
        bounded(wait, async {
            let mut conn = PgConnection::connect_with(&pool.connect_options()).await?;
            sqlx::query(SESSION)
                .bind(wait.as_millis().to_string())
                .execute(&mut conn)
                .await?;
            if let Some(role) = &settings.role {
                sqlx::query("SELECT set_config('role', $1, false)")
                    .bind(role)
                    .execute(&mut conn)
                    .await?;
            }
            let taken: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
                .bind(LOCK_KEY)
                .fetch_one(&mut conn)
                .await?;
            Ok(taken.then_some(Lock { conn, wait }))
        })
        .await
    }

    /// Lets the lock go now, rather than when the connection's end reaches
    /// the database.
    pub(crate) async fn release(mut self) {
        let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(LOCK_KEY)
            .execute(&mut self.conn)
            .await;
    }

    /// A connection that still answers, in time, still holds its session's
    /// lock.
    pub(crate) async fn still_held(&mut self) -> bool {
        let wait = self.wait;
        let held = sqlx::query_scalar::<_, bool>(HELD_HERE)
            .bind(LOCK_KEY)
            .fetch_one(&mut self.conn);
        matches!(tokio::time::timeout(wait, held).await, Ok(Ok(true)))
    }

    /// A transaction on the lock's own connection: it commits only while this
    /// process holds the lock, so a relay that lost it cannot write over the
    /// one that took it.
    pub(crate) async fn fenced(&mut self) -> Result<Transaction<'_, Postgres>, Error> {
        let mut tx = self.conn.begin().await?;
        let here: bool = sqlx::query_scalar(HELD_HERE)
            .bind(LOCK_KEY)
            .fetch_one(&mut *tx)
            .await?;
        if !here {
            return Err(Error::Misuse(
                "the relay's lock is not held by the server process its transaction runs on: \
                 a pooler in transaction mode hands each transaction to any server process. \
                 The relay needs session pooling, or a direct connection"
                    .to_owned(),
            ));
        }
        Ok(tx)
    }
}

/// What publishing a batch came to: how many rows, from the first, the
/// stream acknowledged, and the failure that stopped it, if one did — the
/// row right after them.
#[derive(Debug, Default)]
pub(crate) struct Published {
    pub(crate) acked: usize,
    pub(crate) failure: Option<Failure>,
}

/// How a relay's leadership ended.
pub(crate) enum Led {
    /// It was told to stop: the lock back, to let go of, unless it was lost
    /// meanwhile.
    Stopped(Option<Lock>),
    /// The lock was lost, or the streams could not be declared: stand by.
    Lost,
}

/// The relay of one process: its database, its broker and its settings.
pub(crate) struct Relay {
    pub(crate) pool: PgPool,
    pub(crate) publish: Arc<dyn Publish>,
    pub(crate) settings: Settings,
}

impl Relay {
    /// Runs until `shutdown` resolves: stands by while another process holds
    /// the lock, and leads while this one does. A pass under way when it
    /// resolves is finished — what it published deleted — and the lock let
    /// go, so the next relay neither waits for it nor sends anything again.
    pub(crate) async fn run(&self, shutdown: impl Future<Output = ()>) {
        tokio::pin!(shutdown);
        loop {
            let lock = tokio::select! {
                () = &mut shutdown => return,
                lock = self.lock() => lock,
            };
            // Once `shutdown` resolved it is not asked again: `lead` says so.
            match self.lead(lock, &mut shutdown).await {
                Led::Stopped(lock) => {
                    if let Some(lock) = lock {
                        lock.release().await;
                    }
                    return;
                }
                Led::Lost => {}
            }
        }
    }

    /// The lock, once this process gets it.
    async fn lock(&self) -> Lock {
        loop {
            match Lock::take(&self.pool, &self.settings).await {
                Ok(Some(lock)) => return lock,
                Ok(None) => tracing::debug!("events: another process runs the relay — standing by"),
                Err(e) => tracing::warn!("events: the relay's lock could not be asked for: {e}"),
            }
            tokio::time::sleep(self.settings.standby).await;
        }
    }

    /// The relay, for as long as this process holds `lock`, until
    /// `shutdown` resolves — every lane stopping between passes, never
    /// inside one — or the lock is lost. It first declares its streams: the
    /// lanes follow this process's settings, whatever another's said.
    async fn lead<F: Future<Output = ()>>(
        &self,
        mut lock: Lock,
        shutdown: &mut std::pin::Pin<&mut F>,
    ) -> Led {
        if let Err(e) = self.declare(&mut lock).await {
            tracing::error!("events: the relay's streams could not be declared — standing by: {e}");
            return Led::Lost;
        }
        tracing::info!("events: this process runs the relay");
        let lock = Mutex::new(lock);
        // Set once the relay stops leading — stopped, or its lock lost — so
        // every lane ends after the pass it is in.
        let (stop, _) = watch::channel(false);
        let (stopped, lost) = {
            let led = futures_util::future::join3(
                futures_util::future::join_all(
                    self.settings
                        .declared()
                        .map(|declared| self.lane(&lock, declared, &stop)),
                ),
                self.holding(&lock, &stop),
                self.pruning(&lock, &stop),
            );
            tokio::pin!(led);
            tokio::select! {
                biased;
                () = &mut *shutdown => {
                    stop.send_replace(true);
                    (true, (&mut led).await.1)
                }
                (_, lost, ()) = &mut led => (false, lost),
            }
        };
        match (stopped, lost) {
            (true, false) => Led::Stopped(Some(lock.into_inner())),
            (true, true) => Led::Stopped(None),
            (false, _) => Led::Lost,
        }
    }

    /// Writes, fenced, what this process declares: a relay row for each of
    /// its streams — its JetStream name, its prefix and its `max_body`,
    /// which `emit` holds bodies to — and every other stream undeclared, so
    /// `emit` refuses what nothing publishes.
    pub(crate) async fn declare(&self, lock: &mut Lock) -> Result<(), Error> {
        bounded(lock.wait, async {
            let mut tx = lock.fenced().await?;
            let mut keys = Vec::new();
            for declared in self.settings.declared() {
                let stream = declared.stream;
                sqlx::query(
                    "INSERT INTO magic_events.relay \
                            (stream, name, subject_prefix, max_body, declared) \
                     VALUES ($1, $2, $3, $4, true) \
                     ON CONFLICT (stream) DO UPDATE SET name = excluded.name, \
                            subject_prefix = excluded.subject_prefix, \
                            max_body = excluded.max_body, declared = true",
                )
                .bind(declared.key)
                .bind(&stream.name)
                .bind(&stream.subject_prefix)
                .bind(i32::try_from(stream.max_body).unwrap_or(i32::MAX))
                .execute(&mut *tx)
                .await?;
                keys.push(declared.key.to_owned());
            }
            sqlx::query(
                "UPDATE magic_events.relay SET declared = false \
                  WHERE declared AND stream <> ALL($1)",
            )
            .bind(&keys)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(())
        })
        .await
    }

    /// One stream's passes, until `stop` is set.
    async fn lane(&self, lock: &Mutex<Lock>, declared: Declared<'_>, stop: &watch::Sender<bool>) {
        let mut stopping = stop.subscribe();
        let name = &declared.stream.name;
        let (first, max) = self.settings.backoff;
        let mut backoff = first;
        while !*stopping.borrow() {
            let wait = match self.pass(lock, declared, backoff).await {
                Ok(0) => {
                    backoff = first;
                    self.settings.idle
                }
                Ok(_) => {
                    backoff = first;
                    Duration::ZERO
                }
                Err(e) => {
                    if matches!(e, Error::Misuse(_)) {
                        tracing::error!(stream = %name, "events: a pass failed, and runs again in {backoff:?}: {e}");
                    } else {
                        tracing::warn!(stream = %name, "events: a pass failed, and runs again in {backoff:?}: {e}");
                    }
                    let wait = backoff;
                    backoff = backoff.saturating_mul(2).min(max);
                    wait
                }
            };
            tokio::select! {
                _ = stopping.wait_for(|stop| *stop) => {}
                () = tokio::time::sleep(wait) => {}
            }
        }
    }

    /// Asks every `idle` whether the lock is still held, until `stop` is
    /// set: answers whether it was lost, and then sets `stop`, so every lane
    /// ends.
    async fn holding(&self, lock: &Mutex<Lock>, stop: &watch::Sender<bool>) -> bool {
        let mut stopping = stop.subscribe();
        loop {
            if *stopping.borrow() {
                return false;
            }
            if !lock.lock().await.still_held().await {
                stop.send_replace(true);
                tracing::error!("events: the relay's lock is gone, or its database does not answer — standing by");
                return true;
            }
            tokio::select! {
                _ = stopping.wait_for(|stop| *stop) => return false,
                () = tokio::time::sleep(self.settings.idle) => {}
            }
        }
    }

    /// Forgets the published counts past `PUBLISHED_KEPT_DAYS` now and every
    /// `PRUNE_EVERY`, until `stop` is set.
    async fn pruning(&self, lock: &Mutex<Lock>, stop: &watch::Sender<bool>) {
        let mut stopping = stop.subscribe();
        loop {
            if let Err(e) = self.prune(&mut *lock.lock().await).await {
                tracing::warn!("events: pruning the published counts failed: {e}");
            }
            tokio::select! {
                _ = stopping.wait_for(|stop| *stop) => return,
                () = tokio::time::sleep(PRUNE_EVERY) => {}
            }
        }
    }

    /// Forgets the published counts older than `PUBLISHED_KEPT_DAYS`.
    async fn prune(&self, lock: &mut Lock) -> Result<(), Error> {
        bounded(lock.wait, async {
            sqlx::query(
                "DELETE FROM magic_events.published \
                  WHERE day < (now() AT TIME ZONE 'UTC')::date - $1",
            )
            .bind(PUBLISHED_KEPT_DAYS)
            .execute(&mut lock.conn)
            .await?;
            Ok(())
        })
        .await
    }

    /// One pass of the stream `declared`, as the relay holding `lock` —
    /// shared with the other streams' lanes, which take turns on its
    /// connection to settle: answers how many rows it published or parked.
    /// `backoff` is how long the relay waits if the broker fails it, which
    /// the status shows.
    pub(crate) async fn pass(
        &self,
        lock: &Mutex<Lock>,
        declared: Declared<'_>,
        backoff: Duration,
    ) -> Result<usize, Error> {
        let mut rows = self.pending(declared).await?;
        if rows.is_empty() {
            return Ok(0);
        }
        let published = self.send(declared, &mut rows).await;
        self.settle(&mut *lock.lock().await, declared, &rows, published, backoff)
            .await
    }

    /// The next batch of the stream's rows to publish, in order, read
    /// through the pool: a read needs no fence — what a relay that lost its
    /// lock publishes, it cannot settle, and the stream drops when the next
    /// relay sends it again.
    pub(crate) async fn pending(&self, declared: Declared<'_>) -> Result<Vec<Pending>, Error> {
        bounded(self.settings.database_timeout, async {
            let mut tx = begin(&self.pool, &self.settings).await?;
            let rows = sqlx::query_as::<_, Pending>(
                "SELECT id, event_id, subject, type, headers, body, occurred_at \
                   FROM magic_events.outbox WHERE stream = $1 AND parked_at IS NULL \
                  ORDER BY id LIMIT $2",
            )
            .bind(declared.key)
            .bind(i64::from(self.settings.batch))
            .fetch_all(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(rows)
        })
        .await
    }

    /// Publishes `rows` in order, up to `in_flight` of them sent and not
    /// acknowledged at once, each within `send_timeout`; stops sending at the
    /// first failure, and still awaits what was sent before it. Each row's
    /// body goes with its message, and is gone from the row after.
    pub(crate) async fn send(&self, declared: Declared<'_>, rows: &mut [Pending]) -> Published {
        let in_flight = usize::try_from(self.settings.in_flight).unwrap_or(usize::MAX);
        let timeout = self.settings.send_timeout;
        let late = || Failure::transient(format!("no acknowledgement within {timeout:?}"));
        let mut waiting: VecDeque<(usize, Ack)> = VecDeque::new();
        let mut published = Published::default();
        let mut next = 0;
        loop {
            while published.failure.is_none() && next < rows.len() && waiting.len() < in_flight {
                let row = &mut rows[next];
                let message = row.message(&declared.stream.subject_prefix, &self.settings.source);
                let body = std::mem::take(&mut row.body);
                let sent = tokio::time::timeout(timeout, self.publish.send(message, body))
                    .await
                    .unwrap_or_else(|_| Err(late()));
                match sent {
                    Ok(ack) => waiting.push_back((next, ack)),
                    Err(failure) => published.failure = Some(failure),
                }
                next += 1;
            }
            let Some((index, ack)) = waiting.pop_front() else {
                return published;
            };
            match tokio::time::timeout(timeout, ack)
                .await
                .unwrap_or_else(|_| Err(late()))
            {
                Ok(()) => published.acked = index + 1,
                Err(failure) => {
                    published.acked = index;
                    published.failure = Some(failure);
                    return published;
                }
            }
        }
    }

    /// Records what `published` came to, in one transaction fenced on the
    /// lock: the acknowledged rows deleted and counted, and the failure that
    /// stopped the batch — the relay's, or the row's. Answers how many rows
    /// left the queue, or the transient failure as an error.
    pub(crate) async fn settle(
        &self,
        lock: &mut Lock,
        declared: Declared<'_>,
        rows: &[Pending],
        published: Published,
        backoff: Duration,
    ) -> Result<usize, Error> {
        let wait = lock.wait;
        bounded(wait, self.record(lock, declared, rows, published, backoff)).await
    }

    /// `settle`, with no bound on how long the database takes.
    async fn record(
        &self,
        lock: &mut Lock,
        declared: Declared<'_>,
        rows: &[Pending],
        published: Published,
        backoff: Duration,
    ) -> Result<usize, Error> {
        let key = declared.key;
        let acked: Vec<i64> = rows[..published.acked].iter().map(|r| r.id).collect();
        let mut tx = lock.fenced().await?;
        if !acked.is_empty() {
            sqlx::query(
                "WITH gone AS (DELETE FROM magic_events.outbox WHERE id = ANY($1) RETURNING type) \
                 INSERT INTO magic_events.published AS p (stream, day, type, count) \
                 SELECT $2, (now() AT TIME ZONE 'UTC')::date, type, count(*) FROM gone GROUP BY type \
                 ON CONFLICT (stream, day, type) DO UPDATE SET count = p.count + excluded.count",
            )
            .bind(&acked)
            .bind(key)
            .execute(&mut *tx)
            .await?;
        }
        let mut done = acked.len();
        let outcome = match published.failure {
            None => {
                sqlx::query(
                    "UPDATE magic_events.relay SET last_published_at = now(), failures = 0, \
                       last_error = NULL, retry_after = NULL WHERE stream = $1",
                )
                .bind(key)
                .execute(&mut *tx)
                .await?;
                Ok(())
            }
            Some(failure) if failure.transient => {
                sqlx::query(
                    "UPDATE magic_events.relay SET failures = failures + 1, last_error = $1, \
                       retry_after = now() + make_interval(secs => $2), \
                       last_published_at = CASE WHEN $3 THEN now() ELSE last_published_at END \
                     WHERE stream = $4",
                )
                .bind(&failure.reason)
                .bind(backoff.as_secs_f64())
                .bind(done > 0)
                .bind(key)
                .execute(&mut *tx)
                .await?;
                Err(Error::Broker {
                    reason: failure.reason,
                    transient: true,
                })
            }
            Some(failure) => {
                let row = &rows[published.acked];
                // `None`: the row was deleted since it was read — by hand,
                // say — and there is nothing left to count against.
                let parked: bool = sqlx::query_scalar::<_, bool>(
                    "UPDATE magic_events.outbox SET attempts = attempts + 1, last_error = $2, \
                       parked_at = CASE WHEN attempts + 1 >= $3 THEN now() END \
                     WHERE id = $1 RETURNING parked_at IS NOT NULL",
                )
                .bind(row.id)
                .bind(&failure.reason)
                .bind(ATTEMPTS)
                .fetch_optional(&mut *tx)
                .await?
                .unwrap_or(false);
                if parked {
                    tracing::error!(
                        stream = %declared.stream.name,
                        id = row.id,
                        event = %row.event_id,
                        subject = %row.subject,
                        r#type = %row.ty,
                        error = %failure.reason,
                        "events: the broker refuses a message for good — parked; `Events::unpark` sends it again"
                    );
                    done += 1;
                } else {
                    tracing::warn!(
                        stream = %declared.stream.name,
                        id = row.id,
                        error = %failure.reason,
                        "events: the broker refuses a message — it is tried again"
                    );
                }
                sqlx::query(
                    "UPDATE magic_events.relay SET last_published_at = \
                       CASE WHEN $1 THEN now() ELSE last_published_at END WHERE stream = $2",
                )
                .bind(published.acked > 0)
                .bind(key)
                .execute(&mut *tx)
                .await?;
                Ok(())
            }
        };
        tx.commit().await?;
        // A refusal not parked yet is tried again at once: it is progress
        // all the same, not an idle outbox.
        outcome.map(|()| done.max(usize::from(published.acked < rows.len())))
    }
}

#[cfg(test)]
#[path = "relay_tests.rs"]
mod tests;
