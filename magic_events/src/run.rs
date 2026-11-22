//! `Events`: the relay, run for the host in a task of its own.

use crate::publish::Publish;
use crate::relay::{Lock, Relay};
use crate::settings::HEADROOM;
use crate::status::{self, Filter, Recent, Status};
use crate::{schema, stream, Error, Settings};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// The relay, started once per process: it publishes what committed, in a
/// task of its own. Only one process at a time publishes; the others stand
/// by, and one of them takes over when it stops or dies.
///
/// ```no_run
/// use magic_events::{Events, Settings};
///
/// # async fn main_(pool: sqlx::PgPool, nats: async_nats::Client) -> Result<(), magic_events::Error> {
/// let jetstream = async_nats::jetstream::new(nats);
/// let events = Events::start(pool, jetstream, Settings::new("SHOP_EVENTS", "shop")).await?;
/// // … the host runs, and emits …
/// events.stop().await;
/// # Ok(())
/// # }
/// ```
///
/// Dropped without [`Events::stop`], the relay stops as `stop` stops it, in
/// a task nobody waits for: the process may end before the pass under way
/// does, as after a crash — whatever it published and did not delete yet is
/// published again, and the stream drops it as a duplicate.
pub struct Events {
    relay: Arc<Relay>,
    jetstream: async_nats::jetstream::Context,
    stop: watch::Sender<bool>,
    /// The relay's task, until it is stopped: behind a lock so `stop` takes
    /// `&self`, and a host that shares its `Events` can still stop it.
    task: std::sync::Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for Events {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Events")
            .field("settings", &self.relay.settings)
            .finish_non_exhaustive()
    }
}

impl Events {
    /// Starts the relay: checks the settings and the database's schema,
    /// creates or checks every stream they declare
    /// ([`StreamSettings::manage_stream`](crate::StreamSettings::manage_stream)),
    /// declares the streams when no other relay leads — [`emit`](crate::emit)
    /// takes their events from then on — or checks they agree with the
    /// leading relay's, and spawns the relay on the current Tokio runtime.
    /// The relay that leads publishes the streams its own settings declare:
    /// give every process the same.
    ///
    /// The relay holds a connection of its own while it leads, for its lock:
    /// opened with `pool`'s options, not taken from it — one more than the
    /// pool's `max_connections`, on which the pool's `after_connect` does not
    /// run, so it works with the role it logs in as. It must reach one server
    /// process for as long as it lives — a direct connection, or a pooler in
    /// session mode; one in transaction mode is refused here.
    ///
    /// # Errors
    ///
    /// [`Error::Misuse`] for settings the relay cannot run with — among them
    /// a stream key declared twice, two keys of one stream, subject
    /// prefixes that overlap, or a stream the leading relay declares as
    /// another JetStream stream or under another prefix — a schema not at
    /// this build's version
    /// ([`Events::install`]), a stream that does not take the relay's
    /// subjects or that the broker refuses as the settings say it, or a
    /// `max_body` the broker's `max_payload` cannot carry with 4 KiB of
    /// headers; [`Error::Broker`] when the broker does not answer;
    /// [`Error::Database`] when the database fails.
    pub async fn start(
        pool: PgPool,
        jetstream: async_nats::jetstream::Context,
        settings: Settings,
    ) -> Result<Self, Error> {
        Self::start_with(pool, jetstream.clone(), Arc::new(jetstream), settings).await
    }

    /// `start`, publishing through `publish`: JetStream, or in tests one that
    /// fails on purpose.
    pub(crate) async fn start_with(
        pool: PgPool,
        jetstream: async_nats::jetstream::Context,
        publish: Arc<dyn Publish>,
        settings: Settings,
    ) -> Result<Self, Error> {
        let relay = Self::prepare(pool, publish, settings).await?;
        let max_payload = jetstream.client().server_info().max_payload;
        for declared in relay.settings.declared() {
            let max_body = declared.stream.max_body;
            let needed = u64::from(max_body) + u64::from(HEADROOM);
            if max_payload > 0 && u64::try_from(max_payload).unwrap_or(u64::MAX) < needed {
                return Err(Error::Misuse(format!(
                    "the broker takes messages of {max_payload} bytes at most (`max_payload`), \
                     and a body of `max_body` {max_body} bytes needs {needed} with its headers \
                     (the stream `{}`)",
                    declared.stream.name
                )));
            }
        }
        for declared in relay.settings.declared() {
            stream::ensure(&jetstream, declared.stream).await?;
        }
        Self::probe(&relay).await?;
        Ok(Self::spawn(relay, jetstream))
    }

    /// The relay, its settings and the database's schema checked, not
    /// running yet.
    pub(crate) async fn prepare(
        pool: PgPool,
        publish: Arc<dyn Publish>,
        settings: Settings,
    ) -> Result<Relay, Error> {
        settings.check()?;
        let mut tx = crate::relay::begin(&pool, &settings).await?;
        schema::check(&mut tx).await?;
        tx.commit().await?;
        Ok(Relay {
            pool,
            publish,
            settings,
        })
    }

    /// Refuses a database the relay's lock cannot be held on — a pooler in
    /// transaction mode, which may hand each transaction to another server
    /// process — by taking the lock, when no other process holds it, and
    /// declaring the streams in a fenced transaction, so `emit` takes their
    /// events from now on. When another process holds it, refuses settings
    /// that say a stream it declares is another JetStream stream, or under
    /// another prefix: two relays would publish one key's events to two
    /// places. Either way the lock is let go: the relay takes it again as it
    /// starts.
    async fn probe(relay: &Relay) -> Result<(), Error> {
        let Some(mut lock) = Lock::take(&relay.pool, &relay.settings).await? else {
            return Self::agrees(relay).await;
        };
        let declared = relay.declare(&mut lock).await;
        lock.release().await;
        declared
    }

    /// Refuses settings that declare a stream otherwise than the leading
    /// relay does.
    async fn agrees(relay: &Relay) -> Result<(), Error> {
        let mut tx = crate::relay::begin(&relay.pool, &relay.settings).await?;
        let leading: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT stream, name, subject_prefix FROM magic_events.relay \
              WHERE declared AND name IS NOT NULL AND subject_prefix IS NOT NULL",
        )
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        for (key, name, prefix) in leading {
            let Some(ours) = relay.settings.find(&key) else {
                continue;
            };
            if ours.stream.name != name || ours.stream.subject_prefix != prefix {
                return Err(Error::Misuse(format!(
                    "the leading relay declares {} as `{name}` under `{prefix}`, and these \
                     settings as `{}` under `{}`: two relays would publish its events to two \
                     places. A stream's name and prefix change with every relay stopped — \
                     the first to start declares them anew",
                    if key.is_empty() {
                        "the main stream".to_owned()
                    } else {
                        format!("the stream `{key}`")
                    },
                    ours.stream.name,
                    ours.stream.subject_prefix
                )));
            }
        }
        Ok(())
    }

    /// `relay`, running in a task of its own.
    pub(crate) fn spawn(relay: Relay, jetstream: async_nats::jetstream::Context) -> Self {
        let relay = Arc::new(relay);
        let (stop, mut stopped) = watch::channel(false);
        let task = tokio::spawn({
            let relay = relay.clone();
            async move {
                // Stopped, or the `Events` dropped.
                let shutdown = async move {
                    let _ = stopped.wait_for(|stop| *stop).await;
                };
                relay.run(shutdown).await;
            }
        });
        Events {
            relay,
            jetstream,
            stop,
            task: std::sync::Mutex::new(Some(task)),
        }
    }

    /// `relay`, not running: for tests that run each pass themselves.
    #[cfg(test)]
    pub(crate) fn idle(relay: Relay, jetstream: async_nats::jetstream::Context) -> Self {
        Events {
            relay: Arc::new(relay),
            jetstream,
            stop: watch::channel(false).0,
            task: std::sync::Mutex::new(None),
        }
    }

    /// The relay, for a test that runs its steps one by one.
    #[cfg(test)]
    pub(crate) fn relay(&self) -> &Relay {
        &self.relay
    }

    /// Stops the relay at once, wherever it is, as a crash would.
    #[cfg(test)]
    pub(crate) fn abort(self) {
        if let Some(task) = self.task() {
            task.abort();
        }
    }

    /// The relay's task, taken: `None` once stopped, or never started.
    fn task(&self) -> Option<JoinHandle<()>> {
        self.task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    /// Stops the relay and waits for it: a pass under way is finished —
    /// what it published deleted, within its own timeouts — and the lock let
    /// go, so a process standing by takes it within its `standby`, and sends
    /// nothing twice. It takes `&self`, so an `Events` shared with request
    /// handlers — behind an `Arc`, for [`Events::status`] — is stopped where
    /// the host shuts down; stopping it again, or from two places, waits for
    /// nothing more.
    pub async fn stop(&self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task() {
            let _ = task.await;
        }
    }

    /// Brings the database's schema `magic_events` to this build's version,
    /// for a host with no migrations of its own: in one transaction, and one
    /// process at a time, every step the database lacks
    /// ([`migration`](crate::migration)). Installing again is harmless.
    ///
    /// The tables are the host's to grant: a role that emits needs `USAGE`
    /// on the schema, `INSERT` on `magic_events.outbox`, `USAGE` on its
    /// sequence (`magic_events.outbox_id_seq`) and `SELECT` on
    /// `magic_events.relay`; the relay's role needs `SELECT`, `UPDATE` and
    /// `DELETE` on the outbox, `SELECT`, `INSERT` and `UPDATE` on
    /// `magic_events.relay` and on `magic_events.published`, and `SELECT` on
    /// `magic_events.version` — and, to [retire](Events::retire) a stream,
    /// `DELETE` on `magic_events.relay` and `magic_events.published`.
    ///
    /// # Errors
    ///
    /// [`Error::Misuse`] when the schema is at a later version than this
    /// build's, or a schema of that name is not the crate's;
    /// [`Error::Database`] when the database fails. Whatever fails, nothing
    /// is installed.
    pub async fn install(pool: &PgPool) -> Result<(), Error> {
        let mut tx = pool.begin().await?;
        schema::install(&mut tx).await?;
        tx.commit().await?;
        Ok(())
    }

    /// One pass of every declared stream now, one after the other, as the
    /// relay — its streams declared first, as a relay that takes the lock
    /// declares them — for a host's tests or a tool: how many rows they published or
    /// set aside, together, or `None` when a relay — this process's own, or
    /// another's — holds the lock. A stream whose pass fails stops no other
    /// stream's.
    ///
    /// # Errors
    ///
    /// What stopped the first pass that failed — the other streams' passes
    /// ran all the same: [`Error::Broker`] when the broker failed it,
    /// [`Error::Database`] when the database did.
    pub async fn pass_now(&self) -> Result<Option<usize>, Error> {
        let Some(lock) = Lock::take(&self.relay.pool, &self.relay.settings).await? else {
            return Ok(None);
        };
        let mut lock = lock;
        if let Err(e) = self.relay.declare(&mut lock).await {
            lock.release().await;
            return Err(e);
        }
        let lock = tokio::sync::Mutex::new(lock);
        let mut done = Ok(0);
        for declared in self.relay.settings.declared() {
            let pass = self
                .relay
                .pass(&lock, declared, self.relay.settings.backoff.0)
                .await;
            done = match (done, pass) {
                (Ok(n), Ok(m)) => Ok(n + m),
                (Err(e), _) | (Ok(_), Err(e)) => Err(e),
            };
        }
        lock.into_inner().release().await;
        done.map(Some)
    }

    /// Retires the stream `key`, which no relay declares any more: its relay
    /// row and its counts deleted, so the status lists it no longer. Its
    /// events still waiting — which nothing publishes — are refused, unless
    /// `drop_waiting`: then they are deleted with it, never published.
    /// Answers how many were.
    ///
    /// # Errors
    ///
    /// [`Error::Misuse`] for the main stream, a stream these settings or the
    /// leading relay's declare — take it out of every relay's settings
    /// first — a key no stream has, or events waiting without
    /// `drop_waiting`; [`Error::Database`] when the database fails.
    pub async fn retire(&self, key: &str, drop_waiting: bool) -> Result<u64, Error> {
        if key.is_empty() {
            return Err(Error::Misuse(
                "the main stream is never retired: every relay declares it".to_owned(),
            ));
        }
        if self.relay.settings.find(key).is_some() {
            return Err(Error::Misuse(format!(
                "these settings declare the stream `{key}`: take it out of every relay's \
                 settings before retiring it"
            )));
        }
        let mut tx = crate::relay::begin(&self.relay.pool, &self.relay.settings).await?;
        // The row locked: a relay that takes the lock and declares the stream
        // meanwhile waits, and declares it anew after.
        let declared: Option<bool> = sqlx::query_scalar(
            "SELECT declared FROM magic_events.relay WHERE stream = $1 FOR UPDATE",
        )
        .bind(key)
        .fetch_optional(&mut *tx)
        .await?;
        match declared {
            None => {
                return Err(Error::Misuse(format!(
                    "there is no stream `{key}` to retire"
                )))
            }
            Some(true) => {
                return Err(Error::Misuse(format!(
                    "the leading relay declares the stream `{key}`: take it out of every \
                     relay's settings before retiring it"
                )))
            }
            Some(false) => {}
        }
        let waiting: i64 =
            sqlx::query_scalar("SELECT count(*) FROM magic_events.outbox WHERE stream = $1")
                .bind(key)
                .fetch_one(&mut *tx)
                .await?;
        if waiting > 0 && !drop_waiting {
            return Err(Error::Misuse(format!(
                "{waiting} events wait for the stream `{key}`: declare it again to publish \
                 them, or retire it dropping them"
            )));
        }
        let dropped = sqlx::query("DELETE FROM magic_events.outbox WHERE stream = $1")
            .bind(key)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        for table in ["published", "relay"] {
            sqlx::query(&format!(
                "DELETE FROM magic_events.{table} WHERE stream = $1"
            ))
            .bind(key)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(dropped)
    }

    /// Sends the parked row `id` again with the next pass, its attempts
    /// counted anew: after a fix to the broker or the stream that refused it.
    /// Answers whether it was parked.
    ///
    /// # Errors
    ///
    /// [`Error::Database`] when the database fails.
    pub async fn unpark(&self, id: i64) -> Result<bool, Error> {
        Ok(self.unpark_where(Some(id), None, None).await? > 0)
    }

    /// Sends every parked row under `filter` — of one stream, and its
    /// subjects the filter takes — again with the stream's next pass, in
    /// their order: answers how many.
    ///
    /// # Errors
    ///
    /// [`Error::Misuse`] for a filter of no declared stream, or that is not a
    /// subject filter under a stream's prefix; [`Error::Database`] when the
    /// database fails.
    pub async fn unpark_all(&self, filter: &Filter) -> Result<u64, Error> {
        let scope = filter.resolve(&self.relay.settings)?;
        self.unpark_where(None, Some(scope.declared.key), scope.pattern())
            .await
    }

    /// What the relay, the outbox and the stream are as `filter` sees them —
    /// one stream, and its subjects the filter takes: see
    /// [`status`](crate::status). A broker that does not answer within
    /// five seconds leaves its part out and says why, so the answer still
    /// shows what the database knows.
    ///
    /// Counting the messages per subject reads every subject the filter
    /// takes: with an id in every subject — one per order, say — that is a
    /// subject per message, and the broker may not answer in time. Put in a
    /// subject what a consumer filters by (a store, a tenant), and ids in the
    /// body.
    ///
    /// # Errors
    ///
    /// [`Error::Misuse`] for a filter of no declared stream, or that is not a
    /// subject filter under a stream's prefix; [`Error::Database`] when the
    /// database fails.
    pub async fn status(&self, filter: &Filter) -> Result<Status, Error> {
        status::status(
            &self.relay.pool,
            &self.jetstream,
            &self.relay.settings,
            filter,
        )
        .await
    }

    /// The last `limit` messages — 1,000 at most — the stream holds under
    /// `filter`, oldest first, with their headers and bodies: read through the
    /// broker's ephemeral ordered consumers, which leave nothing behind.
    ///
    /// # Errors
    ///
    /// [`Error::Misuse`] for a filter of no declared stream, or that is not a
    /// subject filter under a stream's prefix; [`Error::Broker`] when the
    /// broker fails.
    pub async fn recent(&self, filter: &Filter, limit: u32) -> Result<Recent, Error> {
        status::recent(&self.jetstream, &self.relay.settings, filter, limit).await
    }

    /// Unparks the parked rows of id `id`, of the stream `stream`, and whose
    /// subject matches `pattern`: `None` for any.
    async fn unpark_where(
        &self,
        id: Option<i64>,
        stream: Option<&str>,
        pattern: Option<String>,
    ) -> Result<u64, Error> {
        let mut tx = crate::relay::begin(&self.relay.pool, &self.relay.settings).await?;
        let done = sqlx::query(
            "UPDATE magic_events.outbox SET parked_at = NULL, attempts = 0 \
              WHERE parked_at IS NOT NULL AND ($1::bigint IS NULL OR id = $1) \
                AND ($2::text IS NULL OR stream = $2) AND ($3::text IS NULL OR subject ~ $3)",
        )
        .bind(id)
        .bind(stream)
        .bind(pattern)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(done.rows_affected())
    }
}

#[cfg(test)]
#[path = "run_tests.rs"]
mod tests;
