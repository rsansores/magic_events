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
/// Dropped without [`Events::stop`], the relay stops at its next wait, as
/// after a crash: whatever it published and did not delete yet is published
/// again, and the stream drops it as a duplicate.
pub struct Events {
    relay: Arc<Relay>,
    jetstream: async_nats::jetstream::Context,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
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
    /// creates or checks the stream ([`Settings::manage_stream`]), records
    /// [`Settings::max_body`] for [`emit`](crate::emit) — every process that
    /// starts records its own, so they should agree — and spawns the relay
    /// on the current Tokio runtime.
    ///
    /// The relay holds one connection of `pool` while it runs, for its lock;
    /// that connection must reach one server process for as long as it
    /// lives — a direct connection, or a pooler in session mode.
    ///
    /// # Errors
    ///
    /// [`Error::Misuse`] for settings the relay cannot run with, a schema
    /// not at this build's version ([`Events::install`]), a stream that does
    /// not take the relay's subjects or that the broker refuses as the
    /// settings say it, or a `max_body` the broker's `max_payload` cannot
    /// carry with 4 KiB of headers; [`Error::Broker`] when the broker does
    /// not answer; [`Error::Database`] when the database fails.
    pub async fn start(
        pool: PgPool,
        jetstream: async_nats::jetstream::Context,
        settings: Settings,
    ) -> Result<Self, Error> {
        let relay = Self::prepare(pool, Arc::new(jetstream.clone()), settings).await?;
        let max_payload = jetstream.client().server_info().max_payload;
        let needed = u64::from(relay.settings.max_body) + u64::from(HEADROOM);
        if max_payload > 0 && u64::try_from(max_payload).unwrap_or(u64::MAX) < needed {
            return Err(Error::Misuse(format!(
                "the broker takes messages of {max_payload} bytes at most (`max_payload`), and a \
                 body of `max_body` {} bytes needs {needed} with its headers",
                relay.settings.max_body
            )));
        }
        stream::ensure(&jetstream, &relay.settings).await?;
        Self::record(&relay).await?;
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
        schema::check(&mut *pool.acquire().await?).await?;
        Ok(Relay {
            pool,
            publish,
            settings,
        })
    }

    /// Records the relay's `max_body`, which `emit` holds bodies to: the
    /// last process to start says it for all of them.
    pub(crate) async fn record(relay: &Relay) -> Result<(), Error> {
        sqlx::query("UPDATE magic_events.relay SET max_body = $1 WHERE id = 1")
            .bind(i32::try_from(relay.settings.max_body).unwrap_or(i32::MAX))
            .execute(&relay.pool)
            .await?;
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
            task: Some(task),
        }
    }

    /// `relay`, not running: for tests that run each pass themselves.
    #[cfg(test)]
    pub(crate) fn idle(relay: Relay, jetstream: async_nats::jetstream::Context) -> Self {
        Events {
            relay: Arc::new(relay),
            jetstream,
            stop: watch::channel(false).0,
            task: None,
        }
    }

    /// The relay, for a test that runs its steps one by one.
    #[cfg(test)]
    pub(crate) fn relay(&self) -> &Relay {
        &self.relay
    }

    /// Stops the relay at once, wherever it is, as a crash would.
    #[cfg(test)]
    pub(crate) fn abort(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }

    /// Stops the relay and waits for it: a pass under way is cut short, and
    /// its messages published again by whoever runs the relay next — the
    /// stream drops them as duplicates. Another process standing by takes
    /// the lock within its `standby`.
    pub async fn stop(mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take() {
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
    /// `DELETE` on the outbox, `SELECT` and `UPDATE` on `magic_events.relay`,
    /// `SELECT`, `INSERT` and `UPDATE` on `magic_events.published`, and
    /// `SELECT` on `magic_events.version`.
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

    /// One pass now, as the relay, for a host's tests or a tool: how many
    /// rows it published or set aside, or `None` when a relay — this
    /// process's own, or another's — holds the lock.
    ///
    /// # Errors
    ///
    /// What stopped the pass: [`Error::Broker`] when the broker failed it,
    /// [`Error::Database`] when the database did.
    pub async fn pass_now(&self) -> Result<Option<usize>, Error> {
        let Some(mut lock) =
            Lock::take(&self.relay.pool, self.relay.settings.database_timeout).await?
        else {
            return Ok(None);
        };
        let done = self
            .relay
            .pass(&mut lock, self.relay.settings.backoff.0)
            .await;
        lock.release().await;
        done.map(Some)
    }

    /// Sends the parked row `id` again with the next pass, its attempts
    /// counted anew: after a fix to the broker or the stream that refused it.
    /// Answers whether it was parked.
    ///
    /// # Errors
    ///
    /// [`Error::Database`] when the database fails.
    pub async fn unpark(&self, id: i64) -> Result<bool, Error> {
        Ok(self.unpark_where(Some(id)).await? > 0)
    }

    /// Sends every parked row again with the next pass, in their order:
    /// answers how many.
    ///
    /// # Errors
    ///
    /// [`Error::Database`] when the database fails.
    pub async fn unpark_all(&self) -> Result<u64, Error> {
        self.unpark_where(None).await
    }

    /// What the relay, the outbox and the stream are as `filter` sees them:
    /// see [`status`](crate::status). A broker that does not answer within
    /// five seconds leaves its part out and says why, so the answer still
    /// shows what the database knows.
    ///
    /// # Errors
    ///
    /// [`Error::Misuse`] for a filter that is not a subject filter under the
    /// settings' prefix; [`Error::Database`] when the database fails.
    pub async fn status(&self, filter: &Filter) -> Result<Status, Error> {
        status::status(
            &self.relay.pool,
            &self.jetstream,
            &self.relay.settings,
            filter,
        )
        .await
    }

    /// The last `limit` messages the stream holds under `filter`, oldest
    /// first, with their headers and bodies: read through the broker's
    /// ephemeral ordered consumers, which leave nothing behind.
    ///
    /// # Errors
    ///
    /// [`Error::Misuse`] for a filter that is not a subject filter under the
    /// settings' prefix; [`Error::Broker`] when the broker fails.
    pub async fn recent(&self, filter: &Filter, limit: u32) -> Result<Recent, Error> {
        status::recent(&self.jetstream, &self.relay.settings, filter, limit).await
    }

    async fn unpark_where(&self, id: Option<i64>) -> Result<u64, Error> {
        let done = sqlx::query(
            "UPDATE magic_events.outbox SET parked_at = NULL, attempts = 0 \
              WHERE parked_at IS NOT NULL AND ($1::bigint IS NULL OR id = $1)",
        )
        .bind(id)
        .execute(&self.relay.pool)
        .await?;
        Ok(done.rows_affected())
    }
}

#[cfg(test)]
#[path = "run_tests.rs"]
mod tests;
