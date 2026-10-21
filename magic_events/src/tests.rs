//! What every test of the crate stands on: a database of its own, made by
//! `#[sqlx::test]` from `DATABASE_URL`, and the JetStream at `NATS_URL`,
//! shared — each test with a stream and a subject prefix of its own, so the
//! tests run side by side.

use crate::envelope::Message;
use crate::publish::{Ack, Boxed, Failure, Publish};
use crate::{Events, Settings, Storage};
use async_nats::jetstream::Context;
use serde::Serialize;
use sqlx::PgPool;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

/// The JetStream every test publishes to: `NATS_URL`, or a local server.
pub(crate) async fn jetstream() -> Context {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_owned());
    let client = async_nats::connect(&url)
        .await
        .unwrap_or_else(|e| panic!("no NATS at {url} (NATS_URL): {e}"));
    async_nats::jetstream::new(client)
}

/// An event of the tests: `poison` in its subject has the broker refuse it,
/// while `Faulty` is told to.
#[derive(Serialize, crate::Event)]
#[event(
    type = "order.shipped",
    version = 1,
    subject = "orders.{store}.shipped"
)]
pub(crate) struct Shipped {
    pub(crate) store: String,
    pub(crate) n: u64,
}

pub(crate) fn shipped(store: &str, n: u64) -> Shipped {
    Shipped {
        store: store.to_owned(),
        n,
    }
}

/// A stream of a test's own, in memory, and settings that wait milliseconds
/// where a host's wait seconds.
pub(crate) struct Broker {
    pub(crate) jetstream: Context,
    pub(crate) settings: Settings,
}

impl Broker {
    pub(crate) async fn new() -> Broker {
        let name = format!("T{}", Uuid::now_v7().simple());
        let settings = Settings::new(&name, "//tests")
            .storage(Storage::Memory)
            .idle(Duration::from_millis(10))
            .standby(Duration::from_millis(10))
            .backoff(Duration::from_millis(10), Duration::from_millis(80))
            .send_timeout(Duration::from_secs(5));
        Broker {
            jetstream: jetstream().await,
            settings,
        }
    }

    /// Every message the stream holds, in order: its subject, its headers by
    /// name, and its body.
    pub(crate) async fn messages(&self) -> Vec<Received> {
        let Ok(mut stream) = self.jetstream.get_stream(&self.settings.stream).await else {
            return Vec::new();
        };
        let info = stream.info().await.unwrap().state.clone();
        let mut out = Vec::new();
        for sequence in info.first_sequence..=info.last_sequence {
            if info.messages == 0 {
                break;
            }
            let Ok(m) = stream.get_raw_message(sequence).await else {
                continue;
            };
            let header = |name: &str| m.headers.get(name).map(|v| v.as_str().to_owned());
            out.push(Received {
                subject: m.subject.to_string(),
                id: header("ce-id").expect("a ce-id"),
                msg_id: header("Nats-Msg-Id"),
                ty: header("ce-type").expect("a ce-type"),
                source: header("ce-source"),
                time: header("ce-time"),
                spec: header("ce-specversion"),
                content: header("ce-datacontenttype"),
                body: String::from_utf8(m.payload.to_vec()).unwrap(),
            });
        }
        out
    }

    /// The bodies the stream holds, in order.
    pub(crate) async fn bodies(&self) -> Vec<String> {
        self.messages().await.into_iter().map(|m| m.body).collect()
    }

    /// Deletes the test's stream.
    pub(crate) async fn done(self) {
        let _ = self.jetstream.delete_stream(&self.settings.stream).await;
    }
}

/// A message as the stream holds it.
#[derive(Debug, Clone)]
pub(crate) struct Received {
    pub(crate) subject: String,
    pub(crate) id: String,
    pub(crate) msg_id: Option<String>,
    pub(crate) ty: String,
    pub(crate) source: Option<String>,
    pub(crate) time: Option<String>,
    pub(crate) spec: Option<String>,
    pub(crate) content: Option<String>,
    pub(crate) body: String,
}

/// How `Faulty` fails.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    /// It does not: every message goes to JetStream.
    Up,
    /// Every send fails, as with the broker away.
    Down,
    /// Every acknowledgement is lost, as with the broker away after it took
    /// the message.
    Unacked,
}

/// JetStream, failing as a test says: `Mode`, and a refusal for good of a
/// message whose subject holds `poison` while `refusing` is set.
#[derive(Clone)]
pub(crate) struct Faulty {
    pub(crate) jetstream: Context,
    pub(crate) mode: Arc<Mutex<Mode>>,
    pub(crate) refusing: Arc<Mutex<bool>>,
    /// When each message was sent, in order, failed or not.
    pub(crate) sent: Arc<Mutex<Vec<std::time::Instant>>>,
}

impl Faulty {
    pub(crate) fn new(jetstream: Context) -> Faulty {
        Faulty {
            jetstream,
            mode: Arc::new(Mutex::new(Mode::Up)),
            refusing: Arc::new(Mutex::new(true)),
            sent: Arc::default(),
        }
    }

    pub(crate) fn set(&self, mode: Mode) {
        *self.mode.lock().unwrap() = mode;
    }
}

impl Publish for Faulty {
    fn send(&self, message: Message, body: Vec<u8>) -> Boxed<'_, Result<Ack, Failure>> {
        Box::pin(async move {
            self.sent.lock().unwrap().push(std::time::Instant::now());
            if message.subject.contains("poison") && *self.refusing.lock().unwrap() {
                return Err(Failure {
                    transient: false,
                    reason: "a message the stream refuses".to_owned(),
                });
            }
            let mode = *self.mode.lock().unwrap();
            match mode {
                Mode::Down => Err(Failure::transient("the broker is away")),
                Mode::Up => self.jetstream.send(message, body).await,
                Mode::Unacked => {
                    let ack = self.jetstream.send(message, body).await?;
                    drop(ack);
                    let lost: Ack = Box::pin(async {
                        Err(Failure::transient(
                            "no acknowledgement: the broker went away",
                        ))
                    });
                    Ok(lost)
                }
            }
        })
    }
}

/// The schema installed, and `Events` over `publish` with the broker's
/// settings, its stream created — not running: the test runs each pass.
pub(crate) async fn idle(pool: &PgPool, broker: &Broker, publish: Arc<dyn Publish>) -> Events {
    Events::install(pool).await.unwrap();
    crate::stream::ensure(&broker.jetstream, &broker.settings)
        .await
        .unwrap();
    let relay = Events::prepare(pool.clone(), publish, broker.settings.clone())
        .await
        .unwrap();
    Events::record(&relay).await.unwrap();
    Events::idle(relay, broker.jetstream.clone())
}

/// Emits each of `events` in a transaction of its own.
pub(crate) async fn emit_each(pool: &PgPool, events: &[Shipped]) {
    for event in events {
        let mut tx = pool.begin().await.unwrap();
        crate::emit(&mut tx, event).await.unwrap();
        tx.commit().await.unwrap();
    }
}

/// Counts what `sql` counts.
pub(crate) async fn count(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(pool).await.unwrap()
}

/// Waits until `done` holds, or fails after ten seconds.
pub(crate) async fn until<F: std::future::Future<Output = bool>>(what: &str, done: impl Fn() -> F) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !done().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "not within ten seconds: {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The database and the broker answer, and the broker runs JetStream.
#[sqlx::test(migrations = false)]
async fn the_database_and_the_broker_answer(pool: PgPool) {
    let one: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(one, 1);
    let account = jetstream().await.query_account().await;
    assert!(account.is_ok(), "JetStream is not enabled: {account:?}");
}
