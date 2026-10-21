//! The relay, pass by pass, against a real JetStream: what committed is
//! published once, in order, with every header; a broker away keeps the rows
//! and backs off; a message published twice is held once; a message refused
//! for good is set aside and holds back nothing.

use super::*;
use crate::tests::{count, emit_each, idle, shipped, until, Broker, Faulty, Mode};
use crate::Events;
use sqlx::PgPool;

/// How long a test's relay waits for the database.
const WAIT: Duration = Duration::from_secs(30);

/// Settings whose database wait is `wait`: what the lock is taken with.
fn waiting(wait: Duration) -> Settings {
    let mut settings = Settings::new("LOCK", "test");
    settings.database_timeout = wait;
    settings
}

/// Passes until the outbox has nothing left to publish.
async fn drain(events: &Events) {
    while events.pass_now().await.unwrap().expect("the lock") > 0 {}
}

#[sqlx::test(migrations = false)]
async fn a_committed_event_is_one_message_with_every_header(pool: PgPool) {
    let broker = Broker::new().await;
    let events = idle(&pool, &broker, Arc::new(broker.jetstream.clone())).await;
    let mut tx = pool.begin().await.unwrap();
    let id = crate::emit(&mut tx, &shipped("north", 1)).await.unwrap();
    tx.commit().await.unwrap();

    assert_eq!(events.pass_now().await.unwrap(), Some(1));
    let messages = broker.messages().await;
    assert_eq!(messages.len(), 1);
    let m = &messages[0];
    let prefix = &broker.settings.subject_prefix;
    assert_eq!(m.subject, format!("{prefix}.orders.north.shipped"));
    assert_eq!(m.id, id.to_string());
    assert_eq!(m.msg_id.as_deref(), Some(m.id.as_str()));
    assert_eq!(m.ty, "order.shipped.v1");
    assert_eq!(m.source.as_deref(), Some("//tests"));
    assert_eq!(m.spec.as_deref(), Some("1.0"));
    assert_eq!(m.content.as_deref(), Some("application/json"));
    let time = m.time.as_deref().expect("a ce-time");
    assert!(chrono::DateTime::parse_from_rfc3339(time).is_ok(), "{time}");
    assert_eq!(m.body, r#"{"store":"north","n":1}"#);

    assert_eq!(
        count(&pool, "SELECT count(*) FROM magic_events.outbox").await,
        0
    );
    assert_eq!(
        count(
            &pool,
            "SELECT sum(count)::bigint FROM magic_events.published WHERE type = 'order.shipped.v1'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM magic_events.relay WHERE last_published_at IS NOT NULL"
        )
        .await,
        1
    );
    assert_eq!(events.pass_now().await.unwrap(), Some(0), "nothing left");
    broker.done().await;
}

#[sqlx::test(migrations = false)]
async fn a_rolled_back_transaction_publishes_nothing(pool: PgPool) {
    let broker = Broker::new().await;
    let events = idle(&pool, &broker, Arc::new(broker.jetstream.clone())).await;
    let mut tx = pool.begin().await.unwrap();
    crate::emit(&mut tx, &shipped("north", 1)).await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(events.pass_now().await.unwrap(), Some(0));
    assert!(broker.messages().await.is_empty());
    broker.done().await;
}

#[sqlx::test(migrations = false)]
async fn the_events_of_one_transaction_go_out_in_emit_order(pool: PgPool) {
    let broker = Broker::new().await;
    let events = idle(&pool, &broker, Arc::new(broker.jetstream.clone())).await;
    let mut tx = pool.begin().await.unwrap();
    crate::emit(&mut tx, &shipped("north", 1)).await.unwrap();
    crate::emit(&mut tx, &shipped("south", 2)).await.unwrap();
    tx.commit().await.unwrap();
    drain(&events).await;
    assert_eq!(
        broker.bodies().await,
        [r#"{"store":"north","n":1}"#, r#"{"store":"south","n":2}"#]
    );
    broker.done().await;
}

/// More than a batch, more than `in_flight` of them in the air at once: the
/// stream holds them in the order they were emitted.
#[sqlx::test(migrations = false)]
async fn pipelining_keeps_the_order_of_a_batch(pool: PgPool) {
    let broker = Broker::new().await;
    let events = idle(&pool, &broker, Arc::new(broker.jetstream.clone())).await;
    let mut tx = pool.begin().await.unwrap();
    for n in 0..600 {
        crate::emit(&mut tx, &shipped("north", n)).await.unwrap();
    }
    tx.commit().await.unwrap();
    assert_eq!(events.pass_now().await.unwrap(), Some(256), "a batch");
    drain(&events).await;
    let ns: Vec<u64> = broker
        .bodies()
        .await
        .iter()
        .map(|b| {
            serde_json::from_str::<serde_json::Value>(b).unwrap()["n"]
                .as_u64()
                .unwrap()
        })
        .collect();
    assert_eq!(ns, (0..600).collect::<Vec<_>>());
    broker.done().await;
}

/// With the broker away a pass fails, keeps every row and says so; once it
/// is back, everything goes, once.
#[sqlx::test(migrations = false)]
async fn the_broker_away_keeps_the_rows_until_it_is_back(pool: PgPool) {
    let mut broker = Broker::new().await;
    // What the status says the relay waits: long enough to read it as ahead.
    broker.settings = broker
        .settings
        .backoff(Duration::from_secs(30), Duration::from_secs(60));
    let faulty = Faulty::new(broker.jetstream.clone());
    let events = idle(&pool, &broker, Arc::new(faulty.clone())).await;
    emit_each(&pool, &[shipped("north", 1), shipped("north", 2)]).await;
    faulty.set(Mode::Down);
    for failures in 1..=2 {
        match events.pass_now().await {
            Err(e @ crate::Error::Broker { .. }) => assert!(e.is_transient()),
            other => panic!("{other:?}"),
        }
        let (n, error, waits): (i32, Option<String>, bool) = sqlx::query_as(
            "SELECT failures, last_error, retry_after > now() FROM magic_events.relay",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            (n, error.as_deref(), waits),
            (failures, Some("the broker is away"), true)
        );
    }
    assert_eq!(
        count(&pool, "SELECT count(*) FROM magic_events.outbox").await,
        2
    );

    faulty.set(Mode::Up);
    drain(&events).await;
    assert_eq!(broker.bodies().await.len(), 2);
    let (n, error): (i32, Option<String>) =
        sqlx::query_as("SELECT failures, last_error FROM magic_events.relay")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((n, error), (0, None), "the relay's failures are over");
    broker.done().await;
}

/// The running relay waits longer after each failure in a row, up to the
/// longest backoff, and publishes everything once the broker is back.
#[sqlx::test(migrations = false)]
async fn the_relay_backs_off_while_the_broker_is_away(pool: PgPool) {
    let mut broker = Broker::new().await;
    broker.settings = broker
        .settings
        .backoff(Duration::from_millis(50), Duration::from_millis(400));
    let faulty = Faulty::new(broker.jetstream.clone());
    Events::install(&pool).await.unwrap();
    crate::stream::ensure(&broker.jetstream, &broker.settings)
        .await
        .unwrap();
    faulty.set(Mode::Down);
    emit_each(&pool, &[shipped("north", 1)]).await;
    let events = Events::spawn(
        Events::prepare(
            pool.clone(),
            Arc::new(faulty.clone()),
            broker.settings.clone(),
        )
        .await
        .unwrap(),
        broker.jetstream.clone(),
    );
    until("six tries", || async {
        faulty.sent.lock().unwrap().len() >= 6
    })
    .await;
    faulty.set(Mode::Up);
    until("published", || async { broker.bodies().await.len() == 1 }).await;
    events.stop().await;
    let tried = faulty.sent.lock().unwrap().clone();
    let gaps: Vec<u128> = tried
        .windows(2)
        .map(|w| (w[1] - w[0]).as_millis())
        .collect();
    // 50, 100, 200, then 400 ms at most, each plus a pass: waits that grow,
    // and stop growing at the longest.
    assert!(gaps[0] >= 50 && gaps[0] < 200, "{gaps:?}");
    assert!(gaps[2] >= 200, "{gaps:?}");
    assert!(
        gaps[3..5].iter().all(|g| (400..800).contains(g)),
        "{gaps:?}"
    );
    broker.done().await;
}

/// A relay that died after the stream took its messages, and before it
/// deleted their rows: the next pass sends them again, and the stream holds
/// each once.
#[sqlx::test(migrations = false)]
async fn a_message_published_and_not_deleted_is_held_once(pool: PgPool) {
    let broker = Broker::new().await;
    let events = idle(&pool, &broker, Arc::new(broker.jetstream.clone())).await;
    emit_each(&pool, &[shipped("north", 1), shipped("north", 2)]).await;
    let mut lock = Lock::take(&pool, &waiting(WAIT)).await.unwrap().unwrap();
    let mut rows = events_relay(&events).pending(&mut lock).await.unwrap();
    let published = events_relay(&events).send(&mut rows).await;
    assert_eq!((published.acked, published.failure), (2, None));
    lock.release().await;
    assert_eq!(
        count(&pool, "SELECT count(*) FROM magic_events.outbox").await,
        2
    );

    drain(&events).await;
    assert_eq!(broker.bodies().await.len(), 2, "each once");
    assert_eq!(
        count(&pool, "SELECT count(*) FROM magic_events.outbox").await,
        0
    );
    broker.done().await;
}

/// The stream took a message, and its acknowledgement was lost: the relay
/// keeps the row and sends it again, and the stream holds it once.
#[sqlx::test(migrations = false)]
async fn a_lost_acknowledgement_is_sent_again_and_held_once(pool: PgPool) {
    let broker = Broker::new().await;
    let faulty = Faulty::new(broker.jetstream.clone());
    let events = idle(&pool, &broker, Arc::new(faulty.clone())).await;
    emit_each(&pool, &[shipped("north", 1)]).await;
    faulty.set(Mode::Unacked);
    assert!(events.pass_now().await.is_err());
    until("the stream holds it", || async {
        broker.bodies().await.len() == 1
    })
    .await;
    faulty.set(Mode::Up);
    drain(&events).await;
    assert_eq!(broker.bodies().await.len(), 1);
    broker.done().await;
}

/// A message the broker refuses for good is tried three times, then parked:
/// the rows behind it go on, and `unpark` sends it again after a fix.
#[sqlx::test(migrations = false)]
async fn a_message_refused_for_good_is_parked_and_holds_back_nothing(pool: PgPool) {
    let broker = Broker::new().await;
    let faulty = Faulty::new(broker.jetstream.clone());
    let events = idle(&pool, &broker, Arc::new(faulty.clone())).await;
    emit_each(
        &pool,
        &[
            shipped("north", 1),
            shipped("poison", 2),
            shipped("north", 3),
        ],
    )
    .await;
    for attempt in 1..=3 {
        assert!(events.pass_now().await.unwrap().unwrap() > 0);
        let (attempts, parked, error): (i32, bool, Option<String>) = sqlx::query_as(
            "SELECT attempts, parked_at IS NOT NULL, last_error FROM magic_events.outbox \
              WHERE subject LIKE '%poison%'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            (attempts, parked, error.as_deref()),
            (attempt, attempt == 3, Some("a message the stream refuses"))
        );
    }
    drain(&events).await;
    assert_eq!(
        broker.bodies().await,
        [r#"{"store":"north","n":1}"#, r#"{"store":"north","n":3}"#]
    );
    assert_eq!(
        count(&pool, "SELECT failures::bigint FROM magic_events.relay").await,
        0,
        "a refusal is the message's, not the relay's"
    );

    *faulty.refusing.lock().unwrap() = false;
    let parked: i64 = count(&pool, "SELECT id FROM magic_events.outbox").await;
    assert!(!events.unpark(parked + 1).await.unwrap(), "no such row");
    assert!(events.unpark(parked).await.unwrap());
    assert!(!events.unpark(parked).await.unwrap(), "not parked any more");
    drain(&events).await;
    assert_eq!(broker.bodies().await.len(), 3);
    assert_eq!(
        count(&pool, "SELECT count(*) FROM magic_events.outbox").await,
        0
    );
    assert_eq!(events.unpark_all().await.unwrap(), 0);
    broker.done().await;
}

/// The stream itself refusing a message as too large is a refusal for good:
/// the real broker's answer, not a test's.
#[sqlx::test(migrations = false)]
async fn a_message_the_stream_refuses_as_too_large_is_parked(pool: PgPool) {
    let broker = Broker::new().await;
    let events = idle(&pool, &broker, Arc::new(broker.jetstream.clone())).await;
    let mut stream = broker
        .jetstream
        .get_stream(&broker.settings.stream)
        .await
        .unwrap();
    let mut config = stream.info().await.unwrap().config.clone();
    config.max_message_size = 512;
    broker.jetstream.update_stream(&config).await.unwrap();
    emit_each(&pool, &[shipped(&"x".repeat(600), 1), shipped("north", 2)]).await;
    for _ in 0..3 {
        events.pass_now().await.unwrap();
    }
    let (parked, error): (bool, String) = sqlx::query_as(
        "SELECT parked_at IS NOT NULL, last_error FROM magic_events.outbox WHERE id = 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(parked, "{error}");
    assert!(error.contains("error code 10054"), "{error}");
    drain(&events).await;
    assert_eq!(broker.bodies().await, [r#"{"store":"north","n":2}"#]);
    broker.done().await;
}

/// A relay whose lock's connection is gone commits nothing: what it
/// published is published again by whoever runs the relay next, and the
/// stream holds it once.
#[sqlx::test(migrations = false)]
async fn a_relay_that_lost_its_lock_commits_nothing(pool: PgPool) {
    let broker = Broker::new().await;
    let events = idle(&pool, &broker, Arc::new(broker.jetstream.clone())).await;
    emit_each(&pool, &[shipped("north", 1)]).await;
    let relay = events_relay(&events);
    let mut lock = Lock::take(&pool, &waiting(WAIT)).await.unwrap().unwrap();
    let mut rows = relay.pending(&mut lock).await.unwrap();
    let published = relay.send(&mut rows).await;
    sqlx::query(
        "SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'advisory' \
            AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(relay
        .settle(&mut lock, &rows, published, Duration::from_millis(10))
        .await
        .is_err());
    assert!(!lock.still_held().await);
    assert_eq!(
        count(&pool, "SELECT count(*) FROM magic_events.outbox").await,
        1
    );
    assert_eq!(
        count(&pool, "SELECT count(*) FROM magic_events.published").await,
        0
    );
    drain(&events).await;
    assert_eq!(broker.bodies().await.len(), 1);
    broker.done().await;
}

/// Through a pooler in transaction mode a relay's transaction can run on a
/// server process that does not hold its lock: it is refused, rather than
/// committing beside the process that does.
#[sqlx::test(migrations = false)]
async fn a_transaction_on_a_connection_without_the_lock_is_refused(pool: PgPool) {
    Events::install(&pool).await.unwrap();
    let held = Lock::take(&pool, &waiting(WAIT)).await.unwrap().unwrap();
    let mut elsewhere = Lock {
        conn: pool.acquire().await.unwrap().detach(),
        wait: WAIT,
    };
    match elsewhere.fenced().await.err() {
        Some(crate::Error::Misuse(why)) => assert!(why.contains("session pooling"), "{why}"),
        other => panic!("{other:?}"),
    }
    assert!(!elsewhere.still_held().await, "nor counts as held");
    held.release().await;
}

/// The relay inside `events`, for a test that runs its steps one by one.
fn events_relay(events: &Events) -> &Relay {
    events.relay()
}

/// A relay whose database stops answering — here, a table lock it waits
/// behind — gives up within its wait, and its session lets the lock go, so
/// another process can take over; and its session is set to be ended by the
/// server when its network dies.
#[sqlx::test(migrations = false)]
async fn a_relay_the_database_does_not_answer_lets_its_lock_go(pool: PgPool) {
    let broker = Broker::new().await;
    let events = idle(&pool, &broker, Arc::new(broker.jetstream.clone())).await;
    emit_each(&pool, &[shipped("north", 1)]).await;
    let wait = Duration::from_millis(500);
    let mut lock = Lock::take(&pool, &waiting(wait)).await.unwrap().unwrap();
    let user_timeout: String = sqlx::query_scalar("SHOW tcp_user_timeout")
        .fetch_one(&mut lock.conn)
        .await
        .unwrap();
    assert_eq!(user_timeout, "20000");

    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE magic_events.outbox IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let started = std::time::Instant::now();
    assert!(events_relay(&events).pending(&mut lock).await.is_err());
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    drop(lock);
    until("another takes the lock", || async {
        Lock::take(&pool, &waiting(wait))
            .await
            .ok()
            .flatten()
            .is_some()
    })
    .await;
    blocker.rollback().await.unwrap();
    broker.done().await;
}
