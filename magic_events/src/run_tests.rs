//! `Events` as a host runs it: started in `main`, publishing on its own, one
//! process at a time, another taking over when it dies; the stream made and
//! kept to the settings; and what it cannot run with refused at start.

use super::*;
use crate::tests::{count, emit_each, shipped, until, Broker};
use crate::Storage;
use std::time::Duration;

/// How many hold an advisory lock on the test's database: the relay's.
const HELD: &str = "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted \
                      AND database = (SELECT oid FROM pg_database WHERE datname = current_database())";

#[sqlx::test(migrations = false)]
async fn started_it_publishes_what_commits_and_stopped_it_stops(pool: PgPool) {
    let broker = Broker::new().await;
    Events::install(&pool).await.unwrap();
    let events = Events::start(
        pool.clone(),
        broker.jetstream.clone(),
        broker.settings.clone(),
    )
    .await
    .unwrap();
    assert!(format!("{events:?}").starts_with("Events { settings: Settings { stream: "));
    emit_each(&pool, &[shipped("north", 1), shipped("south", 2)]).await;
    until("both published", || async {
        broker.bodies().await.len() == 2
    })
    .await;
    events.stop().await;
    until("the lock let go", || async {
        count(&pool, HELD).await == 0
    })
    .await;
    emit_each(&pool, &[shipped("north", 3)]).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(broker.bodies().await.len(), 2, "stopped");
    assert_eq!(
        count(&pool, "SELECT count(*) FROM magic_events.outbox").await,
        1
    );
    broker.done().await;
}

/// Stopped in the middle of a long outbox, the relay finishes the pass
/// under way: whatever it published is deleted, so nothing is in the stream
/// and in the outbox at once, to be sent twice by the relay after it.
#[sqlx::test(migrations = false)]
async fn stopped_mid_drain_it_leaves_nothing_to_send_twice(pool: PgPool) {
    let broker = Broker::new().await;
    Events::install(&pool).await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    for n in 0..3000 {
        crate::emit(&mut tx, &shipped("north", n)).await.unwrap();
    }
    tx.commit().await.unwrap();
    let events = Events::start(
        pool.clone(),
        broker.jetstream.clone(),
        broker.settings.clone(),
    )
    .await
    .unwrap();
    until("some published", || async {
        !broker.messages().await.is_empty()
    })
    .await;
    events.stop().await;
    let held = i64::try_from(broker.messages().await.len()).unwrap();
    let left = count(&pool, "SELECT count(*) FROM magic_events.outbox").await;
    assert!(left > 0, "stopped before the end");
    assert_eq!(
        held + left,
        3000,
        "{held} in the stream, {left} in the outbox"
    );
    assert_eq!(count(&pool, HELD).await, 0, "the lock let go");
    broker.done().await;
}

/// Two processes on one database: one leads and the other stands by, and
/// every event is published once.
#[sqlx::test(migrations = false)]
async fn of_two_relays_on_one_database_one_leads(pool: PgPool) {
    let broker = Broker::new().await;
    Events::install(&pool).await.unwrap();
    let start = || {
        Events::start(
            pool.clone(),
            broker.jetstream.clone(),
            broker.settings.clone(),
        )
    };
    let (a, b) = (start().await.unwrap(), start().await.unwrap());
    let events: Vec<_> = (0..50).map(|n| shipped("north", n)).collect();
    emit_each(&pool, &events).await;
    until("all published", || async {
        broker.bodies().await.len() == 50
    })
    .await;
    assert_eq!(count(&pool, HELD).await, 1, "one leads");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(broker.bodies().await.len(), 50, "each once");
    a.stop().await;
    b.stop().await;
    broker.done().await;
}

/// The leader's connection killed — its process gone, or its network — the
/// other process takes the lock within its standby and publishes what the
/// first left.
#[sqlx::test(migrations = false)]
async fn killing_the_leaders_connection_the_other_takes_over(pool: PgPool) {
    let broker = Broker::new().await;
    Events::install(&pool).await.unwrap();
    let start = || {
        Events::start(
            pool.clone(),
            broker.jetstream.clone(),
            broker.settings.clone(),
        )
    };
    let (a, b) = (start().await.unwrap(), start().await.unwrap());
    emit_each(&pool, &[shipped("north", 1)]).await;
    until("published", || async { broker.bodies().await.len() == 1 }).await;
    let holder = || async {
        sqlx::query_scalar::<_, i32>(
            "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted \
               AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
        )
        .fetch_optional(&pool)
        .await
        .unwrap()
    };
    let first = holder().await.expect("a leader");
    sqlx::query("SELECT pg_terminate_backend($1)")
        .bind(first)
        .execute(&pool)
        .await
        .unwrap();
    until("another leads", || async {
        holder().await.is_some_and(|pid| pid != first)
    })
    .await;
    emit_each(&pool, &[shipped("north", 2)]).await;
    until("published", || async { broker.bodies().await.len() == 2 }).await;
    a.stop().await;
    b.stop().await;
    broker.done().await;
}

/// The stream is created as the settings say, brought to them when they
/// change, and a storage it cannot change to is refused.
#[sqlx::test(migrations = false)]
async fn the_stream_is_made_and_kept_to_the_settings(pool: PgPool) {
    let broker = Broker::new().await;
    Events::install(&pool).await.unwrap();
    let config = || async {
        broker
            .jetstream
            .get_stream(&broker.settings.stream)
            .await
            .unwrap()
            .info()
            .await
            .unwrap()
            .config
            .clone()
    };
    let start =
        |settings: Settings| Events::start(pool.clone(), broker.jetstream.clone(), settings);
    start(broker.settings.clone()).await.unwrap().stop().await;
    let made = config().await;
    let prefix = &broker.settings.subject_prefix;
    assert_eq!(made.subjects, [format!("{prefix}.>")]);
    assert_eq!(made.max_age, Duration::from_secs(7 * 24 * 60 * 60));
    assert_eq!(made.duplicate_window, Duration::from_secs(120));

    let longer = broker
        .settings
        .clone()
        .retention(Duration::from_secs(30 * 24 * 60 * 60))
        .duplicate_window(Duration::from_secs(600));
    start(longer).await.unwrap().stop().await;
    let kept = config().await;
    assert_eq!(kept.max_age, Duration::from_secs(30 * 24 * 60 * 60));
    assert_eq!(kept.duplicate_window, Duration::from_secs(600));
    assert_eq!(kept.subjects, made.subjects);

    match start(broker.settings.clone().storage(Storage::File)).await {
        Err(Error::Misuse(why)) => assert!(why.contains("storage does not change"), "{why}"),
        other => panic!("{other:?}"),
    }
    broker.done().await;
}

/// With the stream left to the operator, it must be there and take every
/// subject under the prefix; the relay does not touch it.
#[sqlx::test(migrations = false)]
async fn an_operators_stream_must_take_the_relays_subjects(pool: PgPool) {
    let broker = Broker::new().await;
    Events::install(&pool).await.unwrap();
    let settings = broker.settings.clone().manage_stream(false);
    let start = || Events::start(pool.clone(), broker.jetstream.clone(), settings.clone());
    match start().await {
        Err(Error::Misuse(why)) => assert!(why.contains("there is no stream"), "{why}"),
        other => panic!("{other:?}"),
    }
    let prefix = broker.settings.subject_prefix.clone();
    let mut config = async_nats::jetstream::stream::Config {
        name: broker.settings.stream.clone(),
        subjects: vec![format!("{prefix}.orders.>")],
        storage: async_nats::jetstream::stream::StorageType::Memory,
        ..Default::default()
    };
    broker
        .jetstream
        .create_stream(config.clone())
        .await
        .unwrap();
    match start().await {
        Err(Error::Misuse(why)) => assert!(why.contains("not every subject"), "{why}"),
        other => panic!("{other:?}"),
    }
    config.subjects = vec![format!("{prefix}.>")];
    config.max_age = Duration::from_secs(60);
    broker.jetstream.update_stream(&config).await.unwrap();
    start().await.unwrap().stop().await;
    let after = broker
        .jetstream
        .get_stream(&broker.settings.stream)
        .await
        .unwrap()
        .cached_info()
        .config
        .clone();
    assert_eq!(after.max_age, Duration::from_secs(60), "left as it was");
    broker.done().await;
}

#[sqlx::test(migrations = false)]
async fn start_refuses_a_schema_not_at_its_version(pool: PgPool) {
    let broker = Broker::new().await;
    let start = || {
        Events::start(
            pool.clone(),
            broker.jetstream.clone(),
            broker.settings.clone(),
        )
    };
    match start().await {
        Err(Error::Misuse(why)) => assert!(why.contains("not installed"), "{why}"),
        other => panic!("{other:?}"),
    }
    Events::install(&pool).await.unwrap();
    sqlx::query("UPDATE magic_events.version SET version = 0")
        .execute(&pool)
        .await
        .unwrap();
    match start().await {
        Err(Error::Misuse(why)) => assert!(why.contains("version 0"), "{why}"),
        other => panic!("{other:?}"),
    }
    broker.done().await;
}

/// The relay's `max_body` is what `emit` holds bodies to, and one the
/// broker's `max_payload` cannot carry is refused.
#[sqlx::test(migrations = false)]
async fn max_body_is_recorded_for_emit_and_held_to_the_broker(pool: PgPool) {
    let broker = Broker::new().await;
    Events::install(&pool).await.unwrap();
    let max_payload = broker.jetstream.client().server_info().max_payload;
    let too_large = u32::try_from(max_payload).unwrap() - 1024;
    match Events::start(
        pool.clone(),
        broker.jetstream.clone(),
        broker.settings.clone().max_body(too_large),
    )
    .await
    {
        Err(Error::Misuse(why)) => assert!(why.contains("max_payload"), "{why}"),
        other => panic!("{other:?}"),
    }
    let events = Events::start(
        pool.clone(),
        broker.jetstream.clone(),
        broker.settings.clone().max_body(100),
    )
    .await
    .unwrap();
    assert_eq!(
        count(&pool, "SELECT max_body::bigint FROM magic_events.relay").await,
        100
    );
    let mut conn = pool.acquire().await.unwrap();
    match crate::emit(&mut conn, &shipped(&"x".repeat(100), 1)).await {
        Err(Error::Invalid(why)) => assert!(why.contains("100 at most"), "{why}"),
        other => panic!("{other:?}"),
    }
    events.stop().await;
    broker.done().await;
}

/// `pass_now` is the relay's: while a running relay holds the lock, it does
/// nothing and says so.
#[sqlx::test(migrations = false)]
async fn pass_now_stands_aside_for_a_running_relay(pool: PgPool) {
    let broker = Broker::new().await;
    Events::install(&pool).await.unwrap();
    let events = Events::start(
        pool.clone(),
        broker.jetstream.clone(),
        broker.settings.clone(),
    )
    .await
    .unwrap();
    until("it leads", || async { count(&pool, HELD).await == 1 }).await;
    assert_eq!(events.pass_now().await.unwrap(), None);
    events.stop().await;
    broker.done().await;
}
