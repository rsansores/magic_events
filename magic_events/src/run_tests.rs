//! `Events` as a host runs it: started in `main`, publishing on its own, one
//! process at a time, another taking over when it dies; the stream made and
//! kept to the settings; and what it cannot run with refused at start.

use super::*;
use crate::status::Filter;
use crate::tests::{count, emit_each, seen, shipped, until, Broker};
use crate::{Storage, StreamSettings};
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
    assert!(format!("{events:?}").starts_with("Events { settings: Settings { source: "));
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
    // One row a pass, so the drain takes seconds: stopped while it runs, not
    // after it ran out, however fast the machine.
    let events = Events::start(
        pool.clone(),
        broker.jetstream.clone(),
        broker.settings.clone().batch(1).in_flight(1),
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
            .get_stream(&broker.settings.main.name)
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
    let prefix = &broker.settings.main.subject_prefix;
    assert_eq!(made.subjects, [format!("{prefix}.>")]);
    assert_eq!(made.max_age, Duration::from_secs(7 * 24 * 60 * 60));
    assert_eq!(made.duplicate_window, Duration::from_secs(120));

    let longer = broker.main(|main| {
        main.retention(Duration::from_secs(30 * 24 * 60 * 60))
            .duplicate_window(Duration::from_secs(600))
    });
    start(longer).await.unwrap().stop().await;
    let kept = config().await;
    assert_eq!(kept.max_age, Duration::from_secs(30 * 24 * 60 * 60));
    assert_eq!(kept.duplicate_window, Duration::from_secs(600));
    assert_eq!(kept.subjects, made.subjects);

    match start(broker.main(|main| main.storage(Storage::File))).await {
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
    let settings = broker.main(|main| main.manage_stream(false));
    let start = || Events::start(pool.clone(), broker.jetstream.clone(), settings.clone());
    match start().await {
        Err(Error::Misuse(why)) => assert!(why.contains("there is no stream"), "{why}"),
        other => panic!("{other:?}"),
    }
    let prefix = broker.settings.main.subject_prefix.clone();
    let mut config = async_nats::jetstream::stream::Config {
        name: broker.settings.main.name.clone(),
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
        .get_stream(&broker.settings.main.name)
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
        broker.main(|main| main.max_body(too_large)),
    )
    .await
    {
        Err(Error::Misuse(why)) => assert!(why.contains("max_payload"), "{why}"),
        other => panic!("{other:?}"),
    }
    let events = Events::start(
        pool.clone(),
        broker.jetstream.clone(),
        broker.main(|main| main.max_body(100)),
    )
    .await
    .unwrap();
    assert_eq!(
        count(
            &pool,
            "SELECT max_body::bigint FROM magic_events.relay WHERE stream = ''"
        )
        .await,
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

/// A role of its own for one test, granted `grants` on the crate's schema,
/// which the test's login role may set: roles are the cluster's, so its name
/// is the test's alone, and `drop_role` takes it away again.
async fn role(pool: &PgPool, grants: &[&str]) -> String {
    let name = format!("relay_{}", uuid::Uuid::now_v7().simple());
    sqlx::query(&format!("CREATE ROLE {name} NOLOGIN"))
        .execute(pool)
        .await
        .unwrap();
    for grant in grants {
        sqlx::query(&format!("GRANT {grant} TO {name}"))
            .execute(pool)
            .await
            .unwrap();
    }
    name
}

async fn drop_role(pool: &PgPool, name: &str) {
    for sql in [format!("DROP OWNED BY {name}"), format!("DROP ROLE {name}")] {
        sqlx::query(&sql).execute(pool).await.unwrap();
    }
}

/// What the README grants the relay's role.
const RELAY_GRANTS: &[&str] = &[
    "USAGE ON SCHEMA magic_events",
    "SELECT, UPDATE, DELETE ON magic_events.outbox",
    "SELECT, INSERT, UPDATE ON magic_events.relay",
    "SELECT, INSERT, UPDATE ON magic_events.published",
    "SELECT ON magic_events.version",
];

/// With a role, the relay's session and the crate's own transactions run as
/// it: the grants the README names are all it needs to start, publish,
/// answer a status and unpark.
#[sqlx::test(migrations = false)]
async fn with_a_role_it_runs_on_the_relays_grants_alone(pool: PgPool) {
    let broker = Broker::new().await;
    Events::install(&pool).await.unwrap();
    let relay = role(&pool, RELAY_GRANTS).await;
    let events = Events::start(
        pool.clone(),
        broker.jetstream.clone(),
        broker.settings.clone().role(&relay),
    )
    .await
    .unwrap();
    emit_each(&pool, &[shipped("north", 1), shipped("south", 2)]).await;
    until("both published", || async {
        broker.bodies().await.len() == 2
    })
    .await;
    until("both deleted", || async {
        count(&pool, "SELECT count(*) FROM magic_events.outbox").await == 0
    })
    .await;
    let status = events.status(&Filter::main()).await.unwrap();
    assert_eq!(status.outbox.pending, 0);
    assert_eq!(events.unpark_all(&Filter::main()).await.unwrap(), 0);
    events.stop().await;
    drop_role(&pool, &relay).await;
    broker.done().await;
}

/// The relay's session is the role's, not the login role's: a role that may
/// not delete from the outbox publishes, and fails to take the rows away.
#[sqlx::test(migrations = false)]
async fn the_relays_session_is_the_roles(pool: PgPool) {
    let broker = Broker::new().await;
    Events::install(&pool).await.unwrap();
    let relay = role(
        &pool,
        &[
            "USAGE ON SCHEMA magic_events",
            "SELECT, UPDATE ON magic_events.outbox",
            "SELECT, INSERT, UPDATE ON magic_events.relay",
            "SELECT, INSERT, UPDATE ON magic_events.published",
            "SELECT ON magic_events.version",
        ],
    )
    .await;
    let events = Events::start(
        pool.clone(),
        broker.jetstream.clone(),
        broker.settings.clone().role(&relay),
    )
    .await
    .unwrap();
    emit_each(&pool, &[shipped("north", 1)]).await;
    until("published", || async { broker.bodies().await.len() == 1 }).await;
    // A pass, and the retry after it, would have deleted it as the login role.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(broker.bodies().await.len(), 1, "sent again, deduplicated");
    assert_eq!(
        count(&pool, "SELECT count(*) FROM magic_events.outbox").await,
        1
    );
    events.stop().await;
    drop_role(&pool, &relay).await;
    broker.done().await;
}

/// A role the login role may not hold is refused at start, before anything
/// runs: the start-up checks are the role's too.
#[sqlx::test(migrations = false)]
async fn a_role_without_the_grants_is_refused_at_start(pool: PgPool) {
    let broker = Broker::new().await;
    Events::install(&pool).await.unwrap();
    let bare = role(&pool, &[]).await;
    let refused = Events::start(
        pool.clone(),
        broker.jetstream.clone(),
        broker.settings.clone().role(&bare),
    )
    .await
    .unwrap_err();
    assert!(
        refused.to_string().contains("permission denied"),
        "{refused}"
    );
    drop_role(&pool, &bare).await;
    broker.done().await;
}

/// An `Events` shared with others — request handlers asking its status — is
/// stopped through a reference: the lock let go, and a second stop waits for
/// nothing.
#[sqlx::test(migrations = false)]
async fn a_shared_events_is_stopped_through_a_reference(pool: PgPool) {
    let broker = Broker::new().await;
    Events::install(&pool).await.unwrap();
    let events = std::sync::Arc::new(
        Events::start(
            pool.clone(),
            broker.jetstream.clone(),
            broker.settings.clone(),
        )
        .await
        .unwrap(),
    );
    let handlers = events.clone();
    until("leading", || async { count(&pool, HELD).await == 1 }).await;
    events.stop().await;
    assert_eq!(count(&pool, HELD).await, 0, "the lock let go");
    tokio::time::timeout(Duration::from_secs(1), handlers.stop())
        .await
        .expect("stopped already");
    assert_eq!(
        handlers
            .status(&Filter::main())
            .await
            .unwrap()
            .outbox
            .pending,
        0
    );
    broker.done().await;
}

/// Every stream the settings declare is made as its own settings say, and
/// recorded for `emit`; the relay publishes each event to its own.
#[sqlx::test(migrations = false)]
async fn every_stream_declared_is_made_recorded_and_published_to(pool: PgPool) {
    let mut broker = Broker::live().await;
    let live = broker
        .stream("live")
        .clone()
        .retention(Duration::from_secs(600))
        .max_body(1000);
    broker.settings.others[0].1 = live.clone();
    Events::install(&pool).await.unwrap();
    let events = Events::start(
        pool.clone(),
        broker.jetstream.clone(),
        broker.settings.clone(),
    )
    .await
    .unwrap();
    let made = broker
        .jetstream
        .get_stream(&live.name)
        .await
        .unwrap()
        .cached_info()
        .config
        .clone();
    assert_eq!(made.subjects, [format!("{}.>", live.subject_prefix)]);
    assert_eq!(made.max_age, Duration::from_secs(600));
    let recorded: Vec<(String, i32)> =
        sqlx::query_as("SELECT stream, max_body FROM magic_events.relay ORDER BY stream")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        recorded,
        [(String::new(), 64 * 1024), ("live".to_owned(), 1000)]
    );

    emit_each(&pool, &[seen("north", 1)]).await;
    emit_each(&pool, &[shipped("north", 1)]).await;
    until("each on its stream", || async {
        broker.bodies().await.len() == 1 && broker.bodies_of("live").await.len() == 1
    })
    .await;
    events.stop().await;
    broker.done().await;
}

/// Streams that would publish over each other are refused at start, before
/// anything is made.
#[sqlx::test(migrations = false)]
async fn streams_that_collide_are_refused_at_start(pool: PgPool) {
    let broker = Broker::new().await;
    Events::install(&pool).await.unwrap();
    let main = broker.settings.main.clone();
    for (settings, what) in [
        (
            broker
                .settings
                .clone()
                .stream("live", StreamSettings::new("A"))
                .stream("live", StreamSettings::new("B")),
            "declared twice",
        ),
        (
            broker
                .settings
                .clone()
                .stream("live", StreamSettings::new(&main.name).subject_prefix("x")),
            "both the JetStream stream",
        ),
        (
            broker.settings.clone().stream(
                "live",
                StreamSettings::new("LIVE").subject_prefix(format!("{}.live", main.subject_prefix)),
            ),
            "overlap",
        ),
    ] {
        match Events::start(pool.clone(), broker.jetstream.clone(), settings).await {
            Err(Error::Misuse(why)) => assert!(why.contains(what), "{what}: {why}"),
            other => panic!("{what}: {other:?}"),
        }
    }
    assert!(
        broker.jetstream.get_stream(&main.name).await.is_err(),
        "nothing made"
    );
    broker.done().await;
}

/// A stream declared once and no longer: its rows stay where they are —
/// nothing publishes them, and nothing drops them — `emit` refuses its
/// events, the status of every subject lists it, undeclared, with what
/// waits; and retiring it takes it away, its rows only when told to.
#[sqlx::test(migrations = false)]
async fn a_stream_no_longer_declared_keeps_its_rows_until_retired(pool: PgPool) {
    let broker = Broker::live().await;
    Events::install(&pool).await.unwrap();
    let start =
        |settings: Settings| Events::start(pool.clone(), broker.jetstream.clone(), settings);
    start(broker.settings.clone()).await.unwrap().stop().await;
    emit_each(&pool, &[seen("north", 1)]).await;
    let without = Settings {
        others: Vec::new(),
        ..broker.settings.clone()
    };
    let events = start(without).await.unwrap();
    emit_each(&pool, &[shipped("north", 1)]).await;
    until("the main stream published, and its row gone", || async {
        count(
            &pool,
            "SELECT count(*) FROM magic_events.outbox WHERE stream = ''",
        )
        .await
            == 0
    })
    .await;
    assert_eq!(broker.bodies().await.len(), 1);
    assert!(broker.bodies_of("live").await.is_empty());
    let mut conn = pool.acquire().await.unwrap();
    match crate::emit(&mut conn, &seen("north", 2)).await {
        Err(Error::Misuse(why)) => assert!(why.contains("does not declare"), "{why}"),
        other => panic!("{other:?}"),
    }
    let status = events.status(&Filter::main()).await.unwrap();
    let live = &broker.stream("live");
    // Each stream's key, whether declared, as what, and what waits.
    type Outbox<'a> = (&'a str, bool, Option<&'a str>, Option<&'a str>, u64);
    let outboxes: Vec<Outbox<'_>> = status
        .outboxes
        .as_ref()
        .unwrap()
        .iter()
        .map(|s| {
            (
                s.key.as_str(),
                s.declared,
                s.name.as_deref(),
                s.subject_prefix.as_deref(),
                s.pending,
            )
        })
        .collect();
    assert_eq!(
        outboxes,
        [
            (
                "",
                true,
                Some(broker.settings.main.name.as_str()),
                Some(broker.settings.main.subject_prefix.as_str()),
                0
            ),
            (
                "live",
                false,
                Some(live.name.as_str()),
                Some(live.subject_prefix.as_str()),
                1
            )
        ]
    );
    match events.status(&Filter::stream("live")).await {
        Err(Error::Misuse(why)) => assert!(why.contains("no stream `live` is declared"), "{why}"),
        other => panic!("{other:?}"),
    }

    for (key, refused) in [
        ("", "never retired"),
        ("gone", "no stream `gone`"),
        ("live", "1 events wait"),
    ] {
        match events.retire(key, false).await {
            Err(Error::Misuse(why)) => assert!(why.contains(refused), "{key}: {why}"),
            other => panic!("{key}: {other:?}"),
        }
    }
    assert_eq!(events.retire("live", true).await.unwrap(), 1);
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM magic_events.outbox WHERE stream = 'live'"
        )
        .await
            + count(
                &pool,
                "SELECT count(*) FROM magic_events.relay WHERE stream = 'live'"
            )
            .await,
        0
    );
    let status = events.status(&Filter::main()).await.unwrap();
    assert_eq!(status.outboxes.unwrap().len(), 1);
    events.stop().await;
    broker.done().await;
}

/// A stream a relay declares is not retired: neither by a process whose own
/// settings declare it, nor while the leading relay does.
#[sqlx::test(migrations = false)]
async fn a_stream_a_relay_declares_is_not_retired(pool: PgPool) {
    let broker = Broker::live().await;
    Events::install(&pool).await.unwrap();
    let leading = Events::start(
        pool.clone(),
        broker.jetstream.clone(),
        broker.settings.clone(),
    )
    .await
    .unwrap();
    until("it leads", || async { count(&pool, HELD).await == 1 }).await;
    match leading.retire("live", true).await {
        Err(Error::Misuse(why)) => assert!(why.contains("these settings declare"), "{why}"),
        other => panic!("{other:?}"),
    }
    let other = Events::start(
        pool.clone(),
        broker.jetstream.clone(),
        Settings {
            others: Vec::new(),
            ..broker.settings.clone()
        },
    )
    .await
    .unwrap();
    match other.retire("live", true).await {
        Err(Error::Misuse(why)) => assert!(why.contains("the leading relay declares"), "{why}"),
        other => panic!("{other:?}"),
    }
    other.stop().await;
    leading.stop().await;
    broker.done().await;
}

/// Settings that say a stream the leading relay declares is another
/// JetStream stream, or under another prefix, are refused at start: two
/// relays would publish one key's events to two places. With no relay
/// leading, the first to start declares them anew.
#[sqlx::test(migrations = false)]
async fn a_start_that_declares_a_stream_otherwise_than_the_leader_is_refused(pool: PgPool) {
    let broker = Broker::live().await;
    Events::install(&pool).await.unwrap();
    let leading = Events::start(
        pool.clone(),
        broker.jetstream.clone(),
        broker.settings.clone(),
    )
    .await
    .unwrap();
    until("it leads", || async { count(&pool, HELD).await == 1 }).await;
    let mut moved = broker.settings.clone();
    moved.others[0].1 = moved.others[0].1.clone().subject_prefix("moved");
    match Events::start(pool.clone(), broker.jetstream.clone(), moved.clone()).await {
        Err(Error::Misuse(why)) => assert!(
            why.contains("the leading relay declares the stream `live` as")
                && why.contains("under `moved`"),
            "{why}"
        ),
        other => panic!("{other:?}"),
    }
    leading.stop().await;
    Events::start(pool.clone(), broker.jetstream.clone(), moved)
        .await
        .unwrap()
        .stop()
        .await;
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT subject_prefix FROM magic_events.relay WHERE stream = 'live'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        "moved"
    );
    broker.done().await;
}

/// A stream added while a relay of older settings leads: `emit` refuses its
/// events until a relay that publishes it leads — and the status of it
/// answers meanwhile, nothing published, nothing failed.
#[sqlx::test(migrations = false)]
async fn a_stream_is_taken_once_a_relay_that_declares_it_leads(pool: PgPool) {
    let broker = Broker::live().await;
    Events::install(&pool).await.unwrap();
    let older = Events::start(
        pool.clone(),
        broker.jetstream.clone(),
        Settings {
            others: Vec::new(),
            ..broker.settings.clone()
        },
    )
    .await
    .unwrap();
    until("it leads", || async { count(&pool, HELD).await == 1 }).await;
    let newer = Events::start(
        pool.clone(),
        broker.jetstream.clone(),
        broker.settings.clone(),
    )
    .await
    .unwrap();
    let mut conn = pool.acquire().await.unwrap();
    match crate::emit(&mut conn, &seen("north", 1)).await {
        Err(Error::Misuse(why)) => assert!(why.contains("does not declare"), "{why}"),
        other => panic!("{other:?}"),
    }
    let status = newer.status(&Filter::stream("live")).await.unwrap();
    assert_eq!(
        (
            status.relay.failures,
            status.relay.last_published_at,
            status.outbox.pending
        ),
        (0, None, 0)
    );

    older.stop().await;
    until("the newer relay declares `live`", || async {
        count(
            &pool,
            "SELECT count(*) FROM magic_events.relay WHERE stream = 'live' AND declared",
        )
        .await
            == 1
    })
    .await;
    crate::emit(&mut conn, &seen("north", 1)).await.unwrap();
    until("published to `live`", || async {
        broker.bodies_of("live").await.len() == 1
    })
    .await;
    newer.stop().await;
    broker.done().await;
}
