//! The schema as a host installs it: once or again, with migrations of its
//! own or without, and never over a schema that is not the crate's.

use super::*;
use crate::tests::{count, until, Broker};
use crate::Events;
use sqlx::PgPool;

async fn installed(pool: &PgPool) -> Option<i32> {
    version(&mut pool.acquire().await.unwrap()).await.unwrap()
}

#[sqlx::test(migrations = false)]
async fn installing_twice_is_installing_once(pool: PgPool) {
    assert_eq!(installed(&pool).await, None);
    Events::install(&pool).await.unwrap();
    Events::install(&pool).await.unwrap();
    assert_eq!(installed(&pool).await, Some(SCHEMA_VERSION));
    check(&mut pool.acquire().await.unwrap()).await.unwrap();
}

/// A host with migrations of its own runs each step, in order, and has what
/// `install` makes.
#[sqlx::test(migrations = false)]
async fn every_step_a_host_runs_is_what_install_runs(pool: PgPool) {
    let mut tx = pool.begin().await.unwrap();
    for step in 1..=SCHEMA_VERSION {
        sqlx::raw_sql(&migration(step).unwrap())
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    assert_eq!(installed(&pool).await, Some(SCHEMA_VERSION));
    Events::install(&pool).await.unwrap();
    assert_eq!(migration(0), None);
    assert_eq!(migration(SCHEMA_VERSION + 1), None);
    assert_eq!(migration(-1), None);
}

#[sqlx::test(migrations = false)]
async fn a_schema_of_a_later_build_is_refused(pool: PgPool) {
    Events::install(&pool).await.unwrap();
    sqlx::query("UPDATE magic_events.version SET version = $1")
        .bind(SCHEMA_VERSION + 1)
        .execute(&pool)
        .await
        .unwrap();
    for refused in [
        Events::install(&pool).await,
        check(&mut pool.acquire().await.unwrap()).await,
    ] {
        match refused {
            Err(Error::Misuse(why)) => assert!(why.contains("later build"), "{why}"),
            other => panic!("{other:?}"),
        }
    }
}

#[sqlx::test(migrations = false)]
async fn a_schema_of_that_name_that_is_not_the_crates_is_refused(pool: PgPool) {
    sqlx::raw_sql("CREATE SCHEMA magic_events; CREATE TABLE magic_events.mine (id INT)")
        .execute(&pool)
        .await
        .unwrap();
    for refused in [
        Events::install(&pool).await,
        check(&mut pool.acquire().await.unwrap()).await,
    ] {
        match refused {
            Err(Error::Misuse(why)) => assert!(why.contains("not this crate's"), "{why}"),
            other => panic!("{other:?}"),
        }
    }
    let mine: bool = sqlx::query_scalar("SELECT to_regclass('magic_events.mine') IS NOT NULL")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(mine, "left as it was");
}

#[sqlx::test(migrations = false)]
async fn a_relay_needs_the_schema_installed(pool: PgPool) {
    match check(&mut pool.acquire().await.unwrap()).await {
        Err(Error::Misuse(why)) => assert!(why.contains("not installed"), "{why}"),
        other => panic!("{other:?}"),
    }
}

/// A truncate would drop committed events without a trace.
#[sqlx::test(migrations = false)]
async fn the_outbox_cannot_be_truncated(pool: PgPool) {
    Events::install(&pool).await.unwrap();
    let refused = sqlx::query("TRUNCATE magic_events.outbox")
        .execute(&pool)
        .await
        .unwrap_err()
        .to_string();
    assert!(refused.contains("holds committed events"), "{refused}");
}

/// The outbox is a queue: vacuumed by the thousand dead rows, whatever its
/// size, and read by the relay and the status each through an index.
#[sqlx::test(migrations = false)]
async fn the_outbox_is_kept_as_a_queue(pool: PgPool) {
    Events::install(&pool).await.unwrap();
    let options: Vec<String> = sqlx::query_scalar(
        "SELECT unnest(reloptions) FROM pg_class WHERE oid = 'magic_events.outbox'::regclass",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        options.contains(&"autovacuum_vacuum_scale_factor=0".to_owned()),
        "{options:?}"
    );
    let indexes: Vec<String> = sqlx::query_scalar(
        "SELECT indexname::text FROM pg_indexes WHERE schemaname = 'magic_events' \
            AND tablename = 'outbox' ORDER BY 1",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    for index in ["outbox_parked", "outbox_pending"] {
        assert!(indexes.contains(&index.to_owned()), "{indexes:?}");
    }
    // A stream's rows, in order: what a pass of one stream reads.
    let pending: String =
        sqlx::query_scalar("SELECT pg_get_indexdef('magic_events.outbox_pending'::regclass)")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        pending.ends_with("(stream, id) WHERE (parked_at IS NULL)"),
        "{pending}"
    );
}

/// What `emit` ran in builds of schema 1: a writer of such a build may still
/// run while a database is upgraded.
const V1_EMIT: &str = "WITH bound AS ( \
         SELECT coalesce((SELECT max_body FROM magic_events.relay WHERE id = 1), 65536) AS max_body), \
     written AS ( \
         INSERT INTO magic_events.outbox (event_id, subject, type, headers, body) \
         SELECT $1, $2, 'order.shipped.v1', '{\"ce-type\": \"order.shipped.v1\"}', $3 FROM bound \
          WHERE length($3) <= bound.max_body \
         RETURNING 1) \
     SELECT max_body, EXISTS (SELECT 1 FROM written) FROM bound";

async fn v1_emit(pool: &PgPool, store: &str) {
    let (_, written): (i32, bool) = sqlx::query_as(V1_EMIT)
        .bind(uuid::Uuid::now_v7())
        .bind(format!("orders.{store}.shipped"))
        .bind(format!(r#"{{"store":"{store}"}}"#).into_bytes())
        .fetch_one(pool)
        .await
        .unwrap();
    assert!(written);
}

/// A database of schema 1 with events waiting — and a relay's limit, and
/// counts — upgraded in place: what was there is the main stream's, a
/// writer of the build before goes on writing to it, and the relay
/// publishes it all there.
#[sqlx::test(migrations = false)]
async fn a_database_of_version_1_upgrades_in_place(pool: PgPool) {
    sqlx::raw_sql(&migration(1).unwrap())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql(
        "UPDATE magic_events.relay SET max_body = 1000, failures = 2 WHERE id = 1; \
         INSERT INTO magic_events.published (day, type, count) \
         VALUES ((now() AT TIME ZONE 'UTC')::date, 'order.shipped.v1', 5)",
    )
    .execute(&pool)
    .await
    .unwrap();
    v1_emit(&pool, "north").await;
    v1_emit(&pool, "south").await;

    Events::install(&pool).await.unwrap();
    assert_eq!(installed(&pool).await, Some(SCHEMA_VERSION));
    let relay: Vec<(String, Option<i32>, Option<i32>, i32)> = sqlx::query_as(
        "SELECT stream, id, max_body, failures FROM magic_events.relay ORDER BY stream",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(relay, [(String::new(), Some(1), Some(1000), 2)]);
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM magic_events.outbox WHERE stream = ''"
        )
        .await,
        2
    );
    assert_eq!(
        count(
            &pool,
            "SELECT count FROM magic_events.published WHERE stream = ''"
        )
        .await,
        5
    );
    v1_emit(&pool, "east").await;

    let broker = Broker::new().await;
    let events = Events::start(
        pool.clone(),
        broker.jetstream.clone(),
        broker.settings.clone(),
    )
    .await
    .unwrap();
    until("the three published", || async {
        broker.bodies().await.len() == 3
    })
    .await;
    events.stop().await;
    let prefix = &broker.settings.main.subject_prefix;
    let subjects: Vec<String> = broker
        .messages()
        .await
        .into_iter()
        .map(|m| m.subject)
        .collect();
    assert_eq!(
        subjects,
        ["north", "south", "east"].map(|s| format!("{prefix}.orders.{s}.shipped"))
    );
    assert_eq!(
        count(
            &pool,
            "SELECT count FROM magic_events.published WHERE stream = ''"
        )
        .await,
        8
    );
    broker.done().await;
}
