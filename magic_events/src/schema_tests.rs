//! The schema as a host installs it: once or again, with migrations of its
//! own or without, and never over a schema that is not the crate's.

use super::*;
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
