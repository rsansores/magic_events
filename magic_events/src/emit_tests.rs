//! `emit` as a writer calls it: one row per event, in the writing
//! transaction, or an error at the call and nothing written.

use super::*;
use crate::Events;
use serde::Serialize;
use sqlx::PgPool;

#[derive(Serialize, crate::Event)]
#[event(
    type = "order.shipped",
    version = 1,
    subject = "orders.{store}.shipped"
)]
struct OrderShipped {
    store: String,
    ids: Vec<u64>,
}

#[derive(Serialize, crate::Event)]
enum Catalog {
    #[event(
        type = "product.updated",
        version = 2,
        subject = "catalog.{store}.product.updated",
        ce_subject = "{store}/one \"two\""
    )]
    ProductUpdated { store: String, ids: Vec<u64> },
}

fn shipped(store: &str) -> OrderShipped {
    OrderShipped {
        store: store.to_owned(),
        ids: vec![1, 2],
    }
}

/// The outbox's rows: subject, type, headers and body, in order.
async fn outbox(pool: &PgPool) -> Vec<(String, String, serde_json::Value, String)> {
    sqlx::query_as::<_, (String, String, serde_json::Value, Vec<u8>)>(
        "SELECT subject, type, headers, body FROM magic_events.outbox ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|(s, t, h, b)| (s, t, h, String::from_utf8(b).unwrap()))
    .collect()
}

#[sqlx::test(migrations = false)]
async fn an_event_is_one_row_of_the_transaction_that_emits_it(pool: PgPool) {
    Events::install(&pool).await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    let first = emit(&mut tx, &shipped("north")).await.unwrap();
    let second = emit(
        &mut tx,
        &Catalog::ProductUpdated {
            store: "south".to_owned(),
            ids: vec![7],
        },
    )
    .await
    .unwrap();
    assert!(first < second, "ids in emit order");
    tx.commit().await.unwrap();
    assert_eq!(
        outbox(&pool).await,
        [
            (
                "orders.north.shipped".to_owned(),
                "order.shipped.v1".to_owned(),
                serde_json::json!({ "ce-type": "order.shipped.v1" }),
                r#"{"store":"north","ids":[1,2]}"#.to_owned()
            ),
            (
                "catalog.south.product.updated".to_owned(),
                "product.updated.v2".to_owned(),
                serde_json::json!({
                    "ce-type": "product.updated.v2",
                    "ce-subject": "south/one%20%22two%22"
                }),
                r#"{"store":"south","ids":[7]}"#.to_owned()
            ),
        ]
    );
}

#[sqlx::test(migrations = false)]
async fn a_rolled_back_transaction_leaves_no_event(pool: PgPool) {
    Events::install(&pool).await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    emit(&mut tx, &shipped("north")).await.unwrap();
    tx.rollback().await.unwrap();
    assert!(outbox(&pool).await.is_empty());
}

#[sqlx::test(migrations = false)]
async fn a_subject_no_subject_may_hold_is_refused_at_emit(pool: PgPool) {
    Events::install(&pool).await.unwrap();
    let mut conn = pool.acquire().await.unwrap();
    for store in ["", "a.b", "*", ">", "two words", "ñ", &"x".repeat(1025)] {
        match emit(&mut conn, &shipped(store)).await {
            Err(Error::Invalid(why)) => assert!(why.contains("is no subject"), "{why}"),
            other => panic!("{store:?}: {other:?}"),
        }
    }
    emit(&mut conn, &shipped(&"x".repeat(1024))).await.unwrap();
    assert_eq!(outbox(&pool).await.len(), 1);
}

#[sqlx::test(migrations = false)]
async fn a_body_over_the_relays_limit_is_refused_at_emit(pool: PgPool) {
    Events::install(&pool).await.unwrap();
    let mut conn = pool.acquire().await.unwrap();
    let large = |n: usize| OrderShipped {
        store: "north".to_owned(),
        ids: vec![1; n],
    };
    // 64 KiB unless a relay set another: `{"store":"north","ids":[` is 24
    // bytes, each id 2 but the last, and `]}` 2.
    let fits = (64 * 1024 - 24 - 2_usize).div_ceil(2);
    emit(&mut conn, &large(fits)).await.unwrap();
    match emit(&mut conn, &large(fits + 1)).await {
        Err(Error::Invalid(why)) => assert!(why.contains("65536 at most"), "{why}"),
        other => panic!("{other:?}"),
    }
    sqlx::query("UPDATE magic_events.relay SET max_body = 30")
        .execute(&mut *conn)
        .await
        .unwrap();
    match emit(&mut conn, &large(3)).await {
        Err(Error::Invalid(why)) => assert!(why.contains("30 at most"), "{why}"),
        other => panic!("{other:?}"),
    }
    emit(&mut conn, &large(1)).await.unwrap();
    assert_eq!(outbox(&pool).await.len(), 2);
}

#[sqlx::test(migrations = false)]
async fn emit_needs_the_schema(pool: PgPool) {
    let mut conn = pool.acquire().await.unwrap();
    match emit(&mut conn, &shipped("north")).await {
        Err(Error::Database(e)) => assert!(e.to_string().contains("magic_events"), "{e}"),
        other => panic!("{other:?}"),
    }
}
