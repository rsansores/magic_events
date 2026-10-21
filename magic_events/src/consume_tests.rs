//! `consume` as a consuming system runs it: every event once, in order, by
//! its id; again after a handler fails; never one it cannot read; and from
//! where a durable consumer stopped.

use super::*;
use crate::tests::{emit_each, idle, shipped, until, Broker};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::{Arc, Mutex};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Ids {
    store: String,
    ids: Vec<u64>,
}

/// An enum serde renames: decoding finds a variant by its place, not its name.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, crate::Event)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum Orders {
    #[event(type = "order.placed", version = 1, subject = "orders.{store}.placed")]
    Placed {
        store: String,
        #[serde(rename = "orders")]
        ids: Vec<u64>,
    },
    #[event(
        type = "order.shipped",
        version = 2,
        subject = "orders.{store}.shipped"
    )]
    Shipped(Ids),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, crate::Event)]
#[event(
    type = "order.shipped",
    version = 1,
    subject = "orders.{store}.shipped"
)]
struct OrderShipped {
    store: String,
    n: u64,
}

/// What `emit` writes is what `decode` reads back, for a struct and for
/// each kind of variant.
#[test]
fn what_is_emitted_decodes_as_it_was() {
    let placed = Orders::Placed {
        store: "north".to_owned(),
        ids: vec![1, 2],
    };
    let shipped = Orders::Shipped(Ids {
        store: "south".to_owned(),
        ids: vec![3],
    });
    for event in [placed, shipped] {
        let d = crate::Event::__describe(&event);
        let body = crate::envelope::body(&event, true).unwrap();
        let ty = format!("{}.v{}", d.r#type, d.version);
        assert_eq!(decode::<Orders>(&ty, &body).unwrap(), event);
    }
    let one = OrderShipped {
        store: "north".to_owned(),
        n: 7,
    };
    let body = crate::envelope::body(&one, false).unwrap();
    assert_eq!(
        decode::<OrderShipped>("order.shipped.v1", &body).unwrap(),
        one
    );

    match decode::<Orders>("order.shipped.v3", b"{}") {
        Err(Unread::Unknown(why)) => assert!(why.contains("no type this consumer declares")),
        other => panic!("{other:?}"),
    }
    for body in [&b"{\"store\":1}"[..], b"{\"store\":\"a\",\"orders\":[]} x"] {
        match decode::<Orders>("order.placed.v1", body) {
            Err(Unread::Malformed(why)) => assert!(why.contains("does not decode"), "{why}"),
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(decoded("a%20b%"), "a b%");
    assert_eq!(decoded("%E2%82%AC%zz"), "€%zz");
}

/// Each event a consumer handled: its id, the event, and how often it had
/// been delivered before.
type Got = Arc<Mutex<Vec<(String, Orders, u64)>>>;

/// Runs `consume` for `durable` in a task, collecting each event and how
/// often it had been delivered; `fail_first` fails the first delivery of
/// each event.
fn consuming(
    broker: &Broker,
    durable: &str,
    filter: String,
    fail_first: bool,
) -> (tokio::task::JoinHandle<Result<(), Error>>, Got) {
    let got: Got = Arc::default();
    let task = tokio::spawn({
        let (jetstream, stream, durable) = (
            broker.jetstream.clone(),
            broker.settings.stream.clone(),
            durable.to_owned(),
        );
        let got = got.clone();
        async move {
            consume(
                &jetstream,
                &stream,
                &durable,
                &filter,
                move |d: Delivery<Orders>| {
                    let got = got.clone();
                    async move {
                        if fail_first && d.redelivered == 0 {
                            return Err("not yet".into());
                        }
                        got.lock().unwrap().push((d.id, d.event, d.redelivered));
                        Ok(())
                    }
                },
            )
            .await
        }
    });
    (task, got)
}

async fn publish(pool: &PgPool, events: &crate::Events, orders: &[Orders]) -> Vec<String> {
    let mut ids = Vec::new();
    for order in orders {
        let mut tx = pool.begin().await.unwrap();
        ids.push(crate::emit(&mut tx, order).await.unwrap().to_string());
        tx.commit().await.unwrap();
    }
    while events.pass_now().await.unwrap().unwrap() > 0 {}
    ids
}

fn placed(n: u64) -> Orders {
    Orders::Placed {
        store: "north".to_owned(),
        ids: vec![n],
    }
}

#[sqlx::test(migrations = false)]
async fn a_consumer_gets_every_event_once_in_order_with_its_id(pool: PgPool) {
    let broker = Broker::new().await;
    let events = idle(&pool, &broker, Arc::new(broker.jetstream.clone())).await;
    let orders = [
        placed(1),
        Orders::Shipped(Ids {
            store: "north".to_owned(),
            ids: vec![1],
        }),
        placed(2),
    ];
    let ids = publish(&pool, &events, &orders).await;
    let prefix = &broker.settings.subject_prefix;
    let (task, got) = consuming(&broker, "billing", format!("{prefix}.orders.>"), false);
    until("three events", || async { got.lock().unwrap().len() == 3 }).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    task.abort();
    let got = got.lock().unwrap().clone();
    assert_eq!(got.iter().map(|g| g.0.clone()).collect::<Vec<_>>(), ids);
    assert_eq!(
        got.iter().map(|g| &g.1).collect::<Vec<_>>(),
        orders.iter().collect::<Vec<_>>()
    );
    assert!(got.iter().all(|g| g.2 == 0), "each once");
    broker.done().await;
}

#[sqlx::test(migrations = false)]
async fn a_handler_that_fails_has_the_event_again(pool: PgPool) {
    let broker = Broker::new().await;
    let events = idle(&pool, &broker, Arc::new(broker.jetstream.clone())).await;
    let ids = publish(&pool, &events, &[placed(1)]).await;
    let prefix = &broker.settings.subject_prefix;
    let (task, got) = consuming(&broker, "billing", format!("{prefix}.orders.>"), true);
    until("delivered again", || async {
        got.lock().unwrap().len() == 1
    })
    .await;
    task.abort();
    let got = got.lock().unwrap().clone();
    assert_eq!((got[0].0.as_str(), got[0].2), (ids[0].as_str(), 1));
    broker.done().await;
}

/// The consumer, upgraded: it reads the orders, and `order.shipped`
/// version 1 too.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, crate::Event)]
enum Upgraded {
    #[event(type = "order.placed", version = 1, subject = "orders.{store}.placed")]
    Placed {
        store: String,
        #[serde(rename = "orders")]
        ids: Vec<u64>,
    },
    #[event(
        type = "order.shipped",
        version = 1,
        subject = "orders.{store}.shipped"
    )]
    ShippedV1 { store: String, n: u64 },
}

/// `order.placed` version 1, written by a producer that got its body wrong.
#[derive(Serialize, crate::Event)]
#[event(type = "order.placed", version = 1, subject = "orders.{store}.placed")]
struct Misplaced {
    store: String,
    n: u64,
}

/// Runs `consume` of `Upgraded` for `durable` until it handled `wanted`
/// events, or a second and a half passed: what it handled.
async fn upgraded(broker: &Broker, durable: &str, filter: String, wanted: usize) -> Vec<Upgraded> {
    let got: Arc<Mutex<Vec<Upgraded>>> = Arc::default();
    let task = tokio::spawn({
        let (jetstream, stream, durable) = (
            broker.jetstream.clone(),
            broker.settings.stream.clone(),
            durable.to_owned(),
        );
        let got = got.clone();
        async move {
            consume(
                &jetstream,
                &stream,
                &durable,
                &filter,
                move |d: Delivery<Upgraded>| {
                    let got = got.clone();
                    async move {
                        got.lock().unwrap().push(d.event);
                        Ok(())
                    }
                },
            )
            .await
        }
    });
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(1500);
    while got.lock().unwrap().len() < wanted && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    task.abort();
    let got = got.lock().unwrap().clone();
    got
}

/// An event of a type the consumer does not declare yet — the producer
/// upgraded first — holds back nothing behind it, and is not lost: the
/// consumer's next build, which declares it, reads it.
#[sqlx::test(migrations = false)]
async fn an_event_of_a_type_not_declared_waits_for_a_build_that_declares_it(pool: PgPool) {
    let broker = Broker::new().await;
    let events = idle(&pool, &broker, Arc::new(broker.jetstream.clone())).await;
    // `order.shipped` version 1, which `Orders` does not declare.
    emit_each(&pool, &[shipped("north", 1)]).await;
    let ids = publish(&pool, &events, &[placed(2)]).await;
    let prefix = &broker.settings.subject_prefix;
    let (task, got) = consuming(&broker, "billing", format!("{prefix}.orders.>"), false);
    until("the readable one", || async {
        got.lock().unwrap().len() == 1
    })
    .await;
    task.abort();
    let _ = task.await;
    assert_eq!(got.lock().unwrap()[0].0, ids[0]);

    let later = upgraded(&broker, "billing", format!("{prefix}.orders.>"), 1).await;
    assert_eq!(
        later,
        [Upgraded::ShippedV1 {
            store: "north".to_owned(),
            n: 1
        }]
    );
    broker.done().await;
}

/// An event of a type the consumer declares, whose body does not decode as
/// that type, is never delivered to it again: no build reads it.
#[sqlx::test(migrations = false)]
async fn an_event_whose_body_does_not_decode_is_never_delivered_again(pool: PgPool) {
    let broker = Broker::new().await;
    let events = idle(&pool, &broker, Arc::new(broker.jetstream.clone())).await;
    let mut tx = pool.begin().await.unwrap();
    crate::emit(
        &mut tx,
        &Misplaced {
            store: "north".to_owned(),
            n: 1,
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let ids = publish(&pool, &events, &[placed(2)]).await;
    let prefix = &broker.settings.subject_prefix;
    let (task, got) = consuming(&broker, "billing", format!("{prefix}.orders.>"), false);
    until("the readable one", || async {
        got.lock().unwrap().len() == 1
    })
    .await;
    task.abort();
    let _ = task.await;
    assert_eq!(got.lock().unwrap()[0].0, ids[0]);
    assert!(
        upgraded(&broker, "billing", format!("{prefix}.orders.>"), 1)
            .await
            .is_empty()
    );
    broker.done().await;
}

/// A durable consumer stopped and started again takes up where it stopped:
/// what it acknowledged does not come again.
#[sqlx::test(migrations = false)]
async fn a_durable_consumer_resumes_where_it_stopped(pool: PgPool) {
    let broker = Broker::new().await;
    let events = idle(&pool, &broker, Arc::new(broker.jetstream.clone())).await;
    let prefix = broker.settings.subject_prefix.clone();
    publish(&pool, &events, &[placed(1), placed(2)]).await;
    let (task, got) = consuming(&broker, "billing", format!("{prefix}.orders.>"), false);
    until("the first two", || async { got.lock().unwrap().len() == 2 }).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    task.abort();
    let _ = task.await;

    let later = publish(&pool, &events, &[placed(3)]).await;
    let (task, got) = consuming(&broker, "billing", format!("{prefix}.orders.>"), false);
    until("the third", || async { !got.lock().unwrap().is_empty() }).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    task.abort();
    let got = got.lock().unwrap().clone();
    assert_eq!(got.iter().map(|g| g.0.clone()).collect::<Vec<_>>(), later);
    broker.done().await;
}

/// A durable consumer that reads another filter is refused, not read: its
/// events are not `E`'s, and each would be set aside for good.
#[sqlx::test(migrations = false)]
async fn a_durable_of_that_name_reading_another_filter_is_refused(pool: PgPool) {
    let broker = Broker::new().await;
    let events = idle(&pool, &broker, Arc::new(broker.jetstream.clone())).await;
    let prefix = broker.settings.subject_prefix.clone();
    publish(&pool, &events, &[placed(1)]).await;
    let (task, got) = consuming(&broker, "billing", format!("{prefix}.orders.>"), false);
    until("the first", || async { got.lock().unwrap().len() == 1 }).await;
    task.abort();
    let _ = task.await;

    let (task, _) = consuming(&broker, "billing", format!("{prefix}.catalog.>"), false);
    match task.await.unwrap() {
        Err(Error::Misuse(why)) => assert!(why.contains("its filter changed"), "{why}"),
        other => panic!("{other:?}"),
    }
    broker.done().await;
}
