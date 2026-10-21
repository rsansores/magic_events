//! What `emit` costs a writer, and how fast the relay publishes: ignored by
//! default, run one at a time on a quiet machine —
//!
//! ```sh
//! cargo test -p magic_events --release bench_ -- --ignored --nocapture --test-threads 1
//! ```

use crate::tests::{idle, shipped, Broker};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::{Duration, Instant};

const EVENTS: u64 = 20_000;

/// What `emit` adds to a writer: `EVENTS` rows of a table like the shop's
/// written in one transaction, each with an event or without — the
/// statement's own cost, apart from the commit (whose wait for the disk is
/// the same either way, and much larger).
#[sqlx::test(migrations = false)]
#[ignore]
async fn bench_emit_overhead(pool: PgPool) {
    crate::Events::install(&pool).await.unwrap();
    sqlx::raw_sql("CREATE TABLE orders (id BIGSERIAL PRIMARY KEY, store TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    let run = |with_event: bool| {
        let pool = pool.clone();
        async move {
            let mut tx = pool.begin().await.unwrap();
            let start = Instant::now();
            for n in 0..EVENTS {
                sqlx::query("INSERT INTO orders (store) VALUES ('north')")
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                if with_event {
                    crate::emit(&mut tx, &shipped("north", n)).await.unwrap();
                }
            }
            let took = start.elapsed();
            tx.commit().await.unwrap();
            took
        }
    };
    // Warm both paths, then measure each.
    let _ = (run(false).await, run(true).await);
    let without = run(false).await;
    let with = run(true).await;
    let per = |d: Duration| d.as_secs_f64() * 1e6 / EVENTS as f64;
    println!(
        "emit: {:.0} µs a write without an event, {:.0} µs with one: {:.0} µs for the event",
        per(without),
        per(with),
        per(with) - per(without)
    );
}

/// The relay draining an outbox of `EVENTS` rows into a stream on file, at
/// each `in_flight`.
#[sqlx::test(migrations = false)]
#[ignore]
async fn bench_relay_throughput(pool: PgPool) {
    for in_flight in [1, 16, 64] {
        let mut broker = Broker::new().await;
        broker.settings = broker
            .settings
            .clone()
            .storage(crate::Storage::File)
            .in_flight(in_flight);
        let events = idle(&pool, &broker, Arc::new(broker.jetstream.clone())).await;
        let mut tx = pool.begin().await.unwrap();
        for n in 0..EVENTS {
            crate::emit(&mut tx, &shipped("north", n)).await.unwrap();
        }
        tx.commit().await.unwrap();
        let start = Instant::now();
        while events.pass_now().await.unwrap().unwrap() > 0 {}
        let took = start.elapsed();
        println!(
            "relay: {:.0} events a second at in_flight {in_flight}",
            EVENTS as f64 / took.as_secs_f64()
        );
        broker.done().await;
    }
}
