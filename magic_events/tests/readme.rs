//! The README's code, compiled: every Rust block of `README.md` is here,
//! line for line (indentation aside), inside a function that gives it what
//! it names — and `every_rust_block_of_the_readme_is_here` holds the two to
//! each other, so the README cannot show code that does not build.

#![allow(dead_code, clippy::unused_async)]

use async_nats::jetstream::Context;
use sqlx::PgPool;

type StoreId = u32;
type OrderId = i64;
type TenantId = String;
type Error = Box<dyn std::error::Error + Send + Sync>;

async fn hero(
    pool: PgPool,
    jetstream: Context,
    store: StoreId,
    ids: Vec<OrderId>,
) -> Result<(), Error> {
    use magic_events::{emit, Event, Events, Settings};

    // An event is a serde type that derives Event: its type, its version, and
    // the subject it goes to, each {field} checked at compile time.
    #[derive(serde::Serialize, Event)]
    #[event(
        type = "order.shipped",
        version = 1,
        subject = "orders.{store}.shipped"
    )]
    pub struct OrderShipped {
        pub store: StoreId,
        pub ids: Vec<OrderId>,
    }

    // Once, in main: checks the schema and the stream, and runs the relay.
    let settings = Settings::new("SHOP_EVENTS", "//shop.example.com");
    let events = Events::start(pool.clone(), jetstream, settings).await?;

    // Wherever you write: one more statement of the transaction.
    let mut tx = pool.begin().await?;
    sqlx::query("UPDATE orders SET shipped_at = now() WHERE id = ANY($1)")
        .bind(&ids)
        .execute(&mut *tx)
        .await?;
    emit(&mut tx, &OrderShipped { store, ids }).await?;
    tx.commit().await?; // Committed: it is published. Rolled back: it never was.

    // At shutdown.
    events.stop().await;
    Ok(())
}

mod enums {
    use super::*;

    #[derive(serde::Serialize, serde::Deserialize, magic_events::Event)]
    pub enum Catalog {
        #[event(
            type = "product.updated",
            version = 1,
            subject = "catalog.{store}.updated"
        )]
        ProductUpdated { store: StoreId, ids: Vec<i64> },
        #[event(
            type = "product.archived",
            version = 1,
            subject = "catalog.{store}.archived"
        )]
        ProductArchived { store: StoreId, ids: Vec<i64> },
        // A body that changes for its readers is a new version, a variant
        // of its own: `ce-type` says which, `product.archived.v2`.
        #[event(
            type = "product.archived",
            version = 2,
            subject = "catalog.{store}.archived"
        )]
        ProductArchivedV2 {
            store: StoreId,
            ids: Vec<i64>,
            reason: String,
        },
    }
}

async fn install(pool: PgPool) -> Result<(), Error> {
    // A host with no migrations of its own: once, at start, before anything emits.
    magic_events::Events::install(&pool).await?;
    Ok(())
}

fn migrations() {
    // A host with migrations of its own: one migration per step, each written once.
    for step in 1..=magic_events::SCHEMA_VERSION {
        let sql = magic_events::migration(step).expect("a step of this build");
        std::fs::write(format!("migrations/magic_events_{step}.sql"), sql).unwrap();
    }
}

fn settings() -> magic_events::Settings {
    use magic_events::{Settings, Storage};
    use std::time::Duration;

    Settings::new("SHOP_EVENTS", "//shop.example.com")
        .subject_prefix("shop.events") // the lowercase stream name by default
        .retention(Duration::from_secs(30 * 24 * 60 * 60)) // 7 days by default
        .replicas(3) // 1 by default
        .storage(Storage::File) // the default
}

#[cfg(feature = "consume")]
async fn billing(jetstream: Context) -> Result<(), Error> {
    use enums::Catalog;
    use magic_events::consume::{consume, Delivery};

    consume(
        &jetstream,
        "SHOP_EVENTS",
        "billing",
        "shop_events.catalog.>",
        |delivery: Delivery<Catalog>| async move {
            // delivery.id is the ce-id: the same however often it comes.
            if let Catalog::ProductUpdated { store, ids } = delivery.event {
                println!("invoice the products {ids:?} of store {store}");
            }
            Ok(())
        },
    )
    .await?;
    Ok(())
}

async fn status(events: magic_events::Events) -> Result<String, Error> {
    use magic_events::status::Filter;

    // Behind your own authorization: the operator's view, or the subjects
    // this caller may see.
    let status = events.status(&Filter::all()).await?;
    let one_store = events
        .status(&Filter::subject("shop_events.orders.7.>"))
        .await?;
    let last_ten = events.recent(&Filter::all(), 10).await?;
    Ok(serde_json::to_string(&(status, one_store, last_ten))?)
}

async fn unpark(events: magic_events::Events, id: i64) -> Result<(), Error> {
    // After fixing what the broker refused: send it again with the next pass.
    events.unpark(id).await?;
    events.unpark_all().await?;
    Ok(())
}

mod tenants {
    use super::*;

    #[derive(serde::Serialize, magic_events::Event)]
    #[event(
        type = "order.placed",
        version = 1,
        subject = "{tenant}.orders.{store}.placed"
    )]
    pub struct OrderPlaced {
        pub tenant: TenantId,
        pub store: StoreId,
        pub ids: Vec<OrderId>,
    }
}

async fn no_write(pool: PgPool) -> Result<(), Error> {
    #[derive(serde::Serialize, magic_events::Event)]
    #[event(type = "store.offline", version = 1, subject = "stores.offline")]
    pub struct StoresOffline {
        pub ids: Vec<i64>,
    }

    // Every minute: the fact first, then its event, in one transaction.
    let mut tx = pool.begin().await?;
    let ids: Vec<i64> = sqlx::query_scalar(
        "UPDATE stores SET offline_at = now() \
          WHERE offline_at IS NULL AND last_seen_at < now() - interval '5 minutes' \
          RETURNING id",
    )
    .fetch_all(&mut *tx)
    .await?;
    if !ids.is_empty() {
        magic_events::emit(&mut tx, &StoresOffline { ids }).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// The lines of `text` that hold code, as rustfmt leaves them, without
/// their indentation.
fn code(text: &str) -> Vec<String> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_owned)
        .collect()
}

#[test]
fn every_rust_block_of_the_readme_is_here() {
    let root = env!("CARGO_MANIFEST_DIR");
    let readme = std::fs::read_to_string(format!("{root}/../README.md")).unwrap();
    let here = code(&std::fs::read_to_string(format!("{root}/tests/readme.rs")).unwrap());
    let mut blocks = 0;
    let mut rest = readme.as_str();
    while let Some(start) = rest.find("```rust\n") {
        let body = &rest[start + 8..];
        let end = body.find("```").expect("a block that ends");
        let block = code(&body[..end]);
        assert!(
            here.windows(block.len()).any(|w| w == block.as_slice()),
            "a Rust block of the README is not in tests/readme.rs as it is:\n{}",
            block.join("\n")
        );
        blocks += 1;
        rest = &body[end..];
    }
    assert!(blocks >= 8, "the README's Rust blocks: {blocks}");
}
