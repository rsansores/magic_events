//! The shop's events, and what its three programs share.

use magic_events::Event;
use serde::{Deserialize, Serialize};

/// Everything the shop says happened. The bodies are thin — a store and
/// ids — and a consumer reads the rest where the shop keeps it.
#[derive(Serialize, Deserialize, Debug, Event)]
pub enum Shop {
    #[event(type = "order.placed", version = 1, subject = "orders.{store}.placed")]
    OrderPlaced { store: String, ids: Vec<i64> },
    #[event(
        type = "order.shipped",
        version = 1,
        subject = "orders.{store}.shipped"
    )]
    OrderShipped { store: String, ids: Vec<i64> },
}

/// The stream the shop's events go to; every subject starts `shop_events.`.
pub const STREAM: &str = "SHOP_EVENTS";

/// The shop's PostgreSQL: `DATABASE_URL`, or the one `docker compose up`
/// starts.
pub async fn database() -> sqlx::PgPool {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://shop:shop@localhost:5434/shop".to_owned());
    sqlx::PgPool::connect(&url)
        .await
        .expect("the shop's PostgreSQL")
}

/// The shop's JetStream: `NATS_URL`, or the one `docker compose up` starts.
pub async fn jetstream() -> async_nats::jetstream::Context {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4223".to_owned());
    let client = async_nats::connect(&url).await.expect("the shop's NATS");
    async_nats::jetstream::new(client)
}

/// Logs to the terminal, `RUST_LOG` deciding how much (`info` by default).
pub fn logs() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
}
