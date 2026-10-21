//! Billing: one invoice per order placed. It reads the order where the shop
//! keeps it, and records the events it acted on, so an event delivered
//! twice is billed once.
//!
//! Billing reads the shop's own table here, to keep the example to one
//! database; a system of its own reads the order through the shop's API,
//! with credentials of its own — the event carries only the order's id.

use magic_events::consume::{consume, Delivery};
use shop::Shop;

#[tokio::main]
async fn main() {
    shop::logs();
    let pool = shop::database().await;
    let jetstream = shop::jetstream().await;
    consume(
        &jetstream,
        shop::STREAM,
        "billing",
        "shop_events.orders.*.placed",
        |delivery: Delivery<Shop>| {
            let pool = pool.clone();
            async move {
                let Shop::OrderPlaced { ids, .. } = delivery.event else {
                    return Ok(());
                };
                let mut tx = pool.begin().await?;
                let first = sqlx::query(
                    "INSERT INTO shop.billed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING",
                )
                .bind(&delivery.id)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                    == 1;
                if first {
                    sqlx::query(
                        "INSERT INTO shop.invoices (order_id, total) \
                         SELECT id, total FROM shop.orders WHERE id = ANY($1) \
                         ON CONFLICT DO NOTHING",
                    )
                    .bind(&ids)
                    .execute(&mut *tx)
                    .await?;
                    println!("billing: invoiced orders {ids:?}");
                }
                tx.commit().await?;
                Ok(())
            }
        },
    )
    .await
    .expect("billing's consumer");
}
