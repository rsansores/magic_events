//! Shipping: every order, as it is placed and as it ships.

use magic_events::consume::{consume, Delivery};
use shop::Shop;

#[tokio::main]
async fn main() {
    shop::logs();
    let jetstream = shop::jetstream().await;
    consume(
        &jetstream,
        shop::STREAM,
        "shipping",
        "shop_events.orders.>",
        |delivery: Delivery<Shop>| async move {
            match delivery.event {
                Shop::OrderPlaced { store, ids } => {
                    println!("shipping: {store} has orders {ids:?} to pack");
                }
                Shop::OrderShipped { store, ids } => {
                    println!("shipping: {store} shipped orders {ids:?}");
                }
            }
            Ok(())
        },
    )
    .await
    .expect("shipping's consumer");
}
