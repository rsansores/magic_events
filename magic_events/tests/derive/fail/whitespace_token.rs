#[derive(serde::Serialize, magic_events::Event)]
#[event(type = "order.shipped", version = 1, subject = "orders.{store} now")]
struct OrderShipped {
    store: u32,
}

fn main() {}
