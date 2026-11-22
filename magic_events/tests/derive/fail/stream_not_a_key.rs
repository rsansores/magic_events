#[derive(serde::Serialize, magic_events::Event)]
#[event(type = "order.seen", version = 1, subject = "orders.seen", stream = "Live-Screens")]
struct OrderSeen {
    ids: Vec<u64>,
}

fn main() {}
