#[derive(serde::Serialize, magic_events::Event)]
#[event(type = "order.seen", version = 1, subject = "orders.seen", stream = "")]
struct OrderSeen {
    ids: Vec<u64>,
}

fn main() {}
