#[derive(serde::Serialize, magic_events::Event)]
#[event(type = "order.shipped", version = 0, subject = "orders.shipped")]
struct OrderShipped {
    ids: Vec<u64>,
}

fn main() {}
