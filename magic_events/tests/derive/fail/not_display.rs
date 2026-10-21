#[derive(serde::Serialize, magic_events::Event)]
#[event(type = "order.shipped", version = 1, subject = "orders.{ids}.shipped")]
struct OrderShipped {
    ids: Vec<u64>,
}

fn main() {}
