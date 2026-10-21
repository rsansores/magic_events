#[derive(serde::Serialize, magic_events::Event)]
#[event(type = "order.shipped", version = 1, subject = "orders.shipped", ce_subject = "{id}")]
struct OrderShipped {
    ids: Vec<u64>,
}

fn main() {}
