#[derive(serde::Serialize, magic_events::Event)]
#[event(type = "catalog", version = 1, subject = "catalog")]
enum Catalog {
    #[event(type = "product.updated", version = 1, subject = "catalog.updated")]
    ProductUpdated { ids: Vec<u64> },
}

fn main() {}
