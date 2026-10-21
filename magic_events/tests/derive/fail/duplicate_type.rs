#[derive(serde::Serialize, magic_events::Event)]
enum Catalog {
    #[event(type = "product.updated", version = 1, subject = "catalog.updated")]
    ProductUpdated { ids: Vec<u64> },
    #[event(type = "product.updated", version = 1, subject = "catalog.changed")]
    ProductChanged { ids: Vec<u64> },
}

fn main() {}
