#[derive(serde::Serialize, magic_events::Event)]
enum Catalog {
    #[event(type = "product.updated", subject = "catalog.updated")]
    ProductUpdated { ids: Vec<u64> },
}

fn main() {}
