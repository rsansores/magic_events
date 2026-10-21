#[derive(serde::Serialize, magic_events::Event)]
#[serde(tag = "kind")]
enum Catalog {
    #[event(type = "product.updated", version = 1, subject = "catalog.updated")]
    ProductUpdated { ids: Vec<u64> },
}

fn main() {}
