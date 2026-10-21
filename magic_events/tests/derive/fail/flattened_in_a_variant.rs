#[derive(serde::Serialize)]
struct Ids {
    ids: Vec<u64>,
}

#[derive(serde::Serialize, magic_events::Event)]
enum Catalog {
    #[event(type = "product.updated", version = 1, subject = "catalog.updated")]
    ProductUpdated {
        #[serde(flatten)]
        ids: Ids,
    },
}

fn main() {}
