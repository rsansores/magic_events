#[derive(serde::Serialize, magic_events::Event)]
enum Catalog {
    #[event(type = "catalog.reset", version = 1, subject = "catalog.reset")]
    Reset,
}

fn main() {}
