//! The derive's code names nothing a host may shadow, and reaches the crate
//! by another path when told.

#![no_implicit_prelude]

mod platform {
    pub use ::magic_events as events;
}

#[derive(::serde::Serialize, ::magic_events::Event)]
#[event(type = "order.shipped", version = 1, subject = "orders.{store}", crate = "crate::platform::events")]
pub struct OrderShipped {
    pub store: u32,
}

#[derive(::serde::Serialize, ::magic_events::Event)]
#[event(crate = "crate::platform::events")]
pub enum Catalog {
    #[event(type = "a", version = 1, subject = "a.{store}")]
    A { store: u32 },
}

fn main() {}
