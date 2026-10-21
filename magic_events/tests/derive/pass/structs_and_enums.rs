//! Every shape the derive accepts: a struct, an enum of struct and newtype
//! variants, a token of text and fields together, a `ce_subject`, generics,
//! and the serde attributes that leave the body an object.

use magic_events::Event;
use serde::Serialize;
use std::fmt::Display;

#[derive(Serialize, Event)]
#[event(type = "order.shipped", version = 1, subject = "orders.{store}.shipped")]
pub struct OrderShipped {
    pub store: u32,
    pub ids: Vec<u64>,
}

#[derive(Serialize, Event)]
#[event(type = "noted", version = 7, subject = "notes")]
pub struct Noted {}

#[derive(Serialize)]
pub struct Ids {
    pub store: String,
    pub ids: Vec<u64>,
}

#[derive(Serialize, Event)]
#[serde(rename_all = "snake_case")]
pub enum Catalog {
    #[event(
        type = "product.updated",
        version = 1,
        subject = "catalog.store-{store}.product.updated",
        ce_subject = "{store}/{first}"
    )]
    ProductUpdated {
        store: u32,
        first: u64,
        #[serde(rename = "products")]
        ids: Vec<u64>,
    },
    #[event(type = "product.updated", version = 2, subject = "catalog.{store}.product.updated")]
    ProductUpdatedAgain(Ids),
    #[event(type = "catalog.reset", version = 1, subject = "catalog.reset")]
    Reset(Ids),
}

#[derive(Serialize, Event)]
#[event(type = "thing.done", version = 1, subject = "things.{who}.done")]
pub struct Done<W: Display + Serialize> {
    pub who: W,
}

fn takes<E: Event>(_: &E) {}

fn main() {
    takes(&OrderShipped { store: 1, ids: vec![] });
    takes(&Noted {});
    takes(&Catalog::Reset(Ids { store: "a".into(), ids: vec![] }));
    takes(&Done { who: "me" });
}
