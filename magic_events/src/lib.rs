//! Events that leave PostgreSQL only if the transaction that raised them
//! committed — and always leave if it did.
//!
//! An event is a type that derives [`Event`](macro@Event). [`emit`] writes
//! one to an outbox table in the writing transaction, as one more statement;
//! a relay inside the host process ([`Events`]), one at a time across every
//! process that runs it, publishes what committed to NATS JetStream as
//! CloudEvents, and deletes it once the stream acknowledged it.
//!
//! ```no_run
//! use magic_events::{emit, Event, Events, Settings};
//!
//! #[derive(serde::Serialize, Event)]
//! #[event(type = "order.shipped", version = 1, subject = "orders.{store}.shipped")]
//! pub struct OrderShipped {
//!     pub store: u32,
//!     pub ids: Vec<i64>,
//! }
//!
//! # async fn main_(pool: sqlx::PgPool, nats: async_nats::Client) -> Result<(), magic_events::Error> {
//! // Once, in main.
//! Events::install(&pool).await?;
//! let jetstream = async_nats::jetstream::new(nats);
//! let events = Events::start(pool.clone(), jetstream, Settings::new("SHOP_EVENTS", "shop")).await?;
//!
//! // Wherever you write.
//! let mut tx = pool.begin().await?;
//! // … the writes the event is about …
//! emit(&mut tx, &OrderShipped { store: 7, ids: vec![41, 42] }).await?;
//! tx.commit().await?;
//!
//! events.stop().await;
//! # Ok(())
//! # }
//! ```
//!
//! [The README](https://github.com/rsansores/magic_events#readme) says what
//! is guaranteed, exactly, and how to consume, monitor and set it up;
//! [how it works](https://github.com/rsansores/magic_events/blob/main/HOW_IT_WORKS.md)
//! goes through the relay and every failure.
//!
//! # Features
//!
//! - `consume`: [`consume::consume`], a Rust consumer of the stream that
//!   decodes each event into its type.
//!
//! # Public dependencies
//!
//! What a host passes to the crate or reads back is another crate's type,
//! and a new major version of any of them is a new major version of this
//! crate:
//!
//! - [`sqlx`] 0.8: the pools and connections a host hands the crate, and the
//!   database error ([`Error::Database`]).
//! - [`async_nats`] 0.50: the JetStream context the relay publishes through.
//! - [`uuid`] 1: an event's id is a [`uuid::Uuid`].
//! - [`serde`] 1: an event is `Serialize`.

#![warn(missing_docs, clippy::missing_errors_doc)]

#[cfg(feature = "consume")]
pub mod consume;
mod emit;
mod envelope;
mod error;
mod publish;
mod relay;
mod run;
mod schema;
mod settings;
pub mod status;
mod stream;

pub use emit::emit;
pub use error::Error;
pub use run::Events;
pub use schema::migration;
pub use settings::{Settings, Storage};

/// The version of the crate's tables, in the schema `magic_events`: the relay
/// refuses to run against a database that installed another.
pub const SCHEMA_VERSION: i32 = 1;

/// An event: a type whose values [`emit`] writes, each a message on the
/// stream. Implemented by `#[derive(Event)]`, and by nothing else.
pub trait Event: serde::Serialize + __private::Sealed {
    #[doc(hidden)]
    const __TYPES: &'static [(&'static str, u32)];

    #[doc(hidden)]
    const __ENUM: bool;

    #[doc(hidden)]
    fn __describe(&self) -> __private::Described;
}

/// One type is one event, and says its type, its version and its subject:
/// implements [`Event`](trait@Event). A struct with named fields is one
/// event; an enum is one event per variant, each saying its own.
///
/// ```
/// use magic_events::Event;
/// use serde::Serialize;
///
/// #[derive(Serialize, Event)]
/// #[event(type = "order.shipped", version = 1, subject = "orders.{store}.shipped")]
/// pub struct OrderShipped {
///     pub store: u32,
///     pub ids: Vec<u64>,
/// }
///
/// #[derive(Serialize, Event)]
/// pub enum Catalog {
///     #[event(type = "product.updated", version = 1, subject = "catalog.{store}.product.updated")]
///     ProductUpdated { store: u32, ids: Vec<u64> },
///     #[event(type = "product.archived", version = 1, subject = "catalog.{store}.product.archived")]
///     ProductArchived { store: u32, ids: Vec<u64> },
/// }
/// ```
///
/// `#[event(…)]` takes, on a struct or on each variant of an enum:
///
/// | key | means |
/// |---|---|
/// | `type` | the event's type (required): printable ASCII, no whitespace. The message's `ce-type` is the type and the version, as `order.shipped.v1` |
/// | `version` | the version of the type's body (required), a whole number from 1: a change consumers cannot read is a new version |
/// | `subject` | where the message is published (required), after the settings' prefix: tokens joined by dots, each text, `{field}`s, or both (`orders.{store}.shipped`). A field is any `Display` value; the text of a token is printable ASCII, with no `*` or `>` |
/// | `ce_subject` | the CloudEvents `subject` attribute, if any: text and `{field}`s, as `"{id}"` |
/// | `crate` | the path to this crate, when a host reaches it through another: `crate = "platform::events"` — on an enum, on the enum itself |
///
/// The body is a JSON object: the event's fields, as serde writes them; for
/// an enum, the variant's own fields — the variant is in `ce-type` already.
/// So an enum is serialized as serde does by default (externally tagged),
/// each variant has named fields or wraps one struct, and a serde attribute
/// that writes a type as something else (`transparent`, `into`, `from`,
/// `try_from`, `remote`) is refused. A body that is still no object — a
/// variant wrapping a number — is refused by [`emit`].
///
/// Whatever the derive cannot honour is a compile error pointing at what
/// says it: a `{field}` that is no field, or not `Display`; a token no
/// subject may hold; two variants of one type and version. What only a
/// value shows — a field that renders as a token no subject may hold — is
/// refused by [`emit`].
pub use magic_events_derive::Event;

// The derive's generated code names this crate.
extern crate self as magic_events;

/// What the derive's generated code names, and nothing else may: none of it
/// is part of the crate's API.
#[doc(hidden)]
pub mod __private {
    use std::fmt::Write as _;

    /// Keeps [`Event`](crate::Event) to what the derive writes.
    pub trait Sealed {}

    /// One event as a value describes it.
    #[derive(Debug)]
    pub struct Described {
        /// Its type, without the version.
        pub r#type: &'static str,
        /// Its version.
        pub version: u32,
        /// Its variant's index, for a variant of an enum.
        pub variant: Option<u32>,
        /// Its subject's tokens, rendered and not checked yet.
        pub subject: Vec<String>,
        /// Its `ce-subject`, rendered and not encoded yet.
        pub ce_subject: Option<String>,
    }

    /// Writes `value` at the end of `out`.
    pub fn push<T: std::fmt::Display + ?Sized>(out: &mut String, value: &T) {
        let _ = write!(out, "{value}");
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod walk_tests;

#[cfg(test)]
mod bench_tests;
