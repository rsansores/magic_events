//! Events that leave PostgreSQL only if the transaction that raised them
//! committed — and always leave if it did.
//!
//! An event is written to an outbox table in the writing transaction, as one
//! more statement; a relay inside the host process publishes what committed
//! to NATS JetStream, as CloudEvents.
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

#![warn(missing_docs, clippy::missing_errors_doc)]

mod error;

pub use error::Error;

#[cfg(test)]
mod tests;
