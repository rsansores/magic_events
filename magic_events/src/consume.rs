//! `consume`: a Rust consumer of the stream, for a system that reads the
//! events, and for a host's tests. A consumer in any other language reads
//! the stream as plain JetStream: the README shows how.

use crate::{Error, Event};
use async_nats::jetstream::consumer::{pull, AckPolicy};
use async_nats::jetstream::{AckKind, Context};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use serde::de::{self, DeserializeOwned, IntoDeserializer};
use std::future::Future;
use std::time::Duration;

/// One event as the stream delivered it.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Delivery<E> {
    /// The event's id, its `ce-id`: the same however often it is delivered,
    /// so a handler that must not act twice records it.
    pub id: String,
    /// The event.
    pub event: E,
    /// The subject it was published on.
    pub subject: String,
    /// When it was emitted, its `ce-time`.
    pub time: Option<DateTime<Utc>>,
    /// How many times it was delivered before this one.
    pub redelivered: u64,
}

/// What a handler answers: `Ok` acknowledges the event, an error has it
/// delivered again after a backoff.
pub type Handled = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// The first wait before an event a handler failed on is delivered again;
/// it doubles with each delivery, up to [`LONGEST_WAIT`].
const FIRST_WAIT: Duration = Duration::from_secs(1);

/// The longest wait before an event a handler failed on is delivered again.
const LONGEST_WAIT: Duration = Duration::from_secs(60);

/// How long the broker waits for an acknowledgement before it delivers an
/// event again: a handler still at work says so every [`PROGRESS_EVERY`].
const ACK_WAIT: Duration = Duration::from_secs(30);

/// How often a handler still at work tells the broker it is.
const PROGRESS_EVERY: Duration = Duration::from_secs(10);

/// Reads the events of `stream` under `filter` through the durable consumer
/// `durable` — created if it is not there, resumed where it stopped if it is
/// — and hands each to `handler`, one at a time, in the stream's order:
/// acknowledged once the handler answers `Ok`, delivered again after a
/// backoff when it fails (1 s, doubling, up to a minute). While a handler
/// works, the broker is told so every 10 seconds, so a slow handler is not
/// taken for a dead one. Runs until the broker fails it; spawn it, and abort
/// it to stop: an event its handler had not answered is delivered again.
///
/// Delivery is at least once: an event can come twice — a handler that
/// failed, a consumer stopped before its acknowledgement — and always with
/// the same `id`, which a handler that must not act twice records. An event
/// a handler failed on is delivered again after the backoff, so the events
/// behind it go first: order holds only while handlers succeed.
///
/// An event `E` cannot read — a type it does not declare, a body that does
/// not decode — is logged and terminated: it is never delivered to this
/// consumer again. Its filter should take only what `E` declares.
///
/// ```no_run
/// use magic_events::consume::{consume, Delivery};
///
/// #[derive(serde::Serialize, serde::Deserialize, magic_events::Event)]
/// enum Orders {
///     #[event(type = "order.placed", version = 1, subject = "orders.{store}.placed")]
///     Placed { store: u32, ids: Vec<u64> },
/// }
///
/// # async fn billing(jetstream: async_nats::jetstream::Context) -> Result<(), magic_events::Error> {
/// consume(&jetstream, "SHOP_EVENTS", "billing", "shop_events.orders.>", |d: Delivery<Orders>| async move {
///     let Orders::Placed { store, ids } = d.event;
///     println!("invoice {ids:?} of store {store}, once: event {}", d.id);
///     Ok(())
/// })
/// .await
/// # }
/// ```
///
/// # Errors
///
/// [`Error::Misuse`] when a durable of that name exists and reads another
/// filter, or acknowledges otherwise: it is someone else's, or this one's
/// filter changed — delete it, or take another name, rather than read what
/// `E` does not declare and set it aside for good. [`Error::Broker`] when
/// the stream cannot be read, the consumer cannot be made, or the broker
/// fails while it runs.
pub async fn consume<E, H, F>(
    jetstream: &Context,
    stream: &str,
    durable: &str,
    filter: &str,
    mut handler: H,
) -> Result<(), Error>
where
    E: Event + DeserializeOwned,
    H: FnMut(Delivery<E>) -> F,
    F: Future<Output = Handled>,
{
    let broker = |what: &str, e: &dyn std::fmt::Display| Error::Broker {
        reason: format!("the consumer `{durable}` of `{stream}`: {what}: {e}"),
        transient: true,
    };
    let mut consumer = jetstream
        .get_stream(stream)
        .await
        .map_err(|e| broker("the stream could not be read", &e))?
        .get_or_create_consumer(
            durable,
            pull::Config {
                durable_name: Some(durable.to_owned()),
                filter_subject: filter.to_owned(),
                ack_policy: AckPolicy::Explicit,
                ack_wait: ACK_WAIT,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| broker("it could not be made", &e))?;
    // A durable that was there already is taken as it is: one that reads
    // something else would have every event it holds terminated as one `E`
    // cannot read.
    let found = &consumer
        .info()
        .await
        .map_err(|e| broker("it could not be read", &e))?
        .config;
    let filters: Vec<&str> = std::iter::once(found.filter_subject.as_str())
        .filter(|f| !f.is_empty())
        .chain(found.filter_subjects.iter().map(String::as_str))
        .collect();
    if filters != [filter] || found.ack_policy != AckPolicy::Explicit {
        return Err(Error::Misuse(format!(
            "the consumer `{durable}` of `{stream}` reads {filters:?} acknowledging {:?}, not \
             `{filter}` acknowledging each: it is another's, or its filter changed — delete it \
             (`nats consumer rm {stream} {durable}`) or take another name",
            found.ack_policy
        )));
    }
    // One at a time: a message waiting in a batch would count against its
    // acknowledgement's wait while the ones before it are handled.
    let mut messages = consumer
        .stream()
        .max_messages_per_batch(1)
        .messages()
        .await
        .map_err(|e| broker("its messages could not be read", &e))?;
    while let Some(message) = messages.next().await {
        let message = message.map_err(|e| broker("a message could not be read", &e))?;
        let redelivered = message
            .info()
            .map(|i| u64::try_from(i.delivered.saturating_sub(1)).unwrap_or(0))
            .unwrap_or(0);
        let header = |name: &str| {
            message
                .headers
                .as_ref()
                .and_then(|h| h.get(name))
                .map(|v| decoded(v.as_str()))
        };
        let (id, ty) = (header("ce-id").unwrap_or_default(), header("ce-type"));
        let event = match ty.as_deref().map(|ty| decode::<E>(ty, &message.payload)) {
            Some(Ok(event)) => event,
            Some(Err(why)) => {
                tracing::error!(%id, subject = %message.subject, "events: an event this consumer cannot read — terminated: {why}");
                message
                    .ack_with(AckKind::Term)
                    .await
                    .map_err(|e| broker("a message could not be terminated", &e))?;
                continue;
            }
            None => {
                tracing::error!(subject = %message.subject, "events: a message with no `ce-type` — terminated");
                message
                    .ack_with(AckKind::Term)
                    .await
                    .map_err(|e| broker("a message could not be terminated", &e))?;
                continue;
            }
        };
        let delivery = Delivery {
            id,
            event,
            subject: message.subject.to_string(),
            time: header("ce-time")
                .and_then(|t| DateTime::parse_from_rfc3339(&t).ok())
                .map(|t| t.with_timezone(&Utc)),
            redelivered,
        };
        let handled = handler(delivery);
        tokio::pin!(handled);
        let mut progress =
            tokio::time::interval_at(tokio::time::Instant::now() + PROGRESS_EVERY, PROGRESS_EVERY);
        let handled = loop {
            tokio::select! {
                handled = &mut handled => break handled,
                _ = progress.tick() => {
                    // Still at work: the broker waits another `ACK_WAIT`.
                    let _ = message.ack_with(AckKind::Progress).await;
                }
            }
        };
        match handled {
            Ok(()) => message
                .ack()
                .await
                .map_err(|e| broker("a message could not be acknowledged", &e))?,
            Err(e) => {
                let wait = FIRST_WAIT
                    .saturating_mul(1 << redelivered.min(16))
                    .min(LONGEST_WAIT);
                tracing::warn!(subject = %message.subject, redelivered, "events: a handler failed — delivered again in {wait:?}: {e}");
                message
                    .ack_with(AckKind::Nak(Some(wait)))
                    .await
                    .map_err(|e| broker("a message could not be refused", &e))?;
            }
        }
    }
    Err(broker("the broker ended its messages", &"no more"))
}

/// A header value as the binding writes it, percent-decoded: as it was, when
/// it does not decode.
fn decoded(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |b: u8| (b as char).to_digit(16);
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                out.push(u8::try_from(h * 16 + l).unwrap_or(b'?'));
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| value.to_owned())
}

/// The event of type `ce_type` (as `order.shipped.v1`) whose body is `body`.
pub(crate) fn decode<E: Event + DeserializeOwned>(ce_type: &str, body: &[u8]) -> Result<E, String> {
    let index = E::__TYPES
        .iter()
        .position(|(ty, version)| ce_type == format!("{ty}.v{version}"))
        .ok_or_else(|| format!("`{ce_type}` is no type this consumer declares"))?;
    let decoded = if E::__ENUM {
        E::deserialize(Variant {
            index: u32::try_from(index).unwrap_or(u32::MAX),
            body,
        })
    } else {
        serde_json::from_slice(body)
    };
    decoded.map_err(|e| format!("its body does not decode as `{ce_type}`: {e}"))
}

/// One variant of an event enum, by its index, and its body: what serde's
/// derived `Deserialize` reads as that variant, whatever its name is
/// renamed to.
struct Variant<'a> {
    index: u32,
    body: &'a [u8],
}

impl<'de> de::Deserializer<'de> for Variant<'de> {
    type Error = serde_json::Error;

    fn deserialize_any<V: de::Visitor<'de>>(self, _: V) -> Result<V::Value, Self::Error> {
        Err(de::Error::custom(
            "an event enum is read as serde's default, externally tagged",
        ))
    }

    fn deserialize_enum<V: de::Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_enum(self)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct identifier ignored_any
    }
}

impl<'de> de::EnumAccess<'de> for Variant<'de> {
    type Error = serde_json::Error;
    type Variant = Body<'de>;

    fn variant_seed<V: de::DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> Result<(V::Value, Body<'de>), Self::Error> {
        let index: de::value::U32Deserializer<serde_json::Error> = self.index.into_deserializer();
        Ok((seed.deserialize(index)?, Body(self.body)))
    }
}

/// A variant's body: its fields, or the value it wraps.
struct Body<'a>(&'a [u8]);

impl<'de> de::VariantAccess<'de> for Body<'de> {
    type Error = serde_json::Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Err(de::Error::custom("an event is never a unit variant"))
    }

    fn newtype_variant_seed<T: de::DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> Result<T::Value, Self::Error> {
        let mut json = serde_json::Deserializer::from_slice(self.0);
        let value = seed.deserialize(&mut json)?;
        json.end()?;
        Ok(value)
    }

    fn tuple_variant<V: de::Visitor<'de>>(self, _: usize, _: V) -> Result<V::Value, Self::Error> {
        Err(de::Error::custom("an event is never a tuple variant"))
    }

    fn struct_variant<V: de::Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        let mut json = serde_json::Deserializer::from_slice(self.0);
        let value = de::Deserializer::deserialize_struct(&mut json, "", fields, visitor)?;
        json.end()?;
        Ok(value)
    }
}

#[cfg(test)]
#[path = "consume_tests.rs"]
mod tests;
