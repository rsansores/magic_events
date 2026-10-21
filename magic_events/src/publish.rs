//! The relay's way to the broker: JetStream, or in tests one that fails on
//! purpose. Publishing is two steps, so many messages wait for the stream at
//! once: [`Publish::send`] hands a message to the broker, and the [`Ack`] it
//! answers resolves once the stream holds it.

use crate::envelope::Message;
use async_nats::jetstream::context::{PublishError, PublishErrorKind};
use async_nats::jetstream::ErrorCode;
use std::future::Future;
use std::pin::Pin;

/// A future the relay awaits.
pub(crate) type Boxed<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What resolves once the stream holds a message sent, or says why not.
pub(crate) type Ack = Boxed<'static, Result<(), Failure>>;

/// Why the broker did not take a message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Failure {
    /// Whether a retry may get it through — the broker away, slow, or its
    /// stream unable to take anything now — rather than never: this message
    /// refused for what it is.
    pub(crate) transient: bool,
    /// Why, as the broker says it.
    pub(crate) reason: String,
}

impl Failure {
    pub(crate) fn transient(reason: impl Into<String>) -> Self {
        Failure {
            transient: true,
            reason: reason.into(),
        }
    }
}

/// Where the relay publishes.
pub(crate) trait Publish: Send + Sync + 'static {
    /// Hands `message` (with `body`) to the broker: answers what resolves
    /// once the stream holds it.
    fn send(&self, message: Message, body: Vec<u8>) -> Boxed<'_, Result<Ack, Failure>>;
}

impl Publish for async_nats::jetstream::Context {
    fn send(&self, message: Message, body: Vec<u8>) -> Boxed<'_, Result<Ack, Failure>> {
        Box::pin(async move {
            let mut headers = async_nats::HeaderMap::new();
            for (name, value) in message.headers {
                headers.insert(name, value.as_str());
            }
            let ack = self
                .publish_with_headers(message.subject, headers, body.into())
                .await
                .map_err(|e| failure(&e))?;
            let ack: Ack = Box::pin(async move { ack.await.map(|_| ()).map_err(|e| failure(&e)) });
            Ok(ack)
        })
    }
}

/// Whether the broker refused a message for what it is: too large for the
/// server or the stream. Anything else — a timeout, no stream answering, the
/// connection lost, the stream unable to take more — is the broker's state,
/// and the same message may go through later; a refusal nobody listed is
/// retried, and shows in the relay's status, rather than set aside.
fn failure(e: &PublishError) -> Failure {
    let permanent = match e.kind() {
        PublishErrorKind::MaxPayloadExceeded
        | PublishErrorKind::WrongLastMessageId
        | PublishErrorKind::WrongLastSequence => true,
        PublishErrorKind::Other => std::error::Error::source(e)
            .and_then(|s| s.downcast_ref::<async_nats::jetstream::Error>())
            .is_some_and(|api| {
                matches!(
                    api.error_code(),
                    ErrorCode::STREAM_MESSAGE_EXCEEDS_MAXIMUM
                        | ErrorCode::STREAM_HEADER_EXCEEDS_MAXIMUM
                )
            }),
        _ => false,
    };
    Failure {
        transient: !permanent,
        reason: e.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_message_too_large_is_refused_for_good() {
        assert!(!failure(&PublishError::new(PublishErrorKind::MaxPayloadExceeded)).transient);
        for kind in [
            PublishErrorKind::TimedOut,
            PublishErrorKind::StreamNotFound,
            PublishErrorKind::BrokenPipe,
            PublishErrorKind::MaxAckPending,
            PublishErrorKind::Other,
        ] {
            assert!(failure(&PublishError::new(kind)).transient, "{kind:?}");
        }
    }
}
