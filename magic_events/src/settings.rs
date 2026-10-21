//! How the relay runs, and the stream it publishes to.

use crate::Error;
use std::time::Duration;

/// The largest body [`emit`](crate::emit) takes by default: 64 KiB.
pub(crate) const MAX_BODY: u32 = 64 << 10;

/// Room a message keeps beside its body for its subject and headers: the
/// broker's `max_payload` counts both.
#[allow(dead_code)] // The relay holds it to the broker's.
pub(crate) const HEADROOM: u32 = 4 << 10;

/// The longest subject token, `ce-subject` or event type `emit` takes, in
/// bytes: together they stay within [`HEADROOM`].
pub(crate) const LONGEST_HEADER: usize = 1 << 10;

/// Where the stream keeps its messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Storage {
    /// On disk: what survives a restart of the broker.
    File,
    /// In memory: lost when the broker restarts.
    Memory,
}

/// How the relay runs, and the stream it publishes to. [`Settings::new`]
/// names the stream and the events' source; everything else has a default
/// and a setter.
///
/// ```
/// use magic_events::Settings;
/// use std::time::Duration;
///
/// let settings = Settings::new("SHOP_EVENTS", "shop")
///     .retention(Duration::from_secs(30 * 24 * 60 * 60))
///     .replicas(3);
/// ```
#[derive(Clone, Debug)]
#[allow(dead_code)] // The relay reads them.
pub struct Settings {
    pub(crate) stream: String,
    pub(crate) source: String,
    pub(crate) subject_prefix: String,
    pub(crate) retention: Duration,
    pub(crate) duplicate_window: Duration,
    pub(crate) storage: Storage,
    pub(crate) replicas: usize,
    pub(crate) manage_stream: bool,
    pub(crate) idle: Duration,
    pub(crate) batch: u32,
    pub(crate) in_flight: u32,
    pub(crate) send_timeout: Duration,
    pub(crate) backoff: (Duration, Duration),
    pub(crate) standby: Duration,
    pub(crate) max_body: u32,
}

impl Settings {
    /// The relay of the JetStream stream `stream`, its events' CloudEvents
    /// `source` the URI-reference `source` (`"shop"`,
    /// `"//shop.example.com"`). Every subject is the subject prefix — the
    /// stream's name in lowercase, unless [`Settings::subject_prefix`] says
    /// otherwise — then a dot, then the event's own subject.
    #[must_use]
    pub fn new(stream: impl Into<String>, source: impl Into<String>) -> Self {
        let stream = stream.into();
        Settings {
            subject_prefix: stream.to_lowercase(),
            stream,
            source: source.into(),
            retention: Duration::from_secs(7 * 24 * 60 * 60),
            duplicate_window: Duration::from_secs(2 * 60),
            storage: Storage::File,
            replicas: 1,
            manage_stream: true,
            idle: Duration::from_millis(200),
            batch: 256,
            in_flight: 64,
            send_timeout: Duration::from_secs(10),
            backoff: (Duration::from_millis(500), Duration::from_secs(60)),
            standby: Duration::from_secs(30),
            max_body: MAX_BODY,
        }
    }

    /// The first tokens of every subject: one token or several, joined by
    /// dots (`"shop.events"`). The lowercase stream name by default.
    #[must_use]
    pub fn subject_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.subject_prefix = prefix.into();
        self
    }

    /// How long the stream keeps a message (its `max_age`): 7 days by
    /// default. A consumer that is away longer misses what expired.
    #[must_use]
    pub fn retention(mut self, retention: Duration) -> Self {
        self.retention = retention;
        self
    }

    /// How long the stream remembers a message's id, and drops a message
    /// sent again under it: 2 minutes by default. A relay that died after
    /// the stream took a message, and before it deleted its row, sends it
    /// again; within the window that is no duplicate.
    #[must_use]
    pub fn duplicate_window(mut self, window: Duration) -> Self {
        self.duplicate_window = window;
        self
    }

    /// Where the stream keeps its messages: [`Storage::File`] by default.
    /// Set when the stream is created; a stream's storage does not change.
    #[must_use]
    pub fn storage(mut self, storage: Storage) -> Self {
        self.storage = storage;
        self
    }

    /// How many servers of the cluster hold the stream: 1 by default.
    #[must_use]
    pub fn replicas(mut self, replicas: usize) -> Self {
        self.replicas = replicas;
        self
    }

    /// Whether the relay creates the stream, and brings it to these
    /// settings (`true`, the default), or only requires that it exists and
    /// takes every subject under the prefix (`false`): the stream is then
    /// the operator's, with its own retention.
    #[must_use]
    pub fn manage_stream(mut self, manage: bool) -> Self {
        self.manage_stream = manage;
        self
    }

    /// How long the relay waits when the outbox is empty: 200 ms by default.
    #[must_use]
    pub fn idle(mut self, idle: Duration) -> Self {
        self.idle = idle;
        self
    }

    /// Rows read per pass: 256 by default.
    #[must_use]
    pub fn batch(mut self, batch: u32) -> Self {
        self.batch = batch;
        self
    }

    /// Messages sent and not acknowledged yet, at most: 64 by default. 1
    /// sends one message at a time, each after the one before it was
    /// acknowledged.
    #[must_use]
    pub fn in_flight(mut self, in_flight: u32) -> Self {
        self.in_flight = in_flight;
        self
    }

    /// How long a message may wait for the stream's acknowledgement before
    /// the broker counts as away: 10 s by default.
    #[must_use]
    pub fn send_timeout(mut self, timeout: Duration) -> Self {
        self.send_timeout = timeout;
        self
    }

    /// How long the relay waits after a failed pass: `first`, doubling with
    /// every failure in a row, up to `max`. 500 ms to 60 s by default.
    #[must_use]
    pub fn backoff(mut self, first: Duration, max: Duration) -> Self {
        self.backoff = (first, max);
        self
    }

    /// How often a process that is not the relay asks for the relay's lock:
    /// 30 s by default.
    #[must_use]
    pub fn standby(mut self, standby: Duration) -> Self {
        self.standby = standby;
        self
    }

    /// The largest body, in bytes, [`emit`](crate::emit) takes: 64 KiB by
    /// default. Held to the broker's `max_payload` when the relay starts.
    #[must_use]
    pub fn max_body(mut self, bytes: u32) -> Self {
        self.max_body = bytes;
        self
    }

    /// Refuses settings the relay cannot run with.
    #[allow(dead_code)] // The relay checks them as it starts.
    pub(crate) fn check(&self) -> Result<(), Error> {
        let misuse = |why: String| Err(Error::Misuse(why));
        if self.stream.is_empty()
            || !self
                .stream
                .chars()
                .all(|c| matches!(c, '!'..='~') && !matches!(c, '.' | '*' | '>' | '/' | '\\'))
        {
            return misuse(format!(
                "`{}` is no stream name: one is printable ASCII, with no whitespace, and none of \
                 `.`, `*`, `>`, `/` or `\\`",
                self.stream
            ));
        }
        if self.source.is_empty() {
            return misuse("an empty source: CloudEvents requires one".to_owned());
        }
        if self.subject_prefix.len() > LONGEST_HEADER
            || self.subject_prefix.split('.').any(|t| !is_token(t))
        {
            return misuse(format!(
                "`{}` is no subject prefix: one is one token or several, joined by dots, each \
                 printable ASCII with no `*` or `>`",
                self.subject_prefix
            ));
        }
        let zero = [
            ("retention", self.retention),
            ("duplicate_window", self.duplicate_window),
            ("idle", self.idle),
            ("send_timeout", self.send_timeout),
            ("backoff", self.backoff.0),
            ("standby", self.standby),
        ];
        if let Some((what, _)) = zero.iter().find(|(_, d)| d.is_zero()) {
            return misuse(format!(
                "`{what}` is zero: the relay would spin, or give up on what it waits for at once"
            ));
        }
        if self.backoff.0 > self.backoff.1 {
            return misuse(format!(
                "`backoff` waits {:?} first and {:?} at most: the first wait is not longer than \
                 the longest",
                self.backoff.0, self.backoff.1
            ));
        }
        if self.duplicate_window > self.retention {
            return misuse(format!(
                "`duplicate_window` ({:?}) is longer than `retention` ({:?}): the stream refuses it",
                self.duplicate_window, self.retention
            ));
        }
        for (what, n) in [
            ("batch", self.batch),
            ("in_flight", self.in_flight),
            ("max_body", self.max_body),
        ] {
            if n == 0 {
                return misuse(format!("`{what}` is zero: the relay would publish nothing"));
            }
        }
        if self.replicas == 0 {
            return misuse(
                "`replicas` is zero: a stream is held by one server at least".to_owned(),
            );
        }
        Ok(())
    }
}

/// Whether `token` may be one token of a subject: not empty, printable
/// ASCII, and none of the separator `.` or the wildcards `*` and `>`.
pub(crate) fn is_token(token: &str) -> bool {
    !token.is_empty()
        && token
            .chars()
            .all(|c| matches!(c, '!'..='~') && !matches!(c, '.' | '*' | '>'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_prefix_is_the_stream_in_lowercase_unless_said() {
        assert_eq!(
            Settings::new("SHOP_EVENTS", "shop").subject_prefix,
            "shop_events"
        );
        assert_eq!(
            Settings::new("SHOP_EVENTS", "shop")
                .subject_prefix("shop.events")
                .subject_prefix,
            "shop.events"
        );
        assert!(Settings::new("SHOP_EVENTS", "shop").check().is_ok());
    }

    #[test]
    fn settings_the_relay_cannot_run_with_are_refused() {
        let base = || Settings::new("SHOP", "shop");
        for (settings, what) in [
            (Settings::new("SHOP.EVENTS", "shop"), "stream name"),
            (Settings::new("SHOP EVENTS", "shop"), "stream name"),
            (Settings::new("", "shop"), "stream name"),
            (Settings::new("SHOP", ""), "source"),
            (base().subject_prefix("shop.*"), "prefix"),
            (base().subject_prefix("shop..x"), "prefix"),
            (base().subject_prefix(""), "prefix"),
            (base().idle(Duration::ZERO), "`idle`"),
            (
                base().backoff(Duration::from_secs(2), Duration::from_secs(1)),
                "`backoff`",
            ),
            (
                base()
                    .retention(Duration::from_secs(60))
                    .duplicate_window(Duration::from_secs(120)),
                "`duplicate_window`",
            ),
            (base().in_flight(0), "`in_flight`"),
            (base().batch(0), "`batch`"),
            (base().max_body(0), "`max_body`"),
            (base().replicas(0), "`replicas`"),
        ] {
            match settings.check() {
                Err(Error::Misuse(why)) => assert!(why.contains(what), "{what}: {why}"),
                other => panic!("{what}: {other:?}"),
            }
        }
    }

    #[test]
    fn a_token_is_printable_ascii_with_no_separator_or_wildcard() {
        for good in ["orders", "a-b_c", "0", "~!@#"] {
            assert!(is_token(good), "{good}");
        }
        for bad in ["", "a.b", "*", ">", "a b", "a\tb", "é", "a\u{7f}"] {
            assert!(!is_token(bad), "{bad:?}");
        }
    }
}
