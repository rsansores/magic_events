//! How the relay runs, and the stream it publishes to.

use crate::stream::covers;
use crate::Error;
use std::time::Duration;

/// The largest body [`emit`](crate::emit) takes by default: 64 KiB.
pub(crate) const MAX_BODY: u32 = 64 << 10;

/// Room a message keeps beside its body for its headers: the broker's
/// `max_payload` counts both. The longest headers the limits below allow —
/// a `ce-source` and a `ce-subject` of [`LONGEST_HEADER`] each, a `ce-type`
/// of [`LONGEST_TYPE`] and its version, two ids and a time — stay within it.
pub(crate) const HEADROOM: u32 = 4 << 10;

/// The longest `ce-subject` and `ce-source`, percent-encoded, in bytes.
pub(crate) const LONGEST_HEADER: usize = 1 << 10;

/// The longest subject an event renders, after the prefix, in bytes; with
/// the prefix, a subject stays far within the 4 KiB a NATS client's control
/// line holds.
pub(crate) const LONGEST_SUBJECT: usize = 1 << 10;

/// The longest subject prefix, in bytes.
pub(crate) const LONGEST_PREFIX: usize = 255;

/// The longest event type, in bytes, without its version: the derive holds
/// a type to it.
pub(crate) const LONGEST_TYPE: usize = 255;

/// The longest stream key, in bytes.
const LONGEST_KEY: usize = 32;

/// Where the stream keeps its messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Storage {
    /// On disk: what survives a restart of the broker.
    File,
    /// In memory: lost when the broker restarts.
    Memory,
}

/// One JetStream stream the relay publishes to: its name, its subject
/// prefix, and what it keeps. [`Settings::new`] takes the main stream's —
/// or only its name, every other setting the default — and
/// [`Settings::stream`] declares another with these.
///
/// ```
/// use magic_events::StreamSettings;
/// use std::time::Duration;
///
/// let live = StreamSettings::new("SHOP_LIVE").retention(Duration::from_secs(10 * 60));
/// ```
#[derive(Clone, Debug)]
pub struct StreamSettings {
    pub(crate) name: String,
    pub(crate) subject_prefix: String,
    pub(crate) retention: Duration,
    pub(crate) duplicate_window: Duration,
    pub(crate) storage: Storage,
    pub(crate) replicas: usize,
    pub(crate) manage_stream: bool,
    pub(crate) max_body: u32,
}

impl StreamSettings {
    /// The JetStream stream `name`. Every subject is the subject prefix —
    /// the stream's name in lowercase, unless
    /// [`StreamSettings::subject_prefix`] says otherwise — then a dot, then
    /// the event's own subject.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        StreamSettings {
            subject_prefix: name.to_lowercase(),
            name,
            retention: Duration::from_secs(7 * 24 * 60 * 60),
            duplicate_window: Duration::from_secs(2 * 60),
            storage: Storage::File,
            replicas: 1,
            manage_stream: true,
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

    /// The largest body, in bytes, [`emit`](crate::emit) takes for an event
    /// of this stream: 64 KiB by default. Held to the broker's `max_payload`
    /// when the relay starts.
    #[must_use]
    pub fn max_body(mut self, bytes: u32) -> Self {
        self.max_body = bytes;
        self
    }

    /// Refuses a stream the relay cannot publish to, each refusal led by
    /// `of` — nothing for the main stream, "the stream `live`: " for another
    /// — and its `duplicate_window` shorter than `takeover`, what a takeover
    /// of the relay may take.
    fn check(&self, of: &str, takeover: Duration) -> Result<(), Error> {
        let misuse = |why: String| Err(Error::Misuse(format!("{of}{why}")));
        if self.name.is_empty()
            || !self
                .name
                .chars()
                .all(|c| matches!(c, '!'..='~') && !matches!(c, '.' | '*' | '>' | '/' | '\\'))
        {
            return misuse(format!(
                "`{}` is no stream name: one is printable ASCII, with no whitespace, and none of \
                 `.`, `*`, `>`, `/` or `\\`",
                self.name
            ));
        }
        if self.subject_prefix.len() > LONGEST_PREFIX
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
        ];
        if let Some((what, _)) = zero.iter().find(|(_, d)| d.is_zero()) {
            return misuse(format!(
                "`{what}` is zero: the stream would keep nothing, or drop nothing sent twice"
            ));
        }
        if self.duplicate_window > self.retention {
            return misuse(format!(
                "`duplicate_window` ({:?}) is longer than `retention` ({:?}): the stream refuses it",
                self.duplicate_window, self.retention
            ));
        }
        if self.duplicate_window < takeover {
            return misuse(format!(
                "`duplicate_window` ({:?}) is shorter than a takeover may take — `standby`, the \
                 database's 30 s and `send_timeout`, {takeover:?}: a message sent again after it \
                 would be kept twice",
                self.duplicate_window
            ));
        }
        if self.max_body == 0 {
            return misuse("`max_body` is zero: the relay would publish nothing".to_owned());
        }
        if self.replicas == 0 {
            return misuse(
                "`replicas` is zero: a stream is held by one server at least".to_owned(),
            );
        }
        Ok(())
    }
}

/// A stream of that name, every other setting the default.
impl From<&str> for StreamSettings {
    fn from(name: &str) -> Self {
        StreamSettings::new(name)
    }
}

/// A stream of that name, every other setting the default.
impl From<String> for StreamSettings {
    fn from(name: String) -> Self {
        StreamSettings::new(name)
    }
}

/// A stream of that name, every other setting the default.
impl From<&String> for StreamSettings {
    fn from(name: &String) -> Self {
        StreamSettings::new(name)
    }
}

/// One stream the settings declare: its key — empty for the main stream —
/// and its settings.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Declared<'a> {
    pub(crate) key: &'a str,
    pub(crate) stream: &'a StreamSettings,
}

/// How the relay runs, and the streams it publishes to. [`Settings::new`]
/// takes the main stream and the events' source; [`Settings::stream`]
/// declares another stream; everything else has a default and a setter.
///
/// ```
/// use magic_events::{Settings, StreamSettings};
/// use std::time::Duration;
///
/// let settings = Settings::new(
///     StreamSettings::new("SHOP_EVENTS").retention(Duration::from_secs(30 * 24 * 60 * 60)),
///     "shop",
/// )
/// .stream("live", StreamSettings::new("SHOP_LIVE").retention(Duration::from_secs(600)));
/// ```
#[derive(Clone, Debug)]
pub struct Settings {
    pub(crate) source: String,
    /// The main stream: an event that names no stream goes there.
    pub(crate) main: StreamSettings,
    /// Every other stream, by key, as declared.
    pub(crate) others: Vec<(String, StreamSettings)>,
    pub(crate) idle: Duration,
    pub(crate) batch: u32,
    pub(crate) in_flight: u32,
    pub(crate) send_timeout: Duration,
    pub(crate) backoff: (Duration, Duration),
    pub(crate) standby: Duration,
    pub(crate) role: Option<String>,
    /// How long a statement of the relay's may take before the relay counts
    /// the database as away, lets go of its lock and stands by.
    pub(crate) database_timeout: Duration,
}

impl Settings {
    /// The relay of the main stream `stream` — its name, as
    /// `"SHOP_EVENTS"`, or all its settings, as a [`StreamSettings`] — its
    /// events' CloudEvents `source` the URI-reference `source` (`"shop"`,
    /// `"//shop.example.com"`). An event goes to the main stream unless it
    /// names another (`#[event(stream = …)]`).
    #[must_use]
    pub fn new(stream: impl Into<StreamSettings>, source: impl Into<String>) -> Self {
        Settings {
            source: source.into(),
            main: stream.into(),
            others: Vec::new(),
            idle: Duration::from_millis(200),
            batch: 256,
            in_flight: 64,
            send_timeout: Duration::from_secs(10),
            backoff: (Duration::from_millis(500), Duration::from_secs(60)),
            standby: Duration::from_secs(30),
            role: None,
            database_timeout: Duration::from_secs(30),
        }
    }

    /// Declares the stream `key` — what an event's `#[event(stream = …)]`
    /// names — as `stream`: a JetStream stream of its own, with its own
    /// subject prefix, retention and `max_body`, its own passes and its own
    /// backoff, so a stream the broker fails holds back no other. One relay
    /// publishes every stream it declares. A key is lowercase ASCII
    /// letters, digits and `_`, starting with a letter; [`Events::start`]
    /// refuses a key declared twice, two keys of one stream name, and
    /// subject prefixes one of which takes another's subjects.
    ///
    /// [`Events::start`]: crate::Events::start
    #[must_use]
    pub fn stream(mut self, key: impl Into<String>, stream: StreamSettings) -> Self {
        self.others.push((key.into(), stream));
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

    /// Messages sent and not acknowledged yet, at most, per stream: 64 by
    /// default. 1 sends one message at a time, each after the one before it
    /// was acknowledged.
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

    /// How long the relay waits after a failed pass of a stream: `first`,
    /// doubling with every failure of that stream in a row, up to `max`.
    /// 500 ms to 60 s by default.
    #[must_use]
    pub fn backoff(mut self, first: Duration, max: Duration) -> Self {
        self.backoff = (first, max);
        self
    }

    /// How often a process that is not the relay asks for the relay's lock:
    /// 30 s by default. With `send_timeout` and 30 s for the server to let a
    /// dead relay's lock go, within every stream's `duplicate_window`: a
    /// takeover inside the window sends nothing twice, and `start` refuses
    /// settings that would not fit.
    #[must_use]
    pub fn standby(mut self, standby: Duration) -> Self {
        self.standby = standby;
        self
    }

    /// The database role the crate's own work runs as: the relay's session,
    /// and the transactions of [`Events::start`](crate::Events::start),
    /// [`Events::status`](crate::Events::status),
    /// [`Events::unpark`](crate::Events::unpark) and
    /// [`Events::retire`](crate::Events::retire). The role the pool logs in
    /// as by default. With one, the pool's login role must be allowed to
    /// `SET ROLE` to it, and the grants the relay needs are this role's:
    /// a host whose transactions each assume a role of their own — under row
    /// security, say — keeps the outbox out of every role but the relay's.
    /// [`emit`](crate::emit) runs in the caller's transaction, as the
    /// caller's role; [`Events::install`](crate::Events::install) runs as
    /// the login role, which owns what it creates.
    #[must_use]
    pub fn role(mut self, role: impl Into<String>) -> Self {
        self.role = Some(role.into());
        self
    }

    /// Every stream declared, the main one first.
    pub(crate) fn declared(&self) -> impl Iterator<Item = Declared<'_>> {
        std::iter::once(Declared {
            key: "",
            stream: &self.main,
        })
        .chain(self.others.iter().map(|(key, stream)| Declared {
            key: key.as_str(),
            stream,
        }))
    }

    /// The stream `key` declares, if it declares one: empty for the main.
    pub(crate) fn find(&self, key: &str) -> Option<Declared<'_>> {
        self.declared().find(|d| d.key == key)
    }

    /// Refuses settings the relay cannot run with.
    pub(crate) fn check(&self) -> Result<(), Error> {
        let misuse = |why: String| Err(Error::Misuse(why));
        if self.role.as_deref() == Some("") {
            return misuse("an empty role: name one, or leave the login role's".to_owned());
        }
        if self.source.is_empty() {
            return misuse("an empty source: CloudEvents requires one".to_owned());
        }
        let source = crate::envelope::header_value(&self.source).len();
        if source > LONGEST_HEADER {
            return misuse(format!(
                "a source of {source} bytes as a header: it is 1 KiB at most"
            ));
        }
        let zero = [
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
        for (what, n) in [("batch", self.batch), ("in_flight", self.in_flight)] {
            if n == 0 {
                return misuse(format!("`{what}` is zero: the relay would publish nothing"));
            }
        }
        // A relay that died after the stream took a message, and before it
        // deleted the row, is followed by one that sends it again: within
        // the window, the stream drops it. The next relay takes over within
        // `standby`, the server letting the dead one's lock go within its
        // own wait, and sends within `send_timeout`.
        let takeover = self.standby + self.database_timeout + self.send_timeout;
        let declared: Vec<Declared<'_>> = self.declared().collect();
        for (i, d) in declared.iter().enumerate() {
            let of = if i == 0 {
                String::new()
            } else {
                if d.key.is_empty() {
                    return misuse(
                        "an empty stream key: the main stream is `Settings::new`'s, and another \
                         stream has a key of its own, as `live`"
                            .to_owned(),
                    );
                }
                if !is_key(d.key) {
                    return misuse(format!(
                        "`{}` is no stream key: one is lowercase ASCII letters, digits and `_`, \
                         starting with a letter, 32 bytes at most, as `live`",
                        d.key
                    ));
                }
                format!("the stream `{}`: ", d.key)
            };
            d.stream.check(&of, takeover)?;
            for earlier in &declared[..i] {
                let (a, b) = (earlier.stream, d.stream);
                let named = |key: &str| {
                    if key.is_empty() {
                        "the main stream".to_owned()
                    } else {
                        format!("the stream `{key}`")
                    }
                };
                if earlier.key == d.key {
                    return misuse(format!(
                        "the stream `{}` is declared twice: a key names one stream",
                        d.key
                    ));
                }
                if a.name == b.name {
                    return misuse(format!(
                        "{} and {} are both the JetStream stream `{}`: each key is a stream of \
                         its own",
                        named(earlier.key),
                        named(d.key),
                        b.name
                    ));
                }
                let (wa, wb) = (
                    format!("{}.>", a.subject_prefix),
                    format!("{}.>", b.subject_prefix),
                );
                if covers(&wa, &wb) || covers(&wb, &wa) {
                    return misuse(format!(
                        "the subject prefixes of {} (`{}`) and {} (`{}`) overlap: one takes the \
                         other's subjects, and a stream would hold another's events",
                        named(earlier.key),
                        a.subject_prefix,
                        named(d.key),
                        b.subject_prefix
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Whether `key` may name a stream: lowercase ASCII letters, digits and
/// `_`, starting with a letter, 32 bytes at most. The derive's code asks it
/// of every `stream = …` as it compiles, so a key is held to one rule.
pub const fn is_key(key: &str) -> bool {
    let bytes = key.as_bytes();
    if bytes.is_empty() || bytes.len() > LONGEST_KEY || !bytes[0].is_ascii_lowercase() {
        return false;
    }
    let mut i = 1;
    while i < bytes.len() {
        let c = bytes[i];
        if !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_') {
            return false;
        }
        i += 1;
    }
    true
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
            Settings::new("SHOP_EVENTS", "shop").main.subject_prefix,
            "shop_events"
        );
        assert_eq!(
            Settings::new(
                StreamSettings::new("SHOP_EVENTS").subject_prefix("shop.events"),
                "shop"
            )
            .main
            .subject_prefix,
            "shop.events"
        );
        assert_eq!(StreamSettings::new("SHOP_LIVE").subject_prefix, "shop_live");
        // The main stream by its name alone, however a host holds it.
        let name = "SHOP_EVENTS".to_owned();
        for settings in [
            Settings::new("SHOP_EVENTS", "shop"),
            Settings::new(&name, "shop"),
            Settings::new(name.clone(), "shop"),
        ] {
            assert_eq!(settings.main.name, "SHOP_EVENTS");
        }
        assert!(Settings::new("SHOP_EVENTS", "shop").check().is_ok());
    }

    #[test]
    fn every_stream_declared_is_found_by_its_key_the_main_one_first() {
        let settings = Settings::new("SHOP_EVENTS", "shop")
            .stream("live", StreamSettings::new("SHOP_LIVE"))
            .stream("audit", StreamSettings::new("SHOP_AUDIT"));
        assert!(settings.check().is_ok());
        let keys: Vec<(&str, &str)> = settings
            .declared()
            .map(|d| (d.key, d.stream.name.as_str()))
            .collect();
        assert_eq!(
            keys,
            [
                ("", "SHOP_EVENTS"),
                ("live", "SHOP_LIVE"),
                ("audit", "SHOP_AUDIT")
            ]
        );
        assert_eq!(settings.find("live").unwrap().stream.name, "SHOP_LIVE");
        assert!(settings.find("gone").is_none());
    }

    #[test]
    fn settings_the_relay_cannot_run_with_are_refused() {
        let base = || Settings::new("SHOP", "shop");
        // The main stream `SHOP` as `with` makes it.
        let main = |with: fn(StreamSettings) -> StreamSettings| {
            Settings::new(with(StreamSettings::new("SHOP")), "shop")
        };
        let live = || StreamSettings::new("LIVE");
        for (settings, what) in [
            (Settings::new("SHOP.EVENTS", "shop"), "stream name"),
            (Settings::new("SHOP EVENTS", "shop"), "stream name"),
            (Settings::new("", "shop"), "stream name"),
            (Settings::new("SHOP", ""), "source"),
            (Settings::new("SHOP", "x".repeat(1025)), "source"),
            (main(|s| s.subject_prefix("x".repeat(256))), "prefix"),
            (main(|s| s.subject_prefix("shop.*")), "prefix"),
            (main(|s| s.subject_prefix("shop..x")), "prefix"),
            (main(|s| s.subject_prefix("")), "prefix"),
            (base().idle(Duration::ZERO), "`idle`"),
            (
                base().backoff(Duration::from_secs(2), Duration::from_secs(1)),
                "`backoff`",
            ),
            (
                main(|s| {
                    s.retention(Duration::from_secs(60))
                        .duplicate_window(Duration::from_secs(120))
                }),
                "`duplicate_window`",
            ),
            (base().in_flight(0), "`in_flight`"),
            (
                base().standby(Duration::from_secs(300)),
                "a takeover may take",
            ),
            (base().batch(0), "`batch`"),
            (main(|s| s.max_body(0)), "`max_body`"),
            (main(|s| s.replicas(0)), "`replicas`"),
            (base().stream("Live", live()), "no stream key"),
            (base().stream("", live()), "an empty stream key"),
            (base().stream("2live", live()), "no stream key"),
            (base().stream("x".repeat(33), live()), "no stream key"),
            (
                base().stream("live", live().max_body(0)),
                "the stream `live`: `max_body`",
            ),
            (
                base().stream("live", StreamSettings::new("LIVE STREAM")),
                "the stream `live`: `LIVE STREAM` is no stream name",
            ),
            (
                base().stream("live", live().retention(Duration::from_secs(60))),
                "the stream `live`: `duplicate_window`",
            ),
            (
                base()
                    .stream("live", live())
                    .stream("live", StreamSettings::new("LIVE2")),
                "declared twice",
            ),
            (
                base().stream("live", StreamSettings::new("SHOP").subject_prefix("live")),
                "the main stream and the stream `live` are both the JetStream stream `SHOP`",
            ),
            (
                base().stream("live", live().subject_prefix("shop.live")),
                "the subject prefixes of the main stream (`shop`) and the stream `live` \
                 (`shop.live`) overlap",
            ),
            (
                main(|s| s.subject_prefix("shop.events"))
                    .stream("live", live().subject_prefix("shop")),
                "overlap",
            ),
            (
                base()
                    .stream("live", live())
                    .stream("audit", StreamSettings::new("AUDIT").subject_prefix("live")),
                "the subject prefixes of the stream `live` (`live`) and the stream `audit` \
                 (`live`) overlap",
            ),
        ] {
            match settings.check() {
                Err(Error::Misuse(why)) => assert!(why.contains(what), "{what}: {why}"),
                other => panic!("{what}: {other:?}"),
            }
        }
        // Prefixes that share text, and no token, do not overlap.
        assert!(main(|s| s.subject_prefix("shop.events"))
            .stream("live", live().subject_prefix("shop.events_live"))
            .check()
            .is_ok());
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

    #[test]
    fn a_key_is_a_short_lowercase_identifier() {
        for good in ["live", "a", "live_2", &"x".repeat(32)] {
            assert!(is_key(good), "{good}");
        }
        for bad in [
            "",
            "Live",
            "2live",
            "_live",
            "li-ve",
            "li.ve",
            "é",
            &"x".repeat(33),
        ] {
            assert!(!is_key(bad), "{bad:?}");
        }
    }
}
