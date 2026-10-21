//! The status against a known state, field by field, under every subject
//! and under a narrower filter; the JSON a dashboard reads, held to the
//! dashboard's fixtures; and `recent`, which leaves nothing behind.

use super::*;
use crate::tests::{emit_each, idle, shipped, Broker, Faulty};
use crate::Events;
use async_nats::jetstream::consumer::pull;
use std::sync::Arc;

/// The known state: three messages published (two to `north`, one to
/// `south`), one parked (`poison`), one pending (`north`); two durable
/// consumers, `billing` of every order and `shipping` of the north's.
async fn known(pool: &PgPool) -> (Broker, Events) {
    let broker = Broker::new().await;
    let faulty = Faulty::new(broker.jetstream.clone());
    let events = idle(pool, &broker, Arc::new(faulty)).await;
    emit_each(
        pool,
        &[
            shipped("north", 1),
            shipped("north", 2),
            shipped("south", 3),
            shipped("poison", 4),
        ],
    )
    .await;
    for _ in 0..3 {
        events.pass_now().await.unwrap();
    }
    emit_each(pool, &[shipped("north", 5)]).await;
    let prefix = &broker.settings.subject_prefix;
    let stream = broker
        .jetstream
        .get_stream(&broker.settings.stream)
        .await
        .unwrap();
    for (name, filter) in [
        ("billing", format!("{prefix}.orders.>")),
        ("shipping", format!("{prefix}.orders.north.>")),
    ] {
        stream
            .create_consumer(pull::Config {
                durable_name: Some(name.to_owned()),
                filter_subject: filter,
                ..Default::default()
            })
            .await
            .unwrap();
    }
    (broker, events)
}

#[sqlx::test(migrations = false)]
async fn every_field_of_the_status_under_every_subject(pool: PgPool) {
    let (broker, events) = known(&pool).await;
    let prefix = broker.settings.subject_prefix.clone();
    let status = events.status(&Filter::all()).await.unwrap();
    assert_eq!(status.contract, CONTRACT);
    assert_eq!(status.filter, format!("{prefix}.>"));

    assert_eq!((status.outbox.pending, status.outbox.parked), (1, 1));
    assert!(status
        .outbox
        .oldest_pending_at
        .is_some_and(|t| t <= status.at));
    let parked = &status.outbox.first_parked[0];
    assert_eq!(parked.subject, format!("{prefix}.orders.poison.shipped"));
    assert_eq!(parked.ty, "order.shipped.v1");
    assert_eq!(parked.attempts, 3);
    assert_eq!(
        parked.last_error.as_deref(),
        Some("a message the stream refuses")
    );
    assert!(parked.emitted_at <= parked.parked_at);

    assert!(!status.relay.running, "nothing runs the relay");
    assert!(status.relay.last_published_at.is_some());
    assert_eq!(
        (status.relay.failures, &status.relay.last_error),
        (0, &None)
    );

    assert_eq!(status.broker_error, None);
    let stream = status.stream.as_ref().unwrap();
    assert_eq!(stream.name, broker.settings.stream);
    assert_eq!(stream.messages, 3);
    assert!(stream.bytes.is_some_and(|b| b > 0));
    assert!(stream.first_at.is_some() && stream.last_at.is_some());
    assert_eq!(stream.subjects_total, 2);
    let subjects: Vec<(&str, u64)> = stream
        .subjects
        .iter()
        .map(|s| (s.subject.as_str(), s.messages))
        .collect();
    assert_eq!(
        subjects,
        [
            (format!("{prefix}.orders.north.shipped").as_str(), 2),
            (format!("{prefix}.orders.south.shipped").as_str(), 1)
        ]
    );

    let consumers: Vec<(&str, u64)> = status
        .consumers
        .as_ref()
        .unwrap()
        .iter()
        .map(|c| (c.name.as_str(), c.pending))
        .collect();
    assert_eq!(consumers, [("billing", 3), ("shipping", 2)]);
    let billing = &status.consumers.as_ref().unwrap()[0];
    assert_eq!(billing.filters, [format!("{prefix}.orders.>")]);
    assert_eq!((billing.ack_pending, billing.redelivered), (0, 0));

    let published = status.published.as_ref().unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(
        (published[0].ty.as_str(), published[0].count),
        ("order.shipped.v1", 3)
    );
    assert_eq!(published[0].day, status.at.date_naive());
    broker.done().await;
}

/// Under a narrower filter, everything is narrowed, and what cannot be — the
/// consumers, the published counts, the stream's own totals — is left out.
#[sqlx::test(migrations = false)]
async fn a_narrower_filter_answers_only_what_it_takes(pool: PgPool) {
    let (broker, events) = known(&pool).await;
    let prefix = broker.settings.subject_prefix.clone();
    let north = events
        .status(&Filter::subject(format!("{prefix}.orders.north.>")))
        .await
        .unwrap();
    assert_eq!((north.outbox.pending, north.outbox.parked), (1, 0));
    assert!(north.outbox.first_parked.is_empty());
    let stream = north.stream.as_ref().unwrap();
    assert_eq!((stream.messages, stream.subjects_total), (2, 1));
    assert_eq!(
        (stream.bytes, stream.first_at, stream.last_at),
        (None, None, None)
    );
    assert!(north.consumers.is_none() && north.published.is_none());

    // The prefix's every subject, spelled as a filter, is every subject.
    let every = events
        .status(&Filter::subject(format!("{prefix}.>")))
        .await
        .unwrap();
    assert!(every.consumers.is_some() && every.published.is_some());

    let poison = events
        .status(&Filter::subject(format!("{prefix}.*.poison.*")))
        .await
        .unwrap();
    assert_eq!((poison.outbox.pending, poison.outbox.parked), (0, 1));
    assert_eq!(poison.stream.as_ref().unwrap().messages, 0);
    broker.done().await;
}

#[sqlx::test(migrations = false)]
async fn a_filter_not_under_the_prefix_is_refused(pool: PgPool) {
    let (broker, events) = known(&pool).await;
    let prefix = broker.settings.subject_prefix.clone();
    for filter in [
        "other.>".to_owned(),
        ">".to_owned(),
        prefix.clone(),
        format!("{prefix}.>.x"),
        format!("{prefix}..x"),
        format!("{prefix}.a b"),
    ] {
        match events.status(&Filter::subject(&filter)).await {
            Err(crate::Error::Misuse(why)) => assert!(why.contains("no subject filter"), "{why}"),
            other => panic!("{filter}: {other:?}"),
        }
    }
    broker.done().await;
}

/// A broker that cannot answer leaves its part out and says why: the status
/// is what one looks at when things fail.
#[sqlx::test(migrations = false)]
async fn without_the_stream_the_status_says_why(pool: PgPool) {
    let (broker, events) = known(&pool).await;
    broker
        .jetstream
        .delete_stream(&broker.settings.stream)
        .await
        .unwrap();
    let status = events.status(&Filter::all()).await.unwrap();
    assert!(status.stream.is_none() && status.consumers.is_none());
    let why = status.broker_error.unwrap();
    assert!(why.contains("could not be read"), "{why}");
    assert_eq!(status.outbox.pending, 1, "the database's part is there");
}

#[sqlx::test(migrations = false)]
async fn recent_reads_the_last_messages_and_leaves_nothing(pool: PgPool) {
    let broker = Broker::new().await;
    let events = idle(&pool, &broker, Arc::new(broker.jetstream.clone())).await;
    let all: Vec<_> = (0..30)
        .map(|n| shipped(if n % 3 == 0 { "north" } else { "south" }, n))
        .collect();
    emit_each(&pool, &all).await;
    while events.pass_now().await.unwrap().unwrap() > 0 {}
    let prefix = broker.settings.subject_prefix.clone();
    let n = |recent: &Recent| -> Vec<u64> {
        recent
            .messages
            .iter()
            .map(|m| {
                serde_json::from_str::<serde_json::Value>(&m.body).unwrap()["n"]
                    .as_u64()
                    .unwrap()
            })
            .collect()
    };

    let north = events
        .recent(&Filter::subject(format!("{prefix}.orders.north.>")), 4)
        .await
        .unwrap();
    assert_eq!(n(&north), [18, 21, 24, 27]);
    let m = &north.messages[3];
    assert_eq!(m.subject, format!("{prefix}.orders.north.shipped"));
    assert_eq!(m.headers["ce-type"], "order.shipped.v1");
    assert_eq!(m.headers["ce-id"], m.headers["Nats-Msg-Id"]);
    assert_eq!(m.sequence, 28);

    assert_eq!(
        n(&events.recent(&Filter::all(), 100).await.unwrap()),
        (0..30).collect::<Vec<_>>()
    );
    assert_eq!(n(&events.recent(&Filter::all(), 1).await.unwrap()), [29]);
    assert!(events
        .recent(&Filter::all(), 0)
        .await
        .unwrap()
        .messages
        .is_empty());
    let none = events
        .recent(&Filter::subject(format!("{prefix}.catalog.>")), 5)
        .await
        .unwrap();
    assert!(none.messages.is_empty());

    // Not a consumer left on the broker, durable or not.
    let left: Vec<String> = broker
        .jetstream
        .get_stream(&broker.settings.stream)
        .await
        .unwrap()
        .consumer_names()
        .try_collect()
        .await
        .unwrap();
    assert!(left.is_empty(), "{left:?}");
    broker.done().await;
}

/// The JSON a dashboard reads is what `dashboard/src/core/contract/fixtures/`
/// holds: the known state under every subject and under one store, and the
/// last messages, ids, times and names made stable. Each fixture is checked
/// against the generated types by the dashboard's type-check and read by its
/// tests: a change on either side shows on the other. `UPDATE_CONTRACT=1`
/// writes them.
#[sqlx::test(migrations = false)]
async fn the_json_a_dashboard_reads_is_the_contract(pool: PgPool) {
    let (broker, events) = known(&pool).await;
    let prefix = broker.settings.subject_prefix.clone();
    let all = events.status(&Filter::all()).await.unwrap();
    let north = events
        .status(&Filter::subject(format!("{prefix}.orders.north.>")))
        .await
        .unwrap();
    let recent = events.recent(&Filter::all(), 10).await.unwrap();
    let mut stable = Stable::new(&broker.settings.stream, &prefix);
    for (file, ty, value) in [
        ("status", "Status", serde_json::to_value(&all).unwrap()),
        ("narrowed", "Status", serde_json::to_value(&north).unwrap()),
        ("recent", "Recent", serde_json::to_value(&recent).unwrap()),
    ] {
        let json = serde_json::to_string_pretty(&stable.of(value)).unwrap();
        let text = format!(
            "// Generated by magic_events' `the_json_a_dashboard_reads_is_the_contract`: \
             UPDATE_CONTRACT=1 rewrites it.\n\
             import type {{ {ty} }} from '../types.js'\n\n\
             export default {json} satisfies {ty}\n"
        );
        matches_contract(&format!("fixtures/{file}.ts"), &text);
    }
    broker.done().await;
}

/// Holds `text` against `dashboard/src/core/contract/<file>`, the copy the
/// dashboard builds and tests against; `UPDATE_CONTRACT=1` writes it.
pub(crate) fn matches_contract(file: &str, text: &str) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../dashboard/src/core/contract")
        .join(file);
    if std::env::var_os("UPDATE_CONTRACT").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, text).unwrap();
    }
    let written = std::fs::read_to_string(&path).unwrap_or_default();
    assert!(
        written == text,
        "{file} is not what the relay answers: UPDATE_CONTRACT=1 rewrites it"
    );
}

/// The dashboard's types for the answers are these types, generated: a
/// field renamed, added or typed otherwise here changes them there, and the
/// dashboard's type-check reads its fixtures against them.
#[test]
fn the_dashboards_types_are_generated_from_these() {
    use ts_rs::TS;
    let cfg = ts_rs::Config::new().with_large_int("number");
    let decls = [
        (Status::docs(), Status::decl(&cfg)),
        (Outbox::docs(), Outbox::decl(&cfg)),
        (Parked::docs(), Parked::decl(&cfg)),
        (Relay::docs(), Relay::decl(&cfg)),
        (Stream::docs(), Stream::decl(&cfg)),
        (SubjectCount::docs(), SubjectCount::decl(&cfg)),
        (Consumer::docs(), Consumer::decl(&cfg)),
        (Published::docs(), Published::decl(&cfg)),
        (Recent::docs(), Recent::decl(&cfg)),
        (Message::docs(), Message::decl(&cfg)),
    ];
    let mut text = format!(
        "// Generated from magic_events::status by its test \
         `the_dashboards_types_are_generated_from_these`: UPDATE_CONTRACT=1 rewrites it.\n\n\
         /** The contract these types describe: an answer of another is refused. */\n\
         export const CONTRACT = {CONTRACT}\n"
    );
    for (docs, decl) in decls {
        text.push('\n');
        // Rust's intra-doc links read as plain code in TypeScript.
        text.push_str(
            &docs
                .unwrap_or_default()
                .replace("[`", "`")
                .replace("`]", "`"),
        );
        text.push_str("export ");
        text.push_str(&decl);
        text.push('\n');
    }
    matches_contract("types.ts", &text);
}

/// A filter's pattern takes, in PostgreSQL, the subjects NATS would: every
/// character but `*` and `>` as written.
#[sqlx::test(migrations = false)]
async fn a_filter_is_a_pattern_postgresql_matches(pool: PgPool) {
    for (filter, subject, matches) in [
        ("shop.>", "shop.orders.north", true),
        ("shop.>", "shop", false),
        ("shop.orders.*", "shop.orders.north", true),
        ("shop.orders.*", "shop.orders.north.x", false),
        ("shop.orders.n+rth", "shop.orders.n+rth", true),
        ("shop.orders.n+rth", "shop.orders.nnrth", false),
        ("shop_events.a-b~c{1}", "shop_events.a-b~c{1}", true),
        ("shop_events.a-b~c{1}", "shopXevents.a-b~c{1}", false),
        ("shop.a$b.^c", "shop.a$b.^c", true),
    ] {
        let found: bool = sqlx::query_scalar("SELECT $1 ~ $2")
            .bind(subject)
            .bind(pattern(filter))
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(found, matches, "{filter} {subject} {}", pattern(filter));
    }
}

/// Ids, times, days and the test's own names made stable: each id the same
/// placeholder wherever it appears, in the order it first does; every time
/// and every day one moment; the stream and prefix the shop's.
pub(crate) struct Stable {
    ids: BTreeMap<String, String>,
    stream: String,
    prefix: String,
}

impl Stable {
    pub(crate) fn new(stream: &str, prefix: &str) -> Self {
        Stable {
            ids: BTreeMap::new(),
            stream: stream.to_owned(),
            prefix: prefix.to_owned(),
        }
    }

    pub(crate) fn of(&mut self, value: serde_json::Value) -> serde_json::Value {
        use serde_json::Value;
        match value {
            Value::String(s) => Value::String(self.text(&s)),
            Value::Array(items) => Value::Array(items.into_iter().map(|v| self.of(v)).collect()),
            Value::Object(fields) => Value::Object(
                fields
                    .into_iter()
                    .map(|(k, v)| (self.text(&k), self.of(v)))
                    .collect(),
            ),
            other => other,
        }
    }

    fn text(&mut self, s: &str) -> String {
        if chrono::DateTime::parse_from_rfc3339(s).is_ok() {
            let moment = DateTime::from_timestamp(1_800_000_000, 123_456_000).unwrap();
            return serde_json::to_value(moment)
                .unwrap()
                .as_str()
                .unwrap()
                .to_owned();
        }
        if NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok() {
            return "2027-01-15".to_owned();
        }
        let mut out = s
            .replace(&self.stream, "SHOP_EVENTS")
            .replace(&self.prefix, "shop_events");
        let mut at = 0;
        while let Some(found) = out.get(at..).and_then(|rest| {
            rest.char_indices()
                .find(|(i, _)| {
                    rest.get(*i..*i + 36)
                        .is_some_and(|c| Uuid::parse_str(c).is_ok())
                })
                .map(|(i, _)| at + i)
        }) {
            let id = out[found..found + 36].to_owned();
            let next = self.ids.len() + 1;
            let stable = self
                .ids
                .entry(id)
                .or_insert_with(|| format!("0192f6a0-0000-7000-8000-{next:012}"))
                .clone();
            out.replace_range(found..found + 36, &stable);
            at = found + 36;
        }
        out
    }
}
