//! What a message is, byte for byte: headers as the binding encodes them,
//! and the body as serde writes the event — for an enum, its variant's
//! fields only.

use super::*;
use chrono::TimeZone;
use serde::Serialize;

#[derive(Serialize, crate::Event)]
#[event(
    type = "order.shipped",
    version = 1,
    subject = "orders.{store}.shipped",
    ce_subject = "{store}"
)]
struct OrderShipped {
    store: String,
    ids: Vec<u64>,
}

#[derive(Serialize)]
struct Ids {
    store: u32,
    ids: Vec<u64>,
}

#[derive(Serialize, crate::Event)]
#[serde(rename_all = "snake_case")]
enum Catalog {
    #[event(
        type = "product.updated",
        version = 1,
        subject = "catalog.{store}.product.updated"
    )]
    ProductUpdated {
        store: u32,
        #[serde(rename = "products")]
        ids: Vec<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        note: Option<String>,
    },
    #[event(
        type = "product.archived",
        version = 3,
        subject = "catalog.{store}.product.archived"
    )]
    ProductArchived(Ids),
}

#[test]
fn a_header_value_is_percent_encoded_as_the_binding_says() {
    for (value, encoded) in [
        ("order.shipped.v1", "order.shipped.v1"),
        ("//shop.example.com/a?b=c", "//shop.example.com/a?b=c"),
        ("two words", "two%20words"),
        ("\"quoted\"", "%22quoted%22"),
        ("100%", "100%25"),
        ("tab\there", "tab%09here"),
        ("del\u{7f}", "del%7F"),
        // The binding's own example.
        ("Euro \u{20ac} \u{1f600}", "Euro%20%E2%82%AC%20%F0%9F%98%80"),
    ] {
        assert_eq!(header_value(value), encoded, "{value:?}");
    }
}

#[test]
fn a_variants_body_is_its_fields_alone_as_serde_writes_them() {
    let body = |c: &Catalog| String::from_utf8(super::body(c, true).unwrap()).unwrap();
    assert_eq!(
        body(&Catalog::ProductUpdated {
            store: 7,
            ids: vec![1, u64::MAX],
            note: None,
        }),
        r#"{"store":7,"products":[1,18446744073709551615]}"#
    );
    assert_eq!(
        body(&Catalog::ProductUpdated {
            store: 7,
            ids: vec![],
            note: Some("a \"note\"".to_owned()),
        }),
        r#"{"store":7,"products":[],"note":"a \"note\""}"#
    );
    assert_eq!(
        body(&Catalog::ProductArchived(Ids {
            store: 7,
            ids: vec![2]
        })),
        r#"{"store":7,"ids":[2]}"#
    );
}

/// A value that is no variant with fields: only a hand-written `Serialize`
/// gets here, and it is refused rather than half-written.
#[test]
fn a_value_that_is_no_variant_with_fields_is_refused() {
    #[derive(Serialize)]
    enum Unit {
        Gone,
    }
    match super::body(&Unit::Gone, true) {
        Err(Error::Invalid(why)) => assert!(why.contains("externally tagged"), "{why}"),
        other => panic!("{other:?}"),
    }
    match super::body(&7_u32, true) {
        Err(Error::Invalid(why)) => assert!(why.contains("does not serialize"), "{why}"),
        other => panic!("{other:?}"),
    }
    // A newtype variant around something that is not a struct.
    #[derive(Serialize)]
    enum Wraps {
        Number(u32),
    }
    match super::body(&Wraps::Number(5), true) {
        Err(Error::Invalid(why)) => assert!(why.contains("not a JSON object: 5"), "{why}"),
        other => panic!("{other:?}"),
    }
    match super::body(&"text", false) {
        Err(Error::Invalid(why)) => assert!(why.contains("not a JSON object"), "{why}"),
        other => panic!("{other:?}"),
    }
}

/// A row of the outbox as `emit` writes it for `event`, with a fixed id and
/// time.
fn pending<E: crate::Event>(event: &E, n: u128) -> Pending {
    let described = event.__describe();
    let ty = format!("{}.v{}", described.r#type, described.version);
    let mut headers = serde_json::Map::new();
    headers.insert("ce-type".to_owned(), header_value(&ty).into());
    if let Some(s) = &described.ce_subject {
        headers.insert("ce-subject".to_owned(), header_value(s).into());
    }
    Pending {
        id: 1,
        event_id: Uuid::from_u128(0x0192_f6a0_0000_7000_8000_0000_0000_0000 | n),
        subject: described.subject.join("."),
        ty,
        headers: sqlx::types::Json(headers),
        body: super::body(event, described.variant.is_some()).unwrap(),
        emitted_at: Utc
            .with_ymd_and_hms(2027, 1, 15, 8, 0, 0)
            .unwrap()
            .checked_add_signed(chrono::Duration::microseconds(123_456))
            .unwrap(),
    }
}

/// Holds `text` against the golden file `path`; `MAGIC_EVENTS_BLESS=1`
/// writes it anew.
pub(crate) fn golden(path: &str, text: &str) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(path);
    if std::env::var_os("MAGIC_EVENTS_BLESS").is_some() {
        std::fs::write(&path, text).unwrap();
    }
    let written = std::fs::read_to_string(&path).unwrap_or_default();
    assert!(
        written == text,
        "{} is not what this build publishes: run its test with MAGIC_EVENTS_BLESS=1, then \
         read the difference before committing it",
        path.display()
    );
}

/// The messages a struct and an enum's variants publish, headers and body,
/// are the ones checked in: a change shows as a difference to read.
#[test]
fn the_messages_are_the_golden_ones() {
    let events: [(&str, Pending); 3] = [
        (
            "a struct",
            pending(
                &OrderShipped {
                    store: "north-1".to_owned(),
                    ids: vec![10, 11],
                },
                1,
            ),
        ),
        (
            "a struct variant",
            pending(
                &Catalog::ProductUpdated {
                    store: 7,
                    ids: vec![3],
                    note: Some("ñ".to_owned()),
                },
                2,
            ),
        ),
        (
            "a newtype variant",
            pending(
                &Catalog::ProductArchived(Ids {
                    store: 7,
                    ids: vec![4, 5],
                }),
                3,
            ),
        ),
    ];
    let mut text = String::from(
        "# What the relay publishes for an event, under the prefix `shop` and the source\n\
         # `//shop.example.com/eu west`: the subject, each header as sent, then the body.\n\
         # Generated by envelope's `the_messages_are_the_golden_ones`; MAGIC_EVENTS_BLESS=1\n\
         # writes it anew.\n",
    );
    for (what, row) in events {
        let message = row.message("shop", "//shop.example.com/eu west");
        text.push_str(&format!("\n# {what}\nsubject {}\n", message.subject));
        for (name, value) in &message.headers {
            text.push_str(&format!("header {name}: {value}\n"));
        }
        text.push_str(&format!("body {}\n", String::from_utf8(row.body).unwrap()));
    }
    golden("tests/golden/messages.txt", &text);
}
