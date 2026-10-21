//! What a message is: a subject, the CloudEvents attributes as NATS headers
//! (the binding's binary content mode), and the event's JSON as the body.
//!
//! The [NATS protocol binding] of CloudEvents puts each attribute in a header
//! of its own, named `ce-` and the attribute, its value percent-encoded; the
//! body is the event's data, as its `datacontenttype` says. The binding's
//! last release (1.0.2) predates NATS headers and knows only the structured
//! mode; binary mode is in the binding since, and is what this follows.
//!
//! [NATS protocol binding]: https://github.com/cloudevents/spec/blob/main/cloudevents/bindings/nats-protocol-binding.md

// The relay, which publishes these, comes next.
#![allow(dead_code)]

use crate::Error;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::ser::{self, Serialize};
use uuid::Uuid;

/// The CloudEvents version every message says it is.
pub(crate) const SPEC_VERSION: &str = "1.0";

/// The body's media type: the event, serialized by serde as JSON.
pub(crate) const CONTENT_TYPE: &str = "application/json";

/// `value` as a NATS header value, percent-encoded as the binding says:
/// each byte of a space, a double quote, a percent sign, or anything outside
/// printable ASCII, as `%XY`, upper case.
pub(crate) fn header_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        if matches!(c, '!'..='~') && !matches!(c, '"' | '%') {
            out.push(c);
        } else {
            let mut bytes = [0; 4];
            for b in c.encode_utf8(&mut bytes).bytes() {
                out.push_str(&format!("%{b:02X}"));
            }
        }
    }
    out
}

/// The body of `event`: its JSON, or for a variant of an enum, the JSON of
/// the variant's own fields — the variant is in `ce-type` already.
pub(crate) fn body<E: Serialize + ?Sized>(event: &E, variant: bool) -> Result<Vec<u8>, Error> {
    let mut out = Vec::with_capacity(128);
    let written = if variant {
        event.serialize(Unwrap { out: &mut out })
    } else {
        serde_json::to_writer(&mut out, event).map_err(|e| Refused(e.to_string()))
    };
    match written {
        Ok(()) => Ok(out),
        Err(Refused(why)) => Err(Error::Invalid(format!(
            "the event does not serialize as JSON: {why}"
        ))),
    }
}

/// Why a body could not be written.
#[derive(Debug)]
struct Refused(String);

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Refused {}

impl ser::Error for Refused {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Refused(msg.to_string())
    }
}

impl From<serde_json::Error> for Refused {
    fn from(e: serde_json::Error) -> Self {
        Refused(e.to_string())
    }
}

/// A serializer of one enum variant that writes only what it carries: the
/// fields of a struct variant as one JSON object, or the value a newtype
/// variant wraps, just as `serde_json` writes them.
struct Unwrap<'a> {
    out: &'a mut Vec<u8>,
}

/// The fields of a struct variant, written one by one.
struct Fields<'a> {
    out: &'a mut Vec<u8>,
    first: bool,
}

impl ser::SerializeStructVariant for Fields<'_> {
    type Ok = ();
    type Error = Refused;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Refused> {
        if !self.first {
            self.out.push(b',');
        }
        self.first = false;
        serde_json::to_writer(&mut *self.out, key)?;
        self.out.push(b':');
        serde_json::to_writer(&mut *self.out, value)?;
        Ok(())
    }

    fn end(self) -> Result<(), Refused> {
        self.out.push(b'}');
        Ok(())
    }
}

/// What a value that is no variant of fields answers: the derive refuses
/// such an enum, so only a hand-written `Serialize` gets here.
fn not_a_variant<T>() -> Result<T, Refused> {
    Err(Refused(
        "an event enum serializes each value as a variant with fields of its own (serde's \
         default, externally tagged)"
            .to_owned(),
    ))
}

impl<'a> ser::Serializer for Unwrap<'a> {
    type Ok = ();
    type Error = Refused;
    type SerializeSeq = ser::Impossible<(), Refused>;
    type SerializeTuple = ser::Impossible<(), Refused>;
    type SerializeTupleStruct = ser::Impossible<(), Refused>;
    type SerializeTupleVariant = ser::Impossible<(), Refused>;
    type SerializeMap = ser::Impossible<(), Refused>;
    type SerializeStruct = ser::Impossible<(), Refused>;
    type SerializeStructVariant = Fields<'a>;

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<(), Refused> {
        serde_json::to_writer(self.out, value)?;
        Ok(())
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Fields<'a>, Refused> {
        self.out.push(b'{');
        Ok(Fields {
            out: self.out,
            first: true,
        })
    }

    fn serialize_bool(self, _: bool) -> Result<(), Refused> {
        not_a_variant()
    }
    fn serialize_i8(self, _: i8) -> Result<(), Refused> {
        not_a_variant()
    }
    fn serialize_i16(self, _: i16) -> Result<(), Refused> {
        not_a_variant()
    }
    fn serialize_i32(self, _: i32) -> Result<(), Refused> {
        not_a_variant()
    }
    fn serialize_i64(self, _: i64) -> Result<(), Refused> {
        not_a_variant()
    }
    fn serialize_u8(self, _: u8) -> Result<(), Refused> {
        not_a_variant()
    }
    fn serialize_u16(self, _: u16) -> Result<(), Refused> {
        not_a_variant()
    }
    fn serialize_u32(self, _: u32) -> Result<(), Refused> {
        not_a_variant()
    }
    fn serialize_u64(self, _: u64) -> Result<(), Refused> {
        not_a_variant()
    }
    fn serialize_f32(self, _: f32) -> Result<(), Refused> {
        not_a_variant()
    }
    fn serialize_f64(self, _: f64) -> Result<(), Refused> {
        not_a_variant()
    }
    fn serialize_char(self, _: char) -> Result<(), Refused> {
        not_a_variant()
    }
    fn serialize_str(self, _: &str) -> Result<(), Refused> {
        not_a_variant()
    }
    fn serialize_bytes(self, _: &[u8]) -> Result<(), Refused> {
        not_a_variant()
    }
    fn serialize_none(self) -> Result<(), Refused> {
        not_a_variant()
    }
    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> Result<(), Refused> {
        not_a_variant()
    }
    fn serialize_unit(self) -> Result<(), Refused> {
        not_a_variant()
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<(), Refused> {
        not_a_variant()
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
    ) -> Result<(), Refused> {
        not_a_variant()
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: &T,
    ) -> Result<(), Refused> {
        not_a_variant()
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Refused> {
        not_a_variant()
    }
    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, Refused> {
        not_a_variant()
    }
    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct, Refused> {
        not_a_variant()
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, Refused> {
        not_a_variant()
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Refused> {
        not_a_variant()
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct, Refused> {
        not_a_variant()
    }
}

/// One row of the outbox, as the relay publishes it.
#[derive(Clone, Debug, sqlx::FromRow)]
pub(crate) struct Pending {
    pub(crate) id: i64,
    pub(crate) event_id: Uuid,
    pub(crate) subject: String,
    #[sqlx(rename = "type")]
    pub(crate) ty: String,
    pub(crate) headers: sqlx::types::Json<serde_json::Map<String, serde_json::Value>>,
    pub(crate) body: Vec<u8>,
    pub(crate) emitted_at: DateTime<Utc>,
}

/// The message of a row: its full subject, and its headers, in the order
/// they are written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Message {
    pub(crate) subject: String,
    pub(crate) headers: Vec<(&'static str, String)>,
}

/// The NATS header JetStream deduplicates by.
pub(crate) const MSG_ID: &str = "Nats-Msg-Id";

impl Pending {
    /// Its message: the subject under `prefix`, the event's attributes with
    /// `source`'s, and its id again as the broker's deduplication id.
    pub(crate) fn message(&self, prefix: &str, source: &str) -> Message {
        let id = self.event_id.to_string();
        let rendered = |name: &str| {
            self.headers
                .get(name)
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        };
        let mut headers = vec![
            ("ce-specversion", SPEC_VERSION.to_owned()),
            ("ce-id", id.clone()),
            ("ce-source", header_value(source)),
            (
                "ce-type",
                rendered("ce-type").unwrap_or_else(|| header_value(&self.ty)),
            ),
            (
                "ce-time",
                self.emitted_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            ),
            ("ce-datacontenttype", CONTENT_TYPE.to_owned()),
        ];
        if let Some(subject) = rendered("ce-subject") {
            headers.push(("ce-subject", subject));
        }
        headers.push((MSG_ID, id));
        Message {
            subject: format!("{prefix}.{}", self.subject),
            headers,
        }
    }
}

#[cfg(test)]
#[path = "envelope_tests.rs"]
mod tests;
