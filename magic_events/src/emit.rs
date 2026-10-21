//! `emit`: an event, written to the outbox in the caller's transaction.

use crate::envelope::{self, header_value};
use crate::settings::{is_token, LONGEST_HEADER, LONGEST_SUBJECT, LONGEST_TYPE, MAX_BODY};
use crate::{Error, Event};
use sqlx::PgConnection;
use uuid::Uuid;

/// Writes `event` to the outbox on `conn`, as one more statement of the
/// caller's transaction: it is published if and only if that transaction
/// commits. Answers the event's id — its `ce-id`.
///
/// It does no broker I/O, so it may stand wherever a write does, and slows
/// the writer by one `INSERT`.
///
/// ```no_run
/// # #[derive(serde::Serialize, magic_events::Event)]
/// # #[event(type = "order.shipped", version = 1, subject = "orders.{store}.shipped")]
/// # struct OrderShipped { store: u32, ids: Vec<u64> }
/// # async fn ship(pool: sqlx::PgPool, store: u32, ids: Vec<u64>) -> Result<(), magic_events::Error> {
/// let mut tx = pool.begin().await?;
/// // … the writes the event is about …
/// magic_events::emit(&mut *tx, &OrderShipped { store, ids }).await?;
/// tx.commit().await?;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// [`Error::Invalid`], before anything is written, for an event that could
/// never be published as it is: a subject token that is empty or holds a
/// `.`, a `*`, a `>`, whitespace or anything but printable ASCII; a body
/// larger than the relay's `max_body`
/// ([`Settings::max_body`](crate::Settings::max_body), 64 KiB unless a relay
/// started with another), or that is not a JSON object; a subject over
/// 1 KiB, or a `ce-subject` over 1 KiB once percent-encoded; a value that
/// does not serialize. [`Error::Database`] when the database
/// fails — the schema not installed, say, or a role that may not write it.
pub async fn emit<E: Event>(conn: &mut PgConnection, event: &E) -> Result<Uuid, Error> {
    let described = event.__describe();
    let subject = described.subject.join(".");
    if let Some(token) = described.subject.iter().find(|t| !is_token(t)) {
        return Err(Error::Invalid(format!(
            "`{subject}` is no subject: its token {token:?} is empty, or holds a `.`, a `*`, a \
             `>`, whitespace or anything but printable ASCII"
        )));
    }
    if subject.len() > LONGEST_SUBJECT {
        return Err(Error::Invalid(format!(
            "a subject of {} bytes: it is 1 KiB at most",
            subject.len()
        )));
    }
    if described.r#type.len() > LONGEST_TYPE {
        return Err(Error::Invalid(format!(
            "a type of {} bytes: it is 255 at most",
            described.r#type.len()
        )));
    }
    let ce_subject = described.ce_subject.as_deref().map(header_value);
    if let Some(s) = ce_subject.as_ref().filter(|s| s.len() > LONGEST_HEADER) {
        return Err(Error::Invalid(format!(
            "a `ce-subject` of {} bytes as a header: it is 1 KiB at most",
            s.len()
        )));
    }
    let body = envelope::body(event, described.variant.is_some())?;
    let ty = format!("{}.v{}", described.r#type, described.version);
    let mut headers = serde_json::Map::new();
    headers.insert("ce-type".to_owned(), header_value(&ty).into());
    if let Some(ce_subject) = ce_subject {
        headers.insert("ce-subject".to_owned(), ce_subject.into());
    }
    let id = Uuid::now_v7();
    let length = i64::try_from(body.len()).unwrap_or(i64::MAX);
    // One statement: the limit the relay set, and the row, written only
    // within it.
    let (limit, written): (i32, bool) = sqlx::query_as(
        "WITH bound AS ( \
             SELECT coalesce((SELECT max_body FROM magic_events.relay WHERE id = 1), $6) AS max_body), \
         written AS ( \
             INSERT INTO magic_events.outbox (event_id, subject, type, headers, body) \
             SELECT $1, $2, $3, $4, $5 FROM bound WHERE $7 <= bound.max_body \
             RETURNING 1) \
         SELECT max_body, EXISTS (SELECT 1 FROM written) FROM bound",
    )
    .bind(id)
    .bind(&subject)
    .bind(&ty)
    .bind(sqlx::types::Json(&headers))
    .bind(&body)
    .bind(i32::try_from(MAX_BODY).expect("64 KiB"))
    .bind(length)
    .fetch_one(&mut *conn)
    .await?;
    if !written {
        return Err(Error::Invalid(format!(
            "a body of {} bytes ({ty}): the relay takes {limit} at most (`Settings::max_body`)",
            body.len()
        )));
    }
    Ok(id)
}

#[cfg(test)]
#[path = "emit_tests.rs"]
mod tests;
