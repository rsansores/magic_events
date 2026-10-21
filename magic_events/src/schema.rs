//! The crate's tables, in the schema `magic_events`: a list of steps, each as
//! released and never edited, one per [`SCHEMA_VERSION`].

use crate::{Error, SCHEMA_VERSION};
use sqlx::PgConnection;

/// The steps, in order, each as released and never edited: the `n`th (from
/// 1) brings the tables from version `n - 1` to `n`.
const MIGRATIONS: [&str; SCHEMA_VERSION as usize] = [include_str!("../sql/postgres/1.sql")];

/// Installs and upgrades one process at a time ("EVNTINST").
const INSTALL_LOCK: i64 = 0x4556_4e54_494e_5354;

/// The step to `version` of the crate's schema, as one SQL script that also
/// records the version it brings the database to — `None` for a version no
/// build of the crate has had yet. [`Events::install`] runs every step a
/// database lacks, in order; a host with migrations of its own runs each in
/// one of its own: every step up to [`SCHEMA_VERSION`], in order, and each
/// later one when a release brings it (its changelog says). A step is never
/// edited once released.
///
/// [`Events::install`]: crate::Events::install
#[must_use]
pub fn migration(version: i32) -> Option<String> {
    let step = MIGRATIONS.get(usize::try_from(version.checked_sub(1)?).ok()?)?;
    Some(format!(
        "{step}\nINSERT INTO magic_events.version (id, version) VALUES (1, {version}) \
         ON CONFLICT (id) DO UPDATE SET version = excluded.version;\n"
    ))
}

/// Brings the schema to this build's version, in the caller's transaction:
/// one process at a time, every missing step or none.
pub(crate) async fn install(conn: &mut PgConnection) -> Result<(), Error> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(INSTALL_LOCK)
        .execute(&mut *conn)
        .await?;
    let installed = version(conn).await?.unwrap_or(0);
    if installed > SCHEMA_VERSION {
        return Err(Error::Misuse(format!(
            "the schema magic_events is version {installed}, from a later build than this one \
             ({SCHEMA_VERSION})"
        )));
    }
    for step in (installed + 1)..=SCHEMA_VERSION {
        let script = migration(step).expect("a step of this build");
        // Not `raw_sql`: its future is not `Send` for every lifetime a host's
        // spawn asks of it.
        sqlx::Executor::execute(&mut *conn, script.as_str()).await?;
    }
    Ok(())
}

/// Refuses a database whose schema is not at this build's version.
pub(crate) async fn check(conn: &mut PgConnection) -> Result<(), Error> {
    match version(conn).await? {
        Some(v) if v == SCHEMA_VERSION => Ok(()),
        Some(v) if v > SCHEMA_VERSION => Err(Error::Misuse(format!(
            "the schema magic_events is version {v}, from a later build than this one \
             ({SCHEMA_VERSION}): run that build, or a later one"
        ))),
        v => Err(Error::Misuse(format!(
            "the schema magic_events is {}, and this build needs version {SCHEMA_VERSION}: \
             `Events::install` brings it there, or a migration of yours runs \
             `magic_events::migration` for each step",
            v.map_or("not installed".to_owned(), |v| format!("version {v}"))
        ))),
    }
}

/// The version of the crate's schema the database has: `None` when there
/// is none. A schema `magic_events` that is not the crate's is refused.
async fn version(conn: &mut PgConnection) -> Result<Option<i32>, Error> {
    let (schema, table): (bool, bool) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'magic_events'), \
                to_regclass('magic_events.version') IS NOT NULL",
    )
    .fetch_one(&mut *conn)
    .await?;
    if !schema {
        return Ok(None);
    }
    let version = if table {
        sqlx::query_scalar::<_, i32>("SELECT version FROM magic_events.version WHERE id = 1")
            .fetch_optional(&mut *conn)
            .await?
    } else {
        None
    };
    match version {
        Some(version) => Ok(Some(version)),
        None => Err(Error::Misuse(
            "the database has a schema named `magic_events` that is not this crate's: the crate \
             keeps its tables there, so that one needs another name"
                .to_owned(),
        )),
    }
}

#[cfg(test)]
#[path = "schema_tests.rs"]
mod tests;
