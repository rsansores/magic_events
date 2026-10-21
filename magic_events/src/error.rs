/// What the crate can fail with. [`Error::is_transient`] says whether the
/// same call may succeed as it is once the infrastructure is back.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// The database failed, or refused a statement.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    /// A setting, a schema or a deployment the crate cannot work with: a
    /// programming or an operations error, never the data's.
    #[error("{0}")]
    Misuse(String),
    /// An event [`emit`](crate::emit) refuses as it is: one that could never
    /// be published. Nothing was written; the same event fails the same way.
    #[error("{0}")]
    Invalid(String),
    /// The broker failed, or refused what the relay asked of it.
    #[error("{reason}")]
    Broker {
        /// Why, as the broker says it.
        reason: String,
        /// Whether the same call may succeed once the broker is back.
        transient: bool,
    },
}

impl Error {
    /// Whether the infrastructure failed rather than the code: the same call
    /// may succeed as it is once the database or the broker is back.
    ///
    /// The transient side is the listed one, because it is the short, closed
    /// set: an error nobody listed is permanent, and shows.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            Error::Database(e) => is_transient_sqlx(e),
            Error::Misuse(_) | Error::Invalid(_) => false,
            Error::Broker { transient, .. } => *transient,
        }
    }
}

fn is_transient_sqlx(e: &sqlx::Error) -> bool {
    match e {
        sqlx::Error::Io(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => true,
        sqlx::Error::Database(db) => db.code().as_deref().is_some_and(is_transient_code),
        _ => false,
    }
}

/// PostgreSQL: serialization failure, deadlock, a statement whose completion
/// is unknown, a lock or statement timeout, shutdown, the server out of disk,
/// memory or connections, a system or I/O error, and the connection
/// exceptions — each the machine's, not the rows'.
fn is_transient_code(code: &str) -> bool {
    matches!(
        code,
        "40001"
            | "40P01"
            | "40003"
            | "55P03"
            | "57014"
            | "57P01"
            | "57P02"
            | "57P03"
            | "53100"
            | "53200"
            | "53300"
            | "53400"
            | "58000"
            | "58030"
            | "08000"
            | "08001"
            | "08003"
            | "08004"
            | "08006"
            | "08007"
            | "08P01"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_infrastructure_is_worth_retrying_as_it_is() {
        assert!(Error::Database(sqlx::Error::PoolTimedOut).is_transient());
        assert!(!Error::Misuse("a stream of another subject".into()).is_transient());
        assert!(!Error::Database(sqlx::Error::RowNotFound).is_transient());
        for code in ["58030", "40001", "08P01"] {
            assert!(is_transient_code(code), "{code}");
        }
        assert!(!is_transient_code("23505"), "a duplicate is the rows'");
    }
}
