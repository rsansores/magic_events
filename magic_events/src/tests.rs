//! What every test of the crate stands on: a database of its own, made by
//! `#[sqlx::test]` from `DATABASE_URL`, and the JetStream at `NATS_URL`,
//! shared — each test with a stream of its own.

use sqlx::PgPool;

/// The JetStream every test publishes to: `NATS_URL`, or a local server.
pub(crate) async fn jetstream() -> async_nats::jetstream::Context {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_owned());
    let client = async_nats::connect(&url)
        .await
        .unwrap_or_else(|e| panic!("no NATS at {url} (NATS_URL): {e}"));
    async_nats::jetstream::new(client)
}

/// The database and the broker answer, and the broker runs JetStream.
#[sqlx::test(migrations = false)]
async fn the_database_and_the_broker_answer(pool: PgPool) {
    let one: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(one, 1);
    let account = jetstream().await.query_account().await;
    assert!(account.is_ok(), "JetStream is not enabled: {account:?}");
}
