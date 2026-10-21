//! The shop: it places orders and ships them, each write with its event in
//! the same transaction, and runs the relay. Its events page — `web/`, the
//! dashboard over the shop's answers — is at http://127.0.0.1:3000, and the
//! answers themselves at `/events/status` and `/events/recent`.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Html;
use axum::routing::get;
use axum::{Json, Router};
use magic_events::status::{Filter, Recent, Status};
use magic_events::{emit, Events, Settings};
use shop::Shop;
use std::sync::Arc;
use std::time::Duration;
use tower_http::services::ServeDir;

/// The events page, built by `pnpm build` in `web/`.
const PAGE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/web/dist");

#[tokio::main]
async fn main() {
    shop::logs();
    let pool = shop::database().await;
    sqlx::raw_sql(include_str!("../../schema.sql"))
        .execute(&pool)
        .await
        .expect("the shop's schema");
    Events::install(&pool).await.expect("the outbox");
    let events = Events::start(
        pool.clone(),
        shop::jetstream().await,
        Settings::new(shop::STREAM, "//shop.example.com"),
    )
    .await
    .expect("the relay");
    let events = Arc::new(events);

    // The status, as the dashboard reads it. A real shop puts its own
    // authorization in front, and the filter the caller may see.
    let app = Router::new()
        .route("/events/status", get(status))
        .route("/events/recent", get(recent))
        .with_state(events.clone())
        .fallback_service(ServeDir::new(PAGE).fallback(get(unbuilt)));
    // `SHOP_LISTEN=0.0.0.0:3000` serves it beyond this machine: it has no
    // login.
    let listen = std::env::var("SHOP_LISTEN").unwrap_or_else(|_| "127.0.0.1:3000".to_owned());
    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .unwrap_or_else(|e| panic!("{listen}: {e}"));
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });

    let stores = ["north", "south"];
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    // One signal for the whole run: a Ctrl-C during a step stops the next.
    let interrupted = tokio::signal::ctrl_c();
    tokio::pin!(interrupted);
    for n in 0_u64.. {
        tokio::select! {
            _ = tick.tick() => {}
            _ = &mut interrupted => break,
        }
        let store = stores[(n % 2) as usize];
        if let Err(e) = step(&pool, store, n).await {
            eprintln!("a step failed: {e}");
        }
    }
    // The server first, which holds the relay too; then the relay, which
    // lets its lock go for whoever runs it next.
    let _ = stop.send(());
    let _ = server.await;
    match Arc::try_unwrap(events) {
        Ok(events) => events.stop().await,
        Err(_) => eprintln!("the relay is still held elsewhere: it stops as the process ends"),
    }
}

/// An answer of the relay's, or why there is none.
type Answer<T> = Result<Json<T>, (StatusCode, String)>;

async fn status(State(events): State<Arc<Events>>) -> Answer<Status> {
    events
        .status(&Filter::all())
        .await
        .map(Json)
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e.to_string()))
}

async fn recent(State(events): State<Arc<Events>>) -> Answer<Recent> {
    events
        .recent(&Filter::all(), 20)
        .await
        .map(Json)
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e.to_string()))
}

async fn unbuilt() -> Html<&'static str> {
    Html(
        "<p>The events page is not built: run <code>pnpm install &amp;&amp; pnpm build</code> \
         in <code>examples/shop/web</code>. The answers are at \
         <a href=\"/events/status\">/events/status</a>.</p>",
    )
}

/// One step of the shop: an order placed, and every third step the store's
/// orders shipped — one event for all of them. One step in five changes its
/// mind and rolls back: its event never leaves.
async fn step(pool: &sqlx::PgPool, store: &str, n: u64) -> Result<(), magic_events::Error> {
    let mut tx = pool.begin().await?;
    let id: i64 =
        sqlx::query_scalar("INSERT INTO shop.orders (store, total) VALUES ($1, $2) RETURNING id")
            .bind(store)
            .bind(i64::try_from(n % 7 + 1).unwrap_or(1) * 1000)
            .fetch_one(&mut *tx)
            .await?;
    emit(
        &mut tx,
        &Shop::OrderPlaced {
            store: store.to_owned(),
            ids: vec![id],
        },
    )
    .await?;
    if n % 3 == 2 {
        let ids: Vec<i64> = sqlx::query_scalar(
            "UPDATE shop.orders SET shipped_at = now() \
              WHERE store = $1 AND shipped_at IS NULL RETURNING id",
        )
        .bind(store)
        .fetch_all(&mut *tx)
        .await?;
        emit(
            &mut tx,
            &Shop::OrderShipped {
                store: store.to_owned(),
                ids,
            },
        )
        .await?;
    }
    if n % 5 == 4 {
        tx.rollback().await?;
        println!("step {n}: rolled back — order {id} and its event never were");
    } else {
        tx.commit().await?;
        println!("step {n}: order {id} placed in {store}");
    }
    Ok(())
}
