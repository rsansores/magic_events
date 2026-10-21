//! The walk: writers commit and roll back at random while relays start,
//! stop, crash and lose their connection under them; afterwards the stream
//! holds exactly what committed — nothing lost, nothing twice, each
//! transaction's events in the order it emitted them.
//!
//! Each walk takes a seed, which decides what every writer and the killer
//! do (the timing is the machine's). The walks take the seeds in [`SEEDS`];
//! `MAGIC_EVENTS_WALK_SEEDS` gives them others — seeds, comma-separated, or
//! `random` — and each walk prints the seed it took. A seed worth keeping
//! joins `SEEDS` with its fix.

use crate::tests::{count, until, Broker, Shipped};
use crate::Events;
use sqlx::PgPool;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

/// The seeds every run of the suite walks.
const SEEDS: [u64; 4] = [1, 7, 42, 2027];

const WRITERS: u64 = 4;
const TRANSACTIONS: u64 = 50;

/// A deterministic walk: the same seed makes the same choices.
struct Walk(u64);

impl Walk {
    fn next(&mut self) -> u64 {
        // Xorshift stays at zero from zero: that seed walks from another.
        if self.0 == 0 {
            self.0 = 0x9E37_79B9_7F4A_7C15;
        }
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn seeds() -> Vec<u64> {
    match std::env::var("MAGIC_EVENTS_WALK_SEEDS") {
        Ok(s) if s == "random" => (0..4).map(|_| Uuid::now_v7().as_u128() as u64).collect(),
        Ok(s) => s
            .split(',')
            .map(|seed| seed.trim().parse().expect("a seed is a whole number"))
            .collect(),
        Err(_) => SEEDS.to_vec(),
    }
}

/// Each committed transaction's events, in emit order.
type Committed = Arc<Mutex<Vec<Vec<Uuid>>>>;

/// One writer: `TRANSACTIONS` transactions of one to five events each,
/// three in four committed.
async fn write(pool: PgPool, seed: u64, writer: u64, committed: Committed) {
    let mut walk = Walk(seed ^ (writer + 1).wrapping_mul(0x9E37_79B9));
    for tx_n in 0..TRANSACTIONS {
        let mut tx = pool.begin().await.unwrap();
        let mut ids = Vec::new();
        for k in 0..=walk.below(5) {
            let event = Shipped {
                store: format!("w{writer}"),
                n: tx_n * 10 + k,
            };
            ids.push(crate::emit(&mut tx, &event).await.unwrap());
        }
        tokio::time::sleep(Duration::from_millis(walk.below(4))).await;
        if walk.below(4) == 0 {
            tx.rollback().await.unwrap();
        } else {
            tx.commit().await.unwrap();
            committed.lock().unwrap().push(ids);
        }
    }
}

/// The killer: relays started with settings of the seed's choosing, and
/// ended a moment later — stopped, crashed, or their lock's connection
/// killed — until `writing` says the writers are done.
async fn kill(pool: PgPool, broker: &Broker, seed: u64, writing: Arc<Mutex<bool>>) -> u64 {
    let mut walk = Walk(seed);
    let mut ended = 0;
    while *writing.lock().unwrap() {
        ended += 1;
        let settings = broker
            .settings
            .clone()
            .in_flight([1, 4, 64][walk.below(3) as usize])
            .batch([1, 7, 256][walk.below(3) as usize]);
        let events = Events::start(pool.clone(), broker.jetstream.clone(), settings)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(walk.below(20))).await;
        match walk.below(3) {
            0 => events.stop().await,
            1 => events.abort(),
            _ => {
                sqlx::query(
                    "SELECT pg_terminate_backend(pid) FROM pg_locks \
                      WHERE locktype = 'advisory' AND granted \
                        AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
                )
                .execute(&pool)
                .await
                .unwrap();
                tokio::time::sleep(Duration::from_millis(walk.below(10))).await;
                events.abort();
            }
        }
    }
    ended
}

async fn walk(pool: &PgPool, seed: u64) {
    eprintln!("walk: seed {seed} (MAGIC_EVENTS_WALK_SEEDS={seed} takes it again)");
    let broker = Broker::new().await;
    let committed: Committed = Arc::default();
    let writing = Arc::new(Mutex::new(true));
    let writers = (0..WRITERS)
        .map(|writer| tokio::spawn(write(pool.clone(), seed, writer, committed.clone())));
    let writers = futures_join(writers.collect());
    let killer = kill(pool.clone(), &broker, seed, writing.clone());
    let ((), ended) = tokio::join!(
        async {
            writers.await;
            *writing.lock().unwrap() = false;
        },
        killer
    );
    // A relay that runs to the end publishes what is left.
    let last = Events::start(
        pool.clone(),
        broker.jetstream.clone(),
        broker.settings.clone(),
    )
    .await
    .unwrap();
    until("the outbox drained", || async {
        count(pool, "SELECT count(*) FROM magic_events.outbox").await == 0
    })
    .await;
    last.stop().await;

    let held: Vec<String> = broker.messages().await.into_iter().map(|m| m.id).collect();
    let committed = committed.lock().unwrap().clone();
    let wanted: BTreeSet<String> = committed.iter().flatten().map(Uuid::to_string).collect();
    let unique: BTreeSet<String> = held.iter().cloned().collect();
    assert_eq!(
        unique.len(),
        held.len(),
        "seed {seed}: a message held twice"
    );
    assert_eq!(
        unique, wanted,
        "seed {seed}: the stream is not what committed"
    );
    let at: HashMap<&str, usize> = held
        .iter()
        .enumerate()
        .map(|(i, id)| (id.as_str(), i))
        .collect();
    for ids in &committed {
        let places: Vec<usize> = ids.iter().map(|id| at[id.to_string().as_str()]).collect();
        assert!(
            places.windows(2).all(|w| w[0] < w[1]),
            "seed {seed}: a transaction's events out of emit order: {places:?}"
        );
    }
    assert!(!committed.is_empty());
    eprintln!(
        "walk: seed {seed}: {} transactions committed, {ended} relays ended",
        committed.len()
    );
    broker.done().await;
}

/// Awaits every one of `tasks`, failing with the first that failed.
async fn futures_join(tasks: Vec<tokio::task::JoinHandle<()>>) {
    for task in tasks {
        task.await.unwrap();
    }
}

#[sqlx::test(migrations = false)]
async fn the_stream_holds_exactly_what_committed_whatever_ends_the_relay(pool: PgPool) {
    Events::install(&pool).await.unwrap();
    for seed in seeds() {
        walk(&pool, seed).await;
    }
}
