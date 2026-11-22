//! The walk: writers commit and roll back at random, each transaction's
//! events going to two streams, while relays start, stop, crash and lose
//! their connection under them, and one of the streams refuses everything
//! for a while; afterwards each stream holds exactly what committed to it —
//! nothing lost, nothing twice, each transaction's events in the order it
//! emitted them — and the other stream went on publishing while the one
//! refused, and while one of its messages waited for an acknowledgement
//! that did not come.
//!
//! Each walk takes a seed, which decides what every writer and the killer
//! do (the timing is the machine's). The walks take the seeds in [`SEEDS`];
//! `MAGIC_EVENTS_WALK_SEEDS` gives them others — seeds, comma-separated, or
//! `random` — and each walk prints the seed it took. A seed worth keeping
//! joins `SEEDS` with its fix.

use crate::tests::{count, until, Broker, Faulty, Seen, Shipped};
use crate::Events;
use async_nats::jetstream::stream::DiscardPolicy;
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

/// Each committed transaction's events, in emit order: each one's stream
/// key and id.
type Committed = Arc<Mutex<Vec<Vec<(&'static str, Uuid)>>>>;

/// One writer: `TRANSACTIONS` transactions of one to five events each, to
/// the main stream or to `live`, three in four committed.
async fn write(pool: PgPool, seed: u64, writer: u64, committed: Committed) {
    let mut walk = Walk(seed ^ (writer + 1).wrapping_mul(0x9E37_79B9));
    for tx_n in 0..TRANSACTIONS {
        let mut tx = pool.begin().await.unwrap();
        let mut ids = Vec::new();
        for k in 0..=walk.below(5) {
            let (store, n) = (format!("w{writer}"), tx_n * 10 + k);
            let id = if walk.below(2) == 0 {
                ("", crate::emit(&mut tx, &Shipped { store, n }).await)
            } else {
                ("live", crate::emit(&mut tx, &Seen { store, n }).await)
            };
            ids.push((id.0, id.1.unwrap()));
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
async fn kill(
    pool: PgPool,
    broker: &Broker,
    faulty: &Faulty,
    seed: u64,
    writing: Arc<Mutex<bool>>,
) -> u64 {
    let mut walk = Walk(seed);
    let mut ended = 0;
    while *writing.lock().unwrap() {
        ended += 1;
        let settings = broker
            .settings
            .clone()
            .in_flight([1, 4, 64][walk.below(3) as usize])
            .batch([1, 7, 256][walk.below(3) as usize]);
        let events = Events::start_with(
            pool.clone(),
            broker.jetstream.clone(),
            Arc::new(faulty.clone()),
            settings,
        )
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

/// Has the stream `live` refuse every message — full, and discarding what
/// is new — until the main stream has published while the relay counts
/// `live`'s failures, then take messages again. A deleted stream would be
/// made again by the next relay that starts; a full one is left as it is.
async fn refuse_for_a_while(pool: &PgPool, broker: &Broker) {
    let name = &broker.stream("live").name;
    let set = |max_messages: i64, discard: DiscardPolicy| async move {
        let mut stream = broker.jetstream.get_stream(name).await.unwrap();
        let mut config = stream.info().await.unwrap().config.clone();
        config.max_messages = max_messages;
        config.discard = discard;
        broker.jetstream.update_stream(&config).await.unwrap();
    };
    set(1, DiscardPolicy::New).await;
    let before = broker.messages().await.len();
    until("the main stream publishes while `live` refuses", || async {
        broker.messages().await.len() > before
            && count(
                pool,
                "SELECT failures::bigint FROM magic_events.relay WHERE stream = 'live'",
            )
            .await
                > 0
    })
    .await;
    set(-1, DiscardPolicy::Old).await;
}

/// Has a message of `live` wait for an acknowledgement that does not come —
/// as from a stream whose leader does not answer — and requires the main
/// stream to publish meanwhile, well within the wait: passes taken one after
/// the other would hold it until `live`'s gave up.
async fn stall_for_a_while(pool: &PgPool, broker: &Broker, faulty: &Faulty, committed: &Committed) {
    *faulty.stalling.lock().unwrap() = Some(broker.stream("live").subject_prefix.clone());
    let sent = faulty.sent.lock().unwrap().len();
    let seen = Seen {
        store: "stall".to_owned(),
        n: 0,
    };
    let shipped = Shipped {
        store: "stall".to_owned(),
        n: 0,
    };
    let mut tx = pool.begin().await.unwrap();
    let id = crate::emit(&mut tx, &seen).await.unwrap();
    tx.commit().await.unwrap();
    committed.lock().unwrap().push(vec![("live", id)]);
    until("`live`'s message sent, and waiting", || async {
        faulty.sent.lock().unwrap().len() > sent
    })
    .await;
    let before = broker.messages().await.len();
    let started = std::time::Instant::now();
    let mut tx = pool.begin().await.unwrap();
    let id = crate::emit(&mut tx, &shipped).await.unwrap();
    tx.commit().await.unwrap();
    committed.lock().unwrap().push(vec![("", id)]);
    until("the main stream publishes", || async {
        broker.messages().await.len() > before
    })
    .await;
    assert!(
        started.elapsed() < broker.settings.send_timeout / 2,
        "the main stream waited for `live`: {:?}",
        started.elapsed()
    );
    *faulty.stalling.lock().unwrap() = None;
}

async fn walk(pool: &PgPool, seed: u64) {
    eprintln!("walk: seed {seed} (MAGIC_EVENTS_WALK_SEEDS={seed} takes it again)");
    let mut broker = Broker::live().await;
    // What `live`'s stalled message waits, at most.
    broker.settings = broker.settings.send_timeout(Duration::from_secs(1));
    // Both streams declared, as a host's first start does, before anything
    // emits to them.
    Events::start(
        pool.clone(),
        broker.jetstream.clone(),
        broker.settings.clone(),
    )
    .await
    .unwrap()
    .stop()
    .await;
    let committed: Committed = Arc::default();
    let writing = Arc::new(Mutex::new(true));
    let writers = (0..WRITERS)
        .map(|writer| tokio::spawn(write(pool.clone(), seed, writer, committed.clone())));
    let writers = futures_join(writers.collect());
    let faulty = Faulty::new(broker.jetstream.clone());
    let killer = kill(pool.clone(), &broker, &faulty, seed, writing.clone());
    // The relays keep starting and ending until the writers are done and
    // `live` took messages again.
    let ((), ended) = tokio::join!(
        async {
            tokio::join!(writers, refuse_for_a_while(pool, &broker));
            *writing.lock().unwrap() = false;
        },
        killer
    );
    // A relay that runs to the end publishes what is left.
    let last = Events::start_with(
        pool.clone(),
        broker.jetstream.clone(),
        Arc::new(faulty.clone()),
        broker.settings.clone(),
    )
    .await
    .unwrap();
    let drained = || async { count(pool, "SELECT count(*) FROM magic_events.outbox").await == 0 };
    until("the outbox drained", drained).await;
    stall_for_a_while(pool, &broker, &faulty, &committed).await;
    until("the outbox drained", drained).await;
    last.stop().await;

    let committed = committed.lock().unwrap().clone();
    for key in ["", "live"] {
        let held: Vec<String> = broker
            .messages_of(key)
            .await
            .into_iter()
            .map(|m| m.id)
            .collect();
        let wanted: BTreeSet<String> = committed
            .iter()
            .flatten()
            .filter(|(k, _)| *k == key)
            .map(|(_, id)| id.to_string())
            .collect();
        let unique: BTreeSet<String> = held.iter().cloned().collect();
        assert_eq!(
            unique.len(),
            held.len(),
            "seed {seed}: a message held twice on `{key}`"
        );
        assert_eq!(
            unique, wanted,
            "seed {seed}: the stream `{key}` is not what committed to it"
        );
        let at: HashMap<&str, usize> = held
            .iter()
            .enumerate()
            .map(|(i, id)| (id.as_str(), i))
            .collect();
        for ids in &committed {
            let places: Vec<usize> = ids
                .iter()
                .filter(|(k, _)| *k == key)
                .map(|(_, id)| at[id.to_string().as_str()])
                .collect();
            assert!(
                places.windows(2).all(|w| w[0] < w[1]),
                "seed {seed}: a transaction's events out of emit order on `{key}`: {places:?}"
            );
        }
        assert!(
            !wanted.is_empty(),
            "seed {seed}: nothing committed to `{key}`"
        );
    }
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
