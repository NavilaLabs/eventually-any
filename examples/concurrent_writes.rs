//! # Concurrent Writes — Optimistic Concurrency Control
//!
//! Event sourcing systems must handle concurrent writes to the same stream
//! safely.  `eventually-any` uses **Optimistic Concurrency Control (OCC)**:
//! every write declares the version it expects the stream to be at.  If
//! another writer has already advanced the version, the write fails with
//! `AppendError::Conflict` or `SaveError::Conflict`, and the caller can
//! retry after reloading the latest state.
//!
//! This example demonstrates:
//!
//! 1. **Event-store level** — two writers claim the same version; exactly one
//!    wins, the other gets a `Conflict` error.
//! 2. **Aggregate repository level** — two savers hold stale roots; exactly
//!    one commits, the other gets `SaveError::Conflict`.
//! 3. **Retry loop** — a realistic pattern: reload, re-apply the command,
//!    and retry the save until it succeeds.
//!
//! ## How the conflict is triggered
//!
//! OCC does not require two goroutines to run in parallel.  What matters is
//! that both writers read the *same* version before either writes.  Here we
//! reproduce that by writing with Writer A first (advancing the version), then
//! attempting Writer B's write at the now-stale version — the conflict fires
//! deterministically on every database, including SQLite.
//!
//! Run with:
//! ```sh
//! cargo run --example concurrent_writes --features "sqlite,migrations"
//!
//! DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres" \
//!   cargo run --example concurrent_writes --features "postgres,migrations"
//! ```

use std::fmt;

use eventually::aggregate::{
    self, Aggregate,
    repository::{Getter, Saver},
};
use eventually::event::Envelope;
use eventually::event::store::{AppendError, Appender};
use eventually::message::Message;
use eventually::version;
use eventually_any::aggregate::Repository;
use eventually_any::event::Store;
use serde::{Deserialize, Serialize};
use sqlx::any::install_default_drivers;

// ── Domain types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CounterId(pub u32);

impl fmt::Display for CounterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "counter:{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CounterEvent {
    // The id is embedded so apply() stores the real CounterId in state,
    // ensuring aggregate_id() is correct after a save/load round-trip.
    Created { id: CounterId },
    Incremented { by: u64 },
}

impl Message for CounterEvent {
    fn name(&self) -> &'static str {
        match self {
            CounterEvent::Created { .. } => "CounterCreated",
            CounterEvent::Incremented { .. } => "CounterIncremented",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counter {
    id: CounterId,
    value: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum CounterError {
    #[error("counter does not exist")]
    NotFound,
    #[error("already created")]
    AlreadyCreated,
}

impl Aggregate for Counter {
    type Id = CounterId;
    type Event = CounterEvent;
    type Error = CounterError;

    fn type_name() -> &'static str {
        "Counter"
    }
    fn aggregate_id(&self) -> &Self::Id {
        &self.id
    }

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, Self::Error> {
        match (state, event) {
            (None, CounterEvent::Created { id }) => Ok(Counter { id, value: 0 }),
            (Some(_), CounterEvent::Created { .. }) => Err(CounterError::AlreadyCreated),
            (None, _) => Err(CounterError::NotFound),
            (Some(mut c), CounterEvent::Incremented { by }) => {
                c.value += by;
                Ok(c)
            }
        }
    }
}

#[eventually_macros::aggregate_root(Counter)]
#[derive(Debug, Clone, PartialEq)]
pub struct CounterRoot;

impl CounterRoot {
    pub fn create(id: CounterId) -> Self {
        aggregate::Root::<Counter>::record_new(CounterEvent::Created { id }.into())
            .unwrap()
            .into()
    }

    pub fn increment(&mut self, by: u64) {
        self.record_that(CounterEvent::Incremented { by }.into())
            .unwrap();
    }
}

// ── Main ──────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    install_default_drivers();

    let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite::memory:".to_string());
    println!("🔌  Connecting to: {db_url}\n");

    // max_connections(1): SQLite only supports one writer at a time anyway,
    // and a single connection avoids shared-cache isolation issues.
    // For Postgres, the pool can be larger; the OCC logic is the same.
    let pool = sqlx::any::AnyPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await?;

    // ── Part 1: event-store level OCC ────────────────────────────────────
    println!("══════════════════════════════════════════");
    println!("  Part 1: Event-store level OCC");
    println!("══════════════════════════════════════════\n");

    let store: Store<String, CounterEvent, _> = Store::new(
        pool.clone(),
        eventually::serde::Json::<CounterEvent>::default(),
    )
    .await?;

    let stream = "occ-stream-1".to_string();

    // Writer A goes first and succeeds.
    let v_a = store
        .append(
            stream.clone(),
            version::Check::MustBe(0),
            vec![Envelope::from(CounterEvent::Incremented { by: 1 })],
        )
        .await?;
    println!("✅  Writer A committed — stream now at version {v_a}");

    // Writer B tries to append at version 0 — but the stream is already at 1.
    let result_b = store
        .append(
            stream.clone(),
            version::Check::MustBe(0), // stale: stream is already at v1
            vec![Envelope::from(CounterEvent::Incremented { by: 2 })],
        )
        .await;

    match result_b {
        Err(AppendError::Conflict(e)) => {
            println!(
                "❌  Writer B lost  — conflict: expected v{}, found v{}",
                e.expected, e.actual
            );
            assert_eq!(e.expected, 0);
            assert_eq!(e.actual, v_a);
        }
        other => panic!("expected a Conflict, got: {other:?}"),
    }
    println!();

    // ── Part 2: aggregate repository level OCC ────────────────────────────
    println!("══════════════════════════════════════════");
    println!("  Part 2: Aggregate repository level OCC");
    println!("══════════════════════════════════════════\n");

    let repo: Repository<Counter, _, _> = Repository::new(
        pool.clone(),
        eventually::serde::Json::<Counter>::default(),
        eventually::serde::Json::<CounterEvent>::default(),
    )
    .await?;

    let id = CounterId(42);

    // Both "savers" load the aggregate at version 0 before either writes.
    let mut root_a = CounterRoot::create(id);
    root_a.increment(10);
    let mut root_b = root_a.clone(); // same uncommitted state, same expected version

    // Saver A commits first.
    repo.save(&mut root_a).await?;
    println!(
        "✅  Saver A committed — root at version {}",
        root_a.version()
    );

    // Saver B now tries to save at the same (now stale) version.
    match repo.save(&mut root_b).await {
        Err(aggregate::repository::SaveError::Conflict(e)) => {
            println!(
                "❌  Saver B conflicted — expected v{}, actual v{}",
                e.expected, e.actual
            );
        }
        other => panic!("expected a Conflict, got: {other:?}"),
    }
    println!();

    // ── Part 3: optimistic retry loop ────────────────────────────────────
    println!("══════════════════════════════════════════");
    println!("  Part 3: Optimistic retry loop");
    println!("══════════════════════════════════════════\n");

    let retry_id = CounterId(99);
    let mut initial = CounterRoot::create(retry_id);
    repo.save(&mut initial).await?;

    // Simulate another process incrementing the counter while we weren't looking.
    {
        let mut fresh: CounterRoot = repo.get(&retry_id).await.map(CounterRoot::from)?;
        fresh.increment(100);
        repo.save(&mut fresh).await?;
        println!(
            "⚡  Background increment by 100 — counter at version {}",
            fresh.version()
        );
    }

    // Our process retries until it wins.
    let max_retries = 5;
    let mut attempts = 0;
    loop {
        attempts += 1;
        let mut root: CounterRoot = repo.get(&retry_id).await.map(CounterRoot::from)?;
        println!(
            "    attempt {attempts}: value = {}, version = {}",
            root.value,
            root.version()
        );
        root.increment(5);
        match repo.save(&mut root).await {
            Ok(()) => {
                println!(
                    "✅  Saved after {attempts} attempt(s) — value = {}, version = {}",
                    root.value,
                    root.version()
                );
                assert_eq!(root.value, 105, "100 (background) + 5 (ours) = 105");
                break;
            }
            Err(aggregate::repository::SaveError::Conflict(_)) if attempts < max_retries => {
                println!("    Conflict, retrying…");
            }
            Err(e) => return Err(e.into()),
        }
    }

    println!("\n🎉  All OCC scenarios handled correctly.");
    Ok(())
}
