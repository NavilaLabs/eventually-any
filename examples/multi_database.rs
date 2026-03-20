//! # Multi-Database — Runtime Backend Selection
//!
//! `eventually-any` is designed around `sqlx::AnyPool`, which means the same
//! application binary can target PostgreSQL, MySQL, or SQLite by simply
//! changing the connection string — zero code changes needed.
//!
//! This example proves that by running the **exact same domain logic** against
//! SQLite in-memory (always available) and optionally PostgreSQL and MySQL.
//!
//! Run with only SQLite:
//! ```sh
//! cargo run --example multi_database --features "sqlite,migrations"
//! ```
//!
//! Run against all backends (requires running instances):
//! ```sh
//! POSTGRES_URL="postgres://postgres:postgres@localhost:5432/postgres" \
//! MYSQL_URL="mysql://root@localhost:3306/test_db" \
//! cargo run --example multi_database --features "sqlite,postgres,mysql,migrations"
//! ```

use std::fmt;

use eventually::aggregate::{
    self, Aggregate,
    repository::{Getter, Saver},
};
use eventually::event::store::{AppendError, Appender, Streamer};
use eventually::event::{Envelope, VersionSelect};
use eventually::message::Message;
use eventually::version;
use eventually_any::event::Store;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use sqlx::AnyPool;
use sqlx::any::install_default_drivers;

// ── Aggregate ID ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoteId(pub u64);

impl fmt::Display for NoteId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "note:{}", self.0)
    }
}

// ── Domain types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NoteEvent {
    // NoteId embedded so apply() stores the real id in state.
    Created {
        id: NoteId,
        title: String,
        body: String,
    },
    Edited {
        new_body: String,
    },
    Archived,
}

impl Message for NoteEvent {
    fn name(&self) -> &'static str {
        match self {
            NoteEvent::Created { .. } => "NoteCreated",
            NoteEvent::Edited { .. } => "NoteEdited",
            NoteEvent::Archived => "NoteArchived",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Note {
    id: NoteId,
    title: String,
    body: String,
    archived: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum NoteError {
    #[error("not created")]
    NotCreated,
    #[error("already created")]
    AlreadyCreated,
    #[error("already archived")]
    AlreadyArchived,
}

impl Aggregate for Note {
    type Id = NoteId;
    type Event = NoteEvent;
    type Error = NoteError;

    fn type_name() -> &'static str {
        "Note"
    }
    fn aggregate_id(&self) -> &Self::Id {
        &self.id
    }

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, Self::Error> {
        match (state, event) {
            (None, NoteEvent::Created { id, title, body }) => Ok(Note {
                id,
                title,
                body,
                archived: false,
            }),
            (Some(_), NoteEvent::Created { .. }) => Err(NoteError::AlreadyCreated),
            (None, _) => Err(NoteError::NotCreated),
            (Some(mut n), NoteEvent::Edited { new_body }) => {
                n.body = new_body;
                Ok(n)
            }
            (Some(mut n), NoteEvent::Archived) => {
                if n.archived {
                    return Err(NoteError::AlreadyArchived);
                }
                n.archived = true;
                Ok(n)
            }
        }
    }
}

#[eventually_macros::aggregate_root(Note)]
#[derive(Debug, Clone, PartialEq)]
pub struct NoteRoot;

impl NoteRoot {
    fn create(id: NoteId, title: String, body: String) -> Result<Self, NoteError> {
        Ok(
            aggregate::Root::<Note>::record_new(NoteEvent::Created { id, title, body }.into())?
                .into(),
        )
    }

    fn edit(&mut self, body: String) -> Result<(), NoteError> {
        self.record_that(NoteEvent::Edited { new_body: body }.into())
    }
}

// ── Backend-agnostic test suite ───────────────────────────────────────────

/// Run the same operations on any `AnyPool`.
/// Returns the event count in the raw stream (asserted identical across backends).
async fn run_suite(pool: AnyPool, backend_name: &str) -> anyhow::Result<usize> {
    println!("  ┌── {backend_name}");

    // ── Raw event store ───────────────────────────────────────────────────
    let store: Store<String, NoteEvent, _> = Store::new(
        pool.clone(),
        eventually::serde::Json::<NoteEvent>::default(),
    )
    .await?;

    let stream = format!(
        "note-backend-test-{}",
        backend_name
            .to_lowercase()
            .replace(' ', "-")
            .replace('(', "")
            .replace(')', "")
    );

    let v1 = store
        .append(
            stream.clone(),
            version::Check::MustBe(0),
            vec![Envelope::from(NoteEvent::Created {
                id: NoteId(0), // stream-only; no aggregate round-trip needed
                title: "Meeting notes".into(),
                body: "Draft".into(),
            })],
        )
        .await?;

    let v2 = store
        .append(
            stream.clone(),
            version::Check::MustBe(v1),
            vec![
                Envelope::from(NoteEvent::Edited {
                    new_body: "Final draft".into(),
                }),
                Envelope::from(NoteEvent::Archived),
            ],
        )
        .await?;

    println!("  │  appended 3 events → stream version {v2}");

    let all: Vec<_> = store
        .stream(&stream, VersionSelect::All)
        .try_collect()
        .await?;
    let count = all.len();
    println!("  │  read back {count} events");
    for p in &all {
        println!("  │    v{}  {}", p.version, p.event.message.name());
    }

    // ── Aggregate repository ──────────────────────────────────────────────
    let repo: eventually_any::aggregate::Repository<Note, _, _> =
        eventually_any::aggregate::Repository::new(
            pool.clone(),
            eventually::serde::Json::<Note>::default(),
            eventually::serde::Json::<NoteEvent>::default(),
        )
        .await?;

    let note_id = NoteId(1);
    let mut root = NoteRoot::create(note_id, "DB Test".into(), "Hello".into())?;
    root.edit("Hello, world!".into())?;
    repo.save(&mut root).await?;

    let loaded: NoteRoot = repo.get(&note_id).await.map(NoteRoot::from)?;
    assert_eq!(loaded.body, "Hello, world!");
    assert_eq!(loaded.version(), 2);
    println!(
        "  │  aggregate saved & reloaded — v{}, body = {:?}",
        loaded.version(),
        loaded.body
    );

    // ── OCC conflict (sequential — works on all backends including SQLite) ──
    // Both savers load the same version, then saver A writes first.
    // Saver B then attempts to write at the now-stale version → Conflict.
    let mut stale_a: NoteRoot = repo.get(&note_id).await.map(NoteRoot::from)?;
    let mut stale_b = stale_a.clone(); // same expected version
    stale_a.edit("Edit A".into())?;
    stale_b.edit("Edit B".into())?;

    repo.save(&mut stale_a).await?; // wins
    let result_b = repo.save(&mut stale_b).await; // must conflict

    match result_b {
        Err(aggregate::repository::SaveError::Conflict(_)) => {
            println!("  │  OCC conflict detected correctly ✅");
        }
        other => panic!("expected Conflict on {backend_name}, got: {other:?}"),
    }

    println!("  └────────────────────────────────────────────\n");
    Ok(count)
}

// ── Main ──────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    install_default_drivers();

    println!("🗄️   Multi-database backend demonstration\n");

    let mut results: Vec<(&str, usize)> = Vec::new();

    // ── SQLite (always runs) ───────────────────────────────────────────────
    // max_connections(1): SQLite serializes all writes; a single connection
    // also ensures migrations are visible to every subsequent query.
    {
        let pool = sqlx::any::AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        let count = run_suite(pool, "SQLite (in-memory)").await?;
        results.push(("SQLite", count));
    }

    // ── PostgreSQL (optional) ─────────────────────────────────────────────
    #[cfg(feature = "postgres")]
    if let Ok(url) = std::env::var("POSTGRES_URL") {
        println!("🐘  POSTGRES_URL detected — running against PostgreSQL…\n");
        let pool = sqlx::AnyPool::connect(&url).await?;
        let count = run_suite(pool, "PostgreSQL").await?;
        results.push(("PostgreSQL", count));
    } else {
        println!("ℹ️   Set POSTGRES_URL to also test PostgreSQL.\n");
    }

    // ── MySQL (optional) ──────────────────────────────────────────────────
    #[cfg(feature = "mysql")]
    if let Ok(url) = std::env::var("MYSQL_URL") {
        println!("🐬  MYSQL_URL detected — running against MySQL…\n");
        let pool = sqlx::AnyPool::connect(&url).await?;
        let count = run_suite(pool, "MySQL").await?;
        results.push(("MySQL", count));
    } else {
        println!("ℹ️   Set MYSQL_URL to also test MySQL.\n");
    }

    // ── Compare across backends ───────────────────────────────────────────
    println!("📊  Results summary:");
    for (name, count) in &results {
        println!("    {name:<20}  event count = {count}");
    }

    let first = results[0].1;
    for (name, count) in &results {
        assert_eq!(
            *count, first,
            "{name} produced {count} events but {first} were expected"
        );
    }

    println!(
        "\n✅  All {} backend(s) produced identical results.",
        results.len()
    );
    if results.len() == 1 {
        println!("   (Only SQLite ran — provide POSTGRES_URL / MYSQL_URL for more backends.)");
    }

    Ok(())
}
