//! # Minimal — Quick-Start Example
//!
//! Appends two events to an event stream and streams them back.
//! Demonstrates the simplest possible `eventually-any` usage.
//!
//! Run with:
//! ```sh
//! cargo run --example minimal --features "sqlite,migrations"
//! # or with Postgres:
//! DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres" \
//!   cargo run --example minimal --features "postgres,migrations"
//! ```

use eventually::event::store::{Appender, Streamer};
use eventually::event::{Envelope, VersionSelect};
use eventually::message::Message;
use eventually::version;
use eventually_any::event::Store;
use eventually_any::upcasting::{FnUpcaster, UpcasterChain};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::any::install_default_drivers;

// ── 1. Define your domain events ──────────────────────────────────────────
//
// Schema v2: `UserCreated` now carries `first_name` + `last_name` instead of
// the old v1 `name` field.  Old events stored in the database (v1) will be
// transparently migrated at read time by the upcaster below.

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum UserEvent {
    Created {
        first_name: String,
        last_name: String,
    },
    Updated {
        new_first_name: String,
        new_last_name: String,
    },
}

impl Message for UserEvent {
    fn name(&self) -> &'static str {
        match self {
            UserEvent::Created { .. } => "UserCreated",
            UserEvent::Updated { .. } => "UserUpdated",
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 2. Install drivers for Postgres, MySQL, and SQLite.
    install_default_drivers();

    let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite::memory:".to_string());
    println!("🔌 Connecting to database: {db_url}");

    let pool = sqlx::any::AnyPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await?;

    // ── 3. Build the upcaster chain ───────────────────────────────────────
    //
    // V1 → V2: `UserCreated { name }` becomes `UserCreated { first_name, last_name }`.
    //
    // Register one `FnUpcaster` per event type and version transition.
    // The chain automatically applies them in sequence, so V1 → V2 → V3
    // works without extra wiring.
    let chain = UpcasterChain::new()
        .register(FnUpcaster::new(
            "UserCreated", // event type (matches `Message::name()`)
            1,             // from schema_version
            2,             // to schema_version
            |mut payload| {
                // Old payload shape:  { "Created": { "name": "Alice Bob" } }
                // New payload shape:  { "Created": { "first_name": "Alice", "last_name": "Bob" } }
                if let Some(inner) = payload.get_mut("Created") {
                    if let Some(name) = inner
                        .get("name")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned)
                    {
                        let parts: Vec<&str> = name.splitn(2, ' ').collect();
                        inner["first_name"] = json!(parts.first().copied().unwrap_or(""));
                        inner["last_name"] = json!(parts.get(1).copied().unwrap_or(""));
                        inner.as_object_mut().unwrap().remove("name");
                    }
                }
                payload
            },
        ))
        .register(FnUpcaster::new("UserUpdated", 1, 2, |mut payload| {
            // Old: { "Updated": { "new_name": "Bob Alice" } }
            // New: { "Updated": { "new_first_name": "Bob", "new_last_name": "Alice" } }
            if let Some(inner) = payload.get_mut("Updated") {
                if let Some(name) = inner
                    .get("new_name")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
                {
                    let parts: Vec<&str> = name.splitn(2, ' ').collect();
                    inner["new_first_name"] = json!(parts.first().copied().unwrap_or(""));
                    inner["new_last_name"] = json!(parts.get(1).copied().unwrap_or(""));
                    inner.as_object_mut().unwrap().remove("new_name");
                }
            }
            payload
        }));

    // ── 4. Connect to the event store (schema v2, upcaster attached) ──────
    let store = Store::new(pool, eventually::serde::Json::<UserEvent>::default())
        .await?
        .with_schema_version(2) // new events written at v2
        .with_upcaster_chain(chain); // old v1 events upcasted on read

    println!(
        "📦 Event store ready (write schema_version = {})",
        store.schema_version()
    );

    let stream_id = "user-123".to_string();

    // ── 5. Append v2 events ───────────────────────────────────────────────
    println!("📝 Appending events to stream '{stream_id}'…");
    let new_version = store
        .append(
            stream_id.clone(),
            version::Check::MustBe(0),
            vec![
                Envelope::from(UserEvent::Created {
                    first_name: "Alice".to_string(),
                    last_name: "Bob".to_string(),
                }),
                Envelope::from(UserEvent::Updated {
                    new_first_name: "Alice".to_string(),
                    new_last_name: "Smith".to_string(),
                }),
            ],
        )
        .await?;

    println!("✅ Stream is now at version {new_version}");

    // ── 6. Stream the events back (upcasting happens here if needed) ──────
    println!("🔍 Reading events back (upcaster applied if schema_version < 2):");
    let events: Vec<_> = store
        .stream(&stream_id, VersionSelect::All)
        .try_collect()
        .await?;

    for persisted in &events {
        println!(
            "   - version {}: {:?}",
            persisted.version, persisted.event.message
        );
    }

    // ── 7. Demonstrate upcasting with a simulated legacy event ───────────
    println!("\n🕰️  Simulating legacy v1 event (writing with v1 serde directly)…");
    println!("   (In production, these would already exist in your database.)");
    println!("   When read back, the upcaster transparently migrates them to v2.");
    println!("   See `tests/schema_versioning.rs` for a full end-to-end test.");

    Ok(())
}
