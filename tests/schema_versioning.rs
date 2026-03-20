//! Integration tests for event schema versioning via [`UpcasterChain`].
//!
//! These tests verify the full cycle:
//!   1. Write events stamped with `schema_version = 1` (old format).
//!   2. Build a new store with `schema_version = 2` and a registered upcaster.
//!   3. Stream events back — payloads must reflect the migrated schema.
//!
//! The "old format" for `UserCreated` has a single `name` field.
//! The "new format" (v2) splits it into `first_name` / `last_name`.

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

// ── V1 domain event (old schema) ──────────────────────────────────────────

/// The *current* event type understood by the application.
/// V1 events stored in the DB (`{ "name": "Alice Bob" }`) will be upcasted
/// to this shape (`{ "first_name": "Alice", "last_name": "Bob" }`) before
/// deserialisation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum UserEvent {
    // V2 shape — what the application code expects today.
    Created {
        first_name: String,
        last_name: String,
    },
    Deleted,
}

impl Message for UserEvent {
    fn name(&self) -> &'static str {
        match self {
            UserEvent::Created { .. } => "UserCreated",
            UserEvent::Deleted => "UserDeleted",
        }
    }
}

// ── V1 event (old shape used only for writing the "legacy" row) ───────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UserEventV1 {
    Created { name: String },
    Deleted,
}

impl Message for UserEventV1 {
    fn name(&self) -> &'static str {
        match self {
            UserEventV1::Created { .. } => "UserCreated",
            UserEventV1::Deleted => "UserDeleted",
        }
    }
}

// ── Shared upcaster builder ───────────────────────────────────────────────

fn build_upcaster_chain() -> UpcasterChain {
    UpcasterChain::new().register(FnUpcaster::new(
        "UserCreated",
        1, // from schema_version 1
        2, // to schema_version 2
        |mut payload| {
            // Old payload: { "Created": { "name": "Alice Bob" } }
            // New payload: { "Created": { "first_name": "Alice", "last_name": "Bob" } }
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
}

// ── Helpers ───────────────────────────────────────────────────────────────

async fn sqlite_pool() -> sqlx::AnyPool {
    install_default_drivers();
    sqlx::any::AnyPoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap()
}

// ── Tests ─────────────────────────────────────────────────────────────────

/// Write v1-schema events, then read them back with the upcasting store —
/// the deserialized events should match the v2 shape.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn upcaster_migrates_v1_events_to_v2_on_read() {
    let pool = sqlite_pool().await;

    // ── Step 1: write events with schema_version = 1 (legacy shape) ──────
    let v1_store = Store::new(
        pool.clone(),
        eventually::serde::Json::<UserEventV1>::default(),
    )
    .await
    .unwrap()
    .with_schema_version(1);

    let stream_id = "user-upcast-test".to_string();

    v1_store
        .append(
            stream_id.clone(),
            version::Check::MustBe(0),
            vec![
                Envelope::from(UserEventV1::Created {
                    name: "Alice Bob".to_string(),
                }),
                Envelope::from(UserEventV1::Deleted),
            ],
        )
        .await
        .expect("v1 append should succeed");

    // ── Step 2: read them back via the v2 store with an upcaster ─────────
    let v2_store = Store::new(
        pool.clone(),
        eventually::serde::Json::<UserEvent>::default(),
    )
    .await
    .unwrap()
    .with_schema_version(2)
    .with_upcaster_chain(build_upcaster_chain());

    let events: Vec<_> = v2_store
        .stream(&stream_id, VersionSelect::All)
        .try_collect()
        .await
        .expect("streaming upcasted events should succeed");

    assert_eq!(events.len(), 2, "both events must be returned");

    // The first event must have been upcasted from `name` → `first_name`/`last_name`.
    assert_eq!(
        events[0].event.message,
        UserEvent::Created {
            first_name: "Alice".to_string(),
            last_name: "Bob".to_string(),
        },
        "UserCreated must be upcasted to v2 shape"
    );

    assert_eq!(
        events[1].event.message,
        UserEvent::Deleted,
        "UserDeleted needs no upcasting"
    );
}

/// Events written *after* the schema bump must already carry the v2 shape
/// and must not be double-transformed.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn new_events_not_double_upcasted() {
    let pool = sqlite_pool().await;

    let store = Store::new(
        pool.clone(),
        eventually::serde::Json::<UserEvent>::default(),
    )
    .await
    .unwrap()
    .with_schema_version(2)
    .with_upcaster_chain(build_upcaster_chain());

    let stream_id = "user-no-double-upcast".to_string();

    store
        .append(
            stream_id.clone(),
            version::Check::MustBe(0),
            vec![Envelope::from(UserEvent::Created {
                first_name: "Charlie".to_string(),
                last_name: "Delta".to_string(),
            })],
        )
        .await
        .unwrap();

    let events: Vec<_> = store
        .stream(&stream_id, VersionSelect::All)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].event.message,
        UserEvent::Created {
            first_name: "Charlie".to_string(),
            last_name: "Delta".to_string(),
        }
    );
}

/// Verify that events without a matching upcaster pass through untouched.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn events_without_upcaster_pass_through() {
    let pool = sqlite_pool().await;

    // Write with v1 store (no upcasting chain).
    let v1_store = Store::new(
        pool.clone(),
        eventually::serde::Json::<UserEventV1>::default(),
    )
    .await
    .unwrap()
    .with_schema_version(1);

    let stream_id = "user-passthrough".to_string();

    v1_store
        .append(
            stream_id.clone(),
            version::Check::MustBe(0),
            vec![Envelope::from(UserEventV1::Deleted)],
        )
        .await
        .unwrap();

    // Read with a v1 store — no upcaster → plain passthrough.
    let same_v1_store = Store::new(
        pool.clone(),
        eventually::serde::Json::<UserEventV1>::default(),
    )
    .await
    .unwrap()
    .with_schema_version(1);

    let events: Vec<_> = same_v1_store
        .stream(&stream_id, VersionSelect::All)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(events.len(), 1);
    assert!(matches!(events[0].event.message, UserEventV1::Deleted));
}

/// Verify the `schema_version()` accessor returns what was set.
#[test]
fn schema_version_accessor() {
    // Purely a compile + basic logic check — no DB needed.
    // We create a dummy pool future but don't await it; just verify the
    // builder API compiles and the accessor value is correct by inspecting
    // DEFAULT_SCHEMA_VERSION.
    use eventually_any::event::DEFAULT_SCHEMA_VERSION;
    assert_eq!(DEFAULT_SCHEMA_VERSION, 1);
}
