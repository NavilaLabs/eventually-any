//! `eventually-any` provides PostgreSQL, SQLite and MySQL backend
//! implementations for the [`eventually`] crate, built on top of
//! [`sqlx::AnyPool`] so a single binary can target multiple databases.
//!
//! # Feature flags
//!
//! | Flag         | Effect |
//! |---|---|
//! | `postgres`   | Enable PostgreSQL support via sqlx |
//! | `sqlite`     | Enable SQLite support via sqlx |
//! | `mysql`      | Enable MySQL support via sqlx |
//! | `migrations` | Embed and auto-run SQL migrations on repository/store construction |
//! | `snapshots`  | Replace the classic `aggregate` module with the `snapshot` module (see below) |
//! | `full`       | Enable all of the above |
//!
//! # Aggregate repository: two modes
//!
//! **Without `snapshots`** (default): [`aggregate::Repository`] is available.
//! It maintains a single mutable row per aggregate in the `aggregates` table
//! and performs optimistic locking with `UPDATE … WHERE version = expected`.
//!
//! **With `snapshots`**: [`snapshot::Repository`] is available instead.
//! Every save appends a new immutable row to the `snapshots` table.
//! Loading uses a *snapshot + delta-replay* strategy:
//! the latest snapshot is read directly (O(1)), then any events recorded
//! after that snapshot are streamed and folded via [`eventually::aggregate::Aggregate::apply`].
//! This eliminates full event-stream replay after the first save while
//! keeping the complete event history intact.
//!
//! Both modules expose **the same public API**
//! (`Repository<T, Serde, EvtSerde>`, `Getter`, `Saver`) so switching
//! between them is purely a feature-flag change — no application code needs
//! to change.
//!
//! # Event schema versioning
//!
//! Every event row carries a `schema_version` integer column.  When your
//! event payload format changes, you:
//!
//! 1. Bump [`event::Store::with_schema_version`] to the new version number.
//! 2. Implement [`upcasting::Upcaster`] (or use [`upcasting::FnUpcaster`])
//!    for each old-version → new-version transition.
//! 3. Register upcasters via [`event::Store::with_upcaster_chain`].
//!
//! Old events stored in the database are transparently upgraded at read time;
//! no data migration is required.
//!
//! ```rust,ignore
//! use eventually_any::event::Store;
//! use eventually_any::upcasting::{FnUpcaster, UpcasterChain};
//! use eventually::serde;
//! use serde_json::json;
//!
//! let chain = UpcasterChain::new()
//!     .register(FnUpcaster::new("UserCreated", 1, 2, |mut p| {
//!         // v1 had "name", v2 splits it into "first_name" / "last_name"
//!         let name = p["name"].as_str().unwrap_or("").to_owned();
//!         let parts: Vec<_> = name.splitn(2, ' ').collect();
//!         p["first_name"] = json!(parts.first().copied().unwrap_or(""));
//!         p["last_name"]  = json!(parts.get(1).copied().unwrap_or(""));
//!         p.as_object_mut().unwrap().remove("name");
//!         p
//!     }));
//!
//! let store = Store::new(pool, serde::Json::<MyEvent>::default())
//!     .await?
//!     .with_schema_version(2)
//!     .with_upcaster_chain(chain);
//! ```

#![deny(unsafe_code, unused_qualifications, trivial_casts)]
#![deny(clippy::all, clippy::pedantic, clippy::cargo)]
#![warn(missing_docs)]

#[cfg(not(any(feature = "postgres", feature = "sqlite", feature = "mysql")))]
compile_error!("At least one database feature must be enabled: postgres, sqlite, mysql");

/// Raw event store: append events to a stream and stream them back.
///
/// The [`Store`](event::Store) type implements both
/// [`Appender`](eventually::event::store::Appender) and
/// [`Streamer`](eventually::event::store::Streamer) and supports PostgreSQL,
/// SQLite and MySQL transparently via `sqlx::AnyPool`.
pub mod event;
pub mod upcasting;

// Exactly one of `aggregate` or `snapshot` is compiled in, depending on
// whether the `snapshots` feature is active.
#[cfg(not(feature = "snapshots"))]
pub mod aggregate;

#[cfg(feature = "snapshots")]
pub mod snapshot;

// ── Embedded migrations ───────────────────────────────────────────────────

#[cfg(all(
    feature = "postgres",
    feature = "migrations",
    not(feature = "snapshots")
))]
pub(crate) static MIGRATIONS_POSTGRES: sqlx::migrate::Migrator =
    sqlx::migrate!("./migrations/aggregates/postgres");
#[cfg(all(feature = "postgres", feature = "migrations", feature = "snapshots"))]
pub(crate) static MIGRATIONS_POSTGRES: sqlx::migrate::Migrator =
    sqlx::migrate!("./migrations/snapshots/postgres");

#[cfg(all(feature = "sqlite", feature = "migrations", not(feature = "snapshots")))]
pub(crate) static MIGRATIONS_SQLITE: sqlx::migrate::Migrator =
    sqlx::migrate!("./migrations/aggregates/sqlite");
#[cfg(all(feature = "sqlite", feature = "migrations", feature = "snapshots"))]
pub(crate) static MIGRATIONS_SQLITE: sqlx::migrate::Migrator =
    sqlx::migrate!("./migrations/snapshots/sqlite");

#[cfg(all(feature = "mysql", feature = "migrations", not(feature = "snapshots")))]
pub(crate) static MIGRATIONS_MYSQL: sqlx::migrate::Migrator =
    sqlx::migrate!("./migrations/aggregates/mysql");
#[cfg(all(feature = "mysql", feature = "migrations", feature = "snapshots"))]
pub(crate) static MIGRATIONS_MYSQL: sqlx::migrate::Migrator =
    sqlx::migrate!("./migrations/snapshots/mysql");

// ── Internal migration helper ─────────────────────────────────────────────

/// Run whichever embedded migration set matches the pool's backend.
/// No-op when the `migrations` feature is disabled.
#[allow(unused_variables)] // `pool` unused when no migration feature is on
pub async fn run_migrations(pool: &sqlx::AnyPool) -> Result<(), sqlx::migrate::MigrateError> {
    let backend = pool
        .acquire()
        .await
        .map(|c| c.backend_name().to_string())
        .unwrap_or_default();

    #[cfg(all(feature = "postgres", feature = "migrations"))]
    if backend == "PostgreSQL" {
        return MIGRATIONS_POSTGRES.run(pool).await;
    }

    #[cfg(all(feature = "sqlite", feature = "migrations"))]
    if backend == "SQLite" {
        return MIGRATIONS_SQLITE.run(pool).await;
    }

    #[cfg(all(feature = "mysql", feature = "migrations"))]
    if backend == "MySQL" {
        return MIGRATIONS_MYSQL.run(pool).await;
    }

    Ok(())
}
