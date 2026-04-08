//! Periodic-snapshot aggregate repository for `sqlx::Any` backends.
//!
//! Available when the `snapshots` feature **is** active.
//! When `snapshots` is disabled, [`crate::aggregate`] is compiled instead —
//! it provides an identical public API with classic mutable-row semantics.
//!
//! # Strategy
//!
//! This repository implements **Option C** — event-only storage with periodic
//! snapshotting:
//!
//! | Concern | Classic `aggregates` | Snapshot (this module) |
//! |---|---|---|
//! | Event storage | One mutable row per aggregate | Full immutable event stream |
//! | Snapshot writes | Always (every save) | Only every N events (configurable) |
//! | Load strategy | O(1) state read | Latest snapshot + delta event replay |
//! | Audit trail | ❌ Events only via raw store | ✅ Complete event history always |
//!
//! # Tracing
//!
//! When the `tracing` feature is enabled, `get` and `save` open `INFO`-level
//! spans.  Snapshot reads/writes, delta replays, and conflicts are recorded
//! at `DEBUG` or `WARN` as appropriate.

use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use eventually::aggregate::Aggregate;
use eventually::version::Version;
use eventually::{aggregate, event, serde, version};
use futures::TryStreamExt;
use sqlx::{AnyPool, Row};

use crate::event::DEFAULT_SCHEMA_VERSION;
use crate::logging::{debug, error, info, span, warn};
use crate::upcasting::UpcasterChain;

/// Default: write a snapshot every 50 events.
pub const DEFAULT_SNAPSHOT_EVERY: usize = 50;

/// Periodic-snapshot [`eventually::aggregate::Repository`] for SQL databases.
///
/// Identical public API to [`crate::aggregate::Repository`]; switch between
/// them by toggling the `snapshots` Cargo feature.
///
/// Events are **always** appended. A snapshot is only written when the
/// aggregate version crosses a multiple of [`Self::with_snapshot_every`]
/// (default: 50).
#[derive(Debug, Clone)]
pub struct Repository<T, Serde, EvtSerde>
where
    T: Aggregate,
    <T as Aggregate>::Id: ToString,
    Serde: serde::Serde<T>,
    EvtSerde: serde::Serde<T::Event>,
{
    pool: AnyPool,
    aggregate_serde: Serde,
    event_serde: EvtSerde,
    backend: String,
    schema_version: u32,
    upcaster_chain: Arc<UpcasterChain>,
    /// Write a snapshot whenever `new_version % snapshot_every == 0`.
    snapshot_every: usize,
    t: PhantomData<T>,
}

impl<T, Serde, EvtSerde> Repository<T, Serde, EvtSerde>
where
    T: Aggregate,
    <T as Aggregate>::Id: ToString,
    Serde: serde::Serde<T>,
    EvtSerde: serde::Serde<T::Event>,
{
    /// Run migrations (when `migrations` feature is active) and return a new
    /// [`Repository`] instance.
    ///
    /// # Errors
    ///
    /// Returns an error if migrations fail.
    pub async fn new(
        pool: AnyPool,
        aggregate_serde: Serde,
        event_serde: EvtSerde,
    ) -> Result<Self, sqlx::migrate::MigrateError> {
        let backend = pool
            .acquire()
            .await
            .map(|c| c.backend_name().to_string())
            .unwrap_or_default();

        crate::run_migrations(&pool).await?;

        info!(
            aggregate_type = T::type_name(),
            backend = %backend,
            snapshot_every = DEFAULT_SNAPSHOT_EVERY,
            "snapshot repository initialised"
        );

        Ok(Self {
            pool,
            aggregate_serde,
            event_serde,
            backend,
            schema_version: DEFAULT_SCHEMA_VERSION,
            upcaster_chain: Arc::new(UpcasterChain::new()),
            snapshot_every: DEFAULT_SNAPSHOT_EVERY,
            t: PhantomData,
        })
    }

    /// Set the schema version stamped on newly-written events.
    #[must_use]
    pub fn with_schema_version(mut self, version: u32) -> Self {
        self.schema_version = version;
        self
    }

    /// Attach an [`UpcasterChain`] applied to events at read time.
    #[must_use]
    pub fn with_upcaster_chain(mut self, chain: UpcasterChain) -> Self {
        self.upcaster_chain = Arc::new(chain);
        self
    }

    /// Configure how often snapshots are written.
    ///
    /// A snapshot is written after any save where
    /// `new_aggregate_version % snapshot_every == 0`.
    ///
    /// - `1`  → snapshot on every save (eager, minimises replay cost)
    /// - `50` → snapshot every 50 events (default, balanced)
    /// - `usize::MAX` → effectively disabled (always full replay)
    #[must_use]
    pub fn with_snapshot_every(mut self, n: usize) -> Self {
        assert!(n > 0, "snapshot_every must be > 0");
        self.snapshot_every = n;
        self
    }

    /// Returns how many events trigger a snapshot write.
    pub fn snapshot_every(&self) -> usize {
        self.snapshot_every
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────

impl<T, Serde, EvtSerde> Repository<T, Serde, EvtSerde>
where
    T: Aggregate + Send + Sync,
    <T as Aggregate>::Id: ToString,
    Serde: serde::Serde<T> + Send + Sync,
    EvtSerde: serde::Serde<T::Event> + Send + Sync,
{
    fn ph(&self, n: usize) -> String {
        if self.backend == "MySQL" {
            "?".to_owned()
        } else {
            format!("${n}")
        }
    }

    fn version_col(&self) -> &'static str {
        if self.backend == "MySQL" {
            "`version`"
        } else {
            r#""version""#
        }
    }

    /// Fetch the latest snapshot for `(aggregate_type, aggregate_id)`.
    async fn latest_snapshot(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<(Version, Vec<u8>)>, aggregate::repository::GetError> {
        let (p1, p2) = (self.ph(1), self.ph(2));
        let ver = self.version_col();

        let query = format!(
            "SELECT {ver}, state FROM snapshots
             WHERE aggregate_type = {p1} AND aggregate_id = {p2}
             ORDER BY {ver} DESC LIMIT 1"
        );

        let row = sqlx::query(&query)
            .bind(T::type_name())
            .bind(aggregate_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| {
                aggregate::repository::GetError::Internal(anyhow!(
                    "failed to query latest snapshot: {}",
                    err
                ))
            })?;

        let Some(row) = row else { return Ok(None) };

        let version: i32 = row.try_get("version").map_err(|err| {
            aggregate::repository::GetError::Internal(anyhow!(
                "failed to read 'version' from snapshot row: {}",
                err
            ))
        })?;

        let state_bytes: Vec<u8> = row.try_get("state").map_err(|err| {
            aggregate::repository::GetError::Internal(anyhow!(
                "failed to read 'state' from snapshot row: {}",
                err
            ))
        })?;

        #[allow(clippy::cast_sign_loss)]
        Ok(Some((version as Version, state_bytes)))
    }

    /// Write a snapshot row inside an existing transaction.
    async fn write_snapshot_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Any>,
        aggregate_id: &str,
        new_version: i32,
        root: &aggregate::Root<T>,
    ) -> Result<(), aggregate::repository::SaveError> {
        let state_bytes = self
            .aggregate_serde
            .serialize(root.to_aggregate_type::<T>())
            .map_err(|err| {
                aggregate::repository::SaveError::Internal(anyhow!(
                    "failed to serialise aggregate state for snapshot: {}",
                    err
                ))
            })?;

        let (p1, p2, p3, p4, p5) = (self.ph(1), self.ph(2), self.ph(3), self.ph(4), self.ph(5));

        let snap_insert = if self.backend == "MySQL" {
            format!(
                "INSERT INTO snapshots
                 (aggregate_type, aggregate_id, event_stream_id, `version`, state)
                 VALUES ({p1}, {p2}, {p3}, {p4}, {p5})"
            )
        } else {
            format!(
                "INSERT INTO snapshots
                 (aggregate_type, aggregate_id, event_stream_id, \"version\", state)
                 VALUES ({p1}, {p2}, {p3}, {p4}, {p5})"
            )
        };

        sqlx::query(&snap_insert)
            .bind(T::type_name())
            .bind(aggregate_id)
            .bind(aggregate_id)
            .bind(new_version)
            .bind(state_bytes)
            .execute(&mut **tx)
            .await
            .map_err(|err| {
                error!(
                    aggregate_id = aggregate_id,
                    aggregate_type = T::type_name(),
                    version = new_version,
                    error = %err,
                    "failed to write snapshot"
                );
                aggregate::repository::SaveError::Internal(anyhow!(
                    "failed to insert snapshot row: {}",
                    err
                ))
            })?;

        debug!(
            aggregate_id = aggregate_id,
            aggregate_type = T::type_name(),
            version = new_version,
            "snapshot written"
        );

        Ok(())
    }
}

// ── Getter ────────────────────────────────────────────────────────────────

#[async_trait]
impl<T, Serde, EvtSerde> aggregate::repository::Getter<T> for Repository<T, Serde, EvtSerde>
where
    T: Aggregate + Send + Sync,
    T::Id: ToString + Clone,
    T::Error: std::fmt::Display + Send + Sync,
    Serde: serde::Serde<T> + Send + Sync,
    EvtSerde: serde::Serde<T::Event> + Send + Sync + Clone,
{
    async fn get(&self, id: &T::Id) -> Result<aggregate::Root<T>, aggregate::repository::GetError> {
        let aggregate_id = id.to_string();

        let _span = span!(
            "snapshot_repository::get",
            aggregate_id = %aggregate_id,
            aggregate_type = T::type_name()
        );

        info!(
            aggregate_id = %aggregate_id,
            aggregate_type = T::type_name(),
            "loading aggregate (snapshot + delta)"
        );

        // ── Step 1: latest snapshot (may be None) ─────────────────────────
        let snapshot = self.latest_snapshot(&aggregate_id).await?;

        let (base_state, replay_from): (Option<T>, Version) = match snapshot {
            None => {
                debug!(
                    aggregate_id = %aggregate_id,
                    aggregate_type = T::type_name(),
                    "no snapshot found — full event replay"
                );
                (None, 0)
            }
            Some((snap_version, state_bytes)) => {
                let aggregate: T =
                    self.aggregate_serde
                        .deserialize(&state_bytes)
                        .map_err(|err| {
                            aggregate::repository::GetError::Internal(anyhow!(
                                "failed to deserialise snapshot state: {}",
                                err
                            ))
                        })?;
                debug!(
                    aggregate_id = %aggregate_id,
                    aggregate_type = T::type_name(),
                    snapshot_version = snap_version,
                    "snapshot loaded"
                );
                (Some(aggregate), snap_version)
            }
        };

        // ── Step 2: stream delta events ───────────────────────────────────
        let from_select = if replay_from == 0 {
            event::VersionSelect::All
        } else {
            event::VersionSelect::From(replay_from + 1)
        };

        let evt_store = crate::event::Store::<T::Id, T::Event, EvtSerde>::new_unchecked(
            self.pool.clone(),
            &self.event_serde,
            &self.backend,
            self.schema_version,
            Arc::clone(&self.upcaster_chain),
        );

        let delta_events: Vec<event::Envelope<T::Event>> = {
            use eventually::event::store::Streamer as _;

            evt_store
                .stream(id, from_select)
                .map_ok(|p: event::Persisted<T::Id, T::Event>| p.event)
                .try_collect()
                .await
                .map_err(|err| {
                    aggregate::repository::GetError::Internal(anyhow!(
                        "failed to stream delta events: {}",
                        err
                    ))
                })?
        };

        let delta_count = delta_events.len();
        if delta_count > 0 {
            debug!(
                aggregate_id = %aggregate_id,
                aggregate_type = T::type_name(),
                from_version = replay_from,
                delta_events = delta_count,
                "replaying delta events"
            );
        }

        // ── Step 3: nothing at all → NotFound ─────────────────────────────
        if base_state.is_none() && delta_events.is_empty() {
            warn!(
                aggregate_id = %aggregate_id,
                aggregate_type = T::type_name(),
                "aggregate not found"
            );
            return Err(aggregate::repository::GetError::NotFound);
        }

        // ── Step 4: fold delta events onto base state ─────────────────────
        let (final_state, final_version) = {
            let mut state = base_state;
            let mut version = replay_from;

            for envelope in delta_events {
                state = Some(T::apply(state, envelope.message).map_err(|err| {
                    aggregate::repository::GetError::Internal(anyhow!(
                        "failed to apply delta event during rehydration: {}",
                        err
                    ))
                })?);
                version += 1;
            }

            (state, version)
        };

        match final_state {
            None => Err(aggregate::repository::GetError::NotFound),
            Some(agg) => {
                debug!(
                    aggregate_id = %aggregate_id,
                    aggregate_type = T::type_name(),
                    version = final_version,
                    "aggregate loaded (snapshot + delta)"
                );
                Ok(aggregate::Root::rehydrate_from_state(final_version, agg))
            }
        }
    }
}

// ── Saver ─────────────────────────────────────────────────────────────────

#[async_trait]
impl<T, Serde, EvtSerde> aggregate::repository::Saver<T> for Repository<T, Serde, EvtSerde>
where
    T: Aggregate + Send + Sync,
    T::Id: ToString,
    Serde: serde::Serde<T> + Send + Sync,
    EvtSerde: serde::Serde<T::Event> + Send + Sync,
{
    async fn save(
        &self,
        root: &mut aggregate::Root<T>,
    ) -> Result<(), aggregate::repository::SaveError> {
        let events_to_commit = root.take_uncommitted_events();

        if events_to_commit.is_empty() {
            debug!(
                aggregate_type = T::type_name(),
                "save called with no uncommitted events — skipping"
            );
            return Ok(());
        }

        let aggregate_id = root.aggregate_id().to_string();
        let event_count = events_to_commit.len();
        let new_version = root.version() as i32;
        let expected_version = root.version() - (events_to_commit.len() as Version);

        let will_snapshot =
            self.snapshot_every > 0 && (root.version() as usize) % self.snapshot_every == 0;

        let _span = span!(
            "snapshot_repository::save",
            aggregate_id = %aggregate_id,
            aggregate_type = T::type_name(),
            events = event_count,
            snapshot = will_snapshot
        );

        info!(
            aggregate_id = %aggregate_id,
            aggregate_type = T::type_name(),
            events = event_count,
            new_version = new_version,
            snapshot = will_snapshot,
            "saving aggregate"
        );

        let mut tx = self.pool.begin().await.map_err(|err| {
            error!(
                aggregate_id = %aggregate_id,
                error = %err,
                "failed to begin save transaction"
            );
            aggregate::repository::SaveError::Internal(anyhow!(
                "failed to begin transaction: {}",
                err
            ))
        })?;

        if tx.backend_name() == "PostgreSQL" {
            sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE DEFERRABLE")
                .execute(&mut *tx)
                .await
                .map_err(|err| {
                    aggregate::repository::SaveError::Internal(anyhow!(
                        "failed to set transaction isolation: {}",
                        err
                    ))
                })?;
        }

        self.upsert_event_stream(&mut tx, &aggregate_id, expected_version, new_version)
            .await?;

        #[allow(clippy::cast_possible_truncation)]
        crate::event::append_domain_events(
            &mut tx,
            &self.event_serde,
            &aggregate_id,
            new_version,
            self.schema_version,
            events_to_commit,
        )
        .await
        .map_err(|err| {
            error!(
                aggregate_id = %aggregate_id,
                error = %err,
                "failed to append events in snapshot save"
            );
            aggregate::repository::SaveError::Internal(anyhow!(
                "failed to append domain events: {}",
                err
            ))
        })?;

        if will_snapshot {
            self.write_snapshot_in_tx(&mut tx, &aggregate_id, new_version, root)
                .await?;
        }

        tx.commit().await.map_err(|err| {
            error!(
                aggregate_id = %aggregate_id,
                error = %err,
                "failed to commit snapshot save transaction"
            );
            aggregate::repository::SaveError::Internal(anyhow!(
                "failed to commit transaction: {}",
                err
            ))
        })?;

        debug!(
            aggregate_id = %aggregate_id,
            aggregate_type = T::type_name(),
            new_version = new_version,
            events = event_count,
            snapshot_written = will_snapshot,
            "aggregate saved successfully"
        );

        Ok(())
    }
}

impl<T, Serde, EvtSerde> Repository<T, Serde, EvtSerde>
where
    T: Aggregate + Send + Sync,
    <T as Aggregate>::Id: ToString,
    Serde: serde::Serde<T> + Send + Sync,
    EvtSerde: serde::Serde<T::Event> + Send + Sync,
{
    /// Insert (first save) or update (subsequent saves) the `event_streams` row,
    /// using the expected version as an optimistic lock.
    async fn upsert_event_stream(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Any>,
        aggregate_id: &str,
        expected_version: Version,
        new_version: i32,
    ) -> Result<(), aggregate::repository::SaveError> {
        let (p1, p2) = (self.ph(1), self.ph(2));

        if expected_version == 0 {
            let insert =
                format!("INSERT INTO event_streams (event_stream_id, version) VALUES ({p1}, {p2})");
            if let Err(err) = sqlx::query(&insert)
                .bind(aggregate_id)
                .bind(new_version)
                .execute(&mut **tx)
                .await
            {
                let is_dup = err.as_database_error().map_or(false, |e| {
                    let code = e.code().unwrap_or_default();
                    code == "23505"
                        || code == "1062"
                        || code == "23000"
                        || code == "2067"
                        || code == "1555"
                });
                if is_dup {
                    warn!(
                        aggregate_id = aggregate_id,
                        aggregate_type = T::type_name(),
                        expected = expected_version,
                        "save conflict (duplicate stream insert)"
                    );
                    return Err(aggregate::repository::SaveError::Conflict(
                        version::ConflictError {
                            expected: expected_version,
                            actual: expected_version + 1,
                        },
                    ));
                }
                return Err(aggregate::repository::SaveError::Internal(anyhow!(
                    "failed to insert event stream: {}",
                    err
                )));
            }
        } else {
            let (p3) = self.ph(3);
            let update = format!(
                "UPDATE event_streams SET version = {p1}
                 WHERE event_stream_id = {p2} AND version = {p3}"
            );
            match sqlx::query(&update)
                .bind(new_version)
                .bind(aggregate_id)
                .bind(expected_version as i32)
                .execute(&mut **tx)
                .await
            {
                Ok(res) if res.rows_affected() == 0 => {
                    let sel = if self.backend == "MySQL" {
                        "SELECT version FROM event_streams WHERE event_stream_id = ?"
                    } else {
                        "SELECT version FROM event_streams WHERE event_stream_id = $1"
                    };
                    let actual: i32 = sqlx::query(sel)
                        .bind(aggregate_id)
                        .fetch_optional(&mut **tx)
                        .await
                        .ok()
                        .flatten()
                        .and_then(|r| r.try_get("version").ok())
                        .unwrap_or(0);

                    warn!(
                        aggregate_id = aggregate_id,
                        aggregate_type = T::type_name(),
                        expected = expected_version,
                        actual = actual,
                        "save conflict (zero rows affected on stream update)"
                    );
                    return Err(aggregate::repository::SaveError::Conflict(
                        version::ConflictError {
                            expected: expected_version,
                            actual: actual as Version,
                        },
                    ));
                }
                Ok(_) => {}
                Err(err) => {
                    let is_serial = err
                        .as_database_error()
                        .map_or(false, |e| e.code().unwrap_or_default() == "40001");
                    if is_serial {
                        warn!(
                            aggregate_id = aggregate_id,
                            aggregate_type = T::type_name(),
                            expected = expected_version,
                            "save conflict (serialization failure)"
                        );
                        return Err(aggregate::repository::SaveError::Conflict(
                            version::ConflictError {
                                expected: expected_version,
                                actual: expected_version + 1,
                            },
                        ));
                    }
                    return Err(aggregate::repository::SaveError::Internal(anyhow!(
                        "failed to update event stream: {}",
                        err
                    )));
                }
            }
        }
        Ok(())
    }
}
