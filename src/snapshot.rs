//! Append-only snapshot-based aggregate repository for `sqlx::Any` backends.
//!
//! Available when the `snapshots` feature **is** active.
//! When `snapshots` is disabled, [`crate::aggregate`] is compiled instead —
//! it provides an identical public API with classic mutable-row semantics.
//!
//! # Why snapshots?
//!
//! | Concern | Classic `aggregates` table | Snapshot table |
//! |---|---|---|
//! | Audit trail | None — previous state is overwritten | Full history; each save appends a row |
//! | Replay cost | Always O(1) reads, but no event history | O(1) read from snapshot + tiny delta |
//! | Multi-type table | One discriminator column needed | Natural: every row is typed |
//! | Schema | Mutable `UPDATE` | Immutable `INSERT` |
//!
//! # Loading strategy
//!
//! [`Repository::get`] uses *snapshot + delta replay*:
//!
//! 1. Read the most-recent snapshot row for `(aggregate_type, aggregate_id)`.
//! 2. Stream all events from the `events` table with `version > snapshot.version`.
//! 3. Fold delta events via [`eventually::aggregate::Aggregate::apply`].
//! 4. Wrap the final state in [`eventually::aggregate::Root::rehydrate_from_state`].
//!
//! If no snapshot exists, the full event stream is replayed from the beginning
//! (identical behaviour to [`crate::aggregate`]).
//!
//! # Saving strategy
//!
//! [`Repository::save`] runs a single transaction that:
//! 1. Reads the current snapshot version for the aggregate (optimistic lock).
//! 2. Verifies it matches the expected version.
//! 3. Upserts the `event_streams` version.
//! 4. Inserts a **new** snapshot row (old rows are kept).
//! 5. Inserts domain events.

use std::marker::PhantomData;

use anyhow::anyhow;
use async_trait::async_trait;
use eventually::aggregate::Aggregate;
use eventually::version::Version;
use eventually::{aggregate, event, serde, version};
use futures::TryStreamExt;
use sqlx::{Any, AnyPool, Row};

/// Append-only snapshot [`eventually::aggregate::Repository`] for SQL databases.
///
/// Identical public API to [`crate::aggregate::Repository`]; switch between
/// them by toggling the `snapshots` Cargo feature.
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

        Ok(Self {
            pool,
            aggregate_serde,
            event_serde,
            backend,
            t: PhantomData,
        })
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
    /// Returns `(version, state_bytes)` or `None` if no snapshot exists yet.
    async fn latest_snapshot(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<(Version, Vec<u8>)>, aggregate::repository::GetError> {
        let (p1, p2) = (self.ph(1), self.ph(2));
        let ver = self.version_col();

        let query = format!(
            "SELECT {ver}, state FROM snapshots WHERE aggregate_type = {p1} AND aggregate_id = {p2} ORDER BY {ver} DESC LIMIT 1"
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

    /// Read the snapshot version currently stored for this aggregate (0 if none).
    /// Used inside a transaction for the optimistic-lock check on save.
    async fn current_snapshot_version(
        &self,
        tx: &mut sqlx::Transaction<'_, Any>,
        aggregate_id: &str,
    ) -> Result<i32, aggregate::repository::SaveError> {
        let (p1, p2) = (self.ph(1), self.ph(2));
        let ver = self.version_col();

        let query = format!(
            "SELECT {ver} FROM snapshots WHERE aggregate_type = {p1} AND aggregate_id = {p2} ORDER BY {ver} DESC LIMIT 1"
        );

        let row = sqlx::query(&query)
            .bind(T::type_name())
            .bind(aggregate_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(|err| {
                aggregate::repository::SaveError::Internal(anyhow!(
                    "failed to read current snapshot version: {}",
                    err
                ))
            })?;

        Ok(row.map(|r| r.try_get("version").unwrap_or(0)).unwrap_or(0))
    }

    /// Upsert `event_streams` and insert a new snapshot row inside `tx`.
    async fn write_snapshot(
        &self,
        tx: &mut sqlx::Transaction<'_, Any>,
        aggregate_id: &str,
        expected_version: Version,
        root: &aggregate::Root<T>,
    ) -> Result<(), aggregate::repository::SaveError> {
        // ── Optimistic lock ───────────────────────────────────────────────
        let actual = self.current_snapshot_version(tx, aggregate_id).await?;

        if actual != expected_version as i32 {
            return Err(aggregate::repository::SaveError::Conflict(
                version::ConflictError {
                    expected: expected_version,
                    actual: actual as Version,
                },
            ));
        }

        // ── Serialise state ───────────────────────────────────────────────
        let state_bytes = self
            .aggregate_serde
            .serialize(root.to_aggregate_type::<T>())
            .map_err(|err| {
                aggregate::repository::SaveError::Internal(anyhow!(
                    "failed to serialise aggregate state: {}",
                    err
                ))
            })?;

        let new_version = root.version() as i32;
        let (p1, p2, p3) = (self.ph(1), self.ph(2), self.ph(3));

        // ── Upsert event_streams ──────────────────────────────────────────
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
                    code == "23505" || code == "1062" || code == "23000" || code == "2067"
                });
                if is_dup {
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
            let update = format!(
                "UPDATE event_streams SET version = {p1} WHERE event_stream_id = {p2} AND version = {p3}"
            );
            match sqlx::query(&update)
                .bind(new_version)
                .bind(aggregate_id)
                .bind(expected_version as i32)
                .execute(&mut **tx)
                .await
            {
                Ok(res) if res.rows_affected() == 0 => {
                    return Err(aggregate::repository::SaveError::Conflict(
                        version::ConflictError {
                            expected: expected_version,
                            actual: expected_version + 1,
                        },
                    ));
                }
                Ok(_) => {}
                Err(err) => {
                    return Err(aggregate::repository::SaveError::Internal(anyhow!(
                        "failed to update event stream: {}",
                        err
                    )));
                }
            }
        }

        // ── Insert new snapshot row ───────────────────────────────────────
        // MySQL uses backtick-quoted `version`; Postgres/SQLite use "version".
        let (p1, p2, p3, p4, p5, p6) = (
            self.ph(1),
            self.ph(2),
            self.ph(3),
            self.ph(4),
            self.ph(5),
            self.ph(6),
        );
        let snap_insert = if self.backend == "MySQL" {
            format!(
                "INSERT INTO snapshots
                 (aggregate_type, aggregate_id, event_stream_id, `version`, state)
                 VALUES ({p1}, {p2}, {p3}, {p4}, {p5})"
            )
        } else {
            format!(
                r#"INSERT INTO snapshots
                 (aggregate_type, aggregate_id, event_stream_id, "version", state)
                 VALUES ({p1}, {p2}, {p3}, {p4}, {p5})"#
            )
        };

        // p6 is unused here but the variable was pre-bound above; suppress lint.
        let _ = &p6;

        sqlx::query(&snap_insert)
            .bind(T::type_name()) // aggregate_type
            .bind(aggregate_id) // aggregate_id
            .bind(aggregate_id) // event_stream_id (same as aggregate_id)
            .bind(new_version) // version
            .bind(state_bytes) // state
            .execute(&mut **tx)
            .await
            .map_err(|err| {
                aggregate::repository::SaveError::Internal(anyhow!(
                    "failed to insert snapshot row: {}",
                    err
                ))
            })?;

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
    /// Load an aggregate root via *snapshot + delta replay*:
    ///
    /// 1. Read the most-recent snapshot → base state + version.
    /// 2. Stream events after that version → delta.
    /// 3. Fold delta via `T::apply`.
    /// 4. Return `Root::rehydrate_from_state(final_version, final_state)`.
    async fn get(&self, id: &T::Id) -> Result<aggregate::Root<T>, aggregate::repository::GetError> {
        let aggregate_id = id.to_string();

        // ── Step 1: latest snapshot ───────────────────────────────────────
        let snapshot = self.latest_snapshot(&aggregate_id).await?;

        let (base_state, replay_from): (Option<T>, Version) = match snapshot {
            None => (None, 0),
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
                (Some(aggregate), snap_version)
            }
        };

        // ── Step 2: collect delta events ──────────────────────────────────
        let from_select = if replay_from == 0 {
            event::VersionSelect::All
        } else {
            event::VersionSelect::From(replay_from + 1)
        };

        let evt_store = crate::event::Store::<T::Id, T::Event, EvtSerde>::new_unchecked(
            self.pool.clone(),
            &self.event_serde,
            &self.backend,
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

        // ── Step 3: fold delta onto base state ────────────────────────────
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
            Some(agg) => Ok(aggregate::Root::rehydrate_from_state(final_version, agg)),
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
    /// Persist uncommitted events **and** a new snapshot in a single
    /// transaction.
    ///
    /// Transaction order:
    /// 1. Optimistic-lock check (compare snapshot version).
    /// 2. Upsert `event_streams`.
    /// 3. Insert new snapshot row.
    /// 4. Insert domain events.
    /// 5. Commit.
    async fn save(
        &self,
        root: &mut aggregate::Root<T>,
    ) -> Result<(), aggregate::repository::SaveError> {
        let events_to_commit = root.take_uncommitted_events();

        if events_to_commit.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await.map_err(|err| {
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

        let aggregate_id = root.aggregate_id().to_string();
        let expected_version = root.version() - (events_to_commit.len() as Version);

        // ── 1–4: snapshot + event_streams upsert ─────────────────────────
        self.write_snapshot(&mut tx, &aggregate_id, expected_version, root)
            .await?;

        // ── 5: domain events ──────────────────────────────────────────────
        #[allow(clippy::cast_possible_truncation)]
        crate::event::append_domain_events(
            &mut tx,
            &self.event_serde,
            &aggregate_id,
            root.version() as i32,
            events_to_commit,
        )
        .await
        .map_err(|err| {
            aggregate::repository::SaveError::Internal(anyhow!(
                "failed to append domain events: {}",
                err
            ))
        })?;

        tx.commit().await.map_err(|err| {
            aggregate::repository::SaveError::Internal(anyhow!(
                "failed to commit transaction: {}",
                err
            ))
        })?;

        Ok(())
    }
}
