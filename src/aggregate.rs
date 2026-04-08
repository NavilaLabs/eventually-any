//! Classic mutable-row aggregate repository for `sqlx::Any` backends.
//!
//! Available when the `snapshots` feature is **not** active.
//! When `snapshots` is enabled, [`crate::snapshot`] is compiled instead —
//! it provides an identical public API with append-only storage semantics.
//!
//! # Storage layout
//!
//! One row per aggregate in the `aggregates` table, updated in place on
//! every save.  Optimistic locking is performed in application code:
//! the current `version` is read inside the transaction and compared to
//! the expected version before writing.
//!
//! # Schema versioning
//!
//! The [`Repository`] forwards `schema_version` and an [`UpcasterChain`] to
//! its inner [`event::Store`], so all event reads go through the upcasting
//! pipeline automatically.
//!
//! # Tracing
//!
//! When the `tracing` feature is enabled, `get` and `save` open `INFO`-level
//! spans.  Conflicts are recorded at `WARN`, internal errors at `ERROR`.

use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use eventually::aggregate::Aggregate;
use eventually::version::Version;
use eventually::{aggregate, serde, version};
use sqlx::{Any, AnyPool, Row};

use crate::event::DEFAULT_SCHEMA_VERSION;
use crate::logging::{debug, error, info, span, warn};
use crate::upcasting::UpcasterChain;

/// Classic mutable-row [`eventually::aggregate::Repository`] for SQL databases.
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
            "aggregate repository initialised"
        );

        Ok(Self {
            pool,
            aggregate_serde,
            event_serde,
            backend,
            schema_version: DEFAULT_SCHEMA_VERSION,
            upcaster_chain: Arc::new(UpcasterChain::new()),
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
}

// ── Internal helpers ──────────────────────────────────────────────────────

impl<T, Serde, EvtSerde> Repository<T, Serde, EvtSerde>
where
    T: Aggregate + Send + Sync,
    <T as Aggregate>::Id: ToString,
    Serde: serde::Serde<T> + Send + Sync,
    EvtSerde: serde::Serde<T::Event> + Send + Sync,
{
    fn type_col(&self) -> &'static str {
        if self.backend == "MySQL" {
            "`type`"
        } else {
            r#""type""#
        }
    }

    fn ph(&self, n: usize) -> String {
        if self.backend == "MySQL" {
            "?".to_owned()
        } else {
            format!("${n}")
        }
    }

    async fn save_aggregate_state(
        &self,
        tx: &mut sqlx::Transaction<'_, Any>,
        aggregate_id: &str,
        expected_version: Version,
        root: &mut aggregate::Root<T>,
    ) -> Result<(), aggregate::repository::SaveError> {
        let bytes_state = self
            .aggregate_serde
            .serialize(root.to_aggregate_type::<T>())
            .map_err(|err| {
                aggregate::repository::SaveError::Internal(anyhow!(
                    "failed to serialize aggregate root state: {}",
                    err
                ))
            })?;

        let type_col = self.type_col();
        let (p1, p2, p3, p4, p5) = (self.ph(1), self.ph(2), self.ph(3), self.ph(4), self.ph(5));

        let select_query = format!(
            "SELECT version FROM aggregates WHERE aggregate_id = {p1} AND {type_col} = {p2}",
        );

        let current_version_row = sqlx::query(&select_query)
            .bind(aggregate_id)
            .bind(T::type_name())
            .fetch_optional(&mut **tx)
            .await
            .map_err(|err| {
                aggregate::repository::SaveError::Internal(anyhow!(
                    "failed to fetch current aggregate version: {}",
                    err
                ))
            })?;

        let actual_version: i32 = current_version_row
            .map(|row: sqlx::any::AnyRow| row.try_get("version").unwrap_or(0))
            .unwrap_or(0);

        if actual_version != expected_version as i32 {
            warn!(
                aggregate_id = aggregate_id,
                aggregate_type = T::type_name(),
                expected = expected_version,
                actual = actual_version,
                "save conflict"
            );
            return Err(aggregate::repository::SaveError::Conflict(
                version::ConflictError {
                    expected: expected_version,
                    actual: actual_version as Version,
                },
            ));
        }

        if expected_version == 0 {
            let stream_insert =
                format!("INSERT INTO event_streams (event_stream_id, version) VALUES ({p1}, {p2})");
            if let Err(err) = sqlx::query(&stream_insert)
                .bind(aggregate_id)
                .bind(root.version() as i32)
                .execute(&mut **tx)
                .await
            {
                let is_dup = err.as_database_error().map_or(false, |e| {
                    let code = e.code().unwrap_or_default();
                    code == "23505" || code == "1062" || code == "23000" || code == "2067"
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

            let agg_insert = format!(
                "INSERT INTO aggregates (aggregate_id, {type_col}, version, state)
                 VALUES ({p1}, {p2}, {p3}, {p4})"
            );
            sqlx::query(&agg_insert)
                .bind(aggregate_id)
                .bind(T::type_name())
                .bind(root.version() as i32)
                .bind(bytes_state)
                .execute(&mut **tx)
                .await
                .map_err(|err| {
                    aggregate::repository::SaveError::Internal(anyhow!(
                        "failed to insert aggregate: {}",
                        err
                    ))
                })?;
        } else {
            let stream_update = format!(
                "UPDATE event_streams SET version = {p1}
                 WHERE event_stream_id = {p2} AND version = {p3}"
            );
            match sqlx::query(&stream_update)
                .bind(root.version() as i32)
                .bind(aggregate_id)
                .bind(expected_version as i32)
                .execute(&mut **tx)
                .await
            {
                Ok(res) if res.rows_affected() == 0 => {
                    let actual_row = sqlx::query(&select_query)
                        .bind(aggregate_id)
                        .bind(T::type_name())
                        .fetch_optional(&mut **tx)
                        .await
                        .map_err(|err| {
                            aggregate::repository::SaveError::Internal(anyhow!(
                                "failed to fetch actual version: {}",
                                err
                            ))
                        })?;
                    let actual: i32 = actual_row
                        .map(|row: sqlx::any::AnyRow| row.try_get("version").unwrap_or(0))
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

            let agg_update = format!(
                "UPDATE aggregates SET version = {p1}, state = {p2}
                 WHERE aggregate_id = {p3} AND {type_col} = {p4} AND version = {p5}"
            );
            sqlx::query(&agg_update)
                .bind(root.version() as i32)
                .bind(bytes_state)
                .bind(aggregate_id)
                .bind(T::type_name())
                .bind(expected_version as i32)
                .execute(&mut **tx)
                .await
                .map_err(|err| {
                    aggregate::repository::SaveError::Internal(anyhow!(
                        "failed to update aggregate: {}",
                        err
                    ))
                })?;
        }

        Ok(())
    }
}

// ── Getter ────────────────────────────────────────────────────────────────

#[async_trait]
impl<T, Serde, EvtSerde> aggregate::repository::Getter<T> for Repository<T, Serde, EvtSerde>
where
    T: Aggregate + Send + Sync,
    <T as Aggregate>::Id: ToString,
    Serde: serde::Serde<T> + Send + Sync,
    EvtSerde: serde::Serde<T::Event> + Send + Sync,
{
    async fn get(&self, id: &T::Id) -> Result<aggregate::Root<T>, aggregate::repository::GetError> {
        let aggregate_id = id.to_string();
        let type_col = self.type_col();
        let (p1, p2) = (self.ph(1), self.ph(2));

        let _span = span!(
            "aggregate_repository::get",
            aggregate_id = %aggregate_id,
            aggregate_type = T::type_name()
        );

        info!(
            aggregate_id = %aggregate_id,
            aggregate_type = T::type_name(),
            "loading aggregate"
        );

        let query_str = format!(
            "SELECT version, state FROM aggregates
             WHERE aggregate_id = {p1} AND {type_col} = {p2}"
        );

        let row = sqlx::query(&query_str)
            .bind(&aggregate_id)
            .bind(T::type_name())
            .fetch_one(&self.pool)
            .await
            .map_err(|err| match err {
                sqlx::Error::RowNotFound => {
                    warn!(
                        aggregate_id = %aggregate_id,
                        aggregate_type = T::type_name(),
                        "aggregate not found"
                    );
                    aggregate::repository::GetError::NotFound
                }
                _ => {
                    error!(
                        aggregate_id = %aggregate_id,
                        aggregate_type = T::type_name(),
                        error = %err,
                        "database error loading aggregate"
                    );
                    aggregate::repository::GetError::Internal(anyhow!(
                        "failed to fetch aggregate state row: {}",
                        err
                    ))
                }
            })?;

        let version: i32 = row.try_get("version").map_err(|err| {
            aggregate::repository::GetError::Internal(anyhow!(
                "failed to get 'version' column: {}",
                err
            ))
        })?;

        let bytes_state: Vec<u8> = row.try_get("state").map_err(|err| {
            aggregate::repository::GetError::Internal(anyhow!(
                "failed to get 'state' column: {}",
                err
            ))
        })?;

        let aggregate: T = self
            .aggregate_serde
            .deserialize(&bytes_state)
            .map_err(|err| {
                aggregate::repository::GetError::Internal(anyhow!(
                    "failed to deserialize aggregate state: {}",
                    err
                ))
            })?;

        debug!(
            aggregate_id = %aggregate_id,
            aggregate_type = T::type_name(),
            version = version,
            "aggregate loaded"
        );

        #[allow(clippy::cast_sign_loss)]
        Ok(aggregate::Root::rehydrate_from_state(
            version as Version,
            aggregate,
        ))
    }
}

// ── Saver ─────────────────────────────────────────────────────────────────

#[async_trait]
impl<T, Serde, EvtSerde> aggregate::repository::Saver<T> for Repository<T, Serde, EvtSerde>
where
    T: Aggregate + Send + Sync,
    <T as Aggregate>::Id: ToString,
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

        let _span = span!(
            "aggregate_repository::save",
            aggregate_id = %aggregate_id,
            aggregate_type = T::type_name(),
            events = event_count
        );

        info!(
            aggregate_id = %aggregate_id,
            aggregate_type = T::type_name(),
            events = event_count,
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

        let expected_root_version = root.version() - (events_to_commit.len() as Version);

        self.save_aggregate_state(&mut tx, &aggregate_id, expected_root_version, root)
            .await?;

        #[allow(clippy::cast_possible_truncation)]
        crate::event::append_domain_events(
            &mut tx,
            &self.event_serde,
            &aggregate_id,
            root.version() as i32,
            self.schema_version,
            events_to_commit,
        )
        .await
        .map_err(|err| {
            error!(
                aggregate_id = %aggregate_id,
                error = %err,
                "failed to append aggregate events"
            );
            aggregate::repository::SaveError::Internal(anyhow!(
                "failed to append aggregate events: {}",
                err
            ))
        })?;

        tx.commit().await.map_err(|err| {
            error!(
                aggregate_id = %aggregate_id,
                error = %err,
                "failed to commit save transaction"
            );
            aggregate::repository::SaveError::Internal(anyhow!(
                "failed to commit transaction: {}",
                err
            ))
        })?;

        debug!(
            aggregate_id = %aggregate_id,
            aggregate_type = T::type_name(),
            new_version = root.version(),
            events = event_count,
            "aggregate saved successfully"
        );

        Ok(())
    }
}
