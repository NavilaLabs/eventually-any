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

use std::marker::PhantomData;

use anyhow::anyhow;
use async_trait::async_trait;
use eventually::aggregate::Aggregate;
use eventually::version::Version;
use eventually::{aggregate, serde, version};
use sqlx::{Any, AnyPool, Row};

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
    /// Returns the correct SQL column quotation for `type` per backend.
    fn type_col(&self) -> &'static str {
        if self.backend == "MySQL" {
            "`type`"
        } else {
            r#""type""#
        }
    }

    /// Returns `?` (MySQL) or `$N` (Postgres/SQLite) placeholder style.
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

        // ── Read current version inside the transaction ───────────────────
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
            return Err(aggregate::repository::SaveError::Conflict(
                version::ConflictError {
                    expected: expected_version,
                    actual: actual_version as Version,
                },
            ));
        }

        // ── First save: INSERT both event_stream and aggregate rows ───────
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

        // ── Subsequent saves: UPDATE both rows ────────────────────────────
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
                    // Another writer updated the version concurrently.
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
                sqlx::Error::RowNotFound => aggregate::repository::GetError::NotFound,
                _ => aggregate::repository::GetError::Internal(anyhow!(
                    "failed to fetch aggregate state row: {}",
                    err
                )),
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
        let expected_root_version = root.version() - (events_to_commit.len() as Version);

        self.save_aggregate_state(&mut tx, &aggregate_id, expected_root_version, root)
            .await?;

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
                "failed to append aggregate events: {}",
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
