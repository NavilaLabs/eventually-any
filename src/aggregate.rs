//! This module contains the implementation of the [`eventually::aggregate::Repository`] trait,
//! to work specifically with databases using `sqlx::Any`.
//!
//! Check out the [Repository] type for more information.

use std::marker::PhantomData;

use anyhow::anyhow;
use async_trait::async_trait;
use eventually::aggregate::Aggregate;
use eventually::version::Version;
use eventually::{aggregate, serde, version};
use sqlx::{Any, AnyPool, Row};

/// Implements the [`eventually::aggregate::Repository`] trait for
/// SQL databases.
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
    pub async fn new(
        pool: AnyPool,
        aggregate_serde: Serde,
        event_serde: EvtSerde,
    ) -> Result<Self, sqlx::migrate::MigrateError> {
        let backend = pool
            .acquire()
            .await
            .map(|conn| conn.backend_name().to_string())
            .unwrap_or_else(|_| "Unknown".to_string());

        #[cfg(all(feature = "postgres", feature = "migrations"))]
        if &backend == "PostgreSQL" {
            crate::MIGRATIONS_POSTGRES.run(&pool).await?;
        }
        #[cfg(all(feature = "sqlite", feature = "migrations"))]
        if &backend == "SQLite" {
            crate::MIGRATIONS_SQLITE.run(&pool).await?;
        }
        #[cfg(all(feature = "mysql", feature = "migrations"))]
        if &backend == "MySQL" {
            crate::MIGRATIONS_MYSQL.run(&pool).await?;
        }

        Ok(Self {
            pool,
            aggregate_serde,
            event_serde,
            backend,
            t: PhantomData,
        })
    }
}

impl<T, Serde, EvtSerde> Repository<T, Serde, EvtSerde>
where
    T: Aggregate + Send + Sync,
    <T as Aggregate>::Id: ToString,
    Serde: serde::Serde<T> + Send + Sync,
    EvtSerde: serde::Serde<T::Event> + Send + Sync,
{
    async fn save_aggregate_state(
        &self,
        tx: &mut sqlx::Transaction<'_, Any>,
        aggregate_id: &str,
        expected_version: Version,
        root: &mut aggregate::Root<T>,
    ) -> Result<(), aggregate::repository::SaveError> {
        let out_state = root.to_aggregate_type::<T>();
        let bytes_state = self.aggregate_serde.serialize(out_state).map_err(|err| {
            aggregate::repository::SaveError::Internal(anyhow!(
                "failed to serialize aggregate root state: {}",
                err
            ))
        })?;

        let type_col = if self.backend == "MySQL" {
            "`type`"
        } else {
            r#""type""#
        };

        let select_query: String = if self.backend == "MySQL" {
            format!(
                "SELECT version FROM aggregates WHERE aggregate_id = ? AND {} = ?",
                type_col
            )
        } else {
            format!(
                "SELECT version FROM aggregates WHERE aggregate_id = $1 AND {} = $2",
                type_col
            )
        };

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

        if expected_version == 0 {
            let stream_query = if self.backend == "MySQL" {
                "INSERT INTO event_streams (event_stream_id, version) VALUES (?, ?)"
            } else {
                "INSERT INTO event_streams (event_stream_id, version) VALUES ($1, $2)"
            };
            let stream_res = sqlx::query(stream_query)
                .bind(aggregate_id)
                .bind(root.version() as i32)
                .execute(&mut **tx)
                .await;

            if let Err(err) = stream_res {
                let is_conflict = err.as_database_error().map_or(false, |e| {
                    let code = e.code().unwrap_or_default();
                    code == "23505"
                        || code == "1062"
                        || code == "23000"
                        || code == "2067"
                        || code == "40001"
                });

                if is_conflict {
                    return Err(aggregate::repository::SaveError::Conflict(
                        version::ConflictError {
                            expected: expected_version,
                            actual: expected_version + 1,
                        },
                    ));
                } else {
                    return Err(aggregate::repository::SaveError::Internal(anyhow!(
                        "failed to insert event stream: {}",
                        err
                    )));
                }
            }

            let insert_query: String = if self.backend == "MySQL" {
                format!(
                    "INSERT INTO aggregates (aggregate_id, {}, version, state) VALUES (?, ?, ?, ?)",
                    type_col
                )
            } else {
                format!(
                    "INSERT INTO aggregates (aggregate_id, {}, version, state) VALUES ($1, $2, $3, $4)",
                    type_col
                )
            };
            let res = sqlx::query(&insert_query)
                .bind(aggregate_id)
                .bind(T::type_name())
                .bind(root.version() as i32)
                .bind(bytes_state)
                .execute(&mut **tx)
                .await;

            if let Err(err) = res {
                return Err(aggregate::repository::SaveError::Internal(anyhow!(
                    "failed to insert aggregate: {}",
                    err
                )));
            }
        } else {
            let stream_update = if self.backend == "MySQL" {
                "UPDATE event_streams SET version = ? WHERE event_stream_id = ? AND version = ?"
            } else {
                "UPDATE event_streams SET version = $1 WHERE event_stream_id = $2 AND version = $3"
            };
            let stream_res = sqlx::query(stream_update)
                .bind(root.version() as i32)
                .bind(aggregate_id)
                .bind(expected_version as i32)
                .execute(&mut **tx)
                .await;

            match stream_res {
                Ok(result) => {
                    if result.rows_affected() == 0 {
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
                }
                Err(err) => {
                    let is_conflict = err
                        .as_database_error()
                        .map_or(false, |e| e.code().unwrap_or_default() == "40001");

                    if is_conflict {
                        return Err(aggregate::repository::SaveError::Conflict(
                            version::ConflictError {
                                expected: expected_version,
                                actual: expected_version + 1,
                            },
                        ));
                    } else {
                        return Err(aggregate::repository::SaveError::Internal(anyhow!(
                            "failed to update event stream: {}",
                            err
                        )));
                    }
                }
            }

            let update_query: String = if self.backend == "MySQL" {
                format!(
                    "UPDATE aggregates SET version = ?, state = ? WHERE aggregate_id = ? AND {} = ? AND version = ?",
                    type_col
                )
            } else {
                format!(
                    "UPDATE aggregates SET version = $1, state = $2 WHERE aggregate_id = $3 AND {} = $4 AND version = $5",
                    type_col
                )
            };
            let res = sqlx::query(&update_query)
                .bind(root.version() as i32)
                .bind(bytes_state)
                .bind(aggregate_id)
                .bind(T::type_name())
                .bind(expected_version as i32)
                .execute(&mut **tx)
                .await;

            if let Err(err) = res {
                return Err(aggregate::repository::SaveError::Internal(anyhow!(
                    "failed to update aggregate: {}",
                    err
                )));
            }
        }

        Ok(())
    }
}

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

        let type_col = if self.backend == "MySQL" {
            "`type`"
        } else {
            r#""type""#
        };
        let query_str = if self.backend == "MySQL" {
            format!(
                "SELECT version, state FROM aggregates WHERE aggregate_id = ? AND {} = ?",
                type_col
            )
        } else {
            format!(
                "SELECT version, state FROM aggregates WHERE aggregate_id = $1 AND {} = $2",
                type_col
            )
        };

        let row = sqlx::query(&query_str)
            .bind(&aggregate_id)
            .bind(T::type_name())
            .fetch_one(&self.pool)
            .await
            .map_err(|err| match err {
                sqlx::Error::RowNotFound => aggregate::repository::GetError::NotFound,
                _ => aggregate::repository::GetError::Internal(anyhow!(
                    "failed to fetch the aggregate state row: {}",
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
                    "failed to deserialize state: {}",
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
