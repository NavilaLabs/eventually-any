use std::marker::PhantomData;
use std::string::ToString;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use chrono::Utc;
use eventually::message::Message;
use eventually::message::Metadata;
use eventually::version::Version;
use eventually::{event, serde, version};
use futures::future::ready;
use futures::{StreamExt, TryStreamExt};
use sqlx::any::AnyRow;
use sqlx::{Any, AnyPool, Row, Transaction};

use crate::upcasting::UpcasterChain;

// ── Error types ───────────────────────────────────────────────────────────

/// Errors that can occur while streaming events from the database.
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    /// The raw bytes stored for an event could not be deserialized into the
    /// domain event type (e.g. schema mismatch, missing upcaster).
    #[error("failed to deserialize event from database: {0}")]
    DeserializeEvent(#[source] anyhow::Error),
    /// A required column was missing or had an unexpected type in the result row.
    #[error("failed to get column '{name}' from result row: {error}")]
    ReadColumn {
        /// Name of the column that could not be read.
        name: &'static str,
        /// The underlying sqlx error.
        #[source]
        error: sqlx::Error,
    },
    /// The database returned an error while fetching the event rows.
    #[error("db returned an error: {0}")]
    Database(#[source] sqlx::Error),
}

// ── Internal helpers ──────────────────────────────────────────────────────

/// The default schema version written for new events when none is specified.
pub const DEFAULT_SCHEMA_VERSION: u32 = 1;

pub(crate) async fn append_domain_event<Evt>(
    tx: &mut Transaction<'_, Any>,
    serde: &impl serde::Serializer<Evt>,
    event_stream_id: &str,
    event_version: i32,
    new_event_stream_version: i32,
    schema_version: u32,
    event: event::Envelope<Evt>,
) -> anyhow::Result<()>
where
    Evt: Message,
{
    let event_type = event.message.name();
    let mut metadata = event.metadata;
    let serialized_event = serde
        .serialize(event.message)
        .map_err(|err| anyhow!("failed to serialize event message: {}", err))?;

    metadata.insert("recorded-at".to_owned(), Utc::now().to_rfc3339());
    metadata.insert(
        "recorded-with-new-version".to_owned(),
        new_event_stream_version.to_string(),
    );
    // Embed the schema version in metadata so it survives without a dedicated
    // column on databases that don't support ALTER TABLE easily.  The column
    // is the source of truth when present; metadata is the fallback.
    metadata.insert("schema-version".to_owned(), schema_version.to_string());

    let metadata_string = serde_json::to_string(&metadata).unwrap();

    let backend = tx.backend_name();
    let query_str = if backend == "PostgreSQL" {
        r#"INSERT INTO events (event_stream_id, "type", "version", schema_version, event, metadata)
           VALUES ($1, $2, $3, $4, $5, CAST($6 AS jsonb))"#
    } else if backend == "MySQL" {
        r"INSERT INTO events (event_stream_id, `type`, `version`, schema_version, event, metadata)
          VALUES (?, ?, ?, ?, ?, ?)"
    } else {
        r#"INSERT INTO events (event_stream_id, "type", "version", schema_version, event, metadata)
           VALUES ($1, $2, $3, $4, $5, $6)"#
    };

    sqlx::query(query_str)
        .bind(event_stream_id)
        .bind(event_type)
        .bind(event_version)
        .bind(schema_version as i32)
        .bind(serialized_event)
        .bind(metadata_string)
        .execute(&mut **tx)
        .await?;

    Ok(())
}

pub(crate) async fn append_domain_events<Evt>(
    tx: &mut Transaction<'_, Any>,
    serde: &impl serde::Serializer<Evt>,
    event_stream_id: &str,
    new_version: i32,
    schema_version: u32,
    events: Vec<event::Envelope<Evt>>,
) -> anyhow::Result<()>
where
    Evt: Message,
{
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let current_event_stream_version = new_version - (events.len() as i32);

    for (i, evt) in events.into_iter().enumerate() {
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let event_version = current_event_stream_version + (i as i32) + 1;

        append_domain_event(
            tx,
            serde,
            event_stream_id,
            event_version,
            new_version,
            schema_version,
            evt,
        )
        .await?;
    }

    Ok(())
}

// ── Store ─────────────────────────────────────────────────────────────────

/// `sqlx::Any`-backed [`event::Store`] implementation.
///
/// Supports PostgreSQL, SQLite and MySQL transparently via the same pool.
///
/// ## Schema versioning
///
/// Every event row carries a `schema_version` integer column.  On read, the
/// [`UpcasterChain`] transforms any stored payload to the current schema
/// before deserialisation.  On write, the store stamps all new events with
/// [`Store::schema_version`] (default `1`).
///
/// ### Configuring
///
/// ```rust,ignore
/// use eventually_any::event::Store;
/// use eventually_any::upcasting::{FnUpcaster, UpcasterChain};
/// use eventually::serde;
///
/// let chain = UpcasterChain::new()
///     .register(FnUpcaster::new("UserCreated", 1, 2, |mut p| {
///         p["full_name"] = p["name"].clone();
///         p.as_object_mut().unwrap().remove("name");
///         p
///     }));
///
/// let store = Store::new(pool, serde::Json::<UserEvent>::default())
///     .await?
///     .with_schema_version(2)
///     .with_upcaster_chain(chain);
/// ```
#[derive(Debug, Clone)]
pub struct Store<Id, Evt, Serde>
where
    Id: ToString + Clone,
    Serde: serde::Serde<Evt>,
{
    pool: AnyPool,
    serde: Serde,
    backend: String,
    /// Schema version stamped on every newly-written event.
    schema_version: u32,
    /// Upcaster chain applied to events on read.
    upcaster_chain: Arc<UpcasterChain>,
    id_type: PhantomData<Id>,
    evt_type: PhantomData<Evt>,
}

impl<Id, Evt, Serde> Store<Id, Evt, Serde>
where
    Id: ToString + Clone,
    Serde: serde::Serde<Evt>,
{
    /// Run migrations (if the `migrations` feature is active) and return a
    /// new [`Store`] instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the migrations fail to run.
    pub async fn new(pool: AnyPool, serde: Serde) -> Result<Self, sqlx::migrate::MigrateError> {
        let backend = pool
            .acquire()
            .await
            .map(|c| c.backend_name().to_string())
            .unwrap_or_default();

        crate::run_migrations(&pool).await?;

        Ok(Self {
            pool,
            serde,
            backend,
            schema_version: DEFAULT_SCHEMA_VERSION,
            upcaster_chain: Arc::new(UpcasterChain::new()),
            id_type: PhantomData,
            evt_type: PhantomData,
        })
    }

    /// Set the schema version that will be written to **new** events.
    ///
    /// Increment this whenever you introduce a breaking change to an event's
    /// payload format and register a corresponding [`Upcaster`](crate::upcasting::Upcaster)
    /// via [`Self::with_upcaster_chain`].
    #[must_use]
    pub fn with_schema_version(mut self, version: u32) -> Self {
        self.schema_version = version;
        self
    }

    /// Attach an [`UpcasterChain`] that transforms stored events to the
    /// current schema on every read.
    #[must_use]
    pub fn with_upcaster_chain(mut self, chain: UpcasterChain) -> Self {
        self.upcaster_chain = Arc::new(chain);
        self
    }

    /// Returns the current write schema version.
    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    /// Create a [`Store`] that skips migrations.
    ///
    /// `pub(crate)` — used by [`crate::snapshot::Repository`] to build a
    /// lightweight streamer for delta-event replay inside `get()`, where
    /// migrations have already been run by the outer `Repository::new`.
    #[cfg(feature = "snapshots")]
    pub(crate) fn new_unchecked(
        pool: AnyPool,
        serde: &Serde,
        backend: &str,
        schema_version: u32,
        upcaster_chain: Arc<UpcasterChain>,
    ) -> Self
    where
        Serde: Clone,
    {
        Self {
            pool,
            serde: serde.clone(),
            backend: backend.to_owned(),
            schema_version,
            upcaster_chain,
            id_type: PhantomData,
            evt_type: PhantomData,
        }
    }
}

// ── Row helper ────────────────────────────────────────────────────────────

fn try_get_column<T>(row: &AnyRow, name: &'static str) -> Result<T, StreamError>
where
    for<'a> T: sqlx::Type<Any> + sqlx::Decode<'a, Any>,
{
    row.try_get(name)
        .map_err(|err| StreamError::ReadColumn { name, error: err })
}

impl<Id, Evt, Serde> Store<Id, Evt, Serde>
where
    Id: ToString + Clone + Send + Sync,
    Evt: Message + Send + Sync,
    Serde: serde::Serde<Evt> + Send + Sync,
{
    fn event_row_to_persisted_event(
        &self,
        stream_id: Id,
        row: &AnyRow,
    ) -> Result<event::Persisted<Id, Evt>, StreamError> {
        let version_column: i32 = try_get_column(row, "version")?;
        let event_type_column: String = try_get_column(row, "type")?;
        let mut event_bytes: Vec<u8> = try_get_column(row, "event")?;

        // ── Resolve schema_version ─────────────────────────────────────────
        // Primary source: the dedicated `schema_version` column.
        // Fallback: the `schema-version` key embedded in `metadata` (written
        // by older rows that pre-date the column, or in case of a migration).
        let stored_schema_version: u32 = row
            .try_get::<i32, _>("schema_version")
            .map(|v| v as u32)
            .unwrap_or_else(|_| {
                // Try to recover from metadata field
                try_get_column::<String>(row, "metadata")
                    .or_else(|_| {
                        try_get_column::<Vec<u8>>(row, "metadata")
                            .map(|b| String::from_utf8_lossy(&b).into_owned())
                    })
                    .ok()
                    .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                    .and_then(|v| {
                        v.get("schema-version")
                            .and_then(|sv| sv.as_str())
                            .and_then(|sv| sv.parse::<u32>().ok())
                    })
                    .unwrap_or(DEFAULT_SCHEMA_VERSION)
            });

        // ── Apply upcaster chain ───────────────────────────────────────────
        if stored_schema_version < self.schema_version || !self.upcaster_chain.is_empty() {
            // Parse the raw bytes as JSON, upcast, re-serialise.
            // This is only done when there are upcasters or the version is old.
            if let Ok(json_payload) = serde_json::from_slice::<serde_json::Value>(&event_bytes) {
                let (upcasted_payload, _new_version) = self.upcaster_chain.apply(
                    &event_type_column,
                    stored_schema_version,
                    json_payload,
                );
                // Re-encode as bytes for the domain deserialiser.
                if let Ok(new_bytes) = serde_json::to_vec(&upcasted_payload) {
                    event_bytes = new_bytes;
                }
            }
        }

        // Metadata is stored as JSONB (Postgres), JSON (MySQL) or TEXT (SQLite).
        let metadata_column: String = try_get_column(row, "metadata").or_else(|_| {
            try_get_column::<Vec<u8>>(row, "metadata")
                .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        })?;

        let metadata: Metadata = serde_json::from_str(&metadata_column)
            .map_err(|e| StreamError::DeserializeEvent(e.into()))?;

        let deserialized_event = self
            .serde
            .deserialize(&event_bytes)
            .map_err(StreamError::DeserializeEvent)?;

        #[allow(clippy::cast_sign_loss)]
        Ok(event::Persisted {
            stream_id,
            version: version_column as Version,
            event: event::Envelope {
                message: deserialized_event,
                metadata,
            },
        })
    }
}

// ── Streamer ──────────────────────────────────────────────────────────────

impl<Id, Evt, Serde> event::store::Streamer<Id, Evt> for Store<Id, Evt, Serde>
where
    Id: ToString + Clone + Send + Sync,
    Evt: Message + Send + Sync,
    Serde: serde::Serde<Evt> + Send + Sync,
{
    type Error = StreamError;

    fn stream(
        &self,
        id: &Id,
        select: event::VersionSelect,
    ) -> event::Stream<'_, Id, Evt, Self::Error> {
        #[allow(clippy::cast_possible_truncation)]
        let from_version: i32 = match select {
            event::VersionSelect::All => 0,
            event::VersionSelect::From(v) => v as i32,
        };

        let query_str = match self.backend.as_str() {
            "PostgreSQL" => {
                r#"SELECT version, "type", schema_version, event, CAST(metadata AS text) as metadata
                   FROM events
                   WHERE event_stream_id = $1 AND version >= $2
                   ORDER BY version"#
            }
            "MySQL" => {
                r"SELECT version, `type`, schema_version, event, CAST(metadata AS char) as metadata
                   FROM events
                   WHERE event_stream_id = ? AND version >= ?
                   ORDER BY version"
            }
            _ => {
                r#"SELECT version, "type", schema_version, event, metadata
                   FROM events
                   WHERE event_stream_id = $1 AND version >= $2
                   ORDER BY version"#
            }
        };

        let id = id.clone();

        sqlx::query(query_str)
            .bind(id.to_string())
            .bind(from_version)
            .fetch(&self.pool)
            .map_err(StreamError::Database)
            .and_then(move |row| ready(self.event_row_to_persisted_event(id.clone(), &row)))
            .boxed()
    }
}

// ── Appender ──────────────────────────────────────────────────────────────

#[async_trait]
impl<Id, Evt, Serde> event::store::Appender<Id, Evt> for Store<Id, Evt, Serde>
where
    Id: ToString + Clone + Send + Sync,
    Evt: Message + Send + Sync,
    Serde: serde::Serde<Evt> + Send + Sync,
{
    async fn append(
        &self,
        id: Id,
        version_check: version::Check,
        events: Vec<event::Envelope<Evt>>,
    ) -> Result<Version, event::store::AppendError> {
        let string_id = id.to_string();
        let mut attempts = 0;

        let (mut tx, new_version) = loop {
            attempts += 1;

            let mut tx = self.pool.begin().await.map_err(|err| {
                event::store::AppendError::Internal(anyhow!("failed to begin transaction: {}", err))
            })?;

            if tx.backend_name() == "PostgreSQL" {
                sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE DEFERRABLE")
                    .execute(&mut *tx)
                    .await
                    .map_err(|err| {
                        event::store::AppendError::Internal(anyhow!(
                            "failed to set transaction level: {}",
                            err
                        ))
                    })?;
            }

            let select_query = if self.backend == "MySQL" {
                "SELECT version FROM event_streams WHERE event_stream_id = ?"
            } else {
                "SELECT version FROM event_streams WHERE event_stream_id = $1"
            };

            let current_version_row = sqlx::query(select_query)
                .bind(&string_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|err| {
                    event::store::AppendError::Internal(anyhow!(
                        "failed to fetch current stream version: {}",
                        err
                    ))
                })?;

            let current_version: i32 = current_version_row
                .map(|row| row.try_get("version").unwrap_or(0))
                .unwrap_or(0);

            if let version::Check::MustBe(v) = version_check {
                if current_version != v as i32 {
                    return Err(event::store::AppendError::Conflict(
                        version::ConflictError {
                            expected: v,
                            actual: current_version as Version,
                        },
                    ));
                }
            }

            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            let new_version = current_version + events.len() as i32;

            let stream_res = if current_version == 0 {
                let insert_query = if self.backend == "MySQL" {
                    "INSERT INTO event_streams (event_stream_id, version) VALUES (?, ?)"
                } else {
                    "INSERT INTO event_streams (event_stream_id, version) VALUES ($1, $2)"
                };
                sqlx::query(insert_query)
                    .bind(&string_id)
                    .bind(new_version)
                    .execute(&mut *tx)
                    .await
            } else {
                let update_query = if self.backend == "MySQL" {
                    "UPDATE event_streams SET version = ? WHERE event_stream_id = ? AND version = ?"
                } else {
                    "UPDATE event_streams SET version = $1 WHERE event_stream_id = $2 AND version = $3"
                };
                sqlx::query(update_query)
                    .bind(new_version)
                    .bind(&string_id)
                    .bind(current_version)
                    .execute(&mut *tx)
                    .await
            };

            match stream_res {
                Ok(res) => {
                    if current_version > 0 && res.rows_affected() == 0 {
                        if let version::Check::MustBe(v) = version_check {
                            let actual_row = sqlx::query(select_query)
                                .bind(&string_id)
                                .fetch_optional(&mut *tx)
                                .await
                                .unwrap_or(None);
                            let actual: i32 = actual_row
                                .map(|row| row.try_get("version").unwrap_or(0))
                                .unwrap_or(0);
                            return Err(event::store::AppendError::Conflict(
                                version::ConflictError {
                                    expected: v,
                                    actual: actual as Version,
                                },
                            ));
                        } else if attempts < 3 {
                            continue;
                        } else {
                            return Err(event::store::AppendError::Internal(anyhow!(
                                "failed to update event stream due to high concurrency"
                            )));
                        }
                    }
                    break (tx, new_version);
                }
                Err(err) => {
                    let is_conflict = err.as_database_error().map_or(false, |e| {
                        let code = e.code().unwrap_or_default();
                        code == "23505"
                            || code == "1062"
                            || code == "2067"
                            || code == "40001"
                            || code == "23000"
                    });

                    if is_conflict {
                        if let version::Check::MustBe(v) = version_check {
                            return Err(event::store::AppendError::Conflict(
                                version::ConflictError {
                                    expected: v,
                                    actual: v + 1,
                                },
                            ));
                        } else if attempts < 3 {
                            continue;
                        } else {
                            return Err(event::store::AppendError::Internal(anyhow!(
                                "failed to append event stream after retries: {}",
                                err
                            )));
                        }
                    } else {
                        return Err(event::store::AppendError::Internal(anyhow!(
                            "failed to append event stream: {}",
                            err
                        )));
                    }
                }
            }
        };

        append_domain_events(
            &mut tx,
            &self.serde,
            &string_id,
            new_version,
            self.schema_version,
            events,
        )
        .await
        .map_err(|err| {
            event::store::AppendError::Internal(anyhow!(
                "failed to append new domain events: {}",
                err
            ))
        })?;

        tx.commit().await.map_err(|err| {
            event::store::AppendError::Internal(anyhow!("failed to commit transaction: {}", err))
        })?;

        #[allow(clippy::cast_sign_loss)]
        Ok(new_version as Version)
    }
}
