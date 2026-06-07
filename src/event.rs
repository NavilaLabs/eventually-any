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

use crate::backend::{Backend, is_conflict_error_code};
use crate::upcasting::UpcasterChain;
#[cfg(feature = "tracing")]
use tracing::{debug, error, info, info_span as span, warn};

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

/// The default schema version written for new events when none is specified.
pub const DEFAULT_SCHEMA_VERSION: u32 = 1;

pub(crate) async fn append_domain_event<Evt>(
    transaction: &mut Transaction<'_, Any>,
    backend: &Backend,
    serializer: &impl serde::Serializer<Evt>,
    event_stream_id: &str,
    event_version: i32,
    new_event_stream_version: i32,
    schema_version: u32,
    envelope: event::Envelope<Evt>,
) -> anyhow::Result<()>
where
    Evt: Message,
{
    let event_type = envelope.message.name();
    let mut metadata = envelope.metadata;
    let serialized_event = serializer
        .serialize(envelope.message)
        .map_err(|err| anyhow!("failed to serialize event message: {}", err))?;

    metadata.insert("recorded-at".to_owned(), Utc::now().to_rfc3339());
    metadata.insert(
        "recorded-with-new-version".to_owned(),
        new_event_stream_version.to_string(),
    );
    metadata.insert("schema-version".to_owned(), schema_version.to_string());

    let metadata_json = serde_json::to_string(&metadata).unwrap();

    let sql = match backend {
        Backend::Postgres => {
            r#"INSERT INTO events (event_stream_id, "type", "version", schema_version, event, metadata)
               VALUES ($1, $2, $3, $4, $5, CAST($6 AS jsonb))"#
        }
        Backend::MySQL => {
            r"INSERT INTO events (event_stream_id, `type`, `version`, schema_version, event, metadata)
              VALUES (?, ?, ?, ?, ?, ?)"
        }
        Backend::Sqlite => {
            r#"INSERT INTO events (event_stream_id, "type", "version", schema_version, event, metadata)
               VALUES ($1, $2, $3, $4, $5, $6)"#
        }
    };

    sqlx::query(sql)
        .bind(event_stream_id)
        .bind(event_type)
        .bind(event_version)
        .bind(schema_version as i32)
        .bind(serialized_event)
        .bind(metadata_json)
        .execute(&mut **transaction)
        .await?;

    debug!(
        stream_id = event_stream_id,
        event_type = event_type,
        version = event_version,
        schema_version = schema_version,
        "event row inserted"
    );

    Ok(())
}

pub(crate) async fn append_domain_events<Evt>(
    transaction: &mut Transaction<'_, Any>,
    backend: &Backend,
    serializer: &impl serde::Serializer<Evt>,
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

    for (index, envelope) in events.into_iter().enumerate() {
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let event_version = current_event_stream_version + (index as i32) + 1;

        append_domain_event(
            transaction,
            backend,
            serializer,
            event_stream_id,
            event_version,
            new_version,
            schema_version,
            envelope,
        )
        .await?;
    }

    Ok(())
}

/// Outcome returned by [`upsert_event_stream`].
pub(crate) enum StreamUpsertOutcome {
    Success,
    Conflict(version::ConflictError),
}

/// Insert (first save) or update (subsequent saves) the `event_streams` row,
/// using `expected_version` as an optimistic lock.
///
/// Returns [`StreamUpsertOutcome::Conflict`] when another writer has already
/// advanced the stream past `expected_version`.  Returns an error only for
/// unexpected database failures.
pub(crate) async fn upsert_event_stream(
    transaction: &mut Transaction<'_, Any>,
    backend: &Backend,
    aggregate_id: &str,
    expected_version: Version,
    new_version: i32,
) -> anyhow::Result<StreamUpsertOutcome> {
    let placeholder_1 = backend.placeholder(1);
    let placeholder_2 = backend.placeholder(2);

    if expected_version == 0 {
        let insert_sql = format!(
            "INSERT INTO event_streams (event_stream_id, version) VALUES ({placeholder_1}, {placeholder_2})"
        );
        if let Err(err) = sqlx::query(sqlx::AssertSqlSafe(insert_sql))
            .bind(aggregate_id)
            .bind(new_version)
            .execute(&mut **transaction)
            .await
        {
            let is_duplicate = err
                .as_database_error()
                .is_some_and(|database_err| is_conflict_error_code(&database_err.code().unwrap_or_default()));

            if is_duplicate {
                warn!(
                    aggregate_id = aggregate_id,
                    expected = expected_version,
                    "save conflict (duplicate stream insert)"
                );
                return Ok(StreamUpsertOutcome::Conflict(version::ConflictError {
                    expected: expected_version,
                    actual: expected_version + 1,
                }));
            }
            return Err(anyhow!("failed to insert event stream: {}", err));
        }
    } else {
        let placeholder_3 = backend.placeholder(3);
        let update_sql = format!(
            "UPDATE event_streams SET version = {placeholder_1}
             WHERE event_stream_id = {placeholder_2} AND version = {placeholder_3}"
        );
        match sqlx::query(sqlx::AssertSqlSafe(update_sql))
            .bind(new_version)
            .bind(aggregate_id)
            .bind(expected_version as i32)
            .execute(&mut **transaction)
            .await
        {
            Ok(result) if result.rows_affected() == 0 => {
                let select_sql = format!(
                    "SELECT version FROM event_streams WHERE event_stream_id = {placeholder_1}"
                );
                let actual_version: i32 = sqlx::query(sqlx::AssertSqlSafe(select_sql))
                    .bind(aggregate_id)
                    .fetch_optional(&mut **transaction)
                    .await
                    .ok()
                    .flatten()
                    .and_then(|row| row.try_get("version").ok())
                    .unwrap_or(0);

                warn!(
                    aggregate_id = aggregate_id,
                    expected = expected_version,
                    actual = actual_version,
                    "save conflict (zero rows affected on stream update)"
                );
                return Ok(StreamUpsertOutcome::Conflict(version::ConflictError {
                    expected: expected_version,
                    actual: actual_version as Version,
                }));
            }
            Ok(_) => {}
            Err(err) => {
                let is_serialization_failure = err
                    .as_database_error()
                    .is_some_and(|database_err| database_err.code().unwrap_or_default() == "40001");

                if is_serialization_failure {
                    warn!(
                        aggregate_id = aggregate_id,
                        expected = expected_version,
                        "save conflict (serialization failure)"
                    );
                    return Ok(StreamUpsertOutcome::Conflict(version::ConflictError {
                        expected: expected_version,
                        actual: expected_version + 1,
                    }));
                }
                return Err(anyhow!("failed to update event stream: {}", err));
            }
        }
    }

    Ok(StreamUpsertOutcome::Success)
}

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
/// ## Tracing
///
/// When the `tracing` feature is enabled, every `append` and `stream` call
/// opens an `INFO`-level span.  Individual row inserts and conflict errors
/// are recorded at `DEBUG` and `WARN` respectively.
///
/// ### Configuring
///
/// ```rust,ignore
/// use eventually_any::event::Store;
/// use eventually_any::upcasting::{FnUpcaster, UpcasterChain};
/// use eventually::serde;
///
/// let chain = UpcasterChain::new()
///     .register(FnUpcaster::new("UserCreated", 1, 2, |mut payload| {
///         payload["full_name"] = payload["name"].clone();
///         payload.as_object_mut().unwrap().remove("name");
///         payload
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
    backend: Backend,
    schema_version: u32,
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
        let backend_name = pool
            .acquire()
            .await
            .map(|connection| connection.backend_name().to_string())
            .unwrap_or_default();

        let backend = Backend::from_name(&backend_name);

        crate::run_migrations(&pool).await?;

        info!(backend = %backend_name, "event store initialised");

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

    /// Build a streamer that skips migrations — used inside snapshot `get()`
    /// where migrations have already been run by the outer `Repository::new`.
    #[cfg(feature = "snapshots")]
    pub(crate) fn new_unchecked(
        pool: AnyPool,
        serde: &Serde,
        backend: Backend,
        schema_version: u32,
        upcaster_chain: Arc<UpcasterChain>,
    ) -> Self
    where
        Serde: Clone,
    {
        Self {
            pool,
            serde: serde.clone(),
            backend,
            schema_version,
            upcaster_chain,
            id_type: PhantomData,
            evt_type: PhantomData,
        }
    }
}

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

        let stored_schema_version: u32 = row
            .try_get::<i32, _>("schema_version")
            .map(|version| version as u32)
            .unwrap_or_else(|_| {
                try_get_column::<String>(row, "metadata")
                    .or_else(|_| {
                        try_get_column::<Vec<u8>>(row, "metadata")
                            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
                    })
                    .ok()
                    .and_then(|metadata_string| {
                        serde_json::from_str::<serde_json::Value>(&metadata_string).ok()
                    })
                    .and_then(|metadata_value| {
                        metadata_value
                            .get("schema-version")
                            .and_then(|schema_version| schema_version.as_str())
                            .and_then(|schema_version| schema_version.parse::<u32>().ok())
                    })
                    .unwrap_or(DEFAULT_SCHEMA_VERSION)
            });

        if stored_schema_version < self.schema_version || !self.upcaster_chain.is_empty() {
            if let Ok(json_payload) = serde_json::from_slice::<serde_json::Value>(&event_bytes) {
                let (upcasted_payload, new_schema_version) = self.upcaster_chain.apply(
                    &event_type_column,
                    stored_schema_version,
                    json_payload,
                );
                if new_schema_version != stored_schema_version {
                    debug!(
                        stream_id = %stream_id.to_string(),
                        event_type = %event_type_column,
                        from_schema_version = stored_schema_version,
                        to_schema_version = new_schema_version,
                        "event upcasted"
                    );
                }
                if let Ok(new_bytes) = serde_json::to_vec(&upcasted_payload) {
                    event_bytes = new_bytes;
                }
            }
        }

        let metadata_column: String = try_get_column(row, "metadata").or_else(|_| {
            try_get_column::<Vec<u8>>(row, "metadata")
                .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        })?;

        let metadata: Metadata = serde_json::from_str(&metadata_column)
            .map_err(|error| StreamError::DeserializeEvent(error.into()))?;

        let deserialized_event = self.serde.deserialize(&event_bytes).map_err(|error| {
            error!(
                stream_id = %stream_id.to_string(),
                event_type = %event_type_column,
                version = version_column,
                "failed to deserialize event"
            );
            StreamError::DeserializeEvent(error)
        })?;

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

impl<Id, Evt, Serde> event::store::Streamer<Id, Evt> for Store<Id, Evt, Serde>
where
    Id: ToString + Clone + Send + Sync,
    Evt: Message + Send + Sync,
    Serde: serde::Serde<Evt> + Send + Sync,
{
    type Error = StreamError;

    #[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
    fn stream(
        &self,
        id: &Id,
        select: event::VersionSelect,
    ) -> event::Stream<'_, Id, Evt, Self::Error> {
        #[allow(clippy::cast_possible_truncation)]
        let from_version: i32 = match select {
            event::VersionSelect::All => 0,
            event::VersionSelect::From(version) => version as i32,
        };

        let version_select_label = match select {
            event::VersionSelect::All => "All",
            event::VersionSelect::From(_) => "From",
        };

        info!(
            stream_id = %id.to_string(),
            select = version_select_label,
            from_version = from_version,
            "streaming events"
        );

        let sql = match self.backend {
            Backend::Postgres => {
                r#"SELECT version, "type", schema_version, event, CAST(metadata AS text) as metadata
                   FROM events
                   WHERE event_stream_id = $1 AND version >= $2
                   ORDER BY version"#
            }
            Backend::MySQL => {
                r"SELECT version, `type`, schema_version, event, CAST(metadata AS char) as metadata
                   FROM events
                   WHERE event_stream_id = ? AND version >= ?
                   ORDER BY version"
            }
            Backend::Sqlite => {
                r#"SELECT version, "type", schema_version, event, metadata
                   FROM events
                   WHERE event_stream_id = $1 AND version >= $2
                   ORDER BY version"#
            }
        };

        let id = id.clone();

        sqlx::query(sql)
            .bind(id.to_string())
            .bind(from_version)
            .fetch(&self.pool)
            .map_err(StreamError::Database)
            .and_then(move |row| ready(self.event_row_to_persisted_event(id.clone(), &row)))
            .boxed()
    }
}

#[async_trait]
impl<Id, Evt, Serde> event::store::Appender<Id, Evt> for Store<Id, Evt, Serde>
where
    Id: ToString + Clone + Send + Sync,
    Evt: Message + Send + Sync,
    Serde: serde::Serde<Evt> + Send + Sync,
{
    #[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
    async fn append(
        &self,
        id: Id,
        version_check: version::Check,
        events: Vec<event::Envelope<Evt>>,
    ) -> Result<Version, event::store::AppendError> {
        let string_id = id.to_string();
        let event_count = events.len();

        let version_check_label = match version_check {
            version::Check::Any => "Any".to_owned(),
            version::Check::MustBe(version) => format!("MustBe({version})"),
        };

        let _span = span!(
            "event_store::append",
            stream_id = %string_id,
            events = event_count,
            version_check = %version_check_label
        );

        info!(
            stream_id = %string_id,
            events = event_count,
            version_check = %version_check_label,
            "appending events"
        );

        let mut attempts = 0;

        let (mut transaction, new_version) = loop {
            attempts += 1;

            let mut transaction = self.pool.begin().await.map_err(|err| {
                error!(stream_id = %string_id, error = %err, "failed to begin transaction");
                event::store::AppendError::Internal(anyhow!("failed to begin transaction: {}", err))
            })?;

            if self.backend.requires_serializable_isolation() {
                sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE DEFERRABLE")
                    .execute(&mut *transaction)
                    .await
                    .map_err(|err| {
                        event::store::AppendError::Internal(anyhow!(
                            "failed to set transaction isolation level: {}",
                            err
                        ))
                    })?;
            }

            let select_sql = format!(
                "SELECT version FROM event_streams WHERE event_stream_id = {}",
                self.backend.placeholder(1)
            );

            let current_version_row = sqlx::query(sqlx::AssertSqlSafe(&*select_sql))
                .bind(&string_id)
                .fetch_optional(&mut *transaction)
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

            if let version::Check::MustBe(expected) = version_check {
                if current_version != expected as i32 {
                    warn!(
                        stream_id = %string_id,
                        expected = expected,
                        actual = current_version,
                        "append conflict"
                    );
                    return Err(event::store::AppendError::Conflict(
                        version::ConflictError {
                            expected,
                            actual: current_version as Version,
                        },
                    ));
                }
            }

            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            let new_version = current_version + events.len() as i32;

            let stream_result = if current_version == 0 {
                let insert_sql = format!(
                    "INSERT INTO event_streams (event_stream_id, version) VALUES ({}, {})",
                    self.backend.placeholder(1),
                    self.backend.placeholder(2)
                );
                sqlx::query(sqlx::AssertSqlSafe(insert_sql))
                    .bind(&string_id)
                    .bind(new_version)
                    .execute(&mut *transaction)
                    .await
            } else {
                let update_sql = format!(
                    "UPDATE event_streams SET version = {} WHERE event_stream_id = {} AND version = {}",
                    self.backend.placeholder(1),
                    self.backend.placeholder(2),
                    self.backend.placeholder(3)
                );
                sqlx::query(sqlx::AssertSqlSafe(update_sql))
                    .bind(new_version)
                    .bind(&string_id)
                    .bind(current_version)
                    .execute(&mut *transaction)
                    .await
            };

            match stream_result {
                Ok(result) => {
                    if current_version > 0 && result.rows_affected() == 0 {
                        if let version::Check::MustBe(expected) = version_check {
                            let actual_row = sqlx::query(sqlx::AssertSqlSafe(&*select_sql))
                                .bind(&string_id)
                                .fetch_optional(&mut *transaction)
                                .await
                                .unwrap_or(None);
                            let actual_version: i32 = actual_row
                                .map(|row| row.try_get("version").unwrap_or(0))
                                .unwrap_or(0);
                            warn!(
                                stream_id = %string_id,
                                expected = expected,
                                actual = actual_version,
                                "append conflict (zero rows affected)"
                            );
                            return Err(event::store::AppendError::Conflict(
                                version::ConflictError {
                                    expected,
                                    actual: actual_version as Version,
                                },
                            ));
                        } else if attempts < 3 {
                            debug!(stream_id = %string_id, attempt = attempts, "retrying append");
                            continue;
                        } else {
                            return Err(event::store::AppendError::Internal(anyhow!(
                                "failed to update event stream due to high concurrency"
                            )));
                        }
                    }
                    break (transaction, new_version);
                }
                Err(err) => {
                    let is_conflict = err
                        .as_database_error()
                        .is_some_and(|database_err| is_conflict_error_code(&database_err.code().unwrap_or_default()));

                    if is_conflict {
                        if let version::Check::MustBe(expected) = version_check {
                            warn!(
                                stream_id = %string_id,
                                expected = expected,
                                "append conflict (database constraint)"
                            );
                            return Err(event::store::AppendError::Conflict(
                                version::ConflictError {
                                    expected,
                                    actual: expected + 1,
                                },
                            ));
                        } else if attempts < 3 {
                            debug!(stream_id = %string_id, attempt = attempts, "retrying append after conflict");
                            continue;
                        } else {
                            return Err(event::store::AppendError::Internal(anyhow!(
                                "failed to append event stream after retries: {}",
                                err
                            )));
                        }
                    } else {
                        error!(stream_id = %string_id, error = %err, "database error during append");
                        return Err(event::store::AppendError::Internal(anyhow!(
                            "failed to append event stream: {}",
                            err
                        )));
                    }
                }
            }
        };

        append_domain_events(
            &mut transaction,
            &self.backend,
            &self.serde,
            &string_id,
            new_version,
            self.schema_version,
            events,
        )
        .await
        .map_err(|err| {
            error!(stream_id = %string_id, error = %err, "failed to write event rows");
            event::store::AppendError::Internal(anyhow!(
                "failed to append new domain events: {}",
                err
            ))
        })?;

        transaction.commit().await.map_err(|err| {
            error!(stream_id = %string_id, error = %err, "failed to commit append transaction");
            event::store::AppendError::Internal(anyhow!("failed to commit transaction: {}", err))
        })?;

        #[allow(clippy::cast_sign_loss)]
        let new_version = new_version as Version;

        debug!(
            stream_id = %string_id,
            new_version = new_version,
            events = event_count,
            "events appended successfully"
        );

        Ok(new_version)
    }
}
