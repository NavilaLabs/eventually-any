use eventually::event::store::{Appender, Streamer};
use eventually::event::{Envelope, VersionSelect};
use eventually::message::Message;
use eventually::version;
use eventually_any::event::Store;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use sqlx::any::install_default_drivers;

/// 1. Define your domain event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum UserEvent {
    Created { name: String },
    Updated { new_name: String },
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
    // 2. Install drivers for Postgres, MySQL, and SQLite
    install_default_drivers();

    // 3. Pick up the database connection URL from the environment,
    //    or default to an in-memory SQLite database!
    let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite::memory:".to_string());
    println!("🔌 Connecting to database: {}", db_url);

    // 4. Connect to the database
    // Note: We limit connections to 1 for `sqlite::memory:` so the tables aren't lost between queries.
    // For a real Postgres/MySQL URL, the pool handles it normally.
    let pool = sqlx::any::AnyPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await?;

    // 5. Initialize the Event Store (this automatically runs migrations)
    let store = Store::new(pool, eventually::serde::Json::<UserEvent>::default()).await?;

    let stream_id = "user-123".to_string();

    // 6. Append events to a new stream
    println!("📝 Appending events to stream '{}'...", stream_id);
    let new_version = store
        .append(
            stream_id.clone(),
            version::Check::MustBe(0),
            vec![
                Envelope::from(UserEvent::Created {
                    name: "Alice".to_string(),
                }),
                Envelope::from(UserEvent::Updated {
                    new_name: "Alice Bob".to_string(),
                }),
            ],
        )
        .await?;

    println!("✅ Stream is now at version {}", new_version);

    // 7. Stream the events back
    println!("🔍 Reading events back:");
    let events: Vec<_> = store
        .stream(&stream_id, VersionSelect::All)
        .try_collect()
        .await?;

    for persisted in events {
        println!("   - Version {}: {:?}", persisted.version, persisted);
    }

    Ok(())
}
