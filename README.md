# eventually-any

Database-agnostic Event Store and Aggregate Repository for the
[eventually](https://github.com/get-eventually/eventually-rs) Event Sourcing
framework in Rust.

Built on [`sqlx::AnyPool`](https://docs.rs/sqlx/latest/sqlx/any/type.AnyPool.html),
it lets you write your Event Sourcing logic once and run it against
**PostgreSQL**, **MySQL**, or **SQLite** by changing only the connection string.

---

## Features

- **Multi-database** — connect to `postgres://`, `mysql://`, or `sqlite://` without touching application code
- **Event Store** — persistent, versioned event streams with `Appender` and `Streamer`
- **Aggregate Repository** — two modes: classic mutable-row and append-only snapshot (see below)
- **Optimistic Concurrency Control** — safe concurrent writes on all backends
- **Auto-migrations** — database schema created automatically on `Store`/`Repository` construction
- **Event schema versioning** — `schema_version` column per event; old payloads upcasted at read time

---

## Installation

```toml
[dependencies]
eventually     = { version = "0.5", git = "https://github.com/get-eventually/eventually-rs" }
eventually-any = "0.1"

[features]
# Enable at least one backend:
sqlite    = ["eventually-any/sqlite"]
postgres  = ["eventually-any/postgres"]
mysql     = ["eventually-any/mysql"]
# Auto-run migrations on Store/Repository construction:
migrations = ["eventually-any/migrations"]
```

---

## Quick start

```sh
cargo run --example minimal --features "sqlite,migrations"
```

```rust
use eventually::event::store::{Appender, Streamer};
use eventually::event::{Envelope, VersionSelect};
use eventually::version;
use eventually_any::event::Store;
use futures::TryStreamExt;
use sqlx::any::install_default_drivers;

install_default_drivers();

let pool = sqlx::any::AnyPoolOptions::new()
    .max_connections(1)
    .connect("sqlite::memory:")
    .await?;

// Store::new runs migrations automatically (requires the `migrations` feature).
let store = Store::new(pool, eventually::serde::Json::<MyEvent>::default()).await?;

let new_version = store
    .append(
        "stream-1".to_string(),
        version::Check::MustBe(0),
        vec![Envelope::from(MyEvent::Created { name: "Alice".into() })],
    )
    .await?;

let events: Vec<_> = store
    .stream(&"stream-1".to_string(), VersionSelect::All)
    .try_collect()
    .await?;
```

---

## Aggregate ID rule

When using an aggregate repository, **always embed the aggregate ID in the
creation event**. `Aggregate::apply` receives only the event — it has no other
way to learn the ID. The serialized state blob stored in the database must
contain the real ID, otherwise `aggregate_id()` returns a placeholder and the
repository cannot find the row on the next `get()`.

```rust
// ✅ Correct — id travels through the event into self.id
pub enum OrderEvent {
    Created { id: OrderId, customer: String },
}

fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, Self::Error> {
    match (state, event) {
        (None, OrderEvent::Created { id, customer }) => Ok(Order { id, customer }),
        // ...
    }
}

// ❌ Wrong — placeholder breaks save/load round-trips
(None, OrderEvent::Created { customer }) => Ok(Order { id: OrderId(0), customer })
```

---

## Aggregate repository: two modes

### Classic (default)

One mutable row per aggregate in the `aggregates` table, updated in place on
every save. Fast O(1) reads; previous states are not retained.

```sh
# build/test without snapshots feature
cargo build --features "sqlite,migrations"
```

```rust
use eventually_any::aggregate::Repository;

let repo: Repository<MyAggregate, _, _> = Repository::new(
    pool,
    eventually::serde::Json::<MyAggregate>::default(),
    eventually::serde::Json::<MyEvent>::default(),
).await?;

repo.save(&mut root).await?;
let root = repo.get(&id).await?;
```

### Snapshot (append-only)

Every save appends a new immutable row to the `snapshots` table. Loading reads
the latest snapshot then replays only the delta events since that snapshot —
O(1) reads while keeping the full audit trail.

```sh
cargo build --features "sqlite,migrations,snapshots"
```

```rust
use eventually_any::snapshot::Repository;

let repo: Repository<MyAggregate, _, _> = Repository::new(
    pool,
    eventually::serde::Json::<MyAggregate>::default(),
    eventually::serde::Json::<MyEvent>::default(),
).await?;
```

The public API (`Getter`, `Saver`) is identical between both modes — switching
is a feature-flag change only, no application code changes.

| | Classic | Snapshot |
|---|---|---|
| Storage | One row per aggregate (UPDATE) | One row per save (INSERT) |
| History | ❌ Previous states overwritten | ✅ Full audit trail |
| Read cost | O(1) | O(1) snapshot + tiny delta |

---

## Event schema versioning

Every event row stores a `schema_version` integer. When your event payload
format changes, register an `Upcaster` for each version transition. Old events
are upgraded transparently at read time — no data migration script needed.

```rust
use eventually_any::event::Store;
use eventually_any::upcasting::{FnUpcaster, UpcasterChain};

// V1 payload: { "Created": { "name": "Alice Bob" } }
// V2 payload: { "Created": { "first_name": "Alice", "last_name": "Bob" } }
let chain = UpcasterChain::new()
    .register(FnUpcaster::new("UserCreated", 1, 2, |mut p| {
        if let Some(inner) = p.get_mut("Created") {
            if let Some(name) = inner.get("name").and_then(|v| v.as_str()).map(str::to_owned) {
                let parts: Vec<&str> = name.splitn(2, ' ').collect();
                inner["first_name"] = json!(parts.first().copied().unwrap_or(""));
                inner["last_name"]  = json!(parts.get(1).copied().unwrap_or(""));
                inner.as_object_mut().unwrap().remove("name");
            }
        }
        p
    }));

let store = Store::new(pool, eventually::serde::Json::<UserEvent>::default())
    .await?
    .with_schema_version(2)      // new events written at v2
    .with_upcaster_chain(chain); // v1 events transparently upcasted on read
```

For complex, well-tested migrations implement the `Upcaster` trait directly:

```rust
use eventually_any::upcasting::Upcaster;
use serde_json::Value;

pub struct UserCreatedV1ToV2;

impl Upcaster for UserCreatedV1ToV2 {
    fn event_type(&self)   -> &str { "UserCreated" }
    fn from_version(&self) -> u32  { 1 }
    fn to_version(&self)   -> u32  { 2 }

    fn upcast(&self, mut payload: Value) -> Value {
        // transform payload...
        payload
    }
}

// Chain multiple hops: V1→V2 and V2→V3 applied automatically when reading a V1 event.
let chain = UpcasterChain::new()
    .register(UserCreatedV1ToV2)
    .register(UserCreatedV2ToV3);
```

The `Repository` types also accept `.with_schema_version()` and
`.with_upcaster_chain()`, forwarding them to the underlying event store.

---

## Optimistic Concurrency Control

Every write declares the version it expects the stream to be at. If another
writer has already advanced the version, the write fails with
`AppendError::Conflict` or `SaveError::Conflict`.

```rust
// Event store level
match store.append(id, version::Check::MustBe(expected), events).await {
    Err(AppendError::Conflict(e)) => { /* reload and retry */ }
    Ok(new_version) => { /* success */ }
}

// Aggregate repository level
match repo.save(&mut root).await {
    Err(SaveError::Conflict(_)) => { /* reload and retry */ }
    Ok(()) => { /* success */ }
}
```

A realistic retry loop:

```rust
loop {
    let mut root = repo.get(&id).await.map(MyRoot::from)?;
    root.do_something()?;
    match repo.save(&mut root).await {
        Ok(()) => break,
        Err(SaveError::Conflict(_)) => continue, // reload and retry
        Err(e) => return Err(e.into()),
    }
}
```

---

## SQLite connection pool note

`sqlite::memory:` creates a **separate blank database per connection**. With
`max_connections > 1`, migrations run on connection 1 but queries on
connections 2+ see no tables.

```rust
// ✅ Single connection — plain sqlite::memory: is fine
let pool = AnyPoolOptions::new()
    .max_connections(1)
    .connect("sqlite::memory:")
    .await?;

// ✅ Multiple connections — use the shared-cache URI
let pool = AnyPoolOptions::new()
    .max_connections(4)
    .connect("sqlite:file::memory:?cache=shared&mode=memory")
    .await?;
```

SQLite serializes all writes regardless, so for OCC demonstrations
`max_connections(1)` is sufficient and simpler.

---

## Examples

All examples require the `migrations` feature. Run with:

```sh
cargo run --example <name> --features "<features>"
```

| Example | Features | What it shows |
|---|---|---|
| `minimal` | `sqlite,migrations` | Append and stream events; schema versioning with upcasters |
| `bank_account` | `sqlite,migrations` | Full aggregate lifecycle: create, mutate, reload, domain errors, close |
| `order_lifecycle` | `sqlite,migrations` | Raw event-store streaming, metadata, projection rebuild, checkpoint resume |
| `concurrent_writes` | `sqlite,migrations` | OCC at store and repository level, optimistic retry loop |
| `snapshot_repository` | `sqlite,snapshots,migrations` | Audit trail, delta replay, two aggregates in the same snapshots table |
| `multi_schema_upcasting` | `sqlite,migrations` | Chained V1→V2→V3 schema migrations with named-struct upcasters |
| `multi_database` | `sqlite,migrations` | Identical logic against SQLite / PostgreSQL / MySQL |

```sh
cargo run --example minimal               --features "sqlite,migrations"
cargo run --example bank_account          --features "sqlite,migrations"
cargo run --example order_lifecycle       --features "sqlite,migrations"
cargo run --example concurrent_writes     --features "sqlite,migrations"
cargo run --example snapshot_repository   --features "sqlite,snapshots,migrations"
cargo run --example multi_schema_upcasting --features "sqlite,migrations"
cargo run --example multi_database        --features "sqlite,migrations"

# multi_database with all backends:
POSTGRES_URL="postgres://postgres:postgres@localhost:5432/postgres" \
MYSQL_URL="mysql://root@localhost:3306/test_db" \
cargo run --example multi_database --features "sqlite,postgres,mysql,migrations"
```

---

## Running the tests

Tests use [testcontainers](https://github.com/testcontainers/testcontainers-rs)
to spin up Dockerized PostgreSQL and MySQL instances automatically.

```sh
cargo test --features "sqlite,migrations"
cargo test --features "postgres,migrations"
cargo test --features "mysql,migrations"
cargo test --features "sqlite,snapshots,migrations"
```

---

## Feature flags

| Flag | Effect |
|---|---|
| `postgres` | Enable PostgreSQL support via sqlx |
| `sqlite` | Enable SQLite support via sqlx |
| `mysql` | Enable MySQL support via sqlx |
| `migrations` | Embed and auto-run SQL migrations on `Store`/`Repository` construction |
| `snapshots` | Replace `aggregate::Repository` with `snapshot::Repository` (append-only) |

At least one of `postgres`, `sqlite`, or `mysql` must be enabled.

---

## License

MIT — see [LICENSE](LICENSE).
