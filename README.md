# eventually-any

eventually-any provides database-agnostic Event Store and Aggregate Repository implementations for the eventually Event Sourcing framework in Rust.

By leveraging sqlx::Any, this crate allows you to write your Event Sourcing logic once and deploy it against PostgreSQL, MySQL, or SQLite simply by changing your connection string at runtime.

## Features

- Multi-Database Support: Seamlessly connect to `postgres://`, `mysql://`, or `sqlite://` without changing your application code.
- Event Store: Implements `eventually::event::store::Appender` and `Streamer` for persistent event streams.
- Aggregate Repository: Implements `eventually::aggregate::repository::Getter` and `Saver` for managing Aggregate state.
- Optimistic Concurrency Control (OCC): Safely handles concurrent writes and prevents data races across all supported databases.
- Auto-Migrations: Automatically sets up the required database schemas (`events`, `event_streams`, `aggregates`) upon initialization.

## Installation

Add `eventually`, and `eventually-any` to your Cargo.toml. You must enable the database features you intend to support.

```toml
[dependencies]
eventually = "0.5"
eventually-any = "0.1"
```

## Running the Examples and Tests

To run the provided minimal example with different database backends:

```shell
# SQLite (In-Memory)
cargo run --example minimal --features "sqlite"

# PostgreSQL (Requires a running Postgres instance)
DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres" cargo run --example minimal --features "postgres"

# MySQL (Requires a running MySQL instance)
DATABASE_URL="mysql://root@localhost:3306/test_db" cargo run --example minimal --features "mysql"
```

To run the test suite (which uses testcontainers to automatically spin up Dockerized PostgreSQL and MySQL databases):

```shell
cargo test --features postgres
cargo test --features sqlite
cargo test --features mysql
```

## License

This project is licensed under the MIT license.
