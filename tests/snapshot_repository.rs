#![cfg(feature = "snapshots")]

use eventually::aggregate::repository::{self, GetError, Getter, Saver};
use eventually::serde;
use eventually_any::snapshot;
use rand::{Rng, RngExt};
use sqlx::any::install_default_drivers;
#[cfg(feature = "mysql")]
use testcontainers_modules::mysql::Mysql;
#[cfg(feature = "postgres")]
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

mod setup;

// =====================================================================
// Shared runner functions
//
// Each scenario is implemented once as an async fn that accepts a pool.
// The per-backend #[tokio::test] functions at the bottom call them after
// spinning up the appropriate container / in-memory db.
// =====================================================================

async fn run_it_works(pool: sqlx::AnyPool) {
    let repo = snapshot::Repository::new(
        pool,
        serde::Json::<setup::TestAggregate>::default(),
        serde::Json::<setup::TestDomainEvent>::default(),
    )
    .await
    .unwrap();

    let aggregate_id = setup::TestAggregateId(rand::rng().random::<i64>());

    // Not found before any save.
    let result = repo
        .get(&aggregate_id)
        .await
        .expect_err("should return NotFound before any save");

    match result {
        GetError::NotFound => (),
        _ => panic!("unexpected error, expected NotFound, got: {:?}", result),
    };

    let mut root = setup::TestAggregateRoot::create(aggregate_id, "John Dee".to_owned())
        .expect("aggregate root should be created");

    // Record an extra event so the stream has more than one entry.
    root.delete().unwrap();

    repo.save(&mut root)
        .await
        .expect("storing the new aggregate root should be successful");

    let found_root = repo
        .get(&aggregate_id)
        .await
        .map(setup::TestAggregateRoot::from)
        .expect("the aggregate root should be found successfully");

    assert_eq!(found_root, root);
}

async fn run_it_detects_data_races_and_returns_conflict_error(pool: sqlx::AnyPool) {
    let repo = snapshot::Repository::new(
        pool,
        serde::Json::<setup::TestAggregate>::default(),
        serde::Json::<setup::TestDomainEvent>::default(),
    )
    .await
    .unwrap();

    let aggregate_id = setup::TestAggregateId(rand::rng().random::<i64>());

    let mut root = setup::TestAggregateRoot::create(aggregate_id, "John Dee".to_owned())
        .expect("aggregate root should be created");

    root.delete().unwrap();

    // Clone before saving so both roots carry the same uncommitted events.
    let mut cloned_root = root.clone();

    let result = futures::join!(repo.save(&mut root), repo.save(&mut cloned_root),);

    match result {
        (Ok(()), Err(repository::SaveError::Conflict(_))) => (),
        (Err(repository::SaveError::Conflict(_)), Ok(())) => (),
        (first, second) => panic!(
            "invalid state detected, first: {:?}, second: {:?}",
            first, second
        ),
    };
}

/// Save once, load from snapshot, mutate, save again, load again.
/// The second load must reflect both events (one from each save round).
async fn run_snapshot_plus_delta_replay_produces_correct_state(pool: sqlx::AnyPool) {
    let repo = snapshot::Repository::new(
        pool,
        serde::Json::<setup::TestAggregate>::default(),
        serde::Json::<setup::TestDomainEvent>::default(),
    )
    .await
    .unwrap();

    let aggregate_id = setup::TestAggregateId(rand::rng().random::<i64>());

    // First save — writes snapshot at version 1.
    let mut root = setup::TestAggregateRoot::create(aggregate_id, "Jane Dee".to_owned())
        .expect("aggregate root should be created");

    repo.save(&mut root)
        .await
        .expect("first save should succeed");

    assert_eq!(root.version(), 1, "version after first save should be 1");

    // Load from the snapshot.
    let mut loaded_root = repo
        .get(&aggregate_id)
        .await
        .map(setup::TestAggregateRoot::from)
        .expect("root should be found after first save");

    assert_eq!(loaded_root.version(), 1);

    // Mutate and save — writes snapshot at version 2.
    loaded_root.delete().expect("delete should succeed");

    repo.save(&mut loaded_root)
        .await
        .expect("second save should succeed");

    assert_eq!(loaded_root.version(), 2);

    // Final load — snapshot at version 2 is returned directly (zero delta events).
    let final_root = repo
        .get(&aggregate_id)
        .await
        .map(setup::TestAggregateRoot::from)
        .expect("root should be found after second save");

    assert_eq!(final_root.version(), 2);
    assert_eq!(final_root, loaded_root);
}

/// Two aggregates with different ids sharing the same `snapshots` table
/// must not interfere with each other's data.
async fn run_multiple_aggregate_ids_coexist_without_interference(pool: sqlx::AnyPool) {
    let repo = snapshot::Repository::new(
        pool,
        serde::Json::<setup::TestAggregate>::default(),
        serde::Json::<setup::TestDomainEvent>::default(),
    )
    .await
    .unwrap();

    let id_a = setup::TestAggregateId(rand::rng().random::<i64>());
    let id_b = setup::TestAggregateId(rand::rng().random::<i64>());

    let mut root_a = setup::TestAggregateRoot::create(id_a, "Alice".to_owned()).expect("create A");
    let mut root_b = setup::TestAggregateRoot::create(id_b, "Bob".to_owned()).expect("create B");

    repo.save(&mut root_a).await.expect("save A");
    repo.save(&mut root_b).await.expect("save B");

    let loaded_a = repo
        .get(&id_a)
        .await
        .map(setup::TestAggregateRoot::from)
        .expect("get A");
    let loaded_b = repo
        .get(&id_b)
        .await
        .map(setup::TestAggregateRoot::from)
        .expect("get B");

    assert_eq!(loaded_a, root_a, "aggregate A should round-trip correctly");
    assert_eq!(loaded_b, root_b, "aggregate B should round-trip correctly");
    assert_ne!(loaded_a, loaded_b, "A and B must be distinct");
}

// =====================================================================
// POSTGRESQL TESTS
// =====================================================================

#[cfg(feature = "postgres")]
#[tokio::test]
async fn it_works_postgres() {
    install_default_drivers();
    let container = Postgres::default().start().await.expect("start");
    let (host, port) =
        futures::try_join!(container.get_host(), container.get_host_port_ipv4(5432)).unwrap();

    let pool = sqlx::AnyPool::connect(&format!(
        "postgres://postgres:postgres@{}:{}/postgres",
        host, port
    ))
    .await
    .unwrap();

    run_it_works(pool).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn it_detects_data_races_postgres() {
    install_default_drivers();
    let container = Postgres::default().start().await.expect("start");
    let (host, port) =
        futures::try_join!(container.get_host(), container.get_host_port_ipv4(5432)).unwrap();

    let pool = sqlx::AnyPool::connect(&format!(
        "postgres://postgres:postgres@{}:{}/postgres",
        host, port
    ))
    .await
    .unwrap();

    run_it_detects_data_races_and_returns_conflict_error(pool).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn snapshot_plus_delta_replay_produces_correct_state_postgres() {
    install_default_drivers();
    let container = Postgres::default().start().await.expect("start");
    let (host, port) =
        futures::try_join!(container.get_host(), container.get_host_port_ipv4(5432)).unwrap();

    let pool = sqlx::AnyPool::connect(&format!(
        "postgres://postgres:postgres@{}:{}/postgres",
        host, port
    ))
    .await
    .unwrap();

    run_snapshot_plus_delta_replay_produces_correct_state(pool).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn multiple_aggregate_ids_coexist_without_interference_postgres() {
    install_default_drivers();
    let container = Postgres::default().start().await.expect("start");
    let (host, port) =
        futures::try_join!(container.get_host(), container.get_host_port_ipv4(5432)).unwrap();

    let pool = sqlx::AnyPool::connect(&format!(
        "postgres://postgres:postgres@{}:{}/postgres",
        host, port
    ))
    .await
    .unwrap();

    run_multiple_aggregate_ids_coexist_without_interference(pool).await;
}

// =====================================================================
// SQLITE TESTS
// =====================================================================

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn it_works_sqlite() {
    install_default_drivers();
    let pool = sqlx::any::AnyPoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    run_it_works(pool).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn it_detects_data_races_sqlite() {
    install_default_drivers();
    let pool = sqlx::any::AnyPoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    run_it_detects_data_races_and_returns_conflict_error(pool).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn snapshot_plus_delta_replay_produces_correct_state_sqlite() {
    install_default_drivers();
    let pool = sqlx::any::AnyPoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    run_snapshot_plus_delta_replay_produces_correct_state(pool).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn multiple_aggregate_ids_coexist_without_interference_sqlite() {
    install_default_drivers();
    let pool = sqlx::any::AnyPoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    run_multiple_aggregate_ids_coexist_without_interference(pool).await;
}

// =====================================================================
// MYSQL TESTS
// =====================================================================

#[cfg(feature = "mysql")]
async fn mysql_pool() -> sqlx::AnyPool {
    let container = Mysql::default().start().await.expect("start");
    let (host, port) =
        futures::try_join!(container.get_host(), container.get_host_port_ipv4(3306)).unwrap();

    // Create the test database first to avoid writing to the protected `mysql` db.
    let setup_pool = sqlx::AnyPool::connect(&format!("mysql://root@{}:{}", host, port))
        .await
        .unwrap();
    sqlx::query("CREATE DATABASE IF NOT EXISTS test_db")
        .execute(&setup_pool)
        .await
        .unwrap();
    setup_pool.close().await;

    sqlx::AnyPool::connect(&format!("mysql://root@{}:{}/test_db", host, port))
        .await
        .unwrap()
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn it_works_mysql() {
    install_default_drivers();
    run_it_works(mysql_pool().await).await;
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn it_detects_data_races_mysql() {
    install_default_drivers();
    run_it_detects_data_races_and_returns_conflict_error(mysql_pool().await).await;
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn snapshot_plus_delta_replay_produces_correct_state_mysql() {
    install_default_drivers();
    run_snapshot_plus_delta_replay_produces_correct_state(mysql_pool().await).await;
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn multiple_aggregate_ids_coexist_without_interference_mysql() {
    install_default_drivers();
    run_multiple_aggregate_ids_coexist_without_interference(mysql_pool().await).await;
}
