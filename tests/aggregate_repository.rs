use eventually::aggregate::repository::{self, GetError, Getter, Saver};
use eventually::serde;
use eventually_any::aggregate;
use rand::{Rng, RngExt};
use sqlx::any::install_default_drivers;
#[cfg(feature = "mysql")]
use testcontainers_modules::mysql::Mysql;
#[cfg(feature = "postgres")]
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

mod setup;

async fn run_it_works(pool: sqlx::AnyPool) {
    let aggregate_repository = aggregate::Repository::new(
        pool,
        serde::Json::<setup::TestAggregate>::default(),
        serde::Json::<setup::TestDomainEvent>::default(),
    )
    .await
    .unwrap();

    let aggregate_id = setup::TestAggregateId(rand::rng().random::<i64>());

    let result = aggregate_repository
        .get(&aggregate_id)
        .await
        .expect_err("should fail");

    match result {
        GetError::NotFound => (),
        _ => panic!(
            "unexpected error received, should be 'not found': {:?}",
            result
        ),
    };

    let mut root = setup::TestAggregateRoot::create(aggregate_id, "John Dee".to_owned())
        .expect("aggregate root should be created");

    // We also delete it just to cause more Domain Events in its Event Stream.
    root.delete().unwrap();

    aggregate_repository
        .save(&mut root)
        .await
        .expect("storing the new aggregate root should be successful");

    let found_root = aggregate_repository
        .get(&aggregate_id)
        .await
        .map(setup::TestAggregateRoot::from)
        .expect("the aggregate root should be found successfully");

    assert_eq!(found_root, root);
}

async fn run_it_detects_data_races_and_returns_conflict_error(pool: sqlx::AnyPool) {
    let aggregate_repository = aggregate::Repository::new(
        pool,
        serde::Json::<setup::TestAggregate>::default(),
        serde::Json::<setup::TestDomainEvent>::default(),
    )
    .await
    .unwrap();

    let aggregate_id = setup::TestAggregateId(rand::rng().random::<i64>());

    let mut root = setup::TestAggregateRoot::create(aggregate_id, "John Dee".to_owned())
        .expect("aggregate root should be created");

    // We also delete it just to cause more Domain Events in its Event Stream.
    root.delete().unwrap();

    // We clone the Aggregate Root instance so that we have the same
    // uncommitted events list as the original instance.
    let mut cloned_root = root.clone();

    let result = futures::join!(
        aggregate_repository.save(&mut root),
        aggregate_repository.save(&mut cloned_root),
    );

    match result {
        (Ok(()), Err(repository::SaveError::Conflict(_))) => (),
        (Err(repository::SaveError::Conflict(_)), Ok(())) => (),
        (first, second) => panic!(
            "invalid state detected, first: {:?}, second: {:?}",
            first, second
        ),
    };
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

// =====================================================================
// SQLITE TESTS
// =====================================================================

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn it_works_sqlite() {
    install_default_drivers();
    // Use an in-memory SQLite database, limited to 1 connection so state is preserved
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

// =====================================================================
// MYSQL TESTS
// =====================================================================

#[cfg(feature = "mysql")]
#[tokio::test]
async fn it_works_mysql() {
    install_default_drivers();
    let container = Mysql::default().start().await.expect("start");
    let (host, port) =
        futures::try_join!(container.get_host(), container.get_host_port_ipv4(3306)).unwrap();

    // Create the test database first to avoid writing to the protected `mysql` db
    let setup_pool = sqlx::AnyPool::connect(&format!("mysql://root@{}:{}", host, port))
        .await
        .unwrap();
    sqlx::query("CREATE DATABASE IF NOT EXISTS test_db")
        .execute(&setup_pool)
        .await
        .unwrap();
    setup_pool.close().await;

    let pool = sqlx::AnyPool::connect(&format!("mysql://root@{}:{}/test_db", host, port))
        .await
        .unwrap();

    run_it_works(pool).await;
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn it_detects_data_races_mysql() {
    install_default_drivers();
    let container = Mysql::default().start().await.expect("start");
    let (host, port) =
        futures::try_join!(container.get_host(), container.get_host_port_ipv4(3306)).unwrap();

    // Create the test database first to avoid writing to the protected `mysql` db
    let setup_pool = sqlx::AnyPool::connect(&format!("mysql://root@{}:{}", host, port))
        .await
        .unwrap();
    sqlx::query("CREATE DATABASE IF NOT EXISTS test_db")
        .execute(&setup_pool)
        .await
        .unwrap();
    setup_pool.close().await;

    let pool = sqlx::AnyPool::connect(&format!("mysql://root@{}:{}/test_db", host, port))
        .await
        .unwrap();

    run_it_detects_data_races_and_returns_conflict_error(pool).await;
}
