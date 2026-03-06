//! `eventually-any` contains different implementations of traits
//! from the [eventually] crate that are specific for `PostgreSQL`, `Sqlite` and `MySQL` databases.
//!
//! Check out the [`aggregate::Repository`] and [`event::Store`] implementations
//! to know more.

#![deny(unsafe_code, unused_qualifications, trivial_casts)]
#![deny(clippy::all, clippy::pedantic, clippy::cargo)]
#![warn(missing_docs)]

pub mod aggregate;
pub mod event;

#[cfg(not(any(feature = "postgres", feature = "sqlite", feature = "mysql")))]
compile_error!("At least one of the features must be enabled: postgres, sqlite, mysql");

#[cfg(all(feature = "postgres", feature = "migrations",))]
pub(crate) static MIGRATIONS_POSTGRES: sqlx::migrate::Migrator =
    sqlx::migrate!("./migrations/postgres");
#[cfg(all(feature = "sqlite", feature = "migrations",))]
pub(crate) static MIGRATIONS_SQLITE: sqlx::migrate::Migrator =
    sqlx::migrate!("./migrations/sqlite");
#[cfg(all(feature = "mysql", feature = "migrations",))]
pub(crate) static MIGRATIONS_MYSQL: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/mysql");
