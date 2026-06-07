//! Database-backend detection and per-backend SQL helpers.

/// The SQL database backend the connection pool is connected to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Backend {
    Postgres,
    MySQL,
    /// SQLite, or any unrecognised backend (falls back to SQLite-style SQL).
    Sqlite,
}

impl Backend {
    /// Detect the backend from the name returned by `sqlx`.
    pub(crate) fn from_name(name: &str) -> Self {
        match name {
            "PostgreSQL" => Self::Postgres,
            "MySQL" => Self::MySQL,
            _ => Self::Sqlite,
        }
    }

    /// Return the positional placeholder for parameter `n` in a prepared query.
    ///
    /// MySQL uses `?` for every parameter; PostgreSQL and SQLite use `$N`.
    pub(crate) fn placeholder(&self, n: usize) -> String {
        match self {
            Self::MySQL => "?".to_owned(),
            _ => format!("${n}"),
        }
    }

    /// Return the correctly-quoted identifier for the `type` column.
    ///
    /// MySQL requires backtick quoting; other backends use ANSI double-quotes.
    #[cfg(not(feature = "snapshots"))]
    pub(crate) fn type_column(&self) -> &'static str {
        match self {
            Self::MySQL => "`type`",
            _ => r#""type""#,
        }
    }

    /// Return the correctly-quoted identifier for the `version` column.
    #[cfg(feature = "snapshots")]
    pub(crate) fn version_column(&self) -> &'static str {
        match self {
            Self::MySQL => "`version`",
            _ => r#""version""#,
        }
    }

    /// `true` for PostgreSQL — the only backend that needs an explicit
    /// `SERIALIZABLE DEFERRABLE` isolation level on every write transaction
    /// to obtain the same OCC guarantees as MySQL and SQLite.
    pub(crate) fn requires_serializable_isolation(&self) -> bool {
        matches!(self, Self::Postgres)
    }
}

/// Returns `true` if `code` is a database error code indicating a write
/// conflict (unique-constraint violation or serialization failure) on any
/// supported backend.
///
/// | Code    | Meaning |
/// |---------|---------|
/// | `23505` | PostgreSQL unique-constraint violation |
/// | `40001` | PostgreSQL serialization failure |
/// | `1062`  | MySQL duplicate entry |
/// | `23000` | MySQL / SQLite generic integrity-constraint violation |
/// | `2067`  | SQLite unique-constraint violation |
/// | `1555`  | SQLite primary-key constraint violation |
pub(crate) fn is_conflict_error_code(code: &str) -> bool {
    matches!(code, "23505" | "40001" | "1062" | "23000" | "2067" | "1555")
}
