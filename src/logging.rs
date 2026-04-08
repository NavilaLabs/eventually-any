//! Conditional tracing support.
//!
//! When the `tracing` feature is enabled the standard [`tracing`] macros are
//! re-exported unchanged.  When the feature is disabled, lightweight no-op
//! macro replacements are provided so the rest of the crate compiles without
//! any conditional compilation scattered throughout the source.
//!
//! Application code that wants to see projection logs must:
//! 1. Enable the `tracing` feature in their `Cargo.toml`:
//!    ```toml
//!    eventually-projection = { version = "…", features = ["tracing"] }
//!    ```
//! 2. Install a [`tracing`] subscriber (e.g. `tracing_subscriber::fmt::init()`).

// ── Feature-gated real macros ─────────────────────────────────────────────

#[cfg(feature = "tracing")]
pub use tracing::{debug, error, info, info_span as span, warn};

// The logging macros all swallow their arguments with no side effects.
// They accept the same syntax as the real tracing macros:
//   trace!("msg")
//   debug!(key = value, "msg")
//   info!(target: "foo", "msg")
// etc.

#[cfg(not(feature = "tracing"))]
macro_rules! span {
    ($name:expr $(, $($fields:tt)*)?) => {
        ()
    };
}

#[cfg(not(feature = "tracing"))]
macro_rules! debug {
    ($($args:tt)*) => {{}};
}

#[cfg(not(feature = "tracing"))]
macro_rules! info {
    ($($args:tt)*) => {{}};
}

#[cfg(not(feature = "tracing"))]
macro_rules! warn {
    ($($args:tt)*) => {{}};
}

#[cfg(not(feature = "tracing"))]
macro_rules! error {
    ($($args:tt)*) => {{}};
}

// Make the no-op macros available to the whole crate.
#[cfg(not(feature = "tracing"))]
pub(crate) use {debug, error, info, span, warn};
