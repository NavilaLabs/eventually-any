//! Event schema versioning via upcasting.
//!
//! # Overview
//!
//! When an event's payload format changes over time, old events stored in the
//! database keep their original bytes.  Reading them back with the *current*
//! deserialiser would fail or produce wrong data.
//!
//! The upcasting pipeline solves this by running a chain of small, versioned
//! transformations **at read time**, converting any stored payload to the
//! current schema before it reaches the application.
//!
//! ```text
//! DB bytes  ──►  RawEvent { type, schema_version, bytes }
//!               │
//!               ▼
//!         UpcasterChain
//!           ├─ V1 → V2 upcaster  (runs if schema_version == 1)
//!           └─ V2 → V3 upcaster  (runs if schema_version == 2)
//!               │
//!               ▼
//!         bytes at current schema_version
//!               │
//!               ▼
//!         Deserialise → domain event
//! ```
//!
//! # Implementing an upcaster
//!
//! Implement [`Upcaster`] for each version transition you need:
//!
//! ```rust,ignore
//! use eventually_any::upcasting::Upcaster;
//! use serde_json::{json, Value};
//!
//! /// Migrates UserCreated from schema v1 → v2.
//! /// v1 had `{ "name": "Alice" }`, v2 has `{ "first_name": "Alice", "last_name": "" }`.
//! pub struct UserCreatedV1ToV2;
//!
//! impl Upcaster for UserCreatedV1ToV2 {
//!     fn event_type(&self) -> &str { "UserCreated" }
//!     fn from_version(&self) -> u32 { 1 }
//!     fn to_version(&self)   -> u32 { 2 }
//!
//!     fn upcast(&self, mut payload: Value) -> Value {
//!         if let Some(name) = payload.get("name").and_then(|v| v.as_str()).map(str::to_owned) {
//!             payload["first_name"] = json!(name);
//!             payload["last_name"]  = json!("");
//!             payload.as_object_mut().unwrap().remove("name");
//!         }
//!         payload
//!     }
//! }
//! ```
//!
//! # Registering upcasters
//!
//! ```rust,ignore
//! use eventually_any::upcasting::UpcasterChain;
//!
//! let chain = UpcasterChain::new()
//!     .register(UserCreatedV1ToV2)
//!     .register(UserCreatedV2ToV3);
//!
//! // Pass the chain to Store::with_upcaster_chain(…) or
//! // aggregate::Repository::with_upcaster_chain(…).
//! ```
//!
//! # Schema version on write
//!
//! The [`Store`](crate::event::Store) writes the *current* schema version for
//! every new event it appends.  Pass the current version to
//! [`Store::with_schema_version`](crate::event::Store::with_schema_version).
//! Existing events in the database keep their original `schema_version`; the
//! chain upgrades them transparently on the next read.

use serde_json::Value;

// ── Core trait ────────────────────────────────────────────────────────────

/// A single version-to-version JSON payload transformation for one event type.
///
/// Implementations must be **pure** (no I/O, no side effects): they receive
/// the stored JSON `Value` and return the migrated `Value`.  The
/// [`UpcasterChain`] chains multiple upcasters in order.
pub trait Upcaster: Send + Sync {
    /// The `type` column value this upcaster applies to (e.g. `"UserCreated"`).
    fn event_type(&self) -> &str;

    /// The `schema_version` this upcaster *reads* (i.e. the old version).
    fn from_version(&self) -> u32;

    /// The `schema_version` this upcaster *produces* (i.e. the new version).
    fn to_version(&self) -> u32;

    /// Transform the raw JSON payload from `from_version` to `to_version`.
    fn upcast(&self, payload: Value) -> Value;
}

// ── Chain ─────────────────────────────────────────────────────────────────

/// An ordered registry of [`Upcaster`]s.
///
/// When [`UpcasterChain::apply`] is called it repeatedly selects the first
/// matching upcaster for `(event_type, current_schema_version)` until no
/// further transformation is possible.  This allows you to register V1→V2
/// and V2→V3 upcasters independently and have both applied automatically
/// when reading a V1 event.
#[derive(Default)]
pub struct UpcasterChain {
    upcasters: Vec<Box<dyn Upcaster>>,
}

impl UpcasterChain {
    /// Create an empty chain.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an upcaster.  Upcasters are applied in **registration order**
    /// within the same `from_version`, so register lower-version upcasters
    /// first.
    #[must_use]
    pub fn register(mut self, upcaster: impl Upcaster + 'static) -> Self {
        self.upcasters.push(Box::new(upcaster));
        self
    }

    /// Apply all applicable upcasters to `payload` for the given
    /// `event_type`, starting at `current_schema_version`.
    ///
    /// Returns the transformed payload and the final schema version it
    /// now conforms to.
    ///
    /// # Example
    ///
    /// If `current_schema_version == 1` and there are registered upcasters
    /// `V1→V2` and `V2→V3`, the chain runs both and returns `(payload, 3)`.
    #[must_use]
    pub fn apply(
        &self,
        event_type: &str,
        mut schema_version: u32,
        mut payload: Value,
    ) -> (Value, u32) {
        loop {
            let found = self
                .upcasters
                .iter()
                .find(|u| u.event_type() == event_type && u.from_version() == schema_version);

            match found {
                None => break,
                Some(upcaster) => {
                    schema_version = upcaster.to_version();
                    payload = upcaster.upcast(payload);
                }
            }
        }
        (payload, schema_version)
    }

    /// Returns `true` if no upcasters have been registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.upcasters.is_empty()
    }
}

impl std::fmt::Debug for UpcasterChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpcasterChain")
            .field("count", &self.upcasters.len())
            .finish()
    }
}

// ── Closure-based convenience ─────────────────────────────────────────────

/// An [`Upcaster`] created from a plain closure — useful for simple, inline
/// transformations where a named struct is overkill.
///
/// # Example
///
/// ```rust,ignore
/// use eventually_any::upcasting::{FnUpcaster, UpcasterChain};
/// use serde_json::{json, Value};
///
/// let chain = UpcasterChain::new()
///     .register(FnUpcaster::new("UserCreated", 1, 2, |mut payload: Value| {
///         payload["full_name"] = json!(payload["name"].as_str().unwrap_or(""));
///         payload.as_object_mut().unwrap().remove("name");
///         payload
///     }));
/// ```
pub struct FnUpcaster {
    event_type: String,
    from: u32,
    to: u32,
    f: Box<dyn Fn(Value) -> Value + Send + Sync>,
}

impl FnUpcaster {
    /// Create a new closure-based upcaster.
    pub fn new(
        event_type: impl Into<String>,
        from_version: u32,
        to_version: u32,
        f: impl Fn(Value) -> Value + Send + Sync + 'static,
    ) -> Self {
        Self {
            event_type: event_type.into(),
            from: from_version,
            to: to_version,
            f: Box::new(f),
        }
    }
}

impl Upcaster for FnUpcaster {
    fn event_type(&self) -> &str {
        &self.event_type
    }

    fn from_version(&self) -> u32 {
        self.from
    }

    fn to_version(&self) -> u32 {
        self.to
    }

    fn upcast(&self, payload: Value) -> Value {
        (self.f)(payload)
    }
}

impl std::fmt::Debug for FnUpcaster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FnUpcaster")
            .field("event_type", &self.event_type)
            .field("from_version", &self.from)
            .field("to_version", &self.to)
            .finish()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    struct RenameField {
        from: &'static str,
        to_field: &'static str,
        from_v: u32,
        to_v: u32,
    }

    impl Upcaster for RenameField {
        fn event_type(&self) -> &str {
            "TestEvent"
        }
        fn from_version(&self) -> u32 {
            self.from_v
        }
        fn to_version(&self) -> u32 {
            self.to_v
        }
        fn upcast(&self, mut payload: Value) -> Value {
            if let Some(val) = payload.get(self.from).cloned() {
                payload[self.to_field] = val;
                payload.as_object_mut().unwrap().remove(self.from);
            }
            payload
        }
    }

    #[test]
    fn single_upcaster_applied() {
        let chain = UpcasterChain::new().register(RenameField {
            from: "old_name",
            to_field: "name",
            from_v: 1,
            to_v: 2,
        });

        let payload = json!({ "old_name": "Alice" });
        let (result, version) = chain.apply("TestEvent", 1, payload);

        assert_eq!(version, 2);
        assert_eq!(result["name"], "Alice");
        assert!(result.get("old_name").is_none());
    }

    #[test]
    fn chain_applies_multiple_upcasters_in_sequence() {
        let chain = UpcasterChain::new()
            .register(RenameField {
                from: "v1_field",
                to_field: "v2_field",
                from_v: 1,
                to_v: 2,
            })
            .register(RenameField {
                from: "v2_field",
                to_field: "v3_field",
                from_v: 2,
                to_v: 3,
            });

        let payload = json!({ "v1_field": "hello" });
        let (result, version) = chain.apply("TestEvent", 1, payload);

        assert_eq!(version, 3);
        assert_eq!(result["v3_field"], "hello");
        assert!(result.get("v1_field").is_none());
        assert!(result.get("v2_field").is_none());
    }

    #[test]
    fn no_matching_upcaster_returns_unchanged() {
        let chain = UpcasterChain::new().register(RenameField {
            from: "old_name",
            to_field: "name",
            from_v: 1,
            to_v: 2,
        });

        let payload = json!({ "name": "Alice" });
        let (result, version) = chain.apply("TestEvent", 2, payload.clone());

        assert_eq!(version, 2);
        assert_eq!(result, payload);
    }

    #[test]
    fn different_event_type_not_upcasted() {
        let chain = UpcasterChain::new().register(RenameField {
            from: "old_name",
            to_field: "name",
            from_v: 1,
            to_v: 2,
        });

        let payload = json!({ "old_name": "Bob" });
        let (result, version) = chain.apply("OtherEvent", 1, payload.clone());

        assert_eq!(version, 1);
        assert_eq!(result, payload);
    }

    #[test]
    fn fn_upcaster_works() {
        let chain =
            UpcasterChain::new().register(FnUpcaster::new("UserCreated", 1, 2, |mut p: Value| {
                p["full_name"] = json!(p["name"].as_str().unwrap_or("").to_owned());
                p.as_object_mut().unwrap().remove("name");
                p
            }));

        let payload = json!({ "name": "Alice" });
        let (result, version) = chain.apply("UserCreated", 1, payload);

        assert_eq!(version, 2);
        assert_eq!(result["full_name"], "Alice");
    }
}
