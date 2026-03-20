//! # Multi-Schema Upcasting — Chained Version Migrations
//!
//! Real schemas evolve more than once.  This example shows how to handle a
//! three-generation event schema (V1 → V2 → V3) using **named struct
//! upcasters** — the recommended approach for complex, well-tested migrations.
//!
//! ## Schema history for `ProductCreated`
//!
//! | Version | Shape | Change |
//! |---------|-------|--------|
//! | V1 | `{ name, price }` | Initial release |
//! | V2 | `{ name, price_cents, currency }` | Decimal price → integer cents + currency |
//! | V3 | `{ name, price_cents, currency, tags }` | Added optional tags array |
//!
//! The application only knows the V3 shape.  V1 and V2 events are silently
//! upgraded at read time; no data migration is required.
//!
//! Topics covered:
//!
//! - Implementing [`Upcaster`] as a named struct for clear, testable migrations.
//! - Chaining V1→V2 and V2→V3 so reading a V1 event applies both in sequence.
//! - Writing V3 events directly with `schema_version = 3`.
//! - Proving V2 events only need one hop.
//!
//! Run with:
//! ```sh
//! cargo run --example multi_schema_upcasting --features sqlite,migrations
//! ```

use eventually::event::store::{Appender, Streamer};
use eventually::event::{Envelope, VersionSelect};
use eventually::message::Message;
use eventually::version;
use eventually_any::event::Store;
use eventually_any::upcasting::{Upcaster, UpcasterChain};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::any::install_default_drivers;

// ── Current (V3) domain event ─────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CatalogEvent {
    /// V3 shape — what the application understands today.
    ProductCreated {
        name: String,
        price_cents: u64,
        currency: String,
        tags: Vec<String>,
    },
    ProductRenamed {
        new_name: String,
    },
}

impl Message for CatalogEvent {
    fn name(&self) -> &'static str {
        match self {
            CatalogEvent::ProductCreated { .. } => "ProductCreated",
            CatalogEvent::ProductRenamed { .. } => "ProductRenamed",
        }
    }
}

// ── Named-struct upcasters ────────────────────────────────────────────────

/// Migrates `ProductCreated` from V1 → V2.
///
/// V1 payload: `{ "ProductCreated": { "name": "Foo", "price": 9.99 } }`
/// V2 payload: `{ "ProductCreated": { "name": "Foo", "price_cents": 999, "currency": "USD" } }`
pub struct ProductCreatedV1ToV2;

impl Upcaster for ProductCreatedV1ToV2 {
    fn event_type(&self) -> &str {
        "ProductCreated"
    }
    fn from_version(&self) -> u32 {
        1
    }
    fn to_version(&self) -> u32 {
        2
    }

    fn upcast(&self, mut payload: Value) -> Value {
        if let Some(inner) = payload.get_mut("ProductCreated") {
            // Convert float price → integer cents; default currency to USD.
            if let Some(price_f) = inner.get("price").and_then(|v| v.as_f64()) {
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let cents = (price_f * 100.0).round() as u64;
                inner["price_cents"] = json!(cents);
                inner["currency"] = json!("USD");
                inner.as_object_mut().unwrap().remove("price");
            }
        }
        payload
    }
}

/// Migrates `ProductCreated` from V2 → V3.
///
/// V2 payload: `{ "ProductCreated": { "name", "price_cents", "currency" } }`
/// V3 payload: adds `"tags": []`
pub struct ProductCreatedV2ToV3;

impl Upcaster for ProductCreatedV2ToV3 {
    fn event_type(&self) -> &str {
        "ProductCreated"
    }
    fn from_version(&self) -> u32 {
        2
    }
    fn to_version(&self) -> u32 {
        3
    }

    fn upcast(&self, mut payload: Value) -> Value {
        if let Some(inner) = payload.get_mut("ProductCreated") {
            // Old events have no tags — default to empty list.
            inner["tags"] = json!([]);
        }
        payload
    }
}

// ── Helper to build the chain ─────────────────────────────────────────────

fn build_chain() -> UpcasterChain {
    UpcasterChain::new()
        .register(ProductCreatedV1ToV2)
        .register(ProductCreatedV2ToV3)
}

// ── V1/V2 types for writing legacy events ────────────────────────────────
// (In production these would be the types that existed in old deploys.)

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CatalogEventV1 {
    ProductCreated { name: String, price: f64 },
    ProductRenamed { new_name: String },
}
impl Message for CatalogEventV1 {
    fn name(&self) -> &'static str {
        match self {
            CatalogEventV1::ProductCreated { .. } => "ProductCreated",
            CatalogEventV1::ProductRenamed { .. } => "ProductRenamed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CatalogEventV2 {
    ProductCreated {
        name: String,
        price_cents: u64,
        currency: String,
    },
    ProductRenamed {
        new_name: String,
    },
}
impl Message for CatalogEventV2 {
    fn name(&self) -> &'static str {
        match self {
            CatalogEventV2::ProductCreated { .. } => "ProductCreated",
            CatalogEventV2::ProductRenamed { .. } => "ProductRenamed",
        }
    }
}

// ── Main ──────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    install_default_drivers();

    let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite::memory:".to_string());
    println!("🔌  Connecting to: {db_url}\n");

    let pool = sqlx::any::AnyPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await?;

    // ── Write V1 event (legacy — "9.99 USD") ──────────────────────────────
    let v1_store = Store::new(
        pool.clone(),
        eventually::serde::Json::<CatalogEventV1>::default(),
    )
    .await?
    .with_schema_version(1);

    let stream = "catalog-product-1".to_string();

    println!("📝  Writing V1 event (price as float)…");
    let after_v1 = v1_store
        .append(
            stream.clone(),
            version::Check::MustBe(0),
            vec![Envelope::from(CatalogEventV1::ProductCreated {
                name: "Widget".into(),
                price: 9.99,
            })],
        )
        .await?;
    println!("    stream at version {after_v1}");

    // ── Write V2 event (intermediate deploy) ──────────────────────────────
    let v2_store = Store::new(
        pool.clone(),
        eventually::serde::Json::<CatalogEventV2>::default(),
    )
    .await?
    .with_schema_version(2);

    println!("📝  Writing V2 event (price_cents + currency)…");
    let after_v2 = v2_store
        .append(
            stream.clone(),
            version::Check::MustBe(after_v1),
            vec![Envelope::from(CatalogEventV2::ProductCreated {
                name: "Gadget".into(),
                price_cents: 2499,
                currency: "EUR".into(),
            })],
        )
        .await?;
    println!("    stream at version {after_v2}");

    // ── Write V3 event (current deploy) ───────────────────────────────────
    let v3_store = Store::new(
        pool.clone(),
        eventually::serde::Json::<CatalogEvent>::default(),
    )
    .await?
    .with_schema_version(3)
    .with_upcaster_chain(build_chain());

    println!("📝  Writing V3 event (price_cents + currency + tags)…");
    let after_v3 = v3_store
        .append(
            stream.clone(),
            version::Check::MustBe(after_v2),
            vec![Envelope::from(CatalogEvent::ProductCreated {
                name: "Doohickey".into(),
                price_cents: 3999,
                currency: "GBP".into(),
                tags: vec!["new".into(), "featured".into()],
            })],
        )
        .await?;
    println!("    stream at version {after_v3}\n");

    // ── Read all three events back through the V3 store ───────────────────
    println!("📜  Reading all events back (upcaster chain applied):");
    let events: Vec<_> = v3_store
        .stream(&stream, VersionSelect::All)
        .try_collect()
        .await?;

    assert_eq!(events.len(), 3, "must have 3 events");

    for persisted in &events {
        let meta = &persisted.event.metadata;
        let sv = meta
            .get("schema-version")
            .map(String::as_str)
            .unwrap_or("?");
        match &persisted.event.message {
            CatalogEvent::ProductCreated {
                name,
                price_cents,
                currency,
                tags,
            } => {
                println!(
                    "    v{}  ProductCreated  stored_schema={}  → name={name:?}  \
                     price_cents={price_cents}  currency={currency:?}  tags={tags:?}",
                    persisted.version, sv,
                );
            }
            CatalogEvent::ProductRenamed { new_name } => {
                println!("    v{}  ProductRenamed  → {new_name:?}", persisted.version);
            }
        }
    }

    // ── Assertions ────────────────────────────────────────────────────────
    // V1 event: price 9.99 → 999 cents, currency defaulted to USD, tags = []
    assert_eq!(
        events[0].event.message,
        CatalogEvent::ProductCreated {
            name: "Widget".into(),
            price_cents: 999,
            currency: "USD".into(),
            tags: vec![],
        },
        "V1 event must be upcasted V1→V2→V3"
    );
    println!("\n✅  V1 event: price 9.99 → 999 cents, USD, tags=[]");

    // V2 event: already has price_cents + currency, tags added by V2→V3 upcaster.
    assert_eq!(
        events[1].event.message,
        CatalogEvent::ProductCreated {
            name: "Gadget".into(),
            price_cents: 2499,
            currency: "EUR".into(),
            tags: vec![],
        },
        "V2 event must be upcasted V2→V3 only"
    );
    println!("✅  V2 event: only V2→V3 hop applied, tags=[]");

    // V3 event: stored natively, no transformation needed.
    assert_eq!(
        events[2].event.message,
        CatalogEvent::ProductCreated {
            name: "Doohickey".into(),
            price_cents: 3999,
            currency: "GBP".into(),
            tags: vec!["new".into(), "featured".into()],
        },
        "V3 event must pass through unmodified"
    );
    println!("✅  V3 event: stored natively, tags preserved");

    // ── Demonstrate unit-testing upcasters in isolation ───────────────────
    println!("\n🧪  Unit-testing upcasters in isolation:");

    let v1_payload = json!({ "ProductCreated": { "name": "Test", "price": 14.99 } });
    let chain = build_chain();

    // Simulate reading a V1 event.
    let (migrated, final_version) = chain.apply("ProductCreated", 1, v1_payload);
    println!("    input v1 → output v{final_version}: {migrated}");
    assert_eq!(final_version, 3);
    assert_eq!(migrated["ProductCreated"]["price_cents"], 1499);
    assert_eq!(migrated["ProductCreated"]["currency"], "USD");
    assert_eq!(migrated["ProductCreated"]["tags"], json!([]));
    assert!(
        migrated["ProductCreated"].get("price").is_none(),
        "old 'price' key removed"
    );

    println!("\n🎉  All assertions passed.");
    Ok(())
}
