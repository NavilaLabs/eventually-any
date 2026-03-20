//! # Snapshot Repository — Audit Trail & Delta Replay
//!
//! This example uses the `snapshots` feature, which switches the aggregate
//! repository from mutable-row semantics to append-only snapshot semantics.
//!
//! | | Classic | Snapshot |
//! |---|---|---|
//! | Storage | One row per aggregate (UPDATE) | One row per save (INSERT) |
//! | History | ❌ Previous states are lost | ✅ Full audit trail |
//! | Read cost | O(1) | O(1) snapshot + tiny delta |
//!
//! This example shows:
//!
//! 1. The full save/load cycle with the snapshot repository.
//! 2. That the audit trail grows with each save — nothing is overwritten.
//! 3. Delta replay: load from a snapshot, apply a new event, verify state.
//! 4. Two aggregate IDs coexisting in the same `snapshots` table.
//!
//! Run with:
//! ```sh
//! cargo run --example snapshot_repository --features "sqlite,snapshots,migrations"
//! ```

use std::fmt;

use eventually::aggregate::{
    self, Aggregate,
    repository::{Getter, Saver},
};
use eventually::message::Message;
use eventually_any::snapshot::Repository;
use serde::{Deserialize, Serialize};
use sqlx::any::install_default_drivers;

// ── Aggregate ID ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemId(pub u32);

impl fmt::Display for ItemId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "item:{}", self.0)
    }
}

// ── Domain events ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InventoryEvent {
    // ItemId embedded so apply() can set self.id to the real value.
    ItemRegistered {
        id: ItemId,
        name: String,
        stock: u32,
    },
    StockAdded {
        quantity: u32,
    },
    StockRemoved {
        quantity: u32,
    },
    Discontinued,
}

impl Message for InventoryEvent {
    fn name(&self) -> &'static str {
        match self {
            InventoryEvent::ItemRegistered { .. } => "ItemRegistered",
            InventoryEvent::StockAdded { .. } => "StockAdded",
            InventoryEvent::StockRemoved { .. } => "StockRemoved",
            InventoryEvent::Discontinued => "ItemDiscontinued",
        }
    }
}

// ── Aggregate state ───────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryItem {
    id: ItemId,
    name: String,
    stock: u32,
    discontinued: bool,
}

// ── Domain errors ─────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum InventoryError {
    #[error("item not registered yet")]
    NotRegistered,
    #[error("already registered")]
    AlreadyRegistered,
    #[error("item is discontinued")]
    Discontinued,
    #[error("insufficient stock: have {have}, need {need}")]
    InsufficientStock { have: u32, need: u32 },
}

// ── Aggregate impl ────────────────────────────────────────────────────────

impl Aggregate for InventoryItem {
    type Id = ItemId;
    type Event = InventoryEvent;
    type Error = InventoryError;

    fn type_name() -> &'static str {
        "InventoryItem"
    }
    fn aggregate_id(&self) -> &Self::Id {
        &self.id
    }

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, Self::Error> {
        match (state, event) {
            (None, InventoryEvent::ItemRegistered { id, name, stock }) => Ok(InventoryItem {
                id,
                name,
                stock,
                discontinued: false,
            }),
            (Some(_), InventoryEvent::ItemRegistered { .. }) => {
                Err(InventoryError::AlreadyRegistered)
            }
            (None, _) => Err(InventoryError::NotRegistered),
            (Some(mut item), InventoryEvent::StockAdded { quantity }) => {
                item.stock += quantity;
                Ok(item)
            }
            (Some(mut item), InventoryEvent::StockRemoved { quantity }) => {
                if item.stock < quantity {
                    return Err(InventoryError::InsufficientStock {
                        have: item.stock,
                        need: quantity,
                    });
                }
                item.stock -= quantity;
                Ok(item)
            }
            (Some(mut item), InventoryEvent::Discontinued) => {
                item.discontinued = true;
                Ok(item)
            }
        }
    }
}

// ── Aggregate root commands ───────────────────────────────────────────────

#[eventually_macros::aggregate_root(InventoryItem)]
#[derive(Debug, Clone, PartialEq)]
pub struct InventoryRoot;

impl InventoryRoot {
    pub fn register(id: ItemId, name: String, initial_stock: u32) -> Result<Self, InventoryError> {
        Ok(aggregate::Root::<InventoryItem>::record_new(
            InventoryEvent::ItemRegistered {
                id,
                name,
                stock: initial_stock,
            }
            .into(),
        )?
        .into())
    }

    pub fn add_stock(&mut self, qty: u32) -> Result<(), InventoryError> {
        self.record_that(InventoryEvent::StockAdded { quantity: qty }.into())
    }

    pub fn remove_stock(&mut self, qty: u32) -> Result<(), InventoryError> {
        self.record_that(InventoryEvent::StockRemoved { quantity: qty }.into())
    }

    pub fn discontinue(&mut self) -> Result<(), InventoryError> {
        self.record_that(InventoryEvent::Discontinued.into())
    }
}

// ── Main ──────────────────────────────────────────────────────────────────

#[tokio::main]
#[cfg(feature = "snapshots")]
async fn main() -> anyhow::Result<()> {
    install_default_drivers();

    let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite::memory:".to_string());
    println!("🔌  Connecting to: {db_url}");
    println!("📸  Using SNAPSHOT repository mode\n");

    let pool = sqlx::any::AnyPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await?;

    // Use the fully-qualified path to avoid shadowing the `serde` crate.
    let repo: Repository<InventoryItem, _, _> = Repository::new(
        pool.clone(),
        eventually::serde::Json::<InventoryItem>::default(),
        eventually::serde::Json::<InventoryEvent>::default(),
    )
    .await?;

    let id = ItemId(5001);

    // ── Save 1: register ──────────────────────────────────────────────────
    println!("📋  Save 1 — Register item with 100 units…");
    let mut root = InventoryRoot::register(id, "Widget Pro X".into(), 100)?;
    repo.save(&mut root).await?;
    println!("    snapshot written at version {}", root.version());

    // ── Save 2: add stock ─────────────────────────────────────────────────
    println!("📦  Save 2 — Add 50 units…");
    {
        let mut r: InventoryRoot = repo.get(&id).await.map(InventoryRoot::from)?;
        r.add_stock(50)?;
        repo.save(&mut r).await?;
        println!("    snapshot written at version {}", r.version());
    }

    // ── Save 3: remove stock ──────────────────────────────────────────────
    println!("🛒  Save 3 — Remove 30 units (sale)…");
    {
        let mut r: InventoryRoot = repo.get(&id).await.map(InventoryRoot::from)?;
        r.remove_stock(30)?;
        repo.save(&mut r).await?;
        println!("    snapshot written at version {}", r.version());
    }

    let current: InventoryRoot = repo.get(&id).await.map(InventoryRoot::from)?;
    println!("\n🔄  State after 3 saves:");
    println!("    name:    {}", current.name);
    println!("    stock:   {} units  (expected: 120)", current.stock);
    println!("    version: {}", current.version());
    assert_eq!(current.stock, 120, "100 + 50 - 30 = 120");

    // ── Audit trail ───────────────────────────────────────────────────────
    // Unlike the classic repository (which overwrites a single row), the
    // snapshot table retains every historical row — one per save.
    println!("\n📚  Audit trail from snapshots table:");
    let rows = sqlx::query(
        r#"SELECT "version", state FROM snapshots
           WHERE aggregate_type = 'InventoryItem'
           ORDER BY "version" ASC"#,
    )
    .fetch_all(&pool)
    .await?;

    for row in &rows {
        use sqlx::Row;
        let v: i32 = row.try_get("version")?;
        let bytes: Vec<u8> = row.try_get("state")?;
        let item: InventoryItem = serde_json::from_slice(&bytes)?;
        println!(
            "    v{v:>2}  stock = {:>4}  name = {}",
            item.stock, item.name
        );
    }
    assert_eq!(rows.len(), 3, "three snapshot rows must exist");
    println!("    → {} historical snapshots preserved ✅", rows.len());

    // ── Delta replay ──────────────────────────────────────────────────────
    // get() loads the latest snapshot (v3) then streams any events recorded
    // after it.  Here we add one more event, so the next get() finds snapshot
    // v4 directly with zero delta.
    println!("\n⚡  Save 4 — Discontinue item…");
    {
        let mut r: InventoryRoot = repo.get(&id).await.map(InventoryRoot::from)?;
        assert_eq!(r.version(), 3);
        r.discontinue()?;
        repo.save(&mut r).await?;
        println!("    snapshot written at version {}", r.version());
    }

    let final_state: InventoryRoot = repo.get(&id).await.map(InventoryRoot::from)?;
    println!("\n🔍  Final state (snapshot v4):");
    println!("    discontinued: {}", final_state.discontinued);
    println!("    version:      {}", final_state.version());
    assert!(final_state.discontinued);
    assert_eq!(final_state.version(), 4);

    // ── Two aggregate IDs coexist ─────────────────────────────────────────
    // The snapshots table uses (aggregate_type, aggregate_id) to discriminate,
    // so different items are completely independent.
    println!("\n🗂️   Second item alongside the first…");
    let id2 = ItemId(5002);
    let mut item2 = InventoryRoot::register(id2, "Gadget Plus".into(), 200)?;
    repo.save(&mut item2).await?;

    let loaded2: InventoryRoot = repo.get(&id2).await.map(InventoryRoot::from)?;
    assert_eq!(loaded2.stock, 200);
    let still_5001: InventoryRoot = repo.get(&id).await.map(InventoryRoot::from)?;
    assert_eq!(still_5001.stock, 120, "item 5001 stock must be unchanged");

    println!(
        "    item 5001 stock: {} ✅   item 5002 stock: {} ✅",
        still_5001.stock, loaded2.stock
    );

    println!("\n🎉  All assertions passed.");
    Ok(())
}

#[cfg(not(feature = "snapshots"))]
fn main() {
    eprintln!("This example requires the `snapshots` feature flag.");
    eprintln!(
        "Run: cargo run --example snapshot_repository --features \"sqlite,snapshots,migrations\""
    );
}
