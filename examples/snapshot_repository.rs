//! # Snapshot Repository — Periodic Snapshots + Event Replay
//!
//! This example uses the `snapshots` feature.
//!
//! Unlike the classic `aggregate::Repository` (one mutable row per aggregate),
//! this repository keeps the **full event stream** and writes a snapshot only
//! every N events — minimising snapshot writes while bounding replay cost.
//!
//! | | Classic | Snapshot (this example) |
//! |---|---|---|
//! | Event storage | Lost after each save | Full, immutable event stream |
//! | Snapshot writes | Always (one mutable row) | Only every N events |
//! | Load path | O(1) state read | Latest snapshot + delta replay |
//! | Audit trail | ❌ | ✅ |
//!
//! The example walks through:
//!
//! 1. Many saves that **don't** trigger a snapshot (version < `snapshot_every`).
//! 2. The save that **does** write a snapshot (version == `snapshot_every`).
//! 3. A load that reads the snapshot and zero delta events.
//! 4. More saves (delta events only; no new snapshot yet).
//! 5. A load that reads the snapshot + delta replay.
//! 6. Two aggregate IDs coexisting in the same tables.
//! 7. OCC conflict detection.
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
use sqlx::Row;
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

    let pool = sqlx::any::AnyPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await?;

    // ── Configure: snapshot every 3 events (low for demonstration) ────────
    //
    // In production use the default (50) or tune to your event frequency.
    // Here we use 3 so the example is short enough to read.
    let snapshot_every: usize = 3;

    let repo: Repository<InventoryItem, _, _> = Repository::new(
        pool.clone(),
        eventually::serde::Json::<InventoryItem>::default(),
        eventually::serde::Json::<InventoryEvent>::default(),
    )
    .await?
    .with_snapshot_every(snapshot_every);

    println!("📸  Snapshot strategy: every {snapshot_every} events\n");

    let id = ItemId(5001);

    // ── Saves 1-2: events only (version < snapshot_every) ─────────────────
    println!("── Phase 1: saves that do NOT trigger a snapshot ──────────────\n");

    println!("📋  Save 1 — Register item with 100 units (v1)…");
    let mut root = InventoryRoot::register(id, "Widget Pro X".into(), 100)?;
    repo.save(&mut root).await?;
    println!(
        "    v{}  [no snapshot — {} < {snapshot_every}]",
        root.version(),
        root.version()
    );

    println!("📦  Save 2 — Add 50 units (v2)…");
    {
        let mut r: InventoryRoot = repo.get(&id).await.map(InventoryRoot::from)?;
        r.add_stock(50)?;
        repo.save(&mut r).await?;
        println!(
            "    v{}  [no snapshot — {} < {snapshot_every}]",
            r.version(),
            r.version()
        );
    }

    // No snapshot rows yet — confirm with a raw query.
    let snap_count = snapshot_count(&pool, "InventoryItem").await?;
    assert_eq!(snap_count, 0, "no snapshots should exist yet");
    println!("\n    → snapshots table has {snap_count} rows ✅\n");

    // ── Save 3: triggers snapshot (version == snapshot_every) ─────────────
    println!("── Phase 2: save that DOES trigger a snapshot ─────────────────\n");

    println!("🛒  Save 3 — Remove 30 units (v3, triggers snapshot)…");
    {
        let mut r: InventoryRoot = repo.get(&id).await.map(InventoryRoot::from)?;
        r.remove_stock(30)?;
        repo.save(&mut r).await?;
        println!(
            "    v{}  [SNAPSHOT WRITTEN — {} % {snapshot_every} == 0] ✅",
            r.version(),
            r.version()
        );
    }

    let snap_count = snapshot_count(&pool, "InventoryItem").await?;
    assert_eq!(snap_count, 1, "exactly one snapshot should exist");
    println!("\n    → snapshots table now has {snap_count} row ✅\n");

    // ── Load: snapshot only, zero delta events ─────────────────────────────
    println!("── Phase 3: load using snapshot + zero delta events ───────────\n");

    let loaded: InventoryRoot = repo.get(&id).await.map(InventoryRoot::from)?;
    println!("🔄  Loaded from snapshot at v{}:", loaded.version());
    println!("    stock:   {} units  (expected 120)", loaded.stock);
    println!("    version: {}", loaded.version());
    assert_eq!(loaded.stock, 120, "100 + 50 − 30 = 120");
    assert_eq!(loaded.version(), 3);
    println!();

    // ── Saves 4-5: delta events only (no new snapshot yet) ────────────────
    println!("── Phase 4: saves after snapshot (delta events only) ──────────\n");

    println!("📦  Save 4 — Add 10 units (v4)…");
    {
        let mut r: InventoryRoot = repo.get(&id).await.map(InventoryRoot::from)?;
        r.add_stock(10)?;
        repo.save(&mut r).await?;
        println!(
            "    v{}  [no new snapshot — {} % {snapshot_every} != 0]",
            r.version(),
            r.version()
        );
    }

    println!("🛒  Save 5 — Remove 5 units (v5)…");
    {
        let mut r: InventoryRoot = repo.get(&id).await.map(InventoryRoot::from)?;
        r.remove_stock(5)?;
        repo.save(&mut r).await?;
        println!(
            "    v{}  [no new snapshot — {} % {snapshot_every} != 0]",
            r.version(),
            r.version()
        );
    }

    let snap_count = snapshot_count(&pool, "InventoryItem").await?;
    assert_eq!(snap_count, 1, "still only one snapshot");
    println!("\n    → snapshots table still has {snap_count} row ✅\n");

    // ── Load: snapshot (v3) + 2 delta events (v4, v5) ─────────────────────
    println!("── Phase 5: load via snapshot(v3) + delta replay(v4,v5) ───────\n");

    let loaded2: InventoryRoot = repo.get(&id).await.map(InventoryRoot::from)?;
    println!("🔄  Loaded: snapshot(v3) + 2 delta events:");
    println!("    stock:   {} units  (expected 125)", loaded2.stock);
    println!("    version: {}", loaded2.version());
    assert_eq!(loaded2.stock, 125, "120 + 10 − 5 = 125");
    assert_eq!(loaded2.version(), 5);
    println!();

    // ── Save 6: triggers second snapshot ──────────────────────────────────
    println!("── Phase 6: second snapshot at v6 ─────────────────────────────\n");

    println!("🔒  Save 6 — Discontinue item (v6, triggers snapshot)…");
    {
        let mut r: InventoryRoot = repo.get(&id).await.map(InventoryRoot::from)?;
        r.discontinue()?;
        repo.save(&mut r).await?;
        println!(
            "    v{}  [SNAPSHOT WRITTEN — {} % {snapshot_every} == 0] ✅",
            r.version(),
            r.version()
        );
    }

    let snap_count = snapshot_count(&pool, "InventoryItem").await?;
    assert_eq!(snap_count, 2, "two snapshots should exist now");
    println!("\n    → snapshots table now has {snap_count} rows ✅\n");

    let final_state: InventoryRoot = repo.get(&id).await.map(InventoryRoot::from)?;
    assert!(final_state.discontinued);
    assert_eq!(final_state.version(), 6);
    println!(
        "🔍  Final state: discontinued={}, v{} ✅\n",
        final_state.discontinued,
        final_state.version()
    );

    // ── Two aggregate IDs coexist ─────────────────────────────────────────
    println!("── Phase 7: two aggregate IDs coexisting ──────────────────────\n");

    let id2 = ItemId(5002);
    let mut item2 = InventoryRoot::register(id2, "Gadget Plus".into(), 200)?;
    repo.save(&mut item2).await?;

    let loaded_id2: InventoryRoot = repo.get(&id2).await.map(InventoryRoot::from)?;
    let still_id1: InventoryRoot = repo.get(&id).await.map(InventoryRoot::from)?;

    assert_eq!(loaded_id2.stock, 200);
    assert_eq!(still_id1.stock, 125, "item 5001 stock must be unchanged");
    println!(
        "    item 5001 stock: {}  item 5002 stock: {}  ✅",
        still_id1.stock, loaded_id2.stock
    );
    println!();

    // ── OCC conflict detection ────────────────────────────────────────────
    println!("── Phase 8: OCC conflict detection ────────────────────────────\n");

    let id3 = ItemId(5003);
    let mut root_a = InventoryRoot::register(id3, "Concurrent Item".into(), 50)?;
    let mut root_b = root_a.clone(); // same uncommitted events, same expected version

    repo.save(&mut root_a).await?;
    println!("    Saver A committed at v{} ✅", root_a.version());

    match repo.save(&mut root_b).await {
        Err(aggregate::repository::SaveError::Conflict(e)) => {
            println!(
                "    Saver B conflicted — expected v{}, actual v{} ✅",
                e.expected, e.actual
            );
        }
        other => panic!("expected Conflict, got: {other:?}"),
    }
    println!();

    println!("🎉  All assertions passed.");
    Ok(())
}

/// Count snapshot rows for a given aggregate type.
async fn snapshot_count(pool: &sqlx::AnyPool, aggregate_type: &str) -> anyhow::Result<i64> {
    let row = sqlx::query("SELECT COUNT(*) as cnt FROM snapshots WHERE aggregate_type = $1")
        .bind(aggregate_type)
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<i64, _>("cnt")?)
}

#[cfg(not(feature = "snapshots"))]
fn main() {
    eprintln!("This example requires the `snapshots` feature flag.");
    eprintln!(
        "Run: cargo run --example snapshot_repository --features \"sqlite,snapshots,migrations\""
    );
}
