//! # Order Lifecycle — Event Store Streaming & Metadata
//!
//! This example uses the raw `event::Store` (without an aggregate wrapper) to
//! show how to work with event streams directly.  Topics covered:
//!
//! - Appending events in multiple batches (simulating commands over time).
//! - Streaming all events with `VersionSelect::All`.
//! - Streaming a subset with `VersionSelect::From(n)` (useful for projections
//!   that resume from a known checkpoint).
//! - Reading the `recorded-at` timestamp and `schema-version` from metadata.
//! - Replaying the stream to rebuild a read-model projection.
//!
//! Run with:
//! ```sh
//! cargo run --example order_lifecycle --features sqlite,migrations
//! ```

use std::collections::HashMap;

use eventually::event::store::{Appender, Streamer};
use eventually::event::{Envelope, VersionSelect};
use eventually::message::Message;
use eventually::version;
use eventually_any::event::Store;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use sqlx::any::install_default_drivers;

// ── Domain events ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderLine {
    pub sku: String,
    pub qty: u32,
    pub unit_price_cents: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderEvent {
    Placed {
        customer_id: String,
        lines: Vec<OrderLine>,
    },
    PaymentReceived {
        amount_cents: u64,
        transaction_id: String,
    },
    Shipped {
        tracking_number: String,
        carrier: String,
    },
    Delivered,
    ReturnRequested {
        reason: String,
    },
    Refunded {
        amount_cents: u64,
    },
}

impl Message for OrderEvent {
    fn name(&self) -> &'static str {
        match self {
            OrderEvent::Placed { .. } => "OrderPlaced",
            OrderEvent::PaymentReceived { .. } => "OrderPaymentReceived",
            OrderEvent::Shipped { .. } => "OrderShipped",
            OrderEvent::Delivered => "OrderDelivered",
            OrderEvent::ReturnRequested { .. } => "OrderReturnRequested",
            OrderEvent::Refunded { .. } => "OrderRefunded",
        }
    }
}

// ── Projection: Order summary ─────────────────────────────────────────────

#[derive(Debug, Default)]
pub struct OrderSummary {
    pub customer_id: String,
    pub total_cents: u64,
    pub status: String,
    pub tracking: Option<String>,
    pub events_seen: u64,
}

impl OrderSummary {
    /// Fold a single event into the projection.
    pub fn apply(&mut self, event: &OrderEvent, version: u64) {
        self.events_seen = version;
        match event {
            OrderEvent::Placed { customer_id, lines } => {
                self.customer_id = customer_id.clone();
                self.total_cents = lines
                    .iter()
                    .map(|l| l.qty as u64 * l.unit_price_cents)
                    .sum();
                self.status = "placed".into();
            }
            OrderEvent::PaymentReceived { .. } => {
                self.status = "paid".into();
            }
            OrderEvent::Shipped {
                tracking_number, ..
            } => {
                self.status = "shipped".into();
                self.tracking = Some(tracking_number.clone());
            }
            OrderEvent::Delivered => {
                self.status = "delivered".into();
            }
            OrderEvent::ReturnRequested { .. } => {
                self.status = "return_requested".into();
            }
            OrderEvent::Refunded { .. } => {
                self.status = "refunded".into();
            }
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

    let store = Store::new(pool, eventually::serde::Json::<OrderEvent>::default()).await?;

    let order_id = "order-9871".to_string();

    // ── Batch 1: place the order ───────────────────────────────────────────
    println!("📦  Placing order…");
    let v1 = store
        .append(
            order_id.clone(),
            version::Check::MustBe(0),
            vec![Envelope::from(OrderEvent::Placed {
                customer_id: "cust-42".into(),
                lines: vec![
                    OrderLine {
                        sku: "WIDGET-A".into(),
                        qty: 2,
                        unit_price_cents: 1999,
                    },
                    OrderLine {
                        sku: "GADGET-B".into(),
                        qty: 1,
                        unit_price_cents: 4999,
                    },
                ],
            })],
        )
        .await?;
    println!("    stream version: {v1}");

    // ── Batch 2: payment ───────────────────────────────────────────────────
    println!("💳  Recording payment…");
    let v2 = store
        .append(
            order_id.clone(),
            version::Check::MustBe(v1),
            vec![Envelope::from(OrderEvent::PaymentReceived {
                amount_cents: 8997,
                transaction_id: "txn-00123".into(),
            })],
        )
        .await?;
    println!("    stream version: {v2}");

    // ── Batch 3: ship + deliver ────────────────────────────────────────────
    println!("🚚  Shipping and delivering…");
    let v3 = store
        .append(
            order_id.clone(),
            version::Check::MustBe(v2),
            vec![
                Envelope::from(OrderEvent::Shipped {
                    tracking_number: "1Z999AA10123456784".into(),
                    carrier: "UPS".into(),
                }),
                Envelope::from(OrderEvent::Delivered),
            ],
        )
        .await?;
    println!("    stream version: {v3}");

    // ── Stream ALL events and rebuild projection ───────────────────────────
    println!("\n📜  Replaying full event stream (VersionSelect::All):");
    let all_events: Vec<_> = store
        .stream(&order_id, VersionSelect::All)
        .try_collect()
        .await?;

    let mut summary = OrderSummary::default();
    for persisted in &all_events {
        let meta = &persisted.event.metadata;
        let recorded_at = meta.get("recorded-at").map(String::as_str).unwrap_or("?");
        let schema_v = meta
            .get("schema-version")
            .map(String::as_str)
            .unwrap_or("1");
        println!(
            "    v{:>2}  {:25}  recorded-at={recorded_at}  schema-v={schema_v}",
            persisted.version,
            persisted.event.message.name(),
        );
        summary.apply(&persisted.event.message, persisted.version);
    }

    println!("\n📊  Projection after full replay:");
    println!("    customer:  {}", summary.customer_id);
    println!("    total:     {:.2} €", summary.total_cents as f64 / 100.0);
    println!("    status:    {}", summary.status);
    println!("    tracking:  {:?}", summary.tracking);

    // ── Resume from a checkpoint using VersionSelect::From ────────────────
    // Imagine a projection handler that crashed after processing v2.
    // It can resume from v3 without re-processing earlier events.
    let checkpoint = 2u64;
    println!("\n⏩  Resuming projection from checkpoint (v{checkpoint}):");

    let delta_events: Vec<_> = store
        .stream(&order_id, VersionSelect::From(checkpoint + 1))
        .try_collect()
        .await?;

    println!("    {} new event(s) since checkpoint:", delta_events.len());
    for p in &delta_events {
        println!("    v{}  {}", p.version, p.event.message.name());
    }

    assert_eq!(delta_events.len(), 2, "should see Shipped + Delivered");

    // ── Aggregate event counts across multiple orders ──────────────────────
    println!("\n📈  Appending a second order stream to demonstrate multi-stream…");
    let order2_id = "order-9872".to_string();
    store
        .append(
            order2_id.clone(),
            version::Check::MustBe(0),
            vec![Envelope::from(OrderEvent::Placed {
                customer_id: "cust-99".into(),
                lines: vec![OrderLine {
                    sku: "THING-C".into(),
                    qty: 3,
                    unit_price_cents: 599,
                }],
            })],
        )
        .await?;

    // Streams are fully independent.
    let counts: HashMap<String, usize> = {
        let mut m = HashMap::new();
        for id in [&order_id, &order2_id] {
            let n: Vec<_> = store.stream(id, VersionSelect::All).try_collect().await?;
            m.insert(id.clone(), n.len());
        }
        m
    };
    println!("    {order_id}: {} events", counts[&order_id]);
    println!("    {order2_id}: {} events", counts[&order2_id]);

    println!("\n🎉  All assertions passed.");
    Ok(())
}
