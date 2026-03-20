//! # Bank Account — Full Aggregate Lifecycle
//!
//! This example walks through the most common Event Sourcing workflow:
//!
//! 1. Model a `BankAccount` aggregate with domain events and business rules.
//! 2. Persist it using `aggregate::Repository` (classic mutable-row mode).
//! 3. Reload it from the database — state is reconstructed by replaying events.
//! 4. Apply a second command and persist the new events.
//! 5. Attempt an invalid command and observe the domain error.
//!
//! Run with:
//! ```sh
//! cargo run --example bank_account --features "sqlite,migrations"
//! # or with Postgres:
//! DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres" \
//!   cargo run --example bank_account --features "postgres,migrations"
//! ```

use std::fmt;

use eventually::aggregate::{
    self, Aggregate,
    repository::{Getter, Saver},
};
use eventually::message::Message;
use eventually_any::aggregate::Repository;
use serde::{Deserialize, Serialize};
use sqlx::any::install_default_drivers;

// ── Aggregate ID ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountId(pub u64);

impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ── Domain events ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BankAccountEvent {
    // KEY DESIGN POINT: the aggregate ID is embedded in the `Opened` event.
    //
    // `Aggregate::apply` receives only the event — it has no other way to know
    // the ID.  The serialized state blob stored in the `aggregates` table must
    // have the real ID so that after a load/deserialize cycle `aggregate_id()`
    // returns the correct value.  If `aggregate_id()` returns a placeholder the
    // repository cannot find the row on the next `get()` call.
    Opened {
        id: AccountId,
        owner: String,
        initial_deposit: u64,
    },
    Deposited {
        amount: u64,
    },
    Withdrawn {
        amount: u64,
    },
    Closed,
}

impl Message for BankAccountEvent {
    fn name(&self) -> &'static str {
        match self {
            BankAccountEvent::Opened { .. } => "BankAccountOpened",
            BankAccountEvent::Deposited { .. } => "BankAccountDeposited",
            BankAccountEvent::Withdrawn { .. } => "BankAccountWithdrawn",
            BankAccountEvent::Closed => "BankAccountClosed",
        }
    }
}

// ── Aggregate state ───────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BankAccount {
    id: AccountId,
    owner: String,
    balance: u64,
    is_closed: bool,
}

// ── Domain errors ─────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum BankAccountError {
    #[error("account is already open")]
    AlreadyOpen,
    #[error("account does not exist yet")]
    NotYetOpen,
    #[error("account is closed")]
    AlreadyClosed,
    #[error("insufficient funds: balance is {balance}, tried to withdraw {amount}")]
    InsufficientFunds { balance: u64, amount: u64 },
}

// ── Aggregate impl ────────────────────────────────────────────────────────

impl Aggregate for BankAccount {
    type Id = AccountId;
    type Event = BankAccountEvent;
    type Error = BankAccountError;

    fn type_name() -> &'static str {
        "BankAccount"
    }
    fn aggregate_id(&self) -> &Self::Id {
        &self.id
    }

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, Self::Error> {
        match (state, event) {
            // The id from the event becomes self.id — this is what survives
            // serialization and makes aggregate_id() correct after a reload.
            (
                None,
                BankAccountEvent::Opened {
                    id,
                    owner,
                    initial_deposit,
                },
            ) => Ok(BankAccount {
                id,
                owner,
                balance: initial_deposit,
                is_closed: false,
            }),
            (Some(_), BankAccountEvent::Opened { .. }) => Err(BankAccountError::AlreadyOpen),
            (None, _) => Err(BankAccountError::NotYetOpen),

            (Some(mut acc), BankAccountEvent::Deposited { amount }) => {
                if acc.is_closed {
                    return Err(BankAccountError::AlreadyClosed);
                }
                acc.balance += amount;
                Ok(acc)
            }
            (Some(mut acc), BankAccountEvent::Withdrawn { amount }) => {
                if acc.is_closed {
                    return Err(BankAccountError::AlreadyClosed);
                }
                if acc.balance < amount {
                    return Err(BankAccountError::InsufficientFunds {
                        balance: acc.balance,
                        amount,
                    });
                }
                acc.balance -= amount;
                Ok(acc)
            }
            (Some(mut acc), BankAccountEvent::Closed) => {
                acc.is_closed = true;
                Ok(acc)
            }
        }
    }
}

// ── Aggregate root commands ───────────────────────────────────────────────

/// Thin newtype around `aggregate::Root<BankAccount>` that exposes
/// intent-revealing command methods instead of raw event recording.
#[eventually_macros::aggregate_root(BankAccount)]
#[derive(Debug, Clone, PartialEq)]
pub struct BankAccountRoot;

impl BankAccountRoot {
    pub fn open(
        id: AccountId,
        owner: String,
        initial_deposit: u64,
    ) -> Result<Self, BankAccountError> {
        Ok(aggregate::Root::<BankAccount>::record_new(
            // Pass the real id into the event so apply() stores it in state.
            BankAccountEvent::Opened {
                id,
                owner,
                initial_deposit,
            }
            .into(),
        )?
        .into())
    }

    pub fn deposit(&mut self, amount: u64) -> Result<(), BankAccountError> {
        self.record_that(BankAccountEvent::Deposited { amount }.into())
    }

    pub fn withdraw(&mut self, amount: u64) -> Result<(), BankAccountError> {
        self.record_that(BankAccountEvent::Withdrawn { amount }.into())
    }

    pub fn close(&mut self) -> Result<(), BankAccountError> {
        if self.is_closed {
            return Err(BankAccountError::AlreadyClosed);
        }
        self.record_that(BankAccountEvent::Closed.into())
    }
}

// ── Main ──────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    install_default_drivers();

    let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite::memory:".to_string());
    println!("🔌  Connecting to: {db_url}");

    let pool = sqlx::any::AnyPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await?;

    // Spell out the serde type parameters explicitly so Rust can infer the
    // Repository type.  We use the fully-qualified path `eventually::serde::Json`
    // rather than `use eventually::serde` to avoid shadowing the `serde` crate
    // name that the derive macros above rely on.
    let repo: Repository<BankAccount, _, _> = Repository::new(
        pool,
        eventually::serde::Json::<BankAccount>::default(),
        eventually::serde::Json::<BankAccountEvent>::default(),
    )
    .await?;

    let id = AccountId(1001);

    // ── 1. Open account ────────────────────────────────────────────────────
    println!("\n📂  Opening account #{} for 'Alice'…", id.0);
    let mut root = BankAccountRoot::open(id, "Alice".into(), 500)?;
    repo.save(&mut root).await?;
    println!("    ✅  Saved at version {}", root.version());

    // ── 2. Reload from DB ─────────────────────────────────────────────────
    // State is reconstructed by replaying events through BankAccount::apply.
    // Because the id was stored inside the Opened event and therefore inside
    // the serialized state blob, aggregate_id() returns AccountId(1001) after
    // the reload — the repository can find the correct row.
    let mut loaded: BankAccountRoot = repo.get(&id).await.map(BankAccountRoot::from)?;
    println!("\n🔄  Reloaded from DB:");
    println!("    owner:   {}", loaded.owner);
    println!("    balance: {} €", loaded.balance);
    println!("    version: {}", loaded.version());

    // ── 3. Deposit & withdraw ──────────────────────────────────────────────
    println!("\n💰  Depositing 200 €…");
    loaded.deposit(200)?;
    println!("💸  Withdrawing 50 €…");
    loaded.withdraw(50)?;
    repo.save(&mut loaded).await?;
    println!("    ✅  Saved at version {}", loaded.version());

    let reloaded: BankAccountRoot = repo.get(&id).await.map(BankAccountRoot::from)?;
    println!("\n🔄  Reloaded after deposit/withdraw:");
    println!("    balance: {} €  (expected: 650)", reloaded.balance);
    assert_eq!(reloaded.balance, 650, "balance must be 500 + 200 - 50");

    // ── 4. Domain error: insufficient funds ───────────────────────────────
    let mut account: BankAccountRoot = repo.get(&id).await.map(BankAccountRoot::from)?;
    println!("\n🚫  Attempting to overdraft 10 000 €…");
    let err = account.withdraw(10_000).unwrap_err();
    println!("    Got expected error: {err}");
    assert!(matches!(err, BankAccountError::InsufficientFunds { .. }));

    // ── 5. Close the account ───────────────────────────────────────────────
    println!("\n🔒  Closing account…");
    account.close()?;
    repo.save(&mut account).await?;
    println!("    ✅  Closed at version {}", account.version());

    // Any command on a closed account must now fail.
    let err = account.deposit(1).unwrap_err();
    println!("\n🚫  Deposit on closed account: {err}");
    assert_eq!(err, BankAccountError::AlreadyClosed);

    println!("\n🎉  All assertions passed.");
    Ok(())
}
