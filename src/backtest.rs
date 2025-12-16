// src/backtest.rs
// Backtesting framework with SQLite persistence and slippage tracking

use anyhow::Result;
use rusqlite::{Connection, params};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::info;

use crate::strategy::{MarketSnapshot, StrategySignal, StrategyManager, StrategyConfig};
use crate::types::ArbType;

/// Database path for backtest results
const DB_PATH: &str = "backtest_results.db";

/// Opportunity state for tracking lifecycle
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpportunityState {
    /// Just detected, not yet validated
    Detected,
    /// Still valid on next price update
    Confirmed,
    /// Disappeared before we could act (slippage)
    Slipped,
    /// Closed naturally (would have been profitable)
    ClosedProfitable,
    /// Market resolved
    Expired,
}

impl OpportunityState {
    pub fn as_str(&self) -> &'static str {
        match self {
            OpportunityState::Detected => "detected",
            OpportunityState::Confirmed => "confirmed",
            OpportunityState::Slipped => "slipped",
            OpportunityState::ClosedProfitable => "closed_profitable",
            OpportunityState::Expired => "expired",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "detected" => OpportunityState::Detected,
            "confirmed" => OpportunityState::Confirmed,
            "slipped" => OpportunityState::Slipped,
            "closed_profitable" => OpportunityState::ClosedProfitable,
            "expired" => OpportunityState::Expired,
            _ => OpportunityState::Detected,
        }
    }
}

/// Tracked opportunity with slippage data
#[derive(Debug, Clone)]
pub struct TrackedOpportunity {
    pub id: i64,
    pub strategy_id: String,
    pub market_id: u16,
    pub market_description: String,
    pub arb_type: ArbType,

    // Detection state
    pub detected_at_ns: u64,
    pub detected_cost: u16,
    pub detected_profit: i16,
    pub detected_yes_price: u16,
    pub detected_no_price: u16,
    pub detected_yes_size: u16,
    pub detected_no_size: u16,
    pub detected_confidence: f64,

    // Slippage tracking
    pub state: OpportunityState,
    pub ticks_valid: u32,           // How many price updates it remained valid
    pub last_seen_ns: u64,          // Last time we saw this opportunity
    pub duration_ns: u64,           // Total duration opportunity was available

    // Price movement tracking
    pub worst_cost_seen: u16,       // Worst cost during opportunity window
    pub best_cost_seen: u16,        // Best cost during opportunity window
    pub final_cost: Option<u16>,    // Cost when opportunity closed

    // Estimated execution quality
    pub estimated_fill_prob: f64,   // Based on liquidity and duration
    pub estimated_slippage: i16,    // Estimated slippage in cents
    pub adjusted_profit: i16,       // Profit after estimated slippage
}

impl TrackedOpportunity {
    /// Create a new tracked opportunity from a signal
    pub fn from_signal(signal: &StrategySignal, market_description: &str) -> Self {
        let min_size = signal.yes_size.min(signal.no_size);

        // Estimate fill probability based on liquidity
        // Very rough heuristic: more liquidity = higher fill probability
        let fill_prob = if min_size >= 1000 {
            0.8  // $10+ liquidity
        } else if min_size >= 500 {
            0.6  // $5+ liquidity
        } else if min_size >= 200 {
            0.4  // $2+ liquidity
        } else {
            0.2  // Low liquidity
        };

        Self {
            id: 0,  // Set by database
            strategy_id: signal.strategy_id.clone(),
            market_id: signal.market_id,
            market_description: market_description.to_string(),
            arb_type: signal.arb_type,
            detected_at_ns: signal.timestamp_ns,
            detected_cost: signal.cost_cents,
            detected_profit: signal.profit_cents,
            detected_yes_price: signal.yes_price,
            detected_no_price: signal.no_price,
            detected_yes_size: signal.yes_size,
            detected_no_size: signal.no_size,
            detected_confidence: signal.confidence,
            state: OpportunityState::Detected,
            ticks_valid: 1,
            last_seen_ns: signal.timestamp_ns,
            duration_ns: 0,
            worst_cost_seen: signal.cost_cents,
            best_cost_seen: signal.cost_cents,
            final_cost: None,
            estimated_fill_prob: fill_prob,
            estimated_slippage: 0,
            adjusted_profit: signal.profit_cents,
        }
    }

    /// Update opportunity with new price data
    pub fn update(&mut self, still_valid: bool, current_cost: Option<u16>, timestamp_ns: u64) {
        if still_valid {
            self.ticks_valid += 1;
            self.state = OpportunityState::Confirmed;
            self.last_seen_ns = timestamp_ns;
            self.duration_ns = timestamp_ns.saturating_sub(self.detected_at_ns);

            if let Some(cost) = current_cost {
                self.worst_cost_seen = self.worst_cost_seen.max(cost);
                self.best_cost_seen = self.best_cost_seen.min(cost);
            }

            // Increase fill probability with duration (more time = more likely to fill)
            // But cap at reasonable level
            let duration_bonus = (self.duration_ns as f64 / 1_000_000_000.0) * 0.1;  // 10% per second
            self.estimated_fill_prob = (self.estimated_fill_prob + duration_bonus).min(0.95);

        } else {
            // Opportunity disappeared
            self.state = OpportunityState::Slipped;
            self.duration_ns = timestamp_ns.saturating_sub(self.detected_at_ns);
            self.final_cost = current_cost;

            // Calculate slippage
            if let Some(cost) = current_cost {
                self.estimated_slippage = cost as i16 - self.detected_cost as i16;
            }

            // Reduce fill probability based on how quickly it slipped
            if self.ticks_valid <= 1 {
                self.estimated_fill_prob *= 0.2;  // Very unlikely if disappeared immediately
            } else if self.ticks_valid <= 3 {
                self.estimated_fill_prob *= 0.5;
            }

            // Calculate adjusted profit
            self.adjusted_profit = self.detected_profit - self.estimated_slippage.max(0);
        }
    }

    /// Mark as closed profitably (would have made money)
    pub fn close_profitable(&mut self, timestamp_ns: u64) {
        self.state = OpportunityState::ClosedProfitable;
        self.duration_ns = timestamp_ns.saturating_sub(self.detected_at_ns);
        self.estimated_fill_prob = 0.9;  // High confidence if it lasted
    }
}

/// SQLite-backed backtest data store
pub struct BacktestDb {
    conn: Connection,
}

impl BacktestDb {
    /// Open or create the database
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path)?;
        let db = Self { conn };
        db.init_schema()?;
        Ok(db)
    }

    /// Open default database
    pub fn open_default() -> Result<Self> {
        Self::open(DB_PATH)
    }

    /// Initialize database schema
    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(r#"
            -- Strategy configurations
            CREATE TABLE IF NOT EXISTS strategies (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                params_json TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            -- Market snapshots (sampled, not every tick)
            CREATE TABLE IF NOT EXISTS market_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id INTEGER NOT NULL,
                market_description TEXT,
                kalshi_yes INTEGER,
                kalshi_no INTEGER,
                kalshi_yes_size INTEGER,
                kalshi_no_size INTEGER,
                poly_yes INTEGER,
                poly_no INTEGER,
                poly_yes_size INTEGER,
                poly_no_size INTEGER,
                timestamp_ns INTEGER NOT NULL,
                session_id TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            -- Detected opportunities
            CREATE TABLE IF NOT EXISTS opportunities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                market_id INTEGER NOT NULL,
                market_description TEXT,
                arb_type TEXT NOT NULL,

                -- Detection data
                detected_at_ns INTEGER NOT NULL,
                detected_cost INTEGER NOT NULL,
                detected_profit INTEGER NOT NULL,
                detected_yes_price INTEGER NOT NULL,
                detected_no_price INTEGER NOT NULL,
                detected_yes_size INTEGER NOT NULL,
                detected_no_size INTEGER NOT NULL,
                detected_confidence REAL NOT NULL,

                -- Lifecycle data
                state TEXT NOT NULL,
                ticks_valid INTEGER NOT NULL DEFAULT 1,
                last_seen_ns INTEGER,
                duration_ns INTEGER,

                -- Price movement
                worst_cost_seen INTEGER,
                best_cost_seen INTEGER,
                final_cost INTEGER,

                -- Execution estimates
                estimated_fill_prob REAL,
                estimated_slippage INTEGER,
                adjusted_profit INTEGER,

                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            -- Session metadata
            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                markets_tracked INTEGER DEFAULT 0,
                opportunities_detected INTEGER DEFAULT 0,
                config_json TEXT
            );

            -- Indexes for common queries
            CREATE INDEX IF NOT EXISTS idx_opportunities_session ON opportunities(session_id);
            CREATE INDEX IF NOT EXISTS idx_opportunities_strategy ON opportunities(strategy_id);
            CREATE INDEX IF NOT EXISTS idx_opportunities_state ON opportunities(state);
            CREATE INDEX IF NOT EXISTS idx_opportunities_profit ON opportunities(detected_profit);
            CREATE INDEX IF NOT EXISTS idx_snapshots_market ON market_snapshots(market_id);
            CREATE INDEX IF NOT EXISTS idx_snapshots_session ON market_snapshots(session_id);
        "#)?;
        Ok(())
    }

    /// Save a strategy configuration
    pub fn save_strategy(&self, strategy: &StrategyConfig) -> Result<()> {
        let params_json = serde_json::to_string(&strategy.params)?;
        self.conn.execute(
            "INSERT OR REPLACE INTO strategies (id, name, description, params_json, enabled) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![strategy.id, strategy.name, strategy.description, params_json, strategy.enabled],
        )?;
        Ok(())
    }

    /// Start a new session
    pub fn start_session(&self, session_id: &str, config: Option<&str>) -> Result<()> {
        self.conn.execute(
            "INSERT INTO sessions (id, started_at, config_json) VALUES (?1, datetime('now'), ?2)",
            params![session_id, config],
        )?;
        Ok(())
    }

    /// End a session
    pub fn end_session(&self, session_id: &str, markets_tracked: u32, opportunities: u32) -> Result<()> {
        self.conn.execute(
            "UPDATE sessions SET ended_at = datetime('now'), markets_tracked = ?2, opportunities_detected = ?3 WHERE id = ?1",
            params![session_id, markets_tracked, opportunities],
        )?;
        Ok(())
    }

    /// Save a market snapshot (sampled)
    pub fn save_snapshot(&self, snapshot: &MarketSnapshot, description: &str, session_id: &str) -> Result<()> {
        self.conn.execute(
            r#"INSERT INTO market_snapshots
               (market_id, market_description, kalshi_yes, kalshi_no, kalshi_yes_size, kalshi_no_size,
                poly_yes, poly_no, poly_yes_size, poly_no_size, timestamp_ns, session_id)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)"#,
            params![
                snapshot.market_id, description,
                snapshot.kalshi_yes, snapshot.kalshi_no, snapshot.kalshi_yes_size, snapshot.kalshi_no_size,
                snapshot.poly_yes, snapshot.poly_no, snapshot.poly_yes_size, snapshot.poly_no_size,
                snapshot.timestamp_ns, session_id
            ],
        )?;
        Ok(())
    }

    /// Insert a new opportunity
    pub fn insert_opportunity(&self, opp: &TrackedOpportunity, session_id: &str) -> Result<i64> {
        self.conn.execute(
            r#"INSERT INTO opportunities
               (session_id, strategy_id, market_id, market_description, arb_type,
                detected_at_ns, detected_cost, detected_profit, detected_yes_price, detected_no_price,
                detected_yes_size, detected_no_size, detected_confidence,
                state, ticks_valid, last_seen_ns, duration_ns,
                worst_cost_seen, best_cost_seen, final_cost,
                estimated_fill_prob, estimated_slippage, adjusted_profit)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23)"#,
            params![
                session_id, opp.strategy_id, opp.market_id, opp.market_description,
                format!("{:?}", opp.arb_type),
                opp.detected_at_ns, opp.detected_cost, opp.detected_profit,
                opp.detected_yes_price, opp.detected_no_price, opp.detected_yes_size, opp.detected_no_size,
                opp.detected_confidence, opp.state.as_str(), opp.ticks_valid, opp.last_seen_ns, opp.duration_ns,
                opp.worst_cost_seen, opp.best_cost_seen, opp.final_cost,
                opp.estimated_fill_prob, opp.estimated_slippage, opp.adjusted_profit
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Update an existing opportunity
    pub fn update_opportunity(&self, opp: &TrackedOpportunity) -> Result<()> {
        self.conn.execute(
            r#"UPDATE opportunities SET
               state = ?2, ticks_valid = ?3, last_seen_ns = ?4, duration_ns = ?5,
               worst_cost_seen = ?6, best_cost_seen = ?7, final_cost = ?8,
               estimated_fill_prob = ?9, estimated_slippage = ?10, adjusted_profit = ?11,
               updated_at = datetime('now')
               WHERE id = ?1"#,
            params![
                opp.id, opp.state.as_str(), opp.ticks_valid, opp.last_seen_ns, opp.duration_ns,
                opp.worst_cost_seen, opp.best_cost_seen, opp.final_cost,
                opp.estimated_fill_prob, opp.estimated_slippage, opp.adjusted_profit
            ],
        )?;
        Ok(())
    }
}

/// Live backtest tracker (runs alongside the bot)
pub struct BacktestTracker {
    db: BacktestDb,
    session_id: String,
    strategies: StrategyManager,
    start_time: Instant,

    // Active opportunities being tracked
    // Key: (strategy_id, market_id, arb_type_ordinal)
    active_opportunities: HashMap<(String, u16, u8), TrackedOpportunity>,

    // Statistics
    pub total_detected: u64,
    pub total_confirmed: u64,
    pub total_slipped: u64,
    pub snapshot_count: u64,

    // Sampling control
    snapshot_interval_ns: u64,
    last_snapshot_ns: HashMap<u16, u64>,
}

fn arb_type_ordinal(arb_type: ArbType) -> u8 {
    match arb_type {
        ArbType::PolyYesKalshiNo => 0,
        ArbType::KalshiYesPolyNo => 1,
        ArbType::PolyOnly => 2,
        ArbType::KalshiOnly => 3,
    }
}

impl BacktestTracker {
    /// Create a new tracker with default strategies
    pub fn new() -> Result<Self> {
        let db = BacktestDb::open_default()?;
        let session_id = format!("session_{}", chrono::Utc::now().format("%Y%m%d_%H%M%S"));
        let strategies = StrategyManager::with_defaults();

        // Save strategies to DB
        for strategy in strategies.strategies() {
            db.save_strategy(strategy)?;
        }

        db.start_session(&session_id, None)?;

        info!("[BACKTEST] Started session: {}", session_id);
        info!("[BACKTEST] Tracking {} strategies", strategies.strategies().len());

        Ok(Self {
            db,
            session_id,
            strategies,
            start_time: Instant::now(),
            active_opportunities: HashMap::new(),
            total_detected: 0,
            total_confirmed: 0,
            total_slipped: 0,
            snapshot_count: 0,
            snapshot_interval_ns: 1_000_000_000,  // 1 second between snapshots per market
            last_snapshot_ns: HashMap::new(),
        })
    }

    /// Get current timestamp in nanoseconds
    fn now_ns(&self) -> u64 {
        self.start_time.elapsed().as_nanos() as u64
    }

    /// Process a market update and evaluate strategies
    pub fn process_update(&mut self, snapshot: MarketSnapshot, market_description: &str) -> Result<Vec<StrategySignal>> {
        let now_ns = self.now_ns();
        let snapshot_with_time = MarketSnapshot {
            timestamp_ns: now_ns,
            ..snapshot
        };

        // Sample snapshots (not every tick)
        let should_snapshot = self.last_snapshot_ns
            .get(&snapshot.market_id)
            .map(|last| now_ns - last > self.snapshot_interval_ns)
            .unwrap_or(true);

        if should_snapshot && snapshot.has_all_prices() {
            self.db.save_snapshot(&snapshot_with_time, market_description, &self.session_id)?;
            self.last_snapshot_ns.insert(snapshot.market_id, now_ns);
            self.snapshot_count += 1;
        }

        // Evaluate all strategies
        let signals = self.strategies.evaluate_all(&snapshot_with_time);

        // Track active opportunities
        let mut new_signals = Vec::new();
        let mut seen_keys = std::collections::HashSet::new();

        for signal in &signals {
            let key = (signal.strategy_id.clone(), signal.market_id, arb_type_ordinal(signal.arb_type));
            seen_keys.insert(key.clone());

            if let Some(opp) = self.active_opportunities.get_mut(&key) {
                // Existing opportunity - update it
                opp.update(true, Some(signal.cost_cents), now_ns);
                if opp.ticks_valid == 2 {
                    // Just confirmed (second tick)
                    self.total_confirmed += 1;
                }
            } else {
                // New opportunity
                let mut opp = TrackedOpportunity::from_signal(signal, market_description);
                opp.detected_at_ns = now_ns;
                opp.last_seen_ns = now_ns;

                let id = self.db.insert_opportunity(&opp, &self.session_id)?;
                opp.id = id;

                self.active_opportunities.insert(key, opp);
                self.total_detected += 1;
                new_signals.push(signal.clone());
            }
        }

        // Check for slipped opportunities (were active but no longer in signals)
        let market_id = snapshot.market_id;
        let costs = snapshot_with_time.arb_costs();
        let slipped_keys: Vec<_> = self.active_opportunities
            .iter()
            .filter(|(k, opp)| {
                k.1 == market_id && opp.state != OpportunityState::Slipped && !seen_keys.contains(*k)
            })
            .map(|(k, _)| k.clone())
            .collect();

        for key in slipped_keys {
            if let Some(opp) = self.active_opportunities.get_mut(&key) {
                let current_cost = match opp.arb_type {
                    ArbType::PolyYesKalshiNo => costs.poly_yes_kalshi_no,
                    ArbType::KalshiYesPolyNo => costs.kalshi_yes_poly_no,
                    ArbType::PolyOnly => costs.poly_only,
                    ArbType::KalshiOnly => costs.kalshi_only,
                };
                opp.update(false, Some(current_cost), now_ns);
                self.db.update_opportunity(opp)?;
                self.total_slipped += 1;
            }
        }

        Ok(new_signals)
    }

    /// Finalize a session
    pub fn finalize(&mut self) -> Result<()> {
        let now_ns = self.now_ns();

        // Close all remaining active opportunities
        for opp in self.active_opportunities.values_mut() {
            if opp.state != OpportunityState::Slipped {
                opp.close_profitable(now_ns);
                self.db.update_opportunity(opp)?;
            }
        }

        let markets_tracked = self.last_snapshot_ns.len() as u32;
        self.db.end_session(&self.session_id, markets_tracked, self.total_detected as u32)?;

        info!("[BACKTEST] Session {} finalized", self.session_id);
        info!("[BACKTEST] Total detected: {}, Confirmed: {}, Slipped: {}",
              self.total_detected, self.total_confirmed, self.total_slipped);

        Ok(())
    }

    /// Get session ID
    pub fn session_id(&self) -> &str {
        &self.session_id
    }
}

impl Default for BacktestTracker {
    fn default() -> Self {
        Self::new().expect("Failed to create backtest tracker")
    }
}

/// Message type for async backtest operations
enum BacktestMessage {
    ProcessUpdate {
        snapshot: MarketSnapshot,
        description: String,
    },
    Finalize,
    GetStats {
        reply: tokio::sync::oneshot::Sender<(u64, u64, u64, u64)>,
    },
    GetSessionId {
        reply: tokio::sync::oneshot::Sender<String>,
    },
}

/// Async wrapper for BacktestTracker using message passing for thread safety
pub struct AsyncBacktestTracker {
    tx: tokio::sync::mpsc::Sender<BacktestMessage>,
    session_id: String,
    stats: Arc<std::sync::RwLock<(u64, u64, u64, u64)>>,
}

impl AsyncBacktestTracker {
    pub fn new() -> Result<Self> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BacktestMessage>(1000);

        // Create tracker in current thread to get session_id
        let tracker = BacktestTracker::new()?;
        let session_id = tracker.session_id.clone();
        let stats = Arc::new(std::sync::RwLock::new((0u64, 0u64, 0u64, 0u64)));
        let stats_clone = stats.clone();

        // Spawn blocking task for database operations
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                let mut tracker = tracker;

                while let Some(msg) = rx.recv().await {
                    match msg {
                        BacktestMessage::ProcessUpdate { snapshot, description } => {
                            if let Err(e) = tracker.process_update(snapshot, &description) {
                                eprintln!("[BACKTEST] Error processing update: {}", e);
                            }
                            // Update stats
                            if let Ok(mut s) = stats_clone.write() {
                                *s = (tracker.total_detected, tracker.total_confirmed,
                                      tracker.total_slipped, tracker.snapshot_count);
                            }
                        }
                        BacktestMessage::Finalize => {
                            if let Err(e) = tracker.finalize() {
                                eprintln!("[BACKTEST] Error finalizing: {}", e);
                            }
                            break;
                        }
                        BacktestMessage::GetStats { reply } => {
                            let _ = reply.send((tracker.total_detected, tracker.total_confirmed,
                                               tracker.total_slipped, tracker.snapshot_count));
                        }
                        BacktestMessage::GetSessionId { reply } => {
                            let _ = reply.send(tracker.session_id.clone());
                        }
                    }
                }
            });
        });

        Ok(Self { tx, session_id, stats })
    }

    pub async fn process_update(&self, snapshot: MarketSnapshot, market_description: &str) -> Result<()> {
        self.tx.send(BacktestMessage::ProcessUpdate {
            snapshot,
            description: market_description.to_string(),
        }).await.map_err(|e| anyhow::anyhow!("Failed to send update: {}", e))
    }

    pub async fn finalize(&self) -> Result<()> {
        self.tx.send(BacktestMessage::Finalize).await
            .map_err(|e| anyhow::anyhow!("Failed to send finalize: {}", e))
    }

    pub async fn stats(&self) -> (u64, u64, u64, u64) {
        // Read from cached stats for non-blocking access
        self.stats.read().map(|s| *s).unwrap_or((0, 0, 0, 0))
    }

    pub async fn session_id(&self) -> String {
        self.session_id.clone()
    }
}

impl Clone for AsyncBacktestTracker {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            session_id: self.session_id.clone(),
            stats: self.stats.clone(),
        }
    }
}

/// Analytics queries
pub struct BacktestAnalytics {
    db: BacktestDb,
}

impl BacktestAnalytics {
    pub fn open() -> Result<Self> {
        Ok(Self { db: BacktestDb::open_default()? })
    }

    /// Get summary statistics for a session
    pub fn session_summary(&self, session_id: &str) -> Result<SessionSummary> {
        let mut stmt = self.db.conn.prepare(r#"
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN state = 'confirmed' OR state = 'closed_profitable' THEN 1 ELSE 0 END) as confirmed,
                SUM(CASE WHEN state = 'slipped' THEN 1 ELSE 0 END) as slipped,
                AVG(detected_profit) as avg_profit,
                AVG(adjusted_profit) as avg_adjusted_profit,
                AVG(estimated_fill_prob) as avg_fill_prob,
                AVG(duration_ns) as avg_duration_ns,
                AVG(ticks_valid) as avg_ticks_valid,
                SUM(detected_profit) as total_theoretical_profit,
                SUM(adjusted_profit * estimated_fill_prob) as total_expected_profit
            FROM opportunities
            WHERE session_id = ?1
        "#)?;

        let row = stmt.query_row(params![session_id], |row| {
            Ok(SessionSummary {
                session_id: session_id.to_string(),
                total_opportunities: row.get(0)?,
                confirmed: row.get(1)?,
                slipped: row.get(2)?,
                avg_profit: row.get(3)?,
                avg_adjusted_profit: row.get(4)?,
                avg_fill_probability: row.get(5)?,
                avg_duration_ms: row.get::<_, Option<f64>>(6)?.map(|ns| ns / 1_000_000.0),
                avg_ticks_valid: row.get(7)?,
                total_theoretical_profit: row.get(8)?,
                total_expected_profit: row.get(9)?,
            })
        })?;

        Ok(row)
    }

    /// Get per-strategy breakdown
    pub fn strategy_breakdown(&self, session_id: &str) -> Result<Vec<StrategyStats>> {
        let mut stmt = self.db.conn.prepare(r#"
            SELECT
                strategy_id,
                COUNT(*) as total,
                SUM(CASE WHEN state = 'confirmed' OR state = 'closed_profitable' THEN 1 ELSE 0 END) as confirmed,
                SUM(CASE WHEN state = 'slipped' THEN 1 ELSE 0 END) as slipped,
                AVG(detected_profit) as avg_profit,
                AVG(adjusted_profit) as avg_adjusted_profit,
                AVG(estimated_fill_prob) as avg_fill_prob,
                SUM(adjusted_profit * estimated_fill_prob) as expected_profit,
                AVG(duration_ns) / 1000000.0 as avg_duration_ms
            FROM opportunities
            WHERE session_id = ?1
            GROUP BY strategy_id
            ORDER BY expected_profit DESC
        "#)?;

        let rows = stmt.query_map(params![session_id], |row| {
            Ok(StrategyStats {
                strategy_id: row.get(0)?,
                total_opportunities: row.get(1)?,
                confirmed: row.get(2)?,
                slipped: row.get(3)?,
                avg_profit: row.get(4)?,
                avg_adjusted_profit: row.get(5)?,
                avg_fill_probability: row.get(6)?,
                expected_profit: row.get(7)?,
                avg_duration_ms: row.get(8)?,
            })
        })?;

        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Get recent sessions
    pub fn recent_sessions(&self, limit: u32) -> Result<Vec<SessionInfo>> {
        let mut stmt = self.db.conn.prepare(r#"
            SELECT id, started_at, ended_at, markets_tracked, opportunities_detected
            FROM sessions
            ORDER BY started_at DESC
            LIMIT ?1
        "#)?;

        let rows = stmt.query_map(params![limit], |row| {
            Ok(SessionInfo {
                id: row.get(0)?,
                started_at: row.get(1)?,
                ended_at: row.get(2)?,
                markets_tracked: row.get(3)?,
                opportunities_detected: row.get(4)?,
            })
        })?;

        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Get slippage analysis
    pub fn slippage_analysis(&self, session_id: &str) -> Result<SlippageAnalysis> {
        let mut stmt = self.db.conn.prepare(r#"
            SELECT
                AVG(estimated_slippage) as avg_slippage,
                MAX(estimated_slippage) as max_slippage,
                MIN(estimated_slippage) as min_slippage,
                COUNT(CASE WHEN estimated_slippage > 0 THEN 1 END) as positive_slippage_count,
                COUNT(CASE WHEN estimated_slippage <= 0 THEN 1 END) as zero_or_negative_count,
                AVG(CASE WHEN state = 'slipped' THEN ticks_valid END) as avg_ticks_before_slip
            FROM opportunities
            WHERE session_id = ?1 AND state = 'slipped'
        "#)?;

        let row = stmt.query_row(params![session_id], |row| {
            Ok(SlippageAnalysis {
                avg_slippage: row.get(0)?,
                max_slippage: row.get(1)?,
                min_slippage: row.get(2)?,
                positive_slippage_count: row.get(3)?,
                zero_or_negative_count: row.get(4)?,
                avg_ticks_before_slip: row.get(5)?,
            })
        })?;

        Ok(row)
    }

    /// Print formatted report
    pub fn print_report(&self, session_id: &str) -> Result<()> {
        println!("\n{}", "=".repeat(60));
        println!("BACKTEST REPORT: {}", session_id);
        println!("{}\n", "=".repeat(60));

        // Session summary
        let summary = self.session_summary(session_id)?;
        println!("SESSION SUMMARY");
        println!("{}", "-".repeat(40));
        println!("Total Opportunities:     {}", summary.total_opportunities);
        println!("Confirmed:               {} ({:.1}%)",
                 summary.confirmed,
                 summary.confirmed as f64 / summary.total_opportunities.max(1) as f64 * 100.0);
        println!("Slipped:                 {} ({:.1}%)",
                 summary.slipped,
                 summary.slipped as f64 / summary.total_opportunities.max(1) as f64 * 100.0);
        println!("Avg Profit (detected):   {:.2}¢", summary.avg_profit.unwrap_or(0.0));
        println!("Avg Profit (adjusted):   {:.2}¢", summary.avg_adjusted_profit.unwrap_or(0.0));
        println!("Avg Fill Probability:    {:.1}%", summary.avg_fill_probability.unwrap_or(0.0) * 100.0);
        println!("Avg Duration:            {:.1}ms", summary.avg_duration_ms.unwrap_or(0.0));
        println!("Total Theoretical:       {:.2}¢", summary.total_theoretical_profit.unwrap_or(0.0));
        println!("Total Expected:          {:.2}¢", summary.total_expected_profit.unwrap_or(0.0));

        // Strategy breakdown
        println!("\nSTRATEGY BREAKDOWN");
        println!("{}", "-".repeat(40));
        let strategies = self.strategy_breakdown(session_id)?;
        println!("{:<20} {:>6} {:>6} {:>6} {:>8} {:>8}",
                 "Strategy", "Total", "Conf", "Slip", "AvgProf", "ExpProf");
        for s in strategies {
            println!("{:<20} {:>6} {:>6} {:>6} {:>7.1}¢ {:>7.1}¢",
                     s.strategy_id.chars().take(20).collect::<String>(),
                     s.total_opportunities, s.confirmed, s.slipped,
                     s.avg_profit.unwrap_or(0.0),
                     s.expected_profit.unwrap_or(0.0));
        }

        // Slippage analysis
        if let Ok(slippage) = self.slippage_analysis(session_id) {
            println!("\nSLIPPAGE ANALYSIS");
            println!("{}", "-".repeat(40));
            println!("Avg Slippage:            {:.2}¢", slippage.avg_slippage.unwrap_or(0.0));
            println!("Max Slippage:            {}¢", slippage.max_slippage.unwrap_or(0));
            println!("Min Slippage:            {}¢", slippage.min_slippage.unwrap_or(0));
            println!("Avg Ticks Before Slip:   {:.1}", slippage.avg_ticks_before_slip.unwrap_or(0.0));
        }

        println!("\n{}\n", "=".repeat(60));
        Ok(())
    }
}

// Result types
#[derive(Debug)]
pub struct SessionSummary {
    pub session_id: String,
    pub total_opportunities: u32,
    pub confirmed: u32,
    pub slipped: u32,
    pub avg_profit: Option<f64>,
    pub avg_adjusted_profit: Option<f64>,
    pub avg_fill_probability: Option<f64>,
    pub avg_duration_ms: Option<f64>,
    pub avg_ticks_valid: Option<f64>,
    pub total_theoretical_profit: Option<f64>,
    pub total_expected_profit: Option<f64>,
}

#[derive(Debug)]
pub struct StrategyStats {
    pub strategy_id: String,
    pub total_opportunities: u32,
    pub confirmed: u32,
    pub slipped: u32,
    pub avg_profit: Option<f64>,
    pub avg_adjusted_profit: Option<f64>,
    pub avg_fill_probability: Option<f64>,
    pub expected_profit: Option<f64>,
    pub avg_duration_ms: Option<f64>,
}

#[derive(Debug)]
pub struct SessionInfo {
    pub id: String,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub markets_tracked: Option<u32>,
    pub opportunities_detected: Option<u32>,
}

#[derive(Debug)]
pub struct SlippageAnalysis {
    pub avg_slippage: Option<f64>,
    pub max_slippage: Option<i32>,
    pub min_slippage: Option<i32>,
    pub positive_slippage_count: u32,
    pub zero_or_negative_count: u32,
    pub avg_ticks_before_slip: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_db_creation() {
        let tmp = NamedTempFile::new().unwrap();
        let db = BacktestDb::open(tmp.path()).unwrap();

        // Should be able to query tables
        let count: i32 = db.conn.query_row(
            "SELECT COUNT(*) FROM strategies", [], |r| r.get(0)
        ).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_tracked_opportunity_slippage() {
        use crate::strategy::StrategySignal;

        let signal = StrategySignal {
            strategy_id: "test".into(),
            market_id: 1,
            arb_type: ArbType::PolyYesKalshiNo,
            cost_cents: 95,
            profit_cents: 5,
            yes_price: 45,
            no_price: 50,
            yes_size: 1000,
            no_size: 1000,
            confidence: 0.8,
            timestamp_ns: 0,
        };

        let mut opp = TrackedOpportunity::from_signal(&signal, "Test Market");
        assert_eq!(opp.state, OpportunityState::Detected);

        // First update - still valid
        opp.update(true, Some(96), 1_000_000);
        assert_eq!(opp.state, OpportunityState::Confirmed);
        assert_eq!(opp.ticks_valid, 2);

        // Second update - slipped
        opp.update(false, Some(102), 2_000_000);
        assert_eq!(opp.state, OpportunityState::Slipped);
        assert_eq!(opp.estimated_slippage, 7);  // 102 - 95
        assert_eq!(opp.adjusted_profit, -2);     // 5 - 7
    }
}
