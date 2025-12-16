//! Price Collector - Periodic snapshot tool for GitHub Actions / Cron
//!
//! Collects current prices via REST API and stores to SQLite.
//! Designed for scheduled execution (not real-time trading).
//!
//! Usage:
//!   cargo run --bin price-collector              # Collect and store snapshot
//!   cargo run --bin price-collector -- --analyze # Analyze collected data

use anyhow::{Context, Result};
use rusqlite::{Connection, params};
use serde::Deserialize;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

const DB_PATH: &str = "price_snapshots.db";
const KALSHI_API_BASE: &str = "https://api.elections.kalshi.com/trade-api/v2";
const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";

#[derive(Debug, Deserialize)]
struct KalshiMarketsResponse {
    markets: Vec<KalshiMarket>,
}

#[derive(Debug, Deserialize)]
struct KalshiMarket {
    ticker: String,
    title: String,
    yes_ask: Option<i64>,
    no_ask: Option<i64>,
    volume: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct GammaEvent {
    slug: Option<String>,
    title: Option<String>,
    markets: Option<Vec<GammaMarket>>,
}

#[derive(Debug, Deserialize)]
struct GammaMarket {
    #[serde(rename = "outcomePrices")]
    outcome_prices: Option<String>,
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: Option<String>,
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 && args[1] == "--analyze" {
        return analyze_snapshots();
    }

    if args.len() > 1 && args[1] == "--help" {
        println!("Price Collector - Periodic snapshot tool\n");
        println!("Usage:");
        println!("  price-collector           Collect price snapshot");
        println!("  price-collector --analyze Analyze collected data");
        println!("\nEnvironment:");
        println!("  KALSHI_API_KEY_ID    Kalshi API key (optional for public data)");
        return Ok(());
    }

    collect_snapshot()
}

fn collect_snapshot() -> Result<()> {
    println!("📸 Collecting price snapshot...");

    let conn = init_db()?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs() as i64;

    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    // Collect Kalshi prices (public endpoint, no auth needed for market data)
    let kalshi_count = collect_kalshi_prices(&client, &conn, timestamp)?;
    println!("  ✅ Kalshi: {} markets", kalshi_count);

    // Collect Polymarket prices
    let poly_count = collect_poly_prices(&client, &conn, timestamp)?;
    println!("  ✅ Polymarket: {} markets", poly_count);

    // Find near-arbitrage opportunities
    let near_arbs = find_near_arbs(&conn, timestamp)?;
    if !near_arbs.is_empty() {
        println!("\n📊 Near-arbitrage opportunities found:");
        for (ticker, cost, profit) in near_arbs {
            println!("  {} | cost={}¢ | potential={:+}¢", ticker, cost, profit);
        }
    }

    println!("\n✅ Snapshot complete at {}", timestamp);
    Ok(())
}

fn init_db() -> Result<Connection> {
    let conn = Connection::open(DB_PATH)?;

    conn.execute_batch(r#"
        CREATE TABLE IF NOT EXISTS kalshi_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            ticker TEXT NOT NULL,
            title TEXT,
            yes_ask INTEGER,
            no_ask INTEGER,
            volume INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS poly_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            slug TEXT NOT NULL,
            title TEXT,
            yes_price INTEGER,
            no_price INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS arb_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            kalshi_ticker TEXT,
            poly_slug TEXT,
            kalshi_yes INTEGER,
            kalshi_no INTEGER,
            poly_yes INTEGER,
            poly_no INTEGER,
            best_cost INTEGER,
            potential_profit INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_kalshi_ts ON kalshi_snapshots(timestamp);
        CREATE INDEX IF NOT EXISTS idx_poly_ts ON poly_snapshots(timestamp);
        CREATE INDEX IF NOT EXISTS idx_arb_ts ON arb_snapshots(timestamp);
    "#)?;

    Ok(conn)
}

fn collect_kalshi_prices(client: &reqwest::blocking::Client, conn: &Connection, timestamp: i64) -> Result<usize> {
    // Fetch sports markets (public endpoint)
    let series = ["KXEPLGAME", "KXNBAGAME", "KXNFLGAME"];
    let mut count = 0;

    for s in series {
        let url = format!("{}/markets?series_ticker={}&limit=100", KALSHI_API_BASE, s);

        match client.get(&url).send() {
            Ok(resp) => {
                if let Ok(data) = resp.json::<KalshiMarketsResponse>() {
                    for market in data.markets {
                        conn.execute(
                            "INSERT INTO kalshi_snapshots (timestamp, ticker, title, yes_ask, no_ask, volume) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            params![timestamp, market.ticker, market.title, market.yes_ask, market.no_ask, market.volume],
                        )?;
                        count += 1;
                    }
                }
            }
            Err(e) => {
                eprintln!("  ⚠️ Failed to fetch {}: {}", s, e);
            }
        }

        // Rate limit
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    Ok(count)
}

fn collect_poly_prices(client: &reqwest::blocking::Client, conn: &Connection, timestamp: i64) -> Result<usize> {
    // Fetch sports events from Gamma API
    let url = format!("{}/events?active=true&closed=false&limit=100&tag=sports", GAMMA_API_BASE);

    let resp = client.get(&url).send()?;
    let events: Vec<GammaEvent> = resp.json()?;
    let mut count = 0;

    for event in events {
        let slug = event.slug.unwrap_or_default();
        let title = event.title.unwrap_or_default();

        if let Some(markets) = event.markets {
            for market in markets {
                if let Some(prices) = market.outcome_prices {
                    // Parse "[\"0.45\",\"0.55\"]" format
                    let prices: Vec<&str> = prices.trim_matches(|c| c == '[' || c == ']')
                        .split(',')
                        .map(|s| s.trim().trim_matches('"'))
                        .collect();

                    if prices.len() >= 2 {
                        let yes_price = parse_price(prices[0]);
                        let no_price = parse_price(prices[1]);

                        conn.execute(
                            "INSERT INTO poly_snapshots (timestamp, slug, title, yes_price, no_price) VALUES (?1, ?2, ?3, ?4, ?5)",
                            params![timestamp, slug, title, yes_price, no_price],
                        )?;
                        count += 1;
                    }
                }
            }
        }
    }

    Ok(count)
}

fn parse_price(s: &str) -> i64 {
    s.parse::<f64>()
        .map(|p| (p * 100.0).round() as i64)
        .unwrap_or(0)
}

fn find_near_arbs(conn: &Connection, timestamp: i64) -> Result<Vec<(String, i64, i64)>> {
    // Simple heuristic: find Kalshi markets where yes_ask + no_ask < 105 (within 5% of breakeven)
    let mut stmt = conn.prepare(r#"
        SELECT ticker, yes_ask, no_ask, (yes_ask + no_ask) as cost
        FROM kalshi_snapshots
        WHERE timestamp = ?1 AND yes_ask IS NOT NULL AND no_ask IS NOT NULL
        AND (yes_ask + no_ask) < 105
        ORDER BY cost ASC
        LIMIT 10
    "#)?;

    let rows = stmt.query_map(params![timestamp], |row| {
        let ticker: String = row.get(0)?;
        let yes: i64 = row.get(1)?;
        let no: i64 = row.get(2)?;
        let cost: i64 = row.get(3)?;
        let profit = 100 - cost - 2; // Subtract ~2 cent fee estimate
        Ok((ticker, cost, profit))
    })?;

    rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
}

fn analyze_snapshots() -> Result<()> {
    let conn = Connection::open(DB_PATH)?;

    println!("\n{}", "=".repeat(60));
    println!("PRICE SNAPSHOT ANALYSIS");
    println!("{}\n", "=".repeat(60));

    // Count snapshots
    let kalshi_count: i64 = conn.query_row(
        "SELECT COUNT(DISTINCT timestamp) FROM kalshi_snapshots", [], |r| r.get(0)
    )?;
    let poly_count: i64 = conn.query_row(
        "SELECT COUNT(DISTINCT timestamp) FROM poly_snapshots", [], |r| r.get(0)
    )?;

    println!("Snapshots collected:");
    println!("  Kalshi: {} timestamps", kalshi_count);
    println!("  Polymarket: {} timestamps", poly_count);

    // Find best historical near-arbs
    println!("\n{}", "-".repeat(40));
    println!("BEST HISTORICAL OPPORTUNITIES");
    println!("{}", "-".repeat(40));

    let mut stmt = conn.prepare(r#"
        SELECT timestamp, ticker, yes_ask, no_ask, (yes_ask + no_ask) as cost
        FROM kalshi_snapshots
        WHERE yes_ask IS NOT NULL AND no_ask IS NOT NULL
        ORDER BY cost ASC
        LIMIT 20
    "#)?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)?,
        ))
    })?;

    println!("{:<12} {:<30} {:>6} {:>6} {:>6}", "Timestamp", "Ticker", "YES", "NO", "Cost");
    for row in rows {
        let (ts, ticker, yes, no, cost) = row?;
        let ticker_short: String = ticker.chars().take(30).collect();
        println!("{:<12} {:<30} {:>5}¢ {:>5}¢ {:>5}¢", ts, ticker_short, yes, no, cost);
    }

    // Price distribution
    println!("\n{}", "-".repeat(40));
    println!("PRICE DISTRIBUTION (Kalshi)");
    println!("{}", "-".repeat(40));

    let mut stmt = conn.prepare(r#"
        SELECT
            CASE
                WHEN (yes_ask + no_ask) < 100 THEN 'Arb (<100)'
                WHEN (yes_ask + no_ask) < 102 THEN 'Near-arb (100-102)'
                WHEN (yes_ask + no_ask) < 105 THEN 'Close (102-105)'
                ELSE 'Efficient (>105)'
            END as bucket,
            COUNT(*) as count
        FROM kalshi_snapshots
        WHERE yes_ask IS NOT NULL AND no_ask IS NOT NULL
        GROUP BY bucket
        ORDER BY count DESC
    "#)?;

    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;

    for row in rows {
        let (bucket, count) = row?;
        println!("  {:<20} {:>6}", bucket, count);
    }

    println!("\n{}\n", "=".repeat(60));
    Ok(())
}
