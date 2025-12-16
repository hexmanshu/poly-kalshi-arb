//! Backtest Report CLI
//!
//! View and analyze backtest results from the SQLite database.
//!
//! Usage:
//!   cargo run --bin backtest-report              # Show latest session
//!   cargo run --bin backtest-report -- --list    # List all sessions
//!   cargo run --bin backtest-report -- SESSION   # Show specific session
//!   cargo run --bin backtest-report -- --compare # Compare all strategies

use anyhow::Result;
use rusqlite::{Connection, params};
use std::env;

const DB_PATH: &str = "backtest_results.db";

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    let conn = Connection::open(DB_PATH)?;

    if args.len() < 2 {
        // Show latest session
        show_latest_session(&conn)?;
    } else {
        match args[1].as_str() {
            "--list" | "-l" => list_sessions(&conn)?,
            "--compare" | "-c" => compare_strategies(&conn)?,
            "--strategies" | "-s" => list_strategies(&conn)?,
            "--help" | "-h" => print_help(),
            "--export" | "-e" => {
                if args.len() > 2 {
                    export_session(&conn, &args[2])?;
                } else {
                    export_latest(&conn)?;
                }
            }
            session_id => show_session(&conn, session_id)?,
        }
    }

    Ok(())
}

fn print_help() {
    println!(r#"
Backtest Report CLI

USAGE:
    backtest-report [OPTIONS] [SESSION_ID]

OPTIONS:
    -l, --list        List all sessions
    -c, --compare     Compare strategies across all sessions
    -s, --strategies  Show strategy definitions
    -e, --export      Export session data to CSV
    -h, --help        Print this help message

EXAMPLES:
    backtest-report                  # Show latest session report
    backtest-report --list           # List all sessions
    backtest-report session_xxx      # Show specific session
    backtest-report --compare        # Compare strategy performance
    backtest-report --export         # Export latest session to CSV
"#);
}

fn list_sessions(conn: &Connection) -> Result<()> {
    println!("\n{}", "=".repeat(80));
    println!("BACKTEST SESSIONS");
    println!("{}\n", "=".repeat(80));

    let mut stmt = conn.prepare(r#"
        SELECT id, started_at, ended_at, markets_tracked, opportunities_detected
        FROM sessions
        ORDER BY started_at DESC
        LIMIT 20
    "#)?;

    println!("{:<30} {:>12} {:>10} {:>10} {:>10}",
             "Session ID", "Started", "Duration", "Markets", "Opps");
    println!("{}", "-".repeat(80));

    let rows = stmt.query_map([], |row| {
        let id: String = row.get(0)?;
        let started: String = row.get(1)?;
        let ended: Option<String> = row.get(2)?;
        let markets: Option<u32> = row.get(3)?;
        let opps: Option<u32> = row.get(4)?;

        let duration = if let Some(e) = ended {
            format!("{} - {}", &started[11..19], &e[11..19])
        } else {
            "running...".to_string()
        };

        Ok((id, started, duration, markets, opps))
    })?;

    for row in rows {
        let (id, started, duration, markets, opps) = row?;
        println!("{:<30} {:>12} {:>10} {:>10} {:>10}",
                 id.chars().take(30).collect::<String>(),
                 &started[0..10],
                 duration,
                 markets.map(|m| m.to_string()).unwrap_or("-".into()),
                 opps.map(|o| o.to_string()).unwrap_or("-".into()));
    }

    println!();
    Ok(())
}

fn show_latest_session(conn: &Connection) -> Result<()> {
    let session_id: String = conn.query_row(
        "SELECT id FROM sessions ORDER BY started_at DESC LIMIT 1",
        [],
        |row| row.get(0),
    )?;

    show_session(conn, &session_id)
}

fn show_session(conn: &Connection, session_id: &str) -> Result<()> {
    println!("\n{}", "=".repeat(80));
    println!("BACKTEST REPORT: {}", session_id);
    println!("{}\n", "=".repeat(80));

    // Session info
    let (started, ended, markets, opps): (String, Option<String>, Option<u32>, Option<u32>) =
        conn.query_row(
            "SELECT started_at, ended_at, markets_tracked, opportunities_detected FROM sessions WHERE id = ?1",
            params![session_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;

    println!("Session Info:");
    println!("  Started:      {}", started);
    println!("  Ended:        {}", ended.as_deref().unwrap_or("running..."));
    println!("  Markets:      {}", markets.map(|m| m.to_string()).unwrap_or("-".into()));
    println!("  Opportunities: {}", opps.map(|o| o.to_string()).unwrap_or("-".into()));

    // Summary stats
    println!("\n{}", "-".repeat(40));
    println!("SUMMARY STATISTICS");
    println!("{}", "-".repeat(40));

    let summary: (u32, u32, u32, Option<f64>, Option<f64>, Option<f64>, Option<f64>, Option<f64>) =
        conn.query_row(
            r#"SELECT
                COUNT(*),
                SUM(CASE WHEN state IN ('confirmed', 'closed_profitable') THEN 1 ELSE 0 END),
                SUM(CASE WHEN state = 'slipped' THEN 1 ELSE 0 END),
                AVG(detected_profit),
                AVG(adjusted_profit),
                AVG(estimated_fill_prob),
                SUM(detected_profit),
                SUM(adjusted_profit * estimated_fill_prob)
            FROM opportunities WHERE session_id = ?1"#,
            params![session_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?,
                      row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?)),
        )?;

    let (total, confirmed, slipped, avg_profit, avg_adj, avg_fill, sum_profit, sum_expected) = summary;

    println!("Total Opportunities:    {}", total);
    println!("Confirmed:              {} ({:.1}%)", confirmed, confirmed as f64 / total.max(1) as f64 * 100.0);
    println!("Slipped:                {} ({:.1}%)", slipped, slipped as f64 / total.max(1) as f64 * 100.0);
    println!("Avg Detected Profit:    {:.2}¢", avg_profit.unwrap_or(0.0));
    println!("Avg Adjusted Profit:    {:.2}¢", avg_adj.unwrap_or(0.0));
    println!("Avg Fill Probability:   {:.1}%", avg_fill.unwrap_or(0.0) * 100.0);
    println!("Total Theoretical:      {:.0}¢ (${:.2})", sum_profit.unwrap_or(0.0), sum_profit.unwrap_or(0.0) / 100.0);
    println!("Total Expected:         {:.0}¢ (${:.2})", sum_expected.unwrap_or(0.0), sum_expected.unwrap_or(0.0) / 100.0);

    // Strategy breakdown
    println!("\n{}", "-".repeat(40));
    println!("STRATEGY BREAKDOWN");
    println!("{}", "-".repeat(40));

    let mut stmt = conn.prepare(r#"
        SELECT
            strategy_id,
            COUNT(*),
            SUM(CASE WHEN state IN ('confirmed', 'closed_profitable') THEN 1 ELSE 0 END),
            SUM(CASE WHEN state = 'slipped' THEN 1 ELSE 0 END),
            AVG(detected_profit),
            AVG(adjusted_profit),
            SUM(adjusted_profit * estimated_fill_prob)
        FROM opportunities
        WHERE session_id = ?1
        GROUP BY strategy_id
        ORDER BY SUM(adjusted_profit * estimated_fill_prob) DESC
    "#)?;

    println!("{:<22} {:>5} {:>5} {:>5} {:>7} {:>7} {:>8}",
             "Strategy", "Tot", "Conf", "Slip", "AvgP", "AdjP", "ExpProf");
    println!("{}", "-".repeat(70));

    let rows = stmt.query_map(params![session_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, u32>(1)?,
            row.get::<_, u32>(2)?,
            row.get::<_, u32>(3)?,
            row.get::<_, Option<f64>>(4)?,
            row.get::<_, Option<f64>>(5)?,
            row.get::<_, Option<f64>>(6)?,
        ))
    })?;

    for row in rows {
        let (strategy, total, confirmed, slipped, avg_p, adj_p, exp_p) = row?;
        println!("{:<22} {:>5} {:>5} {:>5} {:>6.1}¢ {:>6.1}¢ {:>7.1}¢",
                 strategy.chars().take(22).collect::<String>(),
                 total, confirmed, slipped,
                 avg_p.unwrap_or(0.0),
                 adj_p.unwrap_or(0.0),
                 exp_p.unwrap_or(0.0));
    }

    // Slippage analysis
    println!("\n{}", "-".repeat(40));
    println!("SLIPPAGE ANALYSIS");
    println!("{}", "-".repeat(40));

    let slip: (Option<f64>, Option<i32>, Option<i32>, u32, Option<f64>) = conn.query_row(
        r#"SELECT
            AVG(estimated_slippage),
            MAX(estimated_slippage),
            MIN(estimated_slippage),
            COUNT(*),
            AVG(ticks_valid)
        FROM opportunities
        WHERE session_id = ?1 AND state = 'slipped'"#,
        params![session_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
    )?;

    println!("Slipped Opportunities:  {}", slip.3);
    println!("Avg Slippage:           {:.2}¢", slip.0.unwrap_or(0.0));
    println!("Max Slippage:           {}¢", slip.1.unwrap_or(0));
    println!("Min Slippage:           {}¢", slip.2.unwrap_or(0));
    println!("Avg Ticks Before Slip:  {:.1}", slip.4.unwrap_or(0.0));

    // Arb type breakdown
    println!("\n{}", "-".repeat(40));
    println!("ARB TYPE BREAKDOWN");
    println!("{}", "-".repeat(40));

    let mut stmt = conn.prepare(r#"
        SELECT
            arb_type,
            COUNT(*),
            AVG(detected_profit),
            AVG(estimated_fill_prob),
            SUM(adjusted_profit * estimated_fill_prob)
        FROM opportunities
        WHERE session_id = ?1
        GROUP BY arb_type
        ORDER BY COUNT(*) DESC
    "#)?;

    println!("{:<25} {:>6} {:>8} {:>8} {:>10}",
             "Arb Type", "Count", "AvgProf", "FillPr", "Expected");
    println!("{}", "-".repeat(60));

    let rows = stmt.query_map(params![session_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, u32>(1)?,
            row.get::<_, Option<f64>>(2)?,
            row.get::<_, Option<f64>>(3)?,
            row.get::<_, Option<f64>>(4)?,
        ))
    })?;

    for row in rows {
        let (arb_type, count, avg_p, fill_p, exp_p) = row?;
        println!("{:<25} {:>6} {:>7.1}¢ {:>7.1}% {:>9.1}¢",
                 arb_type, count, avg_p.unwrap_or(0.0),
                 fill_p.unwrap_or(0.0) * 100.0, exp_p.unwrap_or(0.0));
    }

    // Top opportunities
    println!("\n{}", "-".repeat(40));
    println!("TOP 10 OPPORTUNITIES (by expected profit)");
    println!("{}", "-".repeat(40));

    let mut stmt = conn.prepare(r#"
        SELECT
            market_description,
            strategy_id,
            arb_type,
            detected_profit,
            adjusted_profit,
            estimated_fill_prob,
            state,
            ticks_valid
        FROM opportunities
        WHERE session_id = ?1
        ORDER BY (adjusted_profit * estimated_fill_prob) DESC
        LIMIT 10
    "#)?;

    let rows = stmt.query_map(params![session_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, i32>(3)?,
            row.get::<_, i32>(4)?,
            row.get::<_, f64>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, u32>(7)?,
        ))
    })?;

    for row in rows {
        let (market, strategy, arb_type, det_p, adj_p, fill_p, state, ticks) = row?;
        let expected = adj_p as f64 * fill_p;
        println!("  {}...", market.chars().take(50).collect::<String>());
        println!("    Strategy: {} | Type: {} | State: {} ({} ticks)",
                 strategy, arb_type, state, ticks);
        println!("    Detected: {}¢ | Adjusted: {}¢ | Fill: {:.0}% | Expected: {:.1}¢",
                 det_p, adj_p, fill_p * 100.0, expected);
        println!();
    }

    println!("{}\n", "=".repeat(80));
    Ok(())
}

fn compare_strategies(conn: &Connection) -> Result<()> {
    println!("\n{}", "=".repeat(80));
    println!("STRATEGY COMPARISON (ALL SESSIONS)");
    println!("{}\n", "=".repeat(80));

    let mut stmt = conn.prepare(r#"
        SELECT
            strategy_id,
            COUNT(*) as total,
            SUM(CASE WHEN state IN ('confirmed', 'closed_profitable') THEN 1 ELSE 0 END) as confirmed,
            SUM(CASE WHEN state = 'slipped' THEN 1 ELSE 0 END) as slipped,
            AVG(detected_profit) as avg_detected,
            AVG(adjusted_profit) as avg_adjusted,
            AVG(estimated_fill_prob) as avg_fill,
            SUM(detected_profit) as sum_detected,
            SUM(adjusted_profit * estimated_fill_prob) as sum_expected,
            AVG(ticks_valid) as avg_ticks,
            COUNT(DISTINCT session_id) as sessions
        FROM opportunities
        GROUP BY strategy_id
        ORDER BY sum_expected DESC
    "#)?;

    println!("{:<22} {:>6} {:>5}% {:>5}% {:>6} {:>6} {:>6}% {:>8} {:>6}",
             "Strategy", "Total", "Conf", "Slip", "AvgDet", "AvgAdj", "Fill", "Expected", "Ticks");
    println!("{}", "-".repeat(85));

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, u32>(1)?,
            row.get::<_, u32>(2)?,
            row.get::<_, u32>(3)?,
            row.get::<_, Option<f64>>(4)?,
            row.get::<_, Option<f64>>(5)?,
            row.get::<_, Option<f64>>(6)?,
            row.get::<_, Option<f64>>(7)?,
            row.get::<_, Option<f64>>(8)?,
            row.get::<_, Option<f64>>(9)?,
            row.get::<_, u32>(10)?,
        ))
    })?;

    for row in rows {
        let (strategy, total, confirmed, slipped, avg_det, avg_adj, avg_fill, _sum_det, sum_exp, avg_ticks, _sessions) = row?;
        let conf_pct = confirmed as f64 / total.max(1) as f64 * 100.0;
        let slip_pct = slipped as f64 / total.max(1) as f64 * 100.0;
        println!("{:<22} {:>6} {:>5.1} {:>5.1} {:>5.1}¢ {:>5.1}¢ {:>5.1} {:>7.0}¢ {:>6.1}",
                 strategy.chars().take(22).collect::<String>(),
                 total, conf_pct, slip_pct,
                 avg_det.unwrap_or(0.0), avg_adj.unwrap_or(0.0),
                 avg_fill.unwrap_or(0.0) * 100.0,
                 sum_exp.unwrap_or(0.0), avg_ticks.unwrap_or(0.0));
    }

    // Strategy recommendations
    println!("\n{}", "-".repeat(40));
    println!("RECOMMENDATIONS");
    println!("{}", "-".repeat(40));

    // Best by expected profit
    let best: (String, f64) = conn.query_row(
        r#"SELECT strategy_id, SUM(adjusted_profit * estimated_fill_prob) as exp
           FROM opportunities GROUP BY strategy_id ORDER BY exp DESC LIMIT 1"#,
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    println!("Best Expected Profit:   {} ({:.0}¢)", best.0, best.1);

    // Best confirmation rate
    let best_conf: (String, f64) = conn.query_row(
        r#"SELECT strategy_id,
           CAST(SUM(CASE WHEN state IN ('confirmed', 'closed_profitable') THEN 1 ELSE 0 END) AS REAL) / COUNT(*) as rate
           FROM opportunities GROUP BY strategy_id HAVING COUNT(*) > 10 ORDER BY rate DESC LIMIT 1"#,
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).unwrap_or(("N/A".into(), 0.0));
    println!("Best Confirmation Rate: {} ({:.1}%)", best_conf.0, best_conf.1 * 100.0);

    // Lowest slippage
    let low_slip: (String, f64) = conn.query_row(
        r#"SELECT strategy_id, AVG(estimated_slippage) as slip
           FROM opportunities WHERE state = 'slipped'
           GROUP BY strategy_id HAVING COUNT(*) > 5 ORDER BY slip ASC LIMIT 1"#,
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).unwrap_or(("N/A".into(), 0.0));
    println!("Lowest Avg Slippage:    {} ({:.1}¢)", low_slip.0, low_slip.1);

    println!("\n{}\n", "=".repeat(80));
    Ok(())
}

fn list_strategies(conn: &Connection) -> Result<()> {
    println!("\n{}", "=".repeat(80));
    println!("STRATEGY DEFINITIONS");
    println!("{}\n", "=".repeat(80));

    let mut stmt = conn.prepare(r#"
        SELECT id, name, description, params_json, enabled
        FROM strategies
        ORDER BY id
    "#)?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, bool>(4)?,
        ))
    })?;

    for row in rows {
        let (id, name, desc, params, enabled) = row?;
        println!("ID: {} {}", id, if enabled { "[ENABLED]" } else { "[DISABLED]" });
        println!("Name: {}", name);
        println!("Description: {}", desc.unwrap_or_default());
        println!("Parameters: {}", params);
        println!();
    }

    println!("{}\n", "=".repeat(80));
    Ok(())
}

fn export_latest(conn: &Connection) -> Result<()> {
    let session_id: String = conn.query_row(
        "SELECT id FROM sessions ORDER BY started_at DESC LIMIT 1",
        [],
        |row| row.get(0),
    )?;

    export_session(conn, &session_id)
}

fn export_session(conn: &Connection, session_id: &str) -> Result<()> {
    let filename = format!("{}.csv", session_id);

    let mut stmt = conn.prepare(r#"
        SELECT
            strategy_id, market_id, market_description, arb_type,
            detected_at_ns, detected_cost, detected_profit,
            detected_yes_price, detected_no_price, detected_yes_size, detected_no_size,
            state, ticks_valid, duration_ns,
            worst_cost_seen, best_cost_seen, final_cost,
            estimated_fill_prob, estimated_slippage, adjusted_profit
        FROM opportunities
        WHERE session_id = ?1
        ORDER BY detected_at_ns
    "#)?;

    let rows = stmt.query_map(params![session_id], |row| {
        Ok(format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.4},{},{}",
            row.get::<_, String>(0)?,
            row.get::<_, u16>(1)?,
            row.get::<_, String>(2)?.replace(",", ";"),
            row.get::<_, String>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, u16>(5)?,
            row.get::<_, i16>(6)?,
            row.get::<_, u16>(7)?,
            row.get::<_, u16>(8)?,
            row.get::<_, u16>(9)?,
            row.get::<_, u16>(10)?,
            row.get::<_, String>(11)?,
            row.get::<_, u32>(12)?,
            row.get::<_, Option<i64>>(13)?.unwrap_or(0),
            row.get::<_, Option<u16>>(14)?.unwrap_or(0),
            row.get::<_, Option<u16>>(15)?.unwrap_or(0),
            row.get::<_, Option<u16>>(16)?.unwrap_or(0),
            row.get::<_, Option<f64>>(17)?.unwrap_or(0.0),
            row.get::<_, Option<i16>>(18)?.unwrap_or(0),
            row.get::<_, Option<i16>>(19)?.unwrap_or(0),
        ))
    })?;

    let header = "strategy_id,market_id,market_description,arb_type,detected_at_ns,detected_cost,detected_profit,detected_yes_price,detected_no_price,detected_yes_size,detected_no_size,state,ticks_valid,duration_ns,worst_cost_seen,best_cost_seen,final_cost,estimated_fill_prob,estimated_slippage,adjusted_profit";

    let mut output = String::new();
    output.push_str(header);
    output.push('\n');

    for row in rows {
        output.push_str(&row?);
        output.push('\n');
    }

    std::fs::write(&filename, output)?;
    println!("Exported {} to {}", session_id, filename);

    Ok(())
}
