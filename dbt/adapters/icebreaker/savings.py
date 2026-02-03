"""
Icebreaker Savings Tracker

Tracks query executions and calculates cost savings from running locally vs cloud.
Stores data locally (no cloud storage needed - completely private).
"""

import json
import os
import sqlite3
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, List


# Default cost assumptions based on actual cloud pricing (2024/2025)
# Users can override these in profiles.yml under icebreaker.cost_config
# MVP: Snowflake and DuckDB only
COST_CONFIG = {
    "snowflake": {
        # Credit costs vary by edition and region
        "cost_per_credit": {
            "standard": 2.00,      # Standard edition
            "enterprise": 3.00,    # Enterprise edition
            "business_critical": 4.00,
        },
        # Warehouse sizes and credit consumption per hour
        "credits_per_hour": {
            "xs": 1,
            "s": 2,
            "m": 4,
            "l": 8,
            "xl": 16,
            "2xl": 32,
            "3xl": 64,
            "4xl": 128,
        },
        "min_billing_seconds": 60,  # 60-second minimum when warehouse starts
        "default_edition": "standard",
        "default_warehouse_size": "xs",
    },
    "duckdb": {
        "cost_per_query": 0.00,  # FREE!
    },
}


@dataclass
class QueryExecution:
    """Record of a single query execution."""
    timestamp: str
    model_name: str
    engine_used: str  # "duckdb", "snowflake", etc.
    execution_time_seconds: float
    rows_processed: int
    bytes_processed: int
    estimated_cloud_cost: float  # What it WOULD have cost on cloud
    actual_cost: float  # What it actually cost (0 for DuckDB)
    savings: float  # Difference


def get_db_path() -> str:
    """Get path to local savings database."""
    # Store in user's home directory
    icebreaker_dir = os.path.expanduser("~/.icebreaker")
    os.makedirs(icebreaker_dir, exist_ok=True)
    return os.path.join(icebreaker_dir, "savings.db")


def init_db():
    """Initialize the savings database."""
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS executions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            model_name TEXT NOT NULL,
            engine_used TEXT NOT NULL,
            execution_time_seconds REAL,
            rows_processed INTEGER,
            bytes_processed INTEGER,
            estimated_cloud_cost REAL,
            actual_cost REAL,
            savings REAL
        )
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_timestamp ON executions(timestamp)
    """)
    
    conn.commit()
    conn.close()


def log_execution(
    model_name: str,
    engine_used: str,
    execution_time_seconds: float,
    rows_processed: int = 0,
    bytes_processed: int = 0,
    cloud_type: str = "snowflake",
) -> QueryExecution:
    """
    Log a query execution and calculate savings.
    
    Args:
        model_name: Name of the dbt model
        engine_used: Where it ran ("duckdb", "snowflake", etc.)
        execution_time_seconds: How long it took
        rows_processed: Number of rows processed
        bytes_processed: Bytes processed
        cloud_type: What cloud warehouse to compare against
    
    Returns:
        QueryExecution with calculated savings
    """
    init_db()
    
    # Estimate what cloud cost would have been
    estimated_cloud_cost = estimate_cloud_cost(
        cloud_type=cloud_type,
        execution_time_seconds=execution_time_seconds,
        bytes_processed=bytes_processed,
    )
    
    # Actual cost (0 for DuckDB / local)
    is_local = engine_used.lower() in ("duckdb", "local")
    actual_cost = 0.0 if is_local else estimated_cloud_cost
    
    # Savings
    savings = estimated_cloud_cost - actual_cost
    
    execution = QueryExecution(
        timestamp=datetime.now().isoformat(),
        model_name=model_name,
        engine_used=engine_used,
        execution_time_seconds=execution_time_seconds,
        rows_processed=rows_processed,
        bytes_processed=bytes_processed,
        estimated_cloud_cost=estimated_cloud_cost,
        actual_cost=actual_cost,
        savings=savings,
    )
    
    # Store in database
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO executions 
        (timestamp, model_name, engine_used, execution_time_seconds, 
         rows_processed, bytes_processed, estimated_cloud_cost, actual_cost, savings)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        execution.timestamp,
        execution.model_name,
        execution.engine_used,
        execution.execution_time_seconds,
        execution.rows_processed,
        execution.bytes_processed,
        execution.estimated_cloud_cost,
        execution.actual_cost,
        execution.savings,
    ))
    conn.commit()
    conn.close()
    
    return execution


def estimate_cloud_cost(
    cloud_type: str,
    execution_time_seconds: float,
    bytes_processed: int = 0,
    rows_processed: int = 0,
    config_overrides: dict = None,
) -> float:
    """
    Estimate what a query would cost on Snowflake.
    
    MVP: Only Snowflake pricing is supported.
    
    Args:
        cloud_type: "snowflake" (MVP only supports Snowflake)
        execution_time_seconds: Query runtime in seconds
        bytes_processed: Not used for Snowflake (time-based billing)
        rows_processed: Not used for Snowflake (time-based billing)
        config_overrides: Override default pricing (edition, warehouse_size)
    
    Returns:
        Estimated cost in USD
    """
    config = config_overrides or {}
    
    if cloud_type == "snowflake":
        # Snowflake: credits/hour with 60-second minimum
        sf_config = COST_CONFIG["snowflake"]
        
        edition = config.get("edition", sf_config["default_edition"])
        warehouse_size = config.get("warehouse_size", sf_config["default_warehouse_size"])
        
        # 60-second minimum billing
        billable_seconds = max(sf_config["min_billing_seconds"], execution_time_seconds)
        hours = billable_seconds / 3600
        
        # Get credits based on warehouse size
        credits_per_hour = sf_config["credits_per_hour"].get(warehouse_size, 1)
        credits = hours * credits_per_hour
        
        # Get cost per credit based on edition
        cost_per_credit = sf_config["cost_per_credit"].get(edition, 2.0)
        
        return credits * cost_per_credit
    
    # DuckDB is free, other clouds not supported in MVP
    return 0.0


def format_savings_amount(amount: float) -> str:
    """Format savings in a human-readable way."""
    if amount < 0.01:
        return f"${amount:.4f}"
    elif amount < 1.0:
        return f"${amount:.3f}"
    else:
        return f"${amount:.2f}"


def get_savings_summary(
    period: str = "all",  # "today", "week", "month", "all"
) -> dict:
    """
    Get savings summary for a time period.
    
    Returns dict with:
        - total_queries: Number of queries
        - local_queries: Queries run locally
        - cloud_queries: Queries run on cloud  
        - total_savings: Total USD saved
        - total_time_saved: Seconds saved (local is faster)
        - top_models: Models with most savings
    """
    init_db()
    
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    
    # Calculate time filter
    if period == "today":
        cutoff = datetime.now().replace(hour=0, minute=0, second=0).isoformat()
    elif period == "week":
        cutoff = (datetime.now() - timedelta(days=7)).isoformat()
    elif period == "month":
        cutoff = (datetime.now() - timedelta(days=30)).isoformat()
    else:
        cutoff = "1900-01-01"
    
    # Get summary stats
    cursor.execute("""
        SELECT 
            COUNT(*) as total_queries,
            SUM(CASE WHEN engine_used IN ('duckdb', 'local') THEN 1 ELSE 0 END) as local_queries,
            SUM(CASE WHEN engine_used NOT IN ('duckdb', 'local') THEN 1 ELSE 0 END) as cloud_queries,
            COALESCE(SUM(savings), 0) as total_savings,
            COALESCE(SUM(execution_time_seconds), 0) as total_time
        FROM executions
        WHERE timestamp > ?
    """, (cutoff,))
    
    row = cursor.fetchone()
    
    # Get top models by savings
    cursor.execute("""
        SELECT 
            model_name,
            COUNT(*) as runs,
            SUM(savings) as model_savings
        FROM executions
        WHERE timestamp > ? AND engine_used IN ('duckdb', 'local')
        GROUP BY model_name
        ORDER BY model_savings DESC
        LIMIT 5
    """, (cutoff,))
    
    top_models = [
        {"model": r[0], "runs": r[1], "savings": r[2]}
        for r in cursor.fetchall()
    ]
    
    conn.close()
    
    return {
        "period": period,
        "total_queries": row[0] or 0,
        "local_queries": row[1] or 0,
        "cloud_queries": row[2] or 0,
        "total_savings": row[3] or 0.0,
        "total_execution_time": row[4] or 0.0,
        "top_models": top_models,
    }


def format_savings_report(summary: dict) -> str:
    """Format savings summary as a nice report."""
    period_name = {
        "today": "Today",
        "week": "This Week",
        "month": "This Month",
        "all": "All Time",
    }.get(summary["period"], summary["period"])
    
    lines = [
        "",
        "🧊 ICEBREAKER SAVINGS REPORT",
        "═" * 40,
        f"📅 Period: {period_name}",
        "",
        f"💰 Total Saved: ${summary['total_savings']:.2f}",
        "",
        f"📊 Query Stats:",
        f"   Total queries:  {summary['total_queries']:,}",
        f"   Run locally:    {summary['local_queries']:,} (FREE)",
        f"   Run on cloud:   {summary['cloud_queries']:,}",
        "",
    ]
    
    if summary["local_queries"] > 0:
        pct_local = (summary["local_queries"] / summary["total_queries"]) * 100
        lines.append(f"   Local rate:     {pct_local:.1f}%")
    
    if summary["top_models"]:
        lines.extend([
            "",
            "🏆 Top Savings by Model:",
        ])
        for i, model in enumerate(summary["top_models"], 1):
            lines.append(
                f"   {i}. {model['model']}: ${model['savings']:.2f} "
                f"({model['runs']} runs)"
            )
    
    lines.extend([
        "",
        "═" * 40,
        "💡 Keep running locally to save more!",
        "",
    ])
    
    return "\n".join(lines)


def get_weekly_trend() -> dict:
    """Get savings trend for the last 7 days."""
    init_db()
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    
    days = []
    for i in range(7):
        date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        cursor.execute("""
            SELECT 
                COALESCE(SUM(savings), 0),
                COUNT(*)
            FROM executions
            WHERE date(timestamp) = ?
        """, (date,))
        row = cursor.fetchone()
        days.append({
            "date": date,
            "savings": row[0],
            "queries": row[1],
        })
    
    conn.close()
    return {"days": list(reversed(days))}


def get_projected_annual_savings() -> float:
    """Project annual savings based on recent activity."""
    init_db()
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    
    # Get last 30 days average
    cutoff = (datetime.now() - timedelta(days=30)).isoformat()
    cursor.execute("""
        SELECT COALESCE(SUM(savings), 0)
        FROM executions
        WHERE timestamp > ?
    """, (cutoff,))
    monthly_savings = cursor.fetchone()[0] or 0
    conn.close()
    
    # Project to annual
    return monthly_savings * 12


def format_enhanced_savings_report() -> str:
    """Generate the enhanced savings dashboard."""
    # Get summaries for different periods
    today = get_savings_summary("today")
    week = get_savings_summary("week")
    month = get_savings_summary("month")
    
    # Get trends and projections
    trend = get_weekly_trend()
    projected = get_projected_annual_savings()
    
    lines = [
        "",
        "💰 ICEBREAKER SAVINGS DASHBOARD",
        "═" * 55,
        "",
        f"  Today:       ${today['total_savings']:>8.2f}  ({today['local_queries']:>4} local queries)",
        f"  This Week:   ${week['total_savings']:>8.2f}  ({week['local_queries']:>4} local queries)",
        f"  This Month:  ${month['total_savings']:>8.2f}  ({month['local_queries']:>4} local queries)",
        "",
        "─" * 55,
        f"  📈 Projected Annual Savings: ${projected:,.0f}",
        "─" * 55,
        "",
    ]
    
    # Weekly sparkline
    if any(d['savings'] > 0 for d in trend['days']):
        lines.append("  📊 Last 7 Days:")
        max_savings = max(d['savings'] for d in trend['days']) or 1
        for day in trend['days']:
            bar_len = int((day['savings'] / max_savings) * 20) if max_savings > 0 else 0
            bar = "█" * bar_len
            date_short = day['date'][5:]  # MM-DD
            lines.append(f"     {date_short} │{bar:<20} ${day['savings']:.2f}")
        lines.append("")
    
    # Top models
    if month['top_models']:
        lines.append("  🏆 Top Models by Savings (This Month):")
        for i, model in enumerate(month['top_models'][:5], 1):
            lines.append(
                f"     {i}. {model['model']:<25} ${model['savings']:>8.2f}"
            )
        lines.append("")
    
    # Local rate
    if month['total_queries'] > 0:
        rate = (month['local_queries'] / month['total_queries']) * 100
        lines.append(f"  ⚡ Local Execution Rate: {rate:.0f}%")
        lines.append("")
    
    lines.extend([
        "═" * 55,
        "",
    ])
    
    return "\n".join(lines)


def print_savings(period: str = "all"):
    """Print savings report to console."""
    if period == "dashboard":
        print(format_enhanced_savings_report())
    else:
        summary = get_savings_summary(period)
        print(format_savings_report(summary))


def get_summary(period: str = "week") -> dict:
    """Alias for get_savings_summary for backward compatibility."""
    return get_savings_summary(period)


def export_to_json(filepath: str = None) -> str:
    """Export savings data to JSON for external dashboards."""
    if filepath is None:
        filepath = os.path.expanduser("~/.icebreaker/savings_export.json")
    
    data = {
        "exported_at": datetime.now().isoformat(),
        "today": get_savings_summary("today"),
        "week": get_savings_summary("week"),
        "month": get_savings_summary("month"),
        "weekly_trend": get_weekly_trend(),
        "projected_annual": get_projected_annual_savings(),
    }
    
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    
    return filepath

