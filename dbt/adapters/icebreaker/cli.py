"""
Icebreaker CLI

Command-line utilities for the Icebreaker adapter.
"""

import argparse
import json
import sys
from pathlib import Path

from dbt.adapters.icebreaker.console import console


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="icebreaker",
        description="Icebreaker dbt adapter - Zero-config cost optimization for dbt"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # savings command (primary feature!)
    savings_parser = subparsers.add_parser(
        "savings",
        help="Show cost savings from running locally"
    )
    savings_parser.add_argument(
        "--today", action="store_true",
        help="Show today's savings only"
    )
    savings_parser.add_argument(
        "--week", action="store_true",
        help="Show this week's savings"
    )
    savings_parser.add_argument(
        "--month", action="store_true",
        help="Show this month's savings"
    )
    savings_parser.add_argument(
        "--dashboard", action="store_true",
        help="Show enhanced savings dashboard with trends"
    )
    
    # status command
    status_parser = subparsers.add_parser(
        "status",
        help="Show Icebreaker connection status"
    )
    
    # sync-status command (NEW)
    sync_parser = subparsers.add_parser(
        "sync-status",
        help="Show sync status and history"
    )
    sync_parser.add_argument(
        "--hours", type=int, default=24,
        help="Show syncs from last N hours (default: 24)"
    )
    
    # explain command (NEW)
    explain_parser = subparsers.add_parser(
        "explain",
        help="Explain routing decision for a SQL file or query"
    )
    explain_parser.add_argument(
        "input",
        help="SQL file path or inline SQL query"
    )
    
    # stats command (NEW - enhanced)
    stats_parser = subparsers.add_parser(
        "stats",
        help="Show system and performance statistics"
    )
    
    # update-stats command (legacy)
    update_stats_parser = subparsers.add_parser(
        "update-stats",
        help="Fetch cloud execution stats for routing optimization"
    )
    update_stats_parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Number of days of history to fetch (default: 14)"
    )
    
    # version command
    version_parser = subparsers.add_parser(
        "version",
        help="Show Icebreaker version"
    )
    
    # help command (explicit help with examples)
    help_parser = subparsers.add_parser(
        "help",
        help="Show detailed help and examples"
    )
    
    # sync command - manually trigger sync to Snowflake
    sync_cmd_parser = subparsers.add_parser(
        "sync",
        help="Manually sync local tables to Snowflake"
    )
    sync_cmd_parser.add_argument(
        "tables",
        nargs="*",
        help="Tables to sync (schema.table format). If empty, syncs all recent."
    )
    sync_cmd_parser.add_argument(
        "--all", action="store_true",
        dest="sync_all",
        help="Sync all tables in local database"
    )
    
    # verify command - compare local and Snowflake row counts
    verify_parser = subparsers.add_parser(
        "verify",
        help="Verify sync by comparing local and Snowflake row counts"
    )
    verify_parser.add_argument(
        "tables",
        nargs="*",
        help="Tables to verify (schema.table format). If empty, verifies recent syncs."
    )
    verify_parser.add_argument(
        "--hours", type=int, default=24,
        help="Verify syncs from last N hours (default: 24)"
    )
    
    # cache command - manage local source cache
    cache_parser = subparsers.add_parser(
        "cache",
        help="Manage local source cache (for Snowflake-only users)"
    )
    cache_subparsers = cache_parser.add_subparsers(dest="cache_action", help="Cache actions")
    
    cache_status_parser = cache_subparsers.add_parser(
        "status",
        help="Show cache status and contents"
    )
    
    cache_refresh_parser = cache_subparsers.add_parser(
        "refresh",
        help="Refresh all cached tables from Snowflake"
    )
    cache_refresh_parser.add_argument(
        "--force", action="store_true",
        help="Force refresh even if cache is fresh"
    )
    
    cache_clear_parser = cache_subparsers.add_parser(
        "clear",
        help="Clear all cached data"
    )
    
    # summary command - show last run summary
    summary_parser = subparsers.add_parser(
        "summary",
        help="Show summary of last dbt run"
    )
    
    # health command - run health checks
    health_parser = subparsers.add_parser(
        "health",
        help="Run health checks and show status"
    )
    
    args = parser.parse_args()
    
    if args.command == "savings":
        cmd_savings(args)
    elif args.command == "status":
        cmd_status()
    elif args.command == "sync-status":
        cmd_sync_status(args.hours)
    elif args.command == "sync":
        cmd_sync(args)
    elif args.command == "verify":
        cmd_verify(args)
    elif args.command == "explain":
        cmd_explain(args.input)
    elif args.command == "stats":
        cmd_stats()
    elif args.command == "update-stats":
        cmd_update_stats(args.days)
    elif args.command == "version":
        cmd_version()
    elif args.command == "help":
        cmd_help()
    elif args.command == "cache":
        cmd_cache(args)
    elif args.command == "summary":
        cmd_summary()
    elif args.command == "health":
        cmd_health()
    else:
        parser.print_help()


def cmd_savings(args):
    """Show cost savings report."""
    from dbt.adapters.icebreaker.savings import print_savings
    
    # Check for dashboard flag first
    if hasattr(args, 'dashboard') and args.dashboard:
        period = "dashboard"
    elif args.today:
        period = "today"
    elif args.week:
        period = "week"
    elif args.month:
        period = "month"
    else:
        period = "all"
    
    print_savings(period)


def cmd_status():
    """Show Icebreaker connection status."""
    import os
    
    lines = [
        "Version:  dbt-icebreaker v0.2.0",
        "Motto:    Zero-config cost optimization for dbt",
    ]
    console.panel("\n".join(lines), title="Icebreaker Status")
    
    # Check for cloud connections (MVP: Snowflake only)
    snowflake = bool(os.environ.get("SNOWFLAKE_ACCOUNT"))
    
    console.success("DuckDB: Always active (local, FREE)")
    if snowflake:
        console.success("Snowflake: Connected")
    else:
        console.info("Snowflake: Not configured")
    
    # Show quick health summary
    try:
        from dbt.adapters.icebreaker.health_check import run_health_check
        console.info(run_health_check())
    except Exception as e:
        console.warn(f"Health check unavailable: {e}")


def cmd_health():
    """Run health checks."""
    from dbt.adapters.icebreaker.health_check import run_health_check
    console.info(run_health_check())
    
    from dbt.adapters.icebreaker.savings import get_db_path
    console.info(f"Savings data: {get_db_path()}")


def cmd_sync_status(hours: int):
    """Show sync status and history."""
    from dbt.adapters.icebreaker.sync_manager import SyncLedger
    
    ledger = SyncLedger()
    stats = ledger.get_stats(since_hours=hours)
    
    rows = [
        ["Period", f"Last {hours} hours"],
        ["Total syncs", str(stats['total_syncs'])],
        ["Successful", str(stats['successful'])],
        ["Verified", str(stats['verified'])],
        ["Failed", str(stats['failed'])],
        ["Success rate", f"{stats['success_rate']:.1f}%"],
        ["Avg duration", f"{stats['avg_duration_seconds']:.2f}s"],
        ["Total rows", f"{stats['total_rows_synced']:,}"],
    ]
    console.table(["Metric", "Value"], rows, title="Sync Status")
    
    # Show failed syncs if any
    failed = ledger.get_failed_syncs(since_hours=hours)
    if failed:
        for result in failed[:10]:
            console.error(f"{result.table_id}: {result.error}")


def cmd_sync(args):
    """Manually sync tables from local DuckDB to Snowflake."""
    import os
    import duckdb
    
    console.panel("Manual Sync to Snowflake", title="Sync")
    
    # Check Snowflake config
    if not os.environ.get("SNOWFLAKE_ACCOUNT"):
        console.error("Snowflake not configured. Set SNOWFLAKE_ACCOUNT environment variable.")
        return
    
    # Get local database path
    local_db = os.path.expanduser("~/.icebreaker/local.duckdb")
    if not os.path.exists(local_db):
        local_db = ".icebreaker/local.duckdb"
    
    if not os.path.exists(local_db):
        console.error(f"Local database not found at {local_db}")
        console.info("Run 'dbt run' first to create local tables.")
        return
    
    try:
        conn = duckdb.connect(local_db, read_only=True)
        
        # Get tables to sync
        if args.tables:
            tables = args.tables
        elif args.sync_all:
            result = conn.execute("""
                SELECT table_schema || '.' || table_name 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'main')
                  AND table_type = 'BASE TABLE'
            """).fetchall()
            tables = [r[0] for r in result]
        else:
            result = conn.execute("""
                SELECT table_schema || '.' || table_name 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                  AND table_type = 'BASE TABLE'
                LIMIT 10
            """).fetchall()
            tables = [r[0] for r in result]
        
        if not tables:
            console.warn("No tables found to sync.")
            return
        
        console.step(f"Syncing {len(tables)} table(s) to Snowflake...")
        
        from dbt.adapters.icebreaker.warehouse_sync import sync_to_snowflake
        
        # Get Snowflake connection
        try:
            import snowflake.connector
            sf_conn = snowflake.connector.connect(
                account=os.environ.get("SNOWFLAKE_ACCOUNT"),
                user=os.environ.get("SNOWFLAKE_USER"),
                password=os.environ.get("SNOWFLAKE_PASSWORD"),
                database=os.environ.get("SNOWFLAKE_DATABASE", "DEV"),
                warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            )
        except Exception as e:
            console.error(f"Failed to connect to Snowflake: {e}")
            return
        
        success = 0
        failed = 0
        
        for table in tables:
            parts = table.split(".")
            if len(parts) == 2:
                schema, table_name = parts
            else:
                schema, table_name = "main", parts[0]
            
            try:
                result = sync_to_snowflake(conn, sf_conn, schema, table_name)
                if result.success:
                    success += 1
                else:
                    failed += 1
                    console.error(f"{table}: {result.error}")
            except Exception as e:
                failed += 1
                console.error(f"{table}: {e}")
        
        console.success(f"Synced: {success}, Failed: {failed}")
        
        conn.close()
        sf_conn.close()
        
    except Exception as e:
        console.error(f"Error: {e}")


def cmd_verify(args):
    """Verify sync by comparing row counts between local and Snowflake."""
    import os
    import duckdb
    
    console.panel("Sync Verification", title="Verify")
    
    # Check Snowflake config
    if not os.environ.get("SNOWFLAKE_ACCOUNT"):
        console.error("Snowflake not configured. Set SNOWFLAKE_ACCOUNT environment variable.")
        return
    
    # Get local database path
    local_db = os.path.expanduser("~/.icebreaker/local.duckdb")
    if not os.path.exists(local_db):
        local_db = ".icebreaker/local.duckdb"
    
    if not os.path.exists(local_db):
        console.error(f"Local database not found at {local_db}")
        return
    
    try:
        conn = duckdb.connect(local_db, read_only=True)
        
        # Get tables to verify
        if args.tables:
            tables = args.tables
        else:
            from dbt.adapters.icebreaker.sync_manager import SyncLedger
            ledger = SyncLedger()
            recent = ledger.get_recent_syncs(since_hours=args.hours)
            tables = [r.table_id for r in recent if r.success]
            
            if not tables:
                result = conn.execute("""
                    SELECT table_schema || '.' || table_name 
                    FROM information_schema.tables 
                    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                      AND table_type = 'BASE TABLE'
                    LIMIT 20
                """).fetchall()
                tables = [r[0] for r in result]
        
        if not tables:
            console.warn("No tables found to verify.")
            return
        
        console.step(f"Verifying {len(tables)} table(s)...")
        
        # Get Snowflake connection
        try:
            import snowflake.connector
            sf_conn = snowflake.connector.connect(
                account=os.environ.get("SNOWFLAKE_ACCOUNT"),
                user=os.environ.get("SNOWFLAKE_USER"),
                password=os.environ.get("SNOWFLAKE_PASSWORD"),
                database=os.environ.get("SNOWFLAKE_DATABASE", "DEV"),
                warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            )
            sf_cursor = sf_conn.cursor()
        except Exception as e:
            console.error(f"Failed to connect to Snowflake: {e}")
            return
        
        matched = 0
        mismatched = 0
        rows = []
        
        for table in tables:
            parts = table.split(".")
            if len(parts) == 2:
                schema, table_name = parts
            else:
                schema, table_name = "main", parts[0]
            
            try:
                local_count = conn.execute(
                    f"SELECT COUNT(*) FROM {schema}.{table_name}"
                ).fetchone()[0]
            except Exception:
                local_count = "N/A"
            
            try:
                sf_cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
                sf_count = sf_cursor.fetchone()[0]
            except Exception:
                sf_count = "N/A"
            
            if local_count == "N/A" or sf_count == "N/A":
                status = "?"
            elif local_count == sf_count:
                status = "ok"
                matched += 1
            else:
                status = "MISMATCH"
                mismatched += 1
            
            local_str = f"{local_count:,}" if isinstance(local_count, int) else local_count
            sf_str = f"{sf_count:,}" if isinstance(sf_count, int) else sf_count
            rows.append([table, local_str, sf_str, status])
        
        console.table(["Table", "Local", "Snowflake", "Status"], rows, title="Row Count Verification")
        console.success(f"Matched: {matched}, Mismatched: {mismatched}")
        
        conn.close()
        sf_cursor.close()
        sf_conn.close()
        
    except Exception as e:
        console.error(f"Error: {e}")


def cmd_explain(input_str: str):
    """Explain routing decision for SQL."""
    from dbt.adapters.icebreaker.auto_router import AutoRouter
    from dbt.adapters.icebreaker.memory_guard import MemoryGuard, PreFlightChecker
    
    console.panel("Routing Explanation", title="Explain")
    
    # Check if input is a file or SQL
    if input_str.endswith(".sql") and Path(input_str).exists():
        sql = Path(input_str).read_text()
        console.info(f"File: {input_str}")
    else:
        sql = input_str
        console.info("Input: Inline SQL")
    
    # Create mock model
    model = {
        "name": "cli_query",
        "config": {},
        "depends_on": {"nodes": []},
    }
    
    # Get routing decision
    router = AutoRouter()
    explanation = router.explain(sql, model)
    console.info(explanation)
    
    # Run pre-flight checks
    checker = PreFlightChecker()
    warnings = checker.check(sql, model)
    console.info(checker.format_warnings(warnings))


def cmd_stats():
    """Show system and performance statistics."""
    from dbt.adapters.icebreaker.memory_guard import MemoryGuard
    from dbt.adapters.icebreaker.savings import get_summary
    
    # System info
    guard = MemoryGuard()
    sys_info = guard.get_system_info()
    
    sys_rows = [
        ["Total RAM", f"{sys_info['total_gb']:.1f} GB"],
        ["Available", f"{sys_info['available_gb']:.1f} GB"],
        ["Used", f"{sys_info['used_pct']:.1f}%"],
        ["Max query size", f"{sys_info['max_query_gb']:.1f} GB"],
    ]
    console.table(["Resource", "Value"], sys_rows, title="System Resources")
    
    # Execution stats
    try:
        summary = get_summary("week")
        local_pct = summary.get('local_runs', 0) / max(summary.get('total_queries', 1), 1) * 100
        exec_rows = [
            ["Total queries", f"{summary.get('total_queries', 0):,}"],
            ["Local runs", f"{summary.get('local_runs', 0):,}"],
            ["Cloud runs", f"{summary.get('cloud_runs', 0):,}"],
            ["Local rate", f"{local_pct:.1f}%"],
            ["Savings (week)", f"${summary.get('total_savings', 0):.2f}"],
        ]
        console.table(["Metric", "Value"], exec_rows, title="Execution Stats (7 Days)")
    except Exception as e:
        console.warn(f"Could not load execution stats: {e}")


def cmd_update_stats(days: int):
    """Fetch and cache cloud execution statistics."""
    console.step(f"Fetching {days} days of cloud execution stats...")
    
    # This will be implemented in Phase 4 (Telemetry)
    console.warn("Cloud stats harvesting not yet implemented. Coming soon!")
    
    # Create placeholder file
    stats_dir = Path(".icebreaker")
    stats_dir.mkdir(exist_ok=True)
    
    stats_file = stats_dir / "cloud_stats.json"
    stats_file.write_text(json.dumps({"models": {}, "fetched_at": None}, indent=2))
    
    console.success(f"Created {stats_file}")


def cmd_version():
    """Print version information."""
    version_text = (
        "dbt-icebreaker v0.2.0\n"
        "Zero-config cost optimization for dbt.\n\n"
        "Features:\n"
        "  - Automatic SQL-based routing (no tags needed)\n"
        "  - Memory-aware execution\n"
        "  - Verified sync with retry\n"
        "  - Cost savings tracking\n\n"
        "https://github.com/your-org/dbt-icebreaker"
    )
    console.panel(version_text, title="Version")


def cmd_help():
    """Print detailed help with examples."""
    help_text = (
        "Icebreaker runs dbt models locally with DuckDB and syncs\n"
        "results to Snowflake. Read from Iceberg, compute free, sync fast.\n"
    )
    console.panel(help_text, title="Icebreaker Help")
    
    cmd_rows = [
        ["savings [--today|--week|--month]", "Show cost savings from running locally vs cloud"],
        ["status", "Show connection status for DuckDB and Snowflake"],
        ["sync [tables...] [--all]", "Manually sync local tables to Snowflake"],
        ["verify [tables...] [--hours N]", "Compare row counts between local and Snowflake"],
        ["sync-status [--hours N]", "Show sync history to Snowflake"],
        ["explain <sql_file_or_query>", "Explain why a query routes to LOCAL or CLOUD"],
        ["stats", "Show system resources and execution statistics"],
        ["cache status|refresh|clear", "Manage local source cache"],
        ["version", "Show version information"],
    ]
    console.table(["Command", "Description"], cmd_rows, title="Commands")
    
    freshness_text = (
        "Icebreaker auto-syncs to Snowflake after each local run.\n\n"
        "Workflow:\n"
        "  1. dbt run  ->  Model executes locally (DuckDB)\n"
        "  2.          ->  Results sync to Snowflake automatically\n"
        "  3.          ->  Query in Snowflake immediately!\n\n"
        "The sync happens at the END of each model execution,\n"
        "so Snowflake is fresh as soon as dbt reports success.\n\n"
        "To verify: icebreaker sync-status"
    )
    console.panel(freshness_text, title="Data Freshness")


def cmd_summary():
    """Show summary of last dbt run."""
    from dbt.adapters.icebreaker.run_summary import get_run_summary
    
    summary = get_run_summary()
    last_session = summary.get_last_session()
    
    if not last_session:
        console.info("No run sessions found yet. Run 'dbt run' to generate a summary.")
        return
    
    models = last_session.get("models", [])
    local_count = sum(1 for m in models if m.get("venue") == "LOCAL")
    cloud_count = sum(1 for m in models if m.get("venue") == "CLOUD")
    success_count = sum(1 for m in models if m.get("success", True))
    error_count = len(models) - success_count
    total_savings = sum(m.get("estimated_cloud_cost", 0) for m in models if m.get("venue") == "LOCAL")
    total_duration = sum(m.get("duration_seconds", 0) for m in models)
    local_pct = (local_count / max(len(models), 1)) * 100
    
    summary_rows = [
        ["Session", last_session.get('session_id', 'unknown')],
        ["Started", last_session.get('started_at', 'unknown')[:19]],
        ["Models", str(len(models))],
        ["Local (FREE)", f"{local_count} ({local_pct:.0f}%)"],
        ["Cloud", str(cloud_count)],
        ["Succeeded", str(success_count)],
        ["Failed", str(error_count)],
        ["Est. Savings", f"${total_savings:.2f}"],
        ["Duration", f"{total_duration:.1f}s"],
    ]
    console.table(["Metric", "Value"], summary_rows, title="Run Summary")
    
    # Routing breakdown
    reason_counts = {}
    for m in models:
        reason = m.get("reason", "UNKNOWN")
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
    
    if reason_counts:
        route_rows = [[reason, str(count)] for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1])]
        console.table(["Reason", "Count"], route_rows, title="Routing Breakdown")
    
    # Show errors if any
    errors = [m for m in models if not m.get("success", True)]
    if errors:
        for m in errors[:5]:
            console.error(f"{m.get('name')}: {m.get('error', 'Unknown error')}")
        if len(errors) > 5:
            console.info(f"... and {len(errors) - 5} more")


def cmd_cache(args):
    """Manage local source cache."""
    from dbt.adapters.icebreaker.source_cache import get_source_cache, format_cache_status
    
    cache = get_source_cache()
    
    if args.cache_action == "status":
        status = cache.get_status()
        console.info(format_cache_status(status))
    
    elif args.cache_action == "refresh":
        console.step("Refreshing source cache...")
        
        import os
        if not os.environ.get("SNOWFLAKE_ACCOUNT"):
            console.error("Snowflake not configured. Set SNOWFLAKE_ACCOUNT environment variable.")
            return
        
        try:
            import snowflake.connector
            sf_conn = snowflake.connector.connect(
                account=os.environ.get("SNOWFLAKE_ACCOUNT"),
                user=os.environ.get("SNOWFLAKE_USER"),
                password=os.environ.get("SNOWFLAKE_PASSWORD"),
                database=os.environ.get("SNOWFLAKE_DATABASE", "DEV"),
                warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            )
            cache.snowflake_conn = sf_conn
            cache.refresh_all(force=getattr(args, 'force', False))
            sf_conn.close()
            console.success("Cache refreshed!")
        except Exception as e:
            console.error(f"Refresh failed: {e}")
    
    elif args.cache_action == "clear":
        cache.clear()
    
    else:
        status = cache.get_status()
        console.info(format_cache_status(status))
        console.info("Use 'icebreaker cache --help' for available commands.")


if __name__ == "__main__":
    main()

