"""
Icebreaker CLI

Command-line utilities for the Icebreaker adapter.
"""

import argparse
import json
import sys
from pathlib import Path


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
    
    print()
    print("🧊 ICEBREAKER STATUS")
    print("═" * 40)
    print()
    print("Version:  dbt-icebreaker v0.2.0")
    print("Motto:    Zero-config cost optimization for dbt")
    print()
    
    # Check for cloud connections (MVP: Snowflake only)
    snowflake = bool(os.environ.get("SNOWFLAKE_ACCOUNT"))
    
    print("Connected Warehouses:")
    print(f"  🟢 DuckDB:     Always active (local, FREE)")
    print(f"  {'🟢' if snowflake else '⚪'} Snowflake:  {'Connected' if snowflake else 'Not configured'}")
    print()
    
    # Show quick health summary
    try:
        from dbt.adapters.icebreaker.health_check import run_health_check
        print(run_health_check())
    except Exception as e:
        print(f"  Health check unavailable: {e}")


def cmd_health():
    """Run health checks."""
    from dbt.adapters.icebreaker.health_check import run_health_check
    print(run_health_check())
    
    # Show savings db location
    from dbt.adapters.icebreaker.savings import get_db_path
    print(f"Savings data: {get_db_path()}")
    print()
    print("═" * 40)
    print()


def cmd_sync_status(hours: int):
    """Show sync status and history."""
    from dbt.adapters.icebreaker.sync_manager import SyncLedger
    
    print()
    print("🔄 SYNC STATUS")
    print("═" * 50)
    print()
    
    ledger = SyncLedger()
    stats = ledger.get_stats(since_hours=hours)
    
    print(f"📅 Period: Last {hours} hours")
    print()
    print(f"   Total syncs:     {stats['total_syncs']}")
    print(f"   Successful:      {stats['successful']}")
    print(f"   Verified:        {stats['verified']}")
    print(f"   Failed:          {stats['failed']}")
    print(f"   Success rate:    {stats['success_rate']:.1f}%")
    print()
    print(f"   Avg duration:    {stats['avg_duration_seconds']:.2f}s")
    print(f"   Total rows:      {stats['total_rows_synced']:,}")
    print()
    
    # Show failed syncs if any
    failed = ledger.get_failed_syncs(since_hours=hours)
    if failed:
        print("❌ Failed syncs:")
        for result in failed[:10]:  # Show max 10
            print(f"   • {result.table_id}: {result.error}")
        print()
    
    print("═" * 50)
    print()


def cmd_sync(args):
    """Manually sync tables from local DuckDB to Snowflake."""
    import os
    import duckdb
    
    print()
    print("🔄 MANUAL SYNC TO SNOWFLAKE")
    print("═" * 50)
    print()
    
    # Check Snowflake config
    if not os.environ.get("SNOWFLAKE_ACCOUNT"):
        print("❌ Snowflake not configured. Set SNOWFLAKE_ACCOUNT environment variable.")
        print()
        return
    
    # Get local database path
    local_db = os.path.expanduser("~/.icebreaker/local.duckdb")
    if not os.path.exists(local_db):
        local_db = ".icebreaker/local.duckdb"
    
    if not os.path.exists(local_db):
        print(f"❌ Local database not found at {local_db}")
        print("   Run 'dbt run' first to create local tables.")
        print()
        return
    
    try:
        conn = duckdb.connect(local_db, read_only=True)
        
        # Get tables to sync
        if args.tables:
            tables = args.tables
        elif args.sync_all:
            # Get all user tables
            result = conn.execute("""
                SELECT table_schema || '.' || table_name 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'main')
                  AND table_type = 'BASE TABLE'
            """).fetchall()
            tables = [r[0] for r in result]
        else:
            # Get recently modified tables (last 24h based on file modification)
            result = conn.execute("""
                SELECT table_schema || '.' || table_name 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                  AND table_type = 'BASE TABLE'
                LIMIT 10
            """).fetchall()
            tables = [r[0] for r in result]
        
        if not tables:
            print("⚠️  No tables found to sync.")
            print()
            return
        
        print(f"📦 Syncing {len(tables)} table(s) to Snowflake...")
        print()
        
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
            print(f"❌ Failed to connect to Snowflake: {e}")
            print()
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
                    print(f"   ❌ {table}: {result.error}")
            except Exception as e:
                failed += 1
                print(f"   ❌ {table}: {e}")
        
        print()
        print(f"✅ Synced: {success}, Failed: {failed}")
        
        conn.close()
        sf_conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print()
    print("═" * 50)
    print()


def cmd_verify(args):
    """Verify sync by comparing row counts between local and Snowflake."""
    import os
    import duckdb
    
    print()
    print("✓ SYNC VERIFICATION")
    print("═" * 50)
    print()
    
    # Check Snowflake config
    if not os.environ.get("SNOWFLAKE_ACCOUNT"):
        print("❌ Snowflake not configured. Set SNOWFLAKE_ACCOUNT environment variable.")
        print()
        return
    
    # Get local database path
    local_db = os.path.expanduser("~/.icebreaker/local.duckdb")
    if not os.path.exists(local_db):
        local_db = ".icebreaker/local.duckdb"
    
    if not os.path.exists(local_db):
        print(f"❌ Local database not found at {local_db}")
        print()
        return
    
    try:
        conn = duckdb.connect(local_db, read_only=True)
        
        # Get tables to verify
        if args.tables:
            tables = args.tables
        else:
            # Get tables from sync ledger
            from dbt.adapters.icebreaker.sync_manager import SyncLedger
            ledger = SyncLedger()
            recent = ledger.get_recent_syncs(since_hours=args.hours)
            tables = [r.table_id for r in recent if r.success]
            
            if not tables:
                # Fall back to all tables
                result = conn.execute("""
                    SELECT table_schema || '.' || table_name 
                    FROM information_schema.tables 
                    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                      AND table_type = 'BASE TABLE'
                    LIMIT 20
                """).fetchall()
                tables = [r[0] for r in result]
        
        if not tables:
            print("⚠️  No tables found to verify.")
            print()
            return
        
        print(f"📊 Verifying {len(tables)} table(s)...")
        print()
        print(f"{'Table':<40} {'Local':>12} {'Snowflake':>12} {'Match':>8}")
        print("─" * 74)
        
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
            print(f"❌ Failed to connect to Snowflake: {e}")
            print()
            return
        
        matched = 0
        mismatched = 0
        
        for table in tables:
            parts = table.split(".")
            if len(parts) == 2:
                schema, table_name = parts
            else:
                schema, table_name = "main", parts[0]
            
            # Get local count
            try:
                local_count = conn.execute(
                    f"SELECT COUNT(*) FROM {schema}.{table_name}"
                ).fetchone()[0]
            except Exception:
                local_count = "N/A"
            
            # Get Snowflake count
            try:
                sf_cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
                sf_count = sf_cursor.fetchone()[0]
            except Exception:
                sf_count = "N/A"
            
            # Compare
            if local_count == "N/A" or sf_count == "N/A":
                status = "⚠️"
            elif local_count == sf_count:
                status = "✅"
                matched += 1
            else:
                status = "❌"
                mismatched += 1
            
            local_str = f"{local_count:,}" if isinstance(local_count, int) else local_count
            sf_str = f"{sf_count:,}" if isinstance(sf_count, int) else sf_count
            
            print(f"{table:<40} {local_str:>12} {sf_str:>12} {status:>8}")
        
        print("─" * 74)
        print()
        print(f"✅ Matched: {matched}  ❌ Mismatched: {mismatched}")
        
        conn.close()
        sf_cursor.close()
        sf_conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print()
    print("═" * 50)
    print()


def cmd_explain(input_str: str):
    """Explain routing decision for SQL."""
    from dbt.adapters.icebreaker.auto_router import AutoRouter
    from dbt.adapters.icebreaker.memory_guard import MemoryGuard, PreFlightChecker
    
    print()
    print("🔍 ROUTING EXPLANATION")
    print("═" * 50)
    print()
    
    # Check if input is a file or SQL
    if input_str.endswith(".sql") and Path(input_str).exists():
        sql = Path(input_str).read_text()
        print(f"File: {input_str}")
    else:
        sql = input_str
        print("Input: Inline SQL")
    print()
    
    # Create mock model
    model = {
        "name": "cli_query",
        "config": {},
        "depends_on": {"nodes": []},
    }
    
    # Get routing decision
    router = AutoRouter()
    explanation = router.explain(sql, model)
    print(explanation)
    print()
    
    # Run pre-flight checks
    checker = PreFlightChecker()
    warnings = checker.check(sql, model)
    print()
    print(checker.format_warnings(warnings))
    print()
    
    print("═" * 50)
    print()


def cmd_stats():
    """Show system and performance statistics."""
    from dbt.adapters.icebreaker.memory_guard import MemoryGuard
    from dbt.adapters.icebreaker.savings import get_summary
    
    print()
    print("📊 ICEBREAKER STATISTICS")
    print("═" * 50)
    print()
    
    # System info
    guard = MemoryGuard()
    sys_info = guard.get_system_info()
    
    print("💾 System Resources:")
    print(f"   Total RAM:       {sys_info['total_gb']:.1f} GB")
    print(f"   Available:       {sys_info['available_gb']:.1f} GB")
    print(f"   Used:            {sys_info['used_pct']:.1f}%")
    print(f"   Max query size:  {sys_info['max_query_gb']:.1f} GB")
    print()
    
    # Execution stats
    try:
        summary = get_summary("week")
        print("🏃 Execution Stats (Last 7 Days):")
        print(f"   Total queries:   {summary.get('total_queries', 0):,}")
        print(f"   Local runs:      {summary.get('local_runs', 0):,}")
        print(f"   Cloud runs:      {summary.get('cloud_runs', 0):,}")
        local_pct = summary.get('local_runs', 0) / max(summary.get('total_queries', 1), 1) * 100
        print(f"   Local rate:      {local_pct:.1f}%")
        print()
        print("💰 Cost Savings:")
        print(f"   This week:       ${summary.get('total_savings', 0):.2f}")
        print()
    except Exception as e:
        print(f"   ⚠️ Could not load execution stats: {e}")
        print()
    
    print("═" * 50)
    print()


def cmd_update_stats(days: int):
    """Fetch and cache cloud execution statistics."""
    print(f"📊 Fetching {days} days of cloud execution stats...")
    
    # This will be implemented in Phase 4 (Telemetry)
    print("⚠️  Cloud stats harvesting not yet implemented.")
    print("   Coming soon!")
    
    # Create placeholder file
    stats_dir = Path(".icebreaker")
    stats_dir.mkdir(exist_ok=True)
    
    stats_file = stats_dir / "cloud_stats.json"
    stats_file.write_text(json.dumps({"models": {}, "fetched_at": None}, indent=2))
    
    print(f"✅ Created {stats_file}")


def cmd_version():
    """Print version information."""
    print()
    print("🧊 dbt-icebreaker v0.2.0")
    print("   Zero-config cost optimization for dbt.")
    print()
    print("   Features:")
    print("   • Automatic SQL-based routing (no tags needed)")
    print("   • Memory-aware execution")
    print("   • Verified sync with retry")
    print("   • Cost savings tracking")
    print()
    print("   https://github.com/your-org/dbt-icebreaker")
    print()


def cmd_help():
    """Print detailed help with examples."""
    print()
    print("🧊 ICEBREAKER HELP")
    print("═" * 60)
    print()
    print("Icebreaker runs dbt models locally with DuckDB and syncs")
    print("results to Snowflake. Read from Iceberg, compute free, sync fast.")
    print()
    print("COMMANDS:")
    print("─" * 60)
    print()
    print("  icebreaker savings [--today|--week|--month]")
    print("      Show cost savings from running locally vs cloud.")
    print("      Example: icebreaker savings --week")
    print()
    print("  icebreaker status")
    print("      Show connection status for DuckDB and Snowflake.")
    print()
    print("  icebreaker sync [tables...] [--all]")
    print("      Manually sync local tables to Snowflake.")
    print("      Example: icebreaker sync analytics.fct_orders")
    print("      Example: icebreaker sync --all")
    print()
    print("  icebreaker verify [tables...] [--hours N]")
    print("      Compare row counts between local and Snowflake.")
    print("      Example: icebreaker verify analytics.fct_orders")
    print("      Example: icebreaker verify --hours 4")
    print()
    print("  icebreaker sync-status [--hours N]")
    print("      Show sync history to Snowflake.")
    print("      Example: icebreaker sync-status --hours 4")
    print()
    print("  icebreaker explain <sql_file_or_query>")
    print("      Explain why a query routes to LOCAL or CLOUD.")
    print("      Example: icebreaker explain models/fct_orders.sql")
    print()
    print("  icebreaker stats")
    print("      Show system resources and execution statistics.")
    print()
    print("  icebreaker version")
    print("      Show version information.")
    print()
    print("DATA FRESHNESS:")
    print("─" * 60)
    print()
    print("  Icebreaker auto-syncs to Snowflake after each local run.")
    print()
    print("  Workflow:")
    print("    1. dbt run  →  Model executes locally (DuckDB)")
    print("    2.          →  Results sync to Snowflake automatically")
    print("    3.          →  Query in Snowflake immediately!")
    print()
    print("  The sync happens at the END of each model execution,")
    print("  so Snowflake is fresh as soon as dbt reports success.")
    print()
    print("  To verify sync status:")
    print("    icebreaker sync-status")
    print()
    print("CONFIGURATION:")
    print("─" * 60)
    print()
    print("  profiles.yml example:")
    print()
    print("    my_project:")
    print("      target: dev")
    print("      outputs:")
    print("        dev:")
    print("          type: icebreaker")
    print("          iceberg_catalog_uri: https://catalog.example.com")
    print("          iceberg_catalog_warehouse: s3://my-lake/warehouse")
    print("          cloud_type: snowflake")
    print("          snowflake_account: \"{{ env_var('SNOWFLAKE_ACCOUNT') }}\"")
    print("          snowflake_user: \"{{ env_var('SNOWFLAKE_USER') }}\"")
    print("          snowflake_password: \"{{ env_var('SNOWFLAKE_PASSWORD') }}\"")
    print("          snowflake_database: DEV")
    print("          snowflake_schema: MY_DEV")
    print()
    print("═" * 60)
    print()


def cmd_summary():
    """Show summary of last dbt run."""
    from dbt.adapters.icebreaker.run_summary import get_run_summary
    
    print()
    summary = get_run_summary()
    last_session = summary.get_last_session()
    
    if not last_session:
        print("📊 No run sessions found yet.")
        print("   Run 'dbt run' to generate a summary.")
        print()
        return
    
    # Reconstruct the session for display
    print("═" * 60)
    print("🧊 ICEBREAKER RUN SUMMARY")
    print("═" * 60)
    print()
    
    models = last_session.get("models", [])
    local_count = sum(1 for m in models if m.get("venue") == "LOCAL")
    cloud_count = sum(1 for m in models if m.get("venue") == "CLOUD")
    success_count = sum(1 for m in models if m.get("success", True))
    error_count = len(models) - success_count
    total_savings = sum(m.get("estimated_cloud_cost", 0) for m in models if m.get("venue") == "LOCAL")
    total_duration = sum(m.get("duration_seconds", 0) for m in models)
    
    local_pct = (local_count / max(len(models), 1)) * 100
    
    print(f"📅 Session: {last_session.get('session_id')}")
    print(f"   Started: {last_session.get('started_at', 'unknown')[:19]}")
    print()
    print(f"📊 Models: {len(models)} total")
    print(f"   🏠 Local (FREE):  {local_count} ({local_pct:.0f}%)")
    print(f"   ☁️  Cloud:         {cloud_count}")
    print(f"   ✅ Succeeded:     {success_count}")
    if error_count > 0:
        print(f"   ❌ Failed:        {error_count}")
    print()
    print(f"💰 Estimated Savings: ${total_savings:.2f}")
    print(f"⏱️  Total Duration:    {total_duration:.1f}s")
    print()
    
    # Routing breakdown
    reason_counts = {}
    for m in models:
        reason = m.get("reason", "UNKNOWN")
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
    
    if reason_counts:
        print("📍 Routing Breakdown:")
        for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
            icon = "🏠" if "LOCAL" in reason or "AUTO" in reason or "CHEAP" in reason else "☁️"
            print(f"   {icon} {reason}: {count}")
        print()
    
    # Show errors if any
    errors = [m for m in models if not m.get("success", True)]
    if errors:
        print("❌ Errors:")
        for m in errors[:5]:
            print(f"   • {m.get('name')}: {m.get('error', 'Unknown error')}")
        if len(errors) > 5:
            print(f"   ... and {len(errors) - 5} more")
        print()
    
    print("═" * 60)
    print()


def cmd_cache(args):
    """Manage local source cache."""
    from dbt.adapters.icebreaker.source_cache import get_source_cache, format_cache_status
    
    cache = get_source_cache()
    
    if args.cache_action == "status":
        status = cache.get_status()
        print()
        print(format_cache_status(status))
        print()
    
    elif args.cache_action == "refresh":
        print()
        print("🔄 Refreshing source cache...")
        print()
        
        # Need Snowflake connection to refresh
        import os
        if not os.environ.get("SNOWFLAKE_ACCOUNT"):
            print("❌ Snowflake not configured. Set SNOWFLAKE_ACCOUNT environment variable.")
            print()
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
            print()
            print("✅ Cache refreshed!")
        except Exception as e:
            print(f"❌ Refresh failed: {e}")
        print()
    
    elif args.cache_action == "clear":
        print()
        cache.clear()
        print()
    
    else:
        # No subcommand - show status by default
        status = cache.get_status()
        print()
        print(format_cache_status(status))
        print()
        print("Use 'icebreaker cache --help' for available commands.")
        print()


if __name__ == "__main__":
    main()

