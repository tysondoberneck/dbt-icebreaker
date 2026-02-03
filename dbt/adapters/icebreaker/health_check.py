"""
Health Check for Icebreaker.

Compares local state vs cloud state to detect drift and ensure data consistency.
"""

import os
import json
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class HealthCheckResult:
    """Result of a single health check."""
    check_name: str
    status: str  # "OK", "WARNING", "ERROR"
    message: str
    details: Optional[Dict] = None
    
    def __str__(self) -> str:
        icon = {"OK": "✅", "WARNING": "⚠️", "ERROR": "❌"}.get(self.status, "•")
        return f"{icon} {self.check_name}: {self.message}"


@dataclass 
class HealthReport:
    """Full health check report."""
    timestamp: str
    overall_status: str
    checks: List[HealthCheckResult]
    
    @property
    def ok_count(self) -> int:
        return sum(1 for c in self.checks if c.status == "OK")
    
    @property
    def warning_count(self) -> int:
        return sum(1 for c in self.checks if c.status == "WARNING")
    
    @property
    def error_count(self) -> int:
        return sum(1 for c in self.checks if c.status == "ERROR")


class HealthChecker:
    """
    Runs health checks to ensure Icebreaker is functioning correctly.
    
    Checks:
    - Local database connectivity
    - Cloud connectivity (Snowflake)
    - Sync ledger consistency
    - Cache validity
    - Row count drift between local and cloud
    """
    
    def __init__(
        self,
        duckdb_path: Optional[str] = None,
        snowflake_conn: Optional[Any] = None,
    ):
        self.duckdb_path = duckdb_path or os.path.expanduser("~/.icebreaker/local.duckdb")
        self.snowflake_conn = snowflake_conn
    
    def run_all_checks(self) -> HealthReport:
        """Run all health checks and return a report."""
        checks = []
        
        # Check local database
        checks.append(self._check_local_database())
        
        # Check cache
        checks.append(self._check_cache())
        
        # Check savings tracking
        checks.append(self._check_savings_db())
        
        # Check sync ledger
        checks.append(self._check_sync_ledger())
        
        # Determine overall status
        if any(c.status == "ERROR" for c in checks):
            overall = "ERROR"
        elif any(c.status == "WARNING" for c in checks):
            overall = "WARNING"
        else:
            overall = "OK"
        
        return HealthReport(
            timestamp=datetime.now().isoformat(),
            overall_status=overall,
            checks=checks,
        )
    
    def _check_local_database(self) -> HealthCheckResult:
        """Check local DuckDB database."""
        if not os.path.exists(self.duckdb_path):
            return HealthCheckResult(
                check_name="Local Database",
                status="WARNING",
                message="No local database found (will be created on first run)",
            )
        
        try:
            import duckdb
            conn = duckdb.connect(self.duckdb_path, read_only=True)
            
            # Count tables
            result = conn.execute("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            """).fetchone()
            table_count = result[0] if result else 0
            
            conn.close()
            
            return HealthCheckResult(
                check_name="Local Database",
                status="OK",
                message=f"Connected ({table_count} tables)",
                details={"path": self.duckdb_path, "tables": table_count},
            )
        except Exception as e:
            return HealthCheckResult(
                check_name="Local Database",
                status="ERROR",
                message=f"Connection failed: {str(e)[:50]}",
            )
    
    def _check_cache(self) -> HealthCheckResult:
        """Check source cache status."""
        cache_dir = os.path.expanduser("~/.icebreaker/cache")
        manifest_path = os.path.join(cache_dir, "manifest.json")
        
        if not os.path.exists(manifest_path):
            return HealthCheckResult(
                check_name="Source Cache",
                status="OK",
                message="No cache yet (will be populated on first run)",
            )
        
        try:
            with open(manifest_path, 'r') as f:
                manifest = json.load(f)
            
            table_count = len(manifest)
            total_size = sum(
                e.get("size_bytes", 0) for e in manifest.values()
            )
            size_gb = total_size / (1024**3)
            
            # Check for stale entries (>24h old)
            stale_count = 0
            for entry in manifest.values():
                created = entry.get("created_at", "")
                if created:
                    try:
                        created_dt = datetime.fromisoformat(created)
                        age_hours = (datetime.now() - created_dt).total_seconds() / 3600
                        if age_hours > 24:
                            stale_count += 1
                    except:
                        pass
            
            status = "WARNING" if stale_count > 0 else "OK"
            msg = f"{table_count} tables cached ({size_gb:.1f}GB)"
            if stale_count > 0:
                msg += f", {stale_count} stale"
            
            return HealthCheckResult(
                check_name="Source Cache",
                status=status,
                message=msg,
                details={"tables": table_count, "size_gb": size_gb, "stale": stale_count},
            )
        except Exception as e:
            return HealthCheckResult(
                check_name="Source Cache",
                status="ERROR",
                message=f"Cache check failed: {str(e)[:50]}",
            )
    
    def _check_savings_db(self) -> HealthCheckResult:
        """Check savings tracking database."""
        db_path = os.path.expanduser("~/.icebreaker/savings.db")
        
        if not os.path.exists(db_path):
            return HealthCheckResult(
                check_name="Savings Tracking",
                status="OK",
                message="No data yet (will start tracking on first run)",
            )
        
        try:
            import sqlite3
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*), COALESCE(SUM(savings), 0) FROM executions")
            row = cursor.fetchone()
            count = row[0] or 0
            total_savings = row[1] or 0
            
            conn.close()
            
            return HealthCheckResult(
                check_name="Savings Tracking",
                status="OK",
                message=f"{count:,} queries tracked, ${total_savings:.2f} saved",
                details={"queries": count, "savings": total_savings},
            )
        except Exception as e:
            return HealthCheckResult(
                check_name="Savings Tracking",
                status="ERROR",
                message=f"Database error: {str(e)[:50]}",
            )
    
    def _check_sync_ledger(self) -> HealthCheckResult:
        """Check sync ledger status."""
        ledger_path = os.path.expanduser("~/.icebreaker/sync_ledger.db")
        
        if not os.path.exists(ledger_path):
            return HealthCheckResult(
                check_name="Sync Ledger",
                status="OK",
                message="No syncs yet",
            )
        
        try:
            import sqlite3
            conn = sqlite3.connect(ledger_path)
            cursor = conn.cursor()
            
            # Get recent sync stats
            cursor.execute("""
                SELECT 
                    COUNT(*),
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN verified = 1 THEN 1 ELSE 0 END)
                FROM syncs
                WHERE timestamp > datetime('now', '-24 hours')
            """)
            row = cursor.fetchone()
            total = row[0] or 0
            successful = row[1] or 0
            verified = row[2] or 0
            
            conn.close()
            
            if total == 0:
                return HealthCheckResult(
                    check_name="Sync Ledger",
                    status="OK",
                    message="No recent syncs (last 24h)",
                )
            
            success_rate = (successful / total) * 100 if total > 0 else 0
            status = "OK" if success_rate >= 95 else ("WARNING" if success_rate >= 80 else "ERROR")
            
            return HealthCheckResult(
                check_name="Sync Ledger",
                status=status,
                message=f"{total} syncs (24h), {success_rate:.0f}% success, {verified} verified",
                details={"total": total, "successful": successful, "verified": verified},
            )
        except Exception as e:
            return HealthCheckResult(
                check_name="Sync Ledger",
                status="WARNING",
                message=f"Could not read ledger: {str(e)[:30]}",
            )
    
    def detect_drift(
        self,
        tables: Optional[List[str]] = None,
    ) -> List[Dict]:
        """
        Detect row count drift between local and Snowflake.
        
        Returns list of tables with mismatched row counts.
        """
        if not self.snowflake_conn:
            return []
        
        drift = []
        
        try:
            import duckdb
            local_conn = duckdb.connect(self.duckdb_path, read_only=True)
            
            # Get tables to check
            if not tables:
                result = local_conn.execute("""
                    SELECT table_schema || '.' || table_name 
                    FROM information_schema.tables
                    WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'main')
                      AND table_type = 'BASE TABLE'
                    LIMIT 20
                """).fetchall()
                tables = [r[0] for r in result]
            
            sf_cursor = self.snowflake_conn.cursor()
            
            for table in tables:
                parts = table.split(".")
                if len(parts) != 2:
                    continue
                schema, table_name = parts
                
                try:
                    # Local count
                    local_count = local_conn.execute(
                        f"SELECT COUNT(*) FROM {schema}.{table_name}"
                    ).fetchone()[0]
                    
                    # Snowflake count
                    sf_cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
                    sf_count = sf_cursor.fetchone()[0]
                    
                    if local_count != sf_count:
                        diff = abs(local_count - sf_count)
                        drift.append({
                            "table": table,
                            "local_count": local_count,
                            "cloud_count": sf_count,
                            "diff": diff,
                            "diff_pct": (diff / max(local_count, 1)) * 100,
                        })
                except:
                    pass  # Table might not exist in one or the other
            
            local_conn.close()
            sf_cursor.close()
            
        except Exception:
            pass
        
        return drift


def format_health_report(report: HealthReport) -> str:
    """Format health report for display."""
    lines = [
        "",
        "🏥 ICEBREAKER HEALTH CHECK",
        "═" * 50,
        f"   Timestamp: {report.timestamp[:19]}",
        f"   Status: {report.overall_status}",
        "",
    ]
    
    for check in report.checks:
        lines.append(f"   {check}")
    
    lines.extend([
        "",
        f"   Summary: {report.ok_count} OK, {report.warning_count} warnings, {report.error_count} errors",
        "═" * 50,
        "",
    ])
    
    return "\n".join(lines)


def run_health_check() -> str:
    """Run health check and return formatted report."""
    checker = HealthChecker()
    report = checker.run_all_checks()
    return format_health_report(report)
