"""
Sync Manager for Icebreaker.

Handles reliable data synchronization between local and cloud engines
with retry logic, verification, and a sync ledger for tracking.
"""

import os
import time
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class SyncResult:
    """Result of a sync operation."""
    success: bool
    table_id: str
    source_engine: str
    target_engine: str
    source_row_count: int = 0
    target_row_count: int = 0
    verified: bool = False
    duration_seconds: float = 0.0
    error: Optional[str] = None
    attempt: int = 1
    
    def __str__(self) -> str:
        if self.success:
            v = "✓" if self.verified else "?"
            return f"✅ {self.table_id}: {self.source_row_count} rows synced ({v})"
        else:
            return f"❌ {self.table_id}: {self.error}"


@dataclass
class SyncConfig:
    """Configuration for sync operations."""
    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    verify_row_counts: bool = True
    ledger_path: str = "~/.icebreaker/sync_ledger.db"


class SyncManager:
    """
    Manages reliable sync between engines.
    
    Features:
    - Retry logic for transient failures
    - Row count verification
    - Sync ledger for tracking/debugging
    """
    
    def __init__(
        self,
        local_conn: Optional[Any] = None,
        cloud_conn: Optional[Any] = None,
        config: Optional[SyncConfig] = None,
    ):
        self.local = local_conn
        self.cloud = cloud_conn
        self.config = config or SyncConfig()
        self._ledger: Optional[SyncLedger] = None
    
    @property
    def ledger(self) -> 'SyncLedger':
        """Lazy-initialize the sync ledger."""
        if self._ledger is None:
            self._ledger = SyncLedger(self.config.ledger_path)
        return self._ledger
    
    def sync_table(
        self,
        schema: str,
        table: str,
        source_engine: str = "local",
        target_engine: str = "cloud",
    ) -> SyncResult:
        """
        Sync a table with verification and retry.
        
        Args:
            schema: Schema name
            table: Table name
            source_engine: "local" or "cloud"
            target_engine: "local" or "cloud"
            
        Returns:
            SyncResult with success status and details
        """
        table_id = f"{schema}.{table}"
        start_time = time.time()
        
        for attempt in range(1, self.config.max_retries + 1):
            try:
                # Get source row count
                source_count = self._get_row_count(source_engine, schema, table)
                
                # Perform the sync
                self._copy_table(source_engine, target_engine, schema, table)
                
                # Verify target row count
                if self.config.verify_row_counts:
                    target_count = self._get_row_count(target_engine, schema, table)
                    verified = source_count == target_count
                    
                    if not verified:
                        raise Exception(
                            f"Row count mismatch: source={source_count}, target={target_count}"
                        )
                else:
                    target_count = source_count
                    verified = False
                
                # Success!
                duration = time.time() - start_time
                result = SyncResult(
                    success=True,
                    table_id=table_id,
                    source_engine=source_engine,
                    target_engine=target_engine,
                    source_row_count=source_count,
                    target_row_count=target_count,
                    verified=verified,
                    duration_seconds=duration,
                    attempt=attempt,
                )
                
                # Record to ledger
                self.ledger.record(result)
                
                return result
                
            except Exception as e:
                if attempt < self.config.max_retries:
                    print(f"⚠️ Sync attempt {attempt} failed: {e}")
                    time.sleep(self.config.retry_delay_seconds * attempt)
                else:
                    # Max retries exceeded
                    duration = time.time() - start_time
                    result = SyncResult(
                        success=False,
                        table_id=table_id,
                        source_engine=source_engine,
                        target_engine=target_engine,
                        duration_seconds=duration,
                        error=str(e),
                        attempt=attempt,
                    )
                    self.ledger.record(result)
                    return result
        
        # Should not reach here
        return SyncResult(
            success=False,
            table_id=table_id,
            source_engine=source_engine,
            target_engine=target_engine,
            error="Unknown error",
        )
    
    def _get_row_count(self, engine: str, schema: str, table: str) -> int:
        """Get row count for a table."""
        conn = self.local if engine == "local" else self.cloud
        if conn is None:
            return 0
        
        try:
            result = conn.execute(
                f"SELECT COUNT(*) FROM {schema}.{table}"
            ).fetchone()
            return result[0] if result else 0
        except Exception:
            return 0
    
    def _copy_table(
        self,
        source_engine: str,
        target_engine: str,
        schema: str,
        table: str,
    ) -> None:
        """Copy table between engines."""
        
        if source_engine == "local" and target_engine == "cloud":
            # Local → Cloud: Use DuckDB ATTACH if available, else Parquet
            if self._is_attached():
                # Fast path: direct cross-database copy
                self.cloud.execute(f"""
                    CREATE OR REPLACE TABLE {schema}.{table} AS
                    SELECT * FROM local_db.{schema}.{table}
                """)
            else:
                self._copy_via_parquet(
                    self.local, self.cloud, schema, table
                )
                
        elif source_engine == "cloud" and target_engine == "local":
            # Cloud → Local: Use DuckDB ATTACH if available
            if self._is_attached():
                self.cloud.execute(f"""
                    CREATE OR REPLACE TABLE local_db.{schema}.{table} AS
                    SELECT * FROM {schema}.{table}
                """)
            else:
                self._copy_via_parquet(
                    self.cloud, self.local, schema, table
                )
        else:
            raise ValueError(f"Invalid sync direction: {source_engine} → {target_engine}")
    
    def _is_attached(self) -> bool:
        """Check if local_db is attached to cloud connection."""
        if self.cloud is None:
            return False
        
        try:
            result = self.cloud.execute(
                "SELECT database_name FROM duckdb_databases() WHERE database_name = 'local_db'"
            ).fetchone()
            return result is not None
        except Exception:
            return False
    
    def _copy_via_parquet(
        self,
        source_conn: Any,
        target_conn: Any,
        schema: str,
        table: str,
    ) -> None:
        """Copy table via intermediate Parquet file."""
        import tempfile
        
        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = os.path.join(tmpdir, f"{table}.parquet")
            
            # Export to Parquet
            source_conn.execute(f"""
                COPY (SELECT * FROM {schema}.{table}) 
                TO '{parquet_path}' (FORMAT PARQUET)
            """)
            
            # Import from Parquet
            target_conn.execute(f"""
                CREATE SCHEMA IF NOT EXISTS {schema}
            """)
            target_conn.execute(f"""
                CREATE OR REPLACE TABLE {schema}.{table} AS
                SELECT * FROM read_parquet('{parquet_path}')
            """)


class SyncLedger:
    """
    Persistent ledger of all sync operations.
    
    Used for:
    - Debugging sync issues
    - Tracking sync history
    - Identifying tables that need re-sync
    """
    
    def __init__(self, db_path: str = "~/.icebreaker/sync_ledger.db"):
        self.db_path = os.path.expanduser(db_path)
        self._ensure_db()
    
    def _ensure_db(self) -> None:
        """Create database and tables if they don't exist."""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sync_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_id TEXT NOT NULL,
                source_engine TEXT NOT NULL,
                target_engine TEXT NOT NULL,
                source_row_count INTEGER,
                target_row_count INTEGER,
                success BOOLEAN NOT NULL,
                verified BOOLEAN,
                duration_seconds REAL,
                error TEXT,
                attempt INTEGER,
                synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sync_table_id 
            ON sync_history(table_id)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sync_time 
            ON sync_history(synced_at)
        """)
        conn.commit()
        conn.close()
    
    def record(self, result: SyncResult) -> None:
        """Record a sync operation."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT INTO sync_history 
            (table_id, source_engine, target_engine, source_row_count, 
             target_row_count, success, verified, duration_seconds, error, attempt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            result.table_id,
            result.source_engine,
            result.target_engine,
            result.source_row_count,
            result.target_row_count,
            result.success,
            result.verified,
            result.duration_seconds,
            result.error,
            result.attempt,
        ))
        conn.commit()
        conn.close()
    
    def get_last_sync(self, table_id: str) -> Optional[SyncResult]:
        """Get the last sync record for a table."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("""
            SELECT table_id, source_engine, target_engine, source_row_count,
                   target_row_count, success, verified, duration_seconds, error, attempt
            FROM sync_history
            WHERE table_id = ?
            ORDER BY synced_at DESC
            LIMIT 1
        """, (table_id,))
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return None
        
        return SyncResult(
            success=bool(row[5]),
            table_id=row[0],
            source_engine=row[1],
            target_engine=row[2],
            source_row_count=row[3] or 0,
            target_row_count=row[4] or 0,
            verified=bool(row[6]),
            duration_seconds=row[7] or 0.0,
            error=row[8],
            attempt=row[9] or 1,
        )
    
    def get_failed_syncs(self, since_hours: int = 24) -> List[SyncResult]:
        """Get failed syncs in the last N hours."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("""
            SELECT table_id, source_engine, target_engine, source_row_count,
                   target_row_count, success, verified, duration_seconds, error, attempt
            FROM sync_history
            WHERE success = 0
              AND synced_at > datetime('now', ?)
            ORDER BY synced_at DESC
        """, (f"-{since_hours} hours",))
        
        results = []
        for row in cursor:
            results.append(SyncResult(
                success=False,
                table_id=row[0],
                source_engine=row[1],
                target_engine=row[2],
                source_row_count=row[3] or 0,
                target_row_count=row[4] or 0,
                verified=bool(row[6]),
                duration_seconds=row[7] or 0.0,
                error=row[8],
                attempt=row[9] or 1,
            ))
        
        conn.close()
        return results
    
    def get_stats(self, since_hours: int = 24) -> Dict[str, Any]:
        """Get sync statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
                SUM(CASE WHEN verified = 1 THEN 1 ELSE 0 END) as verified,
                SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as failed,
                AVG(duration_seconds) as avg_duration,
                SUM(source_row_count) as total_rows
            FROM sync_history
            WHERE synced_at > datetime('now', ?)
        """, (f"-{since_hours} hours",))
        
        row = cursor.fetchone()
        conn.close()
        
        return {
            "period_hours": since_hours,
            "total_syncs": row[0] or 0,
            "successful": row[1] or 0,
            "verified": row[2] or 0,
            "failed": row[3] or 0,
            "avg_duration_seconds": row[4] or 0.0,
            "total_rows_synced": row[5] or 0,
            "success_rate": (row[1] or 0) / (row[0] or 1) * 100,
        }


class SyncOrchestrator:
    """
    Orchestrates syncs in dependency order.
    
    Ensures upstream tables are synced before downstream
    to prevent stale reads.
    """
    
    def __init__(self, sync_manager: SyncManager):
        self.manager = sync_manager
        self._pending: List[str] = []
    
    def sync_in_order(
        self,
        tables: List[Tuple[str, str]],
        dependency_graph: Optional[Dict[str, List[str]]] = None,
    ) -> List[SyncResult]:
        """
        Sync tables in topological order.
        
        Args:
            tables: List of (schema, table) tuples
            dependency_graph: Optional dict mapping table -> [dependencies]
            
        Returns:
            List of SyncResults
        """
        # If no graph provided, sync in order given
        if dependency_graph is None:
            ordered = tables
        else:
            ordered = self._topological_sort(tables, dependency_graph)
        
        results = []
        for schema, table in ordered:
            result = self.manager.sync_table(schema, table)
            results.append(result)
            
            if not result.success:
                print(f"❌ Sync failed for {schema}.{table}, stopping")
                break
            else:
                print(f"✅ Synced {schema}.{table}")
        
        return results
    
    def _topological_sort(
        self,
        tables: List[Tuple[str, str]],
        graph: Dict[str, List[str]],
    ) -> List[Tuple[str, str]]:
        """Sort tables by dependencies (dependencies first)."""
        
        # Convert to table_id format
        table_ids = {f"{s}.{t}": (s, t) for s, t in tables}
        
        # Kahn's algorithm
        in_degree = {tid: 0 for tid in table_ids}
        for tid, deps in graph.items():
            if tid in in_degree:
                for dep in deps:
                    if dep in in_degree:
                        in_degree[tid] += 1
        
        # Start with nodes that have no dependencies
        queue = [tid for tid, deg in in_degree.items() if deg == 0]
        result = []
        
        while queue:
            tid = queue.pop(0)
            result.append(table_ids[tid])
            
            # Reduce in-degree for dependents
            for other_tid, deps in graph.items():
                if tid in deps and other_tid in in_degree:
                    in_degree[other_tid] -= 1
                    if in_degree[other_tid] == 0:
                        queue.append(other_tid)
        
        # Add any remaining (for cycles or missing deps)
        for tid, st in table_ids.items():
            if st not in result:
                result.append(st)
        
        return result


# =============================================================================
# Singleton
# =============================================================================

_sync_manager: Optional[SyncManager] = None


def get_sync_manager(
    local_conn: Optional[Any] = None,
    cloud_conn: Optional[Any] = None,
) -> SyncManager:
    """Get or create the sync manager singleton."""
    global _sync_manager
    if _sync_manager is None:
        _sync_manager = SyncManager(
            local_conn=local_conn,
            cloud_conn=cloud_conn,
        )
    return _sync_manager
