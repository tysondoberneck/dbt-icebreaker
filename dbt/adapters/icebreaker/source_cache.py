"""
Source Cache for Icebreaker.

Automatically downloads source tables from Snowflake and caches them locally
as Parquet files. Enables zero-infrastructure local development without
requiring Iceberg catalogs or S3 buckets.

Architecture:
    Snowflake ──(first run)──► ~/.icebreaker/cache/*.parquet
        ▲                              │
        │                              ▼
        └────(sync results)────── DuckDB (FREE)
"""

import os
import json
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb


# =============================================================================
# Configuration
# =============================================================================

DEFAULT_CACHE_DIR = os.path.expanduser("~/.icebreaker/cache")
DEFAULT_CACHE_TTL_HOURS = 24
DEFAULT_CACHE_MAX_GB = 10.0


@dataclass
class CacheEntry:
    """Metadata for a cached table."""
    table_id: str  # database.schema.table
    parquet_path: str
    row_count: int
    size_bytes: int
    created_at: str
    source_type: str  # "snowflake", "bigquery", etc.
    
    @property
    def size_gb(self) -> float:
        return self.size_bytes / (1024**3)
    
    @property
    def age_hours(self) -> float:
        created = datetime.fromisoformat(self.created_at)
        return (datetime.now() - created).total_seconds() / 3600
    
    def is_stale(self, ttl_hours: float) -> bool:
        return self.age_hours > ttl_hours


@dataclass
class CacheConfig:
    """Configuration for the source cache."""
    cache_dir: str = DEFAULT_CACHE_DIR
    cache_ttl_hours: float = DEFAULT_CACHE_TTL_HOURS
    cache_max_gb: float = DEFAULT_CACHE_MAX_GB
    cache_enabled: bool = True


# =============================================================================
# Source Cache Manager
# =============================================================================

class SourceCache:
    """
    Manages local Parquet cache for source tables.
    
    Downloads tables from Snowflake (or other cloud warehouses) on first access
    and caches them locally. Subsequent runs use the cache for free local compute.
    """
    
    def __init__(
        self,
        config: Optional[CacheConfig] = None,
        snowflake_conn: Optional[Any] = None,
        duckdb_conn: Optional[duckdb.DuckDBPyConnection] = None,
    ):
        self.config = config or CacheConfig()
        self.snowflake_conn = snowflake_conn
        self.duckdb_conn = duckdb_conn
        self._manifest: Dict[str, CacheEntry] = {}
        
        # Ensure cache directory exists
        Path(self.config.cache_dir).mkdir(parents=True, exist_ok=True)
        
        # Load existing manifest
        self._load_manifest()
    
    @property
    def manifest_path(self) -> str:
        return os.path.join(self.config.cache_dir, "manifest.json")
    
    def _load_manifest(self):
        """Load cache manifest from disk."""
        if os.path.exists(self.manifest_path):
            try:
                with open(self.manifest_path, 'r') as f:
                    data = json.load(f)
                    self._manifest = {
                        k: CacheEntry(**v) for k, v in data.items()
                    }
            except Exception:
                self._manifest = {}
    
    def _save_manifest(self):
        """Save cache manifest to disk."""
        data = {k: asdict(v) for k, v in self._manifest.items()}
        with open(self.manifest_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    def get_table_id(self, database: str, schema: str, table: str) -> str:
        """Generate unique table identifier."""
        return f"{database}.{schema}.{table}".upper()
    
    def get_parquet_path(self, table_id: str) -> str:
        """Get path to cached Parquet file."""
        safe_name = table_id.replace(".", "_").lower()
        return os.path.join(self.config.cache_dir, f"{safe_name}.parquet")
    
    # =========================================================================
    # Cache Operations
    # =========================================================================
    
    def is_cached(self, database: str, schema: str, table: str) -> bool:
        """Check if a table is cached and not stale."""
        table_id = self.get_table_id(database, schema, table)
        
        if table_id not in self._manifest:
            return False
        
        entry = self._manifest[table_id]
        
        # Check if file exists
        if not os.path.exists(entry.parquet_path):
            del self._manifest[table_id]
            self._save_manifest()
            return False
        
        # Check if stale
        if entry.is_stale(self.config.cache_ttl_hours):
            return False
        
        return True
    
    def get_cached_path(self, database: str, schema: str, table: str) -> Optional[str]:
        """Get path to cached Parquet file if it exists and is fresh."""
        if not self.is_cached(database, schema, table):
            return None
        
        table_id = self.get_table_id(database, schema, table)
        return self._manifest[table_id].parquet_path
    
    def cache_table(
        self,
        database: str,
        schema: str,
        table: str,
        force: bool = False,
    ) -> CacheEntry:
        """
        Download a table from Snowflake and cache locally.
        
        Args:
            database: Source database name
            schema: Source schema name
            table: Source table name
            force: Force re-download even if cached
            
        Returns:
            CacheEntry with cache metadata
        """
        table_id = self.get_table_id(database, schema, table)
        
        # Check if already cached and fresh
        if not force and self.is_cached(database, schema, table):
            print(f"📦 Using cached: {table_id}")
            return self._manifest[table_id]
        
        print(f"⬇️  Downloading: {table_id}...")
        start_time = time.time()
        
        parquet_path = self.get_parquet_path(table_id)
        
        # Download from Snowflake to Parquet
        row_count, size_bytes = self._download_from_snowflake(
            database, schema, table, parquet_path
        )
        
        elapsed = time.time() - start_time
        
        # Create cache entry
        entry = CacheEntry(
            table_id=table_id,
            parquet_path=parquet_path,
            row_count=row_count,
            size_bytes=size_bytes,
            created_at=datetime.now().isoformat(),
            source_type="snowflake",
        )
        
        self._manifest[table_id] = entry
        self._save_manifest()
        
        print(f"✅ Cached {table_id}: {row_count:,} rows, {entry.size_gb:.2f}GB in {elapsed:.1f}s")
        
        return entry
    
    def _download_from_snowflake(
        self,
        database: str,
        schema: str,
        table: str,
        parquet_path: str,
    ) -> Tuple[int, int]:
        """
        Download table from Snowflake and save as Parquet.
        
        Returns:
            Tuple of (row_count, size_bytes)
        """
        if self.snowflake_conn is None:
            raise RuntimeError("Snowflake connection not available for caching")
        
        # Query data from Snowflake
        cursor = self.snowflake_conn.cursor()
        try:
            # Get data using pandas for efficient Parquet writing
            # Uppercase identifiers for Snowflake (default case folding)
            query = f'SELECT * FROM {database.upper()}.{schema.upper()}.{table.upper()}'
            cursor.execute(query)
            
            # Fetch to pandas DataFrame
            import pandas as pd
            df = cursor.fetch_pandas_all()
            
            row_count = len(df)
            
            # Write to Parquet
            df.to_parquet(parquet_path, engine='pyarrow', compression='snappy')
            
            size_bytes = os.path.getsize(parquet_path)
            
            return row_count, size_bytes
            
        finally:
            cursor.close()
    
    def register_in_duckdb(
        self,
        database: str,
        schema: str,
        table: str,
        duckdb_conn: Optional[duckdb.DuckDBPyConnection] = None,
    ) -> bool:
        """
        Register a cached table as a view in DuckDB.
        
        Creates: CREATE VIEW {schema}.{table} AS SELECT * FROM read_parquet(...)
        
        Returns:
            True if registered successfully
        """
        conn = duckdb_conn or self.duckdb_conn
        if conn is None:
            return False
        
        parquet_path = self.get_cached_path(database, schema, table)
        if parquet_path is None:
            return False
        
        try:
            # Create schema if needed
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            
            # Create view pointing to Parquet file
            conn.execute(f"""
                CREATE OR REPLACE VIEW {schema}.{table} AS 
                SELECT * FROM read_parquet('{parquet_path}')
            """)
            
            return True
            
        except Exception as e:
            print(f"⚠️ Failed to register {schema}.{table}: {e}")
            return False
    
    def ensure_cached(
        self,
        database: str,
        schema: str,
        table: str,
        duckdb_conn: Optional[duckdb.DuckDBPyConnection] = None,
    ) -> bool:
        """
        Ensure a table is cached and registered in DuckDB.
        
        This is the main entry point - call this before executing SQL that
        references source tables.
        
        Returns:
            True if table is ready for local queries
        """
        if not self.config.cache_enabled:
            return False
        
        # Cache if needed
        if not self.is_cached(database, schema, table):
            try:
                self.cache_table(database, schema, table)
            except Exception as e:
                print(f"⚠️ Failed to cache {database}.{schema}.{table}: {e}")
                return False
        
        # Register in DuckDB
        return self.register_in_duckdb(database, schema, table, duckdb_conn)
    
    # =========================================================================
    # Cache Management
    # =========================================================================
    
    def refresh_all(self, force: bool = False):
        """Refresh all cached tables."""
        for table_id in list(self._manifest.keys()):
            entry = self._manifest[table_id]
            parts = table_id.split(".")
            if len(parts) == 3:
                database, schema, table = parts
                try:
                    self.cache_table(database, schema, table, force=force)
                except Exception as e:
                    print(f"⚠️ Failed to refresh {table_id}: {e}")
    
    def clear(self):
        """Clear all cached data."""
        for entry in self._manifest.values():
            try:
                if os.path.exists(entry.parquet_path):
                    os.remove(entry.parquet_path)
            except Exception:
                pass
        
        self._manifest = {}
        self._save_manifest()
        print("🗑️  Cache cleared")
    
    def get_status(self) -> Dict[str, Any]:
        """Get cache status summary."""
        total_size = sum(e.size_bytes for e in self._manifest.values())
        stale_count = sum(
            1 for e in self._manifest.values() 
            if e.is_stale(self.config.cache_ttl_hours)
        )
        
        return {
            "cache_dir": self.config.cache_dir,
            "table_count": len(self._manifest),
            "total_size_gb": total_size / (1024**3),
            "max_size_gb": self.config.cache_max_gb,
            "stale_count": stale_count,
            "ttl_hours": self.config.cache_ttl_hours,
            "entries": [
                {
                    "table_id": e.table_id,
                    "size_gb": e.size_gb,
                    "age_hours": round(e.age_hours, 1),
                    "stale": e.is_stale(self.config.cache_ttl_hours),
                }
                for e in self._manifest.values()
            ],
        }
    
    def prune(self) -> int:
        """Remove stale entries to stay under max size. Returns count removed."""
        removed = 0
        
        # Remove stale entries first
        for table_id in list(self._manifest.keys()):
            entry = self._manifest[table_id]
            if entry.is_stale(self.config.cache_ttl_hours):
                try:
                    if os.path.exists(entry.parquet_path):
                        os.remove(entry.parquet_path)
                    del self._manifest[table_id]
                    removed += 1
                except Exception:
                    pass
        
        # Then remove oldest if over max size
        total_gb = sum(e.size_bytes for e in self._manifest.values()) / (1024**3)
        
        while total_gb > self.config.cache_max_gb and self._manifest:
            # Find oldest entry
            oldest = min(
                self._manifest.values(),
                key=lambda e: datetime.fromisoformat(e.created_at)
            )
            
            try:
                if os.path.exists(oldest.parquet_path):
                    os.remove(oldest.parquet_path)
                del self._manifest[oldest.table_id]
                removed += 1
                total_gb -= oldest.size_gb
            except Exception:
                break
        
        self._save_manifest()
        return removed


# =============================================================================
# Singleton & Convenience
# =============================================================================

_cache: Optional[SourceCache] = None


def get_source_cache(
    config: Optional[CacheConfig] = None,
    snowflake_conn: Optional[Any] = None,
) -> SourceCache:
    """Get or create the source cache singleton."""
    global _cache
    
    if _cache is None:
        _cache = SourceCache(config=config, snowflake_conn=snowflake_conn)
    elif snowflake_conn is not None:
        _cache.snowflake_conn = snowflake_conn
    
    return _cache


def format_cache_status(status: Dict[str, Any]) -> str:
    """Format cache status as a nice report."""
    lines = [
        "📦 Icebreaker Source Cache",
        "═" * 50,
        f"  Location: {status['cache_dir']}",
        f"  Tables:   {status['table_count']} cached ({status['stale_count']} stale)",
        f"  Size:     {status['total_size_gb']:.2f}GB / {status['max_size_gb']:.1f}GB max",
        f"  TTL:      {status['ttl_hours']}h",
    ]
    
    if status['entries']:
        lines.append("")
        lines.append("  Cached Tables:")
        for entry in status['entries']:
            stale_marker = "⚠️ " if entry['stale'] else "✅"
            lines.append(
                f"    {stale_marker} {entry['table_id']}: "
                f"{entry['size_gb']:.2f}GB, {entry['age_hours']}h old"
            )
    else:
        lines.append("")
        lines.append("  No tables cached yet.")
        lines.append("  Tables will be cached on first `dbt run`.")
    
    lines.append("═" * 50)
    return "\n".join(lines)
