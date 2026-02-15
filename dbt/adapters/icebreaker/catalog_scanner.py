"""
Catalog Scanner for Icebreaker.

Scans cloud warehouse catalogs to estimate data volumes and
inform routing decisions. MVP: Snowflake and DuckDB only.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta

from dbt.adapters.icebreaker.console import console


@dataclass
class TableStats:
    """Statistics for a single table."""
    schema: str
    table: str
    row_count: int
    size_bytes: int
    last_modified: Optional[datetime] = None
    
    @property
    def size_gb(self) -> float:
        return self.size_bytes / (1024 ** 3)


class CatalogScanner:
    """
    Scans catalog metadata for routing decisions.
    
    Provides volume estimation for routing Gate 6 (Physics).
    Caches results to avoid repeated cloud queries.
    """
    
    def __init__(
        self,
        cloud_conn: Optional[Any] = None,
        local_conn: Optional[Any] = None,
        cloud_type: str = "snowflake",
        cache_ttl_minutes: int = 60,
    ):
        self.cloud = cloud_conn
        self.local = local_conn
        self.cloud_type = cloud_type
        self.cache_ttl = timedelta(minutes=cache_ttl_minutes)
        
        self._cache: Dict[str, Tuple[TableStats, datetime]] = {}
    
    def estimate_input_volume(self, model: Dict[str, Any]) -> float:
        """
        Estimate total input data volume in GB for a model.
        
        Sums the sizes of all upstream dependencies.
        
        Args:
            model: dbt model node
            
        Returns:
            Estimated input volume in GB
        """
        total_bytes = 0
        
        depends_on = model.get("depends_on", {})
        nodes = depends_on.get("nodes", [])
        
        for node_id in nodes:
            stats = self.get_table_stats(node_id)
            if stats:
                total_bytes += stats.size_bytes
        
        return total_bytes / (1024 ** 3)
    
    def get_table_stats(self, node_id: str) -> Optional[TableStats]:
        """
        Get stats for a table by node ID.
        
        Args:
            node_id: dbt node ID (e.g., "model.project.table_name")
            
        Returns:
            TableStats or None if not found
        """
        # Check cache first
        if node_id in self._cache:
            stats, cached_at = self._cache[node_id]
            if datetime.now() - cached_at < self.cache_ttl:
                return stats
        
        # Parse node ID
        parts = node_id.split(".")
        if len(parts) < 2:
            return None
        
        table_name = parts[-1]
        schema_name = parts[-2] if len(parts) > 2 else "main"
        
        # Query cloud catalog
        stats = self._query_catalog(schema_name, table_name)
        
        if stats:
            self._cache[node_id] = (stats, datetime.now())
        
        return stats
    
    def _query_catalog(self, schema: str, table: str) -> Optional[TableStats]:
        """Query the cloud catalog for table stats."""
        
        if not self.cloud:
            return None
        
        try:
            if self.cloud_type == "snowflake":
                return self._query_snowflake(schema, table)
            elif self.cloud_type == "bigquery":
                return self._query_bigquery(schema, table)
            elif self.cloud_type in ("motherduck", "duckdb"):
                return self._query_duckdb(schema, table)
            else:
                return None
                
        except Exception as e:
            console.warn(f"Catalog query failed for {schema}.{table}: {e}")
            return None
    
    def _query_snowflake(self, schema: str, table: str) -> Optional[TableStats]:
        """Query Snowflake INFORMATION_SCHEMA."""
        
        query = f"""
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                ROW_COUNT,
                BYTES,
                LAST_ALTERED
            FROM INFORMATION_SCHEMA.TABLES
            WHERE UPPER(TABLE_NAME) = UPPER('{table}')
              AND UPPER(TABLE_SCHEMA) = UPPER('{schema}')
            LIMIT 1
        """
        
        cursor = self.cloud.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        cursor.close()
        
        if not row:
            return None
        
        return TableStats(
            schema=row[0],
            table=row[1],
            row_count=row[2] or 0,
            size_bytes=row[3] or 0,
            last_modified=row[4],
        )
    
    def _query_bigquery(self, schema: str, table: str) -> Optional[TableStats]:
        """Query BigQuery __TABLES__ metadata."""
        
        # BigQuery uses dataset instead of schema
        dataset = schema
        
        query = f"""
            SELECT 
                dataset_id,
                table_id,
                row_count,
                size_bytes,
                TIMESTAMP_MILLIS(last_modified_time) as last_modified
            FROM `{dataset}.__TABLES__`
            WHERE table_id = '{table}'
            LIMIT 1
        """
        
        result = self.cloud.query(query)
        rows = list(result)
        
        if not rows:
            return None
        
        row = rows[0]
        return TableStats(
            schema=row.dataset_id,
            table=row.table_id,
            row_count=row.row_count or 0,
            size_bytes=row.size_bytes or 0,
            last_modified=row.last_modified,
        )
    
    def _query_duckdb(self, schema: str, table: str) -> Optional[TableStats]:
        """Query DuckDB/MotherDuck metadata."""
        
        # DuckDB doesn't have size_bytes in metadata, estimate from row count
        query = f"""
            SELECT 
                schema_name,
                table_name,
                estimated_size
            FROM duckdb_tables()
            WHERE table_name = '{table}'
              AND schema_name = '{schema}'
            LIMIT 1
        """
        
        try:
            result = self.cloud.execute(query).fetchone()
            
            if not result:
                # Try to get row count directly
                count_result = self.cloud.execute(
                    f"SELECT COUNT(*) FROM {schema}.{table}"
                ).fetchone()
                
                if count_result:
                    # Estimate ~100 bytes per row (rough heuristic)
                    return TableStats(
                        schema=schema,
                        table=table,
                        row_count=count_result[0],
                        size_bytes=count_result[0] * 100,
                    )
                return None
            
            return TableStats(
                schema=result[0],
                table=result[1],
                row_count=0,  # Not available
                size_bytes=result[2] or 0,
            )
            
        except Exception:
            return None
    
    def refresh_cache(self, node_ids: Optional[List[str]] = None):
        """
        Refresh cached stats.
        
        Args:
            node_ids: Specific nodes to refresh, or None for all
        """
        if node_ids is None:
            node_ids = list(self._cache.keys())
        
        for node_id in node_ids:
            # Remove from cache to force re-query
            self._cache.pop(node_id, None)
            # Re-query
            self.get_table_stats(node_id)
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get statistics about the cache."""
        total_size = sum(
            stats.size_bytes 
            for stats, _ in self._cache.values()
        )
        
        return {
            "cached_tables": len(self._cache),
            "total_cached_bytes": total_size,
            "total_cached_gb": total_size / (1024 ** 3),
        }


# =============================================================================
# Singleton
# =============================================================================

_scanner: Optional[CatalogScanner] = None


def get_catalog_scanner(
    cloud_conn: Optional[Any] = None,
    cloud_type: str = "snowflake",
) -> CatalogScanner:
    """Get or create the catalog scanner singleton."""
    global _scanner
    if _scanner is None:
        _scanner = CatalogScanner(
            cloud_conn=cloud_conn,
            cloud_type=cloud_type,
        )
    return _scanner
