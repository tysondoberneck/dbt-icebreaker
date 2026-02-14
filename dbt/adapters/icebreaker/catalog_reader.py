"""
Catalog Metadata Reader for Icebreaker.

Reads metadata from user's existing warehouses (Snowflake, BigQuery, etc.)
to enable intelligent query routing decisions.

IMPORTANT: This module ONLY READS. It never creates, modifies, or deletes
any data in the user's warehouse.
"""

from typing import Any, Dict, List, Optional
from dataclasses import dataclass

from dbt.adapters.icebreaker.console import console


@dataclass
class TableMetadata:
    """Metadata about a table for routing decisions."""
    catalog: str          # e.g., "snowflake", "bigquery"
    database: str
    schema: str
    table_name: str
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    column_count: Optional[int] = None
    last_modified: Optional[str] = None
    partitioned: bool = False
    partition_columns: List[str] = None
    
    @property
    def full_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.table_name}"
    
    @property
    def size_gb(self) -> float:
        if self.size_bytes:
            return self.size_bytes / (1024 ** 3)
        return 0.0


def read_snowflake_catalog(
    snowflake_conn: Any,
    database: str = None,
    schema: str = None,
) -> List[TableMetadata]:
    """
    Read table metadata from Snowflake's INFORMATION_SCHEMA.
    
    Args:
        snowflake_conn: Snowflake connection
        database: Filter to specific database (optional)
        schema: Filter to specific schema (optional)
    
    Returns:
        List of TableMetadata objects
    """
    cursor = snowflake_conn.cursor()
    
    # Build query to read table metadata
    # Uses INFORMATION_SCHEMA which is read-only
    query = """
        SELECT 
            t.table_catalog,
            t.table_schema,
            t.table_name,
            t.row_count,
            t.bytes,
            (SELECT COUNT(*) FROM information_schema.columns c 
             WHERE c.table_catalog = t.table_catalog 
               AND c.table_schema = t.table_schema 
               AND c.table_name = t.table_name) as column_count,
            t.last_altered
        FROM information_schema.tables t
        WHERE t.table_type = 'BASE TABLE'
    """
    
    if database:
        query += f" AND t.table_catalog = '{database}'"
    if schema:
        query += f" AND t.table_schema = '{schema}'"
    
    query += " ORDER BY t.table_catalog, t.table_schema, t.table_name"
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        
        metadata = []
        for row in results:
            metadata.append(TableMetadata(
                catalog="snowflake",
                database=row[0],
                schema=row[1],
                table_name=row[2],
                row_count=row[3],
                size_bytes=row[4],
                column_count=row[5],
                last_modified=str(row[6]) if row[6] else None,
            ))
        
        return metadata
        
    finally:
        cursor.close()


def read_snowflake_table_columns(
    snowflake_conn: Any,
    database: str,
    schema: str,
    table_name: str,
) -> List[Dict[str, str]]:
    """
    Read column metadata for a specific table.
    
    Returns:
        List of dicts with column_name, data_type, is_nullable
    """
    cursor = snowflake_conn.cursor()
    
    query = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns
        WHERE table_catalog = '{database}'
          AND table_schema = '{schema}'
          AND table_name = '{table_name}'
        ORDER BY ordinal_position
    """
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        
        return [
            {
                "column_name": row[0],
                "data_type": row[1],
                "is_nullable": row[2] == "YES"
            }
            for row in results
        ]
        
    finally:
        cursor.close()


def estimate_query_cost(
    table_metadata: List[TableMetadata],
    cost_per_tb: float = 5.0,  # Default Snowflake on-demand pricing
) -> Dict[str, float]:
    """
    Estimate the cost of scanning tables based on metadata.
    
    Args:
        table_metadata: List of tables that would be scanned
        cost_per_tb: Cost per TB scanned (default $5 for Snowflake)
    
    Returns:
        Dict with total_gb, estimated_cost, recommendation
    """
    total_bytes = sum(t.size_bytes or 0 for t in table_metadata)
    total_gb = total_bytes / (1024 ** 3)
    total_tb = total_gb / 1024
    estimated_cost = total_tb * cost_per_tb
    
    # Recommendation based on size
    if total_gb < 1:
        recommendation = "duckdb"  # Small enough for local
        reason = f"Under 1GB ({total_gb:.2f}GB) - run locally for free"
    elif total_gb < 10:
        recommendation = "duckdb"  # Still reasonable for local
        reason = f"{total_gb:.2f}GB - likely faster locally"
    else:
        recommendation = "cloud"  # Need cloud compute
        reason = f"{total_gb:.2f}GB - cloud compute recommended"
    
    return {
        "total_gb": total_gb,
        "estimated_cost_usd": estimated_cost,
        "recommendation": recommendation,
        "reason": reason,
    }


# =============================================================================
# Query History Integration (Predictive Routing)
# =============================================================================

@dataclass
class QueryStats:
    """Historical query execution statistics."""
    table_name: str
    avg_bytes_scanned: int
    avg_execution_ms: int
    avg_credits_used: float
    query_count: int
    last_queried: Optional[str] = None
    
    @property
    def avg_cost_usd(self) -> float:
        """Estimate USD cost (Snowflake: ~$2-4 per credit)."""
        return self.avg_credits_used * 3.0  # Mid-range estimate


def read_snowflake_query_history(
    snowflake_conn: Any,
    hours: int = 168,  # 7 days default
    database: str = None,
) -> Dict[str, QueryStats]:
    """
    Read query execution history from Snowflake ACCOUNT_USAGE.
    
    Uses QUERY_HISTORY to understand historical cost patterns
    for smarter routing decisions.
    
    Args:
        snowflake_conn: Snowflake connection
        hours: How many hours of history to read (default 7 days)
        database: Filter to specific database (optional)
    
    Returns:
        Dict mapping table_name -> QueryStats
    
    Note: Requires ACCOUNTADMIN or appropriate grants on ACCOUNT_USAGE.
    """
    cursor = snowflake_conn.cursor()
    
    # Parse table references from query text and aggregate stats
    query = f"""
        WITH parsed_queries AS (
            SELECT 
                query_text,
                bytes_scanned,
                total_elapsed_time,
                credits_used_cloud_services,
                start_time,
                -- Extract table names from common patterns
                REGEXP_SUBSTR(UPPER(query_text), 'FROM\\\\s+([A-Z0-9_\\\\.]+)', 1, 1, 'e') as table_ref
            FROM snowflake.account_usage.query_history
            WHERE start_time > DATEADD(hours, -{hours}, CURRENT_TIMESTAMP())
              AND query_type IN ('SELECT', 'INSERT', 'CREATE_TABLE_AS_SELECT')
              AND bytes_scanned > 0
              {"AND database_name = '" + database + "'" if database else ""}
        )
        SELECT 
            table_ref,
            AVG(bytes_scanned)::INTEGER as avg_bytes,
            AVG(total_elapsed_time)::INTEGER as avg_ms,
            AVG(COALESCE(credits_used_cloud_services, 0)) as avg_credits,
            COUNT(*) as query_count,
            MAX(start_time)::VARCHAR as last_queried
        FROM parsed_queries
        WHERE table_ref IS NOT NULL
        GROUP BY table_ref
        HAVING COUNT(*) >= 2  -- Only tables queried multiple times
        ORDER BY avg_bytes DESC
    """
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        
        stats = {}
        for row in results:
            table_name = row[0]
            stats[table_name] = QueryStats(
                table_name=table_name,
                avg_bytes_scanned=row[1] or 0,
                avg_execution_ms=row[2] or 0,
                avg_credits_used=row[3] or 0.0,
                query_count=row[4] or 0,
                last_queried=row[5],
            )
        
        return stats
        
    except Exception as e:
        # ACCOUNT_USAGE requires specific privileges
        console.warn(f"Query history access failed: {e}")
        console.info("Requires ACCOUNTADMIN or SNOWFLAKE.ACCOUNT_USAGE grants")
        return {}
        
    finally:
        cursor.close()


def get_table_historical_cost(
    query_stats: Dict[str, QueryStats],
    table_name: str,
) -> Optional[float]:
    """
    Get historical average cost for querying a table.
    
    Args:
        query_stats: Dict from read_snowflake_query_history
        table_name: Table name to look up (case-insensitive)
    
    Returns:
        Average cost in USD, or None if no history
    """
    # Normalize table name for lookup
    table_upper = table_name.upper()
    
    # Try exact match first
    if table_upper in query_stats:
        return query_stats[table_upper].avg_cost_usd
    
    # Try partial match (just table name without schema)
    for key, stats in query_stats.items():
        if key.endswith(f".{table_upper}") or key == table_upper:
            return stats.avg_cost_usd
    
    return None


# Future: Add BigQuery, Databricks, Redshift metadata readers
def read_bigquery_catalog(bq_client: Any, project: str = None) -> List[TableMetadata]:
    """Read table metadata from BigQuery. (Placeholder)"""
    raise NotImplementedError("BigQuery catalog reader coming soon")


def read_databricks_catalog(spark: Any, catalog: str = None) -> List[TableMetadata]:
    """Read table metadata from Databricks Unity Catalog. (Placeholder)"""
    raise NotImplementedError("Databricks catalog reader coming soon")
