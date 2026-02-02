"""
Catalog Metadata Reader for Icebreaker.

Reads metadata from user's existing warehouses (Snowflake, BigQuery, etc.)
to enable intelligent query routing decisions.

IMPORTANT: This module ONLY READS. It never creates, modifies, or deletes
any data in the user's warehouse.
"""

from typing import Any, Dict, List, Optional
from dataclasses import dataclass


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


# Future: Add BigQuery, Databricks, Redshift metadata readers
def read_bigquery_catalog(bq_client: Any, project: str = None) -> List[TableMetadata]:
    """Read table metadata from BigQuery. (Placeholder)"""
    raise NotImplementedError("BigQuery catalog reader coming soon")


def read_databricks_catalog(spark: Any, catalog: str = None) -> List[TableMetadata]:
    """Read table metadata from Databricks Unity Catalog. (Placeholder)"""
    raise NotImplementedError("Databricks catalog reader coming soon")
