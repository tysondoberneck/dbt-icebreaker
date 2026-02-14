"""
Warehouse Sync Module for Icebreaker.

After compute executes on the optimal engine (local DuckDB), sync results to 
Snowflake so dashboards/tools work regardless of connection.

MVP Focus: Snowflake only. Other warehouses can be added later.
"""

import os
import tempfile
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

from dbt.adapters.icebreaker.console import console


@dataclass
class SyncResult:
    """Result of a sync operation."""
    warehouse: str
    table_name: str
    success: bool
    row_count: int = 0
    error: str = None


def sync_to_snowflake(
    source_conn: Any,  # DuckDB or any connection with Arrow export
    snowflake_conn: Any,
    source_schema: str,
    source_table: str,
    target_schema: str = None,
    target_table: str = None,
    target_database: str = None,
) -> SyncResult:
    """
    Sync a table to Snowflake.
    
    Uses Parquet as transfer format for efficiency.
    """
    target_schema = target_schema or source_schema
    target_table = target_table or source_table
    full_target = f"{target_schema}.{target_table}"
    
    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_path = os.path.join(tmpdir, f"{source_table}.parquet")
        
        try:
            # Export to Parquet
            full_source = f"{source_schema}.{source_table}"
            source_conn.execute(f"COPY (SELECT * FROM {full_source}) TO '{parquet_path}' (FORMAT PARQUET)")
            
            file_size_mb = os.path.getsize(parquet_path) / (1024 * 1024)
            console.step(f"Exported {full_source} ({file_size_mb:.1f} MB)")
            
            # Upload to Snowflake
            cursor = snowflake_conn.cursor()
            
            if target_database:
                cursor.execute(f"USE DATABASE {target_database}")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
            
            # Upload to table stage
            stage_name = f"@{target_schema}.%{target_table}"
            cursor.execute(f"PUT 'file://{parquet_path}' {stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
            
            # Infer schema and create table
            cursor.execute(f"""
                CREATE OR REPLACE TABLE {full_target}
                USING TEMPLATE (
                    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                    FROM TABLE(INFER_SCHEMA(
                        LOCATION => '{stage_name}',
                        FILE_FORMAT => 'icebreaker_parquet_format'
                    ))
                )
            """)
            
            # COPY INTO
            cursor.execute(f"""
                COPY INTO {full_target}
                FROM {stage_name}
                FILE_FORMAT = (TYPE = PARQUET)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PURGE = TRUE
            """)
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {full_target}")
            row_count = cursor.fetchone()[0]
            
            cursor.close()
            console.success(f"Snowflake: {full_target} ({row_count:,} rows)")
            
            return SyncResult(
                warehouse="snowflake",
                table_name=full_target,
                success=True,
                row_count=row_count
            )
            
        except Exception as e:
            console.error(f"Snowflake sync failed: {e}")
            return SyncResult(
                warehouse="snowflake",
                table_name=full_target,
                success=False,
                error=str(e)
            )


def sync_to_all_warehouses(
    source_conn: Any,
    connections: Dict[str, Any],
    source_schema: str,
    source_table: str,
) -> List[SyncResult]:
    """
    Sync a table to all connected warehouses.
    
    MVP: Only Snowflake is supported currently.
    
    Args:
        source_conn: DuckDB connection with the source data
        connections: Dict of {warehouse_name: connection}
        source_schema: Source schema name
        source_table: Source table name
    
    Returns:
        List of SyncResult for each warehouse
    """
    results = []
    
    # MVP: Snowflake only
    if "snowflake" in connections and connections["snowflake"]:
        results.append(sync_to_snowflake(
            source_conn, connections["snowflake"], source_schema, source_table
        ))
    
    # Summary
    success_count = sum(1 for r in results if r.success)
    total_count = len(results)
    if total_count > 0:
        console.info(f"Synced to {success_count}/{total_count} warehouses")
    
    return results

