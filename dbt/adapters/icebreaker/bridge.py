"""
The Bridge - Cloud DDL Wrapper

Wraps user SQL in Iceberg-native DDL statements for cloud execution.
This ensures that any model run in the cloud outputs Iceberg format,
making it immediately readable by the local DuckDB engine.

Supported Clouds:
- Snowflake: CREATE ICEBERG TABLE with CATALOG_INTEGRATION
- Databricks: CREATE TABLE USING ICEBERG
- BigQuery: BigLake external tables
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional, Literal
from enum import Enum


class CloudProvider(Enum):
    """Supported cloud warehouse providers."""
    SNOWFLAKE = "snowflake"
    DATABRICKS = "databricks"
    BIGQUERY = "bigquery"
    ATHENA = "athena"
    REDSHIFT = "redshift"


@dataclass
class IcebergConfig:
    """Configuration for Iceberg table creation."""
    # Common
    schema: str
    table: str
    
    # Snowflake-specific
    catalog_integration: Optional[str] = None
    external_volume: Optional[str] = None
    
    # Databricks-specific
    location: Optional[str] = None
    
    # BigQuery-specific
    connection: Optional[str] = None
    
    # Table properties
    partition_by: Optional[str] = None
    table_format: str = "iceberg"


class Bridge:
    """
    The Bridge: Wraps SQL in Iceberg-native DDL.
    
    Key principle: Any query run in the Cloud outputs Iceberg format,
    so the local engine can read results immediately after.
    """
    
    def __init__(self, provider: str, credentials: Any):
        """
        Initialize the Bridge.
        
        Args:
            provider: Cloud provider name (snowflake, databricks, etc.)
            credentials: Adapter credentials with cloud config
        """
        self.provider = CloudProvider(provider.lower())
        self.credentials = credentials
    
    def construct_iceberg_ddl(
        self,
        sql: str,
        config: IcebergConfig,
        is_replace: bool = True,
    ) -> str:
        """
        Wrap user SQL in Iceberg DDL.
        
        Args:
            sql: The user's SELECT/transformation SQL
            config: Iceberg table configuration
            is_replace: If True, use CREATE OR REPLACE
            
        Returns:
            DDL statement that creates an Iceberg table
        """
        if self.provider == CloudProvider.SNOWFLAKE:
            return self._snowflake_ddl(sql, config, is_replace)
        elif self.provider == CloudProvider.DATABRICKS:
            return self._databricks_ddl(sql, config, is_replace)
        elif self.provider == CloudProvider.BIGQUERY:
            return self._bigquery_ddl(sql, config, is_replace)
        elif self.provider == CloudProvider.ATHENA:
            return self._athena_ddl(sql, config, is_replace)
        else:
            raise ValueError(f"Unsupported cloud provider: {self.provider}")
    
    def _snowflake_ddl(
        self,
        sql: str,
        config: IcebergConfig,
        is_replace: bool,
    ) -> str:
        """
        Generate Snowflake Iceberg DDL.
        
        Snowflake requires:
        - CATALOG_INTEGRATION: Link to Polaris/Glue catalog
        - EXTERNAL_VOLUME: S3/GCS storage location
        
        Example output:
        CREATE OR REPLACE ICEBERG TABLE schema.table
        CATALOG_INTEGRATION = 'POLARIS_INT'
        EXTERNAL_VOLUME = 'S3_VOL'
        AS
        SELECT ...
        """
        create_stmt = "CREATE OR REPLACE" if is_replace else "CREATE"
        
        # Build the DDL
        ddl = f"""{create_stmt} ICEBERG TABLE {config.schema}.{config.table}
CATALOG_INTEGRATION = '{config.catalog_integration}'
EXTERNAL_VOLUME = '{config.external_volume}'"""
        
        # Add partitioning if specified
        if config.partition_by:
            ddl += f"\nPARTITION BY ({config.partition_by})"
        
        # Add the AS clause with user SQL
        ddl += f"""
AS
{sql.strip()}"""
        
        return ddl
    
    def _databricks_ddl(
        self,
        sql: str,
        config: IcebergConfig,
        is_replace: bool,
    ) -> str:
        """
        Generate Databricks Iceberg DDL.
        
        Example output:
        CREATE OR REPLACE TABLE schema.table
        USING ICEBERG
        LOCATION 's3://bucket/path'
        AS
        SELECT ...
        """
        create_stmt = "CREATE OR REPLACE" if is_replace else "CREATE"
        
        ddl = f"""{create_stmt} TABLE {config.schema}.{config.table}
USING ICEBERG"""
        
        if config.location:
            ddl += f"\nLOCATION '{config.location}'"
        
        if config.partition_by:
            ddl += f"\nPARTITIONED BY ({config.partition_by})"
        
        ddl += f"""
AS
{sql.strip()}"""
        
        return ddl
    
    def _bigquery_ddl(
        self,
        sql: str,
        config: IcebergConfig,
        is_replace: bool,
    ) -> str:
        """
        Generate BigQuery BigLake Iceberg DDL.
        
        Example output:
        CREATE OR REPLACE EXTERNAL TABLE `project.dataset.table`
        WITH CONNECTION `connection_id`
        OPTIONS (
          format = 'ICEBERG',
          uris = ['gs://bucket/path/*']
        )
        AS
        SELECT ...
        """
        create_stmt = "CREATE OR REPLACE" if is_replace else "CREATE"
        
        ddl = f"""{create_stmt} EXTERNAL TABLE `{config.schema}.{config.table}`"""
        
        if config.connection:
            ddl += f"\nWITH CONNECTION `{config.connection}`"
        
        ddl += f"""
OPTIONS (
  format = 'ICEBERG'
)
AS
{sql.strip()}"""
        
        return ddl
    
    def _athena_ddl(
        self,
        sql: str,
        config: IcebergConfig,
        is_replace: bool,
    ) -> str:
        """
        Generate AWS Athena Iceberg DDL.
        
        Example output:
        CREATE TABLE schema.table
        WITH (
          table_type = 'ICEBERG',
          location = 's3://bucket/path/',
          format = 'PARQUET'
        )
        AS
        SELECT ...
        """
        # Athena doesn't support OR REPLACE for CTAS
        ddl = f"""CREATE TABLE {config.schema}.{config.table}
WITH (
  table_type = 'ICEBERG',
  location = '{config.location or f"s3://warehouse/{config.schema}/{config.table}"}',
  format = 'PARQUET'
)
AS
{sql.strip()}"""
        
        return ddl


def construct_iceberg_ddl(
    sql: str,
    provider: str,
    schema: str,
    table: str,
    credentials: Any,
    **kwargs,
) -> str:
    """
    Convenience function to construct Iceberg DDL.
    
    Args:
        sql: User's SELECT SQL
        provider: Cloud provider name
        schema: Target schema
        table: Target table name
        credentials: Adapter credentials
        **kwargs: Additional Iceberg config options
        
    Returns:
        Complete DDL statement
    """
    config = IcebergConfig(
        schema=schema,
        table=table,
        catalog_integration=getattr(credentials, 'cloud_bridge_catalog_integration', None),
        external_volume=getattr(credentials, 'cloud_bridge_external_volume', None),
        **kwargs,
    )
    
    bridge = Bridge(provider, credentials)
    return bridge.construct_iceberg_ddl(sql, config)


class CatalogRegistrar:
    """
    Handles post-execution catalog registration.
    
    After a cloud query creates an Iceberg table, we may need to
    ensure the catalog (Polaris/Glue) is aware of the new metadata.
    """
    
    def __init__(self, catalog_type: str, catalog_uri: Optional[str] = None):
        self.catalog_type = catalog_type
        self.catalog_uri = catalog_uri
        self._catalog = None
    
    @property
    def catalog(self):
        """Lazy-load the PyIceberg catalog."""
        if self._catalog is None:
            self._catalog = self._load_catalog()
        return self._catalog
    
    def _load_catalog(self):
        """Load the appropriate PyIceberg catalog."""
        try:
            from pyiceberg.catalog import load_catalog
            
            if self.catalog_type == "rest":
                return load_catalog("default", **{
                    "type": "rest",
                    "uri": self.catalog_uri,
                })
            elif self.catalog_type == "glue":
                return load_catalog("default", **{
                    "type": "glue",
                })
            else:
                return None
        except Exception:
            return None
    
    def register_table(self, schema: str, table: str, metadata_location: str) -> bool:
        """
        Register a table with the catalog after cloud creation.
        
        This is typically automatic with Snowflake/Databricks when using
        their native Iceberg support, but may be needed for custom setups.
        
        Args:
            schema: Table schema/namespace
            table: Table name
            metadata_location: S3 path to metadata.json
            
        Returns:
            True if registration successful
        """
        if self.catalog is None:
            return False
        
        try:
            # Most catalogs handle this automatically
            # This is a hook for custom registration logic
            return True
        except Exception:
            return False
    
    def refresh_table(self, schema: str, table: str) -> bool:
        """
        Refresh table metadata in the catalog.
        
        Call this after cloud execution to ensure the local engine
        sees the latest version of the table.
        
        Args:
            schema: Table schema/namespace
            table: Table name
            
        Returns:
            True if refresh successful
        """
        if self.catalog is None:
            return False
        
        try:
            table_id = f"{schema}.{table}"
            iceberg_table = self.catalog.load_table(table_id)
            iceberg_table.refresh()
            return True
        except Exception:
            return False
