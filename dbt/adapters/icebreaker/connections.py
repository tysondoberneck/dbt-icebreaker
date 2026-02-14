"""
Icebreaker Connection Manager

Manages dual connections to local DuckDB and optional cloud warehouse.
Implements lazy initialization and dynamic engine switching.
"""

from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Literal, Optional
import os

# Auto-load .env file if it exists (no need to manually source .env)
def _load_env_file():
    """Load environment variables from .env file automatically."""
    # Check common locations for .env
    for env_path in ['.env', '../.env', os.path.join(os.getcwd(), '.env')]:
        if os.path.exists(env_path):
            try:
                with open(env_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#') and '=' in line:
                            # Handle "export VAR=value" and "VAR=value"
                            if line.startswith('export '):
                                line = line[7:]
                            key, value = line.split('=', 1)
                            os.environ.setdefault(key.strip(), value.strip())
                pass  # env loaded (logged after console init)
                return True
            except Exception:
                pass
    return False

_load_env_file()

from dbt.adapters.icebreaker.console import console

import logging
# Suppress noisy Snowflake connector retry warnings (transient network hiccups)
logging.getLogger("snowflake.connector.vendored.urllib3.connectionpool").setLevel(logging.ERROR)

import duckdb
from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    ConnectionState,
    Credentials,
)
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.exceptions import DbtRuntimeError

# Import for auto-caching
import re as regex_module


EngineType = Literal["duckdb", "cloud"]


@dataclass
class IcebreakerCredentials(Credentials):
    """Configuration for the Icebreaker adapter.
    
    Wraps existing cloud adapter credentials and adds routing configuration.
    Users keep their existing Snowflake/BigQuery credentials - we just add:
    - cloud_type: which cloud adapter to wrap
    - routing thresholds for intelligent query routing
    """
    
    # Cloud adapter to wrap (snowflake, bigquery, redshift, databricks)
    cloud_type: Optional[str] = None
    
    # Local engine settings
    engine: str = "duckdb"
    threads: int = 4
    max_local_size_gb: float = 5.0
    max_local_seconds: int = 600
    
    # Transpilation (auto-detected from cloud_type if not set)
    source_dialect: Optional[str] = None
    
    # === Snowflake credentials (passthrough) ===
    account: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    private_key_path: Optional[str] = None
    private_key_passphrase: Optional[str] = None
    authenticator: Optional[str] = None
    warehouse: Optional[str] = None
    role: Optional[str] = None
    
    # === BigQuery credentials (passthrough) ===
    project: Optional[str] = None
    dataset: Optional[str] = None
    keyfile: Optional[str] = None
    location: Optional[str] = None
    
    # === Common cloud settings ===
    database: str = "memory"
    schema: str = "main"
    
    # MotherDuck cloud settings (alternative to Snowflake)
    motherduck_token: Optional[str] = None
    motherduck_database: Optional[str] = None
    
    # === Iceberg REST Catalog Configuration ===
    # Connect to existing Iceberg catalogs (Polaris, Glue, Nessie, etc.)
    iceberg_catalog_url: Optional[str] = None  # e.g., "https://polaris.snowflakecomputing.com"
    iceberg_catalog_type: Optional[str] = None  # "rest", "glue", "nessie"
    iceberg_warehouse: Optional[str] = None  # Warehouse/namespace in catalog
    iceberg_credential: Optional[str] = None  # OAuth client_credentials or token
    iceberg_token: Optional[str] = None  # Bearer token for REST catalog
    iceberg_s3_region: Optional[str] = None  # AWS region for S3 access
    iceberg_s3_access_key: Optional[str] = None  # Optional: explicit S3 creds
    iceberg_s3_secret_key: Optional[str] = None  # Optional: explicit S3 creds
    
    # Cost estimation
    cloud_cost_per_tb_scanned: float = 5.0  # $/TB for cost estimates
    
    # Sync configuration
    # "model" = sync after each model (fresher, slightly more overhead)
    # "batch" = sync all at end of dbt run (more efficient, less fresh)
    sync_mode: str = "model"
    sync_enabled: bool = True  # Set to False to skip sync entirely
    
    # === Local Cache Configuration ===
    # For users without Iceberg - automatically cache sources from Snowflake
    cache_enabled: bool = True  # Enable automatic source caching
    cache_ttl_hours: float = 24.0  # Refresh cached tables after this many hours
    cache_max_gb: float = 10.0  # Maximum cache size in GB
    
    # Internal state
    _duckdb_extensions: list = field(default_factory=lambda: ["httpfs"])
    
    @property
    def effective_dialect(self) -> str:
        """Get the SQL dialect for transpilation."""
        if self.source_dialect:
            return self.source_dialect
        if self.cloud_type:
            return self.cloud_type
        return "duckdb"
    
    @property
    def cloud_enabled(self) -> bool:
        """Check if a cloud warehouse is configured."""
        if self.cloud_type == "snowflake":
            return bool(self.account)
        if self.cloud_type == "bigquery":
            return bool(self.project)
        if self.motherduck_token or os.environ.get("MOTHERDUCK_TOKEN"):
            return True
        return False
    
    @property
    def motherduck_enabled(self) -> bool:
        """Check if MotherDuck is configured."""
        token = self.motherduck_token or os.environ.get("MOTHERDUCK_TOKEN")
        return bool(token)
    
    @property
    def type(self) -> str:
        return "icebreaker"
    
    @property
    def unique_field(self) -> str:
        return f"icebreaker_{self.cloud_type or 'local'}"
    
    def _connection_keys(self) -> tuple:
        return (
            "cloud_type",
            "engine",
            "threads",
            "account",  # Snowflake
            "project",  # BigQuery
            "cloud_bridge_type",
        )


class IcebreakerConnectionManager(SQLConnectionManager):
    """
    Dual connection manager for Icebreaker.
    
    Manages:
    - DuckDB (local): Always available, in-memory with extensions
    - Cloud: Lazy-initialized only when needed
    """
    
    TYPE = "icebreaker"
    
    def __init__(self, profile: Any, mp_context: Any) -> None:
        super().__init__(profile, mp_context)
        self._duckdb_conn: Optional[duckdb.DuckDBPyConnection] = None
        self._cloud_conn: Optional[Any] = None
        self._active_engine: EngineType = "duckdb"
    
    @classmethod
    @contextmanager
    def exception_handler(cls, sql: str):
        """Context manager for handling exceptions during SQL execution."""
        try:
            yield
        except duckdb.Error as e:
            raise DbtRuntimeError(f"DuckDB error: {e}")
        except Exception as e:
            raise DbtRuntimeError(f"Icebreaker error: {e}")
    
    def begin(self):
        """DuckDB auto-commits, so we just track transaction state."""
        connection = self.get_thread_connection()
        # Ensure connection is open before beginning transaction
        if connection.state != ConnectionState.OPEN:
            connection = self.open(connection)
        connection.transaction_open = True
        return connection
    
    def add_begin_query(self):
        """No explicit BEGIN needed for DuckDB, but ensure connection is open."""
        connection = self.get_thread_connection()
        if connection.state != ConnectionState.OPEN:
            self.open(connection)
    
    def commit(self):
        """DuckDB auto-commits, just update transaction state."""
        connection = self.get_thread_connection()
        connection.transaction_open = False
        return connection
    
    def add_commit_query(self):
        """No explicit COMMIT needed for DuckDB."""
        pass
    
    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
        **kwargs,
    ):
        """Override to handle engine routing, auto-caching, transpilation, and skip incomplete DDL statements."""
        # Skip empty or incomplete SQL
        if not sql or not sql.strip():
            return self.get_thread_connection(), None
        
        sql = sql.strip()
        sql_upper = sql.upper()
        
        # === AUTO-CACHE SOURCE TABLES ===
        # If this looks like a SELECT from a source table, try to auto-cache it
        self._auto_cache_sources_from_sql(sql)
        
        # === TRANSPILE SNOWFLAKE → DUCKDB ===
        # Convert Snowflake-specific SQL to DuckDB-compatible SQL
        sql = self._transpile_snowflake_to_duckdb(sql)
        
        # Check for engine switching comment (from materialization)
        if '-- ICEBREAKER_ENGINE:' in sql:
            if '-- ICEBREAKER_ENGINE:cloud' in sql:
                # Initialize MotherDuck if needed
                credentials = self.profile.credentials
                if self._shared_cloud_handle is None:
                    self.get_motherduck_handle(credentials)
                if self._shared_cloud_handle is not None:
                    self._current_engine = "cloud"
                    console.step("Switched to CLOUD (MotherDuck)")
            elif '-- ICEBREAKER_ENGINE:local' in sql:
                self._current_engine = "local"
            # Execute the SELECT 1 but don't need to switch handles yet
            return self.get_thread_connection(), None
        
        # Check for savings log comment - log execution to savings database
        if '-- ICEBREAKER_LOG_SAVINGS:' in sql:
            import re
            match = re.search(r'-- ICEBREAKER_LOG_SAVINGS:([^:]+):([^:]+):([^:]+):([^:]+):([^\n]+)', sql)
            if match:
                model_name, engine, duration, savings_str, cloud_type = match.groups()
                try:
                    from dbt.adapters.icebreaker.savings import log_execution
                    log_execution(
                        model_name=model_name,
                        engine_used=engine,
                        execution_time_seconds=float(duration),
                        cloud_type=cloud_type,
                    )
                except Exception:
                    pass  # Don't fail if logging fails
            return self.get_thread_connection(), None
        
        # Check for sync comment - sync table to local_db if available
        if '-- ICEBREAKER_SYNC:' in sql:
            if self._sync_enabled and self._shared_cloud_handle is not None:
                # Extract table reference from comment
                import re
                match = re.search(r'-- ICEBREAKER_SYNC:(\S+)', sql)
                if match:
                    table_ref = match.group(1)
                    schema_name = table_ref.split('.')[0] if '.' in table_ref else 'main'
                    table_name = table_ref.split('.')[-1]
                    try:
                        self._shared_cloud_handle.execute(f"CREATE SCHEMA IF NOT EXISTS local_db.{schema_name}")
                        self._shared_cloud_handle.execute(f"CREATE OR REPLACE TABLE local_db.{table_ref} AS SELECT * FROM {table_ref}")
                        console.success(f"Synced {table_ref} -> local_db")
                    except Exception as e:
                        console.warn(f"Sync failed: {e}")
            else:
                console.debug("Local-only mode (no sync needed)")
            return self.get_thread_connection(), None
        
        # Skip incomplete DDL statements that would cause parser errors
        if sql_upper.endswith(' EXISTS') or sql_upper.endswith(' EXISTS '):
            return self.get_thread_connection(), None
        if sql_upper == 'CREATE SCHEMA IF NOT EXISTS' or sql_upper == 'DROP SCHEMA IF EXISTS':
            return self.get_thread_connection(), None
        if sql_upper.startswith('CREATE SCHEMA IF NOT EXISTS') and len(sql_upper) < 30:
            return self.get_thread_connection(), None
        if sql_upper.startswith('DROP SCHEMA IF EXISTS') and len(sql_upper) < 25:
            return self.get_thread_connection(), None
        
        # Switch to the appropriate handle based on current engine
        connection = self.get_thread_connection()
        if self._current_engine == "cloud" and self._shared_cloud_handle is not None:
            # Use cloud (MotherDuck) handle
            old_handle = connection.handle
            connection.handle = self._shared_cloud_handle
            if old_handle != self._shared_cloud_handle:
                console.step(f"Executing on CLOUD: {sql[:80]}...")
        elif self._shared_local_handle is not None:
            # Use local DuckDB handle
            connection.handle = self._shared_local_handle
        
        # Try local execution first, fallback to Snowflake if it fails
        try:
            result = super().add_query(sql, auto_begin, bindings, abridge_sql_log, **kwargs)
            
            # After successful local execution, sync to Snowflake if enabled
            self._sync_to_snowflake_if_enabled(sql)
            
            return result
        except Exception as e:
            error_str = str(e)
            # Check if this is a DuckDB-incompatibility error
            if self._is_duckdb_incompatibility(error_str):
                # Fallback to Snowflake
                return self._fallback_to_snowflake(sql, auto_begin, bindings, abridge_sql_log, error_str, **kwargs)
            else:
                # Re-raise other errors
                raise
    
    @staticmethod
    def _is_duckdb_incompatibility(error_str: str) -> bool:
        """
        Check if an error indicates DuckDB cannot handle this SQL.
        
        Returns True for errors that should trigger Snowflake fallback:
        - Missing functions (e.g., Snowflake UDFs)
        - VARIANT/OBJECT/ARRAY type errors
        - Not implemented features in DuckDB
        """
        # Function not found (existing check)
        if "does not exist" in error_str and ("Function" in error_str or "Scalar Function" in error_str):
            return True
        # VARIANT type not supported in DuckDB
        if "VARIANT" in error_str.upper() and ("Not implemented" in error_str or "cannot be created" in error_str):
            return True
        # General DuckDB "Not implemented" errors
        if "Not implemented Error" in error_str:
            return True
        return False
    
    # Sync configuration
    _sync_to_snowflake: bool = True  # Enable by default
    _synced_objects: set = set()  # Track synced objects to avoid duplicates
    
    def _sync_to_snowflake_if_enabled(self, sql: str) -> None:
        """Sync DDL results to Snowflake after local execution."""
        if not self._sync_to_snowflake:
            return
        
        sql_stripped = sql.strip()
        
        # Remove dbt comment headers (/* {"app": "dbt", ...} */)
        import re
        sql_no_comments = re.sub(r'/\*.*?\*/', '', sql_stripped, flags=re.DOTALL).strip()
        sql_upper = sql_no_comments.upper()
        
        # Only sync CREATE VIEW/TABLE statements
        if not sql_upper.startswith("CREATE"):
            return
        
        # Extract the object being created (schema.table_name)
        match = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:VIEW|TABLE)\s+(\S+)', sql_no_comments, re.IGNORECASE)
        if not match:
            return
        
        object_name = match.group(1)
        console.step(f"Syncing {object_name} to Snowflake")
        
        # Skip if already synced
        if object_name.lower() in self._synced_objects:
            return
        
        try:
            # Get Snowflake connection
            if self._snowflake_conn_instance is None:
                from dbt.adapters.icebreaker.snowflake_helper import get_snowflake_connection
                self._snowflake_conn_instance = get_snowflake_connection()
            
            if self._snowflake_conn_instance is None:
                return
            
            cursor = self._snowflake_conn_instance.cursor()
            
            # Ensure schema exists
            schema_name = object_name.split('.')[0] if '.' in object_name else None
            if schema_name:
                try:
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                except Exception:
                    pass
            
            # For both views and tables, materialize from DuckDB and upload
            # via write_pandas. Replaying DDL doesn't work because transpiled
            # DuckDB SQL uses unquoted lowercase identifiers that Snowflake rejects.
            self._sync_view_as_table(object_name, cursor)
            
            self._synced_objects.add(object_name.lower())
            
        except Exception as e:
            console.warn(f"Sync failed for {object_name}: {e}")
    
    def _sync_view_as_table(self, view_name: str, snowflake_cursor) -> None:
        """Materialize a DuckDB view and upload to Snowflake as a table."""
        try:
            # Query the view from DuckDB
            import pandas as pd
            df = self._shared_local_handle.execute(f"SELECT * FROM {view_name}").fetchdf()
            
            if df.empty:
                console.warn(f"{view_name} is empty, skipping sync")
                return
            
            # Write to Snowflake using write_pandas
            from snowflake.connector.pandas_tools import write_pandas
            
            # Parse schema and table name
            parts = view_name.split('.')
            if len(parts) == 2:
                schema, table = parts
            else:
                schema = "PUBLIC"
                table = view_name
            
            # Upload to Snowflake
            success, nchunks, nrows, _ = write_pandas(
                conn=self._snowflake_conn_instance,
                df=df,
                table_name=table.upper(),
                schema=schema.upper(),
                auto_create_table=True,
                overwrite=True,
                use_logical_type=True,
            )
            
            if success:
                console.success(f"Synced {view_name} -> Snowflake ({nrows:,} rows)")
            else:
                console.warn(f"Sync failed for {view_name}")
                
        except Exception as e:
            console.warn(f"Could not sync {view_name}: {e}")
    
    def _fallback_to_snowflake(self, sql, auto_begin, bindings, abridge_sql_log, error_reason, **kwargs):
        """Execute on Snowflake when local DuckDB execution fails."""
        console.warn(f"Local execution failed: {error_reason[:80]}")
        console.step("Falling back to Snowflake...")
        
        # Get Snowflake connection
        if self._snowflake_conn_instance is None:
            from dbt.adapters.icebreaker.snowflake_helper import get_snowflake_connection
            self._snowflake_conn_instance = get_snowflake_connection()
        
        if self._snowflake_conn_instance is None:
            raise RuntimeError("Cannot fallback to Snowflake: no connection available")
        
        # Execute on Snowflake
        cursor = self._snowflake_conn_instance.cursor()
        try:
            # If this is DDL (CREATE VIEW/TABLE), ensure schema exists first
            sql_stripped = sql.strip()
            sql_upper = sql_stripped.upper()
            if sql_upper.startswith("CREATE"):
                # Extract schema from CREATE statement
                import re
                match = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:VIEW|TABLE)\s+(\S+)\.', sql_stripped, re.IGNORECASE)
                if match:
                    schema_part = match.group(1)  # e.g., "_stg_halo"
                    try:
                        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_part}")
                        console.debug(f"Ensured schema exists: {schema_part}")
                    except Exception as schema_err:
                        console.warn(f"Could not create schema {schema_part}: {schema_err}")
            
            cursor.execute(sql, bindings if bindings else None)
            console.success("Executed on Snowflake")
            
            # Return in expected format
            from dbt.adapters.contracts.connection import AdapterResponse
            response = AdapterResponse(
                _message="OK",
                rows_affected=cursor.rowcount or -1,
            )
            connection = self.get_thread_connection()
            return connection, cursor
        except Exception as sf_error:
            console.error(f"Snowflake execution also failed: {sf_error}")
            raise
    
    def rollback(self, connection):
        """No explicit ROLLBACK for in-memory DuckDB."""
        connection.transaction_open = False
        return connection
    
    # === SQL TRANSPILATION (Snowflake → DuckDB) ===
    _transpilation_enabled: bool = True
    _transpilation_logged: set = set()  # Track logged transpilations to avoid spam
    
    @classmethod
    def _transpile_snowflake_to_duckdb(cls, sql: str) -> str:
        """
        Transpile Snowflake SQL to DuckDB-compatible SQL using the Transpiler.
        
        Uses the project's Transpiler class which applies all custom transforms:
        - CONVERT_TIMEZONE → timezone()
        - TRY_CAST → TRY_CAST (DuckDB supports this)
        - FLATTEN → UNNEST
        - IFF → IF
        - VARIANT casts → JSON
        - And more (see transpiler.py)
        """
        if not cls._transpilation_enabled:
            return sql
        
        # Skip if SQL is too short or doesn't look like it needs transpilation
        if len(sql) < 50:
            return sql
        
        try:
            from dbt.adapters.icebreaker.transpiler import Transpiler, TranspilationError
            
            transpiler = Transpiler(source_dialect="snowflake")
            transpiled = transpiler.to_duckdb(sql)
            
            if transpiled and transpiled != sql:
                # Log first time we transpile something
                # Strip dbt comment headers before generating dedup key,
                # otherwise all models share the same prefix
                import re as _re
                sql_for_key = _re.sub(r'/\*.*?\*/', '', sql, count=1, flags=_re.DOTALL).strip()
                sql_preview = sql_for_key[:80].replace('\n', ' ')
                if sql_preview not in cls._transpilation_logged:
                    console.step(f"Transpiled: {sql_preview}")
                    cls._transpilation_logged.add(sql_preview)
                return transpiled
            
            return sql
            
        except Exception as e:
            # If transpilation fails, return original SQL
            # The fallback-to-Snowflake logic will catch any execution errors
            return sql
    
    # === AUTO-CACHE SUPPORT ===
    _source_cache_instance = None
    _snowflake_conn_instance = None
    _cached_tables: set = set()  # Track tables we've already tried to cache
    
    @classmethod
    def _auto_cache_sources_from_sql(cls, sql: str) -> None:
        """
        Parse SQL for source table references and auto-cache from Snowflake.
        
        Looks for patterns like:
        - FROM schema.table
        - JOIN schema.table
        - from "schema"."table"
        
        Caches tables that don't exist in DuckDB yet.
        """
        if cls._shared_local_handle is None:
            return
        
        # Parse SQL for table references (schema.table pattern)
        # Matches: FROM/JOIN followed by schema.table (case insensitive)
        table_pattern = regex_module.compile(
            r'(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\.\s*([a-zA-Z_][a-zA-Z0-9_]*)',
            regex_module.IGNORECASE
        )
        
        matches = table_pattern.findall(sql)
        if not matches:
            return
        
        for schema, table in matches:
            table_key = f"{schema}.{table}".lower()
            
            # Skip if we already tried to cache this
            if table_key in cls._cached_tables:
                continue
            
            # Check if table already exists in DuckDB
            try:
                cls._shared_local_handle.execute(f"SELECT 1 FROM {schema}.{table} LIMIT 0")
                cls._cached_tables.add(table_key)
                continue  # Table exists, no need to cache
            except Exception:
                pass  # Table doesn't exist, need to cache
            
            # Try to cache from Snowflake
            cls._cache_table_from_snowflake(schema, table)
            cls._cached_tables.add(table_key)
    
    @classmethod
    def _cache_table_from_snowflake(cls, schema: str, table: str) -> bool:
        """
        Cache a single table from Snowflake to local Parquet.
        
        Uses key-pair authentication from profiles.yml.
        Resolves actual database/schema from dbt manifest.json.
        """
        try:
            # Get Snowflake connection (lazy initialization)
            if cls._snowflake_conn_instance is None:
                from dbt.adapters.icebreaker.snowflake_helper import get_snowflake_connection
                cls._snowflake_conn_instance = get_snowflake_connection()
                if cls._snowflake_conn_instance:
                    console.success("Connected to Snowflake for source caching")
            
            if cls._snowflake_conn_instance is None:
                console.warn(f"Cannot cache {schema}.{table}: No Snowflake connection")
                return False
            
            # Get source cache instance
            if cls._source_cache_instance is None:
                from dbt.adapters.icebreaker.source_cache import SourceCache, CacheConfig
                cache_config = CacheConfig(cache_enabled=True)
                cls._source_cache_instance = SourceCache(
                    config=cache_config,
                    snowflake_conn=cls._snowflake_conn_instance,
                    duckdb_conn=cls._shared_local_handle,
                )
            
            # Try to cache the table
            console.step(f"Caching {schema}.{table} from Snowflake")
            
            # Resolve actual database and schema from dbt manifest
            database, actual_schema = cls._resolve_source_from_manifest(schema, table)
            
            success = cls._source_cache_instance.ensure_cached(
                database=database,
                schema=actual_schema,
                table=table,
                duckdb_conn=cls._shared_local_handle,
            )
            
            if success:
                console.success(f"Cached {schema}.{table}")
            else:
                console.warn(f"Could not cache {schema}.{table}")
            
            return success
            
        except Exception as e:
            console.error(f"Failed to cache {schema}.{table}: {e}")
            return False
    
    # Cache for manifest sources
    _manifest_sources: dict = {}
    _manifest_loaded: bool = False
    
    @classmethod
    def _resolve_source_from_manifest(cls, schema: str, table: str) -> tuple:
        """
        Resolve actual Snowflake database.schema from dbt manifest.json.
        
        The manifest contains source definitions with their true database/schema.
        Falls back to profile database if manifest unavailable.
        """
        # Load manifest if not already loaded
        if not cls._manifest_loaded:
            cls._load_manifest_sources()
        
        # Look for this source in manifest
        # Source keys are like: source.project_name.source_name.table_name
        # The schema name in compiled SQL is typically the source_name
        lookup_key = f"{schema.lower()}.{table.lower()}"
        
        if lookup_key in cls._manifest_sources:
            source_info = cls._manifest_sources[lookup_key]
            return source_info["database"], source_info["schema"]
        
        # Fallback: use profile database and schema as-is
        from dbt.adapters.icebreaker.snowflake_helper import find_icebreaker_profile
        profile = find_icebreaker_profile() or {}
        fallback_db = profile.get("database", "DBT_ANALYTICS_RAW")
        return fallback_db, schema.upper()
    
    @classmethod
    def _load_manifest_sources(cls) -> None:
        """Load source definitions from dbt manifest.json."""
        import json
        import os
        
        cls._manifest_loaded = True
        
        # Look for manifest in common locations
        manifest_paths = [
            "target/manifest.json",
            os.path.join(os.getcwd(), "target/manifest.json"),
        ]
        
        for manifest_path in manifest_paths:
            if os.path.exists(manifest_path):
                try:
                    with open(manifest_path, 'r') as f:
                        manifest = json.load(f)
                    
                    # Extract source definitions
                    sources = manifest.get("sources", {})
                    for source_id, source_def in sources.items():
                        # source_id like: source.obie_data_model.halo.carriers_raw
                        source_name = source_def.get("source_name", "").lower()
                        table_name = source_def.get("name", "").lower()
                        
                        # Store with key: source_name.table_name
                        key = f"{source_name}.{table_name}"
                        cls._manifest_sources[key] = {
                            "database": source_def.get("database", "").upper(),
                            "schema": source_def.get("schema", "").upper(),
                        }
                    
                    console.info(f"Loaded {len(cls._manifest_sources)} sources from manifest")
                    return
                    
                except Exception as e:
                    console.warn(f"Could not load manifest: {e}")
        
        console.warn("No manifest.json found — run 'dbt parse' first for auto source resolution")
    
    # Class-level shared connections (single connection shared by all threads)
    _shared_local_handle: Optional[duckdb.DuckDBPyConnection] = None
    _shared_cloud_handle: Optional[duckdb.DuckDBPyConnection] = None
    _current_engine: str = "local"  # Track which engine is active
    _sync_enabled: bool = False  # Track if cross-db sync is available
    
    @classmethod
    def set_engine(cls, engine: str, credentials: "IcebreakerCredentials" = None):
        """Set the current engine for query routing. Called from materializations.
        
        Args:
            engine: "local" or "cloud"
            credentials: Optional credentials for initializing cloud connection
        """
        if engine == "cloud" and credentials:
            # Try to initialize MotherDuck connection if not already done
            if cls._shared_cloud_handle is None:
                cls.get_motherduck_handle(credentials)
        
        cls._current_engine = engine
    
    @classmethod
    def get_engine(cls) -> str:
        """Get the current engine."""
        return cls._current_engine
    
    @classmethod
    def _log_routing(cls, model_name: str, engine: str, reason: str = ""):
        """Log routing decision for visibility."""
        import dbt_common.events.types as events
        from dbt_common.events.functions import fire_event
        
        engine_label = "LOCAL" if engine == "local" else "CLOUD (MotherDuck)"
        msg = f"Routing: {model_name} -> {engine_label}"
        if reason:
            msg += f" ({reason})"
        
        # Log to dbt output
        try:
            fire_event(events.Note(msg=msg))
        except Exception:
            # Fallback to print if event system not available
            console.info(msg)
    
    @classmethod
    def get_motherduck_handle(cls, credentials: IcebreakerCredentials) -> Optional[duckdb.DuckDBPyConnection]:
        """Get or create MotherDuck connection.
        
        Instead of separate connections, we ATTACH MotherDuck to the local DuckDB.
        This enables cross-database queries for syncing between engines.
        """
        if cls._shared_cloud_handle is not None:
            return cls._shared_cloud_handle
        
        # Get token from credentials or environment
        token = credentials.motherduck_token or os.environ.get("MOTHERDUCK_TOKEN")
        if not token:
            return None
        
        try:
            # Connect to MotherDuck with token
            db_name = credentials.motherduck_database or "my_db"
            connection_string = f"md:{db_name}?motherduck_token={token}"
            
            try:
                cls._shared_cloud_handle = duckdb.connect(connection_string)
            except Exception as db_error:
                # Database might not exist - try to create it
                if "no database/share named" in str(db_error):
                    console.step(f"Creating MotherDuck database '{db_name}'")
                    # Connect without a specific database to create it
                    temp_conn = duckdb.connect(f"md:?motherduck_token={token}")
                    temp_conn.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
                    temp_conn.close()
                    # Now connect to the new database
                    cls._shared_cloud_handle = duckdb.connect(connection_string)
                    console.success(f"Created MotherDuck database '{db_name}'")
                else:
                    raise db_error
            
            cls._shared_cloud_handle.execute(f"SET threads = {credentials.threads}")
            
            # Also attach MotherDuck to the local connection for cross-db queries
            if cls._shared_local_handle is not None:
                try:
                    cls._shared_local_handle.execute(f"ATTACH '{connection_string}' AS md_cloud")
                    console.success("Attached MotherDuck to local DuckDB for sync")
                except Exception as e:
                    console.warn(f"Could not attach MotherDuck to local: {e}")
            
            # Load extensions
            for ext in credentials._duckdb_extensions:
                try:
                    cls._shared_cloud_handle.execute(f"INSTALL {ext}")
                    cls._shared_cloud_handle.execute(f"LOAD {ext}")
                except Exception:
                    pass
            
            return cls._shared_cloud_handle
        except Exception as e:
            # Log error but don't fail - fall back to local
            console.warn(f"MotherDuck connection failed, using local only: {e}")
            return None
    
    # Shared Snowflake connection
    _shared_snowflake_handle: Optional[Any] = None
    
    @classmethod
    def get_snowflake_handle(cls, credentials: IcebreakerCredentials) -> Optional[Any]:
        """Get or create Snowflake connection using the user's existing credentials."""
        if cls._shared_snowflake_handle is not None:
            return cls._shared_snowflake_handle
        
        if credentials.cloud_type != "snowflake" or not credentials.account:
            return None
        
        try:
            # Try to import snowflake connector
            import snowflake.connector
            
            # Build connection kwargs from credentials
            connect_kwargs = {
                "account": credentials.account,
                "database": credentials.database if credentials.database != "memory" else None,
                "schema": credentials.schema if credentials.schema != "main" else None,
            }
            
            # Add auth method
            if credentials.user:
                connect_kwargs["user"] = credentials.user
            if credentials.password:
                connect_kwargs["password"] = credentials.password
            if credentials.private_key_path:
                connect_kwargs["private_key_file"] = credentials.private_key_path
            if credentials.authenticator:
                connect_kwargs["authenticator"] = credentials.authenticator
            if credentials.warehouse:
                connect_kwargs["warehouse"] = credentials.warehouse
            if credentials.role:
                connect_kwargs["role"] = credentials.role
            
            # Remove None values
            connect_kwargs = {k: v for k, v in connect_kwargs.items() if v is not None}
            
            cls._shared_snowflake_handle = snowflake.connector.connect(**connect_kwargs)
            console.success(f"Connected to Snowflake: {credentials.account}")
            return cls._shared_snowflake_handle
            
        except ImportError:
            console.warn("snowflake-connector-python not installed. Run: pip install dbt-icebreaker[snowflake]")
            return None
        except Exception as e:
            console.warn(f"Snowflake connection failed: {e}")
            return None
    
    @classmethod
    def execute_on_snowflake(cls, sql: str, credentials: IcebreakerCredentials) -> Any:
        """Execute a query on Snowflake and return results."""
        handle = cls.get_snowflake_handle(credentials)
        if not handle:
            raise DbtRuntimeError("Snowflake connection not available")
        
        cursor = handle.cursor()
        try:
            cursor.execute(sql)
            return cursor.fetchall()
        finally:
            cursor.close()
    
    # Iceberg catalog tracking
    _iceberg_catalog_attached = False
    
    @classmethod
    def attach_iceberg_catalog(cls, handle: duckdb.DuckDBPyConnection, credentials: IcebreakerCredentials) -> bool:
        """Attach an Iceberg REST catalog to the DuckDB connection.
        
        Enables reading from existing Iceberg catalogs like Polaris, Glue, or Nessie.
        This allows local DuckDB to read the same tables as Snowflake external tables.
        
        Returns:
            True if catalog was attached successfully, False otherwise.
        """
        if cls._iceberg_catalog_attached:
            return True
        
        if not credentials.iceberg_catalog_url:
            return False
        
        try:
            # Install and load the Iceberg extension
            handle.execute("INSTALL iceberg")
            handle.execute("LOAD iceberg")
            
            # Configure S3 credentials if provided
            if credentials.iceberg_s3_region:
                handle.execute(f"SET s3_region = '{credentials.iceberg_s3_region}'")
            if credentials.iceberg_s3_access_key and credentials.iceberg_s3_secret_key:
                handle.execute(f"SET s3_access_key_id = '{credentials.iceberg_s3_access_key}'")
                handle.execute(f"SET s3_secret_access_key = '{credentials.iceberg_s3_secret_key}'")
            
            # Build the ATTACH statement for the Iceberg REST catalog
            catalog_type = credentials.iceberg_catalog_type or "rest"
            attach_sql = f"""
                ATTACH '' AS iceberg_catalog (
                    TYPE ICEBERG,
                    CATALOG_TYPE '{catalog_type}',
                    URI '{credentials.iceberg_catalog_url}'
            """
            
            # Add optional parameters
            if credentials.iceberg_warehouse:
                attach_sql += f",\n                    WAREHOUSE '{credentials.iceberg_warehouse}'"
            if credentials.iceberg_token:
                attach_sql += f",\n                    TOKEN '{credentials.iceberg_token}'"
            elif credentials.iceberg_credential:
                attach_sql += f",\n                    CREDENTIAL '{credentials.iceberg_credential}'"
            
            attach_sql += "\n                )"
            
            handle.execute(attach_sql)
            
            cls._iceberg_catalog_attached = True
            console.success(f"Connected to Iceberg catalog: {credentials.iceberg_catalog_url}")
            
            # List available schemas/namespaces for visibility
            try:
                result = handle.execute("SELECT * FROM iceberg_catalog.information_schema.schemata LIMIT 5").fetchall()
                if result:
                    schemas = [row[1] for row in result]
                    console.info(f"Available namespaces: {', '.join(schemas[:3])}...")
            except Exception:
                pass
            
            return True
            
        except Exception as e:
            console.warn(f"Iceberg catalog connection failed: {e}")
            return False
    
    # Thread lock for connection initialization
    import threading
    _connection_lock = threading.Lock()
    _local_db_attached = False  # Track if local_db is already attached
    
    @classmethod
    def open(cls, connection: Connection) -> Connection:
        """Open a new connection.
        
        Architecture for sync:
        - If MotherDuck token available: Use MotherDuck as primary, attach local file
        - Otherwise: Use local in-memory DuckDB only
        - This enables cross-database queries for syncing
        
        IMPORTANT: Always tries to connect to MotherDuck if token is available,
        ensuring sync works by default without extra steps.
        
        Thread-safe: Uses lock to prevent race conditions when multiple threads
        try to attach local_db concurrently.
        """
        if connection.state == ConnectionState.OPEN:
            return connection
        
        credentials: IcebreakerCredentials = connection.credentials
        token = credentials.motherduck_token or os.environ.get("MOTHERDUCK_TOKEN")
        
        # Use lock to prevent race conditions with shared connection initialization
        with cls._connection_lock:
            # Double-check pattern: check again after acquiring lock
            if connection.state == ConnectionState.OPEN:
                return connection
            
            # If we already have a working cloud handle, just reuse it
            if cls._shared_cloud_handle is not None and cls._sync_enabled:
                connection.state = ConnectionState.OPEN
                connection.handle = cls._shared_cloud_handle
                return connection
            
            if token and (cls._shared_cloud_handle is None or not cls._sync_enabled):
                # MotherDuck available - use as PRIMARY connection
                try:
                    db_name = credentials.motherduck_database or "my_db"
                    connection_string = f"md:{db_name}?motherduck_token={token}"
                    
                    # Close existing handles if any (fresh start)
                    if cls._shared_cloud_handle is not None:
                        try:
                            cls._shared_cloud_handle.close()
                        except:
                            pass
                        cls._local_db_attached = False
                    
                    cls._shared_cloud_handle = duckdb.connect(connection_string)
                    cls._shared_cloud_handle.execute(f"SET threads = {credentials.threads}")
                    
                    # Attach local file database for sync (only if not already attached)
                    if not cls._local_db_attached:
                        import tempfile
                        local_db_path = os.path.join(tempfile.gettempdir(), "icebreaker_local.duckdb")
                        
                        # Check if local_db is already attached
                        try:
                            result = cls._shared_cloud_handle.execute(
                                "SELECT database_name FROM duckdb_databases() WHERE database_name = 'local_db'"
                            ).fetchone()
                            if result is None:
                                cls._shared_cloud_handle.execute(f"ATTACH '{local_db_path}' AS local_db")
                                console.success(f"Connected to MotherDuck with local_db attached")
                                console.info(f"Local cache: {local_db_path}")
                            cls._local_db_attached = True
                        except Exception as e:
                            # Attachment failed, but connection still works
                            console.warn(f"Could not attach local_db: {e}")
                    
                    cls._shared_local_handle = cls._shared_cloud_handle  # Same connection!
                    cls._sync_enabled = True  # Cross-db sync is available
                    
                    # Load extensions
                    for ext in credentials._duckdb_extensions:
                        try:
                            cls._shared_cloud_handle.execute(f"INSTALL {ext}")
                            cls._shared_cloud_handle.execute(f"LOAD {ext}")
                        except Exception:
                            pass
                    
                    connection.state = ConnectionState.OPEN
                    connection.handle = cls._shared_cloud_handle
                    cls._current_engine = "cloud"  # Default to cloud when available
                    return connection
                    
                except Exception as e:
                    console.warn(f"MotherDuck connection failed, using local only: {e}")
            
            # Fallback: Local-only mode
            if cls._shared_local_handle is None:
                cls._shared_local_handle = duckdb.connect(":memory:")
                cls._shared_local_handle.execute(f"SET threads = {credentials.threads}")
                
                for ext in credentials._duckdb_extensions:
                    try:
                        cls._shared_local_handle.execute(f"INSTALL {ext}")
                        cls._shared_local_handle.execute(f"LOAD {ext}")
                    except Exception:
                        pass
            
            connection.state = ConnectionState.OPEN
            connection.handle = cls._shared_local_handle
            cls._current_engine = "local"
            
            # Attach Iceberg catalog if configured
            cls.attach_iceberg_catalog(cls._shared_local_handle, credentials)
            
            return connection
    
    @classmethod
    def switch_to_cloud(cls, connection: Connection, model_name: str = "") -> bool:
        """Switch to cloud warehouse for the next query.
        
        Supports: Snowflake (native), MotherDuck (DuckDB cloud)
        """
        credentials: IcebreakerCredentials = connection.credentials
        
        # Try Snowflake first if configured
        if credentials.cloud_type == "snowflake":
            snowflake_handle = cls.get_snowflake_handle(credentials)
            if snowflake_handle:
                # Note: Snowflake uses a different cursor pattern, not a DuckDB handle
                # We store it for use in execute_on_snowflake
                cls._current_engine = "snowflake"
                cls._log_routing(model_name, "cloud", f"Snowflake ({credentials.account})")
                return True
            else:
                cls._log_routing(model_name, "local", "Snowflake connection failed")
                return False
        
        # Try MotherDuck as fallback
        cloud_handle = cls.get_motherduck_handle(credentials)
        if cloud_handle:
            connection.handle = cloud_handle
            cls._current_engine = "cloud"
            cls._log_routing(model_name, "cloud", "MotherDuck")
            return True
        
        cls._log_routing(model_name, "local", "No cloud configured")
        return False
    
    @classmethod
    def switch_to_local(cls, connection: Connection, model_name: str = ""):
        """Switch back to local DuckDB."""
        connection.handle = cls._shared_local_handle
        cls._current_engine = "local"
        cls._log_routing(model_name, "local", "default")
    
    @property
    def duckdb(self) -> duckdb.DuckDBPyConnection:
        """Lazy-initialize DuckDB connection with required extensions."""
        if self._duckdb_conn is None:
            self._duckdb_conn = duckdb.connect(":memory:")
            
            # Configure threads
            credentials = self.profile.credentials
            self._duckdb_conn.execute(f"SET threads = {credentials.threads}")
            
            # Load extensions
            for ext in credentials._duckdb_extensions:
                try:
                    self._duckdb_conn.execute(f"INSTALL {ext}")
                    self._duckdb_conn.execute(f"LOAD {ext}")
                except Exception as e:
                    # Extension might not be available, log and continue
                    pass
            
            # Configure AWS credentials if available
            self._configure_aws()
        
        return self._duckdb_conn
    
    def _configure_aws(self) -> None:
        """Configure AWS credentials for S3 access."""
        aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        aws_region = os.environ.get("AWS_REGION", "us-east-1")
        
        if aws_access_key and aws_secret_key:
            self._duckdb_conn.execute(f"""
                SET s3_access_key_id = '{aws_access_key}';
                SET s3_secret_access_key = '{aws_secret_key}';
                SET s3_region = '{aws_region}';
            """)
    
    @property
    def cloud(self) -> Any:
        """Lazy-initialize cloud connection."""
        if self._cloud_conn is None:
            credentials = self.profile.credentials
            
            if credentials.cloud_bridge_type is None:
                raise DbtRuntimeError(
                    "Model routed to CLOUD but no cloud_bridge configured in profiles.yml. "
                    "Either add cloud_bridge config or use `icebreaker_route: 'local'` in model config."
                )
            
            self._cloud_conn = self._open_cloud_connection(credentials)
        
        return self._cloud_conn
    
    def _open_cloud_connection(self, credentials: IcebreakerCredentials) -> Any:
        """Open cloud connection based on configured type."""
        bridge_type = credentials.cloud_bridge_type
        
        if bridge_type == "snowflake":
            return self._open_snowflake(credentials)
        elif bridge_type == "bigquery":
            return self._open_bigquery(credentials)
        elif bridge_type == "databricks":
            return self._open_databricks(credentials)
        else:
            raise DbtRuntimeError(f"Unsupported cloud_bridge type: {bridge_type}")
    
    def _open_snowflake(self, credentials: IcebreakerCredentials) -> Any:
        """Open Snowflake connection using standard env vars."""
        try:
            import snowflake.connector
        except ImportError:
            raise DbtRuntimeError(
                "Snowflake connector not installed. Run: pip install dbt-icebreaker[snowflake]"
            )
        
        return snowflake.connector.connect(
            account=credentials.cloud_bridge_account or os.environ.get("SNOWFLAKE_ACCOUNT"),
            user=os.environ.get("SNOWFLAKE_USER"),
            password=os.environ.get("SNOWFLAKE_PASSWORD"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
            database=os.environ.get("SNOWFLAKE_DATABASE"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA"),
        )
    
    def _open_bigquery(self, credentials: IcebreakerCredentials) -> Any:
        """Stub for BigQuery connection."""
        raise NotImplementedError("BigQuery support coming soon")
    
    def _open_databricks(self, credentials: IcebreakerCredentials) -> Any:
        """Stub for Databricks connection."""
        raise NotImplementedError("Databricks support coming soon")
    
    def set_engine(self, engine: EngineType) -> None:
        """Switch the active execution engine."""
        self._active_engine = engine
    
    @contextmanager
    def use_engine(self, engine: EngineType):
        """Context manager for temporarily switching engines."""
        previous = self._active_engine
        self._active_engine = engine
        try:
            yield
        finally:
            self._active_engine = previous
    
    @classmethod
    def get_response(cls, cursor: Any) -> AdapterResponse:
        """Build response from cursor."""
        return AdapterResponse(_message="OK")
    
    def cancel(self, connection: Connection) -> None:
        """Cancel any running queries."""
        pass  # DuckDB doesn't support cancellation
    
    @classmethod
    def close(cls, connection: Connection) -> Connection:
        """Close the connection."""
        connection.state = ConnectionState.CLOSED
        return connection
