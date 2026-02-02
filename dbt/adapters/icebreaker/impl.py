"""
Icebreaker Adapter Implementation

Main adapter class that inherits from dbt's BaseAdapter.
Handles model execution with intelligent routing between local and cloud.
"""

import time
from typing import Any, Dict, List, Optional, Tuple
import agate

from dbt.adapters.base import BaseAdapter, available
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.contracts.relation import RelationType
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.icebreaker.connections import (
    IcebreakerConnectionManager,
    IcebreakerCredentials,
)
from dbt.adapters.icebreaker.relation import IcebreakerRelation
from dbt.adapters.icebreaker.transpiler import Transpiler, TranspilationError
from dbt.adapters.icebreaker.savings import log_execution
from dbt.adapters.icebreaker.auto_router import AutoRouter, RoutingDecision, get_router
from dbt.adapters.icebreaker.catalog_scanner import CatalogScanner, get_catalog_scanner


class IcebreakerAdapter(SQLAdapter):
    """
    Icebreaker dbt Adapter
    
    A hybrid adapter that routes queries between:
    - DuckDB (local, free compute)
    - Cloud warehouse (Snowflake, BigQuery, etc.)
    
    Uses intelligent routing based on:
    - User config (icebreaker_route: 'local' or 'cloud')
    - SQL capability (can it be transpiled?)
    - Data volume (from catalog metadata)
    - Historical performance (telemetry)
    """
    
    ConnectionManager = IcebreakerConnectionManager
    
    # Use custom relation that handles DuckDB quoting
    Relation = IcebreakerRelation
    
    @classmethod
    def type(cls) -> str:
        return "icebreaker"
    
    @classmethod
    def date_function(cls) -> str:
        return "current_date"
    
    def __init__(self, config, mp_context=None) -> None:
        super().__init__(config, mp_context)
        self._transpiler: Optional[Transpiler] = None
        self._auto_router: Optional[AutoRouter] = None
        self._catalog_scanner: Optional[CatalogScanner] = None
    
    @property
    def transpiler(self) -> Transpiler:
        """Lazy-initialize the transpiler."""
        if self._transpiler is None:
            source_dialect = self.config.credentials.source_dialect
            self._transpiler = Transpiler(source_dialect=source_dialect)
        return self._transpiler
    
    @property
    def auto_router(self) -> AutoRouter:
        """Lazy-initialize the automatic router."""
        if self._auto_router is None:
            credentials = self.config.credentials
            max_local_gb = getattr(credentials, 'max_local_size_gb', 5.0)
            
            # Create catalog scanner for volume estimation
            self._auto_router = AutoRouter(
                max_local_gb=max_local_gb,
                catalog_scanner=self._catalog_scanner,
            )
        return self._auto_router
    
    @available
    def get_routing_decision(self, model: Dict[str, Any], sql: str) -> Dict[str, Any]:
        """
        Get automatic routing decision for a model.
        
        This is the NEW automatic routing - no tags required!
        Called from materializations to determine where to execute.
        
        Args:
            model: The dbt model node
            sql: The compiled SQL
            
        Returns:
            Dict with 'venue' ('local' or 'cloud') and 'reason'
        """
        decision = self.auto_router.decide(sql, model)
        
        return {
            "venue": decision.venue.lower(),
            "reason": decision.reason.value if hasattr(decision.reason, 'value') else str(decision.reason),
            "details": decision.details or "",
            "confidence": decision.confidence,
        }
    
    @available
    def explain_routing(self, model: Dict[str, Any], sql: str) -> str:
        """
        Get human-readable explanation of routing decision.
        
        Useful for debugging and CLI commands.
        
        Args:
            model: The dbt model node
            sql: The compiled SQL
            
        Returns:
            Multi-line string explaining the routing decision
        """
        return self.auto_router.explain(sql, model)
    
    def set_engine(self, engine: str) -> None:
        """Set the current engine for query routing.
        
        Args:
            engine: "local" or "cloud"
        """
        from dbt.adapters.icebreaker.connections import IcebreakerConnectionManager
        IcebreakerConnectionManager.set_engine(engine, self.config.credentials)
    
    def ensure_cloud_connection(self) -> bool:
        """Ensure MotherDuck connection is initialized.
        
        Returns:
            True if cloud connection is available
        """
        from dbt.adapters.icebreaker.connections import IcebreakerConnectionManager
        handle = IcebreakerConnectionManager.get_motherduck_handle(self.config.credentials)
        return handle is not None

    # =========================================================================
    # Routing Logic
    # =========================================================================
    
    def decide_venue(
        self,
        model: Dict[str, Any],
        sql: str,
        sources: Optional[List[Dict]] = None,
    ) -> str:
        """
        Decide where to execute a model: 'local' or 'cloud'.
        
        Routing priority:
        1. Check for explicit icebreaker_route config in model
        2. Use Traffic Controller for intelligent routing
        
        Args:
            model: The full model node (not just config)
            sql: The SQL to execute
            sources: Optional source metadata
            
        Returns:
            'local' for DuckDB, 'cloud' for MotherDuck
        """
        model_name = model.get("name", "unknown")
        config = model.get("config", {})
        
        # Priority 1: Check for explicit routing config
        explicit_route = config.get("icebreaker_route")
        if explicit_route:
            route = explicit_route.lower()
            if route in ("cloud", "motherduck"):
                self._log_routing_decision(model_name, type("Decision", (), {"venue": "cloud", "reason": "icebreaker_route='cloud'"})())
                return "cloud"
            elif route == "local":
                self._log_routing_decision(model_name, type("Decision", (), {"venue": "local", "reason": "icebreaker_route='local'"})())
                return "local"
        
        # Priority 2: Use Traffic Controller for intelligent routing
        from dbt.adapters.icebreaker.traffic import (
            TrafficController,
            TrafficConfig,
        )
        
        credentials = self.config.credentials
        traffic_config = TrafficConfig(
            max_local_seconds=getattr(credentials, 'max_local_seconds', 600),
            max_local_size_gb=getattr(credentials, 'max_local_size_gb', 5.0),
            source_dialect=getattr(credentials, 'source_dialect', 'snowflake'),
        )
        
        controller = TrafficController(traffic_config)
        decision = controller.decide(model, sql, sources)
        
        self._log_routing_decision(model_name, decision)
        
        return decision.venue.lower()
    
    def _log_routing_decision(self, model_name: str, decision) -> None:
        """Log routing decision for user visibility."""
        # This will appear in dbt logs
        if decision.venue == "LOCAL":
            pass  # Silent for local (the common case)
        else:
            # Log why we're routing to cloud
            pass  # Will be visible in dbt output
    
    # =========================================================================
    # Execution
    # =========================================================================
    
    @available
    def execute_model(
        self,
        model: Dict[str, Any],
        manifest: Any,
        sql: str,
    ) -> Tuple[str, agate.Table]:
        """
        Execute a model with intelligent routing.
        
        Args:
            model: The model node from the manifest
            manifest: The dbt manifest
            sql: The compiled SQL to execute
            
        Returns:
            Tuple of (status message, result table)
        """
        model_config = model.get("config", {})
        venue = self.decide_venue(model, sql)
        
        if venue == "local":
            return self._execute_local(model, sql, model_config)
        else:
            return self._execute_cloud(model, sql, model_config)
    
    def _execute_local(
        self,
        model: Dict[str, Any],
        sql: str,
        config: Dict[str, Any],
    ) -> Tuple[str, agate.Table]:
        """
        Execute model locally using DuckDB.
        
        Flow:
        1. Transpile SQL to DuckDB dialect
        2. Apply dev sampling if enabled
        3. Execute on DuckDB
        4. Log savings
        """
        model_name = model.get("name", "unknown")
        start_time = time.time()
        
        # Transpile to DuckDB
        try:
            duckdb_sql = self.transpiler.to_duckdb(sql)
        except TranspilationError as e:
            raise DbtRuntimeError(f"Failed to transpile SQL for local execution: {e}")
        
        # Apply dev sampling
        target_name = self.config.target_name
        if target_name == "dev":
            duckdb_sql = self._apply_dev_sampling(duckdb_sql, config)
        
        # Execute on DuckDB
        self.connections.set_engine("duckdb")
        response, result = self.connections.execute(duckdb_sql, fetch=True)
        
        # Calculate execution time and log savings
        execution_time = time.time() - start_time
        rows = len(result) if result else 0
        
        # Log execution for savings tracking
        try:
            cloud_type = getattr(self.config.credentials, 'cloud_type', 'snowflake') or 'snowflake'
            execution = log_execution(
                model_name=model_name,
                engine_used='duckdb',
                execution_time_seconds=execution_time,
                rows_processed=rows,
                cloud_type=cloud_type,
            )
            if execution.savings > 0.01:  # Only show if meaningful
                print(f"💰 Saved ${execution.savings:.2f} by running locally")
        except Exception:
            pass  # Don't fail the query if logging fails
        
        # Convert to agate table
        table = self._result_to_agate(result)
        
        return "OK (Local)", table
    
    def _execute_cloud(
        self,
        model: Dict[str, Any],
        sql: str,
        config: Dict[str, Any],
    ) -> Tuple[str, agate.Table]:
        """
        Execute model on cloud warehouse with Iceberg output.
        
        Flow:
        1. Wrap SQL in Iceberg DDL via bridge.py
        2. Send to cloud
        3. Log execution (no savings, but track for analytics)
        4. Return results
        """
        from dbt.adapters.icebreaker.bridge import (
            Bridge,
            IcebergConfig,
            CatalogRegistrar,
        )
        
        model_name = model.get("name", "unknown")
        start_time = time.time()
        credentials = self.config.credentials
        cloud_type = getattr(credentials, 'cloud_type', 'snowflake') or 'snowflake'
        
        # Get model identifiers
        schema = model.get("schema", "public")
        table = model.get("alias", model.get("name", "unnamed"))
        
        # Check if we should wrap in Iceberg DDL
        use_iceberg = config.get("icebreaker_iceberg", True)
        cloud_bridge_type = getattr(credentials, 'cloud_bridge_type', None)
        
        if use_iceberg and cloud_bridge_type:
            # Build Iceberg configuration
            iceberg_config = IcebergConfig(
                schema=schema,
                table=table,
                catalog_integration=getattr(credentials, 'cloud_bridge_catalog_integration', None),
                external_volume=getattr(credentials, 'cloud_bridge_external_volume', None),
                partition_by=config.get("partition_by"),
            )
            
            # Wrap SQL in Iceberg DDL
            bridge = Bridge(cloud_bridge_type, credentials)
            wrapped_sql = bridge.construct_iceberg_ddl(sql, iceberg_config)
            
            # Execute on cloud
            self.connections.set_engine("cloud")
            response, result = self.connections.execute(wrapped_sql, fetch=False)
            
            # Log execution (cloud run, no savings)
            execution_time = time.time() - start_time
            try:
                log_execution(
                    model_name=model_name,
                    engine_used=cloud_type,
                    execution_time_seconds=execution_time,
                    rows_processed=0,
                    cloud_type=cloud_type,
                )
            except Exception:
                pass
            
            # Refresh catalog if configured
            catalog_type = getattr(credentials, 'catalog_type', None)
            if catalog_type:
                registrar = CatalogRegistrar(
                    catalog_type=catalog_type,
                    catalog_uri=getattr(credentials, 'catalog_uri', None),
                )
                registrar.refresh_table(schema, table)
            
            return f"OK (Cloud → Iceberg: {schema}.{table})", agate.Table([])
        else:
            # Direct cloud execution (no Iceberg wrapping)
            self.connections.set_engine("cloud")
            response, result = self.connections.execute(sql, fetch=True)
            
            # Log execution (cloud run, no savings)
            execution_time = time.time() - start_time
            rows = len(result) if result else 0
            try:
                log_execution(
                    model_name=model_name,
                    engine_used=cloud_type,
                    execution_time_seconds=execution_time,
                    rows_processed=rows,
                    cloud_type=cloud_type,
                )
            except Exception:
                pass
            
            table_result = self._result_to_agate(result)
            
            return "OK (Cloud)", table_result
    
    def _apply_dev_sampling(self, sql: str, config: Dict[str, Any]) -> str:
        """
        Apply sampling for development mode.
        
        Injects LIMIT clause to speed up dev iterations.
        """
        sample_size = config.get("dev_sample_size", 10000)
        
        # Simple approach: wrap in subquery with LIMIT
        # More sophisticated approach would use USING SAMPLE
        if "LIMIT" not in sql.upper():
            return f"SELECT * FROM ({sql}) AS __sampled LIMIT {sample_size}"
        return sql
    
    def _result_to_agate(self, result: Any) -> agate.Table:
        """Convert query result to agate table."""
        if result is None:
            return agate.Table([])
        
        # Simple conversion - result is a list of tuples
        if isinstance(result, list):
            if len(result) == 0:
                return agate.Table([])
            
            # Use generic column names if we don't have metadata
            num_cols = len(result[0]) if result else 0
            column_names = [f"col_{i}" for i in range(num_cols)]
            column_types = [agate.Text()] * num_cols
            
            return agate.Table(result, column_names, column_types)
        
        return agate.Table([])
    
    # =========================================================================
    # Required Adapter Methods
    # =========================================================================
    
    def get_columns_in_relation(self, relation: BaseRelation) -> List[Any]:
        """Get column information for a relation."""
        # Use DuckDB to introspect
        sql = f"DESCRIBE {relation}"
        self.connections.set_engine("duckdb")
        try:
            _, result = self.connections.execute(sql, fetch=True)
            return result or []
        except:
            return []
    
    def list_relations_without_caching(self, schema_relation: BaseRelation) -> List[BaseRelation]:
        """List all relations in a schema."""
        # DuckDB catalog query
        sql = f"""
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = '{schema_relation.schema}'
        """
        self.connections.set_engine("duckdb")
        try:
            _, result = self.connections.execute(sql, fetch=True)
            relations = []
            for row in (result or []):
                table_name, table_type = row[0], row[1]
                rel_type = RelationType.Table if table_type == "BASE TABLE" else RelationType.View
                relations.append(
                    self.Relation.create(
                        database=schema_relation.database,
                        schema=schema_relation.schema,
                        identifier=table_name,
                        type=rel_type,
                    )
                )
            return relations
        except:
            return []
    
    def create_schema(self, relation: BaseRelation) -> None:
        """Create a schema."""
        sql = f"CREATE SCHEMA IF NOT EXISTS {relation.schema}"
        self.connections.set_engine("duckdb")
        self.connections.execute(sql)
    
    def drop_schema(self, relation: BaseRelation) -> None:
        """Drop a schema."""
        sql = f"DROP SCHEMA IF EXISTS {relation.schema} CASCADE"
        self.connections.set_engine("duckdb")
        self.connections.execute(sql)
    
    def drop_relation(self, relation: BaseRelation) -> None:
        """Drop a relation."""
        sql = f"DROP TABLE IF EXISTS {relation}"
        self.connections.set_engine("duckdb")
        self.connections.execute(sql)
    
    def truncate_relation(self, relation: BaseRelation) -> None:
        """Truncate a relation."""
        sql = f"DELETE FROM {relation}"
        self.connections.set_engine("duckdb")
        self.connections.execute(sql)
    
    def rename_relation(self, from_relation: BaseRelation, to_relation: BaseRelation) -> None:
        """Rename a relation."""
        sql = f"ALTER TABLE {from_relation} RENAME TO {to_relation.identifier}"
        self.connections.set_engine("duckdb")
        self.connections.execute(sql)
    
    @classmethod
    def is_cancelable(cls) -> bool:
        return False
    
    def expand_column_types(self, goal: BaseRelation, current: BaseRelation) -> None:
        """Expand column types - not implemented."""
        pass
    
    def list_schemas(self, database: str) -> List[str]:
        """List all schemas in a database."""
        sql = "SELECT schema_name FROM information_schema.schemata"
        self.connections.set_engine("duckdb")
        try:
            _, result = self.connections.execute(sql, fetch=True)
            return [row[0] for row in (result or [])]
        except:
            return []
    
    def check_schema_exists(self, database: str, schema: str) -> bool:
        """Check if a schema exists."""
        schemas = self.list_schemas(database)
        return schema in schemas
