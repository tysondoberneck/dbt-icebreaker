"""
Automatic SQL Router for Icebreaker.

Analyzes SQL to automatically determine optimal execution venue (LOCAL vs CLOUD).
No manual tags required - routing is based on:
1. External data source detection
2. Cloud-only SQL function detection
3. Upstream dependency analysis
4. Data volume estimation
"""

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Set
from enum import Enum


VenueType = Literal["LOCAL", "CLOUD"]


class RoutingReason(Enum):
    """Reasons for routing decisions."""
    # Cloud routing reasons
    EXTERNAL_SOURCE = "External data source detected"
    CLOUD_FUNCTION = "Cloud-only SQL function"
    CLOUD_DEPENDENCY = "Upstream dependency requires cloud"
    VOLUME_EXCEEDS_LIMIT = "Data volume exceeds local threshold"
    MEMORY_CONSTRAINT = "Estimated memory exceeds available"
    USER_OVERRIDE = "User configured icebreaker_route"
    PREVIOUS_FAILURE = "Previously failed on local execution"
    
    # Local routing reasons
    AUTO_LOCAL = "Automatic routing (free compute)"
    USER_OVERRIDE_LOCAL = "User configured icebreaker_route='local'"
    ICEBERG_LOCAL = "Iceberg catalog source (DuckDB-native)"


@dataclass
class RoutingDecision:
    """Result of an automatic routing decision."""
    venue: VenueType
    reason: RoutingReason
    details: Optional[str] = None
    confidence: float = 1.0  # 0-1 confidence in decision
    
    def __str__(self) -> str:
        icon = "☁️" if self.venue == "CLOUD" else "🏠"
        detail_str = f" ({self.details})" if self.details else ""
        return f"{icon} {self.venue}: {self.reason.value}{detail_str}"


# =============================================================================
# Cloud-Only Functions
# =============================================================================

# Functions that MUST run on cloud warehouses (MVP: Snowflake only)
CLOUD_ONLY_FUNCTIONS = {
    # Snowflake ML/AI
    "snowflake.ml",
    "snowflake.cortex",
    "cortex.complete",
    "cortex.sentiment",
    "cortex.summarize",
    "cortex.translate",
    "cortex.extract_answer",
    
    # Snowflake semi-structured (complex patterns)
    # Note: LATERAL FLATTEN is now supported via transpilation to UNNEST
    "get_path",
    "xmlget",
    "parse_xml",
    
    # Snowflake streams and tasks
    "system$stream_has_data",
    "create stream",
    "create task",
    
    # Snowflake Geo (complex)
    "st_asgeojson",
    "st_geogfromtext",
    "st_makepolygon",
    "geography",
    
    # External functions
    "external_function",
    "invoke ",
}

# Patterns that indicate external data access
EXTERNAL_SOURCE_PATTERNS = [
    # Snowflake stages
    r"@[\w\.]+/",  # @stage/path
    r"from\s+@",   # FROM @stage
    
    # Cross-database references (3-part names)
    r"(\w+)\.(\w+)\.(\w+)",  # db.schema.table
    
    # Direct cloud storage URLs
    r"s3://[\w\-\.]+/",
    r"gs://[\w\-\.]+/",
    r"azure://[\w\-\.]+/",
    r"abfss?://[\w\-\.]+/",
    
    # HTTP endpoints
    r"https?://[\w\-\.]+/",
    
    # Snowflake data sharing
    r"share\.",
    r"snowflake\.account_usage",
    r"snowflake\.organization_usage",
    
    # External tables
    r"external_table",
    r"copy\s+into",
]

# SQL features that DuckDB handles well (safe for local)
DUCKDB_SAFE_FUNCTIONS = {
    "count", "sum", "avg", "min", "max",
    "row_number", "rank", "dense_rank", "ntile",
    "lead", "lag", "first_value", "last_value",
    "coalesce", "nullif", "ifnull",
    "case", "when", "then", "else",
    "cast", "try_cast", "convert",
    "concat", "substring", "trim", "lower", "upper",
    "date_trunc", "dateadd", "datediff", "extract",
    "json_extract", "json_extract_string",
    "array_agg", "list_agg", "string_agg",
    "regexp_matches", "regexp_replace",
}


class AutoRouter:
    """
    Automatic SQL-based routing engine.
    
    Analyzes SQL and model metadata to determine whether to run
    on LOCAL (DuckDB) or CLOUD (Snowflake/BigQuery/etc).
    """
    
    def __init__(
        self,
        max_local_gb: float = 5.0,
        catalog_scanner: Optional[Any] = None,
        routing_history: Optional[Dict] = None,
    ):
        self.max_local_gb = max_local_gb
        self.catalog = catalog_scanner
        self.history = routing_history or {}
        
        # Pre-compile regex patterns for performance
        self._external_patterns = [
            re.compile(p, re.IGNORECASE) 
            for p in EXTERNAL_SOURCE_PATTERNS
        ]
        
        # Iceberg catalog pattern
        self._iceberg_pattern = re.compile(r'\biceberg_catalog\.\w+\.\w+', re.IGNORECASE)
    
    def _is_iceberg_catalog_source(self, sql: str) -> bool:
        """
        Check if SQL references the Iceberg catalog (iceberg_catalog.schema.table).
        
        Iceberg catalog sources can be read locally by DuckDB's Iceberg extension,
        so they should be routed to LOCAL, not CLOUD.
        """
        return bool(self._iceberg_pattern.search(sql))
    
    def decide(
        self,
        sql: str,
        model: Dict[str, Any],
        sources: Optional[List[Dict]] = None,
    ) -> RoutingDecision:
        """
        Main routing decision method.
        
        Args:
            sql: The compiled SQL for the model
            model: The dbt model node dictionary
            sources: Optional list of source metadata
            
        Returns:
            RoutingDecision with venue and reason
        """
        config = model.get("config", {})
        model_name = model.get("name", "unknown")
        
        # 1. Check for user override (still supported, just rarely needed)
        explicit_route = config.get("icebreaker_route")
        if explicit_route:
            if explicit_route.lower() == "cloud":
                return RoutingDecision(
                    venue="CLOUD",
                    reason=RoutingReason.USER_OVERRIDE,
                    details="icebreaker_route='cloud'",
                )
            elif explicit_route.lower() == "local":
                return RoutingDecision(
                    venue="LOCAL",
                    reason=RoutingReason.USER_OVERRIDE_LOCAL,
                    details="icebreaker_route='local'",
                )
        
        # 2. Check for previous local failures
        if self._has_local_failures(model_name):
            return RoutingDecision(
                venue="CLOUD",
                reason=RoutingReason.PREVIOUS_FAILURE,
                details="Local execution failed previously",
                confidence=0.9,
            )
        
        # 3. Check for external data sources
        external = self._detect_external_sources(sql, sources)
        if external:
            return RoutingDecision(
                venue="CLOUD",
                reason=RoutingReason.EXTERNAL_SOURCE,
                details=external,
            )
        
        # 4. Check for cloud-only SQL functions
        cloud_func = self._detect_cloud_functions(sql)
        if cloud_func:
            return RoutingDecision(
                venue="CLOUD",
                reason=RoutingReason.CLOUD_FUNCTION,
                details=cloud_func,
            )
        
        # 5. Check upstream dependencies
        cloud_dep = self._check_cloud_dependencies(model)
        if cloud_dep:
            return RoutingDecision(
                venue="CLOUD",
                reason=RoutingReason.CLOUD_DEPENDENCY,
                details=cloud_dep,
                confidence=0.8,
            )
        
        # 6. Check data volume
        if self.catalog:
            volume_gb = self.catalog.estimate_input_volume(model)
            if volume_gb > self.max_local_gb:
                return RoutingDecision(
                    venue="CLOUD",
                    reason=RoutingReason.VOLUME_EXCEEDS_LIMIT,
                    details=f"{volume_gb:.1f}GB > {self.max_local_gb}GB limit",
                )
        
        # All checks passed - run locally!
        return RoutingDecision(
            venue="LOCAL",
            reason=RoutingReason.AUTO_LOCAL,
            details="Passed all routing checks",
        )
    
    def _detect_external_sources(
        self, 
        sql: str, 
        sources: Optional[List[Dict]] = None
    ) -> Optional[str]:
        """
        Detect if SQL accesses external data sources.
        
        Returns description of external source if found, None otherwise.
        
        NOTE: iceberg_catalog.* references are NOT external - they can be read
        locally by DuckDB's Iceberg extension.
        """
        sql_upper = sql.upper()
        
        # First, check if this is an Iceberg catalog source (these are LOCAL-ready)
        if self._is_iceberg_catalog_source(sql):
            return None  # Not external - can run locally!
        
        # Check regex patterns
        for pattern in self._external_patterns:
            match = pattern.search(sql)
            if match:
                matched_text = match.group(0)[:50]  # Truncate for display
                # Skip if it's an iceberg_catalog reference
                if 'iceberg_catalog' in matched_text.lower():
                    continue
                return f"Pattern: {matched_text}"
        
        # Check source metadata
        if sources:
            for source in sources:
                meta = source.get("meta", {})
                
                # Iceberg sources are local-ready
                if meta.get("iceberg") or meta.get("is_iceberg"):
                    continue
                
                # External table marker
                if meta.get("external") or meta.get("is_external"):
                    return f"Source '{source.get('name')}' is external"
                
                # Cross-database source (but not iceberg_catalog)
                db = source.get("database", "")
                if db and db != "memory" and db != "iceberg_catalog":
                    return f"Source '{source.get('name')}' is cross-database"
                
                # Explicit external format
                if meta.get("format") in ("external", "stage", "s3", "gcs"):
                    return f"Source '{source.get('name')}' format is {meta.get('format')}"
        
        return None
    
    def _detect_cloud_functions(self, sql: str) -> Optional[str]:
        """
        Detect cloud-only SQL functions.
        
        Returns function name if found, None otherwise.
        """
        sql_lower = sql.lower()
        
        for func in CLOUD_ONLY_FUNCTIONS:
            if func.lower() in sql_lower:
                return f"Function: {func}"
        
        # Check for Snowflake semi-structured access patterns
        # e.g., column:field::type or column['field']
        if re.search(r'\w+:\w+::\w+', sql):
            return "Snowflake semi-structured syntax (col:field::type)"
        
        if re.search(r"\w+\['\w+'\]", sql):
            return "Snowflake variant access (col['field'])"
        
        return None
    
    def _check_cloud_dependencies(self, model: Dict[str, Any]) -> Optional[str]:
        """
        Check if any upstream dependencies require cloud.
        
        Returns dependency name if cloud-only found, None otherwise.
        """
        depends_on = model.get("depends_on", {})
        nodes = depends_on.get("nodes", [])
        
        for node_id in nodes:
            # Check if it's a source (often external)
            if node_id.startswith("source."):
                # Sources that reference external databases need cloud
                # This would be enhanced with manifest lookup
                pass
            
            # Check our routing history
            if node_id in self.history:
                if self.history[node_id].get("venue") == "CLOUD":
                    if self.history[node_id].get("reason") in [
                        "EXTERNAL_SOURCE", "CLOUD_FUNCTION"
                    ]:
                        return f"Depends on cloud-only: {node_id.split('.')[-1]}"
        
        return None
    
    def _has_local_failures(self, model_name: str) -> bool:
        """Check if model has previously failed on local execution."""
        if model_name in self.history:
            return self.history[model_name].get("local_failures", 0) > 0
        return False
    
    def explain(self, sql: str, model: Dict[str, Any]) -> str:
        """
        Generate human-readable explanation of routing decision.
        
        Useful for debugging and the `icebreaker explain` CLI command.
        """
        decision = self.decide(sql, model)
        
        lines = [
            f"Model: {model.get('name', 'unknown')}",
            f"Decision: {decision}",
            "",
            "Analysis:",
        ]
        
        # External sources
        external = self._detect_external_sources(sql, None)
        lines.append(f"  External sources: {'✗ ' + external if external else '✓ None detected'}")
        
        # Cloud functions
        cloud_func = self._detect_cloud_functions(sql)
        lines.append(f"  Cloud functions: {'✗ ' + cloud_func if cloud_func else '✓ None detected'}")
        
        # Dependencies
        cloud_dep = self._check_cloud_dependencies(model)
        lines.append(f"  Cloud dependencies: {'✗ ' + cloud_dep if cloud_dep else '✓ All local-compatible'}")
        
        # Volume
        if self.catalog:
            volume = self.catalog.estimate_input_volume(model)
            status = "✗" if volume > self.max_local_gb else "✓"
            lines.append(f"  Estimated volume: {status} {volume:.2f}GB (limit: {self.max_local_gb}GB)")
        else:
            lines.append(f"  Estimated volume: ? (no catalog available)")
        
        return "\n".join(lines)


# =============================================================================
# Singleton & Convenience Functions
# =============================================================================

_router: Optional[AutoRouter] = None


def get_router(
    max_local_gb: float = 5.0,
    catalog_scanner: Optional[Any] = None,
) -> AutoRouter:
    """Get or create the auto router singleton."""
    global _router
    if _router is None:
        _router = AutoRouter(
            max_local_gb=max_local_gb,
            catalog_scanner=catalog_scanner,
        )
    return _router


def decide_venue(
    sql: str,
    model: Dict[str, Any],
    sources: Optional[List[Dict]] = None,
) -> RoutingDecision:
    """Convenience function for routing decision."""
    return get_router().decide(sql, model, sources)
