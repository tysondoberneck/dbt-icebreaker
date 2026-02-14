"""
Icebreaker Error Classes.

Custom exceptions with actionable error messages that help users
understand what went wrong and how to fix it.
"""

from typing import Optional


class IcebreakerError(Exception):
    """Base class for all Icebreaker errors."""
    
    def __init__(
        self,
        message: str,
        suggestion: Optional[str] = None,
        docs_url: Optional[str] = None,
    ):
        self.message = message
        self.suggestion = suggestion
        self.docs_url = docs_url
        super().__init__(self.format())
    
    def format(self) -> str:
        """Format error with suggestion and docs link."""
        lines = [f"Error: {self.message}"]
        
        if self.suggestion:
            lines.append("")
            lines.append(f"Suggestion: {self.suggestion}")
        
        if self.docs_url:
            lines.append(f"Docs: {self.docs_url}")
        
        return "\n".join(lines)


# =============================================================================
# Connection Errors
# =============================================================================

class SnowflakeConnectionError(IcebreakerError):
    """Failed to connect to Snowflake."""
    
    def __init__(self, original_error: Optional[str] = None):
        message = "Could not connect to Snowflake"
        if original_error:
            message += f": {original_error}"
        
        super().__init__(
            message=message,
            suggestion=(
                "Check your Snowflake credentials:\n"
                "  1. SNOWFLAKE_ACCOUNT is set correctly (e.g., 'xy12345.us-east-1')\n"
                "  2. SNOWFLAKE_USER and SNOWFLAKE_PASSWORD are valid\n"
                "  3. Your IP is whitelisted in Snowflake network policies"
            ),
        )


class DuckDBConnectionError(IcebreakerError):
    """Failed to connect to local DuckDB."""
    
    def __init__(self, original_error: Optional[str] = None):
        message = "Could not initialize local DuckDB"
        if original_error:
            message += f": {original_error}"
        
        super().__init__(
            message=message,
            suggestion=(
                "Try these steps:\n"
                "  1. Check disk space: ~/.icebreaker/ needs write access\n"
                "  2. Close other DuckDB connections to the same file\n"
                "  3. Delete and recreate: rm -rf ~/.icebreaker/local.duckdb"
            ),
        )


class CatalogConnectionError(IcebreakerError):
    """Failed to connect to Iceberg catalog."""
    
    def __init__(self, catalog_url: str, original_error: Optional[str] = None):
        message = f"Could not connect to Iceberg catalog at {catalog_url}"
        if original_error:
            message += f": {original_error}"
        
        super().__init__(
            message=message,
            suggestion=(
                "Check your Iceberg catalog configuration:\n"
                "  1. Verify iceberg_catalog_url is correct\n"
                "  2. Ensure iceberg_token or iceberg_credential is valid\n"
                "  3. Check network connectivity to the catalog endpoint\n\n"
                "Note: Iceberg is optional. Remove iceberg_catalog_url from\n"
                "profiles.yml to use local caching instead."
            ),
        )


# =============================================================================
# Sync Errors
# =============================================================================

class SyncError(IcebreakerError):
    """Failed to sync data to cloud warehouse."""
    
    def __init__(self, table_name: str, original_error: Optional[str] = None):
        message = f"Failed to sync '{table_name}' to Snowflake"
        if original_error:
            message += f": {original_error}"
        
        super().__init__(
            message=message,
            suggestion=(
                f"Try manual sync:\n"
                f"  icebreaker sync {table_name}\n\n"
                f"Common issues:\n"
                f"  • Schema mismatch: Table structure changed\n"
                f"  • Permission denied: Check Snowflake role permissions\n"
                f"  • Network timeout: Retry the sync"
            ),
        )


class SyncVerificationError(IcebreakerError):
    """Sync succeeded but verification failed."""
    
    def __init__(self, table_name: str, local_count: int, remote_count: int):
        diff = abs(local_count - remote_count)
        diff_pct = (diff / max(local_count, 1)) * 100
        
        super().__init__(
            message=(
                f"Row count mismatch for '{table_name}':\n"
                f"  Local:  {local_count:,}\n"
                f"  Remote: {remote_count:,}\n"
                f"  Diff:   {diff:,} rows ({diff_pct:.1f}%)"
            ),
            suggestion=(
                "Row counts may differ due to:\n"
                "  1. Concurrent writes to Snowflake table\n"
                "  2. Sync still in progress (wait and re-check)\n"
                "  3. Use 'icebreaker verify' to re-check"
            ),
        )


# =============================================================================
# Cache Errors
# =============================================================================

class CacheError(IcebreakerError):
    """Error with local source cache."""
    
    def __init__(self, operation: str, original_error: Optional[str] = None):
        message = f"Cache {operation} failed"
        if original_error:
            message += f": {original_error}"
        
        super().__init__(
            message=message,
            suggestion=(
                "Try clearing and refreshing the cache:\n"
                "  icebreaker cache clear\n"
                "  icebreaker cache refresh"
            ),
        )


class CacheMissError(IcebreakerError):
    """Source table not in cache and cannot be downloaded."""
    
    def __init__(self, table_ref: str, original_error: Optional[str] = None):
        message = f"Source table '{table_ref}' not cached"
        if original_error:
            message += f" and download failed: {original_error}"
        
        super().__init__(
            message=message,
            suggestion=(
                f"Ensure the source table exists in Snowflake:\n"
                f"  1. Verify '{table_ref}' exists in your Snowflake database\n"
                f"  2. Check your Snowflake credentials have SELECT access\n"
                f"  3. Run 'icebreaker cache refresh' to retry"
            ),
        )


# =============================================================================
# Routing Errors
# =============================================================================

class RoutingError(IcebreakerError):
    """Error determining execution venue."""
    
    def __init__(self, model_name: str, original_error: Optional[str] = None):
        message = f"Could not determine routing for model '{model_name}'"
        if original_error:
            message += f": {original_error}"
        
        super().__init__(
            message=message,
            suggestion=(
                f"Override routing manually in your model config:\n"
                f"  {{{{ config(icebreaker_route='cloud') }}}}\n"
                f"  or\n"
                f"  {{{{ config(icebreaker_route='local') }}}}"
            ),
        )


class LocalExecutionError(IcebreakerError):
    """Model failed during local execution."""
    
    def __init__(self, model_name: str, original_error: Optional[str] = None):
        message = f"Local execution failed for '{model_name}'"
        if original_error:
            message += f": {original_error}"
        
        # Detect common issues
        suggestion_parts = []
        
        if original_error:
            error_lower = original_error.lower()
            
            if "memory" in error_lower or "oom" in error_lower:
                suggestion_parts.append(
                    "Memory issue detected. Try:\n"
                    "  • Reduce max_local_size_gb in profiles.yml\n"
                    "  • Add {{ config(icebreaker_route='cloud') }} to run on Snowflake"
                )
            elif "syntax" in error_lower or "parse" in error_lower:
                suggestion_parts.append(
                    "SQL syntax error. This might be Snowflake-specific SQL.\n"
                    "  • Use {{ config(icebreaker_route='cloud') }} to run on Snowflake\n"
                    "  • Or refactor SQL to use ANSI-compatible syntax"
                )
            elif "not found" in error_lower or "does not exist" in error_lower:
                suggestion_parts.append(
                    "Table/column not found. Ensure:\n"
                    "  • Source tables are cached: icebreaker cache status\n"
                    "  • Upstream models have been run first"
                )
        
        if not suggestion_parts:
            suggestion_parts.append(
                "Try running on cloud instead:\n"
                "  {{ config(icebreaker_route='cloud') }}"
            )
        
        super().__init__(
            message=message,
            suggestion="\n".join(suggestion_parts),
        )


# =============================================================================
# Transpilation Errors
# =============================================================================

class TranspilationError(IcebreakerError):
    """SQL could not be transpiled from Snowflake to DuckDB."""
    
    def __init__(self, sql_snippet: str, original_error: Optional[str] = None):
        # Truncate SQL for display
        truncated = sql_snippet[:100] + "..." if len(sql_snippet) > 100 else sql_snippet
        
        message = f"Could not transpile SQL to DuckDB-compatible syntax"
        if original_error:
            message += f": {original_error}"
        
        super().__init__(
            message=message,
            suggestion=(
                f"The following SQL uses Snowflake-specific features:\n"
                f"  {truncated}\n\n"
                f"Options:\n"
                f"  1. Route to cloud: {{{{ config(icebreaker_route='cloud') }}}}\n"
                f"  2. Use ANSI SQL equivalents where possible\n"
                f"  3. Use Icebreaker's compatibility macros (coming soon)"
            ),
        )
