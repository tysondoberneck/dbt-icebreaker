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
