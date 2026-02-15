"""
Custom Relation for Icebreaker

Handles DuckDB relations without database prefix (DuckDB uses schema.table only).
"""

from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import Policy


@dataclass
class IcebreakerQuotePolicy(Policy):
    """DuckDB-friendly quote policy - no quoting by default."""
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class IcebreakerIncludePolicy(Policy):
    """DuckDB uses schema.table only, no database component."""
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class IcebreakerRelation(BaseRelation):
    """
    Icebreaker-specific relation that:
    - Excludes database from rendering (DuckDB uses schema.table)
    - Avoids empty quoted identifiers
    """
    
    @classmethod
    def get_default_quote_policy(cls) -> Policy:
        """Return DuckDB-friendly quote policy - no quoting."""
        return IcebreakerQuotePolicy()
    
    @classmethod
    def get_default_include_policy(cls) -> Policy:
        """Return DuckDB-friendly include policy - no database."""
        return IcebreakerIncludePolicy()
    
    def render(self) -> str:
        """Render the relation for DuckDB SQL without database prefix."""
        # For DuckDB: just schema.identifier (no database)
        parts = []
        
        if self.include_policy.schema and self.schema:
            parts.append(self._render_component("schema", self.schema))
        if self.include_policy.identifier and self.identifier:
            parts.append(self._render_component("identifier", self.identifier))
        
        if not parts:
            return ""
        
        return ".".join(parts)
    
    def _render_component(self, component_name: str, value: str) -> str:
        """Render a single component with optional quoting."""
        # Check if this component should be quoted
        should_quote = getattr(self.quote_policy, component_name, False)
        
        if should_quote:
            return f'"{value}"'
        return value
