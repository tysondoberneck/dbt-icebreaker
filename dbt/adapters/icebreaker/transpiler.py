"""
SQL Transpiler

Uses SQLGlot to convert SQL between dialects.
Primary use case: Snowflake SQL -> DuckDB SQL for local execution.
"""

from typing import Optional
import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError


# Mapping from profile dialect names to SQLGlot dialect names
# MVP: Snowflake and DuckDB only
DIALECT_MAP = {
    "snowflake": "snowflake",
    "duckdb": "duckdb",
}


class TranspilationError(Exception):
    """Raised when SQL cannot be transpiled."""
    pass


class Transpiler:
    """
    SQL dialect transpiler using SQLGlot.
    
    Handles conversion from cloud SQL dialects to DuckDB for local execution.
    """
    
    def __init__(self, source_dialect: str = "snowflake"):
        self.source_dialect = DIALECT_MAP.get(source_dialect, source_dialect)
    
    def to_duckdb(self, sql: str) -> str:
        """
        Convert SQL from source dialect to DuckDB.
        
        Args:
            sql: SQL string in the source dialect
            
        Returns:
            SQL string in DuckDB dialect
            
        Raises:
            TranspilationError: If the SQL cannot be converted
        """
        # Handle empty/whitespace-only SQL
        if not sql or not sql.strip():
            return ""
        
        try:
            # Parse the SQL
            parsed = sqlglot.parse(sql, dialect=self.source_dialect)
            
            if not parsed:
                return sql  # Return as-is if nothing to parse
            
            # Transpile each statement, skipping None entries
            result_parts = []
            for statement in parsed:
                if statement is None:
                    continue
                    
                # Apply DuckDB-specific transformations
                statement = self._apply_transforms(statement)
                
                # Generate DuckDB SQL
                duckdb_sql = statement.sql(dialect="duckdb")
                result_parts.append(duckdb_sql)
            
            return ";\n".join(result_parts)
            
        except ParseError as e:
            raise TranspilationError(f"Failed to parse SQL: {e}")
        except Exception as e:
            raise TranspilationError(f"Failed to transpile SQL: {e}")
    
    def _apply_transforms(self, statement: exp.Expression) -> exp.Expression:
        """Apply DuckDB-specific transformations to the AST."""
        
        # Transform LATERAL FLATTEN to UNNEST
        statement = self._transform_flatten(statement)
        
        # Transform QUALIFY clauses (DuckDB handles differently)
        statement = self._transform_qualify(statement)
        
        # Transform date functions
        statement = self._transform_date_functions(statement)
        
        # Transform JSON extraction
        statement = self._transform_json(statement)
        
        return statement
    
    def _transform_flatten(self, statement: exp.Expression) -> exp.Expression:
        """
        Transform Snowflake LATERAL FLATTEN to DuckDB UNNEST.
        
        Snowflake: SELECT * FROM table, LATERAL FLATTEN(input => array_col) f
        DuckDB:    SELECT * FROM table, UNNEST(array_col) AS f(value)
        
        Handles common FLATTEN patterns:
        - FLATTEN(input => col)
        - FLATTEN(input => col, path => 'field')
        - LATERAL FLATTEN(...)
        """
        # Find all FLATTEN function calls and transform them
        for flatten_node in list(statement.find_all(exp.Func)):
            func_name = flatten_node.sql_name().upper()
            if func_name == "FLATTEN":
                # Extract the input argument
                args = list(flatten_node.args.get("expressions", []))
                
                # Look for 'input =>' named argument (Snowflake style)
                input_col = None
                for arg in args:
                    if isinstance(arg, exp.EQ):
                        left = arg.left
                        if hasattr(left, 'name') and left.name.upper() == 'INPUT':
                            input_col = arg.right
                            break
                    else:
                        # Positional argument - first arg is the input
                        if input_col is None:
                            input_col = arg
                
                if input_col:
                    # Create UNNEST expression
                    unnest = exp.Unnest(expressions=[input_col])
                    # Replace FLATTEN with UNNEST
                    flatten_node.replace(unnest)
        
        return statement
    
    def _transform_qualify(self, statement: exp.Expression) -> exp.Expression:
        """Transform QUALIFY clauses for DuckDB compatibility."""
        # SQLGlot handles most QUALIFY transformations automatically
        # This is a hook for any custom handling needed
        return statement
    
    def _transform_date_functions(self, statement: exp.Expression) -> exp.Expression:
        """Transform date functions for DuckDB compatibility."""
        # Most common transformations are handled by SQLGlot
        # Add custom transforms here as needed
        return statement
    
    def _transform_json(self, statement: exp.Expression) -> exp.Expression:
        """Transform JSON extraction for DuckDB compatibility."""
        # DuckDB uses -> and ->> like Postgres
        # Snowflake uses : notation
        # SQLGlot handles this automatically
        return statement
    
    def can_transpile(self, sql: str) -> tuple[bool, Optional[str]]:
        """
        Check if SQL can be transpiled to DuckDB.
        
        Returns:
            Tuple of (can_transpile, error_message)
        """
        try:
            self.to_duckdb(sql)
            return True, None
        except TranspilationError as e:
            return False, str(e)
    
    def detect_blacklisted_functions(self, sql: str) -> list[str]:
        """
        Detect functions that cannot run locally.
        
        Returns:
            List of blacklisted function names found in the SQL
        """
        # Functions that are cloud-only
        BLACKLIST = {
            # Snowflake ML/AI
            "SNOWFLAKE.CORTEX",
            "ML.PREDICT",
            "ML.EXPLAIN",
            # Snowflake proprietary (FLATTEN is now supported via UNNEST transform)
            "PARSE_XML",
            "XMLGET",
            "GET_DDL",
            "SYSTEM$",
            # BigQuery ML
            "ML.EVALUATE",
            "ML.TRAINING_INFO",
        }
        
        found = []
        try:
            parsed = sqlglot.parse(sql, dialect=self.source_dialect)
            for statement in parsed:
                for func in statement.find_all(exp.Func):
                    func_name = func.sql_name().upper()
                    for blacklisted in BLACKLIST:
                        if blacklisted in func_name:
                            found.append(func_name)
        except:
            pass  # If we can't parse, we'll catch it during transpilation
        
        return found


def convert_dialect(
    sql: str,
    source: str = "snowflake",
    target: str = "duckdb"
) -> str:
    """
    Convenience function to convert SQL between dialects.
    
    Args:
        sql: SQL string to convert
        source: Source dialect name
        target: Target dialect name (currently only 'duckdb' supported)
        
    Returns:
        Converted SQL string
    """
    if target != "duckdb":
        raise ValueError(f"Only 'duckdb' target is supported, got: {target}")
    
    transpiler = Transpiler(source_dialect=source)
    return transpiler.to_duckdb(sql)
