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
        
        # Transform Snowflake-specific functions
        statement = self._transform_snowflake_functions(statement)
        
        return statement
    
    def _transform_snowflake_functions(self, statement: exp.Expression) -> exp.Expression:
        """
        Transform Snowflake-specific functions to DuckDB equivalents.
        
        Handles:
        - LISTAGG → STRING_AGG
        - IFF → IF / CASE
        - NVL → COALESCE
        - NVL2 → CASE WHEN
        - TRY_* → TRY_CAST / error handling
        - OBJECT_CONSTRUCT → struct
        - PARSE_JSON → json()
        - ARRAY_CONSTRUCT → list
        - TO_VARIANT → cast to JSON
        """
        for func in list(statement.find_all(exp.Func)):
            func_name = func.sql_name().upper()
            
            # LISTAGG → STRING_AGG
            if func_name == "LISTAGG":
                self._transform_listagg(func)
            
            # IFF → IF (works in DuckDB)
            elif func_name == "IFF":
                self._transform_iff(func)
            
            # NVL → COALESCE
            elif func_name == "NVL":
                self._transform_nvl(func)
            
            # NVL2 → CASE WHEN
            elif func_name == "NVL2":
                self._transform_nvl2(func)
            
            # TRY_TO_* functions
            elif func_name.startswith("TRY_TO_"):
                self._transform_try_to(func)
            
            # OBJECT_CONSTRUCT → struct literal
            elif func_name == "OBJECT_CONSTRUCT":
                self._transform_object_construct(func)
            
            # PARSE_JSON → json() or cast
            elif func_name == "PARSE_JSON":
                self._transform_parse_json(func)
            
            # ARRAY_CONSTRUCT → list literal
            elif func_name == "ARRAY_CONSTRUCT":
                self._transform_array_construct(func)
            
            # TO_VARIANT → CAST to JSON
            elif func_name == "TO_VARIANT":
                self._transform_to_variant(func)
            
            # ZEROIFNULL → COALESCE(x, 0)
            elif func_name == "ZEROIFNULL":
                self._transform_zeroifnull(func)
            
            # IFNULL → COALESCE
            elif func_name == "IFNULL":
                self._transform_nvl(func)  # Same as NVL
        
        return statement
    
    def _transform_listagg(self, func: exp.Func):
        """Transform LISTAGG to STRING_AGG."""
        # LISTAGG(col, delimiter) → STRING_AGG(col, delimiter)
        # DuckDB's string_agg works similarly
        args = list(func.args.get("expressions", []))
        if args:
            string_agg = exp.Anonymous(
                this="STRING_AGG",
                expressions=args
            )
            func.replace(string_agg)
    
    def _transform_iff(self, func: exp.Func):
        """Transform IFF to IF (DuckDB syntax)."""
        # IFF(condition, true_val, false_val) → IF(condition, true_val, false_val)
        args = list(func.args.get("expressions", []))
        if len(args) >= 3:
            if_expr = exp.If(
                this=args[0],
                true=args[1],
                false=args[2]
            )
            func.replace(if_expr)
    
    def _transform_nvl(self, func: exp.Func):
        """Transform NVL to COALESCE."""
        # NVL(a, b) → COALESCE(a, b)
        args = list(func.args.get("expressions", []))
        if args:
            coalesce = exp.Coalesce(this=args[0], expressions=args[1:])
            func.replace(coalesce)
    
    def _transform_nvl2(self, func: exp.Func):
        """Transform NVL2 to CASE WHEN."""
        # NVL2(expr, not_null_val, null_val) → CASE WHEN expr IS NOT NULL THEN not_null_val ELSE null_val END
        args = list(func.args.get("expressions", []))
        if len(args) >= 3:
            case_expr = exp.Case(
                ifs=[
                    exp.If(
                        this=exp.Not(this=exp.Is(this=args[0], expression=exp.Null())),
                        true=args[1]
                    )
                ],
                default=args[2]
            )
            func.replace(case_expr)
    
    def _transform_try_to(self, func: exp.Func):
        """Transform TRY_TO_* functions to TRY_CAST."""
        # TRY_TO_NUMBER(x) → TRY_CAST(x AS DOUBLE)
        # TRY_TO_DATE(x) → TRY_CAST(x AS DATE)
        # etc.
        func_name = func.sql_name().upper()
        args = list(func.args.get("expressions", []))
        
        if not args:
            return
        
        # Map function names to target types
        type_map = {
            "TRY_TO_NUMBER": "DOUBLE",
            "TRY_TO_DECIMAL": "DECIMAL",
            "TRY_TO_NUMERIC": "DOUBLE",
            "TRY_TO_DOUBLE": "DOUBLE",
            "TRY_TO_DATE": "DATE",
            "TRY_TO_TIME": "TIME",
            "TRY_TO_TIMESTAMP": "TIMESTAMP",
            "TRY_TO_TIMESTAMP_NTZ": "TIMESTAMP",
            "TRY_TO_TIMESTAMP_LTZ": "TIMESTAMP",
            "TRY_TO_TIMESTAMP_TZ": "TIMESTAMP",
            "TRY_TO_BOOLEAN": "BOOLEAN",
            "TRY_TO_VARCHAR": "VARCHAR",
        }
        
        target_type = type_map.get(func_name, "VARCHAR")
        try_cast = exp.TryCast(
            this=args[0],
            to=exp.DataType.build(target_type)
        )
        func.replace(try_cast)
    
    def _transform_object_construct(self, func: exp.Func):
        """Transform OBJECT_CONSTRUCT to DuckDB struct."""
        # OBJECT_CONSTRUCT('key1', val1, 'key2', val2) → {'key1': val1, 'key2': val2}
        # DuckDB uses struct or map syntax
        args = list(func.args.get("expressions", []))
        
        # For now, use json_object which is more compatible
        json_obj = exp.Anonymous(
            this="JSON_OBJECT",
            expressions=args
        )
        func.replace(json_obj)
    
    def _transform_parse_json(self, func: exp.Func):
        """Transform PARSE_JSON to DuckDB JSON cast."""
        # PARSE_JSON(str) → str::JSON or JSON(str)
        args = list(func.args.get("expressions", []))
        if args:
            # Use CAST to JSON
            json_cast = exp.Cast(
                this=args[0],
                to=exp.DataType.build("JSON")
            )
            func.replace(json_cast)
    
    def _transform_array_construct(self, func: exp.Func):
        """Transform ARRAY_CONSTRUCT to DuckDB list."""
        # ARRAY_CONSTRUCT(a, b, c) → [a, b, c] or list_value(a, b, c)
        args = list(func.args.get("expressions", []))
        list_func = exp.Anonymous(
            this="LIST_VALUE",
            expressions=args
        )
        func.replace(list_func)
    
    def _transform_to_variant(self, func: exp.Func):
        """Transform TO_VARIANT to JSON cast."""
        # TO_VARIANT(x) → CAST(x AS JSON)
        args = list(func.args.get("expressions", []))
        if args:
            json_cast = exp.Cast(
                this=args[0],
                to=exp.DataType.build("JSON")
            )
            func.replace(json_cast)
    
    def _transform_zeroifnull(self, func: exp.Func):
        """Transform ZEROIFNULL to COALESCE(x, 0)."""
        # ZEROIFNULL(x) → COALESCE(x, 0)
        args = list(func.args.get("expressions", []))
        if args:
            coalesce = exp.Coalesce(
                this=args[0],
                expressions=[exp.Literal.number(0)]
            )
            func.replace(coalesce)
    
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
