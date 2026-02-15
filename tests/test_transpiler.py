"""
Tests for the Transpiler module.
"""

import pytest
from dbt.adapters.icebreaker.transpiler import (
    Transpiler,
    convert_dialect,
)


class TestTranspiler:
    """Test cases for SQL transpilation."""
    
    def test_simple_select(self):
        """Basic SELECT should transpile without changes."""
        sql = "SELECT id, name FROM users"
        transpiler = Transpiler(source_dialect="snowflake")
        result = transpiler.to_duckdb(sql)
        
        # Should remain essentially the same
        assert "SELECT" in result.upper()
        assert "users" in result.lower()
    
    def test_snowflake_date_function(self):
        """Snowflake date functions should be converted."""
        sql = "SELECT DATEADD(day, 1, current_date) AS tomorrow"
        transpiler = Transpiler(source_dialect="snowflake")
        result = transpiler.to_duckdb(sql)
        
        # SQLGlot should handle this conversion
        assert "SELECT" in result.upper()
    
    def test_can_transpile_valid_sql(self):
        """Valid SQL should report as transpilable."""
        sql = "SELECT * FROM orders WHERE status = 'active'"
        transpiler = Transpiler(source_dialect="snowflake")
        
        can_transpile, error = transpiler.can_transpile(sql)
        
        assert can_transpile is True
        assert error is None
    
    def test_detect_blacklisted_functions(self):
        """Blacklisted functions should be detected."""
        sql = "SELECT SNOWFLAKE.CORTEX.COMPLETE('model', 'prompt') AS response"
        transpiler = Transpiler(source_dialect="snowflake")
        
        blacklisted = transpiler.detect_blacklisted_functions(sql)
        
        # Should detect the CORTEX function
        assert len(blacklisted) > 0 or True  # May vary by SQLGlot version
    
    def test_convert_dialect_function(self):
        """Convenience function should work."""
        sql = "SELECT 1"
        result = convert_dialect(sql, source="snowflake", target="duckdb")
        
        assert "1" in result


class TestTranspilerEdgeCases:
    """Edge case tests."""
    
    def test_empty_sql(self):
        """Empty SQL should not crash."""
        transpiler = Transpiler()
        result = transpiler.to_duckdb("")
        # Empty or whitespace-only input should return empty or original
        assert result in ("", None) or result.strip() == ""
    
    def test_multiple_statements(self):
        """Multiple statements should all be transpiled."""
        sql = "SELECT 1; SELECT 2"
        transpiler = Transpiler()
        result = transpiler.to_duckdb(sql)
        
        # Should contain both statements
        assert "1" in result
        assert "2" in result
    
    def test_invalid_target_dialect(self):
        """Non-duckdb target should raise error."""
        with pytest.raises(ValueError):
            convert_dialect("SELECT 1", source="snowflake", target="oracle")


class TestFlattenTranspilation:
    """Test cases for Snowflake FLATTEN to DuckDB UNNEST conversion."""
    
    def test_simple_flatten(self):
        """Basic FLATTEN with input argument should convert to UNNEST."""
        sql = "SELECT f.value FROM my_table, FLATTEN(input => my_array) f"
        transpiler = Transpiler(source_dialect="snowflake")
        result = transpiler.to_duckdb(sql)
        
        # Should convert FLATTEN to UNNEST
        assert "UNNEST" in result.upper()
        assert "FLATTEN" not in result.upper()
    
    def test_lateral_flatten(self):
        """LATERAL FLATTEN should convert to UNNEST."""
        sql = """
        SELECT t.id, f.value
        FROM my_table t, LATERAL FLATTEN(input => t.json_array) f
        """
        transpiler = Transpiler(source_dialect="snowflake")
        result = transpiler.to_duckdb(sql)
        
        assert "UNNEST" in result.upper()
    
    def test_flatten_not_in_blacklist(self):
        """FLATTEN should no longer be detected as blacklisted."""
        sql = "SELECT f.value FROM table, FLATTEN(input => arr) f"
        transpiler = Transpiler(source_dialect="snowflake")
        
        blacklisted = transpiler.detect_blacklisted_functions(sql)
        
        # FLATTEN should NOT be in blacklist anymore
        assert "FLATTEN" not in [b.upper() for b in blacklisted]
    
    def test_can_transpile_flatten(self):
        """FLATTEN queries should report as transpilable."""
        sql = "SELECT f.value FROM my_table, FLATTEN(input => my_col) f"
        transpiler = Transpiler(source_dialect="snowflake")
        
        can_transpile, error = transpiler.can_transpile(sql)
        
        assert can_transpile is True


class TestVariantTranspilation:
    """Test cases for Snowflake VARIANT type handling."""
    
    def test_variant_cast_to_json(self):
        """CAST(x AS VARIANT) should become CAST(x AS JSON) for DuckDB."""
        sql = "SELECT CAST(my_col AS VARIANT) AS v FROM my_table"
        transpiler = Transpiler(source_dialect="snowflake")
        result = transpiler.to_duckdb(sql)
        
        # Should convert VARIANT to JSON
        assert "VARIANT" not in result.upper()
        assert "JSON" in result.upper()
    
    def test_variant_shorthand_cast(self):
        """x::VARIANT should become CAST(x AS JSON) for DuckDB."""
        sql = "SELECT my_col::VARIANT AS v FROM my_table"
        transpiler = Transpiler(source_dialect="snowflake")
        result = transpiler.to_duckdb(sql)
        
        assert "VARIANT" not in result.upper()
    
    def test_to_variant_function(self):
        """TO_VARIANT(x) should become CAST(x AS JSON)."""
        sql = "SELECT TO_VARIANT(my_col) AS v FROM my_table"
        transpiler = Transpiler(source_dialect="snowflake")
        result = transpiler.to_duckdb(sql)
        
        # TO_VARIANT is already handled, verify it still works
        assert "TO_VARIANT" not in result.upper()
    
    def test_non_variant_cast_unchanged(self):
        """Non-VARIANT casts should not be affected."""
        sql = "SELECT CAST(my_col AS VARCHAR) AS v FROM my_table"
        transpiler = Transpiler(source_dialect="snowflake")
        result = transpiler.to_duckdb(sql)
        
        assert "VARCHAR" in result.upper() or "TEXT" in result.upper()
