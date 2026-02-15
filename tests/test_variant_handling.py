"""
Tests for VARIANT column handling across the adapter.

Tests both the connection fallback detection and the source cache
VARIANT column detection/casting.
"""

from unittest.mock import MagicMock


class TestDuckDBIncompatibilityDetection:
    """Test the _is_duckdb_incompatibility error classifier."""
    
    def _check(self, error_str: str) -> bool:
        from dbt.adapters.icebreaker.connections import IcebreakerConnectionManager
        return IcebreakerConnectionManager._is_duckdb_incompatibility(error_str)
    
    def test_function_not_found(self):
        """Function-not-found errors should trigger fallback."""
        assert self._check("Scalar Function 'CONVERT_TIMEZONE' does not exist")
    
    def test_variant_not_implemented(self):
        """VARIANT type errors should trigger fallback."""
        assert self._check(
            "DuckDB error: Not implemented Error: A table cannot be created from a VARIANT column yet"
        )
    
    def test_general_not_implemented(self):
        """General 'Not implemented Error' should trigger fallback."""
        assert self._check("Not implemented Error: some feature")
    
    def test_regular_error_no_fallback(self):
        """Regular errors should NOT trigger fallback."""
        assert not self._check("Table 'my_table' does not exist")
    
    def test_syntax_error_no_fallback(self):
        """Syntax errors should NOT trigger fallback."""
        assert not self._check("Parser Error: syntax error at or near 'SELECTT'")


class TestSourceCacheVariantDetection:
    """Test the SourceCache VARIANT column helpers."""
    
    def test_get_variant_columns_detects_variant(self):
        """Should detect VARIANT columns from INFORMATION_SCHEMA."""
        from dbt.adapters.icebreaker.source_cache import SourceCache, CacheConfig
        
        cache = SourceCache(config=CacheConfig(cache_enabled=False))
        
        # Mock cursor that returns VARIANT column
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("FLOW_DOCUMENT", "VARIANT"),
            ("METADATA", "OBJECT"),
        ]
        
        result = cache._get_variant_columns("DB", "SCHEMA", "TABLE", mock_cursor)
        
        assert "FLOW_DOCUMENT" in result
        assert "METADATA" in result
        assert len(result) == 2
    
    def test_get_variant_columns_empty_when_none(self):
        """Should return empty list when no VARIANT columns exist."""
        from dbt.adapters.icebreaker.source_cache import SourceCache, CacheConfig
        
        cache = SourceCache(config=CacheConfig(cache_enabled=False))
        
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        
        result = cache._get_variant_columns("DB", "SCHEMA", "TABLE", mock_cursor)
        
        assert result == []
    
    def test_build_select_casts_variant_columns(self):
        """Should cast VARIANT columns to VARCHAR in SELECT."""
        from dbt.adapters.icebreaker.source_cache import SourceCache, CacheConfig
        
        cache = SourceCache(config=CacheConfig(cache_enabled=False))
        
        # Mock cursor to return all columns
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("ID",),
            ("NAME",),
            ("FLOW_DOCUMENT",),
        ]
        
        result = cache._build_select_with_variant_cast(
            "DB", "SCHEMA", "TABLE",
            variant_columns=["FLOW_DOCUMENT"],
            cursor=mock_cursor,
        )
        
        assert '"ID"' in result
        assert '"NAME"' in result
        assert 'TO_VARCHAR("FLOW_DOCUMENT") AS "FLOW_DOCUMENT"' in result
    
    def test_build_select_no_variant_columns(self):
        """When no VARIANT columns, all should be plain selects."""
        from dbt.adapters.icebreaker.source_cache import SourceCache, CacheConfig
        
        cache = SourceCache(config=CacheConfig(cache_enabled=False))
        
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("ID",), ("NAME",)]
        
        result = cache._build_select_with_variant_cast(
            "DB", "SCHEMA", "TABLE",
            variant_columns=[],
            cursor=mock_cursor,
        )
        
        assert "TO_VARCHAR" not in result
        assert '"ID"' in result
        assert '"NAME"' in result
