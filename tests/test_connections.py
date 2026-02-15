"""
Tests for the Connection Manager module.
"""



class TestIcebreakerCredentials:
    """Test cases for credentials configuration."""
    
    def test_default_values(self):
        """Default credentials should have sensible values."""
        from dbt.adapters.icebreaker.connections import IcebreakerCredentials
        
        creds = IcebreakerCredentials()
        
        assert creds.type == "icebreaker"
        assert creds.engine == "duckdb"
        assert creds.threads == 4
        assert creds.max_local_size_gb == 5.0
        # source_dialect is optional and defaults to None (will use 'snowflake' fallback)
        assert creds.source_dialect is None
    
    def test_cloud_bridge_optional(self):
        """Cloud bridge should be optional."""
        from dbt.adapters.icebreaker.connections import IcebreakerCredentials
        
        creds = IcebreakerCredentials()
        
        # These attributes don't exist by default - cloud bridge is configured separately
        assert not hasattr(creds, 'cloud_bridge_type') or creds.cloud_bridge_type is None


class TestDuckDBConnection:
    """Test cases for DuckDB connection management."""
    
    def test_duckdb_lazy_init(self):
        """DuckDB should not connect until first use."""
        # This is a conceptual test - full test would need mock profile
        pass
    
    def test_duckdb_extensions_loaded(self):
        """Required extensions should be loaded."""
        # Extensions: httpfs, iceberg, delta, aws
        pass


class TestEngineSwitch:
    """Test cases for engine switching."""
    
    def test_set_engine(self):
        """set_engine should change active engine."""
        pass
    
    def test_use_engine_context_manager(self):
        """use_engine should restore previous engine."""
        pass
