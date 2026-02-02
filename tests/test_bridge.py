"""
Tests for the Bridge module.
"""

import pytest
from dbt.adapters.icebreaker.bridge import (
    Bridge,
    IcebergConfig,
    CloudProvider,
    construct_iceberg_ddl,
)


class TestBridge:
    """Test cases for Iceberg DDL generation."""
    
    def test_snowflake_basic_ddl(self):
        """Snowflake DDL should include catalog integration."""
        config = IcebergConfig(
            schema="analytics",
            table="orders",
            catalog_integration="POLARIS_INT",
            external_volume="S3_VOL",
        )
        
        bridge = Bridge("snowflake", None)
        sql = "SELECT * FROM raw.orders"
        
        result = bridge.construct_iceberg_ddl(sql, config)
        
        assert "CREATE OR REPLACE ICEBERG TABLE" in result
        assert "analytics.orders" in result
        assert "CATALOG_INTEGRATION = 'POLARIS_INT'" in result
        assert "EXTERNAL_VOLUME = 'S3_VOL'" in result
        assert "SELECT * FROM raw.orders" in result
    
    def test_snowflake_with_partition(self):
        """Snowflake DDL should support partitioning."""
        config = IcebergConfig(
            schema="analytics",
            table="events",
            catalog_integration="POLARIS_INT",
            external_volume="S3_VOL",
            partition_by="date",
        )
        
        bridge = Bridge("snowflake", None)
        result = bridge.construct_iceberg_ddl("SELECT * FROM src", config)
        
        assert "PARTITION BY (date)" in result
    
    def test_databricks_basic_ddl(self):
        """Databricks DDL should use USING ICEBERG syntax."""
        config = IcebergConfig(
            schema="analytics",
            table="orders",
            location="s3://bucket/path",
        )
        
        bridge = Bridge("databricks", None)
        sql = "SELECT * FROM raw.orders"
        
        result = bridge.construct_iceberg_ddl(sql, config)
        
        assert "CREATE OR REPLACE TABLE" in result
        assert "USING ICEBERG" in result
        assert "LOCATION 's3://bucket/path'" in result
    
    def test_bigquery_ddl(self):
        """BigQuery DDL should use EXTERNAL TABLE syntax."""
        config = IcebergConfig(
            schema="analytics",
            table="orders",
            connection="project-connection",
        )
        
        bridge = Bridge("bigquery", None)
        sql = "SELECT * FROM raw.orders"
        
        result = bridge.construct_iceberg_ddl(sql, config)
        
        assert "EXTERNAL TABLE" in result
        assert "format = 'ICEBERG'" in result
    
    def test_athena_ddl(self):
        """Athena DDL should use WITH clause syntax."""
        config = IcebergConfig(
            schema="analytics",
            table="orders",
            location="s3://bucket/path/",
        )
        
        bridge = Bridge("athena", None)
        sql = "SELECT * FROM raw.orders"
        
        result = bridge.construct_iceberg_ddl(sql, config)
        
        assert "CREATE TABLE" in result
        assert "table_type = 'ICEBERG'" in result
        assert "format = 'PARQUET'" in result


class TestCloudProvider:
    """Test cloud provider enum."""
    
    def test_valid_providers(self):
        """All supported providers should be valid."""
        assert CloudProvider("snowflake") == CloudProvider.SNOWFLAKE
        assert CloudProvider("databricks") == CloudProvider.DATABRICKS
        assert CloudProvider("bigquery") == CloudProvider.BIGQUERY
        assert CloudProvider("athena") == CloudProvider.ATHENA
    
    def test_invalid_provider(self):
        """Invalid provider should raise ValueError."""
        with pytest.raises(ValueError):
            CloudProvider("oracle")


class TestIcebergConfig:
    """Test Iceberg configuration."""
    
    def test_minimal_config(self):
        """Minimal config should work."""
        config = IcebergConfig(schema="public", table="test")
        
        assert config.schema == "public"
        assert config.table == "test"
        assert config.table_format == "iceberg"
    
    def test_full_config(self):
        """Full config should set all fields."""
        config = IcebergConfig(
            schema="analytics",
            table="orders",
            catalog_integration="CAT_INT",
            external_volume="EXT_VOL",
            location="s3://bucket/path",
            partition_by="date",
        )
        
        assert config.catalog_integration == "CAT_INT"
        assert config.external_volume == "EXT_VOL"
        assert config.location == "s3://bucket/path"
        assert config.partition_by == "date"
