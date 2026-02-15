"""
Tests for State Manager and Metadata Harvester.
"""

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from dbt.adapters.icebreaker.state import StateManager, StateConfig
from dbt.adapters.icebreaker.metadata import MetadataHarvester, MetadataConfig, ModelStats


class TestStateManager:
    """Test cases for crash detection."""
    
    def test_mark_running(self):
        """Mark running should persist to disk."""
        with TemporaryDirectory() as tmpdir:
            config = StateConfig(state_dir=Path(tmpdir))
            manager = StateManager(config)
            
            manager.mark_running("model.test_model")
            
            # Check file was created
            assert config.state_dir.exists()
            state_file = config.state_dir / "local_state.json"
            assert state_file.exists()
            
            # Check content
            state = json.loads(state_file.read_text())
            assert "model.test_model" in state["running"]
    
    def test_mark_success(self):
        """Mark success should remove from running."""
        with TemporaryDirectory() as tmpdir:
            config = StateConfig(state_dir=Path(tmpdir))
            manager = StateManager(config)
            
            manager.mark_running("model.test_model")
            manager.mark_success("model.test_model")
            
            # Should be removed from running
            assert "model.test_model" not in manager.state["running"]
            
            # Should be in successes
            assert "model.test_model" in manager.state["successes"]
            
            # Local runs should increment
            assert manager.state["local_runs"] == 1
    
    def test_crash_detection(self):
        """Running status on next run indicates crash."""
        with TemporaryDirectory() as tmpdir:
            config = StateConfig(state_dir=Path(tmpdir))
            
            # First run - mark running but don't mark success (simulate crash)
            manager1 = StateManager(config)
            manager1.mark_running("model.crashy")
            
            # Second run - should detect crash
            manager2 = StateManager(config)
            assert manager2.was_crash("model.crashy") is True
            
            # Should now be in crashes
            assert "model.crashy" in manager2.state["crashes"]
    
    def test_blacklist_after_repeated_crashes(self):
        """Model should be blacklisted after max crashes."""
        with TemporaryDirectory() as tmpdir:
            config = StateConfig(state_dir=Path(tmpdir), max_crash_count=3)
            manager = StateManager(config)
            
            # Simulate 3 crashes
            for i in range(3):
                manager.mark_crash("model.bad", f"Error {i}")
            
            assert manager.is_blacklisted("model.bad") is True
            assert manager.get_crash_count("model.bad") == 3
    
    def test_savings_report(self):
        """Savings report should calculate percentages."""
        with TemporaryDirectory() as tmpdir:
            config = StateConfig(state_dir=Path(tmpdir))
            manager = StateManager(config)
            
            # Simulate runs
            manager.state["local_runs"] = 80
            manager.state["cloud_runs"] = 20
            
            report = manager.get_savings_report()
            
            assert report["local_runs"] == 80
            assert report["cloud_runs"] == 20
            assert report["savings_pct"] == 80.0


class TestMetadataHarvester:
    """Test cases for cloud stats harvesting."""
    
    def test_default_cache(self):
        """Default cache should be empty."""
        with TemporaryDirectory() as tmpdir:
            config = MetadataConfig(state_dir=Path(tmpdir))
            harvester = MetadataHarvester(config)
            
            assert harvester.cache["models"] == {}
            assert harvester.cache["fetched_at"] is None
    
    def test_cache_staleness(self):
        """Stale cache should be detected."""
        with TemporaryDirectory() as tmpdir:
            config = MetadataConfig(state_dir=Path(tmpdir))
            harvester = MetadataHarvester(config)
            
            # Empty cache is stale
            assert harvester.is_stale() is True
    
    def test_update_cache(self):
        """Cache update should persist stats."""
        with TemporaryDirectory() as tmpdir:
            config = MetadataConfig(state_dir=Path(tmpdir))
            harvester = MetadataHarvester(config)
            
            stats = {
                "model_a": ModelStats(
                    model_name="model_a",
                    avg_seconds=120.0,
                    avg_spill_bytes=1000000,
                    run_count=10,
                ),
            }
            
            harvester.update_cache(stats, "snowflake")
            
            # Reload and verify
            harvester2 = MetadataHarvester(config)
            retrieved = harvester2.get_model_stats("model_a")
            
            assert retrieved is not None
            assert retrieved.avg_seconds == 120.0
    
    def test_get_slow_models(self):
        """Should identify slow models."""
        with TemporaryDirectory() as tmpdir:
            config = MetadataConfig(state_dir=Path(tmpdir))
            harvester = MetadataHarvester(config)
            
            harvester._cache = {
                "models": {
                    "fast_model": {"avg_seconds": 30},
                    "slow_model": {"avg_seconds": 1800},
                    "medium_model": {"avg_seconds": 300},
                },
                "fetched_at": None,
                "source": None,
            }
            
            slow = harvester.get_slow_models(threshold_seconds=600)
            
            assert "slow_model" in slow
            assert "fast_model" not in slow
            assert "medium_model" not in slow
    
    def test_snowflake_query(self):
        """Snowflake query should be valid SQL."""
        harvester = MetadataHarvester()
        query = harvester.get_snowflake_query()
        
        assert "SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY" in query
        assert "QUERY_TAG" in query
        assert "AVG" in query
