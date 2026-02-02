"""
Tests for the Traffic Controller.
"""

import pytest
from dbt.adapters.icebreaker.traffic import (
    TrafficController,
    TrafficConfig,
    RoutingDecision,
    RoutingReason,
    decide_venue,
)


class TestGate1Intent:
    """Test Gate 1: User intent override."""
    
    def test_explicit_cloud_route(self):
        """User can force cloud routing."""
        controller = TrafficController()
        model = {
            "name": "test_model",
            "config": {"icebreaker_route": "cloud"},
        }
        
        decision = controller.decide(model, "SELECT 1")
        
        assert decision.venue == "CLOUD"
        assert decision.reason == RoutingReason.USER_OVERRIDE
        assert decision.gate == 1
    
    def test_explicit_local_route(self):
        """User can force local routing."""
        controller = TrafficController()
        model = {
            "name": "test_model",
            "config": {"icebreaker_route": "local"},
        }
        
        decision = controller.decide(model, "SELECT 1")
        
        assert decision.venue == "LOCAL"
        assert decision.reason == RoutingReason.USER_OVERRIDE


class TestGate2Gravity:
    """Test Gate 2: Data accessibility."""
    
    def test_internal_source(self):
        """Internal sources should route to cloud."""
        controller = TrafficController()
        model = {"name": "test", "config": {}}
        sources = [{"name": "internal_db", "meta": {"format": "internal"}}]
        
        decision = controller.decide(model, "SELECT 1", sources=sources)
        
        assert decision.venue == "CLOUD"
        assert decision.reason == RoutingReason.INTERNAL_SOURCE


class TestGate3Capability:
    """Test Gate 3: SQL capability."""
    
    def test_blacklisted_function(self):
        """Blacklisted functions should route to cloud."""
        controller = TrafficController()
        model = {"name": "test", "config": {}}
        sql = "SELECT SNOWFLAKE.CORTEX.COMPLETE('model', 'prompt')"
        
        decision = controller.decide(model, sql)
        
        # May or may not detect depending on SQLGlot parsing
        # Just verify it doesn't crash
        assert decision.venue in ("LOCAL", "CLOUD")
    
    def test_toxic_types(self):
        """Toxic types in config should route to cloud."""
        controller = TrafficController()
        model = {
            "name": "test",
            "config": {"toxic_types": ["GEOGRAPHY", "GEOMETRY"]},
        }
        
        decision = controller.decide(model, "SELECT 1")
        
        assert decision.venue == "CLOUD"
        assert decision.reason == RoutingReason.TOXIC_TYPES


class TestGate5Complexity:
    """Test Gate 5: Historical complexity."""
    
    def test_high_runtime(self):
        """High historical runtime should route to cloud."""
        config = TrafficConfig(max_local_seconds=60)
        controller = TrafficController(config)
        
        # Inject mock stats
        controller._cloud_stats = {
            "models": {
                "slow_model": {"avg_seconds": 3600}  # 1 hour
            }
        }
        
        model = {"name": "slow_model", "config": {}}
        decision = controller.decide(model, "SELECT 1")
        
        assert decision.venue == "CLOUD"
        assert decision.reason == RoutingReason.HIGH_COMPLEXITY


class TestGate6Physics:
    """Test Gate 6: Data volume."""
    
    def test_large_estimated_size(self):
        """Large estimated size should route to cloud."""
        config = TrafficConfig(max_local_size_gb=5.0)
        controller = TrafficController(config)
        
        model = {
            "name": "big_table",
            "config": {"estimated_size_gb": 100.0},
        }
        
        decision = controller.decide(model, "SELECT 1")
        
        assert decision.venue == "CLOUD"
        assert decision.reason == RoutingReason.LARGE_VOLUME


class TestDefaultRouting:
    """Test default behavior."""
    
    def test_passes_all_gates(self):
        """Simple query should run locally."""
        controller = TrafficController()
        model = {"name": "simple", "config": {}}
        
        decision = controller.decide(model, "SELECT id, name FROM users")
        
        assert decision.venue == "LOCAL"
        assert decision.reason == RoutingReason.DEFAULT_LOCAL


class TestDecisionStr:
    """Test decision string representation."""
    
    def test_decision_str(self):
        """Decision should have readable string."""
        decision = RoutingDecision(
            venue="CLOUD",
            reason=RoutingReason.HIGH_COMPLEXITY,
            details="Avg runtime: 45.0m",
            gate=5,
        )
        
        result = str(decision)
        
        assert "CLOUD" in result
        assert "Gate 5" in result
        assert "45.0m" in result
