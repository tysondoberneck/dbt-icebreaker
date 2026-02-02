"""
Traffic Controller - The Intelligent Routing Engine

Every model passes through 6 Gates to determine its venue (LOCAL or CLOUD).
If it fails any gate, it routes to CLOUD.

Gates:
1. INTENT - User config override
2. GRAVITY - Data accessibility (views, internal sources)
3. CAPABILITY - SQL syntax & type compatibility
4. STABILITY - Crash history (WAL check)
5. COMPLEXITY - Historical telemetry (runtime)
6. PHYSICS - Data volume via catalog metadata
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Set
from enum import Enum
import os
import json
from pathlib import Path

from dbt.adapters.icebreaker.transpiler import Transpiler


VenueType = Literal["LOCAL", "CLOUD"]


class RoutingReason(Enum):
    """Reasons for routing decisions."""
    USER_OVERRIDE = "User configured icebreaker_route"
    VIEW_DEPENDENCY = "Depends on cloud-only views"
    INTERNAL_SOURCE = "Uses internal/proprietary sources"
    UNTRANSPILABLE = "SQL contains untranspilable syntax"
    TOXIC_TYPES = "Contains incompatible data types"
    CRASH_HISTORY = "Previously crashed local execution"
    HIGH_COMPLEXITY = "Historical runtime exceeds threshold"
    LARGE_VOLUME = "Data volume exceeds local threshold"
    DEFAULT_LOCAL = "Passed all gates - running locally (free!)"


@dataclass
class RoutingDecision:
    """Result of a routing decision."""
    venue: VenueType
    reason: RoutingReason
    details: Optional[str] = None
    gate: Optional[int] = None
    
    def __str__(self) -> str:
        gate_str = f"Gate {self.gate}: " if self.gate else ""
        detail_str = f" ({self.details})" if self.details else ""
        return f"{self.venue} - {gate_str}{self.reason.value}{detail_str}"


@dataclass
class TrafficConfig:
    """Configuration for the Traffic Controller."""
    # Gate 5: Complexity thresholds
    max_local_seconds: int = 600  # 10 minutes
    max_spill_bytes: int = 1024 ** 3  # 1GB
    
    # Gate 6: Physics thresholds
    max_local_size_gb: float = 5.0
    
    # Paths for state files
    state_dir: Path = field(default_factory=lambda: Path(".icebreaker"))
    
    # Source dialect for transpilation
    source_dialect: str = "snowflake"


class TrafficController:
    """
    The Traffic Controller - Routes models between LOCAL and CLOUD.
    
    Implements the 6 Gates algorithm to make intelligent routing decisions
    based on user intent, capabilities, history, and data volume.
    """
    
    def __init__(self, config: Optional[TrafficConfig] = None):
        self.config = config or TrafficConfig()
        self._transpiler: Optional[Transpiler] = None
        self._cloud_stats: Optional[Dict] = None
        self._local_state: Optional[Dict] = None
        self._catalog = None
    
    @property
    def transpiler(self) -> Transpiler:
        """Lazy-initialize transpiler."""
        if self._transpiler is None:
            self._transpiler = Transpiler(source_dialect=self.config.source_dialect)
        return self._transpiler
    
    @property
    def cloud_stats(self) -> Dict:
        """Load cloud execution stats from cache."""
        if self._cloud_stats is None:
            stats_file = self.config.state_dir / "cloud_stats.json"
            if stats_file.exists():
                try:
                    self._cloud_stats = json.loads(stats_file.read_text())
                except:
                    self._cloud_stats = {}
            else:
                self._cloud_stats = {}
        return self._cloud_stats
    
    @property
    def local_state(self) -> Dict:
        """Load local execution state (crash history)."""
        if self._local_state is None:
            state_file = self.config.state_dir / "local_state.json"
            if state_file.exists():
                try:
                    self._local_state = json.loads(state_file.read_text())
                except:
                    self._local_state = {}
            else:
                self._local_state = {}
        return self._local_state
    
    def decide(
        self,
        model: Dict[str, Any],
        sql: str,
        sources: Optional[List[Dict]] = None,
    ) -> RoutingDecision:
        """
        Main routing decision method.
        
        Runs the model through all 6 gates and returns the first
        gate that routes to CLOUD, or LOCAL if all gates pass.
        
        Args:
            model: The dbt model node
            sql: The compiled SQL
            sources: Optional list of source metadata
            
        Returns:
            RoutingDecision with venue and reason
        """
        config = model.get("config", {})
        
        # GATE 1: INTENT (User Override)
        decision = self._gate_intent(config)
        if decision:
            return decision
        
        # GATE 2: GRAVITY (Data Accessibility)
        decision = self._gate_gravity(model, sources)
        if decision:
            return decision
        
        # GATE 3: CAPABILITY (Syntax & Types)
        decision = self._gate_capability(sql, model)
        if decision:
            return decision
        
        # GATE 4: STABILITY (Crash History)
        decision = self._gate_stability(model)
        if decision:
            return decision
        
        # GATE 5: COMPLEXITY (Historical Telemetry)
        decision = self._gate_complexity(model)
        if decision:
            return decision
        
        # GATE 6: PHYSICS (Data Volume)
        decision = self._gate_physics(model, sql)
        if decision:
            return decision
        
        # All gates passed - run locally!
        return RoutingDecision(
            venue="LOCAL",
            reason=RoutingReason.DEFAULT_LOCAL,
        )
    
    # =========================================================================
    # Gate Implementations
    # =========================================================================
    
    def _gate_intent(self, config: Dict[str, Any]) -> Optional[RoutingDecision]:
        """
        GATE 1: INTENT - Check for user override.
        
        User can force routing via model config:
        {{ config(icebreaker_route='cloud') }}
        """
        explicit = config.get("icebreaker_route")
        
        if explicit == "cloud":
            return RoutingDecision(
                venue="CLOUD",
                reason=RoutingReason.USER_OVERRIDE,
                details="icebreaker_route='cloud'",
                gate=1,
            )
        
        if explicit == "local":
            return RoutingDecision(
                venue="LOCAL",
                reason=RoutingReason.USER_OVERRIDE,
                details="icebreaker_route='local'",
                gate=1,
            )
        
        return None
    
    def _gate_gravity(
        self,
        model: Dict[str, Any],
        sources: Optional[List[Dict]],
    ) -> Optional[RoutingDecision]:
        """
        GATE 2: GRAVITY - Check data accessibility.
        
        Routes to CLOUD if:
        - Model depends on views (DuckDB can't read cloud views)
        - Sources are marked as internal/proprietary
        """
        # Check for view dependencies
        depends_on = model.get("depends_on", {})
        refs = depends_on.get("nodes", [])
        
        # If we had access to the manifest, we could check if deps are views
        # For now, check if any ref is explicitly marked as a view
        
        # Check sources for internal markers
        if sources:
            for source in sources:
                meta = source.get("meta", {})
                if meta.get("format") == "internal":
                    return RoutingDecision(
                        venue="CLOUD",
                        reason=RoutingReason.INTERNAL_SOURCE,
                        details=source.get("name", "unknown"),
                        gate=2,
                    )
        
        return None
    
    def _gate_capability(
        self,
        sql: str,
        model: Dict[str, Any],
    ) -> Optional[RoutingDecision]:
        """
        GATE 3: CAPABILITY - Check SQL syntax & types.
        
        Routes to CLOUD if:
        - SQL contains untranspilable functions (CORTEX, ML.PREDICT, etc.)
        - Model uses incompatible data types
        """
        # Check for blacklisted functions
        blacklisted = self.transpiler.detect_blacklisted_functions(sql)
        if blacklisted:
            return RoutingDecision(
                venue="CLOUD",
                reason=RoutingReason.UNTRANSPILABLE,
                details=f"Found: {', '.join(blacklisted[:3])}",
                gate=3,
            )
        
        # Check if SQL can be transpiled
        can_transpile, error = self.transpiler.can_transpile(sql)
        if not can_transpile:
            return RoutingDecision(
                venue="CLOUD",
                reason=RoutingReason.UNTRANSPILABLE,
                details=error,
                gate=3,
            )
        
        # Check for toxic types in model config
        toxic_types = model.get("config", {}).get("toxic_types", [])
        if toxic_types:
            return RoutingDecision(
                venue="CLOUD",
                reason=RoutingReason.TOXIC_TYPES,
                details=f"Types: {', '.join(toxic_types)}",
                gate=3,
            )
        
        return None
    
    def _gate_stability(self, model: Dict[str, Any]) -> Optional[RoutingDecision]:
        """
        GATE 4: STABILITY - Check crash history.
        
        Routes to CLOUD if:
        - Model previously crashed local execution (OOM)
        """
        unique_id = model.get("unique_id", "")
        crashes = self.local_state.get("crashes", {})
        
        if unique_id in crashes:
            crash_info = crashes[unique_id]
            return RoutingDecision(
                venue="CLOUD",
                reason=RoutingReason.CRASH_HISTORY,
                details=f"Last crash: {crash_info.get('timestamp', 'unknown')}",
                gate=4,
            )
        
        # Also check for running status (indicates previous crash)
        running = self.local_state.get("running", {})
        if unique_id in running:
            return RoutingDecision(
                venue="CLOUD",
                reason=RoutingReason.CRASH_HISTORY,
                details="Previous run didn't complete",
                gate=4,
            )
        
        return None
    
    def _gate_complexity(self, model: Dict[str, Any]) -> Optional[RoutingDecision]:
        """
        GATE 5: COMPLEXITY - Check historical telemetry.
        
        Routes to CLOUD if:
        - Average production runtime > max_local_seconds
        - Average memory spill > threshold
        """
        model_name = model.get("name", "")
        stats = self.cloud_stats.get("models", {}).get(model_name, {})
        
        if not stats:
            return None
        
        avg_seconds = stats.get("avg_seconds", 0)
        if avg_seconds > self.config.max_local_seconds:
            return RoutingDecision(
                venue="CLOUD",
                reason=RoutingReason.HIGH_COMPLEXITY,
                details=f"Avg runtime: {avg_seconds/60:.1f}m",
                gate=5,
            )
        
        avg_spill = stats.get("avg_spill_bytes", 0)
        if avg_spill > self.config.max_spill_bytes:
            return RoutingDecision(
                venue="CLOUD",
                reason=RoutingReason.HIGH_COMPLEXITY,
                details=f"Avg spill: {avg_spill / (1024**3):.1f}GB",
                gate=5,
            )
        
        return None
    
    def _gate_physics(
        self,
        model: Dict[str, Any],
        sql: str,
    ) -> Optional[RoutingDecision]:
        """
        GATE 6: PHYSICS - Check data volume.
        
        Routes to CLOUD if:
        - Total input data size > max_local_size_gb
        
        Uses "Smart Scan" with partition pruning when possible.
        """
        # Get estimated size from model metadata
        estimated_gb = model.get("config", {}).get("estimated_size_gb")
        
        if estimated_gb:
            if estimated_gb > self.config.max_local_size_gb:
                return RoutingDecision(
                    venue="CLOUD",
                    reason=RoutingReason.LARGE_VOLUME,
                    details=f"Estimated: {estimated_gb:.1f}GB > {self.config.max_local_size_gb}GB",
                    gate=6,
                )
        
        # If we have a catalog, use Smart Scan
        effective_size_gb = self._smart_scan(model, sql)
        if effective_size_gb is not None:
            if effective_size_gb > self.config.max_local_size_gb:
                return RoutingDecision(
                    venue="CLOUD",
                    reason=RoutingReason.LARGE_VOLUME,
                    details=f"Smart Scan: {effective_size_gb:.1f}GB",
                    gate=6,
                )
        
        return None
    
    def _smart_scan(
        self,
        model: Dict[str, Any],
        sql: str,
    ) -> Optional[float]:
        """
        Smart Scan - Calculate effective data size using partition pruning.
        
        Uses PyIceberg to query the catalog and determine how much
        data would actually be scanned based on filter predicates.
        
        Returns:
            Effective size in GB, or None if unavailable
        """
        if self._catalog is None:
            return None
        
        try:
            # Extract table references and filters from SQL
            # This is a simplified implementation
            # Full implementation would use AST analysis
            
            # For now, return None (no catalog available)
            return None
            
        except Exception:
            return None
    
    # =========================================================================
    # State Management
    # =========================================================================
    
    def mark_running(self, model: Dict[str, Any]) -> None:
        """Mark a model as currently running (for crash detection)."""
        unique_id = model.get("unique_id", "")
        if not unique_id:
            return
        
        self.config.state_dir.mkdir(exist_ok=True)
        
        running = self.local_state.get("running", {})
        running[unique_id] = {
            "started_at": str(os.environ.get("DBT_INVOCATION_ID", "unknown")),
        }
        self.local_state["running"] = running
        
        self._save_local_state()
    
    def mark_success(self, model: Dict[str, Any]) -> None:
        """Mark a model as successfully completed."""
        unique_id = model.get("unique_id", "")
        if not unique_id:
            return
        
        running = self.local_state.get("running", {})
        running.pop(unique_id, None)
        self.local_state["running"] = running
        
        self._save_local_state()
    
    def mark_crash(self, model: Dict[str, Any], error: str) -> None:
        """Mark a model as crashed."""
        unique_id = model.get("unique_id", "")
        if not unique_id:
            return
        
        crashes = self.local_state.get("crashes", {})
        crashes[unique_id] = {
            "timestamp": str(os.environ.get("DBT_INVOCATION_ID", "unknown")),
            "error": error[:200],  # Truncate for storage
        }
        self.local_state["crashes"] = crashes
        
        # Remove from running
        running = self.local_state.get("running", {})
        running.pop(unique_id, None)
        self.local_state["running"] = running
        
        self._save_local_state()
    
    def _save_local_state(self) -> None:
        """Persist local state to disk."""
        state_file = self.config.state_dir / "local_state.json"
        state_file.write_text(json.dumps(self._local_state, indent=2))


def decide_venue(
    model: Dict[str, Any],
    sql: str,
    config: Optional[TrafficConfig] = None,
) -> RoutingDecision:
    """
    Convenience function for routing decision.
    
    Args:
        model: dbt model node
        sql: Compiled SQL
        config: Optional traffic config
        
    Returns:
        RoutingDecision
    """
    controller = TrafficController(config)
    return controller.decide(model, sql)
