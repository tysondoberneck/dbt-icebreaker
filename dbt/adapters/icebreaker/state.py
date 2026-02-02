"""
State Manager - Crash Detection via Write-Ahead Log (WAL)

Implements crash detection by tracking model execution status.
Since a hard OOM crash kills the process instantly, we use a WAL
approach: write "running" before execution, "success" after.
On next run, "running" status indicates a previous crash.
"""

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass
class StateConfig:
    """Configuration for state management."""
    state_dir: Path = field(default_factory=lambda: Path(".icebreaker"))
    
    # How many crashes before permanent blacklist
    max_crash_count: int = 3
    
    # How long to remember crashes (days)
    crash_memory_days: int = 7


class StateManager:
    """
    Manages local execution state for crash detection.
    
    Uses Write-Ahead Log (WAL) pattern:
    1. Before execution: Write status = "running"
    2. After success: Write status = "success"
    3. On crash: Status stays "running" (process died)
    4. Next run: Detect "running" = crashed, blacklist model
    """
    
    def __init__(self, config: Optional[StateConfig] = None):
        self.config = config or StateConfig()
        self._state: Optional[Dict] = None
        
    @property
    def state_file(self) -> Path:
        return self.config.state_dir / "local_state.json"
    
    @property
    def state(self) -> Dict:
        """Load or initialize state."""
        if self._state is None:
            self._load_state()
        return self._state
    
    def _load_state(self) -> None:
        """Load state from disk."""
        if self.state_file.exists():
            try:
                self._state = json.loads(self.state_file.read_text())
            except (json.JSONDecodeError, IOError):
                self._state = self._default_state()
        else:
            self._state = self._default_state()
    
    def _default_state(self) -> Dict:
        """Create default state structure."""
        return {
            "running": {},
            "crashes": {},
            "successes": {},
            "local_runs": 0,
            "cloud_runs": 0,
        }
    
    def _save_state(self) -> None:
        """Persist state to disk."""
        self.config.state_dir.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(json.dumps(self._state, indent=2, default=str))
    
    # =========================================================================
    # WAL Methods
    # =========================================================================
    
    def mark_running(self, model_id: str) -> None:
        """
        Mark a model as running (pre-execution).
        
        This is the "write-ahead" part of the WAL.
        If the process crashes, this status remains and we detect it.
        """
        self.state["running"][model_id] = {
            "started_at": datetime.now().isoformat(),
            "invocation": os.environ.get("DBT_INVOCATION_ID", "unknown"),
        }
        self._save_state()
    
    def mark_success(self, model_id: str) -> None:
        """
        Mark a model as successfully completed.
        
        Removes from "running" and updates success counter.
        """
        # Remove from running
        self.state["running"].pop(model_id, None)
        
        # Track success
        self.state["successes"][model_id] = {
            "last_success": datetime.now().isoformat(),
        }
        
        # Increment local run counter
        self.state["local_runs"] = self.state.get("local_runs", 0) + 1
        
        self._save_state()
    
    def mark_cloud_run(self) -> None:
        """Increment cloud run counter."""
        self.state["cloud_runs"] = self.state.get("cloud_runs", 0) + 1
        self._save_state()
    
    def mark_crash(self, model_id: str, error: Optional[str] = None) -> None:
        """
        Mark a model as crashed.
        
        Called when we catch an exception during execution.
        Also called on next run when we detect "running" status.
        """
        # Remove from running
        self.state["running"].pop(model_id, None)
        
        # Add to crashes
        crashes = self.state.setdefault("crashes", {})
        crash_entry = crashes.get(model_id, {"count": 0, "history": []})
        
        crash_entry["count"] = crash_entry.get("count", 0) + 1
        crash_entry["last_crash"] = datetime.now().isoformat()
        crash_entry["history"].append({
            "timestamp": datetime.now().isoformat(),
            "error": (error or "Unknown")[:200],
        })
        
        # Keep only last 5 crashes in history
        crash_entry["history"] = crash_entry["history"][-5:]
        
        crashes[model_id] = crash_entry
        self._save_state()
    
    # =========================================================================
    # Query Methods
    # =========================================================================
    
    def was_crash(self, model_id: str) -> bool:
        """
        Check if a model previously crashed.
        
        Returns True if:
        - Model has any crash history
        - Model was left in "running" state (previous crash)
        """
        # Check for leftover running status
        if model_id in self.state.get("running", {}):
            # Previous run didn't complete - mark as crash
            self.mark_crash(model_id, "Previous run didn't complete (OOM?)")
            return True
        
        # Check crash history
        crashes = self.state.get("crashes", {}).get(model_id, {})
        return crashes.get("count", 0) > 0
    
    def get_crash_count(self, model_id: str) -> int:
        """Get number of times a model has crashed."""
        return self.state.get("crashes", {}).get(model_id, {}).get("count", 0)
    
    def is_blacklisted(self, model_id: str) -> bool:
        """Check if model is permanently blacklisted due to repeated crashes."""
        return self.get_crash_count(model_id) >= self.config.max_crash_count
    
    def clear_crash_history(self, model_id: str) -> None:
        """Clear crash history for a model (use after fixing issues)."""
        self.state.get("crashes", {}).pop(model_id, None)
        self._save_state()
    
    def clear_all_running(self) -> None:
        """
        Clear all running statuses.
        
        Call this at the start of a dbt run to reset state
        if the user confirms crashes are resolved.
        """
        running = self.state.get("running", {})
        for model_id in list(running.keys()):
            self.mark_crash(model_id, "Cleared at run start")
        self._save_state()
    
    # =========================================================================
    # Statistics
    # =========================================================================
    
    def get_savings_report(self) -> Dict[str, Any]:
        """Generate cost savings report."""
        local = self.state.get("local_runs", 0)
        cloud = self.state.get("cloud_runs", 0)
        total = local + cloud
        
        return {
            "local_runs": local,
            "cloud_runs": cloud,
            "total_runs": total,
            "savings_pct": (local / total * 100) if total > 0 else 0,
            "crashes": len(self.state.get("crashes", {})),
        }


# Singleton for easy access
_state_manager: Optional[StateManager] = None


def get_state_manager(config: Optional[StateConfig] = None) -> StateManager:
    """Get or create the state manager singleton."""
    global _state_manager
    if _state_manager is None:
        _state_manager = StateManager(config)
    return _state_manager
