"""
Run Summary for Icebreaker.

Tracks routing decisions and execution metrics during a dbt run,
then generates a clear summary showing where models ran and cost savings.
"""

import os
import json
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from enum import Enum

from dbt.adapters.icebreaker.console import console


# =============================================================================
# Run Session Tracking
# =============================================================================

@dataclass
class ModelExecution:
    """Track a single model's execution."""
    name: str
    venue: str  # "LOCAL" or "CLOUD"
    reason: str
    duration_seconds: float = 0.0
    rows_affected: int = 0
    success: bool = True
    error: Optional[str] = None
    estimated_cloud_cost: float = 0.0
    started_at: str = ""
    
    @property
    def savings(self) -> float:
        """Calculate savings (only if ran locally)."""
        if self.venue == "LOCAL" and self.success:
            return self.estimated_cloud_cost
        return 0.0


@dataclass
class RunSession:
    """Track an entire dbt run session."""
    session_id: str
    started_at: str
    ended_at: str = ""
    models: List[ModelExecution] = field(default_factory=list)
    total_models: int = 0
    
    @property
    def local_count(self) -> int:
        return sum(1 for m in self.models if m.venue == "LOCAL")
    
    @property
    def cloud_count(self) -> int:
        return sum(1 for m in self.models if m.venue == "CLOUD")
    
    @property
    def success_count(self) -> int:
        return sum(1 for m in self.models if m.success)
    
    @property
    def error_count(self) -> int:
        return sum(1 for m in self.models if not m.success)
    
    @property
    def total_duration(self) -> float:
        return sum(m.duration_seconds for m in self.models)
    
    @property
    def total_savings(self) -> float:
        return sum(m.savings for m in self.models)


# =============================================================================
# Run Summary Manager
# =============================================================================

class RunSummary:
    """
    Manages run session tracking and summary generation.
    
    Usage:
        summary = RunSummary()
        summary.start_session()
        
        # During dbt run...
        summary.log_model(name="orders", venue="LOCAL", reason="AUTO_LOCAL", ...)
        
        # After run...
        summary.end_session()
        console.info(summary.format_summary())
    """
    
    def __init__(self, data_dir: Optional[str] = None):
        self.data_dir = data_dir or os.path.expanduser("~/.icebreaker/runs")
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        self._session: Optional[RunSession] = None
    
    @property
    def session_file(self) -> str:
        if self._session:
            return os.path.join(self.data_dir, f"{self._session.session_id}.json")
        return os.path.join(self.data_dir, "current.json")
    
    def start_session(self) -> str:
        """Start a new run session. Returns session ID."""
        session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._session = RunSession(
            session_id=session_id,
            started_at=datetime.now().isoformat(),
        )
        return session_id
    
    def log_model(
        self,
        name: str,
        venue: str,
        reason: str,
        duration_seconds: float = 0.0,
        rows_affected: int = 0,
        success: bool = True,
        error: Optional[str] = None,
        estimated_cloud_cost: float = 0.0,
    ):
        """Log a model execution to the current session."""
        if not self._session:
            self.start_session()
        
        execution = ModelExecution(
            name=name,
            venue=venue,
            reason=reason,
            duration_seconds=duration_seconds,
            rows_affected=rows_affected,
            success=success,
            error=error,
            estimated_cloud_cost=estimated_cloud_cost,
            started_at=datetime.now().isoformat(),
        )
        self._session.models.append(execution)
        self._session.total_models = len(self._session.models)
    
    def end_session(self):
        """End the current session."""
        if self._session:
            self._session.ended_at = datetime.now().isoformat()
            self._save_session()
    
    def _save_session(self):
        """Save session to disk."""
        if self._session:
            data = {
                "session_id": self._session.session_id,
                "started_at": self._session.started_at,
                "ended_at": self._session.ended_at,
                "total_models": self._session.total_models,
                "models": [asdict(m) for m in self._session.models],
            }
            with open(self.session_file, 'w') as f:
                json.dump(data, f, indent=2)
    
    def format_summary(self, colorize: bool = True) -> str:
        """
        Generate a formatted summary of the current run.
        
        This is the key UX improvement - users see this after every dbt run.
        """
        if not self._session:
            return "No run session active."
        
        s = self._session
        lines = []
        
        # Header
        lines.append("")
        lines.append("=" * 60)
        lines.append("ICEBREAKER RUN SUMMARY")
        lines.append("=" * 60)
        lines.append("")
        
        # Stats overview
        local_pct = (s.local_count / max(len(s.models), 1)) * 100
        lines.append(f"Models: {len(s.models)} total")
        lines.append(f"  Local (FREE):  {s.local_count} ({local_pct:.0f}%)")
        lines.append(f"  Cloud:         {s.cloud_count}")
        lines.append(f"  Succeeded:     {s.success_count}")
        if s.error_count > 0:
            lines.append(f"  Failed:        {s.error_count}")
        lines.append("")
        
        # Savings
        lines.append(f"Estimated Savings: ${s.total_savings:.2f}")
        lines.append(f"Total Duration:    {s.total_duration:.1f}s")
        lines.append("")
        
        # Routing breakdown by reason
        reason_counts: Dict[str, int] = {}
        for m in s.models:
            reason_counts[m.reason] = reason_counts.get(m.reason, 0) + 1
        
        if reason_counts:
            lines.append("Routing Breakdown:")
            for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
                lines.append(f"  {reason}: {count}")
            lines.append("")
        
        # Show errors if any
        errors = [m for m in s.models if not m.success]
        if errors:
            lines.append("Errors:")
            for m in errors[:5]:  # Show max 5
                lines.append(f"  - {m.name}: {m.error or 'Unknown error'}")
            if len(errors) > 5:
                lines.append(f"  ... and {len(errors) - 5} more")
            lines.append("")
        
        # Footer
        lines.append("=" * 60)
        lines.append("Run 'icebreaker savings' for detailed cost analysis")
        lines.append("")
        
        return "\n".join(lines)
    
    def get_last_session(self) -> Optional[Dict]:
        """Load the most recent session from disk."""
        try:
            files = sorted(Path(self.data_dir).glob("*.json"), reverse=True)
            if files:
                with open(files[0], 'r') as f:
                    return json.load(f)
        except Exception:
            pass
        return None


# =============================================================================
# Singleton
# =============================================================================

_summary: Optional[RunSummary] = None


def get_run_summary() -> RunSummary:
    """Get or create the run summary singleton."""
    global _summary
    if _summary is None:
        _summary = RunSummary()
    return _summary


def print_run_summary():
    """Print the run summary (call after dbt run)."""
    summary = get_run_summary()
    console.info(summary.format_summary())
