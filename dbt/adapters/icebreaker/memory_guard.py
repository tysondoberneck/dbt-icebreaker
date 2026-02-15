"""
Memory Guard for Icebreaker.

Prevents OOM crashes by estimating query memory requirements
and routing to cloud when local memory is insufficient.
"""

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# Try to import psutil for system memory info
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False


@dataclass
class MemoryEstimate:
    """Memory estimate for a query."""
    estimated_gb: float
    available_gb: float
    safe_to_run: bool
    complexity: str  # simple, medium, complex, heavy
    details: str
    
    def __str__(self) -> str:
        status = "OK" if self.safe_to_run else "WARN"
        return f"[{status}] Need ~{self.estimated_gb:.1f}GB, have {self.available_gb:.1f}GB ({self.complexity})"


class MemoryGuard:
    """
    Prevents OOM crashes by checking memory before execution.
    
    Features:
    - Estimates query memory requirements
    - Checks system available memory
    - Routes to cloud when local memory insufficient
    """
    
    def __init__(
        self,
        max_memory_pct: float = 0.75,
        min_available_gb: float = 1.0,
    ):
        """
        Initialize MemoryGuard.
        
        Args:
            max_memory_pct: Max fraction of system memory to use (0-1)
            min_available_gb: Always leave at least this much memory free
        """
        self.max_memory_pct = max_memory_pct
        self.min_available_gb = min_available_gb
        
        # Get system memory info
        if HAS_PSUTIL:
            mem = psutil.virtual_memory()
            self.system_memory_gb = mem.total / (1024 ** 3)
        else:
            # Fallback: assume 16GB (common laptop)
            self.system_memory_gb = 16.0
        
        self.max_query_gb = self.system_memory_gb * max_memory_pct
    
    def check_query(
        self,
        sql: str,
        input_size_gb: float = 0.0,
        catalog_scanner: Optional[Any] = None,
        model: Optional[Dict] = None,
    ) -> MemoryEstimate:
        """
        Check if a query is safe to run locally.
        
        Args:
            sql: The SQL query to run
            input_size_gb: Known input data size in GB
            catalog_scanner: Optional scanner for size estimation
            model: Optional dbt model for dependency analysis
            
        Returns:
            MemoryEstimate with safety determination
        """
        # Get current available memory
        if HAS_PSUTIL:
            mem = psutil.virtual_memory()
            available_gb = mem.available / (1024 ** 3)
        else:
            # Conservative estimate if psutil unavailable
            available_gb = self.system_memory_gb * 0.5
        
        # Estimate query memory requirements
        complexity = self._analyze_complexity(sql)
        estimated_gb = self._estimate_memory(sql, input_size_gb, complexity)
        
        # Determine if safe to run
        effective_available = available_gb - self.min_available_gb
        safe_to_run = estimated_gb <= effective_available * 0.8
        
        return MemoryEstimate(
            estimated_gb=estimated_gb,
            available_gb=available_gb,
            safe_to_run=safe_to_run,
            complexity=complexity,
            details=self._get_details(sql, complexity),
        )
    
    def _analyze_complexity(self, sql: str) -> str:
        """
        Analyze SQL complexity for memory estimation.
        
        Categories:
        - simple: Basic SELECT with filters
        - medium: JOINs
        - complex: Multiple JOINs + aggregations
        - heavy: Window functions, CUBE, ROLLUP, etc.
        """
        sql_upper = sql.upper()
        
        # Heavy operations
        if any(kw in sql_upper for kw in ["CUBE", "ROLLUP", "GROUPING SETS"]):
            return "heavy"
        
        # Window functions (expensive)
        window_count = len(re.findall(r"OVER\s*\(", sql_upper))
        if window_count > 3:
            return "heavy"
        elif window_count > 0:
            # Has window functions but not too many
            pass
        
        # Count JOINs
        join_count = sql_upper.count(" JOIN ")
        
        # Count aggregations
        agg_count = sum(1 for agg in ["COUNT(", "SUM(", "AVG(", "MAX(", "MIN("]
                       if agg in sql_upper)
        
        # Count subqueries
        subquery_count = sql_upper.count("SELECT") - 1
        
        # Classify
        if join_count > 4 or subquery_count > 3:
            return "complex"
        elif join_count > 0 or window_count > 0:
            return "medium"
        else:
            return "simple"
    
    def _estimate_memory(
        self,
        sql: str,
        input_size_gb: float,
        complexity: str,
    ) -> float:
        """
        Estimate memory requirements for a query.
        
        Uses complexity-based multipliers on input size.
        """
        # Base multiplier by complexity
        multipliers = {
            "simple": 1.2,   # Just need to hold data + output
            "medium": 1.5,   # Intermediate buffers for JOINs
            "complex": 2.5,  # Multiple intermediate results
            "heavy": 4.0,    # Window functions, cubes need full dataset
        }
        multiplier = multipliers.get(complexity, 2.0)
        
        # If we don't know input size, estimate from SQL
        if input_size_gb <= 0:
            # Use heuristics based on query patterns
            sql_upper = sql.upper()
            
            # Check for LIMIT clauses (reduces memory)
            limit_match = re.search(r"LIMIT\s+(\d+)", sql_upper)
            if limit_match:
                limit = int(limit_match.group(1))
                if limit < 10000:
                    return 0.1  # Very small result
                elif limit < 100000:
                    return 0.5
            
            # Check for date filters (likely filtered data)
            if any(kw in sql_upper for kw in ["WHERE", "BETWEEN", "DATE", ">", "<"]):
                return 1.0 * multiplier
            
            # Unknown - use conservative estimate
            return 2.0 * multiplier
        
        return input_size_gb * multiplier
    
    def _get_details(self, sql: str, complexity: str) -> str:
        """Get human-readable details about the analysis."""
        sql_upper = sql.upper()
        
        details = []
        
        # JOINs
        join_count = sql_upper.count(" JOIN ")
        if join_count > 0:
            details.append(f"{join_count} JOIN(s)")
        
        # Window functions
        window_count = len(re.findall(r"OVER\s*\(", sql_upper))
        if window_count > 0:
            details.append(f"{window_count} window function(s)")
        
        # Subqueries
        subquery_count = sql_upper.count("SELECT") - 1
        if subquery_count > 0:
            details.append(f"{subquery_count} subquery/CTE(s)")
        
        # Heavy ops
        for op in ["CUBE", "ROLLUP", "GROUPING SETS"]:
            if op in sql_upper:
                details.append(op)
        
        if not details:
            details.append("Simple query")
        
        return ", ".join(details)
    
    def get_system_info(self) -> Dict[str, Any]:
        """Get current system memory information."""
        if HAS_PSUTIL:
            mem = psutil.virtual_memory()
            return {
                "total_gb": mem.total / (1024 ** 3),
                "available_gb": mem.available / (1024 ** 3),
                "used_pct": mem.percent,
                "max_query_gb": self.max_query_gb,
                "psutil_available": True,
            }
        else:
            return {
                "total_gb": self.system_memory_gb,
                "available_gb": self.system_memory_gb * 0.5,  # Estimate
                "used_pct": 50.0,  # Estimate
                "max_query_gb": self.max_query_gb,
                "psutil_available": False,
            }


@dataclass
class PreFlightWarning:
    """A pre-flight check warning."""
    level: str  # INFO, WARNING, BLOCKER
    category: str  # memory, sql, dependency, etc.
    message: str
    recommendation: Optional[str] = None


class PreFlightChecker:
    """
    Run pre-flight checks before query execution.
    
    Catches issues early to avoid long-running queries failing.
    """
    
    def __init__(
        self,
        memory_guard: Optional[MemoryGuard] = None,
        catalog_scanner: Optional[Any] = None,
    ):
        self.memory_guard = memory_guard or MemoryGuard()
        self.catalog = catalog_scanner
    
    def check(
        self,
        sql: str,
        model: Dict[str, Any],
        input_size_gb: float = 0.0,
    ) -> List[PreFlightWarning]:
        """
        Run all pre-flight checks.
        
        Args:
            sql: The SQL to execute
            model: The dbt model node
            input_size_gb: Known input data size
            
        Returns:
            List of warnings/blockers
        """
        warnings = []
        
        # Memory check
        mem_estimate = self.memory_guard.check_query(
            sql, input_size_gb, self.catalog, model
        )
        
        if not mem_estimate.safe_to_run:
            warnings.append(PreFlightWarning(
                level="BLOCKER",
                category="memory",
                message=f"Estimated memory {mem_estimate.estimated_gb:.1f}GB exceeds available {mem_estimate.available_gb:.1f}GB",
                recommendation="Will route to cloud",
            ))
        elif mem_estimate.estimated_gb > mem_estimate.available_gb * 0.6:
            warnings.append(PreFlightWarning(
                level="WARNING",
                category="memory",
                message=f"Query may use {mem_estimate.estimated_gb:.1f}GB of {mem_estimate.available_gb:.1f}GB available",
                recommendation="Consider running on cloud for large datasets",
            ))
        
        # SQL complexity check
        if mem_estimate.complexity == "heavy":
            warnings.append(PreFlightWarning(
                level="WARNING",
                category="sql",
                message=f"Complex query detected: {mem_estimate.details}",
                recommendation="Heavy operations may be slow locally",
            ))
        
        # Check for potentially slow patterns
        sql_upper = sql.upper()
        
        if "CROSS JOIN" in sql_upper:
            warnings.append(PreFlightWarning(
                level="WARNING",
                category="sql",
                message="CROSS JOIN detected - may produce very large result",
            ))
        
        if sql_upper.count("SELECT") > 5:
            warnings.append(PreFlightWarning(
                level="INFO",
                category="sql",
                message=f"Query has {sql_upper.count('SELECT')} SELECT statements",
            ))
        
        # Check for missing dependencies (would need manifest)
        # This is a placeholder for future implementation
        
        return warnings
    
    def format_warnings(self, warnings: List[PreFlightWarning]) -> str:
        """Format warnings for display."""
        if not warnings:
            return "All pre-flight checks passed"
        
        lines = ["Pre-flight checks:"]
        for w in warnings:
            marker = {"BLOCKER": "BLOCK", "WARNING": "WARN", "INFO": "INFO"}.get(w.level, "-")
            lines.append(f"  [{marker}] [{w.category}] {w.message}")
            if w.recommendation:
                lines.append(f"       -> {w.recommendation}")
        
        return "\n".join(lines)


# =============================================================================
# Singleton
# =============================================================================

_memory_guard: Optional[MemoryGuard] = None


def get_memory_guard() -> MemoryGuard:
    """Get or create the memory guard singleton."""
    global _memory_guard
    if _memory_guard is None:
        _memory_guard = MemoryGuard()
    return _memory_guard
