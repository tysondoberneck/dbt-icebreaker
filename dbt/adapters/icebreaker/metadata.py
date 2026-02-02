"""
Metadata Harvester - Cloud Telemetry Collection

Fetches execution statistics from cloud warehouses to inform
Gate 5 (Complexity) routing decisions.

Supports:
- Snowflake: QUERY_HISTORY view
- BigQuery: INFORMATION_SCHEMA.JOBS
- Databricks: Query history API
"""

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class MetadataConfig:
    """Configuration for metadata harvesting."""
    state_dir: Path = field(default_factory=lambda: Path(".icebreaker"))
    
    # How many days of history to fetch
    history_days: int = 14
    
    # Cache staleness threshold (hours)
    cache_ttl_hours: int = 24


@dataclass
class ModelStats:
    """Execution statistics for a model."""
    model_name: str
    avg_seconds: float = 0.0
    avg_spill_bytes: float = 0.0
    avg_rows_produced: float = 0.0
    run_count: int = 0
    last_run: Optional[str] = None


class MetadataHarvester:
    """
    Harvests execution telemetry from cloud warehouses.
    
    Uses cloud-specific query history views to understand
    how long models take in production, enabling intelligent
    routing decisions.
    """
    
    def __init__(self, config: Optional[MetadataConfig] = None):
        self.config = config or MetadataConfig()
        self._cache: Optional[Dict] = None
    
    @property
    def cache_file(self) -> Path:
        return self.config.state_dir / "cloud_stats.json"
    
    @property
    def cache(self) -> Dict:
        """Load or initialize cache."""
        if self._cache is None:
            self._load_cache()
        return self._cache
    
    def _load_cache(self) -> None:
        """Load stats cache from disk."""
        if self.cache_file.exists():
            try:
                self._cache = json.loads(self.cache_file.read_text())
            except (json.JSONDecodeError, IOError):
                self._cache = self._default_cache()
        else:
            self._cache = self._default_cache()
    
    def _default_cache(self) -> Dict:
        """Create default cache structure."""
        return {
            "models": {},
            "fetched_at": None,
            "source": None,
        }
    
    def _save_cache(self) -> None:
        """Save cache to disk."""
        self.config.state_dir.mkdir(parents=True, exist_ok=True)
        self.cache_file.write_text(json.dumps(self._cache, indent=2, default=str))
    
    def is_stale(self) -> bool:
        """Check if cache needs refresh."""
        fetched_at = self.cache.get("fetched_at")
        if not fetched_at:
            return True
        
        try:
            last_fetch = datetime.fromisoformat(fetched_at)
            age = datetime.now() - last_fetch
            return age > timedelta(hours=self.config.cache_ttl_hours)
        except:
            return True
    
    # =========================================================================
    # Query Generation
    # =========================================================================
    
    def get_snowflake_query(self) -> str:
        """
        Generate Snowflake query history query.
        
        Requires model name to be in QUERY_TAG as JSON:
        {{ config(query_tag='{"model": "model_name"}') }}
        """
        return f"""
SELECT 
    PARSE_JSON(QUERY_TAG):model::STRING AS model_name,
    AVG(TOTAL_ELAPSED_TIME) / 1000 AS avg_seconds,
    AVG(BYTES_SPILLED_TO_LOCAL_STORAGE) AS avg_spill_bytes,
    AVG(ROWS_PRODUCED) AS avg_rows_produced,
    COUNT(*) AS run_count,
    MAX(START_TIME)::STRING AS last_run
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME > DATEADD('day', -{self.config.history_days}, CURRENT_TIMESTAMP())
AND QUERY_TAG LIKE '%"model"%'
AND EXECUTION_STATUS = 'SUCCESS'
GROUP BY 1
HAVING model_name IS NOT NULL
"""
    
    def get_bigquery_query(self, project_id: str) -> str:
        """
        Generate BigQuery jobs query.
        
        Uses job labels to identify dbt models:
        {{ config(labels={'dbt_model': 'model_name'}) }}
        """
        return f"""
SELECT
    labels.value AS model_name,
    AVG(TIMESTAMP_DIFF(end_time, start_time, SECOND)) AS avg_seconds,
    AVG(total_bytes_billed) AS avg_bytes_billed,
    COUNT(*) AS run_count,
    MAX(start_time) AS last_run
FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS`,
    UNNEST(labels) AS labels
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {self.config.history_days} DAY)
AND labels.key = 'dbt_model'
AND state = 'DONE'
GROUP BY 1
"""
    
    def get_databricks_query(self) -> str:
        """
        Databricks query for system.query_history.
        
        Requires comment with model name in SQL.
        """
        return f"""
SELECT
    regexp_extract(statement_text, '/\\*.*model=([^,\\*]+).*\\*/', 1) AS model_name,
    AVG(total_elapsed_time_ms / 1000) AS avg_seconds,
    AVG(rows_produced) AS avg_rows_produced,
    COUNT(*) AS run_count,
    MAX(start_time) AS last_run
FROM system.query_history
WHERE start_time > NOW() - INTERVAL {self.config.history_days} DAY
AND status = 'SUCCESS'
GROUP BY 1
HAVING model_name IS NOT NULL AND model_name != ''
"""
    
    # =========================================================================
    # Harvesting
    # =========================================================================
    
    def harvest_snowflake(self, connection: Any) -> Dict[str, ModelStats]:
        """
        Fetch stats from Snowflake.
        
        Args:
            connection: Active Snowflake connection
            
        Returns:
            Dict mapping model names to stats
        """
        query = self.get_snowflake_query()
        
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            
            stats = {}
            for row in rows:
                model_name = row[0]
                if model_name:
                    stats[model_name] = ModelStats(
                        model_name=model_name,
                        avg_seconds=float(row[1] or 0),
                        avg_spill_bytes=float(row[2] or 0),
                        avg_rows_produced=float(row[3] or 0),
                        run_count=int(row[4] or 0),
                        last_run=str(row[5]) if row[5] else None,
                    )
            
            return stats
            
        except Exception as e:
            print(f"Warning: Failed to harvest Snowflake stats: {e}")
            return {}
    
    def update_cache(self, stats: Dict[str, ModelStats], source: str) -> None:
        """Update cache with harvested stats."""
        models = {}
        for name, model_stats in stats.items():
            models[name] = {
                "avg_seconds": model_stats.avg_seconds,
                "avg_spill_bytes": model_stats.avg_spill_bytes,
                "avg_rows_produced": model_stats.avg_rows_produced,
                "run_count": model_stats.run_count,
                "last_run": model_stats.last_run,
            }
        
        self._cache = {
            "models": models,
            "fetched_at": datetime.now().isoformat(),
            "source": source,
        }
        self._save_cache()
    
    # =========================================================================
    # Access Methods
    # =========================================================================
    
    def get_model_stats(self, model_name: str) -> Optional[ModelStats]:
        """Get stats for a specific model."""
        model_data = self.cache.get("models", {}).get(model_name)
        if not model_data:
            return None
        
        return ModelStats(
            model_name=model_name,
            avg_seconds=model_data.get("avg_seconds", 0),
            avg_spill_bytes=model_data.get("avg_spill_bytes", 0),
            avg_rows_produced=model_data.get("avg_rows_produced", 0),
            run_count=model_data.get("run_count", 0),
            last_run=model_data.get("last_run"),
        )
    
    def get_all_stats(self) -> Dict[str, ModelStats]:
        """Get stats for all models."""
        result = {}
        for name in self.cache.get("models", {}).keys():
            stats = self.get_model_stats(name)
            if stats:
                result[name] = stats
        return result
    
    def get_slow_models(self, threshold_seconds: float = 600) -> List[str]:
        """Get list of models that exceed runtime threshold."""
        slow = []
        for name, stats in self.get_all_stats().items():
            if stats.avg_seconds > threshold_seconds:
                slow.append(name)
        return slow


# Singleton
_harvester: Optional[MetadataHarvester] = None


def get_metadata_harvester(config: Optional[MetadataConfig] = None) -> MetadataHarvester:
    """Get or create the metadata harvester singleton."""
    global _harvester
    if _harvester is None:
        _harvester = MetadataHarvester(config)
    return _harvester
