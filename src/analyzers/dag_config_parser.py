from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import yaml


@dataclass
class DAGConfig:
    path: Path
    config_type: str
    tasks: List[str]


class DAGConfigParser:
    """
    Airflow/dbt YAML config parsing.

    This is intentionally shallow but provides hooks to detect pipeline topology
    and CONFIGURES edges for the knowledge graph.
    """

    def parse(self, path: Path) -> DAGConfig | None:
        text = path.read_text(encoding="utf-8", errors="ignore")
        try:
            data: Dict[str, Any] = yaml.safe_load(text) or {}
        except Exception:
            return None

        # Heuristics
        if "dags" in data or "schedule_interval" in data:
            config_type = "airflow"
            tasks = list(data.get("tasks", {}).keys()) if isinstance(data.get("tasks"), dict) else []
        elif "models" in data or "sources" in data:
            config_type = "dbt"
            tasks = list(data.get("models", {}).keys()) if isinstance(data.get("models"), dict) else []
        else:
            config_type = "unknown"
            tasks = []

        return DAGConfig(path=path, config_type=config_type, tasks=tasks)

