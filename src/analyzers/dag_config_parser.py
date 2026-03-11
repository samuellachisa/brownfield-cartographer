from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class DAGConfig:
    path: Path
    config_type: str
    tasks: List[str]
    # Task dependency graph: task -> list of upstream task ids.
    dependencies: Dict[str, List[str]]
    # Best-effort schedule string, when available (e.g. Airflow schedule_interval).
    schedule: Optional[str] = None


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
        dependencies: Dict[str, List[str]] = {}
        schedule: Optional[str] = None

        if "dags" in data or "schedule_interval" in data:
            # Airflow-style config
            config_type = "airflow"
            tasks_section = data.get("tasks", {})
            if isinstance(tasks_section, dict):
                tasks = list(tasks_section.keys())
                # Look for common dependency keys on each task definition.
                for task_name, task_cfg in tasks_section.items():
                    if not isinstance(task_cfg, dict):
                        continue
                    upstream: List[str] = []
                    for key in ("upstream", "upstreams", "depends_on", "dependencies"):
                        val = task_cfg.get(key)
                        if isinstance(val, list):
                            upstream.extend(str(v) for v in val)
                        elif isinstance(val, str):
                            upstream.append(val)
                    if upstream:
                        # De-duplicate while preserving order.
                        seen: set[str] = set()
                        uniq: List[str] = []
                        for u in upstream:
                            if u not in seen:
                                seen.add(u)
                                uniq.append(u)
                        dependencies[task_name] = uniq
            else:
                tasks = []
            schedule = (
                data.get("schedule_interval")
                or data.get("schedule")
                or data.get("cron")
            )
        elif "models" in data or "sources" in data:
            # dbt-style project/model config
            config_type = "dbt"
            models_section = data.get("models", {})
            if isinstance(models_section, dict):
                tasks = list(models_section.keys())
                # Optional: model-level dependencies if expressed explicitly.
                for model_name, model_cfg in models_section.items():
                    if not isinstance(model_cfg, dict):
                        continue
                    deps = model_cfg.get("depends_on") or model_cfg.get("parents")
                    if isinstance(deps, dict):
                        # dbt graph metadata often nests under "nodes" / "sources"
                        node_ids: List[str] = []
                        for _, values in deps.items():
                            if isinstance(values, list):
                                node_ids.extend(str(v) for v in values)
                        if node_ids:
                            dependencies[model_name] = node_ids
                    elif isinstance(deps, list):
                        dependencies[model_name] = [str(d) for d in deps]
            else:
                tasks = []
        else:
            config_type = "unknown"
            tasks = []

        return DAGConfig(
            path=path,
            config_type=config_type,
            tasks=tasks,
            dependencies=dependencies,
            schedule=schedule,
        )

