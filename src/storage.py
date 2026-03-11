"""Persistence layer for run metadata and cached analysis state."""

from __future__ import annotations

import json
import subprocess
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .graph.knowledge_graph import KnowledgeGraph
from .models import DayOneAnswer, DatasetNode, ModuleNode


@dataclass
class RunMetadata:
    run_id: str
    repo_path: str
    commit_sha: Optional[str]
    timestamp: str
    incremental: bool = False
    changed_files: List[str] = field(default_factory=list)
    status: str = "running"  # running | success | error


def _git_head(repo_root: Path) -> Optional[str]:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=str(repo_root),
            stderr=subprocess.DEVNULL,
        )
        return out.decode("utf-8", errors="ignore").strip()
    except Exception:
        return None


def _git_changed_files(repo_root: Path, since_commit: str) -> List[str]:
    try:
        out = subprocess.check_output(
            ["git", "diff", "--name-only", since_commit, "HEAD"],
            cwd=str(repo_root),
            stderr=subprocess.DEVNULL,
        )
        return [f.strip() for f in out.decode("utf-8", errors="ignore").splitlines() if f.strip()]
    except Exception:
        return []


def get_cartography_dir(repo_path: Path) -> Path:
    return Path(repo_path).resolve() / ".cartography"


def get_repo_cart_dir(repo_id: str) -> Path:
    """Get the per-repo cartography directory under .cartography/repos/<repo_id>."""
    from pathlib import Path
    project_root = Path(__file__).resolve().parents[1]
    return project_root / ".cartography" / "repos" / repo_id


def get_runs_path(cart_path: Path) -> Path:
    """Get runs.json path from a cartography directory."""
    return cart_path / "runs.json"


def load_run_metadata(cart_path: Path) -> Optional[RunMetadata]:
    """Load the most recent run metadata from a cartography directory."""
    path = get_runs_path(cart_path)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        runs = data.get("runs", [])
        if not runs:
            return None
        latest = runs[-1]
        return RunMetadata(
            run_id=latest["run_id"],
            repo_path=latest["repo_path"],
            commit_sha=latest.get("commit_sha"),
            timestamp=latest["timestamp"],
            incremental=latest.get("incremental", False),
            changed_files=latest.get("changed_files", []),
            status=latest.get("status", "success"),
        )
    except Exception:
        return None


def save_run_metadata(cart_path: Path, meta: RunMetadata) -> None:
    """Save run metadata to a cartography directory."""
    cart_path.mkdir(parents=True, exist_ok=True)
    path = get_runs_path(cart_path)
    runs: List[Dict] = []
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            runs = data.get("runs", [])
        except Exception:
            pass
    runs.append(asdict(meta))
    path.write_text(json.dumps({"runs": runs}, indent=2), encoding="utf-8")

    # Also update global repos registry so the UI can show this repo and status.
    try:
        project_root = Path(__file__).resolve().parents[1]
        registry_path = project_root / "repos.json"
        registry: Dict[str, Any] = {"repos": []}
        if registry_path.exists():
            try:
                registry = json.loads(registry_path.read_text(encoding="utf-8"))
            except Exception:
                registry = {"repos": []}
        repos = registry.get("repos", [])
        updated = False
        for r in repos:
            if r.get("path") == meta.repo_path:
                r["status"] = meta.status
                r["last_run_at"] = meta.timestamp
                updated = True
                break
        if not updated:
            repos.append(
                {
                    "id": Path(meta.repo_path).name,
                    "path": meta.repo_path,
                    "status": meta.status,
                    "last_run_at": meta.timestamp,
                }
            )
        registry["repos"] = repos
        registry_path.write_text(json.dumps(registry, indent=2), encoding="utf-8")
    except Exception:
        # Registry is best-effort; ignore errors so analysis still succeeds.
        pass


def _serialize_datetime(obj: Any) -> str:
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def save_state(
    cart_path: Path,
    graph: KnowledgeGraph,
    modules: Dict[str, ModuleNode],
    datasets: Dict[str, DatasetNode],
    day_one_answers: Optional[Dict[str, Any]] = None,
) -> None:
    """Save state to a cartography directory."""
    cart_path.mkdir(parents=True, exist_ok=True)
    state_path = cart_path / "state.json"

    modules_data = {k: m.model_dump(mode="json") for k, m in modules.items()}
    datasets_data = {k: d.model_dump(mode="json") for k, d in datasets.items()}

    payload = {
        "graph": graph.to_json(),
        "modules": modules_data,
        "datasets": datasets_data,
        "day_one_answers": day_one_answers or {},
    }
    state_path.write_text(
        json.dumps(payload, indent=2, default=_serialize_datetime),
        encoding="utf-8",
    )


def load_state(
    cart_path: Path,
) -> Optional[tuple[KnowledgeGraph, Dict[str, ModuleNode], Dict[str, DatasetNode], Dict[str, Any]]]:
    """Load cached state from a cartography directory. Returns None if not found or invalid."""
    state_path = cart_path / "state.json"
    if not state_path.exists():
        return None
    try:
        data = json.loads(state_path.read_text(encoding="utf-8"))
        kg = KnowledgeGraph()
        # Rebuild graphs from JSON
        for n in data["graph"]["module_graph"].get("nodes", []):
            nid = n.pop("id", None)
            if nid:
                kg.module_graph.add_node(nid, **n)
        for e in data["graph"]["module_graph"].get("edges", []):
            src, tgt = e.pop("source", None), e.pop("target", None)
            if src and tgt:
                kg.module_graph.add_edge(src, tgt, **e)
        for n in data["graph"].get("lineage_graph", {}).get("nodes", []):
            nid = n.pop("id", None)
            if nid:
                kg.lineage_graph.add_node(nid, **n)
        for e in data["graph"]["lineage_graph"].get("edges", []):
            src, tgt = e.pop("source", None), e.pop("target", None)
            if src and tgt:
                kg.lineage_graph.add_edge(src, tgt, **e)

        modules = {k: ModuleNode.model_validate(v) for k, v in data.get("modules", {}).items()}
        datasets = {k: DatasetNode.model_validate(v) for k, v in data.get("datasets", {}).items()}
        day_one_raw = data.get("day_one_answers", {})
        day_one = {k: DayOneAnswer.model_validate(v) for k, v in day_one_raw.items()}
        return kg, modules, datasets, day_one
    except Exception:
        return None
