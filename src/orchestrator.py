"""Orchestrator: wires all agents and manages pipeline execution."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

from .agents.archivist import ArchivistAgent
from .agents.day_one import answer_day_one_questions
from .agents.hydrologist import HydrologistAgent
from .agents.semanticist import SemanticistAgent
from .agents.surveyor import SurveyorAgent
from .graph.knowledge_graph import KnowledgeGraph
from .models import DayOneAnswer, DatasetNode, ModuleNode
from .semantic_index import SemanticIndex
from .storage import (
    _git_changed_files,
    _git_head,
    get_cartography_dir,
    load_run_metadata,
    load_state,
    save_run_metadata,
    save_state,
    RunMetadata,
)


@dataclass
class OrchestratorResult:
    graph: KnowledgeGraph
    modules: Dict[str, ModuleNode]
    datasets: Dict[str, DatasetNode]
    day_one_answers: Dict[str, DayOneAnswer]
    semantic_index: Optional[SemanticIndex]


def run_analysis(
    repo: str,
    incremental: bool = False,
    local_only: bool = False,
) -> OrchestratorResult:
    """
    Full pipeline: Surveyor → Hydrologist → Semanticist → Day-One → Archivist.

    If incremental and prior run exists, only re-analyze changed files.
    If local_only, skip LLM (Semanticist, Day-One synthesis).
    """
    root = Path(repo).resolve()
    cart = get_cartography_dir(root)
    cart.mkdir(parents=True, exist_ok=True)

    traces: List[Dict] = []
    run_id = str(uuid.uuid4())[:8]
    commit_sha = _git_head(root)

    # Try incremental: load prior state and changed files
    prior_meta = load_run_metadata(root)
    changed_files: Optional[Set[str]] = None
    kg = KnowledgeGraph()
    modules: Dict[str, ModuleNode] = {}
    datasets: Dict[str, DatasetNode] = {}
    day_one_answers: Dict[str, DayOneAnswer] = {}

    if incremental and prior_meta and prior_meta.commit_sha:
        cached = load_state(root)
        if cached:
            kg, modules, datasets, day_one_answers = cached
            changed_files = set(_git_changed_files(root, prior_meta.commit_sha))
            if not changed_files:
                traces.append({"action": "incremental_skip", "reason": "no changes"})
                return OrchestratorResult(kg, modules, datasets, day_one_answers, None)

    # Surveyor
    surveyor = SurveyorAgent()
    new_modules = surveyor.run(root, kg, changed_files)
    if not modules:
        modules = new_modules
    else:
        for k, v in new_modules.items():
            modules[k] = v
    traces.append({"action": "surveyor", "modules_count": len(modules)})

    # Hydrologist
    hydrologist = HydrologistAgent()
    new_datasets = hydrologist.run(root, kg, changed_files)
    if not datasets:
        datasets = new_datasets
    else:
        for k, v in new_datasets.items():
            datasets[k] = v
    traces.append({"action": "hydrologist", "datasets_count": len(datasets)})

    # Dead code detection
    for path, module in modules.items():
        if path not in kg.module_graph:
            continue
        has_incoming = any(
            d.get("type") in {"IMPORTS", "CALLS"}
            for _, _, d in kg.module_graph.in_edges(path, data=True)
        )
        if not has_incoming:
            module.is_dead_code_candidate = True
            if path in kg.module_graph.nodes:
                kg.module_graph.nodes[path]["is_dead_code_candidate"] = True

    # Semanticist (LLM) and Day-One
    if not local_only:
        semanticist = SemanticistAgent()
        semanticist.run(root, modules)
        traces.append({"action": "semanticist"})
    day_one_answers = answer_day_one_questions(kg, modules, datasets)
    traces.append({"action": "day_one"})

    # Archivist
    archivist = ArchivistAgent()
    archivist.run(root, kg, modules, datasets, day_one_answers, traces)

    # Persist
    kg.write_module_graph(cart / "module_graph.json")
    kg.write_lineage_graph(cart / "lineage_graph.json")
    save_state(
        root,
        kg,
        modules,
        datasets,
        day_one_answers={k: v.model_dump(mode="json") for k, v in day_one_answers.items()},
    )
    save_run_metadata(root, RunMetadata(
        run_id=run_id,
        repo_path=str(root),
        commit_sha=commit_sha,
        timestamp=datetime.utcnow().isoformat(),
        incremental=incremental,
        changed_files=list(changed_files or []),
    ))

    # Semantic index
    idx = SemanticIndex()
    idx.build(modules)
    idx_path = cart / "semantic_index" / "index.json"
    idx.save(idx_path)

    return OrchestratorResult(kg, modules, datasets, day_one_answers, idx)
