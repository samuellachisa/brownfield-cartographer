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
    get_repo_cart_dir,
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
    repo_id: Optional[str] = None,
    incremental: bool = False,
    local_only: bool = False,
) -> OrchestratorResult:
    """
    Full pipeline: Surveyor → Hydrologist → Semanticist → Day-One → Archivist.

    If incremental and prior run exists, only re-analyze changed files.
    If local_only, skip LLM (Semanticist, Day-One synthesis).
    """
    root = Path(repo).resolve()
    print(f"[cartographer] Starting analysis for repo: {root}")
    
    # Use per-repo directory if repo_id provided, otherwise fall back to old behavior
    if repo_id:
        cart = get_repo_cart_dir(repo_id)
    else:
        cart = get_cartography_dir(root)
    cart.mkdir(parents=True, exist_ok=True)

    traces: List[Dict] = []
    run_id = str(uuid.uuid4())[:8]
    commit_sha = _git_head(root)

    # Try incremental: load prior state and changed files
    prior_meta = load_run_metadata(cart)
    changed_files: Optional[Set[str]] = None
    kg = KnowledgeGraph()
    modules: Dict[str, ModuleNode] = {}
    datasets: Dict[str, DatasetNode] = {}
    day_one_answers: Dict[str, DayOneAnswer] = {}

    # Write initial run metadata with status=running so the UI can show in-progress state.
    save_run_metadata(
        cart,
        RunMetadata(
            run_id=run_id,
            repo_path=str(root),
            commit_sha=commit_sha,
            timestamp=datetime.utcnow().isoformat(),
            incremental=incremental,
            changed_files=[],
            status="running",
        ),
    )

    if incremental and prior_meta and prior_meta.commit_sha:
        cached = load_state(cart)
        if cached:
            kg, modules, datasets, day_one_answers = cached
            changed_files = set(_git_changed_files(root, prior_meta.commit_sha))
            if not changed_files:
                traces.append({"action": "incremental_skip", "reason": "no changes"})
                print("[cartographer] Incremental mode: no changed files since last run, skipping.")
                return OrchestratorResult(kg, modules, datasets, day_one_answers, None)

    # Surveyor
    print("[cartographer] Running Surveyor (module analysis)...")
    surveyor = SurveyorAgent()
    new_modules = surveyor.run(root, kg, changed_files)
    if not modules:
        modules = new_modules
    else:
        for k, v in new_modules.items():
            modules[k] = v
    traces.append({"action": "surveyor", "modules_count": len(modules)})
    print(f"[cartographer] Surveyor complete. Modules analyzed: {len(modules)}")

    # Derive a concise report of the "hottest" modules by combining structural
    # centrality (PageRank computed in Surveyor) with recent git velocity.
    hotspots = sorted(
        (
            {
                "path": path,
                "pagerank": float(mod.pagerank),
                "change_velocity_30d": int(mod.change_velocity_30d),
            }
            for path, mod in modules.items()
        ),
        key=lambda x: (x["pagerank"], x["change_velocity_30d"]),
        reverse=True,
    )[:10]

    traces.append(
        {
            "action": "surveyor_metrics",
            "pagerank_computed_for": len([m for m in modules.values() if m.pagerank > 0]),
            "scc_components": None,
            "hotspots": hotspots,
        }
    )
    if hotspots:
        print("[cartographer] Top modules by centrality and velocity:")
        for h in hotspots:
            print(
                f"  - {h['path']}: pagerank={h['pagerank']:.4f}, "
                f"changes_30d={h['change_velocity_30d']}"
            )

    # Hydrologist
    print("[cartographer] Running Hydrologist (SQL lineage)...")
    hydrologist = HydrologistAgent()
    new_datasets = hydrologist.run(root, kg, changed_files)
    if not datasets:
        datasets = new_datasets
    else:
        for k, v in new_datasets.items():
            datasets[k] = v
    traces.append({"action": "hydrologist", "datasets_count": len(datasets)})
    print(f"[cartographer] Hydrologist complete. Datasets discovered: {len(datasets)}")

    # Semanticist (LLM) and Day-One
    if not local_only:
        print("[cartographer] Running Semanticist (LLM enrichment)...")
        semanticist = SemanticistAgent()
        semanticist.run(root, modules)
        traces.append({"action": "semanticist"})
    else:
        print("[cartographer] Skipping Semanticist (local-only mode).")
    day_one_answers = answer_day_one_questions(kg, modules, datasets)
    traces.append({"action": "day_one"})
    print("[cartographer] Day-One onboarding answers synthesized.")

    # Archivist
    print("[cartographer] Running Archivist (persisting artifacts)...")
    archivist = ArchivistAgent()
    archivist.run(cart, kg, modules, datasets, day_one_answers, traces)

    # Persist
    kg.write_module_graph(cart / "module_graph.json")
    kg.write_lineage_graph(cart / "lineage_graph.json")
    save_state(
        cart,
        kg,
        modules,
        datasets,
        day_one_answers={k: v.model_dump(mode="json") for k, v in day_one_answers.items()},
    )
    save_run_metadata(
        cart,
        RunMetadata(
            run_id=run_id,
            repo_path=str(root),
            commit_sha=commit_sha,
            timestamp=datetime.utcnow().isoformat(),
            incremental=incremental,
            changed_files=list(changed_files or []),
            status="success",
        ),
    )

    # Semantic index (skip when local_only to avoid embedding API calls)
    idx = SemanticIndex()
    if not local_only:
        print("[cartographer] Building semantic index (embeddings)...")
        idx.build(modules)
        print("[cartographer] Semantic index built.")
    else:
        print("[cartographer] Skipping semantic index build (local-only mode).")
    idx_path = cart / "semantic_index" / "index.json"
    idx.save(idx_path)

    print(f"[cartographer] Analysis finished. Artifacts written to: {cart}")
    return OrchestratorResult(kg, modules, datasets, day_one_answers, idx)
