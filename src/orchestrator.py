"""Orchestrator: wires all agents and manages pipeline execution."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

from .agents.archivist import ArchivistAgent
from .utils.logging import get_logger, setup_logging
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
    setup_logging()
    log = get_logger("orchestrator")
    root = Path(repo).resolve()
    run_id = str(uuid.uuid4())[:8]
    log.info("Starting analysis for repo: %s", root, extra={"run_id": run_id})

    # Use per-repo directory if repo_id provided, otherwise fall back to old behavior
    if repo_id:
        cart = get_repo_cart_dir(repo_id)
    else:
        cart = get_cartography_dir(root)
    cart.mkdir(parents=True, exist_ok=True)

    traces: List[Dict] = []
    commit_sha = _git_head(root)
    traces.append({
        "action": "pipeline_start",
        "run_id": run_id,
        "repo": str(root),
        "commit_sha": commit_sha,
        "incremental": incremental,
        "local_only": local_only,
    })

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
            traces.append({
                "action": "incremental_diff",
                "changed_files_count": len(changed_files),
                "changed_files": list(changed_files)[:50],  # cap for trace size
            })
            if not changed_files:
                traces.append({"action": "incremental_skip", "reason": "no changes"})
                log.info("Incremental mode: no changed files since last run, skipping.", extra={"run_id": run_id})
                return OrchestratorResult(kg, modules, datasets, day_one_answers, None)

    error_collector: List[Dict] = []

    # Surveyor
    log.info("Running Surveyor (module analysis)...", extra={"run_id": run_id, "agent": "surveyor"})
    surveyor = SurveyorAgent()
    new_modules = surveyor.run(root, kg, changed_files, error_collector=error_collector)
    if not modules:
        modules = new_modules
    else:
        for k, v in new_modules.items():
            modules[k] = v
    files_by_lang: Dict[str, int] = {}
    for m in modules.values():
        lang = getattr(m, "language", "other")
        files_by_lang[lang] = files_by_lang.get(lang, 0) + 1
    surveyor_skipped = [e for e in error_collector if e.get("agent") == "surveyor"]
    traces.append({
        "action": "surveyor",
        "run_id": run_id,
        "modules_count": len(modules),
        "files_by_language": files_by_lang,
        "files_total": sum(files_by_lang.values()),
        "skipped_files_count": len(surveyor_skipped),
        "skipped_files": surveyor_skipped[:20],
    })
    log.info(
        "Surveyor complete. Modules: %d, skipped: %d",
        len(modules),
        len(surveyor_skipped),
        extra={"run_id": run_id, "agent": "surveyor"},
    )

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
        log.info(
            "Top modules by centrality and velocity: %s",
            [h["path"] for h in hotspots],
            extra={"run_id": run_id},
        )

    # Hydrologist
    log.info("Running Hydrologist (lineage)...", extra={"run_id": run_id, "agent": "hydrologist"})
    hydrologist = HydrologistAgent()
    new_datasets = hydrologist.run(root, kg, changed_files, error_collector=error_collector)
    if not datasets:
        datasets = new_datasets
    else:
        for k, v in new_datasets.items():
            datasets[k] = v
    transformations = [
        n for n, d in kg.lineage_graph.nodes(data=True)
        if d.get("type") == "transformation"
    ]
    source_files = set()
    for n, d in kg.lineage_graph.nodes(data=True):
        if sf := d.get("source_file"):
            source_files.add(sf)
    hydrologist_skipped = [e for e in error_collector if e.get("agent") == "hydrologist"]
    traces.append({
        "action": "hydrologist",
        "run_id": run_id,
        "datasets_count": len(datasets),
        "transformations_count": len(transformations),
        "source_files_analyzed": len(source_files),
        "skipped_files_count": len(hydrologist_skipped),
        "skipped_files": hydrologist_skipped[:20],
    })
    log.info(
        "Hydrologist complete. Datasets: %d, skipped: %d",
        len(datasets),
        len(hydrologist_skipped),
        extra={"run_id": run_id, "agent": "hydrologist"},
    )

    # Semanticist (LLM) and Day-One
    if not local_only:
        log.info("Running Semanticist (LLM enrichment)...", extra={"run_id": run_id, "agent": "semanticist"})
        semanticist = SemanticistAgent()
        semanticist.run(root, modules, error_collector=error_collector)
        modules_with_purpose = sum(1 for m in modules.values() if getattr(m, "purpose_statement", None))
        modules_with_drift = sum(1 for m in modules.values() if getattr(m, "doc_drift", None))
        semanticist_skipped = [e for e in error_collector if e.get("agent") == "semanticist"]
        traces.append({
            "action": "semanticist",
            "run_id": run_id,
            "modules_with_purpose": modules_with_purpose,
            "modules_drift_checked": modules_with_drift,
            "modules_total": len(modules),
            "skipped_modules_count": len(semanticist_skipped),
            "skipped_modules": semanticist_skipped[:20],
        })
    else:
        log.info("Skipping Semanticist (local-only mode).", extra={"run_id": run_id})
    day_one_answers = answer_day_one_questions(kg, modules, datasets)
    total_evidence = sum(len(a.evidence) for a in day_one_answers.values())
    traces.append({
        "action": "day_one",
        "questions_answered": len(day_one_answers),
        "evidence_citations_total": total_evidence,
        "questions": list(day_one_answers.keys()),
    })
    log.info("Day-One onboarding answers synthesized.", extra={"run_id": run_id})

    # Archivist
    log.info("Running Archivist (persisting artifacts)...", extra={"run_id": run_id, "agent": "archivist"})
    traces.append({
        "action": "archivist",
        "artifacts": [
            "CODEBASE.md",
            "onboarding_brief.md",
            "lineage_graph.json",
            "module_graph.json",
            "state.json",
            "cartography_trace.jsonl",
        ],
    })
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
        log.info("Building semantic index (embeddings)...", extra={"run_id": run_id})
        idx.build(modules)
        log.info("Semantic index built.", extra={"run_id": run_id})
    else:
        log.info("Skipping semantic index build (local-only mode).", extra={"run_id": run_id})
    idx_path = cart / "semantic_index" / "index.json"
    idx.save(idx_path)

    pipeline_complete_evt = {
        "action": "pipeline_complete",
        "run_id": run_id,
        "status": "success",
        "modules_count": len(modules),
        "datasets_count": len(datasets),
    }
    ArchivistAgent()._append_trace(cart / "cartography_trace.jsonl", [pipeline_complete_evt])

    log.info(
        "Analysis finished. Artifacts: %s",
        str(cart),
        extra={"run_id": run_id},
    )
    return OrchestratorResult(kg, modules, datasets, day_one_answers, idx)
