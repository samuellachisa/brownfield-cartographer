"""Navigator agent: query interface with 4 tools."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from ..graph.knowledge_graph import KnowledgeGraph
from ..models import Evidence, ModuleNode
from ..semantic_index import SemanticIndex
from ..storage import get_cartography_dir


class NavigatorAgent:
    """Query interface over the knowledge graph with evidence-backed answers."""

    def __init__(self, semantic_index: Optional[SemanticIndex] = None) -> None:
        self._index = semantic_index

    def find_implementation(
        self,
        concept: str,
        modules: Dict[str, ModuleNode],
        index: Optional[SemanticIndex] = None,
        top_k: int = 10,
    ) -> Dict[str, Any]:
        """Semantic search over purpose statements."""
        idx = index or self._index
        if idx:
            hits = idx.search(concept, top_k=top_k)
            results = []
            for path, score in hits:
                m = modules.get(path)
                if m:
                    ev = Evidence(
                        file=path,
                        line_range=(1, 1),
                        analysis_method="llm",
                        agent="navigator",
                        confidence=float(score),
                    )
                    results.append({
                        "module_path": path,
                        "purpose_statement": m.purpose_statement,
                        "score": score,
                        "evidence": [ev.model_dump()],
                    })
            return {"answer": results, "evidence": [e["evidence"][0] for e in results]}
        # Fallback: string match
        concept_lower = concept.lower()
        matches = [
            m for m in modules.values()
            if concept_lower in (m.purpose_statement or "").lower() or concept_lower in m.path.lower()
        ]
        matches.sort(key=lambda m: (m.change_velocity_30d, m.complexity_score), reverse=True)
        results = []
        for m in matches[:top_k]:
            ev = Evidence(
                file=m.path,
                line_range=(1, 1),
                analysis_method="static",
                agent="navigator",
                confidence=0.5,
            )
            results.append({
                "module_path": m.path,
                "purpose_statement": m.purpose_statement,
                "score": m.change_velocity_30d + m.complexity_score,
                "evidence": [ev.model_dump()],
            })
        return {"answer": results, "evidence": [e["evidence"][0] for e in results]}

    def trace_lineage(
        self,
        graph: KnowledgeGraph,
        dataset: str,
        direction: Literal["upstream", "downstream"] = "downstream",
    ) -> Dict[str, Any]:
        """Traverse lineage graph with file:line citations."""
        ds_id = dataset if dataset in graph.lineage_graph else None
        if not ds_id:
            return {"answer": f"Dataset '{dataset}' not found.", "evidence": []}
        sub = graph.blast_radius(ds_id, direction=direction)
        nodes = [{"id": n, **d} for n, d in sub.nodes(data=True)]
        edges = [{"source": u, "target": v, **d} for u, v, d in sub.edges(data=True)]
        evidence = []
        for _, _, d in sub.edges(data=True):
            if "source_file" in d:
                evidence.append({
                    "file": d["source_file"],
                    "line_range": d.get("line_range", (0, 0)),
                    "analysis_method": "static",
                    "agent": "navigator",
                    "confidence": 0.9,
                })
        return {"answer": {"direction": direction, "nodes": nodes, "edges": edges}, "evidence": evidence}

    def blast_radius(
        self,
        graph: KnowledgeGraph,
        module_path: str,
        modules: Dict[str, ModuleNode],
    ) -> Dict[str, Any]:
        """Downstream impact if module changes."""
        mod = modules.get(module_path)
        if not mod:
            return {"answer": f"Module '{module_path}' not found.", "evidence": []}
        # Find datasets touched by this module (via transformations)
        impacted = set()
        for n, d in graph.lineage_graph.nodes(data=True):
            if d.get("type") == "transformation" and d.get("source_file", "").endswith(module_path.split("/")[-1]):
                sub = graph.blast_radius(n, direction="downstream")
                for nn, dd in sub.nodes(data=True):
                    if dd.get("type") == "dataset":
                        impacted.add(nn)
        ev = Evidence(
            file=module_path,
            line_range=(1, 1),
            analysis_method="static",
            agent="navigator",
            confidence=0.85,
        )
        return {
            "answer": {
                "module": module_path,
                "impacted_datasets": sorted(impacted),
            },
            "evidence": [ev.model_dump()],
        }

    def explain_module(self, module_path: str, modules: Dict[str, ModuleNode]) -> Dict[str, Any]:
        """Generative explanation from static + semantic data."""
        mod = modules.get(module_path)
        if not mod:
            return {"answer": f"Module '{module_path}' not found.", "evidence": []}
        answer = (
            f"Module `{module_path}` ({mod.language}) has complexity {mod.complexity_score:.1f} "
            f"and {mod.change_velocity_30d} changes in the last 30 days. "
        )
        if mod.purpose_statement:
            answer += f"Purpose: {mod.purpose_statement} "
        if mod.domain_cluster:
            answer += f"Domain: {mod.domain_cluster}. "
        if mod.is_dead_code_candidate:
            answer += "Flagged as potential dead code (no inbound references). "
        ev = Evidence(
            file=module_path,
            line_range=(1, 1),
            analysis_method="static",
            agent="navigator",
            confidence=0.9,
        )
        return {"answer": answer.strip(), "evidence": [ev.model_dump()]}
