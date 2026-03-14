"""Day-One question synthesis via LLM."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from ..config import CartographerConfig, load_config
from ..models import DayOneAnswer, Evidence
from .semanticist import LLMClient, LLMConfig, ContextWindowBudget


def _evidence_from_graph_and_modules(
    graph: Any,
    modules: Dict[str, Any],
    question: str,
) -> List[Evidence]:
    """Build evidence list with actual file paths and line ranges from graph/modules."""
    evidence: List[Evidence] = []
    seen: set[tuple[str, tuple[int, int]]] = set()

    def add_ev(file: str, line_range: tuple[int, int], confidence: float = 0.85) -> None:
        key = (file, line_range)
        if key not in seen:
            seen.add(key)
            evidence.append(
                Evidence(
                    file=file,
                    line_range=line_range,
                    analysis_method="static",
                    agent="semanticist",
                    confidence=confidence,
                )
            )

    g = getattr(graph, "lineage_graph", None)
    if not g:
        return evidence

    # Q1: ingestion path – transformations that read from source datasets
    if "ingestion" in question.lower() or "primary" in question.lower():
        sources = list(graph.find_sources())[:5] if hasattr(graph, "find_sources") else []
        for ds in sources:
            for _, _, d in g.out_edges(ds, data=True):
                sf, lr = d.get("source_file"), d.get("line_range", (1, 1))
                if sf and lr:
                    add_ev(sf, tuple(lr), 0.9)

    # Q2: critical outputs – transformations that write to sink datasets
    if "critical" in question.lower() or "output" in question.lower() or "sink" in question.lower():
        sinks = list(graph.find_sinks())[:5] if hasattr(graph, "find_sinks") else []
        for ds in sinks:
            for _, _, d in g.in_edges(ds, data=True):
                sf, lr = d.get("source_file"), d.get("line_range", (1, 1))
                if sf and lr:
                    add_ev(sf, tuple(lr), 0.9)

    # Q3: blast radius – top modules by PageRank
    if "blast" in question.lower():
        pr = graph.pagerank() if hasattr(graph, "pagerank") else {}
        for path, _ in sorted(pr.items(), key=lambda x: x[1], reverse=True)[:5]:
            if path in modules:
                add_ev(path, (1, 1), 0.85)

    # Q4: business logic – high-complexity modules
    if "business" in question.lower() or "concentrated" in question.lower():
        by_complexity = sorted(
            modules.values(),
            key=lambda m: getattr(m, "complexity_score", 0),
            reverse=True,
        )[:5]
        for m in by_complexity:
            add_ev(m.path, (1, 1), 0.8)

    # Q5: changed frequently – high-velocity modules
    if "changed" in question.lower() or "frequently" in question.lower() or "90" in question.lower():
        by_vel = sorted(
            modules.values(),
            key=lambda m: getattr(m, "change_velocity_30d", 0),
            reverse=True,
        )[:5]
        for m in by_vel:
            if getattr(m, "change_velocity_30d", 0) > 0:
                add_ev(m.path, (1, 1), 0.85)

    return evidence


def answer_day_one_questions(
    graph: Any,
    modules: Dict[str, Any],
    datasets: Dict[str, Any],
    llm_config: LLMConfig | None = None,
    global_config: CartographerConfig | None = None,
) -> Dict[str, DayOneAnswer]:
    """Synthesis prompt to answer the Five FDE Day-One Questions."""
    config = llm_config or LLMConfig()
    if not config.api_key:
        return _heuristic_answers(modules, graph)

    try:
        client = LLMClient(config)
    except Exception:
        return _heuristic_answers(modules, graph)

    cfg = global_config or load_config(getattr(graph, "repo_root", None) or Path("."))  # type: ignore[arg-type]
    budget = ContextWindowBudget(
        max_bulk=cfg.budget.max_bulk_tokens,
        max_synthesis=cfg.budget.max_synthesis_tokens,
    )

    pr = graph.pagerank() if hasattr(graph, "pagerank") else {}
    top_modules = sorted(pr.items(), key=lambda x: x[1], reverse=True)[:5]
    sources = list(graph.find_sources())[:10] if hasattr(graph, "find_sources") else []
    sinks = list(graph.find_sinks())[:10] if hasattr(graph, "find_sinks") else []
    high_vel = sorted(
        modules.values(),
        key=lambda m: getattr(m, "change_velocity_30d", 0),
        reverse=True,
    )[:10]

    context = (
        "## Module graph (top by PageRank)\n"
        + "\n".join(f"- {n} (score={s:.4f})" for n, s in top_modules)
        + "\n\n## Data sources (entry points)\n"
        + "\n".join(f"- {s}" for s in sources)
        + "\n\n## Data sinks (exit points)\n"
        + "\n".join(f"- {s}" for s in sinks)
        + "\n\n## High-velocity files (recent changes)\n"
        + "\n".join(f"- {m.path} ({getattr(m,'change_velocity_30d',0)} commits)" for m in high_vel)
    )

    questions = [
        "What is the primary data ingestion path?",
        "What are the 3-5 most critical output datasets/endpoints?",
        "What is the blast radius if the most critical module fails?",
        "Where is business logic concentrated vs. distributed?",
        "What has changed most frequently in the last 90 days?",
    ]

    prompt = (
        "You are a senior FDE analyzing a data engineering codebase.\n\n"
        "Context:\n"
        f"{context}\n\n"
        "Answer each of the Five Day-One Questions in 2-4 sentences. "
        "Be specific: cite module paths, dataset names, and file paths when possible. "
        "Format your response as JSON with keys matching each question exactly, "
        "each value being the answer string."
    )
    for q in questions:
        prompt += f'\n- "{q}"'

    est_tokens = max(1, len(prompt) // 4)
    if not budget.can_spend_synthesis(est_tokens):
        return _heuristic_answers(modules, graph)

    try:
        client._rate_limiter.wait()
        r = client._client.chat.completions.create(
            model=client.config.model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1024,
            temperature=0.2,
        )
        raw = r.choices[0].message.content or "{}"
        # Try to parse JSON
        import json
        # Handle markdown code blocks
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        data = json.loads(raw.strip())
        budget.record_synthesis(est_tokens)
    except Exception:
        return _heuristic_answers(modules, graph)

    results: Dict[str, DayOneAnswer] = {}
    fallback_ev = Evidence(
        file="synthesis",
        line_range=(1, 1),
        analysis_method="llm",
        agent="semanticist",
        confidence=0.8,
    )
    for q in questions:
        ans = data.get(q, "(Answer not generated)")
        if isinstance(ans, str):
            ev_list = _evidence_from_graph_and_modules(graph, modules, q)
            if not ev_list:
                ev_list = [fallback_ev]
            results[q] = DayOneAnswer(question=q, answer=ans, evidence=ev_list)
    return results


def _heuristic_answers(
    modules: Dict[str, Any],
    graph: Any | None = None,
) -> Dict[str, DayOneAnswer]:
    def _ev_for(q: str) -> List[Evidence]:
        if graph and modules:
            return _evidence_from_graph_and_modules(graph, modules, q)
        return [
            Evidence(
                file="heuristic",
                line_range=(1, 1),
                analysis_method="static",
                agent="semanticist",
                confidence=0.3,
            )
        ]

    q1 = "What is the primary data ingestion path?"
    q2 = "What are the 3-5 most critical output datasets/endpoints?"
    q3 = "What is the blast radius if the most critical module fails?"
    q4 = "Where is business logic concentrated vs. distributed?"
    q5 = "What has changed most frequently in the last 90 days?"
    return {
        q1: DayOneAnswer(question=q1, answer="Set OPENAI_API_KEY for LLM-based synthesis.", evidence=_ev_for(q1)),
        q2: DayOneAnswer(question=q2, answer="Run with LLM enabled for full Day-One answers.", evidence=_ev_for(q2)),
        q3: DayOneAnswer(question=q3, answer="Use 'cartographer query blast-radius' for specific modules.", evidence=_ev_for(q3)),
        q4: DayOneAnswer(question=q4, answer="Domain clusters appear in CODEBASE.md when LLM is configured.", evidence=_ev_for(q4)),
        q5: DayOneAnswer(question=q5, answer="High-velocity files are listed in CODEBASE.md.", evidence=_ev_for(q5)),
    }
