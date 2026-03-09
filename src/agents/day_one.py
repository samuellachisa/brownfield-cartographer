"""Day-One question synthesis via LLM."""

from __future__ import annotations

from typing import Any, Dict

from ..models import DayOneAnswer, Evidence
from .semanticist import LLMClient, LLMConfig


def answer_day_one_questions(
    graph: Any,
    modules: Dict[str, Any],
    datasets: Dict[str, Any],
    llm_config: LLMConfig | None = None,
) -> Dict[str, DayOneAnswer]:
    """Synthesis prompt to answer the Five FDE Day-One Questions."""
    config = llm_config or LLMConfig()
    if not config.api_key:
        return _heuristic_answers(modules)

    try:
        client = LLMClient(config)
    except Exception:
        return _heuristic_answers(modules)

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
    except Exception:
        return _heuristic_answers(modules)

    results: Dict[str, DayOneAnswer] = {}
    ev = Evidence(
        file="synthesis",
        line_range=(1, 1),
        analysis_method="llm",
        agent="semanticist",
        confidence=0.8,
    )
    for q in questions:
        ans = data.get(q, "(Answer not generated)")
        if isinstance(ans, str):
            results[q] = DayOneAnswer(question=q, answer=ans, evidence=[ev])
    return results


def _heuristic_answers(modules: Dict[str, Any]) -> Dict[str, DayOneAnswer]:
    ev = Evidence(
        file="heuristic",
        line_range=(1, 1),
        analysis_method="static",
        agent="semanticist",
        confidence=0.3,
    )
    q1 = "What is the primary data ingestion path?"
    q2 = "What are the 3-5 most critical output datasets/endpoints?"
    q3 = "What is the blast radius if the most critical module fails?"
    q4 = "Where is business logic concentrated vs. distributed?"
    q5 = "What has changed most frequently in the last 90 days?"
    return {
        q1: DayOneAnswer(question=q1, answer="Set OPENAI_API_KEY for LLM-based synthesis.", evidence=[ev]),
        q2: DayOneAnswer(question=q2, answer="Run with LLM enabled for full Day-One answers.", evidence=[ev]),
        q3: DayOneAnswer(question=q3, answer="Use 'cartographer query blast-radius' for specific modules.", evidence=[ev]),
        q4: DayOneAnswer(question=q4, answer="Domain clusters appear in CODEBASE.md when LLM is configured.", evidence=[ev]),
        q5: DayOneAnswer(question=q5, answer="High-velocity files are listed in CODEBASE.md.", evidence=[ev]),
    }
