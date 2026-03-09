# Brownfield Cartographer – Production & Enterprise Roadmap

Implementation plan for making the Cartographer production-ready and enterprise-grade.

---

## Phase 1: Foundation (Persistence + Incremental)

**Goal:** Stop re-running full analysis on every query. Enable fast iterations.

| # | Task | Effort | Deps | Output |
|---|------|--------|------|--------|
| 1.1 | Add run metadata storage | 0.5d | — | ` runs.json` or SQLite table with `run_id`, `repo_path`, `commit_sha`, `timestamp` |
| 1.2 | Serialize full `OrchestratorResult` (graph + modules + datasets) to disk | 0.5d | 1.1 | `.cartography/state.json` or `.cartography/cache/` |
| 1.3 | Implement `load_state(repo) -> OrchestratorResult \| None` | 0.5d | 1.2 | CLI/query can reuse prior run |
| 1.4 | Incremental mode: `git diff --name-only HEAD <last_commit>` for changed files | 1d | 1.1 | `--incremental` flag; only re-parse changed files |
| 1.5 | Merge incremental results into existing graph; mark stale nodes | 1d | 1.4 | Graph stays coherent across runs |

**Deliverable:** `cartographer analyze --incremental`; `cartographer query` loads from cache when available.

---

## Phase 2: Archivist & Living Context

**Goal:** Produce the documents FDEs actually use.

| # | Task | Effort | Deps | Output |
|---|------|--------|------|--------|
| 2.1 | `ArchivistAgent` – write `CODEBASE.md` | 1d | Phase 1 | Sections: Architecture Overview, Critical Path (top 5 by PageRank), Data Sources & Sinks, Known Debt (SCCs + dead code), High-Velocity Files |
| 2.2 | `ArchivistAgent` – write `onboarding_brief.md` | 1d | 2.1, Semanticist | Five Day-One answers with evidence citations |
| 2.3 | `ArchivistAgent` – write `cartography_trace.jsonl` | 0.5d | — | Append-only log: `{action, evidence_source, confidence, timestamp}` |
| 2.4 | Wire Archivist into orchestrator after Semanticist | 0.5d | 2.1–2.3 | Full pipeline: Surveyor → Hydrologist → Semanticist → Archivist |

**Deliverable:** Every `analyze` run produces `CODEBASE.md`, `onboarding_brief.md`, `cartography_trace.jsonl`.

---

## Phase 3: Day-One Answers & Semanticist Completion

**Goal:** Semanticist actually answers the Five FDE questions; purpose statements feed Archivist.

| # | Task | Effort | Deps | Output |
|---|------|--------|------|--------|
| 3.1 | `ContextWindowBudget` – track token estimates + cumulative spend per run | 0.5d | — | Budget enforces caps; tiered model routing (bulk vs synthesis) |
| 3.2 | `answer_day_one_questions(kg, modules, datasets)` – synthesis LLM prompt | 1.5d | 3.1 | Structured JSON: 5 answers + evidence list |
| 3.3 | Feed Day-One answers into `onboarding_brief.md` | 0.5d | 2.2, 3.2 | Brief populated from real analysis |
| 3.4 | Doc drift detection – compare docstring vs implementation, flag contradictions | 1d | LLM | `doc_drift` flag on ModuleNode; surfaced in CODEBASE.md |
| 3.5 | Domain clustering – embed purpose statements, k-means (k=5–8), infer labels | 1d | embeddings | `domain_cluster` on ModuleNode |

**Deliverable:** Day-One Brief with correct, cited answers; domain architecture map; doc drift flags.

---

## Phase 4: Vector Index & Semantic Search

**Goal:** Navigator can find implementations by meaning, not just string match.

| # | Task | Effort | Deps | Output |
|---|------|--------|------|--------|
| 4.1 | Add embedding provider (OpenAI embeddings or local sentence-transformers) | 0.5d | .env | Configurable via `CARTOGRAPHER_EMBEDDING_MODEL` |
| 4.2 | Build `semantic_index/` – embed purpose statements + optional code chunks | 1d | 4.1 | FAISS or Chroma index on disk |
| 4.3 | `find_implementation(concept)` – query index, return ranked modules + evidence | 1d | 4.2 | Navigator tool #1 |
| 4.4 | Update index incrementally when modules change | 0.5d | 1.4, 4.2 | Index stays in sync with graph |

**Deliverable:** Semantic search over the codebase; `query find_implementation "revenue calculation"`.

---

## Phase 5: Navigator Agent (Full Tool Suite)

**Goal:** LangGraph agent with all four tools, evidence-backed answers.

| # | Task | Effort | Deps | Output |
|---|------|--------|------|--------|
| 5.1 | `trace_lineage(dataset, direction)` – return subgraph + narrative with file:line citations | 0.5d | existing blast_radius | Tool #2 |
| 5.2 | `blast_radius(module_path)` – structural + data lineage impact; risk summary | 0.5d | existing | Tool #3 |
| 5.3 | `explain_module(path)` – generative summary from Surveyor + Semanticist data | 0.5d | LLM | Tool #4 |
| 5.4 | LangGraph agent – route user question to appropriate tool(s) | 1.5d | 5.1–5.3 | Single `query "<question>"` entrypoint |
| 5.5 | Enforce evidence format: `{file, line_range, analysis_method}` on every response | 0.5d | — | Trust + traceability |

**Deliverable:** `cartographer query "What produces daily_active_users?"` → structured answer with citations.

---

## Phase 6: Operational Hardening

**Goal:** Reliable, observable, safe in production.

| # | Task | Effort | Deps | Output |
|---|------|--------|------|--------|
| 6.1 | LLM retries – exponential backoff, max 3 retries | 0.5d | — | Resilient to transient failures |
| 6.2 | Rate limiting – configurable req/min for LLM calls | 0.5d | — | Avoid provider throttling |
| 6.3 | Structured logging – JSON logs with `run_id`, `agent`, `duration`, `error` | 0.5d | — | Easier debugging + SIEM ingestion |
| 6.4 | Optional metrics – Prometheus-compatible counters (files_parsed, llm_calls, cache_hits) | 0.5d | — | Observability |
| 6.5 | Timeout safeguards – max runtime per agent; abort gracefully | 0.5d | — | No runaway runs |
| 6.6 | `.env` validation at startup – fail fast if required keys missing when LLM needed | 0.25d | — | Clear error messages |

**Deliverable:** Production-grade reliability and observability.

---

## Phase 7: Enterprise Features

**Goal:** Security, compliance, integration.

| # | Task | Effort | Deps | Output |
|---|------|--------|------|--------|
| 7.1 | GitHub URL support – `cartographer analyze https://github.com/org/repo` | 1d | — | Clone, analyze, optional cleanup |
| 7.2 | Local-only mode – no external LLM; static + heuristic outputs only | 0.5d | — | Air-gapped / sensitive environments |
| 7.3 | Secrets manager integration – read API key from env injection / Vault | 0.5d | — | No keys in .env in prod |
| 7.4 | Auth (if exposing as service) – API keys or OAuth for query endpoint | 1d | — | Multi-tenant safe |
| 7.5 | CI/CD plugin – run on PR; report lineage changes, debt deltas | 1.5d | — | Shift-left adoption |

**Deliverable:** Enterprise deployment options; CI integration.

---

## Phase 8: UX & Adoption

**Goal:** Easier to use and demonstrate.

| # | Task | Effort | Deps | Output |
|---|------|--------|------|--------|
| 8.1 | Interactive REPL – `cartographer shell /path/to/repo` | 1d | Phase 5 | Exploratory query session |
| 8.2 | Graph export – Mermaid or JSON for visualization tools | 0.5d | — | Lineage diagrams |
| 8.3 | README + docs – install, .env setup, analyze/query examples, architecture diagram | 0.5d | — | Onboarding |
| 8.4 | Docker image – `docker run cartographer analyze /repo` | 0.5d | — | Portable runs |

**Deliverable:** Clear docs; Docker; optional REPL.

---

## Summary: Suggested Execution Order

```
Phase 1 (Persistence + Incremental)     → 3–4 days
Phase 2 (Archivist + Living Context)    → 3 days
Phase 3 (Day-One + Semanticist)         → 4–5 days
Phase 4 (Vector Index)                  → 3 days
Phase 5 (Navigator Agent)               → 3–4 days
Phase 6 (Operational Hardening)         → 2–3 days
Phase 7 (Enterprise)                    → 4–5 days  [optional]
Phase 8 (UX & Adoption)                 → 2–3 days  [optional]
```

**Minimum viable production:** Phases 1–6 (~18–22 days).  
**Full enterprise:** Phases 1–8 (~25–30 days).

---

## Dependency Graph (High-Level)

```
Phase 1 ──────────────────────────────────────────────────────────► Phase 2, 4, 5
   │                                                                     │
   └── Incremental + Cache ──────────────────────────────────────────────┘

Phase 2 (Archivist) ◄── Phase 3 (Day-One answers)
   │
   └── CODEBASE.md, onboarding_brief, trace

Phase 4 (Vector Index) ──► Phase 5 (Navigator find_implementation)
Phase 5 (Navigator) ──────► Phase 8 (REPL)

Phase 6 (Hardening) ──────► All phases (cross-cutting)
```
