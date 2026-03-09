# Brownfield Cartographer

Multi-agent codebase intelligence for data engineering repos. Maps unfamiliar codebases into a queryable knowledge graph of architecture, data lineage, and semantic structure.

Answers the Five FDE Day-One Questions:

1. What is the primary data ingestion path?
2. What are the 3–5 most critical output datasets/endpoints?
3. What is the blast radius if the most critical module fails?
4. Where is business logic concentrated vs. distributed?
5. What has changed most frequently in the last 90 days?

## Install

```bash
pip install -e .
# or: pip install -r requirements.txt
```

## Configure (.env)

Copy `.env.example` to `.env` and set:

```bash
OPENAI_API_KEY=sk-...
CARTOGRAPHER_LLM_MODEL=gpt-4o-mini
# CARTOGRAPHER_LLM_BASE_URL=https://openrouter.ai/api/v1
# CARTOGRAPHER_LLM_RPM=60
```

If `OPENAI_API_KEY` is unset, the pipeline runs in `--local-only` mode (static analysis only).

## Usage

**Primary interface: interactive shell.** One session to analyze and ask.

```bash
# Start shell (default repo: current dir)
cartographer
cartographer /path/to/repo
cartographer shell /path/to/repo

# One-off analysis for scripts
cartographer analyze /path/to/repo [--incremental] [--local-only]
```

### Shell commands

| Command | Example |
|---------|---------|
| `analyze` | `analyze`, `analyze --incremental`, `analyze --local-only` |
| `ask` | `ask "what produces the customers table?"` |
| `find` | `find revenue calculation` |
| `lineage` | `lineage customers`, `lineage orders up` |
| `blast` | `blast models/orders.sql` |
| `explain` | `explain dags/ingest.py` |
| `sources` | List data sources |
| `sinks` | List data sinks |
| `quit` | Exit |

Artifacts are written to `.cartography/`: `CODEBASE.md`, `onboarding_brief.md`, `module_graph.json`, `lineage_graph.json`, `cartography_trace.jsonl`, `semantic_index/`.

## Docker

```bash
docker build -t cartographer .
docker run -v /path/to/repo:/repo -e OPENAI_API_KEY=sk-... cartographer analyze /repo
```

## Project layout

- `src/cli.py` – Entry point
- `src/orchestrator.py` – Pipeline wiring
- `src/models/` – Pydantic schemas
- `src/agents/` – Surveyor, Hydrologist, Semanticist, Archivist, Navigator
- `src/analyzers/` – tree-sitter, sqlglot, YAML
- `src/graph/` – KnowledgeGraph (NetworkX)
- `src/storage.py` – Persistence, incremental
- `src/semantic_index.py` – Vector search
