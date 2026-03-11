# Brownfield Cartographer – RECONNAISSANCE (mitodl/ol-data-platform)

## Target Codebase

- **Repo**: `mitodl/ol-data-platform`
- **Stack (observed)**: Python (Dagster assets, utilities), dbt/SQL models, YAML config (Dagster, dbt, CI), shell/CLI tools.
- **Primary domains (inferred)**: edX/Canvas data ingestion, warehouse modeling in `ol_warehouse_production_dimensional`, downstream analytics/exports for course, user, and organization data.

---

## Manual Day-One Answers

### 1. What is the primary data ingestion path?

**Answer (manual)**  
The primary ingestion path appears to be **batch ingestion of learning platform data (edX.org and Canvas)** from S3 and external APIs into the warehouse. The **Dagster project `dg_projects/data_loading`** owns much of this path:

- `dg_projects/data_loading/data_loading/defs/edxorg_s3_ingest/dagster_assets.py` and `.../loads.py` define Dagster assets that ingest TSV files and other exports from an edX.org S3 bucket into staging tables.
- `dg_projects/canvas/canvas/lib/canvas.py` and `dg_projects/canvas/canvas/assets/canvas.py` pull course content and metadata from Canvas via APIs/exports.

These ingestion assets write into warehouse schemas (e.g. `ol_warehouse_production_dimensional`) which then feed dbt models and further Dagster transformations.

**Where I looked**

- Dagster project layout under `dg_projects/data_loading` and `dg_projects/canvas`.
- Purpose statements in `CODEBASE.md` for ingestion-related modules (e.g. `data_loading/defs/edxorg_s3_ingest/loads.py`).

---

### 2. What are the 3–5 most critical output datasets/endpoints?

**Answer (manual)**  
The most critical outputs appear to be **warehouse dimension/fact tables that power course and engagement analytics**:

- Dimension and fact tables in the `ol_warehouse_production_dimensional` schema, especially:
  - `afact_course_page_engagement`
  - `afact_discussion_engagement`
  - `afact_problem_engagement`
  - `dim_course_content`
  - `dim_problem`
  - `dim_discussion_topic`
- These tables are heavily referenced as **sources** or intermediate nodes in the lineage graph and likely back core dashboards/analytics around learner activity and course content.

There are also downstream exports in project-specific assets (e.g. `dg_projects/b2b_organization`, `dg_projects/canvas`) that materialize curated datasets for B2B organizations and Canvas reporting; these are important for particular business workflows but are downstream of the warehouse core.

**Where I looked**

- `.cartography/repos/mitodl/ol-data-platform/lineage_graph.json` – high‑fanout datasets and listed sources.
- `CODEBASE.md` module purpose index for project-specific assets that appear to export or expose data.

---

### 3. What is the blast radius if the most critical module fails?

**Answer (manual)**  
If the **core ingestion and reconciliation assets for edX.org and Canvas data** fail, the blast radius is broad:

- Failures in `dg_projects/data_loading/data_loading/defs/edxorg_s3_ingest/loads.py` or related Dagster assets would block fresh data from arriving in the warehouse, causing **stale or missing records** in all downstream `afact_*` and `dim_*` tables.
- `dg_deployments/reconcile_edxorg_partitions.py` appears to reconcile partitioned data in the warehouse; failure here could leave inconsistent partitions that break incremental loads and queries depending on partition completeness.
- Dagster definitions files such as `dg_projects/data_loading/data_loading/definitions.py` and `dg_projects/data_platform/data_platform/definitions.py` centralize asset graphs; misconfiguration or failure could prevent runs across multiple projects.

The blast radius therefore includes:

- **All warehouse fact/dim tables** that depend on edX/Canvas ingest.
- Any **dbt models** built on top of these tables.
- **Reports and external exports** in `dg_projects/*` that rely on up‑to‑date engagement and course content data.

**Where I looked**

- High‑velocity and complex modules in `CODEBASE.md` (e.g. `reconcile_edxorg_partitions.py`, ingestion defs).
- Lineage graph around `ol_warehouse_production_dimensional.*` sources and their downstream paths.

---

### 4. Where is business logic concentrated vs. distributed?

**Answer (manual)**  
Business logic is split between:

- **Dagster project packages under `dg_projects/`**:
  - `data_loading` – ingestion and consolidation of raw platform data into warehouse‑ready structures.
  - `data_platform` – shared platform‑level assets and orchestrations.
  - `b2b_organization`, `canvas`, and other projects – domain‑specific assets, sensors, and exports.
- **Warehouse modeling logic** in dbt/SQL:
  - SQL models that build the `ol_warehouse_production_dimensional` schema encode metric definitions and content relationships.
- **Operational/CLI tooling** in `bin/*.py` (e.g. `dbt-local-dev`, `uv-operations`) which encode environment and deployment behavior rather than pure business rules.

Concentration is highest in:

- Dagster `definitions.py` and `dagster_assets.py` files (they wire together sources, transformations, and schedules).
- dbt models that define core metrics and dimensional joins.

Logic is **distributed** across multiple projects (`dg_projects/*`) but follows a consistent Dagster/dbt pattern.

**Where I looked**

- Module purpose index in `CODEBASE.md` under `dg_projects/*` and `bin/*`.
- Directory structure and filenames indicating Dagster/DBT entrypoints.

---

### 5. What has changed most frequently in the last 90 days (git velocity map)?

**Answer (manual)**  
Recent changes (last 30–90 days) seem to focus on:

- **Configuration and orchestration**:
  - `docker-compose.yaml`, `build.yaml`, `.pre-commit-config.yaml`.
  - GitHub Actions workflows under `.github/workflows/` (e.g. `publish_dbt_docs.yaml`, `project_automation.yaml`).
- **Deployment and local‑dev tooling**:
  - `bin/dbt-local-dev.py`, `bin/uv-operations.py`, and utility scripts under `bin/utils/`.
- **Dagster local deployment config**:
  - `dg_deployments/local/dagster.yaml`, `dg_deployments/local/workspace.yaml`.
- **Selected Dagster project configs**:
  - `dg_projects/student_risk_probability/build.yaml` and other `dg_projects/*/build.yaml` files.

This suggests active work on **operationalizing and refining** the platform (CI/CD, local dev, deployments) rather than wholesale redesign of core business logic.

**Where I looked**

- High‑velocity files section in `CODEBASE.md`.
- Git velocity annotations on ModuleNodes (via `.cartography` state).

---

## Difficulty & Where I Got Lost

- **Sheer surface area**: The repo mixes Dagster projects, dbt models, YAML configs, and operational scripts. It’s not immediately obvious which Dagster project is the primary “entry point” without following multiple `definitions.py` files.
- **Warehouse schema understanding**: The lineage graph exposes many `ol_warehouse_production_dimensional.*` tables, but understanding the **semantics of each fact/dim table** (what exactly is an “engagement” row, how course content is structured) still requires reading dbt models and upstream Dagster assets in detail.
- **Cross‑project relationships**: `dg_projects/*` contains several domain‑specific projects (B2B, Canvas, risk scoring). Mapping which ones are core vs. peripheral takes time because they share patterns and rely on the same warehouse base.
- **Operational vs. business changes**: High‑velocity files skew toward deployment and tooling; separating “infra churn” from “business logic churn” required correlating velocity with purpose statements.

These pain points strongly justify the Cartographer’s focus on **lineage graphs**, **purpose statements**, and **change‑velocity overlays** to quickly highlight true critical paths and hotspots.

