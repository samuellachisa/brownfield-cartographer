from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from ..analyzers.sql_lineage import SQLLineageAnalyzer
from ..utils.logging import get_logger, log_file_skip
from ..analyzers.python_lineage import PythonLineageAnalyzer
from ..analyzers.dag_config_parser import DAGConfigParser
from ..analyzers.notebook_lineage import NotebookLineageAnalyzer
from ..config import CartographerConfig, load_config
from ..graph.knowledge_graph import KnowledgeGraph
from ..models import DatasetNode, TransformationNode


@dataclass
class HydrologistConfig:
    dialect: str | None = None


class HydrologistAgent:
    """
    DataLineageGraph construction: SQL lineage + hooks for Python/YAML analyzers.
    """

    def __init__(self, config: HydrologistConfig | None = None, global_config: CartographerConfig | None = None) -> None:
        self.config = config or HydrologistConfig()
        self._global_config = global_config
        self.sql = SQLLineageAnalyzer(dialect=self.config.dialect)
        self.python = PythonLineageAnalyzer()
        self.notebooks = NotebookLineageAnalyzer()
        self.dag_parser = DAGConfigParser()

    def run(
        self,
        repo_root: Path,
        graph: KnowledgeGraph,
        changed_files: Optional[Set[str]] = None,
        error_collector: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, DatasetNode]:
        cfg = self._global_config or load_config(repo_root)
        datasets: Dict[str, DatasetNode] = {}

        # SQL lineage
        for path in repo_root.rglob("*.sql"):
            try:
                rel = str(path.relative_to(repo_root))
                if changed_files is not None and rel not in changed_files:
                    continue
                dialect = cfg.sql.for_file(path)
                self.sql.dialect = dialect
                _DBT_PLACEHOLDERS = frozenset({"__dbt_this__", "__dbt_model__", "__dbt_var__"})
                for dep in self.sql.analyze_file(path):
                    if not dep.sources and not dep.targets:
                        continue
                    src_nodes: List[str] = [
                        s for s in dep.sources if s not in _DBT_PLACEHOLDERS
                    ]
                    tgt_nodes: List[str] = list(dep.targets)
                    # dbt model files are SELECT-only; SQL analyzer finds no targets.
                    # Infer target from file path (e.g. models/dim/dim_listings_cleansed.sql -> dim_listings_cleansed).
                    if not tgt_nodes and dep.sources and "models" in rel:
                        inferred = path.stem
                        if inferred and inferred not in (".", ".."):
                            tgt_nodes = [inferred]
                    for t in tgt_nodes:
                        if t not in datasets:
                            datasets[t] = DatasetNode(
                                name=t,
                                storage_type="table",
                                schema_snapshot=None,
                                freshness_sla=None,
                                owner=None,
                                is_source_of_truth=False,
                            )
                            graph.add_dataset(datasets[t])
                    for s in src_nodes:
                        if s not in datasets:
                            datasets[s] = DatasetNode(
                                name=s,
                                storage_type="table",
                                schema_snapshot=None,
                                freshness_sla=None,
                                owner=None,
                                is_source_of_truth=False,
                            )
                            graph.add_dataset(datasets[s])

                    transform = TransformationNode(
                        source_datasets=src_nodes,
                        target_datasets=tgt_nodes,
                        transformation_type="sql",
                        source_file=str(path),
                        line_range=dep.line_range or (1, 1),
                        sql_query_if_applicable=dep.sql,
                    )
                    graph.add_transformation(transform)
            except Exception as e:
                log_file_skip(
                    get_logger("hydrologist"),
                    "hydrologist",
                    str(path.relative_to(repo_root)),
                    e,
                    error_collector=error_collector,
                )

        # Python data operations lineage
        for path in repo_root.rglob("*.py"):
            try:
                rel = str(path.relative_to(repo_root))
                if changed_files is not None and rel not in changed_files:
                    continue
                for dep in self.python.analyze_file(path):
                    if not dep.sources and not dep.targets:
                        continue
                    src_nodes: List[str] = []
                    tgt_nodes: List[str] = []
                    for s in dep.sources:
                        if s not in datasets:
                            datasets[s] = DatasetNode(
                                name=s,
                                storage_type="table",
                                schema_snapshot=None,
                                freshness_sla=None,
                                owner=None,
                                is_source_of_truth=False,
                            )
                            graph.add_dataset(datasets[s])
                        src_nodes.append(s)
                    for t in dep.targets:
                        if t not in datasets:
                            datasets[t] = DatasetNode(
                                name=t,
                                storage_type="table",
                                schema_snapshot=None,
                                freshness_sla=None,
                                owner=None,
                                is_source_of_truth=False,
                            )
                            graph.add_dataset(datasets[t])
                        tgt_nodes.append(t)

                    transform = TransformationNode(
                        source_datasets=src_nodes,
                        target_datasets=tgt_nodes,
                        transformation_type="python",
                        source_file=str(path),
                        line_range=dep.location,
                        sql_query_if_applicable=None,
                    )
                    graph.add_transformation(transform)
            except Exception as e:
                log_file_skip(
                    get_logger("hydrologist"),
                    "hydrologist",
                    str(path.relative_to(repo_root)),
                    e,
                    error_collector=error_collector,
                )

        # YAML / config lineage (dbt / Airflow-style)
        for path in repo_root.rglob("*.yml"):
            try:
                rel = str(path.relative_to(repo_root))
                if changed_files is not None and rel not in changed_files:
                    continue
                cfg = self.dag_parser.parse(path)
                if not cfg:
                    continue

                # Add dbt source tables as entry-point datasets (no producing transformation).
                for tbl in getattr(cfg, "source_tables", []) or []:
                    if tbl not in datasets:
                        datasets[tbl] = DatasetNode(
                            name=tbl,
                            storage_type="table",
                            schema_snapshot=None,
                            freshness_sla=None,
                            owner=None,
                            is_source_of_truth=False,
                        )
                        graph.add_dataset(datasets[tbl])

                if not cfg.tasks:
                    continue

                # Ensure DatasetNode instances exist for every task/pipeline node.
                for task in cfg.tasks:
                    if task not in datasets:
                        datasets[task] = DatasetNode(
                            name=task,
                            storage_type="table",
                            schema_snapshot=None,
                            freshness_sla=None,
                            owner=None,
                            is_source_of_truth=False,
                        )
                        graph.add_dataset(datasets[task])

                # If we have explicit task dependencies, emit one transformation
                # per edge to reflect the pipeline topology. Otherwise, fall
                # back to a single config-level transformation as before.
                if cfg.dependencies:
                    for task, upstreams in cfg.dependencies.items():
                        src_nodes: List[str] = []
                        tgt_nodes: List[str] = [task]
                        for upstream in upstreams:
                            if upstream not in datasets:
                                datasets[upstream] = DatasetNode(
                                    name=upstream,
                                    storage_type="table",
                                    schema_snapshot=None,
                                    freshness_sla=None,
                                    owner=None,
                                    is_source_of_truth=False,
                                )
                                graph.add_dataset(datasets[upstream])
                            src_nodes.append(upstream)

                        transform = TransformationNode(
                            source_datasets=src_nodes,
                            target_datasets=tgt_nodes,
                            transformation_type=cfg.config_type or "yaml",
                            source_file=str(path),
                            line_range=(1, 1),
                            sql_query_if_applicable=None,
                        )
                        graph.add_transformation(transform)
                else:
                    src_nodes: List[str] = []
                    tgt_nodes: List[str] = list(cfg.tasks)
                    transform = TransformationNode(
                        source_datasets=src_nodes,
                        target_datasets=tgt_nodes,
                        transformation_type=cfg.config_type or "yaml",
                        source_file=str(path),
                        line_range=(1, 1),
                        sql_query_if_applicable=None,
                    )
                    graph.add_transformation(transform)
            except Exception as e:
                log_file_skip(
                    get_logger("hydrologist"),
                    "hydrologist",
                    str(path.relative_to(repo_root)),
                    e,
                    error_collector=error_collector,
                )

        # Notebook lineage (.ipynb)
        for path in repo_root.rglob("*.ipynb"):
            try:
                rel = str(path.relative_to(repo_root))
                if changed_files is not None and rel not in changed_files:
                    continue
                for dep in self.notebooks.analyze_file(path):
                    if not dep.sources and not dep.targets:
                        continue
                    src_nodes: List[str] = []
                    tgt_nodes: List[str] = []
                    for s in dep.sources:
                        if s not in datasets:
                            datasets[s] = DatasetNode(
                                name=s,
                                storage_type="table",
                                schema_snapshot=None,
                                freshness_sla=None,
                                owner=None,
                                is_source_of_truth=False,
                            )
                            graph.add_dataset(datasets[s])
                        src_nodes.append(s)
                    for t in dep.targets:
                        if t not in datasets:
                            datasets[t] = DatasetNode(
                                name=t,
                                storage_type="table",
                                schema_snapshot=None,
                                freshness_sla=None,
                                owner=None,
                                is_source_of_truth=False,
                            )
                            graph.add_dataset(datasets[t])
                        tgt_nodes.append(t)

                    transform = TransformationNode(
                        source_datasets=src_nodes,
                        target_datasets=tgt_nodes,
                        transformation_type="python",
                        source_file=str(path),
                        line_range=dep.location,
                        sql_query_if_applicable=None,
                    )
                    graph.add_transformation(transform)
            except Exception as e:
                log_file_skip(
                    get_logger("hydrologist"),
                    "hydrologist",
                    str(path.relative_to(repo_root)),
                    e,
                    error_collector=error_collector,
                )

        return datasets

