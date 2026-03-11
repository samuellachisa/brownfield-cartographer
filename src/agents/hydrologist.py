from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set

from ..analyzers.sql_lineage import SQLLineageAnalyzer
from ..analyzers.python_lineage import PythonLineageAnalyzer
from ..analyzers.dag_config_parser import DAGConfigParser
from ..graph.knowledge_graph import KnowledgeGraph
from ..models import DatasetNode, TransformationNode


@dataclass
class HydrologistConfig:
    dialect: str | None = None


class HydrologistAgent:
    """
    DataLineageGraph construction: SQL lineage + hooks for Python/YAML analyzers.
    """

    def __init__(self, config: HydrologistConfig | None = None) -> None:
        self.config = config or HydrologistConfig()
        self.sql = SQLLineageAnalyzer(dialect=self.config.dialect)
        self.python = PythonLineageAnalyzer()
        self.dag_parser = DAGConfigParser()

    def run(
        self,
        repo_root: Path,
        graph: KnowledgeGraph,
        changed_files: Optional[Set[str]] = None,
    ) -> Dict[str, DatasetNode]:
        datasets: Dict[str, DatasetNode] = {}

        # SQL lineage
        for path in repo_root.rglob("*.sql"):
            rel = str(path.relative_to(repo_root))
            if changed_files is not None and rel not in changed_files:
                continue
            for dep in self.sql.analyze_file(path):
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
                    transformation_type="sql",
                    source_file=str(path),
                    line_range=(1, 1),
                    sql_query_if_applicable=dep.sql,
                )
                graph.add_transformation(transform)

        # Python data operations lineage
        for path in repo_root.rglob("*.py"):
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

        # YAML / config lineage (dbt / Airflow-style)
        for path in repo_root.rglob("*.yml"):
            rel = str(path.relative_to(repo_root))
            if changed_files is not None and rel not in changed_files:
                continue
            cfg = self.dag_parser.parse(path)
            if not cfg or not cfg.tasks:
                continue
            src_nodes: List[str] = []
            tgt_nodes: List[str] = []
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
                tgt_nodes.append(task)

            transform = TransformationNode(
                source_datasets=src_nodes,
                target_datasets=tgt_nodes,
                transformation_type=cfg.config_type or "yaml",
                source_file=str(path),
                line_range=(1, 1),
                sql_query_if_applicable=None,
            )
            graph.add_transformation(transform)

        return datasets

