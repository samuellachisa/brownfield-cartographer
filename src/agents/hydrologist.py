from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set

from ..analyzers.sql_lineage import SQLLineageAnalyzer
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

    def run(
        self,
        repo_root: Path,
        graph: KnowledgeGraph,
        changed_files: Optional[Set[str]] = None,
    ) -> Dict[str, DatasetNode]:
        datasets: Dict[str, DatasetNode] = {}

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

        return datasets

