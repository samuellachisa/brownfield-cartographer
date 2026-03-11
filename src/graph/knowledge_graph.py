from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Any, Iterable, Literal, Optional, List

import networkx as nx

from ..models import (
    ModuleNode,
    DatasetNode,
    FunctionNode,
    TransformationNode,
    ModuleEdgeType,
    LineageEdgeType,
)


@dataclass
class KnowledgeGraph:
    """
    Wrapper around NetworkX for module + lineage graphs with JSON serialization.
    """

    module_graph: nx.DiGraph = field(default_factory=nx.DiGraph)
    lineage_graph: nx.DiGraph = field(default_factory=nx.DiGraph)

    # ---- Module graph operations -------------------------------------------------

    def add_module(self, module: ModuleNode) -> None:
        self.module_graph.add_node(
            module.path,
            type="module",
            language=module.language,
            complexity_score=module.complexity_score,
            change_velocity_30d=module.change_velocity_30d,
            is_dead_code_candidate=module.is_dead_code_candidate,
            last_modified=module.last_modified.isoformat(),
        )

    def add_import_edge(self, src: str, dst: str, weight: int = 1) -> None:
        data = self.module_graph.get_edge_data(src, dst, default={})
        new_weight = data.get("weight", 0) + weight
        self.module_graph.add_edge(src, dst, type=ModuleEdgeType.IMPORTS.value, weight=new_weight)

    def add_configures_edge(self, config_path: str, target: str, config_type: str) -> None:
        self.module_graph.add_edge(
            config_path,
            target,
            type=ModuleEdgeType.CONFIGURES.value,
            config_type=config_type,
        )

    def add_calls_edge(self, caller: str, callee: str, count: int = 1) -> None:
        data = self.module_graph.get_edge_data(caller, callee, default={})
        new_count = data.get("call_count", 0) + count
        self.module_graph.add_edge(caller, callee, type=ModuleEdgeType.CALLS.value, call_count=new_count)

    def pagerank(self) -> Dict[str, float]:
        """
        Compute PageRank over the module graph to identify structurally critical modules.

        The result is also written back onto node attributes as ``pagerank`` for
        downstream consumers (e.g. UI, NavigatorAgent).
        """
        if not self.module_graph.nodes:
            return {}
        scores = nx.pagerank(self.module_graph)
        for node, score in scores.items():
            if node in self.module_graph.nodes:
                self.module_graph.nodes[node]["pagerank"] = float(score)
        return scores

    def strongly_connected_components(self) -> Iterable[set[str]]:
        """
        Return strongly connected components to detect circular dependency clusters.
        """
        return nx.strongly_connected_components(self.module_graph)

    # ---- Lineage operations ------------------------------------------------------

    def add_dataset(self, dataset: DatasetNode) -> None:
        self.lineage_graph.add_node(
            dataset.name,
            type="dataset",
            storage_type=dataset.storage_type,
            sensitivity=dataset.sensitivity,
        )

    def add_transformation(self, transform: TransformationNode) -> None:
        tid = f"{transform.source_file}:{transform.line_range[0]}-{transform.line_range[1]}"
        self.lineage_graph.add_node(
            tid,
            type="transformation",
            transformation_type=transform.transformation_type,
            source_file=transform.source_file,
            line_range=transform.line_range,
            sql_query_if_applicable=transform.sql_query_if_applicable,
        )
        for src in transform.source_datasets:
            src_sensitivity = self.lineage_graph.nodes.get(src, {}).get("sensitivity")
            self.lineage_graph.add_edge(
                src,
                tid,
                type=LineageEdgeType.CONSUMES.value,
                direction="read",
                sensitivity=src_sensitivity,
                source_file=transform.source_file,
                line_range=transform.line_range,
            )
        for tgt in transform.target_datasets:
            tgt_sensitivity = self.lineage_graph.nodes.get(tgt, {}).get("sensitivity")
            self.lineage_graph.add_edge(
                tid,
                tgt,
                type=LineageEdgeType.PRODUCES.value,
                direction="write",
                sensitivity=tgt_sensitivity,
                source_file=transform.source_file,
                line_range=transform.line_range,
            )

    # ---- Blast radius / sources / sinks -----------------------------------------

    def blast_radius(
        self,
        node: str,
        direction: Literal["upstream", "downstream"] = "downstream",
        max_depth: Optional[int] = None,
        max_nodes: Optional[int] = None,
    ) -> nx.DiGraph:
        g = self.lineage_graph
        if node not in g:
            return nx.DiGraph()
        from collections import deque

        visited = set([node])
        queue = deque([(node, 0)])

        while queue:
            current, depth = queue.popleft()
            if max_depth is not None and depth >= max_depth:
                continue
            neighbors = g.predecessors(current) if direction == "upstream" else g.successors(current)
            for nb in neighbors:
                if nb in visited:
                    continue
                visited.add(nb)
                if max_nodes is not None and len(visited) >= max_nodes:
                    # Early stop once we reach the node budget; callers still
                    # get a connected subgraph rooted at ``node``.
                    return g.subgraph(visited).copy()
                queue.append((nb, depth + 1))

        return g.subgraph(visited).copy()

    def top_fanout_datasets(
        self,
        k: int = 10,
        max_depth: Optional[int] = None,
        max_nodes: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Return the top-k datasets with the largest downstream fan-out.

        This is useful for identifying high-blast-radius sinks or hubs in the
        lineage graph.
        """
        g = self.lineage_graph
        scores: List[Dict[str, Any]] = []
        for n, data in g.nodes(data=True):
            if data.get("type") != "dataset":
                continue
            # Count distinct downstream datasets reachable from this node.
            sub = self.blast_radius(
                n,
                direction="downstream",
                max_depth=max_depth,
                max_nodes=max_nodes,
            )
            fanout = sum(1 for nn, dd in sub.nodes(data=True) if dd.get("type") == "dataset" and nn != n)
            scores.append({"name": n, "fanout": fanout})
        scores.sort(key=lambda x: x["fanout"], reverse=True)
        return scores[:k]

    def critical_paths_from(
        self,
        node: str,
        direction: Literal["upstream", "downstream"] = "downstream",
        max_depth: int = 10,
        top_k: int = 5,
        max_paths: int = 5000,
    ) -> List[List[str]]:
        """
        Compute a few of the longest simple paths starting from ``node`` in the
        requested direction, up to ``max_depth``.

        This provides a concise view of critical pipelines for downstream
        consumers without requiring them to run arbitrary graph algorithms.
        """
        g = self.lineage_graph
        if node not in g:
            return []

        paths: List[List[str]] = []
        paths_seen = 0

        def dfs(current: str, path: List[str], depth: int) -> None:
            nonlocal paths_seen
            if paths_seen >= max_paths:
                return
            if depth >= max_depth:
                paths.append(path[:])
                paths_seen += 1
                return
            neighbors = list(g.predecessors(current) if direction == "upstream" else g.successors(current))
            if not neighbors:
                paths.append(path[:])
                paths_seen += 1
                return
            for nb in neighbors:
                if nb in path:
                    continue
                dfs(nb, path + [nb], depth + 1)

        dfs(node, [node], 0)
        paths.sort(key=len, reverse=True)
        return paths[:top_k]

    def find_sources(self) -> Iterable[str]:
        g = self.lineage_graph
        for n, data in g.nodes(data=True):
            if data.get("type") != "dataset":
                continue
            # no incoming PRODUCES edges
            incoming_produces = any(
                d.get("type") == "PRODUCES"
                for _, _, d in g.in_edges(n, data=True)
            )
            if not incoming_produces:
                yield n

    def find_sinks(self) -> Iterable[str]:
        g = self.lineage_graph
        for n, data in g.nodes(data=True):
            if data.get("type") != "dataset":
                continue
            outgoing_consumes = any(
                d.get("type") == "CONSUMES"
                for _, _, d in g.out_edges(n, data=True)
            )
            if not outgoing_consumes:
                yield n

    # ---- Serialization -----------------------------------------------------------

    def to_json(self) -> Dict[str, Any]:
        # Compute a layout for the lineage graph so UIs can render a stable
        # visualization without re-running a layout algorithm client-side.
        lineage_nodes = list(self.lineage_graph.nodes(data=True))
        lineage_edges = list(self.lineage_graph.edges(data=True))
        lineage_positions: Dict[str, Any] = {}
        if lineage_nodes and len(lineage_nodes) <= 2000:
            # spring_layout returns positions in an arbitrary coordinate system;
            # callers are expected to normalize/scale for their viewport.
            lineage_positions = nx.spring_layout(self.lineage_graph, seed=42)

        return {
            "module_graph": {
                "nodes": [{"id": n, **data} for n, data in self.module_graph.nodes(data=True)],
                "edges": [{"source": u, "target": v, **data} for u, v, data in self.module_graph.edges(data=True)],
            },
            "lineage_graph": {
                "nodes": [
                    {
                        "id": n,
                        **data,
                        **(
                            {
                                "x": float(lineage_positions[n][0]),
                                "y": float(lineage_positions[n][1]),
                            }
                            if n in lineage_positions
                            else {}
                        ),
                    }
                    for n, data in lineage_nodes
                ],
                "edges": [{"source": u, "target": v, **data} for u, v, data in lineage_edges],
            },
        }

    def write_module_graph(self, path: Path) -> None:
        import json

        payload = {
            "nodes": [{"id": n, **data} for n, data in self.module_graph.nodes(data=True)],
            "edges": [{"source": u, "target": v, **data} for u, v, data in self.module_graph.edges(data=True)],
        }
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def write_lineage_graph(self, path: Path) -> None:
        import json

        nodes = list(self.lineage_graph.nodes(data=True))
        edges = list(self.lineage_graph.edges(data=True))
        positions: Dict[str, Any] = {}
        if nodes and len(nodes) <= 2000:
            positions = nx.spring_layout(self.lineage_graph, seed=42)

        payload = {
            "nodes": [
                {
                    "id": n,
                    **data,
                    **(
                        {
                            "x": float(positions[n][0]),
                            "y": float(positions[n][1]),
                        }
                        if n in positions
                        else {}
                    ),
                }
                for n, data in nodes
            ],
            "edges": [{"source": u, "target": v, **data} for u, v, data in edges],
        }
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

