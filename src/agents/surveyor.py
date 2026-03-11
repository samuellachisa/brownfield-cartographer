from __future__ import annotations

import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Optional, Set

from ..analyzers.tree_sitter_analyzer import TreeSitterAnalyzer
from ..analyzers.dag_config_parser import DAGConfigParser
from ..config import CartographerConfig, load_config
from ..graph.knowledge_graph import KnowledgeGraph
from ..models import ModuleNode, ModuleEdgeType


@dataclass
class SurveyorConfig:
    days_for_velocity: int = 30


class SurveyorAgent:
    """
    Static structure analysis: module graph, PageRank inputs, git velocity, dead code candidates.
    """

    def __init__(self, config: SurveyorConfig | None = None, global_config: CartographerConfig | None = None) -> None:
        self.config = config or SurveyorConfig()
        self.analyzer = TreeSitterAnalyzer()
        self.dag_parser = DAGConfigParser()
        self._global_config = global_config

    def run(
        self,
        repo_root: Path,
        graph: KnowledgeGraph,
        changed_files: Optional[Set[str]] = None,
    ) -> Dict[str, ModuleNode]:
        cfg = self._global_config or load_config(repo_root)
        modules: Dict[str, ModuleNode] = {}

        for path in self._iter_files(repo_root, changed_files, cfg):
            try:
                language = self._language_for_path(path)
                last_modified = self._last_modified(repo_root, path)
                velocity = self._git_velocity(repo_root, path, self.config.days_for_velocity)

                if language == "python":
                    analysis = self.analyzer.analyze_module(path)
                    complexity = float(analysis.loc)
                    module = ModuleNode(
                        path=str(path.relative_to(repo_root)),
                        language="python",
                        purpose_statement=None,
                        domain_cluster=None,
                        complexity_score=complexity,
                        change_velocity_30d=velocity,
                        is_dead_code_candidate=False,
                        last_modified=last_modified,
                    )
                    modules[module.path] = module
                    graph.add_module(module)

                    # Imports -> IMPORTS edges (string-level; resolution can be added later)
                    for imp in analysis.imports:
                        graph.add_import_edge(module.path, imp, weight=1)
                    # Public classes could be surfaced later; for now they enrich
                    # the semantic index via ModuleAnalysisResult.

                elif language == "yaml":
                    # YAML configs -> CONFIGURES edges
                    cfg = self.dag_parser.parse(path)
                    module = ModuleNode(
                        path=str(path.relative_to(repo_root)),
                        language="yaml",
                        purpose_statement=None,
                        domain_cluster=None,
                        complexity_score=0.0,
                        change_velocity_30d=velocity,
                        is_dead_code_candidate=False,
                        last_modified=last_modified,
                    )
                    modules[module.path] = module
                    graph.add_module(module)
                    if cfg:
                        for task in cfg.tasks:
                            graph.add_configures_edge(module.path, task, cfg.config_type)

                else:
                    module = ModuleNode(
                        path=str(path.relative_to(repo_root)),
                        language=language,
                        purpose_statement=None,
                        domain_cluster=None,
                        complexity_score=0.0,
                        change_velocity_30d=velocity,
                        is_dead_code_candidate=False,
                        last_modified=last_modified,
                    )
                    modules[module.path] = module
                    graph.add_module(module)
            except Exception as e:
                print(f"[surveyor] Skipping file {path} due to error: {e}")

        # Graph-level analyses: PageRank, degrees, cycles, and dead code candidates.
        pr = graph.pagerank()
        scc = list(graph.strongly_connected_components())
        cycle_nodes = {n for comp in scc if len(comp) > 1 for n in comp}

        for path, module in modules.items():
            if path in pr:
                module.pagerank = float(pr[path])
            if path in cycle_nodes:
                module.is_in_cycle = True
            if path in graph.module_graph:
                module.in_degree = int(graph.module_graph.in_degree(path))
                module.out_degree = int(graph.module_graph.out_degree(path))

        # Dead-code candidates: no inbound references, no recent changes, and
        # not in known "entrypoint" locations (e.g. CLI scripts, DAG roots).
        entrypoint_prefixes = ("bin/", "scripts/", "dags/", "dg_deployments/")
        for path, module in modules.items():
            if path not in graph.module_graph:
                continue
            has_incoming = any(
                d.get("type") in {ModuleEdgeType.IMPORTS.value, ModuleEdgeType.CALLS.value}
                for _, _, d in graph.module_graph.in_edges(path, data=True)
            )
            is_entrypoint = path.startswith(entrypoint_prefixes)
            if (not has_incoming) and (module.change_velocity_30d == 0) and (not is_entrypoint):
                module.is_dead_code_candidate = True
                if path in graph.module_graph.nodes:
                    graph.module_graph.nodes[path]["is_dead_code_candidate"] = True

        return modules

    def _iter_files(
        self,
        root: Path,
        changed_files: Optional[Set[str]] = None,
        cfg: CartographerConfig | None = None,
    ) -> Iterable[Path]:
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            ignore_dirs = set((cfg.ignore_dirs if cfg else []))
            if any(part in ignore_dirs for part in path.parts):
                continue
            if path.suffix.lower() in {".py", ".sql", ".yaml", ".yml"}:
                rel = str(path.relative_to(root))
                if changed_files is not None and rel not in changed_files:
                    continue
                yield path

    @staticmethod
    def _language_for_path(path: Path) -> str:
        if path.suffix == ".py":
            return "python"
        if path.suffix == ".sql":
            return "sql"
        if path.suffix in {".yaml", ".yml"}:
            return "yaml"
        return "other"

    @staticmethod
    def _last_modified(repo_root: Path, path: Path) -> datetime:
        try:
            rel = str(path.relative_to(repo_root))
            out = subprocess.check_output(
                ["git", "log", "-1", "--format=%ct", "--", rel],
                cwd=str(repo_root),
                stderr=subprocess.DEVNULL,
            ).decode("utf-8", errors="ignore").strip()
            if out:
                return datetime.fromtimestamp(int(out))
        except Exception:
            pass
        return datetime.fromtimestamp(path.stat().st_mtime)

    @staticmethod
    def _git_velocity(repo_root: Path, path: Path, days: int) -> int:
        try:
            rel = str(path.relative_to(repo_root))
            out = subprocess.check_output(
                ["git", "log", f"--since={days}.days", "--oneline", "--", rel],
                cwd=str(repo_root),
                stderr=subprocess.DEVNULL,
            ).decode("utf-8", errors="ignore")
            return len([ln for ln in out.splitlines() if ln.strip()])
        except Exception:
            return 0

