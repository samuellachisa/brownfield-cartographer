from __future__ import annotations

import re
import subprocess
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

from ..analyzers.tree_sitter_analyzer import TreeSitterAnalyzer
from ..utils.logging import get_logger, log_file_skip
from ..analyzers.dag_config_parser import DAGConfigParser
from ..config import CartographerConfig, load_config
from ..graph.knowledge_graph import KnowledgeGraph
from ..models import ModuleNode, ModuleEdgeType


@dataclass
class SurveyorConfig:
    days_for_velocity: int = 30
    dead_code_stale_days: int = 90


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
        error_collector: Optional[List[Dict[str, Any]]] = None,
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
                    rel_path = str(path.relative_to(repo_root))
                    file_age_days = self._file_age_days(last_modified)
                    has_explicit_exports = self._has_explicit_exports(path)
                    has_test_file = self._has_corresponding_test(repo_root, rel_path)
                    module = ModuleNode(
                        path=rel_path,
                        language="python",
                        purpose_statement=None,
                        domain_cluster=None,
                        complexity_score=complexity,
                        change_velocity_30d=velocity,
                        is_dead_code_candidate=False,
                        last_modified=last_modified,
                        cyclomatic_complexity=getattr(analysis, "cyclomatic_complexity", None),
                        file_age_days=file_age_days,
                        has_explicit_exports=has_explicit_exports,
                        has_test_file=has_test_file,
                    )
                    modules[module.path] = module
                    graph.add_module(module)

                    # Imports -> IMPORTS edges (string-level; resolution can be added later)
                    for imp in analysis.imports:
                        graph.add_import_edge(module.path, imp, weight=1)
                    # Public classes could be surfaced later; for now they enrich
                    # the semantic index via ModuleAnalysisResult.

                elif language == "yaml":
                    cfg = self.dag_parser.parse(path)
                    rel_path = str(path.relative_to(repo_root))
                    module = ModuleNode(
                        path=rel_path,
                        language="yaml",
                        purpose_statement=None,
                        domain_cluster=None,
                        complexity_score=0.0,
                        change_velocity_30d=velocity,
                        is_dead_code_candidate=False,
                        last_modified=last_modified,
                        file_age_days=self._file_age_days(last_modified),
                        has_test_file=self._has_corresponding_test(repo_root, rel_path),
                    )
                    modules[module.path] = module
                    graph.add_module(module)
                    if cfg:
                        for task in cfg.tasks:
                            graph.add_configures_edge(module.path, task, cfg.config_type)

                else:
                    rel_path = str(path.relative_to(repo_root))
                    module = ModuleNode(
                        path=rel_path,
                        language=language,
                        purpose_statement=None,
                        domain_cluster=None,
                        complexity_score=0.0,
                        change_velocity_30d=velocity,
                        is_dead_code_candidate=False,
                        last_modified=last_modified,
                        file_age_days=self._file_age_days(last_modified),
                        has_test_file=self._has_corresponding_test(repo_root, rel_path),
                    )
                    modules[module.path] = module
                    graph.add_module(module)
            except Exception as e:
                rel = str(path.relative_to(repo_root))
                log_file_skip(
                    get_logger("surveyor"),
                    "surveyor",
                    rel,
                    e,
                    error_collector=error_collector,
                )

        # Graph-level analyses: PageRank, degrees, cycles, and dead code candidates.
        pr = graph.pagerank()
        scc = list(graph.strongly_connected_components())
        cycle_nodes = {n for comp in scc if len(comp) > 1 for n in comp}

        import_depths = self._compute_import_depths(graph)

        for path, module in modules.items():
            if path in pr:
                module.pagerank = float(pr[path])
            if path in cycle_nodes:
                module.is_in_cycle = True
            if path in graph.module_graph:
                in_d = int(graph.module_graph.in_degree(path))
                out_d = int(graph.module_graph.out_degree(path))
                module.in_degree = in_d
                module.out_degree = out_d
                module.coupling_score = in_d + out_d
            if path in import_depths:
                module.import_depth = import_depths[path]

        # Dead-code candidates: tuned heuristics
        entrypoint_prefixes = ("bin/", "scripts/", "dags/", "dg_deployments/", "main.py", "__main__")
        for path, module in modules.items():
            if path not in graph.module_graph:
                continue
            has_incoming = any(
                d.get("type") in {ModuleEdgeType.IMPORTS.value, ModuleEdgeType.CALLS.value}
                for _, _, d in graph.module_graph.in_edges(path, data=True)
            )
            is_entrypoint = path.startswith(entrypoint_prefixes) or path.endswith("main.py")
            if is_entrypoint:
                continue
            if has_incoming:
                continue
            if module.change_velocity_30d > 0:
                continue
            # Require sufficient staleness (file age)
            age = module.file_age_days or 0
            if age < self.config.dead_code_stale_days:
                continue
            # Don't flag modules with __all__ (explicit public API)
            if module.has_explicit_exports:
                continue
            # If has corresponding test, require older file to flag (stronger signal)
            if module.has_test_file and age < self.config.dead_code_stale_days * 2:
                continue
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

    @staticmethod
    def _file_age_days(last_modified: datetime) -> float:
        now = datetime.now(timezone.utc)
        if last_modified.tzinfo:
            lm = last_modified.astimezone(timezone.utc)
        else:
            lm = last_modified.replace(tzinfo=timezone.utc)
        delta = now - lm
        return max(0.0, delta.total_seconds() / 86400.0)

    @staticmethod
    def _has_explicit_exports(path: Path) -> bool:
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
            return bool(re.search(r"^\s*__all__\s*=", text, re.MULTILINE))
        except Exception:
            return False

    @staticmethod
    def _has_corresponding_test(repo_root: Path, rel_path: str) -> bool:
        """Check if a corresponding test file exists (tests/test_*.py, *_test.py)."""
        stem = Path(rel_path).stem
        if stem.startswith("test_") or stem.endswith("_test"):
            return True
        candidates = [
            f"tests/test_{stem}.py",
            f"test/test_{stem}.py",
            f"tests/{stem}_test.py",
            str(Path(rel_path).with_name(f"test_{stem}.py")),
        ]
        for c in candidates:
            if (repo_root / c).exists():
                return True
        return False

    @staticmethod
    def _compute_import_depths(graph: KnowledgeGraph) -> Dict[str, int]:
        """Compute import depth: 1 + max(depth of modules this one imports)."""
        g = graph.module_graph
        if not g.nodes:
            return {}
        try:
            import networkx as nx

            order = list(nx.topological_sort(g))
        except (Exception, ImportError):
            return {n: 0 for n in g.nodes}
        depths: Dict[str, int] = {}
        for node in reversed(order):
            succs = list(g.successors(node))
            if not succs:
                depths[node] = 0
            else:
                depths[node] = 1 + max(depths.get(s, 0) for s in succs)
        return depths

