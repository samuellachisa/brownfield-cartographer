from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple

import nbformat


@dataclass
class NotebookDataDependency:
    """
    Extremely lightweight lineage extracted from Jupyter notebooks.

    We intentionally avoid full AST reconstruction and instead scan for common
    IO patterns in code cells:
    - pandas.read_sql_table("table")
    - spark.read.table("table")
    - DataFrame.to_sql("table")
    """

    sources: List[str]
    targets: List[str]
    location: Tuple[int, int]


class NotebookLineageAnalyzer:
    """Best-effort lineage analyzer for .ipynb files."""

    def analyze_file(self, path: Path) -> Iterable[NotebookDataDependency]:
        try:
            nb = nbformat.read(str(path), as_version=4)
        except Exception:
            return []

        deps: List[NotebookDataDependency] = []
        cell_index = 0
        for cell in nb.cells:
            cell_index += 1
            if cell.get("cell_type") != "code":
                continue
            src = cell.get("source") or ""
            if not src.strip():
                continue
            sources: List[str] = []
            targets: List[str] = []

            # Extremely simple string heuristics to avoid importing heavy parsers
            # for notebook content; this is a supplemental signal, not a full
            # substitute for static analysis over .py/.sql.
            lines = src.splitlines()
            for ln in lines:
                stripped = ln.strip()
                if "read_sql_table" in stripped or "spark.read.table" in stripped:
                    tbl = _extract_first_quoted(stripped)
                    if tbl:
                        sources.append(tbl)
                if ".to_sql" in stripped:
                    tbl = _extract_first_quoted(stripped)
                    if tbl:
                        targets.append(tbl)

            if sources or targets:
                deps.append(
                    NotebookDataDependency(
                        sources=sources,
                        targets=targets,
                        location=(cell_index, cell_index),
                    )
                )
        return deps


def _extract_first_quoted(line: str) -> str | None:
    import re

    m = re.search(r'["\']([^"\']+)["\']', line)
    return m.group(1) if m else None

