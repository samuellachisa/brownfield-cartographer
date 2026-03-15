from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple

import ast


@dataclass
class PythonDataDependency:
    """
    Best-effort Python dataflow dependency extracted from IO-style calls.

    This intentionally focuses on common patterns (pandas/Spark-style reads
    and writes) rather than attempting full dataflow analysis.
    """

    sources: List[str]
    targets: List[str]
    op_type: str
    location: Tuple[int, int]


class PythonLineageAnalyzer:
    """
    Lightweight Python data lineage analyzer.

    Heuristics:
    - Writes:
      - DataFrame.to_sql(\"table_name\", ...)
      - DataFrame.write.saveAsTable(\"table_name\")
    - Reads:
      - pandas.read_sql_table(\"table_name\", ...)
      - spark.read.table(\"table_name\")
    """

    WRITE_FUNCS = {"to_sql"}
    READ_FUNCS = {"read_sql_table"}

    def analyze_file(self, path: Path) -> Iterable[PythonDataDependency]:
        try:
            source = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            return []

        try:
            tree = ast.parse(source, filename=str(path))
        except SyntaxError:
            return []

        deps: List[PythonDataDependency] = []

        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                dep = self._from_call(node)
                if dep is not None:
                    deps.append(dep)

        return deps

    def _from_call(self, call: ast.Call) -> PythonDataDependency | None:
        # Attribute-style calls: obj.method(...)
        func = call.func
        if isinstance(func, ast.Attribute):
            name = func.attr
            if name in self.WRITE_FUNCS:
                target = self._first_str_arg(call)
                if target:
                    return PythonDataDependency(
                        sources=[],
                        targets=[target],
                        op_type="python_write",
                        location=(call.lineno, getattr(call, "end_lineno", call.lineno)),
                    )

        # pandas.read_sql_table("table_name", ...)
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            qual = f"{func.value.id}.{func.attr}"
            if qual in {"pd.read_sql_table", "pandas.read_sql_table"}:
                source = self._first_str_arg(call)
                if source:
                    return PythonDataDependency(
                        sources=[source],
                        targets=[],
                        op_type="python_read",
                        location=(call.lineno, getattr(call, "end_lineno", call.lineno)),
                    )

        # spark.read.table("table_name")
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Attribute):
            qual = f"{getattr(func.value.value, 'id', '')}.{func.value.attr}.{func.attr}"
            if qual.endswith("spark.read.table") or qual == "spark.read.table":
                source = self._first_str_arg(call)
                if source:
                    return PythonDataDependency(
                        sources=[source],
                        targets=[],
                        op_type="python_read",
                        location=(call.lineno, getattr(call, "end_lineno", call.lineno)),
                    )

        return None

    @staticmethod
    def _first_str_arg(call: ast.Call) -> str | None:
        if call.args:
            first = call.args[0]
            if isinstance(first, ast.Constant) and isinstance(first.value, str):
                return first.value
        return None

