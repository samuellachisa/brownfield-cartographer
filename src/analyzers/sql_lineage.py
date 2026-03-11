from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple, Optional

import sqlglot
from sqlglot import exp


@dataclass
class SQLDependency:
    sources: List[str]
    targets: List[str]
    sql: str
    statement_index: int
    line_range: Optional[Tuple[int, int]]
    cte_names: List[str]


class SQLLineageAnalyzer:
    """
    sqlglot-based SQL dependency extraction.

    Given a .sql file, extracts table dependencies from SELECT/FROM/JOIN/CTE
    chains and DML/DDL targets.
    """

    def __init__(self, dialect: str | None = None) -> None:
        self.dialect = dialect

    def analyze_file(self, path: Path) -> Iterable[SQLDependency]:
        text = path.read_text(encoding="utf-8", errors="ignore")
        # Best-effort normalization for common dbt-style ref() macros embedded
        # in templating (e.g. {{ ref("my_table") }}). We strip the templating
        # delimiters so sqlglot can parse the underlying SQL and we separately
        # track the referenced objects as sources.
        ref_sources: set[str] = set()
        try:
            import re

            def _ref_repl(match: re.Match) -> str:
                inner = match.group(1)
                # inner is like ref('my_table') or ref(\"my_table\")
                name_match = re.search(r\"ref\\(['\\\"]([^'\\\"]+)['\\\"]\\)\", inner)
                if name_match:
                    tbl = name_match.group(1)
                    ref_sources.add(tbl)
                    return tbl
                return inner

            text_for_parse = re.sub(r\"\\{\\{\\s*(.*?)\\s*\\}\\}\", _ref_repl, text)
        except Exception:
            text_for_parse = text
        try:
            statements = sqlglot.parse(text_for_parse, read=self.dialect)
        except Exception as e:
            # Best-effort: log and skip files we can't tokenize/parse instead of
            # failing the entire hydrologist run.
            print(f"[hydrologist] Failed to parse SQL file {path} with dialect={self.dialect}: {e}")
            return []

        # Pre-split original text for best-effort line range calculation.
        lines = text.splitlines()

        for idx, stmt in enumerate(statements):
            sources = {t.sql(dialect=self.dialect) for t in stmt.find_all(exp.Table)}
            sources.update(ref_sources)
            targets = set()
            for create in stmt.find_all(exp.Create):
                for t in create.find_all(exp.Table):
                    targets.add(t.sql(dialect=self.dialect))
            for insert in stmt.find_all(exp.Insert):
                if insert.this:
                    targets.add(insert.this.sql(dialect=self.dialect))

            # CTE metadata: track names of common table expressions to aid
            # downstream lineage analysis for complex WITH chains.
            cte_names: List[str] = []
            for cte in stmt.find_all(exp.CTE):
                if isinstance(cte.this, exp.Alias) and isinstance(cte.this.this, exp.Subquery):
                    # WITH my_cte AS (SELECT ...)
                    alias = cte.this.alias
                    if alias and alias not in cte_names:
                        cte_names.append(alias)

            # Best-effort line range: search for the rendered SQL within the
            # original file text and compute 1-based line numbers.
            rendered = stmt.sql(dialect=self.dialect)
            start_line: Optional[int] = None
            end_line: Optional[int] = None
            joined = "\n".join(lines)
            try:
                offset = joined.index(rendered)
            except ValueError:
                offset = -1
            if offset >= 0:
                start_line = joined[:offset].count("\n") + 1
                end_line = start_line + rendered.count("\n")

            yield SQLDependency(
                sources=sorted(sources),
                targets=sorted(targets),
                sql=rendered,
                statement_index=idx,
                line_range=(start_line, end_line) if start_line is not None and end_line is not None else None,
                cte_names=cte_names,
            )

